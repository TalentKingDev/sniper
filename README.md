## Small-Cap Gap Scanner (v8.0_Susan)

**Goal**: scan US-listed small-cap stocks for intraday gap-and-go candidates, rank them, export a CSV, and optionally push the top candidates into Option Alpha via webhook using the `CANDIDATE_LIST` / `CANDIDATE` actions.

### Setup

- **Python**: 3.11+
- **Install dependencies**:

```bash
pip install -r requirements.txt
```

- **Environment**:
  - Set your Polygon.io API key:

```bash
set POLYGON_API_KEY=YOUR_KEY_HERE  # Windows PowerShell: $env:POLYGON_API_KEY="YOUR_KEY_HERE"
```

### CLI usage

Basic example:

```bash
python scanner.py \
  --date 2026-02-25 \
  --webhook-url https://your-option-alpha-webhook-url \
  --secret YOUR_SHARED_SECRET
```

Key options:

- `--float-max` (default `10000000`)
- `--gap-min` (default `4.0`)
- `--price-min` (default `2.0`)
- `--price-max` (default `10.0`)
- `--premkt-vol-min` (default `200000`)
- `--rvol-min` (default `3.0`)
- `--max-spread-pct` (default `0.5`)
- `--top` (default `50`)
- `--no-webhook` (only write CSV / log to console)
- `--debug` (enable verbose logging)

### Calculations

- **Universe**: active US stock tickers via Polygon `/v3/reference/tickers` (exclude `market="otc"`).
- **Previous close**: prior calendar day aggregate close (Polygon `/v2/aggs/ticker/.../range/1/day`).
- **Today open**: same-day aggregate open.
- **Last price**: NBBO mid (\( (bid\_price + ask\_price) / 2 \)).
- **Premarket volume**: sum of 1-minute volume between ~04:00–09:30 local session using `/v2/aggs/ticker/.../range/1/minute`. If unavailable, it is set to `None` and filters using it are skipped.
- **Avg 10-day volume**: average of the most recent 10 daily volumes (up to 20 calendar days back).
- **Float shares**: from Polygon reference fundamentals; cached to `cache/float_cache.json`.
- **Gap**:

```text
gap_pct = (open - prev_close) / prev_close * 100
```

- **Spread**:

```text
mid = (bid + ask) / 2
spread_pct = (ask - bid) / mid * 100
```

- **RVOL proxy**:

```text
rvol_proxy = (today_volume or premarket_volume) / avg10_volume
```

### Filters

A symbol is included if:

- `price_min <= price <= price_max` (price = open, else last price)
- `gap_pct >= gap_min`
- `float_shares <= float_max`
- If premarket volume is present: `premkt_volume >= premkt_vol_min`
- If RVOL proxy present: `rvol_proxy >= rvol_min`
- If bid/ask present: `spread_pct <= max_spread_pct`

The `reasons` column in the CSV records the conditions the symbol satisfied.

### Ranking

Each candidate receives a score:

```text
score =
  0.4 * clamp(rvol_proxy, 0, 10)
  + 0.3 * log10(premarket_volume + 1)
  + 0.3 * gap_score(gap_pct)
```

Where `gap_score` peaks around 8% and decays beyond ~15% (Gaussian-style bump).

The scanner sorts by `score` (descending), writes the top `--top` results to `outputs/candidates_<date>.csv`, and logs the top 20 to the console.

### Webhook contract (Option Alpha-friendly)

By default, the scanner sends a single `CANDIDATE_LIST` event with the top N candidates:

```json
{
  "version": "8.0",
  "event_id": "b9a1f1c6-04b3-4e01-9b24-6f3f6a123456",
  "timestamp": "2026-02-25T14:35:12.123456Z",
  "tag": "v8.0_Susan",
  "action": "CANDIDATE_LIST",
  "secret_key": "YOUR_SHARED_SECRET",
  "data": {
    "candidate_count": 50,
    "candidates": [
      {
        "symbol": "XYZ",
        "price": 4.12,
        "gap_pct": 6.1,
        "float": 8200000,
        "premkt_volume": 450000,
        "rvol": 5.2,
        "score": 3.84
      }
    ]
  }
}
```

If Option Alpha cannot accept arrays, enable the **fallback mode** in `webhook.send_candidate_list` to send multiple `CANDIDATE` events instead, each with one symbol:

```json
{
  "version": "8.0",
  "event_id": "f2b4a4a1-c2e7-4f01-a9b0-9123456789ab",
  "timestamp": "2026-02-25T14:36:01.000000Z",
  "tag": "v8.0_Susan",
  "action": "CANDIDATE",
  "secret_key": "YOUR_SHARED_SECRET",
  "symbol": "XYZ",
  "data": {
    "symbol": "XYZ",
    "price": 4.12,
    "gap_pct": 6.1,
    "float": 8200000,
    "premkt_volume": 450000,
    "rvol": 5.2,
    "score": 3.84
  }
}
```

### Testing

Run unit tests:

```bash
pytest
```

Tests cover:

- Gap, spread, and RVOL proxy calculations.
- Ranking behavior.
- Basic filter logic.

### Known limitations

- Market hours and premarket windows are approximated using calendar dates; for production, wire in an explicit US/Eastern trading calendar.
- Some symbols may be skipped if Polygon omits required fields (e.g., no daily aggregates or fundamentals).
- Network and API quota errors are logged; affected symbols are skipped rather than failing the entire run.

