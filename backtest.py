"""
Historical backtest pipeline for the Small-Cap Gap Scanner.
Uses Polygon Grouped Daily Bars (1 API call per date) + float lookups only for shortlisted symbols.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from models import Candidate, compute_gap_pct, compute_rvol_proxy
from providers.polygon_provider import PolygonProvider, GroupedBar
from ranker import rank_candidates
from webhook import send_candidate_list

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    start_date: date
    end_date: date
    price_min: float = 2.0
    price_max: float = 10.0
    gap_min: float = 4.0
    float_max: int = 10_000_000
    daily_vol_min: float = 500_000
    rvol_min: float = 3.0
    top_n: int = 50


@dataclass
class DailySummary:
    date: date
    candidates_found: int
    top_symbol_1: str = ""
    top_symbol_2: str = ""
    top_symbol_3: str = ""
    top_symbol_4: str = ""
    top_symbol_5: str = ""
    avg_gap: float = 0.0
    avg_rvol: float = 0.0
    avg_volume: float = 0.0


def _trading_dates_between(
    provider: PolygonProvider,
    start: date,
    end: date,
) -> List[date]:
    """
    Build list of trading dates by iterating calendar days and checking grouped endpoint.
    A trading day is one where grouped daily returns non-empty results.
    """
    from datetime import timedelta

    dates: List[date] = []
    current = start
    while current <= end:
        bars = provider.get_grouped_daily(current)
        if bars:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def _compute_avg10_volume(
    symbol: str,
    today_date: date,
    trading_dates: List[date],
    grouped_by_date: Dict[date, Dict[str, GroupedBar]],
) -> Optional[float]:
    """Compute 10-day average volume from prior 10 trading days (excluding today)."""
    idx = next((i for i, d in enumerate(trading_dates) if d == today_date), None)
    if idx is None or idx < 10:
        return None
    prior_dates = trading_dates[idx - 10 : idx]
    vols: List[float] = []
    for d in prior_dates:
        day_bars = grouped_by_date.get(d)
        if not day_bars:
            continue
        bar = day_bars.get(symbol)
        if bar and bar.get("volume", 0) > 0:
            vols.append(bar["volume"])
    if len(vols) < 5:  # need some history
        return None
    return sum(vols) / len(vols)


def run_backtest(
    config: BacktestConfig,
    provider: PolygonProvider,
    *,
    webhook_url: Optional[str] = None,
    secret: Optional[str] = None,
    send_webhook: bool = False,
    output_dir: str = "outputs/backtest",
) -> List[DailySummary]:
    """
    Run historical backtest: for each trading day, find candidates, rank, save per-day CSV,
    and optionally send webhook. Returns list of daily summaries for summary CSV.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    trading_dates = _trading_dates_between(provider, config.start_date, config.end_date)
    logger.info("Found %d trading days between %s and %s", len(trading_dates), config.start_date, config.end_date)

    # Cache grouped data by date (we fetch as we iterate)
    grouped_by_date: Dict[date, Dict[str, GroupedBar]] = {}
    summaries: List[DailySummary] = []

    for i, today_date in enumerate(trading_dates):
        # Get previous trading day
        prev_date = trading_dates[i - 1] if i > 0 else None
        if prev_date is None:
            logger.debug("Skipping %s (no previous trading day)", today_date)
            continue

        # Fetch grouped bars for today and prev (prev may already be in cache from prior iteration)
        if today_date not in grouped_by_date:
            grouped_by_date[today_date] = provider.get_grouped_daily(today_date)
        if prev_date not in grouped_by_date:
            grouped_by_date[prev_date] = provider.get_grouped_daily(prev_date)

        today_bars = grouped_by_date[today_date]
        prev_bars = grouped_by_date[prev_date]

        # Pre-filter: price, gap, daily volume (no float yet)
        pre_candidates: List[Dict[str, Any]] = []
        for symbol, bar in today_bars.items():
            open_price = bar.get("open") or 0.0
            today_vol = bar.get("volume") or 0.0
            prev_bar = prev_bars.get(symbol)
            if not prev_bar:
                continue
            prev_close = prev_bar.get("close") or 0.0
            if prev_close <= 0:
                continue
            gap_pct = compute_gap_pct(prev_close, open_price)
            if gap_pct is None or gap_pct < config.gap_min:
                continue
            if not (config.price_min <= open_price <= config.price_max):
                continue
            if today_vol < config.daily_vol_min:
                continue

            avg10 = _compute_avg10_volume(symbol, today_date, trading_dates, grouped_by_date)
            rvol = compute_rvol_proxy(today_vol, avg10)
            if rvol is not None and rvol < config.rvol_min:
                continue

            pre_candidates.append({
                "symbol": symbol,
                "open_price": open_price,
                "prev_close": prev_close,
                "gap_pct": gap_pct,
                "today_volume": today_vol,
                "avg10_volume": avg10,
                "rvol_proxy": rvol,
            })

        # Float filter (expensive - only for shortlist)
        candidates: List[Candidate] = []
        for p in pre_candidates:
            float_shares = provider._get_float_shares(p["symbol"])
            if float_shares is not None and float_shares > config.float_max:
                continue
            reasons = [
                f"price_in_range[{config.price_min},{config.price_max}]",
                f"gap>={config.gap_min}",
                f"daily_vol>={config.daily_vol_min}",
                f"rvol>={config.rvol_min}",
            ]
            if float_shares is not None:
                reasons.append(f"float<={config.float_max}")
            else:
                reasons.append("float_unknown_included")
            c = Candidate(
                symbol=p["symbol"],
                last_price=p["open_price"],
                prev_close=p["prev_close"],
                open_price=p["open_price"],
                gap_pct=p["gap_pct"],
                float_shares=float_shares,
                premkt_volume=None,
                avg10_volume=p["avg10_volume"],
                rvol_proxy=p["rvol_proxy"],
                bid=None,
                ask=None,
                spread_pct=None,
                daily_volume=p["today_volume"],
                reasons=reasons,
                timestamp=datetime.utcnow(),
            )
            candidates.append(c)

        ranked = rank_candidates(candidates)
        top = ranked[: config.top_n]

        # Per-day CSV
        day_csv = output_path / f"candidates_{today_date}.csv"
        fieldnames = [
            "symbol", "last_price", "prev_close", "open", "gap_pct", "float_shares",
            "premkt_volume", "avg10_volume", "rvol_proxy", "bid", "ask", "spread_pct",
            "daily_volume", "rank_score", "reasons", "timestamp",
        ]
        with day_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for c in top:
                row = c.to_csv_row()
                writer.writerow(row)

        # Summary row
        avg_gap = sum((c.gap_pct or 0) for c in top) / len(top) if top else 0.0
        avg_rvol = sum((c.rvol_proxy or 0) for c in top) / len(top) if top else 0.0
        avg_vol = sum((c.daily_volume or 0) for c in top) / len(top) if top else 0.0
        summary = DailySummary(
            date=today_date,
            candidates_found=len(ranked),
            top_symbol_1=top[0].symbol if len(top) >= 1 else "",
            top_symbol_2=top[1].symbol if len(top) >= 2 else "",
            top_symbol_3=top[2].symbol if len(top) >= 3 else "",
            top_symbol_4=top[3].symbol if len(top) >= 4 else "",
            top_symbol_5=top[4].symbol if len(top) >= 5 else "",
            avg_gap=avg_gap,
            avg_rvol=avg_rvol,
            avg_volume=avg_vol,
        )
        summaries.append(summary)

        if send_webhook and webhook_url and secret and top:
            send_candidate_list(webhook_url, secret=secret, candidates=top)

        logger.info(
            "%s: %d candidates, top=%s",
            today_date,
            len(ranked),
            summary.top_symbol_1 or "(none)",
        )

    # Summary CSV
    summary_path = output_path / f"summary_{config.start_date.year}.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date", "candidates_found", "top_symbol_1", "top_symbol_2", "top_symbol_3",
                "top_symbol_4", "top_symbol_5", "avg_gap", "avg_rvol", "avg_volume",
            ],
        )
        writer.writeheader()
        for s in summaries:
            writer.writerow({
                "date": s.date,
                "candidates_found": s.candidates_found,
                "top_symbol_1": s.top_symbol_1,
                "top_symbol_2": s.top_symbol_2,
                "top_symbol_3": s.top_symbol_3,
                "top_symbol_4": s.top_symbol_4,
                "top_symbol_5": s.top_symbol_5,
                "avg_gap": f"{s.avg_gap:.2f}",
                "avg_rvol": f"{s.avg_rvol:.2f}",
                "avg_volume": f"{s.avg_volume:.0f}",
            })

    logger.info("Wrote summary to %s", summary_path)
    return summaries
