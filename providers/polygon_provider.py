from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, TypedDict

import requests

from models import compute_gap_pct, compute_spread_pct, compute_rvol_proxy

logger = logging.getLogger(__name__)


class GroupedBar(TypedDict, total=False):
    open: float
    close: float
    volume: float
    vwap: Optional[float]


@dataclass
class SymbolSnapshot:
    symbol: str
    prev_close: Optional[float]
    open_price: Optional[float]
    last_price: Optional[float]
    premkt_volume: Optional[float]
    avg10_volume: Optional[float]
    float_shares: Optional[int]
    bid: Optional[float]
    ask: Optional[float]
    today_volume: Optional[float]


class PolygonProvider:
    """
    Thin wrapper around Polygon.io APIs used by the scanner.
    """

    def __init__(self, api_key: Optional[str] = None, cache_dir: str = "cache") -> None:
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise RuntimeError("POLYGON_API_KEY is not set in environment.")

        self.base_url = "https://api.polygon.io"
        self.session = requests.Session()
        self.session.params = {"apiKey": self.api_key}

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._grouped_cache_dir = self.cache_dir / "grouped"
        self._grouped_cache_dir.mkdir(parents=True, exist_ok=True)
        self._float_cache_path = self.cache_dir / "float_cache.json"
        self._avgvol_cache_path = self.cache_dir / "avg10_cache.json"
        self._float_cache: Dict[str, Any] = self._load_cache(self._float_cache_path)
        self._avgvol_cache: Dict[str, float] = self._load_cache(self._avgvol_cache_path)

    @staticmethod
    def _load_cache(path: Path) -> Dict[str, Any]:
        if path.exists():
            try:
                with path.open("r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                logger.warning("Failed to load cache from %s", path)
        return {}

    @staticmethod
    def _save_cache(path: Path, data: Dict[str, Any]) -> None:
        try:
            with path.open("w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception:
            logger.warning("Failed to save cache to %s", path)

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        merged_params = dict(self.session.params)
        if params:
            merged_params.update(params)

        backoff = 1.0
        while True:
            try:
                resp = self.session.get(url, params=merged_params, timeout=10)
            except requests.RequestException as exc:
                logger.error("HTTP error %s for %s", exc, url)
                return None

            if resp.status_code == 429:
                logger.warning("Rate limited by Polygon, backing off for %ss", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue

            if not resp.ok:
                logger.error(
                    "Polygon request failed: status=%s body=%s",
                    resp.status_code,
                    resp.text[:500],
                )
                return None

            try:
                return resp.json()
            except ValueError:
                logger.error("Failed to decode JSON from Polygon for %s", url)
                return None

    def _get_float_from_cache(self, symbol: str) -> Optional[int]:
        """Resolve float value from cache, supporting both legacy int and {value, fetched_at} format."""
        raw = self._float_cache.get(symbol)
        if raw is None:
            return None
        if isinstance(raw, dict) and "value" in raw:
            return int(raw["value"])
        if isinstance(raw, (int, float)):
            return int(raw)
        return None

    def _set_float_cache(self, symbol: str, value: int) -> None:
        """Store float with timestamp for audit."""
        from datetime import datetime, timezone

        self._float_cache[symbol] = {
            "value": value,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_cache(self._float_cache_path, self._float_cache)

    def get_grouped_daily(self, target_date: date) -> Dict[str, GroupedBar]:
        """
        Fetch grouped daily bars for all US stocks on target_date.
        Uses Polygon /v2/aggs/grouped/... endpoint. Cached per day in cache/grouped/YYYY-MM-DD.json.
        Returns dict[ticker] = {open, close, volume, vwap(optional)}.
        """
        cache_file = self._grouped_cache_dir / f"{target_date}.json"
        if cache_file.exists():
            try:
                with cache_file.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                return data
            except Exception as exc:
                logger.warning("Failed to load grouped cache %s: %s", cache_file, exc)

        url_path = f"/v2/aggs/grouped/locale/us/market/stocks/{target_date}"
        all_results: Dict[str, GroupedBar] = {}

        def _parse_results(data: Dict[str, Any]) -> None:
            for r in data.get("results") or []:
                ticker = r.get("T")
                if not ticker:
                    continue
                bar: GroupedBar = {
                    "open": float(r.get("o", 0)),
                    "close": float(r.get("c", 0)),
                    "volume": float(r.get("v", 0)),
                }
                if r.get("vw") is not None:
                    bar["vwap"] = float(r["vw"])
                all_results[ticker] = bar

        while url_path:
            if url_path.startswith("http"):
                backoff = 1.0
                while True:
                    try:
                        resp = self.session.get(url_path, timeout=30)
                    except requests.RequestException as exc:
                        logger.error("HTTP error %s", exc)
                        break
                    if resp.status_code == 429:
                        logger.warning("Rate limited, backing off %ss", backoff)
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 60.0)
                        continue
                    break
                if not resp.ok:
                    logger.error("Grouped bars failed: %s %s", resp.status_code, resp.text[:300])
                    break
                try:
                    data = resp.json()
                except ValueError:
                    break
                _parse_results(data)
                url_path = data.get("next_url") or ""
            else:
                data = self._get(url_path)
                if data is None:
                    break
                _parse_results(data)
                url_path = data.get("next_url") or ""

        if all_results:
            try:
                with cache_file.open("w", encoding="utf-8") as f:
                    json.dump(all_results, f)
            except Exception as exc:
                logger.warning("Failed to save grouped cache %s: %s", cache_file, exc)

        return all_results

    def get_symbol_universe(self) -> List[str]:
        """
        Fetch active US-listed stocks (excluding OTC).
        """
        tickers: List[str] = []
        url_path = "/v3/reference/tickers"
        params: Dict[str, Any] = {
            "market": "stocks",
            "active": "true",
            "limit": 1000,
        }

        while True:
            data = self._get(url_path, params=params)
            if not data or "results" not in data:
                break
            for item in data["results"]:
                if item.get("market") == "otc":
                    continue
                if item.get("locale") != "us":
                    continue
                symbol = item.get("ticker")
                if symbol:
                    tickers.append(symbol)

            next_url = data.get("next_url")
            if not next_url:
                break
            # Polygon provides an absolute next_url. Strip base and reuse path/params.
            if "?" in next_url:
                url_path = next_url.split(self.base_url)[-1].split("?")[0]
                # reset params so that _get uses query from next_url only
                params = {}
            else:
                url_path = next_url.split(self.base_url)[-1]
                params = {}

        return tickers

    def _get_daily_agg(self, symbol: str, from_date: date, to_date: date) -> Optional[Dict]:
        path = f"/v2/aggs/ticker/{symbol}/range/1/day/{from_date}/{to_date}"
        data = self._get(path, params={"adjusted": "true", "sort": "asc", "limit": 50})
        if not data or "results" not in data or not data["results"]:
            return None
        return data["results"][-1]

    def _get_previous_close(self, symbol: str, target_date: date) -> Optional[float]:
        # Simple previous-calendar-day approximation; production systems should use a trading calendar.
        prev_date = target_date - timedelta(days=1)
        agg = self._get_daily_agg(symbol, prev_date, prev_date)
        return float(agg["c"]) if agg and "c" in agg else None

    def _get_today_open_and_volume(self, symbol: str, target_date: date) -> (Optional[float], Optional[float]):
        agg = self._get_daily_agg(symbol, target_date, target_date)
        if not agg:
            return None, None
        open_price = float(agg.get("o")) if agg.get("o") is not None else None
        volume = float(agg.get("v")) if agg.get("v") is not None else None
        return open_price, volume

    def _get_last_quote(self, symbol: str) -> (Optional[float], Optional[float], Optional[float]):
        path = f"/v2/last/nbbo/{symbol}"
        data = self._get(path)
        if not data or "results" not in data:
            return None, None, None
        res = data["results"]
        bid = float(res["b"]) if res.get("b") is not None else None
        ask = float(res["a"]) if res.get("a") is not None else None
        last = None
        if res.get("bp") is not None and res.get("ap") is not None:
            last = (float(res["bp"]) + float(res["ap"])) / 2.0
        return last, bid, ask

    def _get_premarket_volume(self, symbol: str, target_date: date) -> Optional[float]:
        """
        Attempt to approximate premarket volume between 04:00 and 09:30 US/Eastern.
        This uses minute aggregates; if unavailable, returns None.
        """
        # Polygon accepts ISO8601 timestamps; here we approximate using local dates.
        # In production, you'd convert explicit US/Eastern times to UTC.
        from_ts = f"{target_date} 04:00:00"
        to_ts = f"{target_date} 09:30:00"
        path = f"/v2/aggs/ticker/{symbol}/range/1/minute/{from_ts}/{to_ts}"
        data = self._get(path, params={"adjusted": "true", "sort": "asc", "limit": 5000})
        if not data or "results" not in data:
            return None
        return float(sum(r.get("v", 0.0) for r in data["results"]))

    def _get_avg10_volume(self, symbol: str, target_date: date) -> Optional[float]:
        if symbol in self._avgvol_cache:
            return float(self._avgvol_cache[symbol])

        from_date = target_date - timedelta(days=20)
        path = f"/v2/aggs/ticker/{symbol}/range/1/day/{from_date}/{target_date}"
        data = self._get(path, params={"adjusted": "true", "sort": "desc", "limit": 15})
        if not data or "results" not in data:
            return None
        volumes = [float(r["v"]) for r in data["results"][:10] if r.get("v") is not None]
        if not volumes:
            return None
        avg10 = sum(volumes) / len(volumes)
        self._avgvol_cache[symbol] = avg10
        self._save_cache(self._avgvol_cache_path, self._avgvol_cache)
        return avg10

    def _get_float_shares(self, symbol: str) -> Optional[int]:
        cached = self._get_float_from_cache(symbol)
        if cached is not None:
            return cached

        path = f"/v3/reference/tickers/{symbol}"
        data = self._get(path)
        if not data or "results" not in data:
            return None
        fundamentals = data["results"]
        # Polygon: share_class_shares_outstanding or weighted_shares_outstanding as proxy for float
        float_shares = fundamentals.get("share_class_shares_outstanding") or fundamentals.get(
            "weighted_shares_outstanding"
        )
        if float_shares is None:
            return None
        float_int = int(float_shares)
        self._set_float_cache(symbol, float_int)
        return float_int

    def get_snapshot(self, symbol: str, target_date: date) -> Optional[SymbolSnapshot]:
        prev_close = self._get_previous_close(symbol, target_date)
        open_price, today_volume = self._get_today_open_and_volume(symbol, target_date)
        last_price, bid, ask = self._get_last_quote(symbol)
        premkt_volume = self._get_premarket_volume(symbol, target_date)
        avg10_volume = self._get_avg10_volume(symbol, target_date)
        float_shares = self._get_float_shares(symbol)

        return SymbolSnapshot(
            symbol=symbol,
            prev_close=prev_close,
            open_price=open_price,
            last_price=last_price,
            premkt_volume=premkt_volume,
            avg10_volume=avg10_volume,
            float_shares=float_shares,
            bid=bid,
            ask=ask,
            today_volume=today_volume,
        )

    def build_candidate_from_snapshot(self, snapshot: SymbolSnapshot) -> Optional[Dict[str, Any]]:
        if snapshot.prev_close is None or snapshot.open_price is None:
            return None
        gap_pct = compute_gap_pct(snapshot.prev_close, snapshot.open_price)
        spread_pct = compute_spread_pct(snapshot.bid, snapshot.ask)
        rvol_proxy = compute_rvol_proxy(snapshot.today_volume or snapshot.premkt_volume, snapshot.avg10_volume)

        return {
            "symbol": snapshot.symbol,
            "prev_close": snapshot.prev_close,
            "open_price": snapshot.open_price,
            "last_price": snapshot.last_price or snapshot.open_price,
            "gap_pct": gap_pct,
            "float_shares": snapshot.float_shares,
            "premkt_volume": snapshot.premkt_volume,
            "avg10_volume": snapshot.avg10_volume,
            "rvol_proxy": rvol_proxy,
            "bid": snapshot.bid,
            "ask": snapshot.ask,
            "spread_pct": spread_pct,
        }

