from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any


def compute_gap_pct(prev_close: float, open_price: float) -> Optional[float]:
    if prev_close is None or open_price is None:
        return None
    if prev_close <= 0:
        return None
    return (open_price - prev_close) / prev_close * 100.0


def compute_spread_pct(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return (ask - bid) / mid * 100.0


def compute_rvol_proxy(
    current_volume: Optional[float], avg10_volume: Optional[float]
) -> Optional[float]:
    if current_volume is None or avg10_volume is None:
        return None
    if avg10_volume <= 0:
        return None
    return current_volume / avg10_volume


@dataclass
class Candidate:
    symbol: str
    last_price: Optional[float]
    prev_close: Optional[float]
    open_price: Optional[float]
    gap_pct: Optional[float]
    float_shares: Optional[int]
    premkt_volume: Optional[float]
    avg10_volume: Optional[float]
    rvol_proxy: Optional[float]
    bid: Optional[float]
    ask: Optional[float]
    spread_pct: Optional[float]
    daily_volume: Optional[float] = None  # Used in backtest; volume term uses daily_volume or premkt_volume
    rank_score: Optional[float] = None
    reasons: List[str] = field(default_factory=list)
    timestamp: datetime | None = None

    def to_csv_row(self) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "symbol": self.symbol,
            "last_price": self.last_price,
            "prev_close": self.prev_close,
            "open": self.open_price,
            "gap_pct": self.gap_pct,
            "float_shares": self.float_shares,
            "premkt_volume": self.premkt_volume,
            "avg10_volume": self.avg10_volume,
            "rvol_proxy": self.rvol_proxy,
            "bid": self.bid,
            "ask": self.ask,
            "spread_pct": self.spread_pct,
            "rank_score": self.rank_score,
            "reasons": ";".join(self.reasons),
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }
        if self.daily_volume is not None:
            row["daily_volume"] = self.daily_volume
        return row

