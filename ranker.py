from __future__ import annotations

import math
from typing import List

from models import Candidate


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def gap_score(gap_pct: float) -> float:
    """
    Score gaps with a peak around 8% and decay after ~15%.
    """
    if gap_pct is None:
        return 0.0
    # Use a Gaussian-like bump centered at 8 with wide spread.
    center = 8.0
    spread = 6.0
    return math.exp(-((gap_pct - center) ** 2) / (2 * spread**2))


def compute_rank_score(candidate: Candidate) -> float:
    rvol = candidate.rvol_proxy or 0.0
    premkt_vol = candidate.premkt_volume or 0.0
    gap = candidate.gap_pct or 0.0

    rvol_term = 0.4 * _clamp(rvol, 0.0, 10.0)
    volume_term = 0.3 * math.log10(premkt_vol + 1.0)
    gap_term = 0.3 * gap_score(gap)
    return rvol_term + volume_term + gap_term


def rank_candidates(candidates: List[Candidate]) -> List[Candidate]:
    for c in candidates:
        c.rank_score = compute_rank_score(c)
    return sorted(candidates, key=lambda c: (c.rank_score or 0.0), reverse=True)

