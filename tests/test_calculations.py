"""Unit tests for scanner calculations, ranking, and filters."""
from __future__ import annotations

from models import compute_gap_pct, compute_spread_pct, compute_rvol_proxy, Candidate
from ranker import compute_rank_score, rank_candidates, gap_score
from scanner import _should_include


def test_compute_gap_pct():
    assert abs((compute_gap_pct(10.0, 10.4) or 0) - 4.0) < 0.0001
    assert abs((compute_gap_pct(10.0, 9.6) or 0) - (-4.0)) < 0.0001
    assert compute_gap_pct(0.0, 10.0) is None


def test_compute_spread_pct():
    pct = compute_spread_pct(9.9, 10.1)
    assert pct is not None
    assert pct > 0


def test_compute_rvol_proxy():
    assert compute_rvol_proxy(1_000_000, 500_000) == 2.0
    assert compute_rvol_proxy(0, 500_000) == 0
    assert compute_rvol_proxy(1_000_000, 0) is None


def _dummy_candidate(**overrides):
    base = dict(
        symbol="XYZ",
        last_price=5.0,
        prev_close=4.8,
        open_price=5.0,
        gap_pct=4.17,
        float_shares=5_000_000,
        premkt_volume=500_000,
        avg10_volume=300_000,
        rvol_proxy=4.0,
        bid=4.99,
        ask=5.01,
        spread_pct=0.4,
    )
    base.update(overrides)
    return Candidate(**base)


def test_ranker_orders_higher_score_first():
    c1 = _dummy_candidate(rvol_proxy=2.0, premkt_volume=200_000)
    c2 = _dummy_candidate(rvol_proxy=8.0, premkt_volume=1_000_000)
    c1.rank_score = compute_rank_score(c1)
    c2.rank_score = compute_rank_score(c2)
    ranked = rank_candidates([c1, c2])
    assert ranked[0].rank_score >= ranked[1].rank_score


def test_filters_basic_pass():
    c = _dummy_candidate()
    assert _should_include(
        c,
        price_min=2.0,
        price_max=10.0,
        gap_min=4.0,
        float_max=10_000_000,
        premkt_vol_min=200_000,
        rvol_min=3.0,
        max_spread_pct=0.5,
    )
    assert c.reasons


def test_filters_reject_on_gap():
    c = _dummy_candidate(gap_pct=3.0)
    assert not _should_include(
        c,
        price_min=2.0,
        price_max=10.0,
        gap_min=4.0,
        float_max=10_000_000,
        premkt_vol_min=200_000,
        rvol_min=3.0,
        max_spread_pct=0.5,
    )


def test_gap_score_peaks_around_8():
    """Gap sweet spot: score should peak near 8% and decay after ~15%."""
    assert gap_score(8.0) > gap_score(4.0)
    assert gap_score(8.0) > gap_score(12.0)
    assert gap_score(8.0) > gap_score(15.0)
    assert gap_score(4.0) > gap_score(20.0)


def test_gap_pct_two_trading_days():
    """Gap = (today_open - prev_close) / prev_close * 100."""
    assert abs((compute_gap_pct(10.0, 10.4) or 0) - 4.0) < 0.0001
    assert abs((compute_gap_pct(5.0, 5.2) or 0) - 4.0) < 0.0001


def test_rvol_proxy_from_daily_volumes():
    """rvol_proxy = today_volume / avg10_volume."""
    assert abs((compute_rvol_proxy(3_000_000, 1_000_000) or 0) - 3.0) < 0.0001
    assert abs((compute_rvol_proxy(500_000, 100_000) or 0) - 5.0) < 0.0001


def test_ranker_prefers_daily_volume_in_backtest():
    """When daily_volume is set, ranker uses it for volume term."""
    c1 = _dummy_candidate(daily_volume=1_000_000, premkt_volume=0, rvol_proxy=5.0)
    c2 = _dummy_candidate(daily_volume=100_000, premkt_volume=0, rvol_proxy=5.0)
    s1 = compute_rank_score(c1)
    s2 = compute_rank_score(c2)
    assert s1 > s2

