from __future__ import annotations

import argparse
import csv
import logging
from datetime import datetime, date
from pathlib import Path
from typing import List

from models import Candidate
from providers.polygon_provider import PolygonProvider
from ranker import rank_candidates
from webhook import send_candidate_list
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Small-Cap Gap Scanner")
    parser.add_argument(
        "--mode",
        choices=("live", "backtest"),
        default="live",
        help="live = single-day scan; backtest = historical scan",
    )
    parser.add_argument("--date", help="Scan date in YYYY-MM-DD (required for live mode)")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (required for backtest)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (required for backtest)")
    parser.add_argument("--webhook-url", help="Option Alpha webhook URL")
    parser.add_argument("--secret", help="Shared secret for webhook")
    parser.add_argument("--float-max", type=int, default=10_000_000)
    parser.add_argument("--gap-min", type=float, default=4.0)
    parser.add_argument("--price-min", type=float, default=2.0)
    parser.add_argument("--price-max", type=float, default=10.0)
    parser.add_argument("--premkt-vol-min", type=float, default=200_000)
    parser.add_argument("--daily-vol-min", type=float, default=500_000, help="Min daily volume (backtest)")
    parser.add_argument("--rvol-min", type=float, default=3.0)
    parser.add_argument("--max-spread-pct", type=float, default=0.5)
    parser.add_argument("--top", type=int, default=50)
    parser.add_argument("--no-webhook", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def _should_include(
    candidate: Candidate,
    *,
    price_min: float,
    price_max: float,
    gap_min: float,
    float_max: int,
    premkt_vol_min: float,
    rvol_min: float,
    max_spread_pct: float,
) -> bool:
    reasons: List[str] = []

    price = candidate.open_price or candidate.last_price
    if price is None:
        return False
    if not (price_min <= price <= price_max):
        return False
    reasons.append(f"price_in_range[{price_min},{price_max}]")

    if candidate.gap_pct is None or candidate.gap_pct < gap_min:
        return False
    reasons.append(f"gap>={gap_min}")

    if candidate.float_shares is None or candidate.float_shares > float_max:
        return False
    reasons.append(f"float<={float_max}")

    if candidate.premkt_volume is not None and candidate.premkt_volume < premkt_vol_min:
        return False
    if candidate.premkt_volume is not None:
        reasons.append(f"premkt_vol>={premkt_vol_min}")

    if candidate.rvol_proxy is not None and candidate.rvol_proxy < rvol_min:
        return False
    if candidate.rvol_proxy is not None:
        reasons.append(f"rvol>={rvol_min}")

    if candidate.spread_pct is not None and candidate.spread_pct > max_spread_pct:
        return False
    if candidate.spread_pct is not None:
        reasons.append(f"spread<={max_spread_pct}")

    candidate.reasons = reasons
    return True


def run_scan(args: argparse.Namespace) -> List[Candidate]:
    scan_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    provider = PolygonProvider()

    universe = provider.get_symbol_universe()
    logger.info("Fetched %d symbols in universe", len(universe))

    candidates: List[Candidate] = []

    for symbol in universe:
        try:
            snapshot = provider.get_snapshot(symbol, scan_date)
            if snapshot is None:
                continue

            data = provider.build_candidate_from_snapshot(snapshot)
            if data is None:
                continue

            candidate = Candidate(
                symbol=data["symbol"],
                last_price=data["last_price"],
                prev_close=data["prev_close"],
                open_price=data["open_price"],
                gap_pct=data["gap_pct"],
                float_shares=data["float_shares"],
                premkt_volume=data["premkt_volume"],
                avg10_volume=data["avg10_volume"],
                rvol_proxy=data["rvol_proxy"],
                bid=data["bid"],
                ask=data["ask"],
                spread_pct=data["spread_pct"],
                timestamp=datetime.utcnow(),
            )

            if _should_include(
                candidate,
                price_min=args.price_min,
                price_max=args.price_max,
                gap_min=args.gap_min,
                float_max=args.float_max,
                premkt_vol_min=args.premkt_vol_min,
                rvol_min=args.rvol_min,
                max_spread_pct=args.max_spread_pct,
            ):
                candidates.append(candidate)
        except Exception as exc:
            logger.exception("Error processing symbol %s: %s", symbol, exc)
            continue

    ranked = rank_candidates(candidates)
    return ranked


def write_csv(candidates: List[Candidate], scan_date: date, output_dir: str = "outputs") -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / f"candidates_{scan_date}.csv"

    if not candidates:
        # still create an empty CSV with headers
        fieldnames = [
            "symbol",
            "last_price",
            "prev_close",
            "open",
            "gap_pct",
            "float_shares",
            "premkt_volume",
            "avg10_volume",
            "rvol_proxy",
            "bid",
            "ask",
            "spread_pct",
            "rank_score",
            "reasons",
            "timestamp",
        ]
        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        return file_path

    fieldnames = list(candidates[0].to_csv_row().keys())
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in candidates:
            writer.writerow(c.to_csv_row())

    return file_path


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.mode == "backtest":
        if not args.start or not args.end:
            raise SystemExit("Backtest mode requires --start and --end (YYYY-MM-DD)")
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
        send_webhook = not args.no_webhook and bool(args.webhook_url and args.secret)
        from backtest import run_backtest, BacktestConfig

        config = BacktestConfig(
            start_date=start_date,
            end_date=end_date,
            price_min=args.price_min,
            price_max=args.price_max,
            gap_min=args.gap_min,
            float_max=args.float_max,
            daily_vol_min=args.daily_vol_min,
            rvol_min=args.rvol_min,
            top_n=args.top,
        )
        provider = PolygonProvider()
        run_backtest(
            config,
            provider,
            webhook_url=args.webhook_url or None,
            secret=args.secret or None,
            send_webhook=send_webhook,
        )
        return

    # Live mode
    if not args.date:
        raise SystemExit("Live mode requires --date (YYYY-MM-DD)")
    scan_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    candidates = run_scan(args)

    logger.info("Total candidates after filtering: %d", len(candidates))
    for c in candidates[:20]:
        logger.info(
            "Candidate %s price=%.2f gap=%.2f%% float=%s premkt_vol=%s rvol=%s score=%.3f",
            c.symbol,
            (c.open_price or c.last_price or 0.0),
            c.gap_pct or 0.0,
            c.float_shares,
            c.premkt_volume,
            c.rvol_proxy,
            c.rank_score or 0.0,
        )

    csv_path = write_csv(candidates[: args.top], scan_date)
    logger.info("Wrote CSV output to %s", csv_path)

    if not args.no_webhook and args.webhook_url and args.secret:
        send_candidate_list(
            args.webhook_url,
            secret=args.secret,
            candidates=candidates[: args.top],
        )
    elif not args.no_webhook:
        logger.info("Webhook URL or secret not provided; skipping webhook send.")


if __name__ == "__main__":
    main()

