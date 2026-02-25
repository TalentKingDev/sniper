from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Any
from uuid import uuid4

import requests

from models import Candidate

logger = logging.getLogger(__name__)


def _candidate_to_dict(candidate: Candidate) -> Dict[str, Any]:
    return {
        "symbol": candidate.symbol,
        "price": candidate.last_price,
        "gap_pct": candidate.gap_pct,
        "float": candidate.float_shares,
        "premkt_volume": candidate.premkt_volume,
        "rvol": candidate.rvol_proxy,
        "score": candidate.rank_score,
    }


def _base_payload(
    action: str,
    secret: str,
    tag: str,
    version: str,
) -> Dict[str, Any]:
    return {
        "version": version,
        "event_id": str(uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tag": tag,
        "action": action,
        "secret_key": secret,
    }


def send_candidate_list(
    webhook_url: str,
    secret: str,
    candidates: Iterable[Candidate],
    *,
    tag: str = "v8.0_Susan",
    version: str = "8.0",
    fallback_single_events: bool = False,
    max_per_event: int = 50,
    timeout: int = 10,
) -> None:
    candidate_list: List[Candidate] = list(candidates)
    if not candidate_list:
        logger.info("No candidates to send to webhook.")
        return

    if not fallback_single_events:
        payload = _base_payload("CANDIDATE_LIST", secret=secret, tag=tag, version=version)
        payload["data"] = {
            "candidate_count": len(candidate_list),
            "candidates": [
                _candidate_to_dict(c) for c in candidate_list[:max_per_event]
            ],
        }
        _post_json(webhook_url, payload, timeout=timeout)
        return

    # Fallback: send each candidate as a separate CANDIDATE event
    for candidate in candidate_list[:max_per_event]:
        payload = _base_payload("CANDIDATE", secret=secret, tag=tag, version=version)
        payload["symbol"] = candidate.symbol
        payload["data"] = _candidate_to_dict(candidate)
        _post_json(webhook_url, payload, timeout=timeout)


def _post_json(url: str, payload: Dict[str, Any], timeout: int = 10) -> None:
    try:
        response = requests.post(
            url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        if not response.ok:
            logger.error(
                "Webhook POST failed: status=%s body=%s",
                response.status_code,
                response.text[:500],
            )
    except Exception as exc:  # pragma: no cover - network failures are environment-specific
        logger.exception("Error sending webhook payload: %s", exc)

