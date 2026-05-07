from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional
from uuid import uuid4

from .config import SCHEMA_VERSION
from .time import utc_now_iso


@dataclass
class SequenceTracker:
    last_seen: dict[tuple[str, str], int] = field(default_factory=dict)

    def check(self, event: dict[str, Any]) -> Optional[dict[str, Any]]:
        key = (event["symbol"], event["exchange"])
        current = int(event["sequence_number"])
        previous = self.last_seen.get(key)
        self.last_seen[key] = max(current, previous or current)

        if previous is None or current == previous + 1:
            return None
        if current <= previous:
            return quality_alert(
                symbol=event["symbol"],
                exchange=event["exchange"],
                alert_type="duplicate_or_reordered_sequence",
                severity="warning",
                message=f"Received sequence {current} after {previous}.",
                observed_sequence=current,
                expected_sequence=previous + 1,
            )
        return quality_alert(
            symbol=event["symbol"],
            exchange=event["exchange"],
            alert_type="sequence_gap",
            severity="critical",
            message=f"Expected sequence {previous + 1} but received {current}.",
            observed_sequence=current,
            expected_sequence=previous + 1,
        )


def canonical_trade(raw: dict[str, Any], ingest_time: Optional[str] = None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "trade",
        "symbol": raw["symbol"].upper(),
        "exchange": raw["exchange"],
        "event_time": raw["event_time"],
        "ingest_time": ingest_time or utc_now_iso(),
        "sequence_number": int(raw["sequence_number"]),
        "price": float(raw["price"]),
        "size": int(raw["size"]),
        "trade_id": raw.get("trade_id") or str(uuid4()),
        "conditions": raw.get("conditions", []),
    }


def canonical_quote(raw: dict[str, Any], ingest_time: Optional[str] = None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "quote",
        "symbol": raw["symbol"].upper(),
        "exchange": raw["exchange"],
        "event_time": raw["event_time"],
        "ingest_time": ingest_time or utc_now_iso(),
        "sequence_number": int(raw["sequence_number"]),
        "bid_price": float(raw["bid_price"]),
        "bid_size": int(raw["bid_size"]),
        "ask_price": float(raw["ask_price"]),
        "ask_size": int(raw["ask_size"]),
    }


def quality_alert(
    *,
    symbol: str,
    exchange: str,
    alert_type: str,
    severity: str,
    message: str,
    observed_sequence: Optional[int] = None,
    expected_sequence: Optional[int] = None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "quality_alert",
        "symbol": symbol.upper(),
        "exchange": exchange,
        "event_time": utc_now_iso(),
        "ingest_time": utc_now_iso(),
        "sequence_number": observed_sequence or 0,
        "alert_type": alert_type,
        "severity": severity,
        "message": message,
        "observed_sequence": observed_sequence,
        "expected_sequence": expected_sequence,
    }
