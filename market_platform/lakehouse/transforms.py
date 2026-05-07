from __future__ import annotations

import json
from collections import defaultdict
from datetime import timezone
from math import log, sqrt
from typing import Any, Iterable

from market_platform.time import parse_utc, utc_now_iso


def second_bucket(value: str) -> str:
    return parse_utc(value).astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def event_date(value: str) -> str:
    return parse_utc(value).astimezone(timezone.utc).date().isoformat()


def to_bronze_row(
    event: dict[str, Any],
    *,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    job_run_id: str,
    processed_at: str | None = None,
) -> dict[str, Any]:
    processed = processed_at or utc_now_iso()
    return {
        "schema_version": event["schema_version"],
        "event_type": event["event_type"],
        "symbol": event["symbol"],
        "exchange": event["exchange"],
        "event_time": event["event_time"],
        "ingest_time": event["ingest_time"],
        "sequence_number": int(event["sequence_number"]),
        "event_date": event_date(event["event_time"]),
        "source_topic": source_topic,
        "source_partition": int(source_partition),
        "source_offset": int(source_offset),
        "job_run_id": job_run_id,
        "processed_at": processed,
        "raw_event": json.dumps(event, sort_keys=True, separators=(",", ":")),
    }


def decode_raw_event(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw_event")
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    return {key: value for key, value in row.items() if key not in {"raw_event"}}


def _dedupe_latest(rows: Iterable[dict[str, Any]], key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    latest: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row[field] for field in key_fields)
        previous = latest.get(key)
        if previous is None or int(row["source_offset"]) >= int(previous["source_offset"]):
            latest[key] = row
    return sorted(latest.values(), key=lambda row: (row["symbol"], row["exchange"], row["sequence_number"]))


def silver_trades(bronze_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for bronze in bronze_rows:
        if bronze["event_type"] != "trade":
            continue
        event = decode_raw_event(bronze)
        rows.append(
            {
                **_lineage(bronze),
                "trade_id": event.get("trade_id", ""),
                "price": float(event["price"]),
                "size": int(event["size"]),
                "conditions": list(event.get("conditions", [])),
            }
        )
    return _dedupe_latest(rows, ("symbol", "exchange", "sequence_number", "trade_id"))


def silver_quotes(bronze_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for bronze in bronze_rows:
        if bronze["event_type"] != "quote":
            continue
        event = decode_raw_event(bronze)
        rows.append(
            {
                **_lineage(bronze),
                "bid_price": float(event["bid_price"]),
                "bid_size": int(event["bid_size"]),
                "ask_price": float(event["ask_price"]),
                "ask_size": int(event["ask_size"]),
                "spread": round(float(event["ask_price"]) - float(event["bid_price"]), 6),
                "mid_price": round((float(event["ask_price"]) + float(event["bid_price"])) / 2, 6),
            }
        )
    return _dedupe_latest(rows, ("symbol", "exchange", "sequence_number"))


def silver_sequence_gaps(bronze_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in bronze_rows:
        if row["event_type"] in {"trade", "quote"}:
            grouped[(row["symbol"], row["exchange"])].append(row)

    gaps = []
    for (symbol, exchange), rows in grouped.items():
        ordered = sorted(rows, key=lambda row: (int(row["sequence_number"]), int(row["source_offset"])))
        previous = None
        for row in ordered:
            current = int(row["sequence_number"])
            if previous is not None and current != previous + 1:
                anomaly = "duplicate_or_reordered_sequence" if current <= previous else "sequence_gap"
                gaps.append(
                    {
                        "schema_version": row["schema_version"],
                        "symbol": symbol,
                        "exchange": exchange,
                        "event_time": row["event_time"],
                        "event_date": row["event_date"],
                        "alert_type": anomaly,
                        "expected_sequence": previous + 1,
                        "observed_sequence": current,
                        "source_topic": row["source_topic"],
                        "source_partition": row["source_partition"],
                        "source_offset": row["source_offset"],
                        "job_run_id": row["job_run_id"],
                        "processed_at": row["processed_at"],
                    }
                )
            previous = max(current, previous or current)
    return gaps


def gold_bars_1s(trades: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        groups[(trade["symbol"], trade["exchange"], second_bucket(trade["event_time"]))].append(trade)

    bars = []
    for (symbol, exchange, window_start), group in groups.items():
        ordered = sorted(group, key=lambda row: (row["event_time"], row["sequence_number"]))
        volume = sum(int(row["size"]) for row in ordered)
        notional = sum(float(row["price"]) * int(row["size"]) for row in ordered)
        prices = [float(row["price"]) for row in ordered]
        bars.append(
            {
                "schema_version": "1.0",
                "symbol": symbol,
                "exchange": exchange,
                "window_start": window_start,
                "event_date": event_date(window_start),
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "volume": volume,
                "vwap": notional / volume if volume else prices[-1],
                "trade_count": len(ordered),
                "first_sequence": min(int(row["sequence_number"]) for row in ordered),
                "last_sequence": max(int(row["sequence_number"]) for row in ordered),
                "first_source_offset": min(int(row["source_offset"]) for row in ordered),
                "last_source_offset": max(int(row["source_offset"]) for row in ordered),
                "job_run_id": ordered[-1]["job_run_id"],
                "processed_at": ordered[-1]["processed_at"],
            }
        )
    return sorted(bars, key=lambda row: (row["symbol"], row["exchange"], row["window_start"]))


def gold_spread_features(quotes: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for quote in quotes:
        groups[(quote["symbol"], quote["exchange"], second_bucket(quote["event_time"]))].append(quote)

    features = []
    for (symbol, exchange, window_start), group in groups.items():
        spreads = [float(row["spread"]) for row in group]
        mids = [float(row["mid_price"]) for row in group]
        features.append(
            {
                "schema_version": "1.0",
                "symbol": symbol,
                "exchange": exchange,
                "window_start": window_start,
                "event_date": event_date(window_start),
                "quote_count": len(group),
                "min_spread": min(spreads),
                "max_spread": max(spreads),
                "avg_spread": sum(spreads) / len(spreads),
                "avg_mid_price": sum(mids) / len(mids),
                "first_sequence": min(int(row["sequence_number"]) for row in group),
                "last_sequence": max(int(row["sequence_number"]) for row in group),
                "first_source_offset": min(int(row["source_offset"]) for row in group),
                "last_source_offset": max(int(row["source_offset"]) for row in group),
                "job_run_id": group[-1]["job_run_id"],
                "processed_at": group[-1]["processed_at"],
            }
        )
    return sorted(features, key=lambda row: (row["symbol"], row["exchange"], row["window_start"]))


def gold_volatility_features(bars: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for bar in bars:
        grouped[(bar["symbol"], bar["exchange"], event_date(bar["window_start"]))].append(bar)

    features = []
    for (symbol, exchange, feature_date), group in grouped.items():
        ordered = sorted(group, key=lambda row: row["window_start"])
        returns = []
        for index in range(1, len(ordered)):
            previous = float(ordered[index - 1]["close"])
            current = float(ordered[index]["close"])
            if previous > 0 and current > 0:
                returns.append(log(current / previous))
        if len(returns) > 1:
            mean = sum(returns) / len(returns)
            variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
            volatility_bps = sqrt(variance) * 10000
        else:
            volatility_bps = 0.0
        features.append(
            {
                "schema_version": "1.0",
                "symbol": symbol,
                "exchange": exchange,
                "event_date": feature_date,
                "bar_count": len(ordered),
                "return_count": len(returns),
                "volatility_bps": round(volatility_bps, 6),
                "first_window_start": ordered[0]["window_start"],
                "last_window_start": ordered[-1]["window_start"],
                "job_run_id": ordered[-1]["job_run_id"],
                "processed_at": ordered[-1]["processed_at"],
            }
        )
    return sorted(features, key=lambda row: (row["symbol"], row["exchange"], row["event_date"]))


def gold_quality_annotations(bronze_rows: Iterable[dict[str, Any]], sequence_gaps: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    annotations = []
    for gap in sequence_gaps:
        annotations.append(
            {
                "schema_version": gap["schema_version"],
                "symbol": gap["symbol"],
                "exchange": gap["exchange"],
                "event_time": gap["event_time"],
                "event_date": gap["event_date"],
                "annotation_type": gap["alert_type"],
                "severity": "critical" if gap["alert_type"] == "sequence_gap" else "warning",
                "message": f"Expected sequence {gap['expected_sequence']} but observed {gap['observed_sequence']}.",
                "source_table": "silver_sequence_gaps",
                "source_topic": gap["source_topic"],
                "source_partition": gap["source_partition"],
                "source_offset": gap["source_offset"],
                "job_run_id": gap["job_run_id"],
                "processed_at": gap["processed_at"],
            }
        )
    for bronze in bronze_rows:
        if bronze["event_type"] != "quality_alert":
            continue
        event = decode_raw_event(bronze)
        annotations.append(
            {
                "schema_version": bronze["schema_version"],
                "symbol": bronze["symbol"],
                "exchange": bronze["exchange"],
                "event_time": bronze["event_time"],
                "event_date": bronze["event_date"],
                "annotation_type": event["alert_type"],
                "severity": event["severity"],
                "message": event["message"],
                "source_table": "bronze_market_events",
                "source_topic": bronze["source_topic"],
                "source_partition": bronze["source_partition"],
                "source_offset": bronze["source_offset"],
                "job_run_id": bronze["job_run_id"],
                "processed_at": bronze["processed_at"],
            }
        )
    return sorted(annotations, key=lambda row: (row["symbol"], row["event_time"], row["annotation_type"]))


def _lineage(bronze: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": bronze["schema_version"],
        "symbol": bronze["symbol"],
        "exchange": bronze["exchange"],
        "event_time": bronze["event_time"],
        "ingest_time": bronze["ingest_time"],
        "sequence_number": int(bronze["sequence_number"]),
        "event_date": bronze["event_date"],
        "source_topic": bronze["source_topic"],
        "source_partition": int(bronze["source_partition"]),
        "source_offset": int(bronze["source_offset"]),
        "job_run_id": bronze["job_run_id"],
        "processed_at": bronze["processed_at"],
    }
