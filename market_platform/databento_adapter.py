from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

NANOSECONDS_PER_SECOND = 1_000_000_000


def ns_to_iso(value: int) -> str:
    seconds, nanos = divmod(int(value), NANOSECONDS_PER_SECOND)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(microsecond=nanos // 1000)
    return dt.isoformat().replace("+00:00", "Z")


def get_attr(record: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(record, name)
    except Exception:  # noqa: BLE001 - third-party DBN record properties can raise on unset fields.
        return default


def enum_value(value: Any) -> str:
    if value is None:
        return ""
    return str(getattr(value, "value", value))


def resolve_symbol(record: Any, symbol_map: dict[int, str]) -> str:
    instrument_id = get_attr(record, "instrument_id")
    if instrument_id is not None and int(instrument_id) in symbol_map:
        return symbol_map[int(instrument_id)]
    for name in ("symbol", "raw_symbol", "stype_in_symbol", "stype_out_symbol"):
        value = get_attr(record, name)
        if value:
            return str(value)
    return f"INSTRUMENT_{instrument_id}" if instrument_id is not None else "UNKNOWN"


def update_symbol_map(record: Any, symbol_map: dict[int, str]) -> bool:
    instrument_id = get_attr(record, "instrument_id")
    stype_in_symbol = get_attr(record, "stype_in_symbol")
    stype_out_symbol = get_attr(record, "stype_out_symbol")
    if instrument_id is None or not (stype_in_symbol or stype_out_symbol):
        return False
    symbol = stype_in_symbol if stype_in_symbol and str(stype_out_symbol).isdigit() else stype_out_symbol
    symbol_map[int(instrument_id)] = str(symbol)
    return True


def trade_from_record(record: Any, symbol_map: dict[int, str], exchange: str) -> Optional[dict[str, Any]]:
    price = get_attr(record, "pretty_price")
    size = get_attr(record, "size")
    ts_event = get_attr(record, "ts_event")
    if price is None or size is None or ts_event is None:
        return None
    return {
        "event_type": "trade",
        "symbol": resolve_symbol(record, symbol_map),
        "exchange": exchange,
        "event_time": ns_to_iso(ts_event),
        "sequence_number": int(get_attr(record, "sequence", 0) or 0),
        "price": float(price),
        "size": int(size),
        "trade_id": str(uuid4()),
        "conditions": [enum_value(get_attr(record, "side")), f"action={enum_value(get_attr(record, 'action'))}"],
    }


def quote_from_record(record: Any, symbol_map: dict[int, str], exchange: str) -> Optional[dict[str, Any]]:
    levels = get_attr(record, "levels", None)
    if not levels:
        return None
    level = levels[0]
    bid_price = get_attr(level, "pretty_bid_px")
    ask_price = get_attr(level, "pretty_ask_px")
    bid_size = get_attr(level, "bid_sz")
    ask_size = get_attr(level, "ask_sz")
    ts_event = get_attr(record, "ts_event")
    if bid_price is None or ask_price is None or bid_size is None or ask_size is None or ts_event is None:
        return None
    if float(bid_price) <= 0 or float(ask_price) <= 0:
        return None
    return {
        "event_type": "quote",
        "symbol": resolve_symbol(record, symbol_map),
        "exchange": exchange,
        "event_time": ns_to_iso(ts_event),
        "sequence_number": int(get_attr(record, "sequence", 0) or 0),
        "bid_price": float(bid_price),
        "bid_size": int(bid_size),
        "ask_price": float(ask_price),
        "ask_size": int(ask_size),
    }


def raw_events_from_databento(record: Any, symbol_map: dict[int, str], exchange: str) -> list[dict[str, Any]]:
    if update_symbol_map(record, symbol_map):
        return []

    class_name = record.__class__.__name__.lower()
    events: list[dict[str, Any]] = []
    if "mbp1" in class_name or "bbo" in class_name:
        quote = quote_from_record(record, symbol_map, exchange)
        if quote:
            events.append(quote)

    action = enum_value(get_attr(record, "action"))
    if "trademsg" in class_name or action == "T":
        trade = trade_from_record(record, symbol_map, exchange)
        if trade:
            events.append(trade)

    return events
