from __future__ import annotations

from typing import Any, Optional


def trade_from_match(msg: dict[str, Any], exchange: str = "COINBASE") -> Optional[dict[str, Any]]:
    try:
        trade_id = int(msg["trade_id"])
        price = float(msg["price"])
        size = float(msg["size"])
        event_time = msg["time"]
        product_id = msg["product_id"]
        side = msg.get("side", "")
    except (KeyError, ValueError, TypeError):
        return None
    return {
        "event_type": "trade",
        "symbol": product_id,
        "exchange": exchange,
        "event_time": event_time,
        "sequence_number": trade_id,
        "price": price,
        "size": size,
        "trade_id": str(trade_id),
        "conditions": [side] if side else [],
    }


def quote_from_ticker(msg: dict[str, Any], quote_counters: dict[str, int], exchange: str = "COINBASE") -> Optional[dict[str, Any]]:
    product_id = msg.get("product_id")
    best_bid = msg.get("best_bid")
    best_bid_size = msg.get("best_bid_size")
    best_ask = msg.get("best_ask")
    best_ask_size = msg.get("best_ask_size")
    event_time = msg.get("time")
    if not all([product_id, best_bid, best_bid_size, best_ask, best_ask_size, event_time]):
        return None
    try:
        bid_price = float(best_bid)
        bid_size = float(best_bid_size)
        ask_price = float(best_ask)
        ask_size = float(best_ask_size)
    except (ValueError, TypeError):
        return None
    if bid_price <= 0 or ask_price <= 0:
        return None
    counter = quote_counters.get(product_id, 0) + 1
    quote_counters[product_id] = counter
    return {
        "event_type": "quote",
        "symbol": product_id,
        "exchange": exchange,
        "event_time": event_time,
        "sequence_number": counter,
        "bid_price": bid_price,
        "bid_size": bid_size,
        "ask_price": ask_price,
        "ask_size": ask_size,
    }


def raw_events_from_coinbase(msg: dict[str, Any], quote_counters: dict[str, int], exchange: str = "COINBASE") -> list[dict[str, Any]]:
    msg_type = msg.get("type")
    if msg_type == "match":
        event = trade_from_match(msg, exchange)
        return [event] if event else []
    if msg_type == "ticker":
        event = quote_from_ticker(msg, quote_counters, exchange)
        return [event] if event else []
    return []
