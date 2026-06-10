from __future__ import annotations

from market_platform.coinbase_adapter import quote_from_ticker, raw_events_from_coinbase, trade_from_match

MATCH = {
    "type": "match",
    "trade_id": 12345,
    "sequence": 54321,
    "time": "2026-06-10T12:00:00.050000Z",
    "product_id": "BTC-USD",
    "size": "0.01234567",
    "price": "50000.10",
    "side": "buy",
}

TICKER = {
    "type": "ticker",
    "sequence": 54322,
    "product_id": "BTC-USD",
    "price": "50000.10",
    "best_bid": "50000.00",
    "best_bid_size": "0.50",
    "best_ask": "50000.20",
    "best_ask_size": "0.30",
    "time": "2026-06-10T12:00:00.100000Z",
}

TICKER_PARTIAL = {
    "type": "ticker",
    "sequence": 54323,
    "product_id": "BTC-USD",
    "price": "50000.10",
    "time": "2026-06-10T12:00:00.150000Z",
}


def test_trade_from_match_maps_fields():
    event = trade_from_match(MATCH)
    assert event is not None
    assert event["event_type"] == "trade"
    assert event["symbol"] == "BTC-USD"
    assert event["exchange"] == "COINBASE"
    assert event["price"] == 50000.10
    assert event["size"] == pytest_approx(0.01234567)
    assert event["sequence_number"] == 12345
    assert event["trade_id"] == "12345"
    assert event["conditions"] == ["buy"]


def test_trade_from_match_fractional_size():
    msg = {**MATCH, "size": "0.00000001"}
    event = trade_from_match(msg)
    assert event is not None
    assert event["size"] == pytest_approx(1e-8)


def test_quote_from_ticker_maps_fields():
    counters: dict[str, int] = {}
    event = quote_from_ticker(TICKER, counters)
    assert event is not None
    assert event["event_type"] == "quote"
    assert event["symbol"] == "BTC-USD"
    assert event["bid_price"] == 50000.0
    assert event["bid_size"] == 0.5
    assert event["ask_price"] == 50000.20
    assert event["ask_size"] == 0.30
    assert event["sequence_number"] == 1


def test_quote_from_ticker_counter_increments_per_product():
    counters: dict[str, int] = {}
    q1 = quote_from_ticker(TICKER, counters)
    q2 = quote_from_ticker(TICKER, counters)
    assert q1 is not None
    assert q2 is not None
    assert q2["sequence_number"] == q1["sequence_number"] + 1


def test_quote_from_ticker_partial_skipped():
    counters: dict[str, int] = {}
    event = quote_from_ticker(TICKER_PARTIAL, counters)
    assert event is None
    assert counters == {}


def test_raw_events_from_coinbase_dispatches_match():
    counters: dict[str, int] = {}
    events = raw_events_from_coinbase(MATCH, counters)
    assert len(events) == 1
    assert events[0]["event_type"] == "trade"


def test_raw_events_from_coinbase_dispatches_ticker():
    counters: dict[str, int] = {}
    events = raw_events_from_coinbase(TICKER, counters)
    assert len(events) == 1
    assert events[0]["event_type"] == "quote"


def test_raw_events_from_coinbase_ignores_unknown_type():
    counters: dict[str, int] = {}
    events = raw_events_from_coinbase({"type": "subscriptions", "channels": []}, counters)
    assert events == []


def test_raw_events_from_coinbase_ignores_heartbeat():
    counters: dict[str, int] = {}
    hb = {"type": "heartbeat", "sequence": 1, "last_trade_id": 100, "product_id": "BTC-USD", "time": "2026-06-10T12:00:00Z"}
    events = raw_events_from_coinbase(hb, counters)
    assert events == []


def pytest_approx(value):
    """Thin shim so the file imports cleanly; real tests use pytest.approx."""
    import pytest
    return pytest.approx(value, rel=1e-6)
