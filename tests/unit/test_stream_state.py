from market_platform.stream_state import StreamState


def test_quote_to_top_of_book_computes_spread_and_mid():
    state = StreamState()
    top, alerts = state.quote_to_top_of_book(
        {
            "schema_version": "1.0",
            "event_type": "quote",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-04T00:00:00Z",
            "ingest_time": "2026-05-04T00:00:00.001Z",
            "sequence_number": 1,
            "bid_price": 100.0,
            "bid_size": 100,
            "ask_price": 100.04,
            "ask_size": 200,
        }
    )

    assert top["event_type"] == "top_of_book"
    assert top["spread"] == 0.04
    assert top["mid_price"] == 100.02
    assert alerts == []


def test_trade_to_bar_rolls_one_second_vwap():
    state = StreamState()
    first = {
        "schema_version": "1.0",
        "event_type": "trade",
        "symbol": "AAPL",
        "exchange": "XNAS",
        "event_time": "2026-05-04T00:00:00.100Z",
        "ingest_time": "2026-05-04T00:00:00.101Z",
        "sequence_number": 1,
        "price": 100.0,
        "size": 10,
        "trade_id": "t1",
        "conditions": [],
    }
    second = {**first, "sequence_number": 2, "price": 102.0, "size": 30, "trade_id": "t2"}

    state.trade_to_bar(first)
    bar = state.trade_to_bar(second)

    assert bar["event_type"] == "bar_1s"
    assert bar["open"] == 100.0
    assert bar["high"] == 102.0
    assert bar["low"] == 100.0
    assert bar["close"] == 102.0
    assert bar["volume"] == 40
    assert bar["vwap"] == 101.5


def test_trade_to_metrics_tracks_volume_vwap_and_volatility():
    state = StreamState()
    base = {
        "schema_version": "1.0",
        "event_type": "trade",
        "symbol": "AAPL",
        "exchange": "XNAS",
        "event_time": "2026-05-04T00:00:00.100Z",
        "ingest_time": "2026-05-04T00:00:00.101Z",
        "sequence_number": 1,
        "price": 100.0,
        "size": 10,
        "trade_id": "t1",
        "conditions": [],
    }
    state.trade_to_metrics(base)
    state.trade_to_metrics({**base, "sequence_number": 2, "price": 102.0, "size": 30, "trade_id": "t2"})
    metrics = state.trade_to_metrics({**base, "sequence_number": 3, "price": 101.0, "size": 20, "trade_id": "t3"})

    assert metrics["event_type"] == "rolling_metrics"
    assert metrics["sample_count"] == 3
    assert metrics["rolling_volume"] == 60
    assert metrics["rolling_vwap"] == 101.33333333333333
    assert metrics["volatility_bps"] > 0
