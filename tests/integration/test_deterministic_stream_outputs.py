from market_platform.events import canonical_quote, canonical_trade
from market_platform.stream_state import StreamState


def test_fixed_events_produce_deterministic_market_state():
    state = StreamState()
    quote = canonical_quote(
        {
            "event_type": "quote",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-07T00:00:00Z",
            "sequence_number": 1,
            "bid_price": 100.00,
            "bid_size": 100,
            "ask_price": 100.04,
            "ask_size": 200,
        },
        ingest_time="2026-05-07T00:00:00.001Z",
    )
    trades = [
        canonical_trade(
            {
                "event_type": "trade",
                "symbol": "AAPL",
                "exchange": "XNAS",
                "event_time": "2026-05-07T00:00:00.100Z",
                "sequence_number": 2,
                "price": 100.00,
                "size": 10,
                "trade_id": "t1",
            },
            ingest_time="2026-05-07T00:00:00.101Z",
        ),
        canonical_trade(
            {
                "event_type": "trade",
                "symbol": "AAPL",
                "exchange": "XNAS",
                "event_time": "2026-05-07T00:00:00.200Z",
                "sequence_number": 3,
                "price": 102.00,
                "size": 30,
                "trade_id": "t2",
            },
            ingest_time="2026-05-07T00:00:00.201Z",
        ),
        canonical_trade(
            {
                "event_type": "trade",
                "symbol": "AAPL",
                "exchange": "XNAS",
                "event_time": "2026-05-07T00:00:00.300Z",
                "sequence_number": 4,
                "price": 101.00,
                "size": 20,
                "trade_id": "t3",
            },
            ingest_time="2026-05-07T00:00:00.301Z",
        ),
    ]

    top, alerts = state.quote_to_top_of_book(quote)
    bars = [state.trade_to_bar(trade) for trade in trades]
    metrics = [state.trade_to_metrics(trade) for trade in trades]

    assert alerts == []
    assert top["spread"] == 0.04
    assert top["mid_price"] == 100.02
    assert bars[-1]["open"] == 100.0
    assert bars[-1]["high"] == 102.0
    assert bars[-1]["low"] == 100.0
    assert bars[-1]["close"] == 101.0
    assert bars[-1]["volume"] == 60
    assert bars[-1]["vwap"] == 101.33333333333333
    assert metrics[-1]["rolling_volume"] == 60
    assert metrics[-1]["rolling_vwap"] == 101.33333333333333
    assert metrics[-1]["sample_count"] == 3
    assert metrics[-1]["volatility_bps"] > 0
