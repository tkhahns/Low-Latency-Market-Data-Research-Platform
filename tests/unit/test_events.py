from market_platform.events import SequenceTracker, canonical_quote, canonical_trade


def test_canonical_trade_adds_required_contract_fields():
    event = canonical_trade(
        {
            "event_type": "trade",
            "symbol": "aapl",
            "exchange": "XNAS",
            "event_time": "2026-05-04T00:00:00Z",
            "sequence_number": 10,
            "price": 180.25,
            "size": 100,
        },
        ingest_time="2026-05-04T00:00:01Z",
    )

    assert event["schema_version"] == "1.0"
    assert event["event_type"] == "trade"
    assert event["symbol"] == "AAPL"
    assert event["ingest_time"] == "2026-05-04T00:00:01Z"
    assert event["trade_id"]


def test_sequence_tracker_detects_gap():
    tracker = SequenceTracker()
    first = canonical_quote(
        {
            "event_type": "quote",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-04T00:00:00Z",
            "sequence_number": 1,
            "bid_price": 100,
            "bid_size": 10,
            "ask_price": 100.01,
            "ask_size": 20,
        }
    )
    gap = {**first, "sequence_number": 3}

    assert tracker.check(first) is None
    alert = tracker.check(gap)

    assert alert is not None
    assert alert["alert_type"] == "sequence_gap"
    assert alert["expected_sequence"] == 2
    assert alert["observed_sequence"] == 3

