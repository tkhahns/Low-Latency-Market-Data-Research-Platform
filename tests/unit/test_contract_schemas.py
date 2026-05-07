import json
from pathlib import Path

from jsonschema import validate

from market_platform.events import canonical_quote, canonical_trade, quality_alert
from market_platform.stream_state import StreamState


CONTRACT_DIR = Path(__file__).resolve().parents[2] / "contracts" / "events"


def load_schema(name: str) -> dict:
    return json.loads((CONTRACT_DIR / name).read_text())


def test_trade_and_quote_schemas_accept_canonical_events():
    trade = canonical_trade(
        {
            "event_type": "trade",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-04T00:00:00Z",
            "sequence_number": 1,
            "price": 100.0,
            "size": 10,
        },
        ingest_time="2026-05-04T00:00:00.001Z",
    )
    quote = canonical_quote(
        {
            "event_type": "quote",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-04T00:00:00Z",
            "sequence_number": 2,
            "bid_price": 100.0,
            "bid_size": 10,
            "ask_price": 100.01,
            "ask_size": 20,
        },
        ingest_time="2026-05-04T00:00:00.001Z",
    )

    validate(trade, load_schema("trade-event.v1.schema.json"))
    validate(quote, load_schema("quote-event.v1.schema.json"))


def test_derived_event_schemas_accept_poc_outputs():
    state = StreamState()
    quote = canonical_quote(
        {
            "event_type": "quote",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-04T00:00:00Z",
            "sequence_number": 2,
            "bid_price": 100.0,
            "bid_size": 10,
            "ask_price": 100.01,
            "ask_size": 20,
        },
        ingest_time="2026-05-04T00:00:00.001Z",
    )
    trade = canonical_trade(
        {
            "event_type": "trade",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-04T00:00:00Z",
            "sequence_number": 3,
            "price": 100.0,
            "size": 10,
        },
        ingest_time="2026-05-04T00:00:00.001Z",
    )

    top, _ = state.quote_to_top_of_book(quote)
    bar = state.trade_to_bar(trade)
    alert = quality_alert(
        symbol="AAPL",
        exchange="XNAS",
        alert_type="sequence_gap",
        severity="critical",
        message="Expected sequence 2 but received 3.",
        observed_sequence=3,
        expected_sequence=2,
    )

    validate(top, load_schema("top-of-book-event.v1.schema.json"))
    validate(bar, load_schema("bar-1s-event.v1.schema.json"))
    validate(alert, load_schema("quality-alert-event.v1.schema.json"))

