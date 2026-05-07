from market_platform.events import canonical_quote, canonical_trade, quality_alert
from market_platform.lakehouse.transforms import (
    gold_bars_1s,
    gold_quality_annotations,
    gold_spread_features,
    gold_volatility_features,
    silver_quotes,
    silver_sequence_gaps,
    silver_trades,
    to_bronze_row,
)


def bronze(event, offset):
    return to_bronze_row(
        event,
        source_topic=f"market.{event['event_type']}s.v1",
        source_partition=0,
        source_offset=offset,
        job_run_id="test-run",
        processed_at="2026-05-07T00:00:01Z",
    )


def test_bronze_silver_gold_research_tables_preserve_lineage():
    quote = canonical_quote(
        {
            "event_type": "quote",
            "symbol": "aapl",
            "exchange": "XNAS",
            "event_time": "2026-05-07T00:00:00.100Z",
            "sequence_number": 1,
            "bid_price": 100.0,
            "bid_size": 100,
            "ask_price": 100.04,
            "ask_size": 200,
        },
        ingest_time="2026-05-07T00:00:00.101Z",
    )
    trades = [
        canonical_trade(
            {
                "event_type": "trade",
                "symbol": "AAPL",
                "exchange": "XNAS",
                "event_time": "2026-05-07T00:00:00.200Z",
                "sequence_number": 2,
                "price": 100.0,
                "size": 10,
                "trade_id": "t1",
            },
            ingest_time="2026-05-07T00:00:00.201Z",
        ),
        canonical_trade(
            {
                "event_type": "trade",
                "symbol": "AAPL",
                "exchange": "XNAS",
                "event_time": "2026-05-07T00:00:00.300Z",
                "sequence_number": 3,
                "price": 102.0,
                "size": 30,
                "trade_id": "t2",
            },
            ingest_time="2026-05-07T00:00:00.301Z",
        ),
    ]
    rows = [bronze(quote, 1), bronze(trades[0], 2), bronze(trades[1], 3)]

    trades_silver = silver_trades(rows)
    quotes_silver = silver_quotes(rows)
    bars = gold_bars_1s(trades_silver)
    spreads = gold_spread_features(quotes_silver)
    volatility = gold_volatility_features(bars)

    assert trades_silver[0]["source_offset"] == 2
    assert quotes_silver[0]["spread"] == 0.04
    assert bars[0]["open"] == 100.0
    assert bars[0]["high"] == 102.0
    assert bars[0]["volume"] == 40
    assert bars[0]["vwap"] == 101.5
    assert bars[0]["first_source_offset"] == 2
    assert spreads[0]["avg_spread"] == 0.04
    assert volatility[0]["bar_count"] == 1
    assert volatility[0]["volatility_bps"] == 0.0


def test_deduplication_sequence_gaps_and_quality_annotations():
    first = canonical_trade(
        {
            "event_type": "trade",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-07T00:00:00Z",
            "sequence_number": 1,
            "price": 100.0,
            "size": 10,
            "trade_id": "t1",
        },
        ingest_time="2026-05-07T00:00:00.001Z",
    )
    duplicate_later_offset = {**first, "price": 100.5}
    gap_trade = canonical_trade(
        {
            "event_type": "trade",
            "symbol": "AAPL",
            "exchange": "XNAS",
            "event_time": "2026-05-07T00:00:01Z",
            "sequence_number": 4,
            "price": 101.0,
            "size": 5,
            "trade_id": "t4",
        },
        ingest_time="2026-05-07T00:00:01.001Z",
    )
    alert = quality_alert(
        symbol="AAPL",
        exchange="XNAS",
        alert_type="wide_spread",
        severity="warning",
        message="Spread exceeded configured threshold.",
        observed_sequence=4,
    )
    rows = [bronze(first, 1), bronze(duplicate_later_offset, 2), bronze(gap_trade, 3), bronze(alert, 4)]

    trades = silver_trades(rows)
    gaps = silver_sequence_gaps(rows)
    annotations = gold_quality_annotations(rows, gaps)

    assert len(trades) == 2
    assert trades[0]["price"] == 100.5
    sequence_gap = next(row for row in gaps if row["alert_type"] == "sequence_gap")
    assert sequence_gap["expected_sequence"] == 2
    assert sequence_gap["observed_sequence"] == 4
    assert {"duplicate_or_reordered_sequence", "sequence_gap", "wide_spread"} <= {
        row["annotation_type"] for row in annotations
    }
