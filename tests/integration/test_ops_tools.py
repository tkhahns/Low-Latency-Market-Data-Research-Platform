import json
from pathlib import Path

from market_platform.ops.audit import JsonlAuditLog
from market_platform.ops.documents import DocumentChunk
from market_platform.ops.tools import OpsTools
from market_platform.ops.vector_store import InMemoryVectorStore
from market_platform.redis_keys import alerts, bar_1s, freshness, latest_quote, metrics, top_of_book
from market_platform.time import utc_now_iso


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}
        self.closed = False

    async def get(self, key):
        return self.values.get(key)

    async def lrange(self, key, start, end):
        stop = None if end == -1 else end + 1
        return self.lists.get(key, [])[start:stop]

    async def aclose(self):
        self.closed = True


def build_store():
    store = InMemoryVectorStore()
    store.upsert_chunks(
        [
            DocumentChunk(
                source_uri="runbook:stale-symbol.md",
                title="Stale Symbol Runbook",
                content="AAPL stale freshness Redis key md:freshness and market.quality.alerts.v1 runbook.",
                tags=("runbook", "freshness"),
                source_type="runbook",
                chunk_index=0,
                indexed_at=utc_now_iso(),
            ),
            DocumentChunk(
                source_uri="lakehouse:tables.yml",
                title="Lakehouse Table Contracts",
                content="gold_bars_1s lineage from silver_trades with first_source_offset and last_source_offset.",
                tags=("lakehouse", "lineage"),
                source_type="lakehouse",
                chunk_index=0,
                indexed_at=utc_now_iso(),
            ),
            DocumentChunk(
                source_uri="incident:INC-001",
                title="INC-001",
                content="INC-001 sequence gap expected sequence 12 observed sequence 14 for AAPL.",
                tags=("incident",),
                source_type="incident",
                chunk_index=0,
                indexed_at=utc_now_iso(),
            ),
        ]
    )
    return store


def fake_redis_factory(redis):
    async def factory():
        return redis

    return factory


def seed_redis(redis):
    redis.values[freshness("AAPL")] = json.dumps({"symbol": "AAPL", "status": "fresh", "freshness_lag_ms": 12})
    redis.values[latest_quote("AAPL")] = json.dumps({"event_type": "quote", "symbol": "AAPL"})
    redis.values[top_of_book("AAPL")] = json.dumps({"event_type": "top_of_book", "symbol": "AAPL", "spread": 0.01})
    redis.values[bar_1s("AAPL")] = json.dumps({"event_type": "bar_1s", "symbol": "AAPL", "volume": 100})
    redis.values[metrics("AAPL")] = json.dumps({"event_type": "rolling_metrics", "symbol": "AAPL", "rolling_vwap": 100.1})
    redis.lists[alerts("AAPL")] = [json.dumps({"event_type": "quality_alert", "alert_type": "sequence_gap"})]


def test_ops_tools_return_structured_results_and_audit(tmp_path: Path):
    async def run():
        redis = FakeRedis()
        seed_redis(redis)
        audit_path = tmp_path / "audit.jsonl"
        tools = OpsTools(
            evidence_store=build_store(),
            redis_factory=fake_redis_factory(redis),
            audit_log=JsonlAuditLog(audit_path),
        )

        fresh = await tools.call("check_symbol_freshness", {"symbol": "aapl"}, caller="pytest")
        gap = await tools.call(
            "explain_sequence_gap",
            {"symbol": "AAPL", "exchange": "XNAS", "start_time": "2026-05-07T00:00:00Z", "end_time": "2026-05-07T00:01:00Z"},
            caller="pytest",
        )
        replay = await tools.call("run_replay_dry_run", {"start_time": "t0", "end_time": "t1", "symbols": ["AAPL"]})
        compare = await tools.call("compare_live_vs_replay", {"symbol": "AAPL", "as_of": "t1"})
        incident = await tools.call("summarize_incident", {"incident_id": "INC-001"})
        lineage = await tools.call("lineage_lookup", {"table": "gold_bars_1s", "symbol": "AAPL"})

        assert fresh["status"] == "ok"
        assert fresh["freshness"]["freshness_lag_ms"] == 12
        assert fresh["redis_keys"]["freshness"] == freshness("AAPL")
        assert gap["expected_tables"] == ["silver_sequence_gaps", "gold_quality_annotations"]
        assert replay["dry_run"] is True
        assert replay["mutated"] is False
        assert compare["live_state"]["top_of_book"]["spread"] == 0.01
        assert incident["sources"][0]["source_type"] in {"incident", "runbook"}
        assert lineage["upstream"] == ["silver_trades"]
        assert audit_path.read_text(encoding="utf-8").count("\n") == 6

    import asyncio

    asyncio.run(run())


def test_unknown_tool_and_table_errors_are_structured():
    async def run():
        tools = OpsTools(evidence_store=build_store())
        unknown_tool = await tools.call("delete_production_data", {})
        unknown_table = await tools.call("lineage_lookup", {"table": "missing_table"})

        assert unknown_tool["status"] == "error"
        assert unknown_tool["error_code"] == "unknown_tool"
        assert unknown_table["status"] == "error"
        assert unknown_table["error_code"] == "unknown_table"

    import asyncio

    asyncio.run(run())
