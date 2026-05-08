from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

import yaml

from market_platform.ops.audit import JsonlAuditLog, audit_record
from market_platform.ops.vector_store import EvidenceStore, SearchResult
from market_platform.redis_keys import alerts, bar_1s, freshness, latest_quote, metrics, top_of_book


AsyncRedisFactory = Callable[[], Awaitable[Any]]


def source_dict(result: SearchResult) -> dict[str, Any]:
    return {
        "source_uri": result.source.source_uri,
        "title": result.source.title,
        "source_type": result.source.source_type,
        "chunk_index": result.source.chunk_index,
        "score": result.source.score,
    }


class OpsTools:
    def __init__(
        self,
        *,
        evidence_store: EvidenceStore,
        redis_factory: AsyncRedisFactory | None = None,
        audit_log: JsonlAuditLog | None = None,
        table_contract_path: Path = Path("lakehouse/contracts/tables.yml"),
    ) -> None:
        self.evidence_store = evidence_store
        self.redis_factory = redis_factory
        self.audit_log = audit_log
        self.table_contract_path = table_contract_path

    async def call(self, tool_name: str, parameters: dict[str, Any], caller: str = "local") -> dict[str, Any]:
        start = time.perf_counter()
        try:
            handler = getattr(self, tool_name)
        except AttributeError:
            result = self.error("unknown_tool", f"Unknown tool {tool_name!r}.", [])
        else:
            try:
                result = await handler(parameters)
            except Exception as exc:  # noqa: BLE001 - tool errors must be structured.
                result = self.error("tool_execution_error", str(exc), [])
        duration_ms = int((time.perf_counter() - start) * 1000)
        result["duration_ms"] = duration_ms
        if self.audit_log:
            self.audit_log.write(
                audit_record(
                    tool_name=tool_name,
                    caller=caller,
                    parameters=parameters,
                    result_status=result["status"],
                    duration_ms=duration_ms,
                )
            )
        return result

    async def check_symbol_freshness(self, parameters: dict[str, Any]) -> dict[str, Any]:
        symbol = required(parameters, "symbol").upper()
        if self.redis_factory is None:
            return self.error("redis_unavailable", "Redis factory is not configured.", self.search_sources(f"{symbol} freshness"))

        redis = await self.redis_factory()
        try:
            fresh = await get_json(redis, freshness(symbol))
            snapshot = {
                "latest_quote": await get_json(redis, latest_quote(symbol)),
                "top_of_book": await get_json(redis, top_of_book(symbol)),
                "bar_1s": await get_json(redis, bar_1s(symbol)),
                "metrics": await get_json(redis, metrics(symbol)),
                "alerts": [json.loads(item) for item in await redis.lrange(alerts(symbol), 0, 9)],
            }
        finally:
            close = getattr(redis, "aclose", None)
            if close:
                await close()
        sources = self.search_sources(f"{symbol} stale freshness redis alert runbook")
        return {
            "status": "ok",
            "symbol": symbol,
            "freshness": fresh,
            "snapshot": snapshot,
            "redis_keys": {
                "freshness": freshness(symbol),
                "latest_quote": latest_quote(symbol),
                "top_of_book": top_of_book(symbol),
                "bar_1s": bar_1s(symbol),
                "metrics": metrics(symbol),
                "alerts": alerts(symbol),
            },
            "sources": sources,
        }

    async def explain_sequence_gap(self, parameters: dict[str, Any]) -> dict[str, Any]:
        symbol = required(parameters, "symbol").upper()
        exchange = parameters.get("exchange", "")
        start_time = parameters.get("start_time")
        end_time = parameters.get("end_time")
        sources = self.search_sources(f"{symbol} {exchange} sequence gap expected observed source offset")
        return {
            "status": "ok",
            "symbol": symbol,
            "exchange": exchange,
            "time_range": {"start_time": start_time, "end_time": end_time},
            "evidence_query": "silver_sequence_gaps and market.quality.alerts.v1",
            "expected_tables": ["silver_sequence_gaps", "gold_quality_annotations"],
            "expected_topics": ["market.raw.v1", "market.trades.v1", "market.quotes.v1", "market.quality.alerts.v1"],
            "sources": sources,
        }

    async def run_replay_dry_run(self, parameters: dict[str, Any]) -> dict[str, Any]:
        start_time = required(parameters, "start_time")
        end_time = required(parameters, "end_time")
        symbols = [symbol.upper() for symbol in parameters.get("symbols", [])]
        sources = self.search_sources(f"replay dry run {symbols} Delta bronze silver gold")
        return {
            "status": "ok",
            "dry_run": True,
            "mutated": False,
            "time_range": {"start_time": start_time, "end_time": end_time},
            "symbols": symbols,
            "source_topics": ["market.raw.v1", "market.trades.v1", "market.quotes.v1", "market.quality.alerts.v1"],
            "target_tables": ["bronze_market_events", "silver_trades", "silver_quotes", "gold_bars_1s"],
            "job": "lakehouse/jobs/replay_dry_run.py",
            "guardrails": ["no hot-path mutation", "isolated replay output", "explicit date scope required"],
            "sources": sources,
        }

    async def compare_live_vs_replay(self, parameters: dict[str, Any]) -> dict[str, Any]:
        symbol = required(parameters, "symbol").upper()
        as_of = parameters.get("as_of")
        live = None
        if self.redis_factory is not None:
            redis = await self.redis_factory()
            try:
                live = {
                    "top_of_book": await get_json(redis, top_of_book(symbol)),
                    "bar_1s": await get_json(redis, bar_1s(symbol)),
                    "metrics": await get_json(redis, metrics(symbol)),
                }
            finally:
                close = getattr(redis, "aclose", None)
                if close:
                    await close()
        sources = self.search_sources(f"{symbol} compare live replay Redis Delta gold_bars_1s")
        return {
            "status": "ok",
            "symbol": symbol,
            "as_of": as_of,
            "live_state": live,
            "replay_state": {
                "expected_tables": ["gold_bars_1s", "gold_spread_features", "gold_volatility_features"],
                "lookup_status": "requires Databricks query runtime",
            },
            "differences": [],
            "sources": sources,
        }

    async def summarize_incident(self, parameters: dict[str, Any]) -> dict[str, Any]:
        incident_id = required(parameters, "incident_id")
        results = self.evidence_store.search(f"{incident_id} incident timeline symptoms runbook alerts", limit=6)
        return {
            "status": "ok",
            "incident_id": incident_id,
            "summary": summarize_results(results),
            "timeline": [result.content[:240] for result in results[:3]],
            "sources": [source_dict(result) for result in results],
        }

    async def lineage_lookup(self, parameters: dict[str, Any]) -> dict[str, Any]:
        table = required(parameters, "table")
        symbol = parameters.get("symbol")
        lookup_time = parameters.get("time")
        contracts = yaml.safe_load(self.table_contract_path.read_text(encoding="utf-8"))
        table_spec = contracts["tables"].get(table)
        if not table_spec:
            return self.error("unknown_table", f"Unknown table {table!r}.", self.search_sources(f"{table} lineage"))
        sources = self.search_sources(f"{table} lineage {symbol or ''} {lookup_time or ''}")
        return {
            "status": "ok",
            "table": table,
            "symbol": symbol,
            "time": lookup_time,
            "zone": table_spec["zone"],
            "grain": table_spec["grain"],
            "partition_columns": table_spec["partition_columns"],
            "required_columns": table_spec["required_columns"],
            "upstream": upstream_for(table),
            "sources": sources,
        }

    def search_sources(self, query: str, limit: int = 5) -> list[dict[str, Any]]:
        return [source_dict(result) for result in self.evidence_store.search(query, limit=limit)]

    @staticmethod
    def error(error_code: str, message: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
        return {"status": "error", "error_code": error_code, "message": message, "sources": sources}


def required(parameters: dict[str, Any], field: str) -> Any:
    value = parameters.get(field)
    if value in (None, ""):
        raise ValueError(f"Missing required parameter: {field}")
    return value


async def get_json(redis: Any, key: str) -> dict[str, Any] | None:
    value = await redis.get(key)
    if not value:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value)


def upstream_for(table: str) -> list[str]:
    mapping = {
        "bronze_market_events": ["Kafka canonical topics"],
        "silver_trades": ["bronze_market_events"],
        "silver_quotes": ["bronze_market_events"],
        "silver_sequence_gaps": ["bronze_market_events"],
        "gold_bars_1s": ["silver_trades"],
        "gold_spread_features": ["silver_quotes"],
        "gold_volatility_features": ["gold_bars_1s"],
        "gold_quality_annotations": ["bronze_market_events", "silver_sequence_gaps"],
    }
    return mapping.get(table, [])


def summarize_results(results: list[SearchResult]) -> str:
    if not results:
        return "No indexed evidence found for this incident."
    first = results[0]
    return f"Most relevant evidence is from {first.source.title}: {first.content[:280]}"
