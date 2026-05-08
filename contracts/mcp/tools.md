# MCP Tool Contracts

MCP tools are diagnostic interfaces. In v1 they must not mutate production state.

| Tool | Input | Output |
| --- | --- | --- |
| `check_symbol_freshness` | `{ "symbol": "AAPL" }` | Freshness status, latest event timestamps, Redis key references. |
| `explain_sequence_gap` | `{ "symbol": "AAPL", "exchange": "XNAS", "start_time": "...", "end_time": "..." }` | Expected and observed sequences, impacted topics, alert references. |
| `run_replay_dry_run` | `{ "start_time": "...", "end_time": "...", "symbols": ["AAPL"] }` | Estimated offsets, target Delta tables, no-write confirmation. |
| `compare_live_vs_replay` | `{ "symbol": "AAPL", "as_of": "..." }` | Redis live state, replayed state, differences, source references. |
| `summarize_incident` | `{ "incident_id": "INC-001" }` | Timeline, symptoms, evidence, runbook links. |
| `lineage_lookup` | `{ "table": "gold_bars_1s", "symbol": "AAPL", "time": "..." }` | Upstream jobs, source topics, offsets, schema versions. |

Every output includes:

```json
{
  "status": "ok",
  "sources": [],
  "duration_ms": 0
}
```

Errors use `status: "error"` and include a deterministic `error_code`.

## Source References

Tools cite evidence from the RAG index using stable source references:

```json
{
  "source_uri": "obsidian:Research/Flink Window Tuning.md",
  "title": "Flink Window Tuning",
  "source_type": "obsidian",
  "chunk_index": 0,
  "score": 0.42
}
```

Repo docs use source types such as `docs`, `contracts`, `lakehouse`, `runbook`, and `incident`.

## Audit Contract

Every tool call writes:

```json
{
  "tool_name": "check_symbol_freshness",
  "caller": "demo",
  "parameters": { "symbol": "AAPL" },
  "result_status": "ok",
  "duration_ms": 12,
  "timestamp": "2026-05-07T00:00:00Z"
}
```

The local implementation writes JSONL audit records. The production target may write the same fields to `mcp_tool_audit` in Postgres.

## Guardrails

- `run_replay_dry_run` returns `mutated=false` and does not write Kafka, Redis, or Delta.
- Tool errors are structured and do not expose stack traces.
- Missing Redis, Databricks, or indexed evidence returns explicit status/error fields.
- Tools use service boundaries: Redis for hot state, Delta/lakehouse metadata for cold state, and indexed docs for explanatory evidence.
