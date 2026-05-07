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
