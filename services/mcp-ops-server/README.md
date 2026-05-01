# MCP Ops Server

Controlled agentic operations layer for reliability diagnostics.

## Initial Tools

| Tool | Purpose |
| --- | --- |
| `check_symbol_freshness` | Inspect Redis, stream metrics, and latest event timestamps for a symbol. |
| `explain_sequence_gap` | Trace observed gaps by symbol, exchange, topic, and time range. |
| `run_replay_dry_run` | Estimate replay scope and impacted tables without mutating production state. |
| `compare_live_vs_replay` | Compare Redis live state against replayed Delta history. |
| `summarize_incident` | Build an incident summary from metrics, alerts, and runbooks. |
| `lineage_lookup` | Explain which jobs and tables produced a research dataset. |

## Guardrails

- Tool calls should be audited.
- Mutating operations should require explicit dry-run support first.
- RAG answers should cite indexed docs, runbooks, incidents, or table metadata.
