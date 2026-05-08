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

## Local Server

```bash
.venv/bin/python -m market_platform.services.mcp_ops_server
```

Endpoints:

- `GET /health`
- `GET /tools`
- `POST /index`
- `POST /tools/{tool_name}`
- `POST /mcp`

The HTTP endpoints are intentionally thin wrappers around structured tool functions. `/mcp` accepts JSON-RPC-style `tools/list` and `tools/call` requests for MCP client adapters.

## Obsidian Indexing

```bash
.venv/bin/python -m market_platform.tools.index_obsidian ~/ObsidianVault docs contracts lakehouse --json-store var/rag/vector-store.json
```

For Postgres + pgvector:

```bash
.venv/bin/python -m market_platform.tools.index_obsidian ~/ObsidianVault --postgres-dsn postgresql://market_ops:market_ops@localhost:5432/market_ops
```

The pgvector schema is `infra/postgres/pgvector.sql`.
