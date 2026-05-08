# MCP Ops Examples

The ops server exposes structured diagnostic tools through HTTP endpoints that map directly to the MCP tool contracts. Every response includes `status`, `sources`, and `duration_ms`.

## Start

```bash
.venv/bin/python -m market_platform.services.mcp_ops_server
```

## Index Obsidian And Repo Docs

```bash
.venv/bin/python -m market_platform.tools.index_obsidian ~/ObsidianVault --source-type obsidian --json-store var/rag/vector-store.json
.venv/bin/python -m market_platform.tools.index_obsidian docs contracts lakehouse --source-type docs --json-store var/rag/vector-store.json
```

For pgvector:

```bash
.venv/bin/python -m market_platform.tools.index_obsidian ~/ObsidianVault --source-type obsidian --postgres-dsn postgresql://market_ops:market_ops@localhost:5432/market_ops
.venv/bin/python -m market_platform.tools.index_obsidian docs contracts lakehouse --source-type docs --postgres-dsn postgresql://market_ops:market_ops@localhost:5432/market_ops
```

## Tool Calls

```bash
curl -X POST http://localhost:8010/tools/check_symbol_freshness \
  -H 'content-type: application/json' \
  -d '{"parameters":{"symbol":"AAPL"},"caller":"demo"}'
```

```bash
curl -X POST http://localhost:8010/tools/lineage_lookup \
  -H 'content-type: application/json' \
  -d '{"parameters":{"table":"gold_bars_1s","symbol":"AAPL","time":"2026-05-07T00:00:00Z"},"caller":"demo"}'
```

## Example Prompts

- Why is AAPL stale right now?
- Explain the sequence gap for AAPL on XNAS between 2026-05-07T00:00:00Z and 2026-05-07T00:05:00Z.
- Run a replay dry-run for AAPL and MSFT for the last five minutes.
- Compare live Redis state against replayed gold tables for AAPL.
- Summarize INC-001 with source references.
- Show the lineage for `gold_bars_1s`.
