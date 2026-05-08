# Obsidian RAG Integration

The agentic ops path can index an Obsidian vault into the project vector store so diagnostics can cite project notes, design decisions, runbooks, and market research notes.

## Supported Sources

- Obsidian vault markdown files.
- Repo docs and contracts.
- Runbooks and incident notes.
- Lakehouse table metadata.

## Local Indexing

Use the repo virtual environment:

```bash
.venv/bin/python -m market_platform.tools.index_obsidian /path/to/ObsidianVault --json-store var/rag/vector-store.json
```

The local JSON store is deterministic and useful for demos/tests. For Postgres + pgvector:

```bash
.venv/bin/python -m market_platform.tools.index_obsidian /path/to/ObsidianVault --postgres-dsn postgresql://market_ops:market_ops@localhost:5432/market_ops
```

The pgvector schema is `infra/postgres/pgvector.sql`.

## Note Conventions

Recommended frontmatter:

```markdown
---
title: Flink Window Tuning Notes
tags: [flink, latency, market-data]
---
```

Good notes include concrete symbols, tables, topics, alerts, incident IDs, and links to source docs. The MCP tools return source references as `obsidian:<relative-path>` when those notes are selected as evidence.

## Guardrails

- Indexed notes are evidence, not authority. Tools still return structured status and known source boundaries.
- Replay tools are dry-run only in v1.
- No tool mutates production Kafka, Redis, Delta, or Databricks state.
- Every tool call writes an audit record when the ops server is configured with `MCP_AUDIT_LOG`.
