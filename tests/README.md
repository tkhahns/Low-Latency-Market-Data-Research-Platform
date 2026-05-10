# Tests

Run all local tests through the repo virtual environment:

```bash
.venv/bin/python -m pytest -q
```

## Current Coverage

- Event schema validation.
- Feed event canonicalization and sequence checks.
- Stream state calculations for top-of-book, bars, VWAP, and volatility.
- Deterministic stream output integration tests.
- FastAPI REST/WebSocket behavior with fake Redis.
- Redis rebuild helper behavior.
- Lakehouse bronze/silver/gold transformations and table contracts.
- MCP/RAG indexing, tool results, audit logging, and JSON-RPC bridge.
- Production artifact validation for CI, Kubernetes, observability, runbooks, secrets, and Terraform scaffolding.

## External Runtime Tests

These require Docker, Maven, Databricks, or managed cloud services:

- Docker Compose end-to-end hot-path smoke test.
- Flink Maven package and runtime job submission.
- Databricks Asset Bundle validation and job execution.
- Redis/Postgres/pgvector integration against real services.
- Throughput and p95/p99 benchmark runs.
