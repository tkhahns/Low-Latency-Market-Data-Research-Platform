# Decision 0001: V1 Architecture Scope

## Status

Accepted

## Context

The project should demonstrate serious data engineering and quant infrastructure ability without becoming an unbounded trading system.

## Decision

V1 will use:

- Kafka for durable streaming.
- Flink for stateful stream processing.
- Redis for hot serving state.
- WebSocket / REST API for trader-facing access.
- Databricks Delta Lake for history, replay, and research datasets.
- PostgreSQL + pgvector for RAG metadata.
- MCP server for controlled reliability tools.

V1 will not include:

- Airflow or Cloud Composer.
- Iceberg alongside Delta Lake.
- ClickHouse.
- Bigtable.
- OpenLineage / Marquez.
- Vertex AI Vector Search.

## Consequences

The architecture stays focused. Databricks supports historical research and replay, while the low-latency path remains Kafka to Flink to Redis to API.
