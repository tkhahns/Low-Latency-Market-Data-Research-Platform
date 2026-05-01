# Operational Model

## Service Ownership

| Area | Runtime | Primary Reliability Concern |
| --- | --- | --- |
| Feed handling | JVM or Go service | Sequence validation, timestamping, canonicalization. |
| Streaming | Flink | Stateful recovery, checkpointing, exactly-once boundaries where practical. |
| Hot serving | Redis + API | Freshness, fan-out, p99 response time, connection stability. |
| Lakehouse | Databricks + Delta | Replayability, data quality, partition design, lineage. |
| Agentic ops | MCP server + pgvector | Tool permissions, auditability, grounded answers. |

## Initial SLO Ideas

- Feed handler publishes validated events within a target p99 budget.
- Redis top-of-book state remains fresh for actively traded symbols.
- Flink checkpoints complete successfully within an agreed interval.
- Bronze tables receive all market events emitted to durable Kafka topics.
- MCP tools return source-backed diagnostics and emit audit logs.

## Incident Types

- Symbol freshness breach.
- Kafka sequence gap.
- Flink checkpoint failure.
- Redis state divergence from replayed Delta history.
- Databricks job failure.
- Dashboard or API websocket degradation.
