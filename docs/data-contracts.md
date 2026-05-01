# Data Contracts

Contracts are versioned before code is written. Producers and consumers should depend on these contracts rather than private service models.

## Canonical Event Principles

- Every event has a `schema_version`.
- Every feed-originating event has `symbol`, `exchange`, `event_time`, `ingest_time`, and `sequence_number`.
- Timestamps use UTC ISO-8601 strings at the API boundary.
- Internal stream jobs may convert timestamps to native types.
- Corrections, gaps, and halts are represented as first-class events or annotations.

## Kafka Topic Strategy

| Topic | Owner | Consumers | Notes |
| --- | --- | --- | --- |
| `market.raw.v1` | Feed handler | Lakehouse bronze writer, diagnostics | Canonical but minimally processed events. |
| `market.trades.v1` | Feed handler / stream processor | Flink jobs, bronze writer | Trade events after validation. |
| `market.quotes.v1` | Feed handler / stream processor | Flink jobs, bronze writer | Quote events after validation. |
| `market.state.top_of_book.v1` | Flink | API, monitoring, lakehouse | Current best bid/ask state. |
| `market.bars.1s.v1` | Flink | API, lakehouse | Rolling one-second bars. |
| `market.quality.alerts.v1` | Flink | API, MCP tools, observability | Sequence gaps, stale symbols, abnormal spreads. |

## Schema Location

JSON schemas live under `contracts/events`. They are intentionally minimal in the first scaffold and should be expanded before implementation.
