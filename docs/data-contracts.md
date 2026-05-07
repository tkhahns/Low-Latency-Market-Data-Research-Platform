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
| `feed.synthetic.raw.v1` | Feed simulator | Feed handler | Local POC input topic for simulator messages before canonicalization. |
| `market.raw.v1` | Feed handler | Lakehouse bronze writer, diagnostics | Canonical but minimally processed events. |
| `market.trades.v1` | Feed handler / stream processor | Flink jobs, bronze writer | Trade events after validation. |
| `market.quotes.v1` | Feed handler / stream processor | Flink jobs, bronze writer | Quote events after validation. |
| `market.state.top_of_book.v1` | Flink | API, monitoring, lakehouse | Current best bid/ask state. |
| `market.bars.1s.v1` | Flink | API, lakehouse | Rolling one-second bars. |
| `market.quality.alerts.v1` | Flink | API, MCP tools, observability | Sequence gaps, stale symbols, abnormal spreads. |

## Schema Location

JSON schemas live under `contracts/events`:

- `trade-event.v1.schema.json`
- `quote-event.v1.schema.json`
- `top-of-book-event.v1.schema.json`
- `bar-1s-event.v1.schema.json`
- `quality-alert-event.v1.schema.json`
- `market-event.v1.schema.json`

API contracts live under `contracts/api`. Redis hot-state contracts live under `contracts/redis`.

## Redis Key Layout

Redis keys are documented in `contracts/redis/keys.md`. The API reads only these hot keys for live request paths:

- `md:latest_quote:{SYMBOL}`
- `md:top_of_book:{SYMBOL}`
- `md:bar:1s:{SYMBOL}`
- `md:freshness:{SYMBOL}`
- `md:alerts:{SYMBOL}`
- `md:symbols:active`

## WebSocket and REST

The POC exposes:

- `GET /health`
- `GET /symbols`
- `GET /latest/{symbol}`
- `GET /freshness/{symbol}`
- `GET /alerts/{symbol}`
- `WS /ws/live`

The dashboard consumes `WS /ws/live`.
