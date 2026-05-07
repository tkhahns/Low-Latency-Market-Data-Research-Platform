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
| `market.metrics.rolling.v1` | Flink | API, lakehouse | Rolling volume, VWAP, and volatility features. |
| `market.quality.alerts.v1` | Flink | API, MCP tools, observability | Sequence gaps, stale symbols, abnormal spreads. |

## Schema Location

JSON schemas live under `contracts/events`:

- `trade-event.v1.schema.json`
- `quote-event.v1.schema.json`
- `top-of-book-event.v1.schema.json`
- `bar-1s-event.v1.schema.json`
- `rolling-metrics-event.v1.schema.json`
- `quality-alert-event.v1.schema.json`
- `market-event.v1.schema.json`

API contracts live under `contracts/api`. Redis hot-state contracts live under `contracts/redis`.

## Redis Key Layout

Redis keys are documented in `contracts/redis/keys.md`. The API reads only these hot keys for live request paths:

- `md:latest_quote:{SYMBOL}`
- `md:top_of_book:{SYMBOL}`
- `md:bar:1s:{SYMBOL}`
- `md:metrics:{SYMBOL}`
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

## Delta Lake Contracts

Cold-path contracts are defined in `lakehouse/contracts/tables.yml` and implemented by Databricks jobs in `lakehouse/jobs`.

| Zone | Tables |
| --- | --- |
| Bronze | `bronze_market_events` |
| Silver | `silver_trades`, `silver_quotes`, `silver_sequence_gaps` |
| Gold | `gold_bars_1s`, `gold_spread_features`, `gold_volatility_features`, `gold_quality_annotations` |

Every cold-path table preserves job lineage through `job_run_id` and `processed_at`. Bronze and silver preserve source topic, partition, and offset. Gold feature tables preserve source offset ranges so research results can be traced back to raw event ranges.
