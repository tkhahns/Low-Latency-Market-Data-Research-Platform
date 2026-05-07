# Bronze Pipeline

Append-only storage for raw canonical Kafka events.

## Table

- `bronze_market_events`

## Inputs

- `market.raw.v1`
- `market.trades.v1`
- `market.quotes.v1`
- `market.state.top_of_book.v1`
- `market.bars.1s.v1`
- `market.metrics.rolling.v1`
- `market.quality.alerts.v1`

## Required Lineage

- `source_topic`
- `source_partition`
- `source_offset`
- `schema_version`
- `ingest_time`
- `job_run_id`
- `processed_at`

Entrypoint: `lakehouse/jobs/bronze_ingest.py`.
