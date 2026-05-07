# Lakehouse

Databricks and Delta Lake assets for historical storage, replay, research, and backtesting.

## Zones

| Zone | Description |
| --- | --- |
| Bronze | Raw canonical Kafka events stored append-only. |
| Silver | Cleaned and deduplicated trade and quote tables. |
| Gold | Research-ready bars, spread features, volatility features, and data-quality annotations. |

## Delta Table Contracts

| Table | Zone | Grain | Required Lineage |
| --- | --- | --- | --- |
| `bronze_market_events` | Bronze | One canonical Kafka event | `source_topic`, `source_partition`, `source_offset`, `schema_version`, `ingest_time`, `job_run_id`, `processed_at` |
| `silver_trades` | Silver | One deduplicated trade | Bronze lineage plus `trade_id`, `sequence_number` |
| `silver_quotes` | Silver | One deduplicated quote | Bronze lineage plus `sequence_number` |
| `silver_sequence_gaps` | Silver | One detected sequence anomaly | Symbol, exchange, observed sequence, expected sequence, source offsets |
| `gold_bars_1s` | Gold | Symbol-exchange-second | Source event range, volume, VWAP, OHLC |
| `gold_spread_features` | Gold | Symbol-exchange-time window | Source quote range, spread statistics |
| `gold_quality_annotations` | Gold | One quality annotation | Alert source, impacted symbols, table references |

## Principles

- The lakehouse is not in the live trading hot path.
- Raw events should remain replayable.
- Curated tables should preserve lineage back to input topics and jobs.
- Backfills should be reproducible and auditable.
