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

The full machine-readable table contract lives in `lakehouse/contracts/tables.yml`.

## Jobs

Databricks job entrypoints live in `lakehouse/jobs`:

| Job | Purpose | Output |
| --- | --- | --- |
| `bronze_ingest.py` | Reads canonical Kafka topics and appends raw JSON events with source topic/partition/offset lineage. | `bronze_market_events` |
| `silver_normalize.py` | Parses bronze events, deduplicates trades/quotes, and detects sequence gaps. | `silver_trades`, `silver_quotes`, `silver_sequence_gaps` |
| `gold_features.py` | Builds research-ready 1s bars, spread features, volatility features, and quality annotations. | `gold_bars_1s`, `gold_spread_features`, `gold_volatility_features`, `gold_quality_annotations` |
| `quality_report.py` | Produces table-level row-count and date coverage reports. | Delta report path |
| `replay_dry_run.py` | Summarizes historical event ranges for a replay date without mutating hot-path state. | Isolated replay Delta path |

The Databricks Asset Bundle is `lakehouse/databricks/bundle.yml`. Runtime configuration is environment-driven:

| Variable | Default |
| --- | --- |
| `DATABRICKS_CATALOG` | `market_data` |
| `DATABRICKS_SCHEMA` | `research` |
| `LAKEHOUSE_BASE_PATH` | `dbfs:/tmp/low_latency_market_data_platform/delta` |
| `LAKEHOUSE_CHECKPOINT_PATH` | `dbfs:/tmp/low_latency_market_data_platform/checkpoints` |
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:9092` |
| `DATABRICKS_JOB_RUN_ID` | `manual` |

## Research Example

`lakehouse/notebooks/research_backtest_example.py` joins gold bars with spread features and quality annotations to produce a small backtest-ready feature frame. It intentionally reads gold tables only; live API requests continue to read Redis, not Databricks.

## Principles

- The lakehouse is not in the live trading hot path.
- Raw events should remain replayable.
- Curated tables should preserve lineage back to input topics and jobs.
- Backfills should be reproducible and auditable.
