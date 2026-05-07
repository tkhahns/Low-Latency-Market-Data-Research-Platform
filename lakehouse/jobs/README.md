# Lakehouse Jobs

Databricks/Spark entrypoints for the cold path. These jobs are batch or `availableNow` jobs and are never called by the live market-data API.

## Entry Points

| Script | Responsibility |
| --- | --- |
| `bronze_ingest.py` | Reads Kafka-compatible topics into append-only Delta bronze with topic, partition, offset, schema version, job run ID, and processing timestamp. |
| `silver_normalize.py` | Parses bronze JSON, deduplicates trades and quotes, and detects sequence gaps. |
| `gold_features.py` | Builds 1s bars, spread features, volatility features, and quality annotations. |
| `replay_dry_run.py` | Produces an isolated summary for one historical event date before any replay is run. |
| `quality_report.py` | Emits table row-count and event-date coverage checks. |
| `common.py` | Shared Databricks path, catalog, schema, and table registration helpers. |

## Expected Order

1. `bronze_ingest.py`
2. `silver_normalize.py`
3. `gold_features.py`
4. `quality_report.py`
5. `replay_dry_run.py` as needed for a selected `REPLAY_DATE`

## Required Environment

- `KAFKA_BOOTSTRAP_SERVERS`
- `DATABRICKS_CATALOG`
- `DATABRICKS_SCHEMA`
- `LAKEHOUSE_BASE_PATH`
- `LAKEHOUSE_CHECKPOINT_PATH`
- `DATABRICKS_JOB_RUN_ID`

The Asset Bundle in `lakehouse/databricks/bundle.yml` wires these defaults for dev/prod targets.
