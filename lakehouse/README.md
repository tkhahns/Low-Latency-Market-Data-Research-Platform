# Lakehouse

Databricks and Delta Lake assets for historical storage, replay, research, and backtesting.

## Zones

| Zone | Description |
| --- | --- |
| Bronze | Raw canonical Kafka events stored append-only. |
| Silver | Cleaned and deduplicated trade and quote tables. |
| Gold | Research-ready bars, spread features, volatility features, and data-quality annotations. |

## Principles

- The lakehouse is not in the live trading hot path.
- Raw events should remain replayable.
- Curated tables should preserve lineage back to input topics and jobs.
- Backfills should be reproducible and auditable.
