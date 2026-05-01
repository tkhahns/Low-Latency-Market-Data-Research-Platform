# Lakehouse Jobs

Spark or Databricks job entrypoints will live here.

## Planned Jobs

- `bronze_ingest`: append canonical Kafka events to Delta bronze tables.
- `silver_trades_quotes`: validate, deduplicate, and normalize trade and quote history.
- `gold_market_features`: build bars, spreads, volatility, and quality annotations.
- `replay_day`: replay a historical market day into isolated outputs.
- `quality_report`: produce table-level freshness and completeness metrics.
