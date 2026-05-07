# Silver Pipeline

Cleaned and deduplicated market data tables for analytics and replay.

## Tables

- `silver_trades`
- `silver_quotes`
- `silver_sequence_gaps`

`silver_trades` and `silver_quotes` preserve bronze lineage. Trades are deduplicated by symbol, exchange, sequence number, and trade ID. Quotes are deduplicated by symbol, exchange, and sequence number.

`silver_sequence_gaps` detects missing, duplicate, and reordered feed sequences from bronze event ordering.

Entrypoint: `lakehouse/jobs/silver_normalize.py`.
