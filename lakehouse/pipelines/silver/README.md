# Silver Pipeline

Cleaned market data tables for analytics and replay.

## Expected Tables

- `silver_trades`
- `silver_quotes`
- `silver_top_of_book`
- `silver_sequence_gaps`

Silver jobs should handle deduplication, sequence ordering, schema validation, and malformed records.
