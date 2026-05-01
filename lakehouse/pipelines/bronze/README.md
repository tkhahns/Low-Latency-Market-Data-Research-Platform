# Bronze Pipeline

Append-only storage for raw canonical market events.

## Expected Tables

- `bronze_market_raw_events`
- `bronze_market_trades`
- `bronze_market_quotes`
- `bronze_market_quality_alerts`

Bronze tables should preserve event payload, source topic, partition, offset, and ingestion metadata.
