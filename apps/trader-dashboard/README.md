# Trader Dashboard

Frontend for monitoring live market state.

## Planned Views

- Symbol watchlist.
- Latest quote and trade state.
- Spread, volume, VWAP, and rolling bars.
- Freshness and latency status.
- Data quality alerts.
- Platform health summary.

## Runtime Boundary

The dashboard consumes the `market-data-api`. It should not connect directly to Kafka, Redis, or Databricks.
