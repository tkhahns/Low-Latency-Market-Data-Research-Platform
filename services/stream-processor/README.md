# Stream Processor

Flink-based stateful streaming layer.

## Responsibilities

- Reconstruct top-of-book state.
- Compute spread, volume, VWAP, and rolling bars.
- Track symbol freshness and latency metrics.
- Detect gaps, stale data, abnormal spread, and volatility alerts.
- Write hot state to Redis.
- Emit derived topics for downstream consumers and lakehouse ingestion.

## State Design Notes

State should be keyed by symbol and exchange where possible. Checkpointing and restart behavior should be treated as production concerns, not afterthoughts.
