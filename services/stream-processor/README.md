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

## Local POC

Iteration 1 uses a minimal Python stream consumer as a replaceable stand-in for Flink. Iteration 2 adds the Flink job under `flink/` while keeping the topics and Redis write model stable.

Implementation entrypoint: `python -m market_platform.services.stream_processor`.

Inputs:

- `market.trades.v1`
- `market.quotes.v1`
- `market.quality.alerts.v1`

Outputs:

- `market.state.top_of_book.v1`
- `market.bars.1s.v1`
- `market.metrics.rolling.v1`
- Redis hot keys documented in `contracts/redis/keys.md`

## Flink MVP

Build and run with Docker:

```bash
./scripts/run-mvp-flink.sh
```

The Flink job configures checkpoints, fixed-delay restart behavior, Kafka sources/sinks, and Redis hot-state sinks. It computes top-of-book, spread alerts, 1-second bars, VWAP, rolling volume, volatility, freshness state, and alert materialization.

The Python processor remains available for local fallback:

```bash
./scripts/run-local-demo.sh
```
