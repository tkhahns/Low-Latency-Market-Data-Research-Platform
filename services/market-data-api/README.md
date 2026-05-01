# Market Data API

Trader-facing API surface backed by Redis hot state.

## Planned Interfaces

- WebSocket stream for live quote, trade, bar, freshness, and alert updates.
- REST endpoints for latest symbol state.
- Health endpoints for dependencies and service readiness.

## Boundaries

- Reads hot state from Redis.
- Does not query Databricks in live request paths.
- Can expose replay metadata from the cold path through separate non-latency-sensitive endpoints later.
