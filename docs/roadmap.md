# Roadmap

## Phase 0: Architecture Scaffold

- Establish repository layout.
- Document hot path, cold path, and agentic ops path.
- Define initial topic and event contracts.
- Keep placeholders lightweight until implementation starts.

## Phase 1: Local Streaming Skeleton

- Add feed simulator.
- Add feed handler.
- Run Kafka and Redis locally.
- Publish canonical trade and quote events.

## Phase 2: Stream Processing

- Add Flink jobs for top-of-book.
- Add rolling one-second bars.
- Add freshness and sequence-gap alerts.
- Write hot state into Redis.

## Phase 3: Serving and Dashboard

- Add WebSocket market data API.
- Add trader dashboard.
- Show live quote, trade, spread, volume, freshness, and alerts.

## Phase 4: Lakehouse

- Add Databricks Asset Bundle.
- Land bronze Delta tables.
- Build silver trades/quotes and gold bars/features.
- Add replay and backtest-ready datasets.

## Phase 5: MCP Reliability Copilot

- Index docs, incidents, schema docs, and runbooks.
- Implement freshness, replay, comparison, and lineage tools.
- Add audit logs for all tool calls.
