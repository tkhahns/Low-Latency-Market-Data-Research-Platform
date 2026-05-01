# Observability

Observability covers service health, stream health, data quality, and user-facing freshness.

## Initial Metrics

- Feed ingress rate.
- Kafka publish latency.
- Sequence gap count by symbol and exchange.
- Flink checkpoint duration and failure count.
- Redis write latency.
- API p50/p95/p99 response latency.
- WebSocket connected clients.
- Symbol freshness lag.
- Live-vs-replay divergence count.

## Alert Categories

- Freshness breaches.
- Sequence gaps.
- Stream processor failures.
- Redis hot state missing.
- Lakehouse job failures.
- MCP tool failures or unsafe requests.
