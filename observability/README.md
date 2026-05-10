# Observability

Observability covers service health, stream health, data quality, and user-facing freshness.

## Assets

| Path | Purpose |
| --- | --- |
| `otel/collector-config.yaml` | OTLP traces, metrics, and logs collector for local and container demos. |
| `grafana/market-data-dashboard.json` | Dashboard panels for ingress, latency, freshness, sequence gaps, Flink, and MCP errors. |
| `alerts/market-data-alerts.yml` | Prometheus-compatible alert rules with runbook links. |
| `metrics.yml` | Metric names, types, labels, and descriptions. |
| `logging-schema.json` | Structured JSON log field contract. |

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
- MCP tool call count, latency, and error rate.

## Alert Categories

- Freshness breaches.
- Sequence gaps.
- Stream processor failures.
- Redis hot state missing or divergent.
- Lakehouse job failures.
- MCP tool failures or unsafe requests.

## Trace Boundaries

Trace spans should use these service names:

- `feed-simulator`
- `feed-handler`
- `stream-processor`
- `market-data-api`
- `mcp-ops-server`
- `lakehouse-jobs`
