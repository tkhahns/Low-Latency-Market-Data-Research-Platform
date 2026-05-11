# Services

Runtime services are separated by operational responsibility.

| Service | Runtime Intent | Notes |
| --- | --- | --- |
| `feed-simulator` | Local feed generator | Useful for development, demos, replay, and failure injection. |
| `databento-feed` | Real provider adapter | Subscribes to Databento Live/replay data and publishes raw events into the feed handler input topic. |
| `feed-handler` | Ingestion edge | Owns parsing, validation, and canonical event publishing. |
| `stream-processor` | Flink jobs | Owns stateful calculations and quality alerts. |
| `market-data-api` | Serving API | Reads hot state from Redis and pushes updates to clients. |
| `mcp-ops-server` | Agentic operations | Exposes scoped reliability tools through MCP. |

Each service should own its runtime configuration, tests, and deployment manifest when implementation begins.
