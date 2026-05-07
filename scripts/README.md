# Scripts

| Script | Purpose |
| --- | --- |
| `run-local-demo.sh` | Start the local POC with the Python stream processor fallback. |
| `run-mvp-flink.sh` | Start the Docker Compose Flink profile and disable the Python stream processor. |
| `load-test-local.py` | Measure local API throughput and p95/p99 latency for `/latest/{symbol}`. |

Operational scripts will live here once implementation begins.

## Planned Scripts

- Create local Kafka topics.
- Seed sample symbols.
- Run replay dry-runs.
- Validate schemas.
- Export incident bundles for MCP indexing.
