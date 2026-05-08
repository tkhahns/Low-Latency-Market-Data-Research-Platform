# Performance

## Local Benchmark Setup

Start the local POC fallback:

```bash
./scripts/run-local-demo.sh
```

Or start the Flink MVP profile:

```bash
./scripts/run-mvp-flink.sh
```

Run the API benchmark:

```bash
.venv/bin/python scripts/load-test-local.py --symbol AAPL --requests 500 --concurrency 25
```

The benchmark records request count, concurrency, failures, throughput, mean latency, p95 latency, and p99 latency for the Redis-backed `/latest/{symbol}` API.

## Current Environment Result

The current coding environment does not provide `docker` or `mvn`, so the live stack and Flink job could not be benchmarked here.

Verified locally through `.venv`:

```text
tests/unit + tests/integration: 23 passed
```

Record real benchmark output here after running the stack on a Docker-enabled machine.

## Resume Claim Status

The architecture now contains Kafka-compatible ingestion, Flink MVP code, Redis hot serving, Delta Lake cold-path jobs, WebSocket APIs, and Obsidian-backed RAG/MCP tooling. The following metrics are not yet proven in this environment:

- `50k+ events/sec`
- `sub-100ms p95` end-to-end latency
- `65%` tick-to-signal latency reduction

Those claims require a Docker/Maven/Databricks-capable benchmark run with before/after measurements for the Python fallback versus Flink/Redis optimized path.
