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
tests/unit + tests/integration: 8 passed
```

Record real benchmark output here after running the stack on a Docker-enabled machine.
