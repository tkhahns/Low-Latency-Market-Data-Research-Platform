---
title: Flink Window Tuning
tags: [flink, latency, redis, streaming]
---

# Flink Window Tuning

The MVP stream processor owns top-of-book, spread, rolling 1s bars, volume, VWAP, volatility, freshness lag, and quality alerts.

Benchmark notes:

- API load test reached 1444.59 requests/sec with p95 latency at 52.46 ms and p99 latency at 65.24 ms against the local dashboard stack.
- The target resume claim should stay tied to measured runs. Current local validation supports sub-100 ms p95 API serving, not a proven 50k events/sec ingestion claim.
- Redis key stability matters for replay rebuilds: derived Kafka topics can repopulate hot keys under `md:*`.

Follow-up:

- Run the local throughput benchmark after the Flink container is active.
- Record event/sec, p95 end-to-end lag, and p99 lag in `docs/performance.md`.
