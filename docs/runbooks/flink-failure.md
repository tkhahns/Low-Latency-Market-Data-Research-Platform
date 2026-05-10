---
title: Flink Failure Runbook
tags: [runbook, flink, checkpoint, streaming]
---

# Flink Failure Runbook

Symptoms:
- Flink checkpoint failures increase.
- `market.state.top_of_book.v1`, `market.bars.1s.v1`, or `market.metrics.rolling.v1` stops updating.
- Redis freshness lag rises for active symbols.

Checks:
1. Inspect Flink JobManager and TaskManager health.
2. Check checkpoint duration, failed checkpoint count, and restart count.
3. Verify Kafka consumer lag for `market.trades.v1` and `market.quotes.v1`.
4. Confirm Redis write errors are not the root cause.
5. Check recent deployment image tags and configuration changes.

Recovery:
- Let the configured restart strategy recover transient failures.
- Roll back the Flink image if failures started after deployment.
- Rebuild Redis from derived Kafka topics after recovery.
- Run replay dry-run for impacted time ranges before any backfill.
