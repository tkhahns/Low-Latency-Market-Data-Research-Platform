---
title: Stale Symbol Runbook
tags: [runbook, redis, freshness, market-data]
---

# Stale Symbol Runbook

Symptoms:
- `md:freshness:{SYMBOL}` reports `status=stale`.
- Dashboard freshness turns warning.
- `market.quality.alerts.v1` contains `stale_symbol`.

Checks:
1. Read Redis keys `md:freshness:{SYMBOL}`, `md:top_of_book:{SYMBOL}`, and `md:alerts:{SYMBOL}`.
2. Verify recent quotes exist in `market.quotes.v1`.
3. Check Flink checkpoint and task health.
4. Compare live Redis state with replayed gold bars if Redis divergence is suspected.

Recovery:
- Restart only the stream processor if Kafka input is healthy and Redis writes stopped.
- Rebuild Redis from derived Kafka topics before declaring data loss.
- Use replay dry-run before any historical backfill.
