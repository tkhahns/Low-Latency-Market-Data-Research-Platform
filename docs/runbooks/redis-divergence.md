---
title: Redis Divergence Runbook
tags: [runbook, redis, replay, divergence]
---

# Redis Divergence Runbook

Symptoms:
- Live Redis state differs from replayed Delta-derived state.
- API returns stale bars, top-of-book, or rolling metrics.
- `market_live_vs_replay_divergence_total` increases.

Checks:
1. Compare `md:top_of_book:{SYMBOL}`, `md:bar:1s:{SYMBOL}`, and `md:metrics:{SYMBOL}` against gold tables.
2. Confirm derived Kafka topics contain the expected events.
3. Check Redis key TTLs and write failures.
4. Inspect stream processor offsets and restarts.

Recovery:
- Do not manually edit Redis hot keys.
- Run `market_platform.tools.rebuild_redis_from_kafka --dry-run` first.
- Rebuild Redis from derived Kafka topics if the dry-run scope is correct.
- Record the divergence in `gold_quality_annotations` if research output is impacted.
