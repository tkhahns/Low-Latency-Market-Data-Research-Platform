---
title: Runbook - Stale Symbol
tags: [runbook, freshness, redis, kafka]
---

# Runbook - Stale Symbol

When a symbol becomes stale:

1. Check Redis freshness key `md:freshness:{symbol}`.
2. Check recent quality alerts under `md:alerts:{symbol}`.
3. Confirm derived Kafka topics still receive top-of-book and rolling metrics.
4. Compare live state with replay output before changing production behavior.

Relevant MCP tools:

- `check_symbol_freshness`
- `explain_sequence_gap`
- `compare_live_vs_replay`
