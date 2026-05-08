---
title: Sequence Gap Runbook
tags: [runbook, kafka, sequence-gap, lakehouse]
---

# Sequence Gap Runbook

Symptoms:
- Feed handler emits `sequence_gap` to `market.quality.alerts.v1`.
- `silver_sequence_gaps` contains expected and observed sequence numbers.
- `gold_quality_annotations` marks affected symbols and source offsets.

Checks:
1. Identify symbol, exchange, expected sequence, observed sequence, topic, partition, and offset.
2. Confirm whether the gap is present in `market.raw.v1`.
3. Check whether the provider adapter skipped messages or emitted reordered events.
4. Use the replay dry-run job for the affected time range.

Recovery:
- Do not mutate live Redis directly.
- Reprocess from Kafka if retention covers the gap.
- Annotate research tables through `gold_quality_annotations`.
