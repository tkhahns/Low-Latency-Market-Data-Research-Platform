---
title: INC-001 Sequence Gap During Synthetic Feed
tags: [incident, sequence-gap, demo]
---

# INC-001 Sequence Gap During Synthetic Feed

Timeline:
- Synthetic simulator skipped an AAPL sequence number.
- Feed handler detected expected sequence 12 but observed sequence 14.
- Quality alert was published to `market.quality.alerts.v1`.
- Silver sequence gap and gold quality annotations should preserve the anomaly for research.

Resolution:
- No production mutation required.
- Replay dry-run can confirm impacted offsets and symbols.
