# Redis Hot State Keys

Redis is the only hot serving cache in v1. Keys are stable service contracts.

| Key | Type | Writer | TTL | Purpose |
| --- | --- | --- | --- | --- |
| `md:symbols:active` | set | stream processor | none | Symbols observed in the hot path. |
| `md:latest_quote:{SYMBOL}` | string JSON | stream processor | 120s | Latest canonical quote. |
| `md:top_of_book:{SYMBOL}` | string JSON | stream processor | 120s | Latest top-of-book state. |
| `md:bar:1s:{SYMBOL}` | string JSON | stream processor | 120s | Latest rolling 1-second bar. |
| `md:freshness:{SYMBOL}` | string JSON | stream processor | 120s | Freshness lag and status. |
| `md:alerts:{SYMBOL}` | list JSON | stream processor | 1h | Most recent quality alerts. |

Symbols are uppercase. JSON payloads preserve the versioned event fields from `contracts/events`.

