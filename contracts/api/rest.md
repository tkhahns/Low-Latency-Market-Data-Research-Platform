# REST API Contracts

The local POC API is backed only by Redis hot state.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Service and Redis health. |
| `GET` | `/symbols` | Active symbols observed by the stream processor. |
| `GET` | `/latest/{symbol}` | Latest quote, top-of-book, 1s bar, freshness, and recent alerts. |
| `GET` | `/freshness/{symbol}` | Freshness status for one symbol. |
| `GET` | `/alerts/{symbol}` | Recent quality alerts for one symbol. |

`/latest/{symbol}` response shape:

```json
{
  "symbol": "AAPL",
  "latest_quote": {},
  "top_of_book": {},
  "bar_1s": {},
  "freshness": {},
  "alerts": []
}
```

