# WebSocket Contracts

## `/ws/live`

The API emits a snapshot every 500 ms:

```json
{
  "channel": "live",
  "time": "2026-05-04T12:00:00Z",
  "symbols": [
    {
      "symbol": "AAPL",
      "latest_quote": {},
      "top_of_book": {},
      "bar_1s": {},
      "metrics": {},
      "freshness": {},
      "alerts": []
    }
  ]
}
```

Future channel names should stay explicit, versioned when breaking changes are introduced, and backed by Redis state or derived Kafka topics.
