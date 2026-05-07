# Feed Handler

Ingestion edge for raw market feed events.

## Responsibilities

- Parse raw feed or simulator messages.
- Validate required fields.
- Track sequence numbers by symbol and exchange.
- Add ingress timestamps.
- Convert messages into canonical event contracts.
- Publish validated events to Kafka.
- Publish data-quality alerts for gaps and malformed messages.

## Non-Responsibilities

- It does not compute bars, spreads, or order-book state.
- It does not write directly to Redis.
- It does not perform historical research transformations.

## Local POC

Implementation entrypoint: `python -m market_platform.services.feed_handler`.

Input topic: `feed.synthetic.raw.v1`.

Output topics:

- `market.raw.v1`
- `market.trades.v1`
- `market.quotes.v1`
- `market.quality.alerts.v1`
