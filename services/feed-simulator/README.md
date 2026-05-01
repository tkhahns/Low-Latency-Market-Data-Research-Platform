# Feed Simulator

Generates synthetic exchange-style messages for local development and replay workflows.

## Planned Events

- Trades.
- Quotes.
- Order-book updates.
- Halts and resumptions.
- Imbalances.
- Sequence gaps and late events.

## Boundaries

- Publishes simulated raw feed messages.
- Does not emit curated market state.
- Can be configured to inject known data quality failures for MCP and observability demos.
