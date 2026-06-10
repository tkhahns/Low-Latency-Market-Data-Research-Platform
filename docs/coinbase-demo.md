# Coinbase Live Feed Demo

The default real-data demo uses the Coinbase Exchange WebSocket (`wss://ws-feed.exchange.coinbase.com`).
**No API key or account required.** The feed is available 24/7.

## Quick start

```bash
docker compose -f infra/docker-compose.yml --profile coinbase up --build \
  redpanda redis feed-handler stream-processor market-data-api coinbase-feed
```

Then open:

- Dashboard: `http://localhost:8000`
- API health: `http://localhost:8000/health`
- Symbols in Redis: `http://localhost:8000/symbols`
- Latest BTC-USD: `http://localhost:8000/latest/BTC-USD`

Stop the stack:

```bash
docker compose -f infra/docker-compose.yml --profile coinbase down
```

## Configuration

| Env var | Default | Notes |
| --- | --- | --- |
| `COINBASE_PRODUCTS` | `BTC-USD,ETH-USD,SOL-USD` | Comma-separated product IDs |
| `COINBASE_CHANNELS` | `matches,ticker,heartbeat` | Feed channels |
| `FEED_STALL_SECONDS` | `30` | Seconds without message before watchdog reconnects |
| `FEED_TIMEOUT_SECONDS` | (unset) | Optional: stop after N seconds (demo use) |

## Sequence gap detection

The adapter maps Coinbase `trade_id` to `sequence_number` for trades (monotonic, +1 per
product). Real dropped or re-ordered trades surface as `sequence_gap` alerts on the dashboard.
Quote sequence numbers use a per-product local counter so the trade and quote spaces don't
interfere.

## Replay mode (no network required)

For CI and local offline demos, use the committed fixture:

```bash
docker compose -f infra/docker-compose.yml --profile replay up --build \
  redpanda redis feed-handler stream-processor market-data-api replay-feed
```

The replay source (`FEED_REPLAY_SPEED=0`) replays the fixture at maximum speed. Set
`FEED_REPLAY_LOOP=true` to loop continuously.

## Capture a new fixture

```bash
.venv/bin/python -m market_platform.tools.capture_feed \
  --source coinbase --seconds 30 \
  --out market_platform/fixtures/coinbase-sample.jsonl \
  --scrub
```

## Notes

- Do not run `feed-simulator` and `coinbase-feed` together unless you want mixed data.
- Databento is still available as a paid-tier upgrade profile; see `docs/databento-demo.md`.
