# Databento Feed Adapter

Subscribes to Databento Live or intraday replay data and publishes raw trade/quote events to `feed.synthetic.raw.v1`, which is the feed handler input topic used by the local POC.

Environment:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABENTO_API_KEY` | required | Databento API key. |
| `DATABENTO_DATASET` | `GLBX.MDP3` | Dataset to subscribe to. |
| `DATABENTO_SYMBOLS` | `ES.FUT,NQ.FUT` | Comma-separated symbols. |
| `DATABENTO_SCHEMAS` | `mbp-1,trades` | Comma-separated schemas. |
| `DATABENTO_STYPE_IN` | `parent` | Input symbology type. |
| `DATABENTO_REPLAY_START` | unset | Optional intraday replay start. |
| `DATABENTO_TIMEOUT_SECONDS` | unset | Optional bounded run duration. |

Run through Docker Compose:

```bash
docker compose -f infra/docker-compose.yml --profile databento up --build redpanda redis feed-handler stream-processor market-data-api databento-feed
```

See `docs/databento-demo.md` for the dashboard workflow.
