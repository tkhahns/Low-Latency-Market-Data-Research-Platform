# Databento Dashboard Demo

The default project data source is the synthetic feed simulator. For a real provider demo, use the Databento feed adapter. It subscribes to Databento Live records, converts `mbp-1` records into raw quote events, converts trade records into raw trade events, and publishes them into the same raw input topic consumed by the feed handler.

The hot path stays unchanged:

```text
Databento -> feed.synthetic.raw.v1 -> feed handler -> Kafka derived topics -> stream processor -> Redis -> WebSocket API -> dashboard
```

## Databento Account

1. Sign up at Databento.
2. Copy an API key from the Databento portal. Keys currently start with `db-`.
3. Export the key in the shell that starts the demo:

```bash
export DATABENTO_API_KEY='db-...'
```

Databento provides free data credits for new accounts. Use a bounded timeout for demos so a live stream does not run longer than intended.

## Recommended Low-Cost Demo

The default real-feed demo uses CME Globex parent futures symbols because Databento's public quickstarts use `GLBX.MDP3`, `ES.FUT`, and `NQ.FUT`.

```bash
export DATABENTO_API_KEY='db-...'
export DATABENTO_DATASET='GLBX.MDP3'
export DATABENTO_SYMBOLS='ES.FUT,NQ.FUT'
export DATABENTO_SCHEMAS='mbp-1,trades'
export DATABENTO_STYPE_IN='parent'
export DATABENTO_TIMEOUT_SECONDS='120'
export EXCHANGE='GLBX'
```

Start only the services needed for the dashboard and the Databento adapter:

```bash
docker compose -f infra/docker-compose.yml --profile databento up --build redpanda redis feed-handler stream-processor market-data-api databento-feed
```

Then open:

- Dashboard: `http://localhost:8000`
- API health: `http://localhost:8000/health`
- Symbols observed by Redis: `http://localhost:8000/symbols`

Stop the stack:

```bash
docker compose -f infra/docker-compose.yml down
```

## Intraday Replay Mode

If your account is not entitled for the live stream you selected, set a Databento intraday replay start within the allowed replay window:

```bash
export DATABENTO_REPLAY_START='2026-05-11T13:30:00Z'
export DATABENTO_TIMEOUT_SECONDS='120'
docker compose -f infra/docker-compose.yml --profile databento up --build redpanda redis feed-handler stream-processor market-data-api databento-feed
```

Leave `DATABENTO_REPLAY_START` unset for current live streaming.

## Local Python Adapter Run

If Redpanda and Redis are already running in Docker and you want to run the adapter from the repo virtual environment:

```bash
export KAFKA_BOOTSTRAP_SERVERS='localhost:19092'
export DATABENTO_API_KEY='db-...'
export DATABENTO_TIMEOUT_SECONDS='120'
.venv/bin/python -m market_platform.services.databento_feed
```

For a full local non-Docker service run, start these in separate terminals:

```bash
export KAFKA_BOOTSTRAP_SERVERS='localhost:19092'
export REDIS_URL='redis://localhost:6379/0'
.venv/bin/python -m market_platform.services.feed_handler
.venv/bin/python -m market_platform.services.stream_processor
.venv/bin/python -m market_platform.services.market_data_api
.venv/bin/python -m market_platform.services.databento_feed
```

## Notes

- Do not run `feed-simulator` and `databento-feed` together unless you intentionally want mixed synthetic and real-provider data.
- The adapter currently supports `mbp-1`, `bbo-*`, and `trades`-style DBN records.
- The dashboard still reads only Redis through the market data API. It never connects directly to Databento, Kafka, or Databricks.
