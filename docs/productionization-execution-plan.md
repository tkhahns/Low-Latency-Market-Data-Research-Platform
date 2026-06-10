# Productionization Execution Plan

Execution-ready companion to `docs/next-iteration-plan.md`. Each phase lists exact files,
interfaces, tests, and a verification gate. Phases are ordered by dependency; finish a
phase's gate before starting the next (Phases 2 and 3 may run in parallel).

Target outcome: the platform runs continuously on a free real data source
(Coinbase Exchange WebSocket), is exercised end to end by CI without secrets, and
deploys to GCP through an automated pipeline with measured benchmarks.

---

## Phase 0 — Baseline

1. Branch from `main`; confirm green baseline:

```bash
.venv/bin/python -m ruff check .
.venv/bin/python -m pytest -q
.venv/bin/python scripts/validate-production-artifacts.py
```

Gate: all pass before any change.

---

## Phase 1 — Feed Source Abstraction + Coinbase Adapter

### 1.1 Contract migration: fractional trade sizes (do this first)

Crypto sizes are fractional (`"size": "0.0102"`); the current contract is `integer`
end to end. Migrate `size`, `bid_size`, `ask_size` from integer to number:

| File | Change |
| --- | --- |
| `contracts/events/trade-event.v1.schema.json` | `size`: `"type": "number"` |
| `contracts/events/quote-event.v1.schema.json` | `bid_size`/`ask_size`: `"type": "number"` |
| `contracts/events/market-event.v1.schema.json`, `top-of-book-event.v1.schema.json`, `bar-1s-event.v1.schema.json` | same for size/volume fields |
| `market_platform/config.py` | `SCHEMA_VERSION = "1.1"`; schemas accept `"enum": ["1.0", "1.1"]` for `schema_version` |
| `market_platform/events.py` | `canonical_trade`/`canonical_quote`: `int(...)` → `float(...)` for sizes |
| `market_platform/stream_state.py` | `volume: int` → `float`; `int(trade["size"])` → `float(...)` (lines ~19, 50, 142, 160) |
| Flink `EventTransforms.java` | `integer(quote, "bid_size")` → decimal accessor; volume aggregation `long` → `double` in `BarAndMetricsProcessFunction.java` |
| `tests/unit/test_contract_schemas.py`, `test_events.py`, `test_stream_state.py` | add fractional-size cases |

Equities/synthetic events keep emitting whole numbers — no consumer change needed beyond
type widening.

### 1.2 Feed source protocol

New package `market_platform/feeds/`:

```python
# market_platform/feeds/base.py
class FeedSource(Protocol):
    name: str
    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Yield raw events matching feed_handler.RAW_REQUIRED_FIELDS."""
```

Raw event shape is unchanged (what `feed_handler.validate_required` checks):
trade = `{event_type, symbol, exchange, event_time, sequence_number, price, size}`,
quote = `{..., bid_price, bid_size, ask_price, ask_size}`.

### 1.3 Coinbase adapter (pure mapping module, mirrors `databento_adapter.py`)

`market_platform/coinbase_adapter.py` — pure functions, no I/O, fully unit-testable:

- Endpoint: `wss://ws-feed.exchange.coinbase.com`; subscribe message:
  `{"type": "subscribe", "product_ids": [...], "channels": ["matches", "ticker", "heartbeat"]}`.
- `trade_from_match(msg)` — input `{"type":"match","trade_id":N,"sequence":N,"time":ISO,"product_id":"BTC-USD","size":"0.01","price":"50000.1","side":"buy"}` →
  raw trade with `price=float`, `size=float`, `conditions=[side]`,
  `trade_id=str(msg["trade_id"])`, `exchange="COINBASE"`.
- `quote_from_ticker(msg)` — input ticker with `best_bid`, `best_bid_size`, `best_ask`,
  `best_ask_size`, `time` → raw quote. Skip if any best-bid/ask field missing
  (first ticker after subscribe can be partial).

**Sequence-number strategy (important):** Coinbase's `sequence` increments per product
across *all* feed messages, so subscribing to a subset of channels makes raw `sequence`
look permanently gappy and would flood `sequence_gap` alerts. Instead:

- Trades: `sequence_number = msg["trade_id"]` — trade IDs are per-product, monotonic,
  +1 per trade. Real dropped trades surface as real gaps via the existing
  `SequenceTracker`. Genuine gap detection on real data, no false positives.
- Quotes: locally assigned per-product monotonic counter (tickers have no dense ID).
- Heartbeat messages carry `last_trade_id`; use them in Phase 2's watchdog to detect
  silent trade loss.
- `SequenceTracker` keys on `(symbol, exchange)` but both trades and quotes flow through
  one tracker in the feed handler. Add `event_type` to the tracker key
  (`market_platform/events.py:18`) so the trade-ID space and the local quote counter
  don't collide. Update `tests/unit/test_events.py` accordingly.

### 1.4 Coinbase feed source (I/O wrapper)

`market_platform/feeds/coinbase.py`: `websockets`-based client (add `websockets>=12`
to `pyproject.toml` deps), reads env:

| Env var | Default |
| --- | --- |
| `COINBASE_WS_URL` | `wss://ws-feed.exchange.coinbase.com` |
| `COINBASE_PRODUCTS` | `BTC-USD,ETH-USD,SOL-USD` |
| `COINBASE_CHANNELS` | `matches,ticker,heartbeat` |

Also wrap existing sources behind the protocol: `feeds/synthetic.py` (reuse simulator
event generation), `feeds/databento.py` (move logic from
`services/databento_feed/__main__.py`, keep that module as a thin alias for one
iteration).

### 1.5 Unified ingestor service

`market_platform/services/feed_ingestor/__main__.py`:

- `FEED_SOURCE=synthetic|coinbase|databento|replay` (replay arrives in Phase 3); factory
  in `market_platform/feeds/__init__.py`.
- Publishes to `FEED_RAW_TOPIC = "feed.raw.v1"` — add to `topics.py`; feed handler
  consumes **both** `feed.raw.v1` and legacy `feed.synthetic.raw.v1` for one iteration
  (`AIOKafkaConsumer` accepts multiple topics), then the legacy topic is dropped.
- Keep `services/feed_simulator` and `services/databento_feed` entrypoints as aliases to
  the ingestor with the source pre-selected, so nothing existing breaks.

### 1.6 Compose + docs

`infra/docker-compose.yml`: add

```yaml
  coinbase-feed:
    profiles: ["coinbase"]
    build: {context: .., dockerfile: infra/python-service.Dockerfile}
    command: ["python", "-m", "market_platform.services.feed_ingestor"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: redpanda:9092
      FEED_SOURCE: coinbase
      COINBASE_PRODUCTS: ${COINBASE_PRODUCTS:-BTC-USD,ETH-USD,SOL-USD}
      EXCHANGE: COINBASE
    depends_on: [redpanda]
    restart: unless-stopped
```

README: make the Coinbase profile the documented "real data demo" (no key needed);
keep Databento section as the paid profile. Add `docs/coinbase-demo.md` modeled on
`docs/databento-demo.md`.

### 1.7 Tests

- `tests/unit/test_coinbase_adapter.py`: fixture `match`/`ticker` JSON → expected raw
  events; partial-ticker skip; fractional sizes; sequence strategy.
- `tests/unit/test_events.py`: tracker keyed by event_type; restart-threshold behavior
  preserved.

### Phase 1 gate

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m ruff check .
docker compose -f infra/docker-compose.yml --profile coinbase up --build \
  redpanda redis feed-handler stream-processor market-data-api coinbase-feed
# http://localhost:8000 shows live BTC-USD/ETH-USD; /latest/BTC-USD fresh; no false gap alerts
```

---

## Phase 2 — Ingestion Hardening (run forever)

All in `market_platform/feeds/coinbase.py` + `services/feed_ingestor/__main__.py`:

1. **Reconnect loop:** wrap the websocket session in
   `while not shutdown: try connect/consume; except → backoff`. Exponential backoff
   with jitter: `min(60, 1 * 2**attempt) * uniform(0.5, 1.5)`; reset attempt counter
   after 60s of healthy streaming. Resubscribe on reconnect.
2. **Stall watchdog:** background task; if no message for `FEED_STALL_SECONDS`
   (default 30; heartbeats arrive every ~1s so this is conservative), close the socket
   to force reconnect and emit a `quality_alert(alert_type="feed_stall",
   severity="critical")` to `market.quality.alerts.v1` so it surfaces on the dashboard.
3. **Heartbeat trade-loss check:** compare heartbeat `last_trade_id` against last
   published trade ID per product; if heartbeat is ahead, emit
   `alert_type="sequence_gap"` evidence (the SequenceTracker will also catch it on the
   next trade — this is the redundant signal).
4. **Graceful shutdown:** handle SIGTERM/SIGINT → drain queue → `producer.stop()`.
   `FEED_TIMEOUT_SECONDS` stays supported for bounded demos but defaults off.
5. **Drop accounting:** replace the silent `QueueFull` warning pattern (see
   `services/databento_feed/__main__.py:44`) with a counter; log a structured summary
   (count, window) every 10s per `observability/logging-schema.json`.
6. **Health endpoint:** minimal `aiohttp`/`asyncio` HTTP server on `:8020` —
   `/health` returns `{status, source, last_message_age_seconds, dropped_events,
   reconnects}`; 503 if last-message age > `FEED_STALL_SECONDS * 2`. Wire into a new
   `feed-ingestor` Deployment in `infra/kubernetes/base/deployments.yaml` (replace the
   `feed-simulator` Deployment; readiness/liveness probes like `market-data-api`'s).

Tests: `tests/unit/test_feed_resilience.py` with a fake source that raises/stalls —
assert backoff schedule, watchdog alert emission, drop counter, clean shutdown.

### Phase 2 gate

- `docker compose pause redpanda` 30s → unpause: ingestor recovers, no restart needed.
- Kill the network path to Coinbase (e.g., `docker network disconnect`): reconnect
  observed in logs, `feed_stall` alert visible at `http://localhost:8000`.
- 24h soak: ingestor uptime unbroken, memory flat, `/health` 200 throughout.

---

## Phase 3 — Capture & Replay (deterministic CI input)

1. `market_platform/tools/capture_feed.py`:
   `--source coinbase --seconds 30 --out tests/fixtures/coinbase-sample.jsonl --scrub`.
   `--scrub` rebases timestamps to t0 and stores `offset_ms` per line so replay can
   re-place events on the current clock.
2. `market_platform/feeds/replay.py`: `FEED_SOURCE=replay`,
   `FEED_REPLAY_FILE=...`, `FEED_REPLAY_SPEED=1.0` (0 = as fast as possible),
   `FEED_REPLAY_LOOP=true|false`. Rewrites `event_time` to now + recorded offset so
   freshness logic behaves as live.
3. Commit a ~30s scrubbed fixture (a few hundred KB) covering ≥2 products, with at
   least one synthetic trade-ID gap injected so CI exercises the alert path.
4. `tests/integration/test_replay_feed.py`: replay fixture through
   adapter→handler logic in-process; assert deterministic canonical event counts and
   exactly one `sequence_gap` alert.

Gate: `FEED_SOURCE=replay` compose run produces identical Redis end-state across two
runs (compare `/latest/BTC-USD` price/volume fields).

---

## Phase 4 — CI/CD Automation

### 4.1 PR e2e smoke (extend `.github/workflows/ci.yml`)

New job `e2e-smoke` (needs: docker):

```yaml
  e2e-smoke:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker compose -f infra/docker-compose.yml --profile replay up -d --build
             redpanda redis feed-handler stream-processor market-data-api feed-ingestor
      - run: ./scripts/wait-for-health.sh http://localhost:8000/health 120
      - run: |
          curl -fsS http://localhost:8000/latest/BTC-USD | python -c '
          import json,sys; d=json.load(sys.stdin); assert d["symbol"]=="BTC-USD"'
      - run: pip install httpx && python scripts/load-test-local.py
             --symbol BTC-USD --requests 500 --concurrency 25 --json-out perf.json
      - uses: actions/upload-artifact@v4
        with: {name: latency-report, path: perf.json}
      - if: always()
        run: docker compose -f infra/docker-compose.yml logs --tail=200 && docker compose -f infra/docker-compose.yml down -v
```

Supporting changes: `scripts/wait-for-health.sh` (poll loop), `--json-out` flag in
`scripts/load-test-local.py`, `replay` compose profile with the fixture mounted.

### 4.2 Nightly live smoke (`.github/workflows/nightly-live.yml`)

`on: schedule: cron "0 3 * * *"` + `workflow_dispatch`. Same as e2e-smoke but
`FEED_SOURCE=coinbase` (keyless — **no repo secrets required**), 120s bounded run via
`FEED_TIMEOUT_SECONDS`, asserts ≥1 trade and ≥1 quote landed in Redis. Catches upstream
API drift. On failure, opens/updates a GitHub issue (`actions/github-script`).

### 4.3 Release pipeline (`.github/workflows/deploy.yml`)

Trigger: push of tag `v*` (or manual dispatch with environment input).

1. `build-push`: build `infra/python-service.Dockerfile` and
   `infra/flink-job.Dockerfile`; tag `us-docker.pkg.dev/$PROJECT/market-data/<svc>:$TAG`
   per the mapping in `infra/images/`; auth via Workload Identity Federation
   (`google-github-actions/auth` — no long-lived JSON keys).
2. `terraform`: `init` + `plan` on `infra/terraform` (vars from GitHub environment);
   `apply` gated by a GitHub *environment protection rule* (manual approval).
3. `deploy`: `gcloud container clusters get-credentials` →
   `kustomize build infra/kubernetes/overlays/gcp | envsubst | kubectl apply -f -`
   (replace the `PROJECT_ID`/`latest` placeholders in
   `infra/kubernetes/base/deployments.yaml` with kustomize `images:` overrides keyed by
   `$TAG` — never deploy `:latest`).
4. `post-deploy-smoke`: poll the deployed `/health` and `/latest/BTC-USD`; on failure
   `kubectl rollout undo deployment -n market-data --to-revision=0` for changed
   deployments and fail the run.
5. Append `perf.json` numbers from the deployed load test to the run summary; update
   `docs/performance.md` manually from the first successful run, then link CI artifacts.

### Phase 4 gate

- A PR with a deliberate adapter bug fails `e2e-smoke`.
- Nightly run green two consecutive nights.
- One tag deploys to a dev GCP project end to end with approval gate exercised.

---

## Phase 5 — Production Runtime

### 5.1 Flink as default processor in K8s

- GCP overlay: scale Python `stream-processor` to 0; add Flink JobManager/TaskManager
  Deployments (or Flink Kubernetes Operator `FlinkDeployment` if installing the
  operator is acceptable — operator preferred for savepoint-based upgrades).
- `MarketStateJob` config: RocksDB state backend, checkpoints to
  `gs://$PROJECT-flink-checkpoints/` (add the GCS bucket +
  `google_storage_bucket_iam_member` for `market-data-workloads` SA to
  `infra/terraform/main.tf`), `restart-strategy: exponential-delay`,
  checkpoint interval 10s.
- Keep Python processor as the default for local compose (unchanged developer UX).

### 5.2 Managed broker

Smallest viable: Redpanda Cloud serverless (or Confluent basic) dev cluster;
`KAFKA_BOOTSTRAP_SERVERS` + SASL creds via Secret Manager → K8s secret (template
already in `infra/kubernetes/base/secret-template.yaml`; add
`KAFKA_SASL_USERNAME/PASSWORD/MECHANISM` keys and SASL support to the three aiokafka
constructors + Flink Kafka connector properties). Terraform: add the Secret Manager
entries to the `google_secret_manager_secret.required` set
(`infra/terraform/main.tf:61`).

### 5.3 API hardening (`market_platform/services/market_data_api/app.py`)

- Optional API-key middleware: `API_KEYS` env (comma-separated); when set, require
  `X-API-Key` on REST + first WebSocket message; 401 otherwise. Unset = open (local dev).
- Per-key token-bucket rate limit (in-process; Redis-based only if multi-replica
  fairness matters — start in-process, 2 replicas is fine for read-only data).
- `WS_MAX_CONNECTIONS` cap (default 500) and CORS allowlist via `CORS_ORIGINS` env.
- Tests in `tests/integration/test_market_data_api.py`: 401 without key, 200 with,
  429 over limit, open mode unchanged.

### 5.4 Benchmarks (closes `docs/performance.md` gaps)

On the deployed stack, record into `docs/performance.md`:

1. Ingest throughput: replay fixture at `FEED_REPLAY_SPEED=0` (max rate) ×
   N parallel ingestors; measure events/sec through `feed.raw.v1` → Redis write rate.
2. End-to-end tick-to-Redis latency: `ingest_time` vs Redis-write time histogram
   (add a one-off measurement consumer; p50/p95/p99).
3. API: `scripts/load-test-local.py` against the GKE service (in-cluster and external).
4. WebSocket fan-out: N concurrent dashboard connections × update rate.

Compare Python processor vs Flink path; this produces the before/after evidence for the
latency-reduction claim or corrects it.

### Phase 5 gate / overall Definition of Done

- Coinbase feed → GKE hot path runs 7 days unattended; alerts and dashboards reflect
  real incidents only.
- CI: PR replay smoke + nightly live smoke green; tagged release deploys with approval
  and auto-rollback.
- `docs/performance.md` contains measured (not aspirational) throughput and p95/p99.
- Databento profile still works unchanged (paid upgrade path).

---

## Risk register

| Risk | Mitigation |
| --- | --- |
| Coinbase WS message format drift | Nightly live smoke fails loudly; adapter is one pure module to patch |
| Coinbase per-IP rate limits in CI | One connection, 3 products — far below limits; replay profile is the PR-path default anyway |
| Fractional-size migration breaks a consumer | Schema 1.1 widens types only; contract tests cover both 1.0 and 1.1 events |
| Flink job semantics differ from Python fallback | `tests/integration/test_deterministic_stream_outputs.py` fixture run against both before switching the overlay |
| GCP cost creep | Autopilot + BASIC Redis + serverless broker; `terraform destroy` runbook in `docs/backup-recovery.md`; nightly smoke runs locally in CI, not against cloud |

## Suggested commit sequence

1. `contracts: widen trade/quote sizes to number (schema 1.1)`
2. `feeds: add FeedSource protocol, coinbase adapter + unit tests`
3. `services: unified feed-ingestor with FEED_SOURCE selection`
4. `infra: coinbase compose profile + docs`
5. `feeds: reconnect, stall watchdog, health endpoint`
6. `tools: feed capture + replay source + committed fixture`
7. `ci: e2e replay smoke + nightly live smoke`
8. `ci: release pipeline (build, terraform, deploy, rollback)`
9. `k8s: flink default in gcp overlay, managed broker config`
10. `api: auth, rate limiting, connection caps`
11. `docs: measured performance results`
