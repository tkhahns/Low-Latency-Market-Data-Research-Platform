# Iteration 6 Plan: Real Free Data Source + Production Automation

This plan covers the next iteration: replacing the paid Databento dependency with a
genuinely free real-time data source, automating ingestion end to end, and closing the
remaining production-readiness gates.

## 1. Current Limits

| # | Limit | Evidence | Impact |
| --- | --- | --- | --- |
| 1 | Real data source requires a paid provider | `services/databento_feed` needs `DATABENTO_API_KEY`; free credits are one-time and metered | Default demo runs on synthetic data; the "real feed" path cannot run unattended or in CI |
| 2 | Ingestion is demo-shaped, not service-shaped | `DATABENTO_TIMEOUT_SECONDS` stops the stream on a timer; no reconnect/backoff; queue overflow silently drops events | Cannot run continuously in production |
| 3 | Default stream processor is the Python fallback | Flink job exists but is opt-in via `--profile flink` | Resume-level claims (stateful Flink, checkpointing) are not the default path |
| 4 | Performance claims unproven | `docs/performance.md`: 50k+ events/sec and sub-100ms p95 not yet measured on a real deployment | Cannot defend the numbers |
| 5 | Nothing is deployed | Terraform/K8s are scaffolding; no managed Redis/Postgres/Kafka/GKE/Databricks provisioned | "Production-ready" is artifact-level, not runtime-level |
| 6 | No schema registry, JSON-only serde | `market_platform/serde.py`; contracts enforced only by tests | Schema drift between producers/consumers is possible |
| 7 | API has no authentication or rate limiting | `market_platform/services/market_data_api/app.py` | Cannot be exposed publicly |
| 8 | Single-node infra assumptions | One Redpanda broker, one Redis, no HA, local Flink checkpoints | No fault tolerance story at runtime |

Limit #1 is the gating one: until a free, unattended-capable real feed exists, every
downstream automation (CI smoke tests with real data, continuous deployment, real
benchmarks) stays blocked.

## 2. Data Source Decision

### Candidates evaluated (free tiers, 2026)

| Source | Cost | Asset class | Transport | Key needed | Limits | Sequence numbers |
| --- | --- | --- | --- | --- | --- | --- |
| Coinbase Exchange WS (`wss://ws-feed.exchange.coinbase.com`) | Free | Crypto | WebSocket: `ticker`, `matches`, `level2_batch` | No | Per-IP connection/rate limits, generous | Yes (per-product `sequence`) |
| Binance public WS (`data-stream.binance.vision`) | Free | Crypto | WebSocket: trades, bookTicker, depth | No | 300 connections/5min/IP class limits | Update IDs |
| Alpaca Basic plan | Free account | US equities | WebSocket (IEX feed) + REST | Yes (free signup) | IEX-only, ~30 symbols/stream, market hours | No exchange seq |
| Finnhub free | Free account | US equities | WebSocket trades only | Yes | 50 symbols, trades only, 60 REST calls/min | No |
| Polygon free | Free account | US equities | REST only | Yes | 5 calls/min, 15-min delayed, no WS | n/a |
| Databento (current) | Paid after intro credits | Futures/equities | Live gateway | Yes | Metered by data volume | Yes |

### Decision

**Primary real feed: Coinbase Exchange WebSocket.** Rationale:

- No API key, no account, no quota — the only candidate that can run in CI and
  unattended demos with zero secrets and zero cost.
- 24/7 market — demos and benchmarks work at any hour (equities feeds are dead
  outside US market hours, which breaks scheduled CI).
- Real trades (`matches` channel) and real top-of-book (`ticker` channel) map directly
  onto the existing canonical `trade`/`quote` event contracts.
- Per-product `sequence` numbers exercise the platform's sequence-gap detection — the
  feature Databento was demonstrating.
- High natural message rates on BTC-USD/ETH-USD provide meaningful load.

**Secondary (optional profile): Alpaca Basic** for a US-equities demo. Free signup,
real-time IEX websocket. Kept behind a compose profile like Databento today, since it
needs a key and only works during market hours.

**Keep the Databento adapter** as a third profile — it is the production-grade upgrade
path when paid data is justified.

## 3. Workstream A: Pluggable Feed Source Layer

Refactor ingestion so the data source is configuration, not code.

1. Create `market_platform/feeds/` package:
   - `base.py`: `FeedSource` protocol — `async def stream(self) -> AsyncIterator[dict]`
     yielding raw events in the existing `RAW_REQUIRED_FIELDS` shape
     (`event_type`, `symbol`, `exchange`, `event_time`, `sequence_number`, payload fields).
   - `synthetic.py`: wrap the existing simulator logic.
   - `coinbase.py`: subscribe to `matches` + `ticker` for configured products
     (default `BTC-USD,ETH-USD,SOL-USD`); map `match` → trade event, `ticker` →
     quote event; carry Coinbase `sequence` into `sequence_number`; `exchange="COINBASE"`.
   - `alpaca.py` (stretch): IEX trades/quotes websocket.
   - `databento.py`: move the existing adapter behind the same protocol.
2. Single runner service `market_platform/services/feed_ingestor/__main__.py`:
   selects the source via `FEED_SOURCE=synthetic|coinbase|alpaca|databento`, publishes
   to the raw input topic. Rename topic constant `SYNTHETIC_RAW_TOPIC` usage to a
   source-neutral `FEED_RAW_TOPIC` (wire format `feed.raw.v1`, keep the old name as an
   alias for one iteration).
3. Compose: `coinbase` profile that needs no secrets; make it the documented
   "real data demo" default in the README.
4. Unit tests: fixture JSON messages for Coinbase `match`/`ticker` → expected canonical
   events (mirror `tests/unit/test_databento_adapter.py`).

Acceptance: `FEED_SOURCE=coinbase docker compose ... up` drives the dashboard with live
crypto data, no API key, and sequence-gap alerts fire on real gaps.

## 4. Workstream B: Ingestion Hardening (run forever, unattended)

1. Reconnect loop with exponential backoff + jitter (1s → 60s cap) around the websocket;
   resubscribe on reconnect; reset `SequenceTracker` expectations via the existing
   restart-threshold mechanism.
2. Heartbeat watchdog: Coinbase sends `heartbeat` channel messages; if no message for
   `FEED_STALL_SECONDS` (default 30), force reconnect and emit a
   `market.quality.alerts.v1` alert (`alert_type=feed_stall`).
3. Replace the fixed `*_TIMEOUT_SECONDS` demo timer with graceful SIGTERM handling;
   timeout stays available but optional.
4. Backpressure: on queue full, increment a dropped-events counter metric instead of
   only logging; expose via the structured log schema in `observability/`.
5. Liveness/readiness: tiny HTTP health endpoint on the ingestor (last-message age),
   wired into the existing K8s deployment probes.

Acceptance: kill the network or the broker mid-stream; ingestor reconnects, alerts are
visible on the dashboard, no manual restart needed over a 24h soak run.

## 5. Workstream C: Capture & Replay for Deterministic CI

Live feeds are non-deterministic; CI needs both a "real-network" smoke and a
deterministic path.

1. `market_platform/tools/capture_feed.py`: record N seconds of a live source to a
   JSONL fixture (`tests/fixtures/coinbase-sample.jsonl`), with a `--scrub` pass that
   truncates/normalizes timestamps.
2. `replay` feed source (`FEED_SOURCE=replay FEED_REPLAY_FILE=...`): replays a fixture
   at recorded or accelerated pace — this also doubles as the disaster-recovery replay
   demo.
3. Commit a small (~30s) scrubbed fixture for CI.

## 6. Workstream D: CI/CD Automation

Extend `.github/workflows/ci.yml` and add a deploy workflow:

1. **PR pipeline (exists, extend):** lint, unit, integration, artifact validation, plus a
   new `e2e-smoke` job: `docker compose up` with `FEED_SOURCE=replay` (deterministic
   fixture), poll `/health` and `/latest/BTC-USD`, assert freshness keys in Redis, run
   `scripts/load-test-local.py`, upload the latency report as an artifact.
2. **Nightly live smoke:** same job with `FEED_SOURCE=coinbase` against the real
   websocket (keyless, so it needs no repository secrets) — catches upstream API drift.
3. **Release pipeline (new `deploy.yml`):** on tag/main merge — build and push images to
   Artifact Registry, `terraform plan` (apply gated on environment approval),
   `kustomize build infra/kubernetes/overlays/gcp | kubectl apply`, post-deploy smoke
   against the deployed `/health`, automatic rollback on smoke failure
   (`kubectl rollout undo`).
4. Publish the benchmark numbers from each nightly run into `docs/performance.md`
   (or a CI artifact) so the performance claims become continuously measured.

## 7. Workstream E: Production Runtime Gates

Ordered by dependency:

1. Make Flink the default processor in the K8s overlay (Python fallback stays for
   local-light mode); enable RocksDB state backend + checkpoint to GCS.
2. Provision via existing Terraform: GKE Autopilot, Memorystore (Redis), Cloud SQL
   (Postgres + pgvector), managed Kafka-compatible broker (Confluent/Redpanda Cloud
   free-tier dev cluster to start).
3. Add API hardening: API-key middleware + per-key rate limit on the FastAPI service,
   CORS allowlist, WebSocket connection cap.
4. Run the real benchmark matrix on the deployed stack; record throughput, p95/p99,
   tick-to-dashboard latency in `docs/performance.md` — replacing "not yet proven".
5. Schema governance (stretch): add JSON-schema validation at the feed-handler boundary
   using the contracts in `contracts/events/` (already versioned) before investing in a
   full registry.

## 8. Sequencing & Definition of Done

Week-level sequencing (each step leaves `main` releasable):

1. A (adapter layer + Coinbase) → unblocks everything else.
2. B (hardening) + C (capture/replay) in parallel.
3. D (CI/CD) — replay smoke first, then nightly live smoke, then deploy workflow.
4. E (cloud + benchmarks) — last, because it now has real data and automated deploys.

Done when:

- `FEED_SOURCE=coinbase` runs the full hot path with zero secrets and survives a 24h soak.
- CI proves the pipeline end to end on every PR without external accounts.
- One tagged release deploys to GCP automatically and reports measured p95/p99 from
  real market data.
- Databento remains available as a paid upgrade profile, unchanged.
