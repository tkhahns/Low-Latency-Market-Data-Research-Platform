# Research Intelligence Integration Plan (Iteration 7)

**Goal:** add a QuantMind-style research-knowledge pipeline — arXiv papers, financial news,
SEC filings → LLM-extracted structured insights → retrieval via MCP tools, REST, and a
dashboard panel — as a **cold-path service** that never touches the hot path.

**Decision (D1):** borrow QuantMind's architecture (two-stage: extraction → retrieval), not
the dependency. QuantMind is early-stage (abstract storage, core modules still in PRs) and
OpenAI-coupled; we already own the landing pad it would need (pgvector `EvidenceStore`,
MCP ops server, audit log). We implement the same shape natively in `market_platform`.

**Non-goals:** no market-data changes, no new prediction features, no change to feed
ingestion, stream processing, or the Redis hot-cache write path.

---

## 1. Hard boundary: hot path is untouched

The platform's identity is low latency. The research pipeline is batch/poll work measured
in seconds per document. Invariants, enforced by design and asserted in review:

| Invariant | How it's guaranteed |
| --- | --- |
| No research code imports in feed/stream services | New code lives in `market_platform/research/` + one new service; feed/stream modules unmodified |
| API stays fast | `/research/*` endpoints read **only Redis-cached digests** (same pattern as `/latest`); Postgres/LLM never in the request path |
| Failure isolation | research-ingestor is a separate container/Deployment; it can crash with zero impact on quotes/trades |
| Cost isolation | LLM spend is capped by a daily budget; on exhaustion the pipeline degrades to rule-based extraction, never stops market data |

```
arXiv API ─┐
news RSS  ─┼─> research_ingestor ──> Extractor (rule-based | Claude) ──> Postgres (pgvector)
SEC EDGAR ─┘        (poll loop)              │                              │
                                             └──> Redis digest cache  <────┘ (writer side)
                                                       │
                  ┌────────────────────────────────────┼──────────────────────┐
                  ▼                                    ▼                      ▼
        market-data-api                        mcp-ops-server          trader dashboard
        GET /research/{symbol}                 research_search          research panel
        GET /research/search                   symbol_research_context  (60s polling)
```

---

## 2. What exists already (reuse, don't rebuild)

| Existing asset | Role in this plan |
| --- | --- |
| `market_platform/ops/vector_store.py` — `EvidenceStore` protocol, `InMemoryVectorStore`, `JsonVectorStore`, `PostgresVectorStore` (pgvector `rag_documents`) | Storage backend for research chunks; selected via `RAG_POSTGRES_DSN` / `RAG_JSON_STORE`, same as today |
| `market_platform/ops/documents.py` — `DocumentChunk` | Chunk shape; research docs become chunks with `source_type="research"` and symbol tags |
| `market_platform/ops/embedding.py` — `deterministic_embedding` | Default embedding (deterministic, free, CI-safe); provider upgrade is Phase 2b |
| `mcp_ops_server` — `/tools`, `/mcp` JSON-RPC, `OpsTools`, `JsonlAuditLog` | New research tools register here; audit logging comes for free |
| `market_platform/feeds/` pattern (Protocol + factory + replay + fixtures) | Mirrored as `ResearchSource` protocol with replay fixture for CI |
| `redis_keys.py`, `_check_auth`/rate limiting in `market_data_api` | Digest cache keys + API hardening reuse |
| Terraform Cloud SQL Postgres + `RAG_POSTGRES_DSN` secret | Same database, two new tables |

---

## 3. Data model

### 3.1 Contracts (new, `contracts/research/`)

`research.document.v1.schema.json` — a raw fetched document:

```json
{
  "schema_version": "1.0",
  "doc_id": "sha256:…",                  // content hash → idempotent dedup
  "source": "arxiv | rss | edgar",
  "source_uri": "https://arxiv.org/abs/…",
  "title": "…",
  "body": "…",                            // plain text, parser-cleaned
  "published_time": "2026-06-11T03:00:00Z",
  "fetched_time": "2026-06-11T03:05:00Z"
}
```

`research.insight.v1.schema.json` — the extracted record:

```json
{
  "schema_version": "1.0",
  "doc_id": "sha256:…",
  "summary": "2–3 sentence summary",
  "symbols": ["BTC-USD", "ETH-USD"],     // watchlist symbols only
  "tags": ["regulation", "etf-flows"],
  "sentiment": "bullish | bearish | neutral",
  "entities": ["SEC", "BlackRock"],
  "extractor": "rule_based | claude",
  "source_uri": "…", "title": "…", "published_time": "…", "extracted_time": "…"
}
```

### 3.2 Postgres (extends the existing RAG database)

```sql
CREATE TABLE IF NOT EXISTS research_insights (
  doc_id TEXT PRIMARY KEY,
  summary TEXT NOT NULL,
  symbols TEXT[] NOT NULL DEFAULT '{}',
  tags TEXT[] NOT NULL DEFAULT '{}',
  sentiment TEXT,
  entities TEXT[] NOT NULL DEFAULT '{}',
  extractor TEXT NOT NULL,
  source TEXT NOT NULL,
  source_uri TEXT NOT NULL,
  title TEXT NOT NULL,
  published_time TIMESTAMPTZ NOT NULL,
  extracted_time TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS research_insights_symbols_idx ON research_insights USING GIN (symbols);

CREATE TABLE IF NOT EXISTS research_cursors (
  source TEXT PRIMARY KEY,               -- arxiv | rss:<feed-url-hash> | edgar
  cursor TEXT NOT NULL,                  -- last seen id / timestamp, source-specific
  updated_at TIMESTAMPTZ NOT NULL
);
```

Semantic search reuses `rag_documents` unchanged: each document body is chunked via
`iter_markdown_chunks`-style splitting and upserted with `source_type="research"` and
tags `["symbol:BTC-USD", "source:arxiv", "doc:sha256:…"]`. The local no-Postgres path
(`RAG_JSON_STORE` / in-memory) keeps working because insights also flow to the Redis
digest cache, which is the only thing the API reads.

### 3.3 Redis digest cache (the only API-visible store)

| Key (add to `redis_keys.py`) | Value |
| --- | --- |
| `md:research:{SYMBOL}` | JSON list (≤10) of latest insights for that symbol, newest first |
| `md:research:digest` | JSON list (≤20) of latest insights across all symbols |

Written by the ingestor after each batch; TTL 24 h (stale research self-evicts). The API
never queries Postgres, so `/research/{symbol}` has the same latency profile as `/latest/…`
and even works in Vercel demo mode (DemoRedis gets 2–3 seeded sample insights).

---

## 4. Sources (all free / keyless)

| Source | Module | Mechanism | Notes |
| --- | --- | --- | --- |
| arXiv | `research/sources/arxiv.py` | Atom API `http://export.arxiv.org/api/query`, categories `q-fin.*` + `cs.CE`, sorted by `submittedDate`, cursor = last submitted timestamp | No key. Be polite: 1 request / 3 s, identify via User-Agent |
| Crypto news RSS | `research/sources/rss.py` | `feedparser` over configurable feed list (default: CoinDesk, Cointelegraph, Decrypt RSS) | No key. Matches the BTC/ETH/SOL watchlist; equities feeds can be added via env later |
| SEC EDGAR | `research/sources/edgar.py` | Full-text search JSON API + filings Atom feed; cursor = last accession number | No key but **requires** a descriptive `User-Agent` with contact email; ≤10 req/s hard limit — we poll once per cycle |

All sources implement one protocol (mirrors `feeds/base.py`):

```python
class ResearchSource(Protocol):
    name: str
    async def fetch_since(self, cursor: Optional[str]) -> tuple[list[ResearchDocument], Optional[str]]: ...
```

Plus `research/sources/replay.py` reading `market_platform/fixtures/research-sample.jsonl`
(committed, scrubbed, ~12 documents covering all three source shapes) so CI and offline
demos are deterministic — the exact trick that made the Coinbase e2e job reliable.

**Symbol tagging** is watchlist-driven keyword matching defined in
`research/symbol_map.py` (`BTC-USD: [bitcoin, btc, ₿…]`, `ETH-USD: […]`, `SOL-USD: […]`),
overridable via `RESEARCH_SYMBOL_MAP` (JSON env). The rule-based extractor uses it
directly; the LLM extractor receives it in the prompt and may only emit symbols from it
(schema-enforced enum), which kills hallucinated tickers.

---

## 5. Extraction

```python
class Extractor(Protocol):
    async def extract(self, doc: ResearchDocument) -> ResearchInsight: ...
```

### 5.1 `RuleBasedExtractor` (default, free, CI-safe)

First ~2 sentences as summary, keyword symbol/tag matching from `symbol_map`, sentiment
from a small lexicon, entities from a capitalized-phrase heuristic. Deterministic →
unit-testable with exact assertions, and the permanent fallback when no API key or budget
is exhausted. The pipeline is **useful without any LLM**.

### 5.2 `ClaudeExtractor` (opt-in via `ANTHROPIC_API_KEY`)

Official `anthropic` SDK, structured outputs via `client.messages.parse()` with a Pydantic
`ResearchInsight` model — guarantees schema-valid JSON, no parsing failures.

- **Model:** `RESEARCH_LLM_MODEL` env, default `claude-opus-4-8` ($5/$25 per MTok).
  For high-volume runs the operator can set `claude-haiku-4-5` ($1/$5 per MTok) — at
  ~1.5 K tokens/document that's ~$0.002/doc, so even 500 docs/day ≈ $1/day. The model
  choice is an explicit operator knob, not hard-coded.
- **Prompt caching:** static system prompt (instructions + symbol map) carries
  `cache_control: {"type": "ephemeral"}`; only the document body varies per call → ~90 %
  input-cost reduction across a polling batch.
- **Batch mode:** nightly backfills (>50 pending docs) go through the Message Batches API
  (50 % price, ≤24 h turnaround) — perfect fit since backfill is not latency-sensitive.
- **Cost guardrails:** every response's `usage` is converted to USD and accumulated in
  Redis (`md:research:llm_spend:{YYYY-MM-DD}`); when it crosses
  `RESEARCH_LLM_DAILY_BUDGET_USD` (default 1.00) the ingestor logs a `budget_exhausted`
  event and switches to `RuleBasedExtractor` until midnight UTC. Content-hash result
  caching in Postgres prevents ever paying twice for the same document.
- **Resilience:** SDK retries handle 429/5xx; any extractor exception falls back to
  rule-based for that document — a Claude outage degrades quality, never availability.

### 5.3 Embeddings

Keep `deterministic_embedding` as the default (free, deterministic, already powers the
Obsidian RAG). Add `research/embeddings.py` with an `EmbeddingProvider` protocol so a real
provider (e.g. Voyage) can be slotted in later via `EMBEDDING_PROVIDER` — explicitly
deferred (Phase 2b, optional) since digest retrieval is tag-based and doesn't need it.

---

## 6. Service: `research-ingestor`

`market_platform/services/research_ingestor/__main__.py`, structured like
`feed_ingestor/__main__.py`:

1. Every `RESEARCH_POLL_SECONDS` (default 900): for each enabled source, load cursor →
   `fetch_since` → dedupe by `doc_id` against `research_insights` → extract → upsert
   insight row + `rag_documents` chunks → refresh Redis digests → save cursor.
2. Raw asyncio health server on `:8030` (same code shape as the feed ingestor's): JSON
   status with per-source last-success timestamps; 503 if **all** sources have failed for
   > 3 cycles.
3. Jittered per-source backoff on errors; SIGTERM drains the in-flight batch;
   `RESEARCH_TIMEOUT_SECONDS` for bounded CI runs.
4. Structured logs match `observability/logging-schema.json`.

Kafka is deliberately **not** in the v1 path (decision D2): volume is ~10²–10³ docs/day,
three orders of magnitude below the feed path; Postgres upsert + content-hash dedup gives
replayability. If fan-out consumers appear later, the ingestor gains an optional publish
to `research.docs.raw.v1` without breaking anything.

---

## 7. Retrieval surfaces

### 7.1 REST (`market_data_api/app.py`)

| Endpoint | Behavior |
| --- | --- |
| `GET /research/{symbol}` | `_check_auth` → return `md:research:{SYMBOL}` (or `[]`) |
| `GET /research/digest` | `_check_auth` → return `md:research:digest` |

Redis-only, auth + token-bucket rate limiting inherited from `_check_auth`. DemoRedis
seeds sample insights so demo mode / Vercel shows the panel populated.

### 7.2 MCP tools (`mcp_ops_server` + `ops/tools.py`)

| Tool | Parameters | Returns |
| --- | --- | --- |
| `research_search` | `query`, `limit` | semantic search over `EvidenceStore`, filtered to `source_type="research"`, with source citations |
| `symbol_research_context` | `symbol` | latest insights for a symbol **joined with** current freshness/metrics from Redis — the "why did NQ move?"-style tool combining both planes |
| `research_digest` | `hours` | recent cross-symbol digest |

All registered in `list_tools()`, dispatched through `OpsTools.call` → audited in the
existing JSONL audit log. `docs/mcp-examples.md` gains a worked example per tool.

### 7.3 Dashboard

`apps/trader-dashboard/static/`: each symbol card gets a collapsible "Research" section —
top-3 insights (title link, summary, sentiment chip, age). One fetch of
`/research/digest` every 60 s (forwarding `?api_key=` like the live polling does), grouped
client-side by symbol. Zero impact on the 1 s live polling loop.

---

## 8. Configuration (all in `market_platform/config.py`)

| Env var | Default | Purpose |
| --- | --- | --- |
| `RESEARCH_SOURCES` | `arxiv,rss` | Enabled sources (`edgar` opt-in because of its UA requirement) |
| `RESEARCH_POLL_SECONDS` | `900` | Poll cadence |
| `RESEARCH_RSS_FEEDS` | CoinDesk/Cointelegraph/Decrypt URLs | Comma-separated feed list |
| `RESEARCH_SYMBOL_MAP` | built-in BTC/ETH/SOL map | JSON override |
| `RESEARCH_EXTRACTOR` | `auto` | `auto` (Claude if key present) \| `rule_based` \| `claude` |
| `RESEARCH_LLM_MODEL` | `claude-opus-4-8` | Extraction model (`claude-haiku-4-5` for high-volume/cheap) |
| `RESEARCH_LLM_DAILY_BUDGET_USD` | `1.00` | Hard daily spend cap |
| `ANTHROPIC_API_KEY` | unset | Enables `ClaudeExtractor`; absent = rule-based only |
| `RESEARCH_EDGAR_USER_AGENT` | unset | Required to enable `edgar` (`name email` format) |
| `RESEARCH_INGESTOR_HEALTH_PORT` | `8030` | Health endpoint |
| `RESEARCH_REPLAY_FILE` / `RESEARCH_TIMEOUT_SECONDS` | fixture path / unset | CI determinism |

Dependencies: `pyproject.toml` optional extra `research = ["anthropic>=0.92.0", "feedparser>=6.0.0", "httpx>=0.27.0"]`,
installed in `infra/python-service.Dockerfile`. **Not** added to the Vercel
`requirements.txt` — the API only reads Redis, so the serverless bundle stays slim
(lesson from the Vercel size incident).

---

## 9. Ops & infra

- **Compose:** `research-ingestor` service under profile `research` (live) and
  `research-replay` under profile `replay` (fixture-driven, used by CI).
- **Kubernetes:** `research-ingestor` Deployment (1 replica, liveness/readiness on
  `:8030`, secretRef for `ANTHROPIC_API_KEY` + `RAG_POSTGRES_DSN`); entries in
  `services.yaml` + `kustomization.yaml`; expected-set updates in
  `tests/unit/test_production_artifacts.py` and `scripts/validate-production-artifacts.py`.
- **Terraform:** Secret Manager entries `ANTHROPIC_API_KEY`, `RESEARCH_EDGAR_USER_AGENT`.
- **Metrics** (`observability/metrics.yml`): `research_docs_ingested_total{source}`,
  `research_insights_extracted_total{extractor}`, `research_source_errors_total{source}`,
  `research_llm_spend_usd_total`, `research_ingest_lag_seconds`.
- **Alerts:** `ResearchIngestStalled` (no successful cycle in 2 h), `ResearchBudgetExhausted`
  (info). Runbook: `docs/runbooks/research-ingest-stalled.md`.
- **Grafana:** one new row (ingest rate, errors by source, LLM spend vs budget, lag).
- **CI:** unit tests run in the normal `python` job (rule-based extractor + replay source
  → fully offline). The `e2e-smoke` job adds: bring up `research-replay` + redis + api →
  assert `GET /research/BTC-USD` returns ≥1 insight. **Nightly live** job adds a 120 s
  keyless arXiv+RSS pull (no LLM) asserting ≥1 document ingested, filing a GH issue on
  failure like the Coinbase nightly.

---

## 10. Phases, files, and gates

### Phase 0 — Contracts & scaffolding (no behavior change)
`contracts/research/*.schema.json`, `config.py` additions, `redis_keys.py:research_*`,
`pyproject.toml` extra, `docs/decisions/research-intelligence.md` (records D1/D2).
**Gate:** suite green; `pip install -e '.[dev,research]'` clean.

### Phase 1 — Sources + fixture
`market_platform/research/{__init__,models,symbol_map}.py`,
`research/sources/{base,arxiv,rss,edgar,replay}.py`,
`market_platform/fixtures/research-sample.jsonl`,
`market_platform/tools/capture_research.py` (fixture refresh tool),
`tests/unit/test_research_sources.py` (parser tests on canned payloads, cursor logic,
dedup hashing — no network).
**Gate:** unit tests; manual `python -m market_platform.tools.capture_research --source arxiv --limit 5` pulls real docs.

### Phase 2 — Extraction
`research/extract.py` (protocol + `RuleBasedExtractor` + `ClaudeExtractor`),
`research/embeddings.py`, budget tracker, `tests/unit/test_research_extract.py`
(rule-based exact assertions; Claude path tested with a stubbed client — no API key in CI).
**Gate:** unit tests; with a real key, one manual extraction produces a schema-valid insight.

### Phase 3 — Store + service
`research/store.py` (DDL bootstrap, insight upsert, chunk upsert into `EvidenceStore`,
digest writer, cursor persistence; in-memory variant for tests),
`services/research_ingestor/__main__.py`, compose services, health server.
**Gate:** `--profile replay` run populates `md:research:BTC-USD` from the fixture end-to-end.

### Phase 4 — Surfaces
API endpoints + DemoRedis seeds + auth tests, MCP tools + audit + `docs/mcp-examples.md`,
dashboard research panel, `tests/integration/test_market_data_api.py` +
`tests/unit/test_ops_tools.py` extensions.
**Gate:** suite green; dashboard shows seeded research in demo mode (including on Vercel).

### Phase 5 — Ops
K8s manifests, Terraform secrets, metrics/alerts/Grafana/runbook, CI e2e extension,
nightly live extension, artifact-validation updates.
**Gate:** `production-artifacts-ok`; e2e-smoke green including the research assertion.

### Phase 6 — Docs & soak
`docs/research-intelligence.md` (quick start, config, cost table, architecture),
README section, roadmap update. 24 h live soak with `RESEARCH_EXTRACTOR=auto` and a real
key at `claude-haiku-4-5` to record actual daily cost in the doc.
**Gate:** soak shows zero hot-path regression (feed latency dashboards unchanged) and
spend within budget.

Suggested commit sequence: one commit per phase (6 commits), same convention as iteration 6.

---

## 11. Risk register

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| LLM cost runaway | Medium | $$ | Daily budget cap + per-doc content-hash cache + rule-based fallback + spend metric/alert |
| Hallucinated symbols/sentiment | Medium | Misleading panel | Symbols schema-constrained to watchlist enum; sentiment enum; `extractor` field shown in UI so users see provenance |
| Source ToS / rate limits (EDGAR UA, arXiv politeness) | Medium | Source ban | EDGAR opt-in with mandatory UA env; 1 req/3 s arXiv throttle; per-source backoff |
| RSS feed churn / dead feeds | High | Silent staleness | Per-source health in `/health`, `ResearchIngestStalled` alert, feeds configurable via env |
| Deterministic embeddings give weak semantic search | High | `research_search` quality | Acceptable v1 (digest/tag retrieval is primary); provider abstraction ready (Phase 2b) |
| Scope creep into hot path | Low | Identity dilution | Section 1 invariants; review checklist item: "does this diff touch feeds/stream/handler? then reject" |
| Postgres unavailable locally | Medium | Dev friction | Store has JSON/in-memory variant; digest cache means API works regardless |

---

## 12. Definition of done

1. `docker compose --profile research up` ingests live arXiv+RSS within one poll cycle,
   and the dashboard research panel populates for BTC/ETH/SOL.
2. CI proves the offline path: replay fixture → ingestor → Redis → `GET /research/BTC-USD`
   in the e2e-smoke job.
3. With `ANTHROPIC_API_KEY` set, insights carry `extractor: "claude"`, spend is visible in
   metrics, and the budget switch was exercised in a test.
4. MCP `symbol_research_context` answers with both market metrics and research citations,
   logged in the audit trail.
5. Measured 24 h cost recorded in `docs/research-intelligence.md`; hot-path latency
   dashboards unchanged throughout the soak.
