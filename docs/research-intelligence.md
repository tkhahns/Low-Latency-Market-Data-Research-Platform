# Research Intelligence Pipeline

The research intelligence pipeline is a **cold-path service** that fetches arXiv papers,
financial news RSS feeds, and SEC EDGAR filings, extracts structured insights via a
rule-based extractor or Claude (opt-in), and surfaces the results through the trader
dashboard, REST API, and MCP ops tools.

The **hot path is never touched**. This service lives in a separate container
(`research-ingestor`) that can crash with zero impact on market data quotes, trades,
or Redis state.

---

## Architecture

```
arXiv API ─┐
news RSS  ─┼─> research_ingestor ──> Extractor ──────────> Postgres (pgvector)
SEC EDGAR ─┘        (poll loop)     rule_based|claude           │
                                         │                      │
                                         └──> Redis digest ←────┘
                                               cache (24h TTL)
                                                   │
              ┌────────────────────────────────────┼──────────────────────┐
              ▼                                    ▼                      ▼
    market-data-api                        mcp-ops-server          trader dashboard
    GET /research/{symbol}                 symbol_research_context  research panel
    GET /research/digest                   research_search          (60s polling)
```

---

## Quick start

### Demo mode (no API keys required)

```bash
# Start the full stack with the replay fixture (rule-based extraction)
docker compose -f infra/docker-compose.yml --profile research-replay up --build

# The API will serve seeded research insights immediately
curl http://localhost:8000/research/BTC-USD | jq '.insights[].title'
curl http://localhost:8000/research/digest | jq '.insights | length'
```

### Live mode (arXiv + RSS, no LLM)

```bash
docker compose -f infra/docker-compose.yml --profile research up --build
```

`GET /research/BTC-USD` returns results after the first poll cycle (~15 minutes).

### Live mode with Claude extraction (opt-in)

```bash
ANTHROPIC_API_KEY=sk-ant-... \
RESEARCH_EXTRACTOR=auto \
docker compose -f infra/docker-compose.yml --profile research up --build
```

Insights will carry `extractor: "claude"` in the response. Daily spend is tracked in
Redis at `md:research:llm_spend:{YYYY-MM-DD}` and capped at `RESEARCH_LLM_DAILY_BUDGET_USD`.

---

## Configuration reference

| Env var | Default | Purpose |
| --- | --- | --- |
| `RESEARCH_SOURCES` | `arxiv,rss` | Comma-separated sources to enable. Add `edgar` with `RESEARCH_EDGAR_USER_AGENT` set. |
| `RESEARCH_POLL_SECONDS` | `900` | Seconds between poll cycles. |
| `RESEARCH_RSS_FEEDS` | CoinDesk,Cointelegraph,Decrypt | Comma-separated RSS feed URLs. |
| `RESEARCH_EXTRACTOR` | `auto` | `auto` (Claude if key present), `rule_based`, or `claude`. |
| `RESEARCH_LLM_MODEL` | `claude-opus-4-8` | Extraction model. Use `claude-haiku-4-5` for lower cost at higher volume. |
| `RESEARCH_LLM_DAILY_BUDGET_USD` | `1.00` | Hard daily spend cap. On exhaustion the pipeline falls back to rule-based. |
| `ANTHROPIC_API_KEY` | unset | Enables `ClaudeExtractor`. Absent = rule-based only. |
| `RESEARCH_EDGAR_USER_AGENT` | unset | Required format: `"Name contact@example.com"`. Enables the EDGAR source. |
| `RESEARCH_INGESTOR_HEALTH_PORT` | `8030` | Health server port. |
| `RESEARCH_REPLAY_FILE` | fixture path | Path to JSONL fixture for offline/CI. |
| `RESEARCH_TIMEOUT_SECONDS` | unset | Stop after N seconds (CI use). |
| `RAG_POSTGRES_DSN` | unset | If set, insights persist to Postgres. If unset, in-memory store is used. |

---

## Cost table

| Model | Input $/MTok | Output $/MTok | Cost/doc (~1.5K tokens) | 500 docs/day |
| --- | --- | --- | --- | --- |
| `claude-opus-4-8` | $5 | $25 | ~$0.009 | ~$4.50/day |
| `claude-haiku-4-5` | $1 | $5 | ~$0.002 | ~$1.00/day |
| Rule-based | $0 | $0 | $0 | $0 |

**Prompt caching** reduces input cost by ~90% across a poll batch since the system
prompt (instructions + symbol map) is cached and only the document body varies.

Effective cost at 500 docs/day with prompt caching:
- `claude-opus-4-8`: ~$0.50/day
- `claude-haiku-4-5`: ~$0.10/day

---

## REST API

### `GET /research/{symbol}`

Returns up to 10 latest insights for a symbol (Redis cache, 24h TTL).

```json
{
  "symbol": "BTC-USD",
  "insights": [
    {
      "schema_version": "1.0",
      "doc_id": "sha256:...",
      "summary": "Bitcoin spot ETF products recorded combined inflows of $1.4B...",
      "symbols": ["BTC-USD"],
      "tags": ["etf-flows", "institutional"],
      "sentiment": "bullish",
      "entities": ["BlackRock", "Fidelity"],
      "extractor": "claude",
      "source": "rss",
      "source_uri": "https://...",
      "title": "Bitcoin Spot ETF Flows Hit Record High",
      "published_time": "2026-06-05T16:00:00+00:00",
      "extracted_time": "2026-06-05T16:05:00+00:00"
    }
  ]
}
```

### `GET /research/digest`

Returns up to 20 latest insights across all symbols, newest first.

Both endpoints respect `X-API-Key` / `?api_key=` when `API_KEYS` is configured.

---

## MCP tools

### `symbol_research_context`

Combines live market metrics with the latest research insights for a symbol.

```json
{
  "name": "symbol_research_context",
  "arguments": {"symbol": "BTC-USD"}
}
```

### `research_search`

Semantic search over indexed research chunks.

```json
{"name": "research_search", "arguments": {"query": "Bitcoin ETF regulation SEC", "limit": 5}}
```

### `research_digest`

Recent cross-symbol digest filtered by time window.

```json
{"name": "research_digest", "arguments": {"hours": 48}}
```

---

## Dashboard

Each symbol card has a collapsible **Research** section showing the top 3 insights:
- Title (link to source)
- Sentiment chip (bullish/bearish/neutral)
- 2-line summary

The panel refreshes every 60 seconds by polling `/research/digest`.

---

## Ops

- **Health:** `GET http://research-ingestor:8030/health` — returns per-source status; 503 if all sources have failed.
- **Alerts:** `ResearchIngestStalled` (warning, 2h), `ResearchBudgetExhausted` (info), `ResearchSourceErrorsHigh` (warning, 1h).
- **Runbook:** `docs/runbooks/research-ingest-stalled.md`
- **Metrics:** `research_docs_ingested_total`, `research_insights_extracted_total`, `research_llm_spend_usd_total`, `research_ingest_lag_seconds`

## Offline / CI

All tests run without any API key or network access. The replay source reads
`market_platform/fixtures/research-sample.jsonl` (12 scrubbed documents covering arXiv,
RSS, and EDGAR shapes). The `RESEARCH_TIMEOUT_SECONDS` env var bounds CI runs.
