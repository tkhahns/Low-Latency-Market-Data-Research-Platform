# Decision: Research Intelligence Integration

**Date:** 2026-06-11  
**Status:** Accepted

## Context

The project evaluated [QuantMind](https://github.com/LLMQuant/quant-mind) as a potential Bloomberg-terminal-equivalent data source. QuantMind is a research-knowledge pipeline (not a market data connector): it fetches academic papers, financial filings, and news, extracts structured insights via LLM, and exposes them for retrieval-augmented generation.

## Decision D1 — Borrow the architecture, not the dependency

We implement the same two-stage pipeline (source fetch → LLM extraction → retrieval) natively in `market_platform.research` rather than importing QuantMind as a library.

**Rationale:**
- QuantMind is early-stage: storage layer is abstract stubs, core modules are still in open PRs
- QuantMind is OpenAI-coupled; we want the official Anthropic SDK with prompt caching and structured outputs
- We already own the landing pad QuantMind would need: pgvector `EvidenceStore`, MCP ops server with audit log, `DocumentChunk` data model
- A native implementation has no additional transitive dependency burden and can be evolved without an upstream constraint

**Rejected alternative:** `pip install quant-mind` as a dependency — too unstable, wrong LLM vendor coupling.

## Decision D2 — No Kafka in v1

Research documents do not go through Kafka/Redpanda in the initial implementation.

**Rationale:**
- Volume is ~10²–10³ documents/day, three orders of magnitude below the market data feed path
- Postgres upsert with SHA-256 content-hash dedup gives full replayability
- Adding a Kafka topic is an isolated future change if fan-out consumers appear

**Rejected alternative:** Publish raw documents to a `research.docs.raw.v1` topic — deferred to v2.

## Decision D3 — Dual extractor with rule-based default

The pipeline ships with two extractors implementing the same `Extractor` protocol:
- `RuleBasedExtractor`: deterministic, free, CI-safe, permanent fallback
- `ClaudeExtractor`: opt-in via `ANTHROPIC_API_KEY`, structured outputs via `client.messages.parse()`

The pipeline is fully useful without any LLM. Claude improves quality but is never required.

## Hard boundary

Research code lives entirely in `market_platform/research/` and `services/research_ingestor/`. Feed, stream, and market-data-api modules are unmodified. The API reads only Redis digest cache keys, never Postgres or LLM, in the request path.
