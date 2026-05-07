# Architecture

## Design Goals

- Serve live quote and trade state with low latency.
- Preserve durable market event history for replay and research.
- Keep Databricks out of the latency-sensitive serving path.
- Make every data contract explicit and versioned.
- Provide AI-assisted operations through controlled MCP tools, not open-ended production access.

## Hot Path

```text
Market Feed / Simulator
  -> Feed Handler
  -> Kafka
  -> Flink
  -> Redis
  -> WebSocket / REST API
  -> Trader Dashboard
```

### Responsibilities

| Component | Responsibility |
| --- | --- |
| Feed Simulator | Produces synthetic trades, quotes, order-book changes, halts, imbalances, and sequence gaps. |
| Feed Handler | Parses raw feed messages, validates sequence numbers, timestamps ingress, and emits canonical events. |
| Kafka | Durable streaming backbone for decoupling, buffering, replay, and fan-out. |
| Flink | Stateful stream processing for market state, rolling windows, and anomaly detection. |
| Redis | Hot cache for latest quote, top-of-book, bars, freshness, and active alerts. |
| Market Data API | Trader-facing WebSocket and REST surface backed by Redis. |
| Trader Dashboard | Live market state, latency, health, and alert visualization. |

### Local POC Ownership

The local step-1 POC uses Python services for every hot-path boundary so it can run on a laptop with Docker Compose. Iteration 2 adds a Flink job as the stateful streaming owner while preserving Kafka topics, Redis keys, and API contracts. The Python processor remains as a lightweight fallback for machines that cannot run the Flink profile.

| Service | Input | Output |
| --- | --- | --- |
| `market_platform.services.feed_simulator` | Synthetic generator config | `feed.synthetic.raw.v1` |
| `market_platform.services.feed_handler` | `feed.synthetic.raw.v1` | `market.raw.v1`, `market.trades.v1`, `market.quotes.v1`, `market.quality.alerts.v1` |
| `market_platform.services.stream_processor` | Trades, quotes, alerts | Redis hot keys, derived Kafka topics |
| `market_platform.services.market_data_api` | Redis hot keys | REST, WebSocket, static dashboard |

### MVP Stateful Streaming

The Flink job under `services/stream-processor/flink` owns:

- top-of-book, spread, and abnormal spread alerts
- rolling 1-second OHLCV bars and VWAP
- rolling volume, VWAP, and volatility metrics
- Redis hot-state writes with stable TTLs
- checkpointing and fixed-delay restart behavior

Run the Flink profile with `./scripts/run-mvp-flink.sh`.

## Cold Path

```text
Kafka / Raw Archive
  -> Databricks Delta Bronze
  -> Delta Silver
  -> Delta Gold
  -> Research Queries / Backtests
```

### Table Zones

| Zone | Purpose |
| --- | --- |
| Bronze | Append-only raw canonical events with minimal transformation. |
| Silver | Cleaned, deduplicated, sequence-aware trades and quotes. |
| Gold | Research-ready bars, spread features, volatility features, and quality annotations. |

Table contracts are documented in `lakehouse/README.md`. All zones preserve lineage metadata where applicable: source topic, partition, offset, schema version, job run ID, and processing timestamp.

## Agentic Ops Path

```text
Docs + lineage + metrics + incidents
  -> RAG corpus
  -> MCP Ops Server
  -> controlled diagnostics and replay tools
```

### Initial MCP Tools

- `check_symbol_freshness`
- `explain_sequence_gap`
- `run_replay_dry_run`
- `compare_live_vs_replay`
- `summarize_incident`
- `lineage_lookup`

Structured tool contracts are documented in `contracts/mcp/tools.md`.

## Demo Targets

| Target | POC Goal |
| --- | --- |
| Freshness lag | Dashboard shows per-symbol lag from event time to processing time. |
| API latency | REST reads are single Redis lookups per symbol snapshot field. |
| Replay correctness | Redis state is derivable from canonical Kafka topics. |
| Dashboard visibility | Watchlist shows bid, ask, spread, VWAP, freshness, and alerts. |
| MVP benchmark | `scripts/load-test-local.py` records throughput and p95/p99 latency for `/latest/{symbol}`. |

## Key Boundary

The hot path serves live traders from Redis. The cold path serves researchers and replay workloads from Delta Lake. The MCP path can inspect both, but it should use explicit tools with scoped permissions.
