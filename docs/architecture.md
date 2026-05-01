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

## Key Boundary

The hot path serves live traders from Redis. The cold path serves researchers and replay workloads from Delta Lake. The MCP path can inspect both, but it should use explicit tools with scoped permissions.
