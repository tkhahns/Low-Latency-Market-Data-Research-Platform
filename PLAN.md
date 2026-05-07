  # POC to Production Plan: Low-Latency Market Data & Research
  Platform

  ## Summary

  Build the project in staged iterations, starting with a local
  end-to-end POC and ending with a production-ready, cloud-
  deployed data platform. The implementation should keep the
  three core paths separate: hot path for live serving, cold
  path for lakehouse research/replay, and agentic ops path for
  MCP reliability tools.

  Default decisions: Java for feed handling and Flink jobs,
  FastAPI for WebSocket/API, Python for MCP and Spark jobs,
  Next.js for dashboard, Redpanda locally as Kafka-compatible
  broker, Databricks Delta Lake for cold path, PostgreSQL +
  pgvector for RAG, and GCP-oriented deployment.

  ## Iteration Plan

  ### 0. Architecture and Contracts

  Checklist:

  - Finalize service boundaries: feed simulator, feed handler,
    stream processor, market data API, dashboard, lakehouse
    jobs, MCP ops server.
  - Define canonical event schemas for trades, quotes, market
    state, bars, and quality alerts.
  - Define Kafka topics, topic ownership, retention intent, and
    replay expectations.
  - Define Redis key layout for latest quote, top-of-book, bars,
    freshness, and alerts.
  - Define WebSocket channels and REST endpoints.
  - Define Delta Lake bronze/silver/gold table contracts.
  - Define MCP tools: check_symbol_freshness,
    explain_sequence_gap, run_replay_dry_run,
    compare_live_vs_replay, summarize_incident, lineage_lookup.
  - Define measurable demo targets: freshness lag, API latency,
    replay correctness, and dashboard visibility.

  Done when:

  - Contracts are versioned.
  - Architecture docs clearly explain hot, cold, and agentic
    paths.
  - No implementation needs to guess data shape or component
    ownership.

  ### 1. POC: Local Hot Path

  Checklist:

  - Build feed simulator that emits synthetic trade and quote
    events.
  - Build feed handler that validates required fields, assigns
    ingest timestamps, checks sequence numbers, and publishes
    canonical events.
  - Run Kafka-compatible broker, Redis, PostgreSQL, and
    OpenTelemetry collector locally.
  - Add basic stream consumer or minimal Flink job to compute
    latest quote/top-of-book.
  - Store hot state in Redis.
  - Expose latest state through FastAPI REST and WebSocket
    endpoints.
  - Build dashboard watchlist with live symbol state, spread,
    freshness, and alerts.

  Done when:

  - A local command starts the stack.
  - Synthetic events flow from simulator to dashboard.
  - Sequence gaps and stale symbols are visible.
  - README includes a working local demo path.

  ### 2. MVP: Stateful Streaming Platform

  Checklist:

  - Replace simple stream logic with Flink jobs.
  - Compute top-of-book, spread, rolling 1s bars, volume, VWAP,
    volatility window, freshness lag, and quality alerts.
  - Configure Flink checkpointing and restart behavior.
  - Publish derived Kafka topics for market state, bars, and
    alerts.
  - Add Redis write model with predictable keys and TTL/
    freshness rules.
  - Add integration tests for deterministic stream outputs from
    fixed input events.
  - Add load test for local throughput and end-to-end latency.

  Done when:

  - Flink owns all stateful market calculations.
  - Redis state can be rebuilt from Kafka replay.
  - Dashboard reflects derived metrics rather than raw feed
    events.
  - Basic latency and throughput numbers are recorded.

  ### 3. Cold Path: Databricks Delta Lake

  Checklist:

  - Add Databricks Asset Bundle structure and environment
    configs.
  - Ingest Kafka/raw archive events into bronze Delta tables.
  - Build silver trades, quotes, top-of-book, and sequence-gap
    tables.
  - Build gold bars, spread features, volatility features, and
    data-quality annotation tables.
  - Add Databricks Jobs for bronze ingest, silver normalization,
    gold feature generation, quality reports, and replay dry-
    runs.
  - Add sample research notebook or query showing historical
    analysis/backtest-ready data.
  - Preserve lineage metadata: source topic, offsets, job run
    ID, schema version, and processing timestamp.

  Done when:

  - Historical events are queryable in Delta.
  - A replayed day can regenerate expected research tables.
  - Gold tables are stable enough for backtesting examples.
  - Cold path remains outside live API request flow.

  ### 4. Agentic Ops: RAG + MCP

  Checklist:

  - Index architecture docs, schemas, runbooks, incidents, table
    metadata, and quality reports into PostgreSQL + pgvector.
  - Build MCP server with scoped tools for freshness, sequence
    gaps, replay dry-runs, live-vs-replay comparison, incident
    summaries, and lineage lookup.
  - Require all MCP tools to return structured results and
    source references.
  - Add audit logs for MCP calls, parameters, caller, result
    status, and execution time.
  - Add guardrails: dry-run first for replay, no direct
    production mutation in v1, explicit error handling for
    missing data.
  - Add example prompts and expected outputs for platform
    reliability scenarios.

  Done when:

  - The MCP server can answer “Why is AAPL stale?” using Redis/
    Kafka/lakehouse evidence.
  - The agent can explain sequence gaps and table lineage.
  - Tool calls are auditable and deterministic enough for demos.

  ### 5. Production Readiness

  Checklist:

  - Add GitHub Actions for linting, unit tests, contract tests,
    Docker builds, and integration tests.
  - Add container images for each runtime service.
  - Add Kubernetes manifests or Helm-style deployment structure.
  - Add GCP deployment target: GKE or Cloud Run where
    appropriate, managed Redis, managed Postgres, Databricks on
    GCP, and managed Kafka/Redpanda/Confluent.
  - Add secrets management for provider API keys, Databricks
    tokens, database credentials, and OpenAI/API keys if used.
  - Add OpenTelemetry traces, structured logs, metrics, Grafana
    dashboards, and alert rules.
  - Add runbooks for stale symbol, sequence gap, Flink failure,
    Redis divergence, Databricks job failure, and MCP tool
    failure.
  - Add backup/recovery notes for Kafka retention, Delta tables,
    Postgres vector store, and Redis rebuild strategy.
  - Add performance report with benchmark setup, throughput,
    p95/p99 latency, and known limits.

  Done when:

  - The platform is deployed and accessible.
  - Observability shows live service and data quality health.
  - Failures can be diagnosed through dashboards, logs,
    runbooks, and MCP tools.
  - The project can be demonstrated end-to-end without manual
    hidden steps.

  ## Required Interfaces

  - Kafka topics must stay versioned: raw events, trades,
    quotes, top-of-book, 1s bars, and quality alerts.
  - Event schemas must include schema version, symbol, exchange,
    event time, ingest time, sequence number, and event-specific
    payload fields.
  - Redis must expose stable keys for latest symbol state, bars,
    freshness, and alerts.
  - API must expose WebSocket live streams plus REST endpoints
    for latest state, freshness, alerts, and health.
  - Delta tables must be separated into bronze, silver, and gold
    zones.
  - MCP tools must use structured inputs/outputs and must not
    bypass service boundaries.

  ## Test Plan

    bars, VWAP, alerts, and replay behavior.
  - Integration tests using local broker, Redis, API, and
    dashboard smoke checks.
  - Lakehouse tests for bronze ingestion, silver deduplication,
    gold feature correctness, and replay reproducibility.
  - MCP tests for freshness diagnosis, sequence-gap explanation,
    lineage lookup, audit logging, and unsafe request rejection.
  - Load tests for event throughput, Redis writes, WebSocket
    fan-out, and API p95/p99 latency.
  - Failure tests for Kafka gaps, late events, stale symbols,
    Redis restart, Flink restart, and Databricks job failure.

  ## Project Requirement Checklist

  - Hot path exists: simulator/feed handler to Kafka to Flink to
    Redis to WebSocket/API to dashboard.
  - Cold path exists: Kafka/raw history to Databricks Delta
    bronze/silver/gold to research/backtesting datasets.
  - Agentic ops path exists: docs/metrics/incidents to RAG to
    MCP tools.
  - Databricks is used for lakehouse and replay, not live
    serving.
  - Redis is the only hot serving cache in v1.
  - Airflow, Iceberg, ClickHouse, Bigtable, OpenLineage, and
    Vertex AI Vector Search are excluded from v1.
  - The project demonstrates data engineering depth: streaming,
    storage, processing, replay, observability, deployment, and
    AI-assisted operations.
  - Final artifact supports resume positioning as a quant data
    platform / low-latency data engineering project.

  ## Assumptions

  - The first serious implementation target is local POC, then
    production-like cloud deployment.
  - GCP is the default cloud direction, but the architecture
    remains portable.
  - Synthetic feed is acceptable for POC; a real provider
    adapter can be added later behind environment variables.
  - “Low latency” means measured low-latency market data serving
    for dashboards and APIs, not exchange-colocated HFT latency.
  - Production-ready means deployed, observable, tested,
    reproducible, documented, and diagnosable, not merely
    feature-complete.