---
title: Architecture Decisions
tags: [architecture, kafka, redis, delta, mcp]
---

# Architecture Decisions

The platform keeps live serving, historical research, and agentic operations on separate paths.

- Hot path: feed simulator or provider adapter to feed handler, Kafka-compatible topics, Flink processing, Redis hot cache, FastAPI WebSocket and REST API, dashboard.
- Cold path: raw Kafka history to Delta bronze, normalized silver tables, gold research features and replay outputs.
- Agentic ops path: project docs, Obsidian notes, runbooks, and quality reports are indexed into the vector store for MCP reliability tools.

Redis remains the only hot serving cache in v1. Databricks Delta is used for replay and research rather than live API requests.
