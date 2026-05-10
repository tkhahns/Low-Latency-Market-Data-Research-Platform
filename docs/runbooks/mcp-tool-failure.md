---
title: MCP Tool Failure Runbook
tags: [runbook, mcp, rag, pgvector]
---

# MCP Tool Failure Runbook

Symptoms:
- MCP tool responses return `status=error`.
- Tool latency increases.
- Source references are missing from diagnostic answers.

Checks:
1. Check `/health` on the MCP ops server.
2. Inspect `mcp_tool_audit` or local JSONL audit logs.
3. Verify Redis, pgvector, and lakehouse metadata connectivity.
4. Confirm the Obsidian/repo index contains expected notes and runbooks.
5. Validate that the request is read-only or dry-run.

Recovery:
- Reindex docs and Obsidian notes if source references are stale.
- Restart the MCP ops server if connection pools are exhausted.
- Restore pgvector from backup if indexed data is missing.
- Keep mutation requests disabled in v1.
