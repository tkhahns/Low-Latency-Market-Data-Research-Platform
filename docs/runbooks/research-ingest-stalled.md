# Runbook: ResearchIngestStalled

**Alert:** `ResearchIngestStalled` — no successful ingest cycle in 2+ hours.

## Impact

The research panel in the dashboard and `/research/*` API endpoints will return stale or empty data. Market data hot path is **unaffected** — this is a cold-path service.

## Diagnosis

1. **Check pod status:**
   ```bash
   kubectl -n market-data get pod -l app=research-ingestor
   kubectl -n market-data logs -l app=research-ingestor --tail=100
   ```

2. **Check health endpoint:**
   ```bash
   kubectl -n market-data port-forward svc/research-ingestor 8030:8030
   curl http://localhost:8030/health | jq .
   ```
   The response shows per-source last-success times and consecutive failure counts.

3. **Check Docker Compose (local/dev):**
   ```bash
   docker compose --profile research logs research-ingestor --tail=50
   ```

## Common causes and fixes

| Cause | Fix |
| --- | --- |
| arXiv API returning 5xx | Wait — arXiv has brief outages. Jittered backoff handles it. |
| RSS feed URL changed | Update `RESEARCH_RSS_FEEDS` env var with correct URLs. |
| EDGAR rate limit hit | Ensure `RESEARCH_EDGAR_USER_AGENT` is set with a valid contact email. Reduce poll frequency. |
| Redis unreachable | Check `REDIS_URL` config and Redis pod health. |
| Postgres unavailable | Store degrades to in-memory (insights won't persist across restarts). No action needed for short outages. |
| `ANTHROPIC_API_KEY` revoked | Set `RESEARCH_EXTRACTOR=rule_based` — pipeline continues without LLM. |
| OOM kill | Increase memory limit in `deployments.yaml` (`research-ingestor` container). |

## Escalation

If all sources show `consecutive_failures > 10` and no obvious external cause: restart the pod.

```bash
kubectl -n market-data rollout restart deployment/research-ingestor
```

This is safe because:
- Market data hot path is unaffected
- Postgres + Redis cursors persist state; after restart the ingestor picks up where it left off
- In-memory mode degrades gracefully (no crash, just no persistence)
