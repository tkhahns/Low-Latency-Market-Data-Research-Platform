# Backup And Recovery

## Kafka / Redpanda

Retention should cover the longest expected replay window for hot-state rebuilds and short research backfills.

Recovery:
1. Identify topic, partition, and offset range.
2. Verify retention still covers the range.
3. Reprocess derived topics or rebuild Redis using `market_platform.tools.rebuild_redis_from_kafka`.
4. If retention is exhausted, use bronze Delta as the recovery source for research tables.

## Delta Lake

Bronze tables are append-only and are the source of truth for historical replay. Silver and gold tables are reproducible from bronze.

Recovery:
1. Restore or time-travel bronze if corruption is detected.
2. Rerun `silver_normalize.py` for affected partitions.
3. Rerun `gold_features.py`.
4. Run `quality_report.py` and compare row counts/date coverage.

## PostgreSQL + pgvector

Postgres stores indexed docs, Obsidian notes, and MCP audit data.

Recovery:
1. Restore Cloud SQL point-in-time backup.
2. Re-run `infra/postgres/pgvector.sql` if extensions/indexes are missing.
3. Reindex repo docs and Obsidian vault notes using `market_platform.tools.index_obsidian`.
4. Confirm MCP source references return for runbooks and incidents.

## Redis

Redis is a hot serving cache, not a system of record.

Recovery:
1. Restart Redis or fail over managed Redis.
2. Run Redis rebuild dry-run from derived Kafka topics.
3. Rebuild hot keys from Kafka if retention covers the outage.
4. If derived Kafka retention is exhausted, use Delta replay to regenerate derived state outside the live request path.
5. Confirm `/latest/{symbol}`, `/freshness/{symbol}`, and `/ws/live` recover.
