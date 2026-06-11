"""Research store — persists insights to Postgres + Redis digest cache.

Works without Postgres: if no RAG_POSTGRES_DSN, uses an in-memory dict for
insights and still writes the Redis digest cache so the API works normally.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from market_platform.research.models import ResearchInsight

logger = logging.getLogger(__name__)

_DIGEST_MAX = 20
_SYMBOL_MAX = 10
_DIGEST_TTL = 86400  # 24 h


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = """
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

CREATE INDEX IF NOT EXISTS research_insights_symbols_idx
  ON research_insights USING GIN (symbols);

CREATE TABLE IF NOT EXISTS research_cursors (
  source TEXT PRIMARY KEY,
  cursor TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Postgres-backed store
# ---------------------------------------------------------------------------


class PostgresResearchStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def bootstrap(self) -> None:
        import psycopg

        with psycopg.connect(self._dsn) as conn:
            conn.execute(DDL)

    def already_ingested(self, doc_id: str) -> bool:
        import psycopg

        with psycopg.connect(self._dsn) as conn:
            row = conn.execute("SELECT 1 FROM research_insights WHERE doc_id=%s", (doc_id,)).fetchone()
            return row is not None

    def upsert_insight(self, insight: ResearchInsight) -> None:
        import psycopg

        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                """
                INSERT INTO research_insights
                  (doc_id, summary, symbols, tags, sentiment, entities,
                   extractor, source, source_uri, title, published_time, extracted_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (doc_id) DO NOTHING
                """,
                (
                    insight.doc_id,
                    insight.summary,
                    insight.symbols,
                    insight.tags,
                    insight.sentiment,
                    insight.entities,
                    insight.extractor,
                    insight.source,
                    insight.source_uri,
                    insight.title,
                    insight.published_time,
                    insight.extracted_time,
                ),
            )

    def get_cursor(self, source: str) -> Optional[str]:
        import psycopg

        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                "SELECT cursor FROM research_cursors WHERE source=%s", (source,)
            ).fetchone()
            return row[0] if row else None

    def save_cursor(self, source: str, cursor: str) -> None:
        import psycopg

        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                """
                INSERT INTO research_cursors (source, cursor, updated_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (source) DO UPDATE SET cursor=EXCLUDED.cursor, updated_at=EXCLUDED.updated_at
                """,
                (source, cursor, datetime.now(timezone.utc)),
            )

    def latest_insights(self, symbol: Optional[str] = None, limit: int = 20) -> List[ResearchInsight]:
        import psycopg

        with psycopg.connect(self._dsn) as conn:
            if symbol:
                rows = conn.execute(
                    """
                    SELECT doc_id, summary, symbols, tags, sentiment, entities,
                           extractor, source, source_uri, title, published_time, extracted_time
                    FROM research_insights
                    WHERE %s = ANY(symbols)
                    ORDER BY published_time DESC
                    LIMIT %s
                    """,
                    (symbol.upper(), limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT doc_id, summary, symbols, tags, sentiment, entities,
                           extractor, source, source_uri, title, published_time, extracted_time
                    FROM research_insights
                    ORDER BY published_time DESC
                    LIMIT %s
                    """,
                    (limit,),
                ).fetchall()
        return [_row_to_insight(r) for r in rows]


def _row_to_insight(row) -> ResearchInsight:
    return ResearchInsight(
        doc_id=row[0],
        summary=row[1],
        symbols=list(row[2] or []),
        tags=list(row[3] or []),
        sentiment=row[4] or "neutral",
        entities=list(row[5] or []),
        extractor=row[6],
        source=row[7],
        source_uri=row[8],
        title=row[9],
        published_time=str(row[10].isoformat()) if hasattr(row[10], "isoformat") else str(row[10]),
        extracted_time=str(row[11].isoformat()) if hasattr(row[11], "isoformat") else str(row[11]),
    )


# ---------------------------------------------------------------------------
# In-memory store (no Postgres dependency)
# ---------------------------------------------------------------------------


class InMemoryResearchStore:
    def __init__(self) -> None:
        self._insights: Dict[str, ResearchInsight] = {}
        self._cursors: Dict[str, str] = {}

    def bootstrap(self) -> None:
        pass

    def already_ingested(self, doc_id: str) -> bool:
        return doc_id in self._insights

    def upsert_insight(self, insight: ResearchInsight) -> None:
        self._insights[insight.doc_id] = insight

    def get_cursor(self, source: str) -> Optional[str]:
        return self._cursors.get(source)

    def save_cursor(self, source: str, cursor: str) -> None:
        self._cursors[source] = cursor

    def latest_insights(self, symbol: Optional[str] = None, limit: int = 20) -> List[ResearchInsight]:
        all_insights = sorted(
            self._insights.values(), key=lambda i: i.published_time, reverse=True
        )
        if symbol:
            all_insights = [i for i in all_insights if symbol.upper() in i.symbols]
        return all_insights[:limit]


# ---------------------------------------------------------------------------
# Redis digest writer
# ---------------------------------------------------------------------------


async def refresh_redis_digests(redis: Any, store: Any) -> None:
    from market_platform import redis_keys
    from market_platform.config import COINBASE_PRODUCTS, SYMBOLS

    all_symbols = list(set(COINBASE_PRODUCTS) | set(SYMBOLS))

    for symbol in all_symbols:
        insights = store.latest_insights(symbol=symbol, limit=_SYMBOL_MAX)
        payload = json.dumps([i.to_dict() for i in insights])
        key = redis_keys.research_symbol(symbol)
        await redis.set(key, payload, ex=_DIGEST_TTL)

    digest_insights = store.latest_insights(limit=_DIGEST_MAX)
    digest_payload = json.dumps([i.to_dict() for i in digest_insights])
    await redis.set(redis_keys.research_digest(), digest_payload, ex=_DIGEST_TTL)
    logger.info("refreshed redis research digests (%d symbols, %d digest)", len(all_symbols), len(digest_insights))


def build_store(postgres_dsn: Optional[str] = None):
    if postgres_dsn:
        store = PostgresResearchStore(postgres_dsn)
        store.bootstrap()
        return store
    return InMemoryResearchStore()
