"""Research ingestor — cold-path batch service.

Runs a poll loop: fetch → dedupe → extract → store → refresh Redis digests.
Never imports or modifies feed/stream/hot-path modules.

Usage:
    python -m market_platform.services.research_ingestor
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from market_platform.config import (
    REDIS_URL,
    RESEARCH_EDGAR_USER_AGENT,
    RESEARCH_EXTRACTOR,
    RESEARCH_INGESTOR_HEALTH_PORT,
    RESEARCH_LLM_DAILY_BUDGET_USD,
    RESEARCH_LLM_MODEL,
    RESEARCH_POLL_SECONDS,
    RESEARCH_REPLAY_FILE,
    RESEARCH_RSS_FEEDS,
    RESEARCH_SOURCES,
    RESEARCH_TIMEOUT_SECONDS,
)
from market_platform.research.extract import BudgetTracker, make_extractor
from market_platform.research.store import build_store, refresh_redis_digests

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
)
logger = logging.getLogger("research_ingestor")


# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------


def _build_sources(names: List[str]):
    from market_platform.research.sources.arxiv import ArxivSource
    from market_platform.research.sources.rss import RssSource
    from market_platform.research.sources.edgar import EdgarSource
    from market_platform.research.sources.replay import ReplaySource

    sources = []
    for name in names:
        if name == "arxiv":
            sources.append(ArxivSource())
        elif name == "rss":
            sources.append(RssSource(RESEARCH_RSS_FEEDS))
        elif name == "edgar":
            if RESEARCH_EDGAR_USER_AGENT:
                sources.append(EdgarSource(RESEARCH_EDGAR_USER_AGENT))
            else:
                logger.warning("edgar source requires RESEARCH_EDGAR_USER_AGENT — skipping")
        elif name == "replay":
            sources.append(ReplaySource(Path(RESEARCH_REPLAY_FILE)))
        else:
            logger.warning("unknown source: %s", name)
    return sources


# ---------------------------------------------------------------------------
# Health server
# ---------------------------------------------------------------------------


class HealthServer:
    def __init__(self, port: int) -> None:
        self._port = port
        self._status: Dict[str, Any] = {"sources": {}}
        self._server: Optional[asyncio.Server] = None

    def record_success(self, source: str) -> None:
        self._status["sources"][source] = {
            "last_success": datetime.now(timezone.utc).isoformat(),
            "status": "ok",
        }

    def record_failure(self, source: str, error: str) -> None:
        entry = self._status["sources"].setdefault(source, {})
        entry["status"] = "error"
        entry["last_error"] = error
        entry.setdefault("consecutive_failures", 0)
        entry["consecutive_failures"] = entry.get("consecutive_failures", 0) + 1

    def _is_healthy(self) -> bool:
        sources = self._status["sources"]
        if not sources:
            return True
        return any(s.get("status") == "ok" for s in sources.values())

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(1024)
        status_code = "200 OK" if self._is_healthy() else "503 Service Unavailable"
        body = json.dumps({"status": "ok" if self._is_healthy() else "degraded", **self._status})
        response = (
            f"HTTP/1.1 {status_code}\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}"
        )
        writer.write(response.encode())
        await writer.drain()
        writer.close()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "0.0.0.0", self._port)
        logger.info("health server listening on :%d", self._port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------


async def run() -> None:
    from redis.asyncio import Redis

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    postgres_dsn = os.getenv("RAG_POSTGRES_DSN")
    store = build_store(postgres_dsn)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    budget_tracker = BudgetTracker(redis, RESEARCH_LLM_DAILY_BUDGET_USD) if api_key else None
    extractor = make_extractor(RESEARCH_EXTRACTOR, api_key, RESEARCH_LLM_MODEL, budget_tracker)

    source_names = list(RESEARCH_SOURCES)
    if "replay" not in source_names and RESEARCH_TIMEOUT_SECONDS:
        source_names.insert(0, "replay")
    sources = _build_sources(source_names)

    health = HealthServer(RESEARCH_INGESTOR_HEALTH_PORT)
    await health.start()

    stop_event = asyncio.Event()

    def _on_signal():
        logger.info("signal received — stopping after current batch")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    deadline = None
    if RESEARCH_TIMEOUT_SECONDS:
        deadline = loop.time() + RESEARCH_TIMEOUT_SECONDS

    logger.info(
        "research-ingestor starting: sources=%s extractor=%s poll=%ds",
        [s.name for s in sources],
        extractor.__class__.__name__,
        RESEARCH_POLL_SECONDS,
    )

    try:
        while not stop_event.is_set():
            if deadline and loop.time() >= deadline:
                logger.info("timeout reached — stopping")
                break

            for source in sources:
                if stop_event.is_set():
                    break
                try:
                    # Switch to rule-based if budget exhausted
                    active_extractor = extractor
                    if budget_tracker and await budget_tracker.is_budget_exhausted():
                        from market_platform.research.extract import RuleBasedExtractor
                        active_extractor = RuleBasedExtractor()
                        logger.warning("budget exhausted — using rule_based extractor")

                    cursor = store.get_cursor(source.name)
                    docs, new_cursor = await source.fetch_since(cursor)

                    ingested = 0
                    for doc in docs:
                        if store.already_ingested(doc.doc_id):
                            continue
                        try:
                            insight = await active_extractor.extract(doc)
                            store.upsert_insight(insight)
                            ingested += 1
                        except Exception as exc:
                            logger.error("extraction failed for %s: %s", doc.doc_id, exc)

                    if new_cursor:
                        store.save_cursor(source.name, new_cursor)

                    await refresh_redis_digests(redis, store)
                    health.record_success(source.name)
                    logger.info("source=%s fetched=%d ingested=%d", source.name, len(docs), ingested)

                except Exception as exc:
                    logger.error("source=%s error: %s", source.name, exc)
                    health.record_failure(source.name, str(exc))
                    # Jittered backoff: 30-90 s
                    await asyncio.sleep(random.uniform(30, 90))

            if not stop_event.is_set():
                logger.info("poll cycle done — sleeping %ds", RESEARCH_POLL_SECONDS)
                try:
                    await asyncio.wait_for(
                        asyncio.shield(stop_event.wait()),
                        timeout=RESEARCH_POLL_SECONDS,
                    )
                except asyncio.TimeoutError:
                    pass
    finally:
        await health.stop()
        await redis.aclose()
        logger.info("research-ingestor stopped")


if __name__ == "__main__":
    asyncio.run(run())
