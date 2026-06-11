"""Replay source — reads from a JSONL fixture file for CI/offline determinism.

Mirrors market_platform/feeds/replay.py pattern.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional, Tuple

from market_platform.research.models import ResearchDocument

logger = logging.getLogger(__name__)


class ReplaySource:
    name = "replay"

    def __init__(self, fixture_path: Path) -> None:
        self._path = fixture_path

    async def fetch_since(
        self, cursor: Optional[str]
    ) -> Tuple[List[ResearchDocument], Optional[str]]:
        if not self._path.exists():
            logger.warning("replay: fixture not found at %s", self._path)
            return [], cursor

        docs: List[ResearchDocument] = []
        new_cursor: Optional[str] = cursor

        with self._path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue

                published_iso = row.get("published_time", "")
                if cursor and published_iso <= cursor:
                    continue

                docs.append(
                    ResearchDocument(
                        source=row.get("source", "replay"),
                        source_uri=row.get("source_uri", ""),
                        title=row.get("title", ""),
                        body=row.get("body", ""),
                        published_time=published_iso,
                        fetched_time=row.get("fetched_time", published_iso),
                    )
                )

                if new_cursor is None or published_iso > new_cursor:
                    new_cursor = published_iso

        logger.info("replay: loaded %d docs from %s", len(docs), self._path)
        return docs, new_cursor
