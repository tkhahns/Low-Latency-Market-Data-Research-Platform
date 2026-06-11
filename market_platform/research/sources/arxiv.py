"""arXiv Atom API source — q-fin.* and cs.CE categories."""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET

from market_platform.research.models import ResearchDocument

logger = logging.getLogger(__name__)

_ATOM_NS = "http://www.w3.org/2005/Atom"
_ARXIV_NS = "http://arxiv.org/schemas/atom"
_BASE_URL = "http://export.arxiv.org/api/query"
_CATEGORIES = "q-fin.CP+OR+q-fin.TR+OR+q-fin.PM+OR+q-fin.MF+OR+cs.CE"
_USER_AGENT = "low-latency-market-data-research-platform/0.1 (research pipeline)"
_POLL_DELAY = 3.0  # seconds between requests — arXiv politeness policy


class ArxivSource:
    name = "arxiv"

    def __init__(self, max_results: int = 20) -> None:
        self._max_results = max_results

    async def fetch_since(
        self, cursor: Optional[str]
    ) -> Tuple[List[ResearchDocument], Optional[str]]:
        now = datetime.now(timezone.utc)
        fetched_time = now.isoformat()

        params = {
            "search_query": f"cat:{_CATEGORIES}",
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "max_results": self._max_results,
        }

        import httpx  # optional dep — only needed when arxiv source is active
        async with httpx.AsyncClient(headers={"User-Agent": _USER_AGENT}, timeout=30.0) as client:
            await asyncio.sleep(_POLL_DELAY)
            resp = client.build_request("GET", _BASE_URL, params=params)
            logger.debug("arxiv fetch %s", resp.url)
            response = await client.send(resp)
            response.raise_for_status()

        root = ET.fromstring(response.text)
        docs: List[ResearchDocument] = []
        new_cursor: Optional[str] = None

        for entry in root.findall(f"{{{_ATOM_NS}}}entry"):
            published_el = entry.find(f"{{{_ATOM_NS}}}published")
            if published_el is None or not published_el.text:
                continue
            published_iso = published_el.text.strip()

            if cursor and published_iso <= cursor:
                continue

            id_el = entry.find(f"{{{_ATOM_NS}}}id")
            title_el = entry.find(f"{{{_ATOM_NS}}}title")
            summary_el = entry.find(f"{{{_ATOM_NS}}}summary")

            source_uri = (id_el.text or "").strip() if id_el is not None else ""
            title = _clean_ws(title_el.text or "") if title_el is not None else ""
            body = _clean_ws(summary_el.text or "") if summary_el is not None else ""

            if not body:
                continue

            docs.append(
                ResearchDocument(
                    source="arxiv",
                    source_uri=source_uri,
                    title=title,
                    body=body,
                    published_time=published_iso,
                    fetched_time=fetched_time,
                )
            )

            if new_cursor is None or published_iso > new_cursor:
                new_cursor = published_iso

        logger.info("arxiv: fetched %d docs (cursor %s → %s)", len(docs), cursor, new_cursor)
        return docs, new_cursor or cursor


def _clean_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
