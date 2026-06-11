"""SEC EDGAR full-text search source.

Requires RESEARCH_EDGAR_USER_AGENT env var (e.g. "Company contact@example.com").
Rate limit: ≤10 req/s hard limit; we make at most 1 request per poll cycle.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET

from market_platform.research.models import ResearchDocument

logger = logging.getLogger(__name__)

_EFTS_URL = "https://efts.sec.gov/LATEST/search-index?q=%22cryptocurrency%22+%22bitcoin%22&dateRange=custom&startdt={start}&enddt={end}&forms=8-K,10-Q,10-K"
_ATOM_NS = "http://www.w3.org/2005/Atom"


class EdgarSource:
    name = "edgar"

    def __init__(self, user_agent: str, max_results: int = 10) -> None:
        self._user_agent = user_agent
        self._max_results = max_results

    async def fetch_since(
        self, cursor: Optional[str]
    ) -> Tuple[List[ResearchDocument], Optional[str]]:
        from datetime import date, timedelta

        now = datetime.now(timezone.utc)
        fetched_time = now.isoformat()
        today = now.date()
        start = (today - timedelta(days=7)).isoformat()
        end = today.isoformat()

        url = f"https://efts.sec.gov/LATEST/search-index?q=%22cryptocurrency%22+%22bitcoin%22&dateRange=custom&startdt={start}&enddt={end}&forms=8-K,10-Q,10-K&hits.hits._source=period_of_report,file_date,display_names,form_type,biz_location,inc_states,category"

        headers = {
            "User-Agent": self._user_agent,
            "Accept": "application/json",
        }

        import httpx  # optional dep — only needed when edgar source is active
        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.warning("edgar: fetch failed: %s", exc)
                return [], cursor

        docs: List[ResearchDocument] = []
        new_cursor: Optional[str] = cursor
        hits = data.get("hits", {}).get("hits", [])

        for hit in hits[: self._max_results]:
            src = hit.get("_source", {})
            accession = hit.get("_id", "")
            if cursor and accession <= cursor:
                continue

            file_date = src.get("file_date", today.isoformat())
            published_iso = f"{file_date}T00:00:00+00:00"
            company = ", ".join(src.get("display_names", ["Unknown"]))
            form_type = src.get("form_type", "filing")
            title = f"{company} — {form_type} ({file_date})"
            body = f"{company} filed a {form_type} with the SEC on {file_date}."

            docs.append(
                ResearchDocument(
                    source="edgar",
                    source_uri=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&accession={accession}",
                    title=title,
                    body=body,
                    published_time=published_iso,
                    fetched_time=fetched_time,
                )
            )

            if new_cursor is None or accession > new_cursor:
                new_cursor = accession

        logger.info("edgar: fetched %d filings (cursor %s → %s)", len(docs), cursor, new_cursor)
        return docs, new_cursor
