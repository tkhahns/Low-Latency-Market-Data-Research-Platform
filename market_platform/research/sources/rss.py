"""RSS/Atom feed source using feedparser."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional, Tuple

from market_platform.research.models import ResearchDocument

logger = logging.getLogger(__name__)


def _parse_entry_time(entry) -> Optional[str]:
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            import time
            ts = time.mktime(val)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    return None


class RssSource:
    name = "rss"

    def __init__(self, feed_urls: List[str]) -> None:
        self._feed_urls = feed_urls

    async def fetch_since(
        self, cursor: Optional[str]
    ) -> Tuple[List[ResearchDocument], Optional[str]]:
        import feedparser  # optional dependency

        now = datetime.now(timezone.utc)
        fetched_time = now.isoformat()
        docs: List[ResearchDocument] = []
        new_cursor: Optional[str] = cursor

        for url in self._feed_urls:
            try:
                feed = feedparser.parse(url)
            except Exception as exc:
                logger.warning("rss: failed to parse %s: %s", url, exc)
                continue

            for entry in feed.entries:
                published_iso = _parse_entry_time(entry)
                if not published_iso:
                    published_iso = fetched_time

                if cursor and published_iso <= cursor:
                    continue

                source_uri = getattr(entry, "link", "") or url
                title = getattr(entry, "title", "") or ""
                summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
                content_blocks = getattr(entry, "content", [])
                body = content_blocks[0].value if content_blocks else summary
                body = _strip_html(body)

                if not body:
                    body = title

                docs.append(
                    ResearchDocument(
                        source="rss",
                        source_uri=source_uri,
                        title=title,
                        body=body,
                        published_time=published_iso,
                        fetched_time=fetched_time,
                    )
                )

                if new_cursor is None or published_iso > new_cursor:
                    new_cursor = published_iso

        logger.info("rss: fetched %d docs across %d feeds", len(docs), len(self._feed_urls))
        return docs, new_cursor


def _strip_html(text: str) -> str:
    import re
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
