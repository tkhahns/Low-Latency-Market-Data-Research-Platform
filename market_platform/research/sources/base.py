"""ResearchSource protocol — mirrors market_platform/feeds/base.py pattern."""
from __future__ import annotations

from typing import List, Optional, Protocol, Tuple, runtime_checkable

from market_platform.research.models import ResearchDocument


@runtime_checkable
class ResearchSource(Protocol):
    name: str

    async def fetch_since(
        self, cursor: Optional[str]
    ) -> Tuple[List[ResearchDocument], Optional[str]]:
        """Fetch documents newer than cursor.

        Returns (documents, new_cursor). new_cursor is None when the source
        provides no stable cursor (e.g. limited RSS feeds).
        """
        ...
