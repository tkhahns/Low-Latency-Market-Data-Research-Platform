from __future__ import annotations

from typing import Any, AsyncIterator, Protocol, runtime_checkable


@runtime_checkable
class FeedSource(Protocol):
    name: str

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Yield raw events matching feed_handler RAW_REQUIRED_FIELDS shape."""
        ...
