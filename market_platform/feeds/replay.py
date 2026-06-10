from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, AsyncIterator

from market_platform.config import FEED_REPLAY_FILE, FEED_REPLAY_LOOP, FEED_REPLAY_SPEED
from market_platform.time import utc_now_iso

LOGGER = logging.getLogger(__name__)


def _rebase_event(event: dict[str, Any], base_now: str, offset_ms: float) -> dict[str, Any]:
    """Return a copy of event with event_time rebased to now + offset_ms."""
    import datetime

    base = datetime.datetime.fromisoformat(base_now.replace("Z", "+00:00"))
    new_time = base + datetime.timedelta(milliseconds=offset_ms)
    rebased = dict(event)
    rebased["event_time"] = new_time.isoformat().replace("+00:00", "Z")
    return rebased


class ReplayFeedSource:
    name = "replay"

    def __init__(self, replay_file: str | None = None, speed: float | None = None, loop: bool | None = None) -> None:
        self._file = Path(replay_file or FEED_REPLAY_FILE)
        self._speed = speed if speed is not None else FEED_REPLAY_SPEED
        self._loop = loop if loop is not None else FEED_REPLAY_LOOP

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        if not self._file.exists():
            raise FileNotFoundError(f"Replay fixture not found: {self._file}")
        LOGGER.info("Replaying fixture %s speed=%.1f loop=%s", self._file, self._speed, self._loop)
        while True:
            async for event in self._replay_once():
                yield event
            if not self._loop:
                return
            LOGGER.info("Replay loop: restarting fixture %s", self._file)

    async def _replay_once(self) -> AsyncIterator[dict[str, Any]]:
        lines = self._file.read_text().splitlines()
        events = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith("#"):
                events.append(json.loads(line))
        if not events:
            return

        base_now = utc_now_iso()
        last_offset = 0.0
        for event in events:
            offset_ms = float(event.pop("_capture_offset_ms", 0))
            rebased = _rebase_event(event, base_now, offset_ms)
            if self._speed > 0 and offset_ms > last_offset:
                delay = (offset_ms - last_offset) / 1000.0 / self._speed
                await asyncio.sleep(delay)
            last_offset = offset_ms
            yield rebased
