from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any, AsyncIterator

from market_platform.coinbase_adapter import raw_events_from_coinbase
from market_platform.config import (
    COINBASE_CHANNELS,
    COINBASE_PRODUCTS,
    COINBASE_WS_URL,
    EXCHANGE,
    FEED_STALL_SECONDS,
)

LOGGER = logging.getLogger(__name__)

_SUBSCRIBE = {
    "type": "subscribe",
    "channels": COINBASE_CHANNELS,
}


def _backoff(attempt: int) -> float:
    return min(60.0, 1.0 * (2**attempt)) * random.uniform(0.5, 1.5)


class CoinbaseFeedSource:
    name = "coinbase"

    def __init__(self) -> None:
        self._quote_counters: dict[str, int] = {}
        self._last_message_time: float = 0.0
        self._reconnects: int = 0
        self._dropped: int = 0

    @property
    def last_message_age(self) -> float:
        if self._last_message_time == 0:
            return float("inf")
        return time.monotonic() - self._last_message_time

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        try:
            import websockets
        except ImportError as exc:
            raise ImportError("pip install 'websockets>=12' to use the Coinbase feed source") from exc

        attempt = 0
        while True:
            try:
                async for event in self._connect(websockets):
                    attempt = 0
                    yield event
            except asyncio.CancelledError:
                return
            except Exception as exc:
                delay = _backoff(attempt)
                LOGGER.warning("Coinbase WS error (attempt %d): %s — reconnecting in %.1fs", attempt, exc, delay)
                self._reconnects += 1
                await asyncio.sleep(delay)
                attempt = min(attempt + 1, 8)

    async def _connect(self, websockets) -> AsyncIterator[dict[str, Any]]:
        subscribe_msg = json.dumps({**_SUBSCRIBE, "product_ids": COINBASE_PRODUCTS})
        LOGGER.info("Connecting to Coinbase WS %s products=%s", COINBASE_WS_URL, COINBASE_PRODUCTS)
        async with websockets.connect(COINBASE_WS_URL) as ws:
            await ws.send(subscribe_msg)
            stall_task = asyncio.create_task(self._stall_watchdog())
            try:
                async for raw in ws:
                    self._last_message_time = time.monotonic()
                    msg = json.loads(raw)
                    for event in raw_events_from_coinbase(msg, self._quote_counters, EXCHANGE):
                        yield event
            finally:
                stall_task.cancel()

    async def _stall_watchdog(self) -> None:
        from market_platform.events import quality_alert
        while True:
            await asyncio.sleep(FEED_STALL_SECONDS)
            if self.last_message_age > FEED_STALL_SECONDS:
                LOGGER.error("Feed stall detected: no message for %.0fs", self.last_message_age)
                alert = quality_alert(
                    symbol="ALL",
                    exchange=EXCHANGE,
                    alert_type="feed_stall",
                    severity="critical",
                    message=f"No message from Coinbase WS for {self.last_message_age:.0f}s.",
                )
                LOGGER.warning("feed_stall alert: %s", alert)
                raise ConnectionError("feed stall — forcing reconnect")
