from __future__ import annotations

import logging
import os
from typing import Any, AsyncIterator

from market_platform.config import EXCHANGE
from market_platform.databento_adapter import raw_events_from_databento

LOGGER = logging.getLogger(__name__)


def _csv_env(name: str, default: str) -> list[str]:
    return [v.strip() for v in os.getenv(name, default).split(",") if v.strip()]


class DatabentoFeedSource:
    name = "databento"

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        import asyncio

        try:
            import databento as db
        except ImportError as exc:
            raise ImportError("pip install 'databento>=0.64.0' to use the Databento feed source") from exc

        dataset = os.getenv("DATABENTO_DATASET", "GLBX.MDP3")
        symbols = _csv_env("DATABENTO_SYMBOLS", "ES.FUT,NQ.FUT")
        schemas = _csv_env("DATABENTO_SCHEMAS", "mbp-1,trades")
        stype_in = os.getenv("DATABENTO_STYPE_IN", "parent")
        replay_start = os.getenv("DATABENTO_REPLAY_START") or None
        timeout_value = os.getenv("DATABENTO_TIMEOUT_SECONDS")
        timeout_seconds = float(timeout_value) if timeout_value else None

        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50_000)
        loop = asyncio.get_running_loop()
        symbol_map: dict[int, str] = {}

        def enqueue(event: dict[str, Any]) -> None:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                LOGGER.warning("Dropping Databento event: adapter queue full")

        def on_record(record: Any) -> None:
            for event in raw_events_from_databento(record, symbol_map, EXCHANGE):
                loop.call_soon_threadsafe(enqueue, event)

        client = db.Live()
        for schema in schemas:
            LOGGER.info("Subscribing Databento dataset=%s schema=%s symbols=%s", dataset, schema, symbols)
            client.subscribe(dataset=dataset, schema=schema, symbols=symbols, stype_in=stype_in, start=replay_start)
        client.add_callback(on_record)
        client.start()

        async def _stop_after_timeout() -> None:
            if timeout_seconds:
                await asyncio.sleep(timeout_seconds)
                client.stop()

        async def _wait_close() -> None:
            await client.wait_for_close()
            await queue.put({"event_type": "_eof"})

        t_stop = asyncio.create_task(_stop_after_timeout())
        t_close = asyncio.create_task(_wait_close())
        try:
            while True:
                event = await queue.get()
                if event.get("event_type") == "_eof":
                    return
                yield event
        finally:
            t_stop.cancel()
            t_close.cancel()
            client.stop()
