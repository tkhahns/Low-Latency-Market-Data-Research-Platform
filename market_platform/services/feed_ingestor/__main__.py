from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from typing import Any

from aiokafka import AIOKafkaProducer

from market_platform.config import (
    FEED_INGESTOR_HEALTH_PORT,
    FEED_SOURCE,
    FEED_TIMEOUT_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
)
from market_platform.feeds import make_feed_source
from market_platform.serde import dumps
from market_platform.topics import FEED_RAW_TOPIC, SYNTHETIC_RAW_TOPIC

LOGGER = logging.getLogger(__name__)


class _IngestorState:
    def __init__(self) -> None:
        self.last_message_time: float = 0.0
        self.dropped: int = 0
        self.reconnects: int = 0
        self.source_name: str = FEED_SOURCE
        self.running: bool = True


async def _health_server(state: _IngestorState, port: int) -> None:
    from market_platform.config import FEED_STALL_SECONDS

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.read(1024)
        except Exception:
            pass
        age = (time.monotonic() - state.last_message_time) if state.last_message_time else float("inf")
        healthy = state.running and (state.last_message_time == 0 or age < FEED_STALL_SECONDS * 2)
        status = "ok" if healthy else "stalled"
        code = b"200 OK" if healthy else b"503 Service Unavailable"
        body = json.dumps(
            {
                "status": status,
                "source": state.source_name,
                "last_message_age_seconds": round(age, 1) if age != float("inf") else None,
                "dropped_events": state.dropped,
                "reconnects": state.reconnects,
            }
        ).encode()
        writer.write(b"HTTP/1.1 " + code + b"\r\nContent-Type: application/json\r\n\r\n" + body)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", port)
    async with server:
        await server.serve_forever()


async def _run(state: _IngestorState) -> None:
    source = make_feed_source(FEED_SOURCE)
    state.source_name = source.name
    LOGGER.info("Starting feed ingestor source=%s topic=%s", source.name, FEED_RAW_TOPIC)

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50_000)
    drop_log_interval = 10.0
    last_drop_log = time.monotonic()

    async def ingest() -> None:
        async for event in source.stream():
            state.last_message_time = time.monotonic()
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                state.dropped += 1
                now = time.monotonic()
                if now - last_drop_log > drop_log_interval:
                    LOGGER.warning("Dropped %d events due to full queue (last %ds)", state.dropped, drop_log_interval)

    async def publish() -> None:
        while state.running or not queue.empty():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            symbol = event.get("symbol", "UNKNOWN")
            payload = dumps(event)
            key = symbol.encode()
            await producer.send_and_wait(FEED_RAW_TOPIC, payload, key=key)
            # Also publish to legacy topic so old feed-handler consumer group still works
            await producer.send_and_wait(SYNTHETIC_RAW_TOPIC, payload, key=key)

    ingest_task = asyncio.create_task(ingest())
    publish_task = asyncio.create_task(publish())

    timeout_task = None
    if FEED_TIMEOUT_SECONDS:
        async def _timeout() -> None:
            await asyncio.sleep(FEED_TIMEOUT_SECONDS)
            LOGGER.info("Feed timeout %.0fs reached — stopping", FEED_TIMEOUT_SECONDS)
            ingest_task.cancel()

        timeout_task = asyncio.create_task(_timeout())

    try:
        await asyncio.gather(ingest_task, publish_task)
    except asyncio.CancelledError:
        pass
    finally:
        state.running = False
        if timeout_task:
            timeout_task.cancel()
        ingest_task.cancel()
        publish_task.cancel()
        await producer.stop()
        LOGGER.info("Feed ingestor stopped. dropped=%d reconnects=%d", state.dropped, state.reconnects)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    state = _IngestorState()

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        LOGGER.info("Shutdown signal received")
        state.running = False
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    health_task = asyncio.create_task(_health_server(state, FEED_INGESTOR_HEALTH_PORT))
    run_task = asyncio.create_task(_run(state))

    await asyncio.gather(health_task, run_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
