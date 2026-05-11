from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import databento as db
from aiokafka import AIOKafkaProducer

from market_platform.config import EXCHANGE, KAFKA_BOOTSTRAP_SERVERS
from market_platform.databento_adapter import raw_events_from_databento
from market_platform.serde import dumps
from market_platform.topics import SYNTHETIC_RAW_TOPIC

LOGGER = logging.getLogger(__name__)


def csv_env(name: str, default: str) -> list[str]:
    return [value.strip() for value in os.getenv(name, default).split(",") if value.strip()]


async def publish_databento_events() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dataset = os.getenv("DATABENTO_DATASET", "GLBX.MDP3")
    symbols = csv_env("DATABENTO_SYMBOLS", "ES.FUT,NQ.FUT")
    schemas = csv_env("DATABENTO_SCHEMAS", "mbp-1,trades")
    stype_in = os.getenv("DATABENTO_STYPE_IN", "parent")
    replay_start = os.getenv("DATABENTO_REPLAY_START") or None
    timeout_value = os.getenv("DATABENTO_TIMEOUT_SECONDS")
    timeout_seconds = float(timeout_value) if timeout_value else None

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50_000)
    loop = asyncio.get_running_loop()
    symbol_map: dict[int, str] = {}

    def enqueue(event: dict[str, Any]) -> None:
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            LOGGER.warning("Dropping Databento event because adapter queue is full")

    def on_record(record: Any) -> None:
        for event in raw_events_from_databento(record, symbol_map, EXCHANGE):
            loop.call_soon_threadsafe(enqueue, event)

    client = db.Live()
    for schema in schemas:
        LOGGER.info("Subscribing to Databento dataset=%s schema=%s symbols=%s", dataset, schema, symbols)
        client.subscribe(dataset=dataset, schema=schema, symbols=symbols, stype_in=stype_in, start=replay_start)
    client.add_callback(on_record)
    client.start()

    async def stop_after_timeout() -> None:
        if timeout_seconds is None:
            return
        await asyncio.sleep(timeout_seconds)
        LOGGER.info("Stopping Databento stream after %.2f seconds", timeout_seconds)
        client.stop()

    async def wait_for_close() -> None:
        await client.wait_for_close()
        await queue.put({"event_type": "adapter_closed"})

    timeout_task = asyncio.create_task(stop_after_timeout())
    close_task = asyncio.create_task(wait_for_close())
    try:
        while True:
            event = await queue.get()
            if event["event_type"] == "adapter_closed":
                return
            await producer.send_and_wait(SYNTHETIC_RAW_TOPIC, dumps(event), key=event["symbol"].encode())
    finally:
        timeout_task.cancel()
        close_task.cancel()
        client.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(publish_databento_events())
