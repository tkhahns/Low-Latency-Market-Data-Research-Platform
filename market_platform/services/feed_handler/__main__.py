from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from market_platform.config import KAFKA_BOOTSTRAP_SERVERS
from market_platform.events import SequenceTracker, canonical_quote, canonical_trade, quality_alert
from market_platform.serde import dumps, loads
from market_platform.time import utc_now_iso
from market_platform.topics import QUALITY_ALERTS_TOPIC, QUOTES_TOPIC, RAW_TOPIC, SYNTHETIC_RAW_TOPIC, TRADES_TOPIC

LOGGER = logging.getLogger(__name__)


RAW_REQUIRED_FIELDS = {
    "trade": {"event_type", "symbol", "exchange", "event_time", "sequence_number", "price", "size"},
    "quote": {
        "event_type",
        "symbol",
        "exchange",
        "event_time",
        "sequence_number",
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
    },
}


def validate_required(raw: dict) -> None:
    event_type = raw.get("event_type")
    if event_type not in RAW_REQUIRED_FIELDS:
        raise ValueError(f"Unsupported event_type {event_type!r}")
    missing = RAW_REQUIRED_FIELDS[event_type] - raw.keys()
    if missing:
        raise ValueError(f"Missing required fields: {sorted(missing)}")


async def publish_alert(producer: AIOKafkaProducer, raw: dict, message: str) -> None:
    alert = quality_alert(
        symbol=raw.get("symbol", "UNKNOWN"),
        exchange=raw.get("exchange", "UNKNOWN"),
        alert_type="feed_validation_error",
        severity="critical",
        message=message,
        observed_sequence=raw.get("sequence_number"),
    )
    await producer.send_and_wait(QUALITY_ALERTS_TOPIC, dumps(alert), key=alert["symbol"].encode())


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = AIOKafkaConsumer(
        SYNTHETIC_RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="feed-handler-poc",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    tracker = SequenceTracker()

    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            raw = loads(msg.value)
            try:
                validate_required(raw)
                ingest_time = utc_now_iso()
                canonical = canonical_trade(raw, ingest_time) if raw["event_type"] == "trade" else canonical_quote(raw, ingest_time)
                gap_alert = tracker.check(canonical)
                await producer.send_and_wait(RAW_TOPIC, dumps(canonical), key=canonical["symbol"].encode())
                await producer.send_and_wait(
                    TRADES_TOPIC if canonical["event_type"] == "trade" else QUOTES_TOPIC,
                    dumps(canonical),
                    key=canonical["symbol"].encode(),
                )
                if gap_alert:
                    await producer.send_and_wait(QUALITY_ALERTS_TOPIC, dumps(gap_alert), key=canonical["symbol"].encode())
            except Exception as exc:  # noqa: BLE001 - bad feed messages should become data-quality alerts.
                LOGGER.warning("Rejected raw message: %s", exc)
                await publish_alert(producer, raw, str(exc))
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
