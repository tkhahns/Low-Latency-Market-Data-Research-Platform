from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from redis.asyncio import Redis

from market_platform.config import KAFKA_BOOTSTRAP_SERVERS, REDIS_URL
from market_platform.events import quality_alert
from market_platform.redis_keys import active_symbols, alerts, bar_1s, freshness, latest_quote, metrics, top_of_book
from market_platform.serde import dumps, loads
from market_platform.stream_state import StreamState
from market_platform.time import parse_utc, utc_now_iso
from market_platform.topics import BARS_1S_TOPIC, QUALITY_ALERTS_TOPIC, QUOTES_TOPIC, ROLLING_METRICS_TOPIC, TOP_OF_BOOK_TOPIC, TRADES_TOPIC

LOGGER = logging.getLogger(__name__)
FRESHNESS_STALE_MS = 2000


async def write_json(redis: Redis, key: str, value: dict, ttl: int = 120) -> None:
    await redis.set(key, dumps(value).decode("utf-8"), ex=ttl)


async def handle_alert(redis: Redis, event: dict) -> None:
    key = alerts(event["symbol"])
    await redis.lpush(key, dumps(event).decode("utf-8"))
    await redis.ltrim(key, 0, 24)
    await redis.expire(key, 3600)
    await redis.sadd(active_symbols(), event["symbol"])


async def handle_quote(redis: Redis, producer: AIOKafkaProducer, state: StreamState, quote: dict) -> None:
    top, generated_alerts = state.quote_to_top_of_book(quote)
    lag_ms = max(0, int((parse_utc(utc_now_iso()) - parse_utc(quote["event_time"])).total_seconds() * 1000))
    fresh = {
        "schema_version": "1.0",
        "symbol": quote["symbol"],
        "exchange": quote["exchange"],
        "event_time": quote["event_time"],
        "last_ingest_time": quote["ingest_time"],
        "last_processed_time": top["processed_time"],
        "freshness_lag_ms": lag_ms,
        "status": "stale" if lag_ms > FRESHNESS_STALE_MS else "fresh",
    }
    await write_json(redis, latest_quote(quote["symbol"]), quote)
    await write_json(redis, top_of_book(quote["symbol"]), top)
    await write_json(redis, freshness(quote["symbol"]), fresh)
    await redis.sadd(active_symbols(), quote["symbol"])
    await producer.send_and_wait(TOP_OF_BOOK_TOPIC, dumps(top), key=quote["symbol"].encode())
    for alert in generated_alerts:
        await producer.send_and_wait(QUALITY_ALERTS_TOPIC, dumps(alert), key=quote["symbol"].encode())
        await handle_alert(redis, alert)


async def handle_trade(redis: Redis, producer: AIOKafkaProducer, state: StreamState, trade: dict) -> None:
    bar = state.trade_to_bar(trade)
    derived_metrics = state.trade_to_metrics(trade)
    await write_json(redis, bar_1s(trade["symbol"]), bar)
    await write_json(redis, metrics(trade["symbol"]), derived_metrics)
    await redis.sadd(active_symbols(), trade["symbol"])
    await producer.send_and_wait(BARS_1S_TOPIC, dumps(bar), key=trade["symbol"].encode())
    await producer.send_and_wait(ROLLING_METRICS_TOPIC, dumps(derived_metrics), key=trade["symbol"].encode())


async def monitor_stale_symbols(redis: Redis, producer: AIOKafkaProducer) -> None:
    last_alerted: dict[str, int] = {}
    while True:
        for symbol in sorted(await redis.smembers(active_symbols())):
            fresh = await redis.get(freshness(symbol))
            if not fresh:
                continue
            event = loads(fresh.encode("utf-8"))
            lag_ms = max(0, int((parse_utc(utc_now_iso()) - parse_utc(event["event_time"])).total_seconds() * 1000))
            if lag_ms <= FRESHNESS_STALE_MS:
                last_alerted.pop(symbol, None)
                continue
            previous = last_alerted.get(symbol, 0)
            if lag_ms - previous < FRESHNESS_STALE_MS:
                continue
            alert = quality_alert(
                symbol=symbol,
                exchange=event["exchange"],
                alert_type="stale_symbol",
                severity="warning",
                message=f"{symbol} freshness lag is {lag_ms} ms.",
            )
            await producer.send_and_wait(QUALITY_ALERTS_TOPIC, dumps(alert), key=symbol.encode())
            await handle_alert(redis, alert)
            await write_json(redis, freshness(symbol), {**event, "freshness_lag_ms": lag_ms, "status": "stale"})
            last_alerted[symbol] = lag_ms
        await asyncio.sleep(1.0)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = AIOKafkaConsumer(
        QUOTES_TOPIC,
        TRADES_TOPIC,
        QUALITY_ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="stream-processor-poc",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    state = StreamState()

    await consumer.start()
    await producer.start()
    stale_task = asyncio.create_task(monitor_stale_symbols(redis, producer))
    try:
        async for msg in consumer:
            event = loads(msg.value)
            if event["event_type"] == "quote":
                await handle_quote(redis, producer, state, event)
            elif event["event_type"] == "trade":
                await handle_trade(redis, producer, state, event)
            elif event["event_type"] == "quality_alert":
                await handle_alert(redis, event)
    finally:
        stale_task.cancel()
        await consumer.stop()
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
