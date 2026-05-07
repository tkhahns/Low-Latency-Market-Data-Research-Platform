from __future__ import annotations

import argparse
import asyncio
import time

from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis

from market_platform.config import KAFKA_BOOTSTRAP_SERVERS, REDIS_URL
from market_platform.redis_keys import active_symbols, alerts, bar_1s, freshness, metrics, top_of_book
from market_platform.serde import dumps, loads
from market_platform.topics import BARS_1S_TOPIC, QUALITY_ALERTS_TOPIC, ROLLING_METRICS_TOPIC, TOP_OF_BOOK_TOPIC


async def write_event(redis: Redis, event: dict, dry_run: bool) -> str:
    symbol = event["symbol"]
    event_type = event["event_type"]
    if event_type == "top_of_book":
        key = top_of_book(symbol)
    elif event_type == "bar_1s":
        key = bar_1s(symbol)
    elif event_type == "rolling_metrics":
        key = metrics(symbol)
    elif event_type == "quality_alert":
        key = alerts(symbol)
    else:
        return "skipped"

    if dry_run:
        return key

    await redis.sadd(active_symbols(), symbol)
    if event_type == "quality_alert":
        await redis.lpush(key, dumps(event).decode("utf-8"))
        await redis.ltrim(key, 0, 24)
        await redis.expire(key, 3600)
    else:
        await redis.set(key, dumps(event).decode("utf-8"), ex=120)
        if event_type == "top_of_book":
            await redis.set(
                freshness(symbol),
                dumps(
                    {
                        "schema_version": "1.0",
                        "symbol": symbol,
                        "exchange": event["exchange"],
                        "event_time": event["event_time"],
                        "last_ingest_time": event["ingest_time"],
                        "last_processed_time": event["processed_time"],
                        "freshness_lag_ms": 0,
                        "status": "rebuilt",
                    }
                ).decode("utf-8"),
                ex=120,
            )
    return key


async def run(args: argparse.Namespace) -> None:
    redis = Redis.from_url(args.redis_url, decode_responses=True)
    consumer = AIOKafkaConsumer(
        TOP_OF_BOOK_TOPIC,
        BARS_1S_TOPIC,
        ROLLING_METRICS_TOPIC,
        QUALITY_ALERTS_TOPIC,
        bootstrap_servers=args.kafka_bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    written = 0
    await consumer.start()
    try:
        idle_started = time.monotonic()
        while (time.monotonic() - idle_started) * 1000 < args.idle_timeout_ms:
            batches = await consumer.getmany(timeout_ms=1000)
            if not batches:
                continue
            idle_started = time.monotonic()
            for records in batches.values():
                for message in records:
                    event = loads(message.value)
                    if args.symbol and event.get("symbol") != args.symbol.upper():
                        continue
                    key = await write_event(redis, event, args.dry_run)
                    if key != "skipped":
                        written += 1
    finally:
        await consumer.stop()
        await redis.aclose()
    mode = "dry_run" if args.dry_run else "write"
    print(f"mode={mode} events_considered={written}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild Redis hot state from derived Kafka topics.")
    parser.add_argument("--kafka-bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--redis-url", default=REDIS_URL)
    parser.add_argument("--group-id", default="redis-rebuild-once")
    parser.add_argument("--symbol")
    parser.add_argument("--idle-timeout-ms", type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
