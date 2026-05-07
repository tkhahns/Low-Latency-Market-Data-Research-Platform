import asyncio

from market_platform.redis_keys import active_symbols, alerts, bar_1s, freshness, metrics, top_of_book
from market_platform.serde import loads
from market_platform.tools.rebuild_redis_from_kafka import write_event


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}
        self.sets = {}
        self.expirations = {}

    async def set(self, key, value, ex=None):
        self.values[key] = value
        if ex is not None:
            self.expirations[key] = ex

    async def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    async def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, end):
        stop = None if end == -1 else end + 1
        self.lists[key] = self.lists.get(key, [])[start:stop]

    async def expire(self, key, ttl):
        self.expirations[key] = ttl


def base_event(event_type):
    return {
        "schema_version": "1.0",
        "event_type": event_type,
        "symbol": "AAPL",
        "exchange": "XNAS",
        "event_time": "2026-05-07T00:00:00Z",
        "ingest_time": "2026-05-07T00:00:00.001Z",
        "processed_time": "2026-05-07T00:00:00.002Z",
        "sequence_number": 1,
    }


def test_rebuild_writer_restores_hot_state_keys():
    async def run():
        redis = FakeRedis()

        top_key = await write_event(redis, {**base_event("top_of_book"), "spread": 0.01}, dry_run=False)
        bar_key = await write_event(redis, {**base_event("bar_1s"), "volume": 100}, dry_run=False)
        metric_key = await write_event(redis, {**base_event("rolling_metrics"), "rolling_volume": 100}, dry_run=False)

        assert top_key == top_of_book("AAPL")
        assert bar_key == bar_1s("AAPL")
        assert metric_key == metrics("AAPL")
        assert loads(redis.values[top_of_book("AAPL")].encode())["event_type"] == "top_of_book"
        assert loads(redis.values[freshness("AAPL")].encode())["status"] == "rebuilt"
        assert loads(redis.values[bar_1s("AAPL")].encode())["volume"] == 100
        assert loads(redis.values[metrics("AAPL")].encode())["rolling_volume"] == 100
        assert redis.sets[active_symbols()] == {"AAPL"}

    asyncio.run(run())


def test_rebuild_writer_handles_alerts_and_dry_run():
    async def run():
        redis = FakeRedis()
        alert = {**base_event("quality_alert"), "alert_type": "sequence_gap", "severity": "critical", "message": "gap"}

        assert await write_event(redis, alert, dry_run=True) == alerts("AAPL")
        assert redis.lists == {}

        assert await write_event(redis, alert, dry_run=False) == alerts("AAPL")
        assert loads(redis.lists[alerts("AAPL")][0].encode())["alert_type"] == "sequence_gap"
        assert redis.expirations[alerts("AAPL")] == 3600

    asyncio.run(run())
