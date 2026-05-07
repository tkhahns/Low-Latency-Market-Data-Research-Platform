import asyncio

from market_platform.events import canonical_quote, canonical_trade
from market_platform.redis_keys import active_symbols, alerts, bar_1s, freshness, latest_quote, metrics, top_of_book
from market_platform.serde import loads
from market_platform.services.stream_processor.__main__ import handle_quote, handle_trade
from market_platform.stream_state import StreamState
from market_platform.topics import BARS_1S_TOPIC, QUALITY_ALERTS_TOPIC, ROLLING_METRICS_TOPIC, TOP_OF_BOOK_TOPIC


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

    async def get(self, key):
        return self.values.get(key)

    async def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    async def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, end):
        stop = None if end == -1 else end + 1
        self.lists[key] = self.lists.get(key, [])[start:stop]

    async def expire(self, key, ttl):
        self.expirations[key] = ttl


class FakeProducer:
    def __init__(self):
        self.messages = []

    async def send_and_wait(self, topic, value, key=None):
        self.messages.append((topic, loads(value), key))


def test_handle_quote_updates_redis_and_publishes_top_of_book_and_alert():
    async def run():
        redis = FakeRedis()
        producer = FakeProducer()
        quote = canonical_quote(
            {
                "event_type": "quote",
                "symbol": "AAPL",
                "exchange": "XNAS",
                "event_time": "2026-05-07T00:00:00Z",
                "sequence_number": 1,
                "bid_price": 101.0,
                "bid_size": 100,
                "ask_price": 100.5,
                "ask_size": 100,
            },
            ingest_time="2026-05-07T00:00:00.001Z",
        )

        await handle_quote(redis, producer, StreamState(), quote)

        assert loads(redis.values[latest_quote("AAPL")].encode())["event_type"] == "quote"
        assert loads(redis.values[top_of_book("AAPL")].encode())["spread"] == -0.5
        assert loads(redis.values[freshness("AAPL")].encode())["status"] in {"fresh", "stale"}
        assert redis.sets[active_symbols()] == {"AAPL"}
        assert loads(redis.lists[alerts("AAPL")][0].encode())["alert_type"] == "crossed_market"
        assert [topic for topic, _, _ in producer.messages] == [TOP_OF_BOOK_TOPIC, QUALITY_ALERTS_TOPIC]

    asyncio.run(run())


def test_handle_trade_updates_bar_metrics_and_derived_topics():
    async def run():
        redis = FakeRedis()
        producer = FakeProducer()
        state = StreamState()
        trade = canonical_trade(
            {
                "event_type": "trade",
                "symbol": "AAPL",
                "exchange": "XNAS",
                "event_time": "2026-05-07T00:00:00.100Z",
                "sequence_number": 2,
                "price": 100.0,
                "size": 25,
                "trade_id": "t1",
            },
            ingest_time="2026-05-07T00:00:00.101Z",
        )

        await handle_trade(redis, producer, state, trade)

        assert loads(redis.values[bar_1s("AAPL")].encode())["volume"] == 25
        assert loads(redis.values[metrics("AAPL")].encode())["rolling_volume"] == 25
        assert redis.sets[active_symbols()] == {"AAPL"}
        assert [topic for topic, _, _ in producer.messages] == [BARS_1S_TOPIC, ROLLING_METRICS_TOPIC]

    asyncio.run(run())
