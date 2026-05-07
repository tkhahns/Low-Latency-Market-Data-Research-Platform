import json

from fastapi.testclient import TestClient

from market_platform.redis_keys import active_symbols, alerts, bar_1s, freshness, latest_quote, metrics, top_of_book
from market_platform.services.market_data_api.app import app


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}
        self.sets = {}

    async def ping(self):
        return True

    async def get(self, key):
        return self.values.get(key)

    async def lrange(self, key, start, end):
        values = self.lists.get(key, [])
        stop = None if end == -1 else end + 1
        return values[start:stop]

    async def smembers(self, key):
        return self.sets.get(key, set())


def payload(**overrides):
    value = {
        "schema_version": "1.0",
        "symbol": "AAPL",
        "exchange": "XNAS",
        "event_time": "2026-05-07T00:00:00Z",
        "ingest_time": "2026-05-07T00:00:00.010000Z",
        "sequence_number": 42,
    }
    value.update(overrides)
    return value


def install_fake_redis():
    redis = FakeRedis()
    redis.sets[active_symbols()] = {"AAPL"}
    redis.values[latest_quote("AAPL")] = json.dumps(
        payload(event_type="quote", bid_price=190.1, bid_size=100, ask_price=190.12, ask_size=200)
    )
    redis.values[top_of_book("AAPL")] = json.dumps(
        payload(event_type="top_of_book", bid_price=190.1, ask_price=190.12, spread=0.02, mid_price=190.11)
    )
    redis.values[bar_1s("AAPL")] = json.dumps(
        payload(event_type="bar_1s", open=190.0, high=190.2, low=189.9, close=190.1, volume=500, vwap=190.08)
    )
    redis.values[metrics("AAPL")] = json.dumps(
        payload(event_type="rolling_metrics", sample_count=3, rolling_volume=900, rolling_vwap=190.05, volatility_bps=1.25)
    )
    redis.values[freshness("AAPL")] = json.dumps(
        {
            "symbol": "AAPL",
            "last_event_time": "2026-05-07T00:00:00Z",
            "updated_at": "2026-05-07T00:00:00.020000Z",
            "lag_ms": 20,
            "status": "fresh",
        }
    )
    redis.lists[alerts("AAPL")] = [
        json.dumps(payload(event_type="quality_alert", alert_type="wide_spread", severity="warning", message="wide"))
    ]
    app.state.redis = redis
    return redis


def test_rest_routes_read_stable_redis_keys():
    install_fake_redis()
    client = TestClient(app)

    assert client.get("/health").json()["status"] == "ok"
    assert client.get("/symbols").json() == {"symbols": ["AAPL"]}

    latest = client.get("/latest/aapl").json()
    assert latest["symbol"] == "AAPL"
    assert latest["top_of_book"]["spread"] == 0.02
    assert latest["bar_1s"]["volume"] == 500
    assert latest["metrics"]["rolling_vwap"] == 190.05
    assert latest["freshness"]["status"] == "fresh"
    assert latest["alerts"][0]["alert_type"] == "wide_spread"

    assert client.get("/freshness/aapl").json()["lag_ms"] == 20
    assert client.get("/alerts/aapl").json()["alerts"][0]["severity"] == "warning"


def test_dashboard_and_websocket_live_frame_are_served():
    install_fake_redis()
    client = TestClient(app)

    dashboard = client.get("/")
    assert dashboard.status_code == 200
    assert "Market Data" in dashboard.text

    with client.websocket_connect("/ws/live") as websocket:
        frame = websocket.receive_json()

    assert frame["channel"] == "live"
    assert frame["symbols"][0]["symbol"] == "AAPL"
    assert frame["symbols"][0]["metrics"]["sample_count"] == 3
