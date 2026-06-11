import json
import os

import pytest
from fastapi.testclient import TestClient

from market_platform.redis_keys import (
    active_symbols,
    alerts,
    bar_1s,
    freshness,
    latest_quote,
    metrics,
    research_digest,
    research_symbol,
    top_of_book,
)
from market_platform.services.market_data_api.app import _rate_buckets, app, demo_redis


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
        "schema_version": "1.1",
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
    assert "Open Obsidian" in dashboard.text

    with client.websocket_connect("/ws/live") as websocket:
        frame = websocket.receive_json()

    assert frame["channel"] == "live"
    assert frame["symbols"][0]["symbol"] == "AAPL"
    assert frame["symbols"][0]["metrics"]["sample_count"] == 3


def test_obsidian_project_endpoint_points_to_repo_vault():
    install_fake_redis()
    client = TestClient(app)

    project = client.get("/obsidian/project").json()

    assert project["vault_name"] == "Market Data Research Vault"
    assert project["source_type"] == "obsidian"
    assert project["home_note"].endswith("obsidian/Market Data Research Vault/Home.md")
    assert project["obsidian_uri"].startswith("obsidian://open?path=")


def test_demo_mode_seed_contains_market_dashboard_state():
    app.state.redis = demo_redis()
    client = TestClient(app)

    symbols = client.get("/symbols").json()["symbols"]
    assert "ES.FUT" in symbols
    assert "NQ.FUT" in symbols

    latest = client.get("/latest/ES.FUT").json()
    assert latest["top_of_book"]["exchange"] == "GLBX"
    assert latest["bar_1s"]["volume"] > 0
    assert latest["freshness"]["status"] == "fresh"


def test_live_snapshot_rest_endpoint_matches_ws_frame_shape():
    install_fake_redis()
    client = TestClient(app)

    frame = client.get("/live/snapshot").json()

    assert frame["channel"] == "live"
    assert frame["symbols"][0]["symbol"] == "AAPL"
    assert frame["symbols"][0]["metrics"]["sample_count"] == 3


@pytest.fixture(autouse=False)
def with_api_keys(monkeypatch):
    """Enable API key enforcement for the duration of a test."""
    import market_platform.config as cfg
    import market_platform.services.market_data_api.app as api_app
    monkeypatch.setattr(cfg, "API_KEYS", {"test-key-1"})
    monkeypatch.setattr(api_app, "API_KEYS", {"test-key-1"})
    _rate_buckets.clear()
    yield
    _rate_buckets.clear()


def test_api_key_required_when_configured(with_api_keys):
    install_fake_redis()
    client = TestClient(app, raise_server_exceptions=False)

    assert client.get("/latest/aapl").status_code == 401
    assert client.get("/symbols").status_code == 401
    assert client.get("/freshness/aapl").status_code == 401
    assert client.get("/alerts/aapl").status_code == 401


def test_valid_api_key_grants_access(with_api_keys):
    install_fake_redis()
    client = TestClient(app)

    resp = client.get("/latest/aapl", headers={"X-API-Key": "test-key-1"})
    assert resp.status_code == 200
    assert resp.json()["symbol"] == "AAPL"


def test_health_endpoint_is_always_open(with_api_keys):
    install_fake_redis()
    client = TestClient(app)
    assert client.get("/health").status_code == 200


def test_api_key_via_query_param(with_api_keys):
    install_fake_redis()
    client = TestClient(app)
    resp = client.get("/symbols?api_key=test-key-1")
    assert resp.status_code == 200


def test_live_snapshot_requires_api_key_when_configured(with_api_keys):
    install_fake_redis()
    client = TestClient(app, raise_server_exceptions=False)

    assert client.get("/live/snapshot").status_code == 401
    assert client.get("/live/snapshot?api_key=test-key-1").status_code == 200


# ---------------------------------------------------------------------------
# Research endpoints
# ---------------------------------------------------------------------------


def test_research_symbol_returns_empty_when_no_data():
    install_fake_redis()
    client = TestClient(app)

    resp = client.get("/research/AAPL")
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["insights"] == []


def test_research_digest_returns_empty_when_no_data():
    install_fake_redis()
    client = TestClient(app)

    resp = client.get("/research/digest")
    assert resp.status_code == 200
    assert resp.json()["insights"] == []


def test_research_symbol_returns_seeded_data():
    redis = install_fake_redis()
    sample_insight = {
        "schema_version": "1.0",
        "doc_id": "sha256:test001",
        "summary": "Bitcoin ETF inflows hit record.",
        "symbols": ["BTC-USD"],
        "tags": ["etf-flows"],
        "sentiment": "bullish",
        "entities": ["BlackRock"],
        "extractor": "rule_based",
        "source": "rss",
        "source_uri": "https://example.com",
        "title": "BTC ETF record",
        "published_time": "2026-06-05T00:00:00+00:00",
        "extracted_time": "2026-06-05T01:00:00+00:00",
    }
    redis.values[research_symbol("BTC-USD")] = json.dumps([sample_insight])
    app.state.redis = redis
    client = TestClient(app)

    resp = client.get("/research/BTC-USD")
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "BTC-USD"
    assert len(body["insights"]) == 1
    assert body["insights"][0]["sentiment"] == "bullish"


def test_research_digest_returns_seeded_data():
    redis = install_fake_redis()
    sample_insight = {
        "schema_version": "1.0",
        "doc_id": "sha256:digest001",
        "summary": "Cross-symbol digest entry.",
        "symbols": ["ETH-USD"],
        "tags": ["defi"],
        "sentiment": "neutral",
        "entities": [],
        "extractor": "rule_based",
        "source": "arxiv",
        "source_uri": "https://arxiv.org/abs/test",
        "title": "ETH paper",
        "published_time": "2026-06-06T00:00:00+00:00",
        "extracted_time": "2026-06-06T01:00:00+00:00",
    }
    redis.values[research_digest()] = json.dumps([sample_insight])
    app.state.redis = redis
    client = TestClient(app)

    resp = client.get("/research/digest")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["insights"]) == 1
    assert body["insights"][0]["symbols"] == ["ETH-USD"]


def test_demo_mode_research_panel_seeded():
    app.state.redis = demo_redis()
    client = TestClient(app)

    btc = client.get("/research/BTC-USD").json()
    assert len(btc["insights"]) >= 1
    assert any(i["sentiment"] == "bullish" for i in btc["insights"])

    digest = client.get("/research/digest").json()
    assert len(digest["insights"]) >= 3


def test_research_requires_api_key_when_configured(with_api_keys):
    install_fake_redis()
    client = TestClient(app, raise_server_exceptions=False)

    assert client.get("/research/BTC-USD").status_code == 401
    assert client.get("/research/digest").status_code == 401
    assert client.get("/research/BTC-USD?api_key=test-key-1").status_code == 200
    assert client.get("/research/digest?api_key=test-key-1").status_code == 200
