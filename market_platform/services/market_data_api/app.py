from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from redis.asyncio import Redis

from market_platform.config import API_KEYS, CORS_ORIGINS, REDIS_URL, WS_MAX_CONNECTIONS
from market_platform.redis_keys import active_symbols, alerts, bar_1s, freshness, latest_quote, metrics, top_of_book
from market_platform.time import utc_now_iso

STATIC_DIR = Path(__file__).resolve().parents[3] / "apps" / "trader-dashboard" / "static"
OBSIDIAN_VAULT_DIR = Path(__file__).resolve().parents[3] / "obsidian" / "Market Data Research Vault"
OBSIDIAN_HOME_NOTE = OBSIDIAN_VAULT_DIR / "Home.md"

app = FastAPI(title="Market Data API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# --- Rate limiting (token bucket, in-process) ---

class _TokenBucket:
    def __init__(self, capacity: float = 200.0, refill_rate: float = 100.0) -> None:
        self._capacity = capacity
        self._tokens = capacity
        self._refill_rate = refill_rate
        self._last = time.monotonic()

    def consume(self) -> bool:
        now = time.monotonic()
        elapsed = now - self._last
        self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_rate)
        self._last = now
        if self._tokens >= 1:
            self._tokens -= 1
            return True
        return False


_rate_buckets: dict[str, _TokenBucket] = {}


def _check_auth(request: Request) -> Optional[str]:
    """Return the caller identity (key or IP) or raise 401/429."""
    if not API_KEYS:
        return "open"
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if not key or key not in API_KEYS:
        raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key")
    if key not in _rate_buckets:
        _rate_buckets[key] = _TokenBucket()
    if not _rate_buckets[key].consume():
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    return key


# --- Demo / DemoRedis ---

class DemoRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> Optional[str]:
        return self.values.get(key)

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        values = self.lists.get(key, [])
        stop = None if end == -1 else end + 1
        return values[start:stop]

    async def smembers(self, key: str) -> set[str]:
        return self.sets.get(key, set())

    async def aclose(self) -> None:
        return None


def event(symbol: str, exchange: str, sequence_number: int) -> dict[str, Any]:
    now = utc_now_iso()
    return {
        "schema_version": "1.1",
        "symbol": symbol,
        "exchange": exchange,
        "event_time": now,
        "ingest_time": now,
        "sequence_number": sequence_number,
    }


def demo_redis() -> DemoRedis:
    redis = DemoRedis()
    rows = [
        ("ES.FUT", "GLBX", 5324.25, 5324.50, 14, 18, 4281, 5324.38, 1.9),
        ("NQ.FUT", "GLBX", 18872.00, 18872.75, 9, 11, 2910, 18872.42, 2.7),
        ("AAPL", "XNAS", 190.10, 190.12, 100, 200, 500, 190.08, 1.25),
    ]
    redis.sets[active_symbols()] = {row[0] for row in rows}
    for index, (symbol, exchange, bid, ask, bid_size, ask_size, volume, vwap, volatility_bps) in enumerate(rows, start=1):
        base = event(symbol, exchange, 1000 + index)
        redis.values[latest_quote(symbol)] = json.dumps(
            {**base, "event_type": "quote", "bid_price": bid, "bid_size": bid_size, "ask_price": ask, "ask_size": ask_size}
        )
        redis.values[top_of_book(symbol)] = json.dumps(
            {
                **base,
                "event_type": "top_of_book",
                "bid_price": bid,
                "bid_size": bid_size,
                "ask_price": ask,
                "ask_size": ask_size,
                "spread": round(ask - bid, 4),
                "mid_price": round((ask + bid) / 2, 4),
                "processed_time": utc_now_iso(),
            }
        )
        redis.values[bar_1s(symbol)] = json.dumps(
            {
                **base,
                "event_type": "bar_1s",
                "open": round(vwap - 0.15, 4),
                "high": round(ask + 0.10, 4),
                "low": round(bid - 0.10, 4),
                "close": round(vwap + 0.03, 4),
                "volume": volume,
                "vwap": vwap,
            }
        )
        redis.values[metrics(symbol)] = json.dumps(
            {
                **base,
                "event_type": "rolling_metrics",
                "sample_count": 60,
                "rolling_volume": volume * 4,
                "rolling_vwap": vwap,
                "volatility_bps": volatility_bps,
            }
        )
        redis.values[freshness(symbol)] = json.dumps(
            {
                "schema_version": "1.1",
                "symbol": symbol,
                "exchange": exchange,
                "event_time": base["event_time"],
                "last_ingest_time": base["ingest_time"],
                "last_processed_time": utc_now_iso(),
                "freshness_lag_ms": 18 + index,
                "status": "fresh",
            }
        )
    redis.lists[alerts("NQ.FUT")] = [
        json.dumps(
            {
                **event("NQ.FUT", "GLBX", 1010),
                "event_type": "quality_alert",
                "alert_type": "wide_spread",
                "severity": "warning",
                "message": "Spread exceeded configured local demo threshold.",
            }
        )
    ]
    return redis


async def redis_client() -> Redis:
    if os.getenv("MARKET_DATA_DEMO_MODE") == "1":
        return demo_redis()  # type: ignore[return-value]
    return Redis.from_url(REDIS_URL, decode_responses=True)


async def get_redis() -> Redis:
    # Serverless hosts (e.g. Vercel) may invoke the ASGI app without running
    # lifespan startup, so initialize the connection lazily on first use.
    redis = getattr(app.state, "redis", None)
    if redis is None:
        redis = await redis_client()
        app.state.redis = redis
    return redis


async def get_json(redis: Redis, key: str) -> Optional[dict[str, Any]]:
    value = await redis.get(key)
    return json.loads(value) if value else None


async def symbol_snapshot(redis: Redis, symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    recent_alerts = await redis.lrange(alerts(symbol), 0, 9)
    return {
        "symbol": symbol,
        "latest_quote": await get_json(redis, latest_quote(symbol)),
        "top_of_book": await get_json(redis, top_of_book(symbol)),
        "bar_1s": await get_json(redis, bar_1s(symbol)),
        "metrics": await get_json(redis, metrics(symbol)),
        "freshness": await get_json(redis, freshness(symbol)),
        "alerts": [json.loads(item) for item in recent_alerts],
    }


@app.on_event("startup")
async def startup() -> None:
    app.state.redis = await redis_client()
    app.state.ws_connections = 0


@app.on_event("shutdown")
async def shutdown() -> None:
    redis = getattr(app.state, "redis", None)
    if redis is not None:
        await redis.aclose()


@app.get("/")
async def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    redis = await get_redis()
    await redis.ping()
    return {"status": "ok", "time": utc_now_iso()}


@app.get("/symbols")
async def symbols(request: Request) -> dict[str, list[str]]:
    _check_auth(request)
    redis = await get_redis()
    members = await redis.smembers(active_symbols())
    return {"symbols": sorted(members)}


@app.get("/obsidian/project")
async def obsidian_project() -> dict[str, str]:
    return {
        "vault_name": OBSIDIAN_VAULT_DIR.name,
        "vault_path": str(OBSIDIAN_VAULT_DIR),
        "home_note": str(OBSIDIAN_HOME_NOTE),
        "obsidian_uri": f"obsidian://open?path={quote(str(OBSIDIAN_HOME_NOTE))}",
        "source_type": "obsidian",
    }


@app.get("/latest/{symbol}")
async def latest(symbol: str, request: Request) -> dict[str, Any]:
    _check_auth(request)
    return await symbol_snapshot(await get_redis(), symbol)


@app.get("/freshness/{symbol}")
async def symbol_freshness(symbol: str, request: Request) -> Optional[dict[str, Any]]:
    _check_auth(request)
    return await get_json(await get_redis(), freshness(symbol.upper()))


@app.get("/alerts/{symbol}")
async def symbol_alerts(symbol: str, request: Request) -> dict[str, Any]:
    _check_auth(request)
    redis = await get_redis()
    items = await redis.lrange(alerts(symbol.upper()), 0, 24)
    return {"symbol": symbol.upper(), "alerts": [json.loads(item) for item in items]}


async def live_frame(redis: Redis) -> dict[str, Any]:
    members = sorted(await redis.smembers(active_symbols()))
    snapshots = [await symbol_snapshot(redis, symbol) for symbol in members]
    return {"channel": "live", "time": utc_now_iso(), "symbols": snapshots}


@app.get("/live/snapshot")
async def live_snapshot(request: Request) -> dict[str, Any]:
    """One live frame over REST, for hosts without WebSocket support (e.g. Vercel)."""
    _check_auth(request)
    return await live_frame(await get_redis())


@app.websocket("/ws/live")
async def live(websocket: WebSocket) -> None:
    if getattr(app.state, "ws_connections", 0) >= WS_MAX_CONNECTIONS:
        await websocket.close(code=1008, reason="Connection limit reached")
        return
    app.state.ws_connections = getattr(app.state, "ws_connections", 0) + 1
    await websocket.accept()
    try:
        redis = await get_redis()
        while True:
            await websocket.send_json(await live_frame(redis))
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        return
    finally:
        app.state.ws_connections = max(0, getattr(app.state, "ws_connections", 1) - 1)
