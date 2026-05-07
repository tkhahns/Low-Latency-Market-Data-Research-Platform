from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from redis.asyncio import Redis

from market_platform.config import REDIS_URL
from market_platform.redis_keys import active_symbols, alerts, bar_1s, freshness, latest_quote, metrics, top_of_book
from market_platform.time import utc_now_iso

STATIC_DIR = Path(__file__).resolve().parents[3] / "apps" / "trader-dashboard" / "static"

app = FastAPI(title="Market Data API", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


async def redis_client() -> Redis:
    return Redis.from_url(REDIS_URL, decode_responses=True)


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


@app.on_event("shutdown")
async def shutdown() -> None:
    await app.state.redis.aclose()


@app.get("/")
async def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    await app.state.redis.ping()
    return {"status": "ok", "time": utc_now_iso()}


@app.get("/symbols")
async def symbols() -> dict[str, list[str]]:
    members = await app.state.redis.smembers(active_symbols())
    return {"symbols": sorted(members)}


@app.get("/latest/{symbol}")
async def latest(symbol: str) -> dict[str, Any]:
    return await symbol_snapshot(app.state.redis, symbol)


@app.get("/freshness/{symbol}")
async def symbol_freshness(symbol: str) -> Optional[dict[str, Any]]:
    return await get_json(app.state.redis, freshness(symbol.upper()))


@app.get("/alerts/{symbol}")
async def symbol_alerts(symbol: str) -> dict[str, Any]:
    items = await app.state.redis.lrange(alerts(symbol.upper()), 0, 24)
    return {"symbol": symbol.upper(), "alerts": [json.loads(item) for item in items]}


@app.websocket("/ws/live")
async def live(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            members = sorted(await app.state.redis.smembers(active_symbols()))
            snapshots = [await symbol_snapshot(app.state.redis, symbol) for symbol in members]
            await websocket.send_json({"channel": "live", "time": utc_now_iso(), "symbols": snapshots})
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        return
