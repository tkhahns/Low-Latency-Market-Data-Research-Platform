from __future__ import annotations

import os


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = env("REDIS_URL", "redis://localhost:6379/0")
EXCHANGE = env("EXCHANGE", "XNAS")
SYMBOLS = [value.strip().upper() for value in env("SYMBOLS", "AAPL,MSFT,NVDA").split(",") if value.strip()]
SIMULATOR_SEQUENCE_GAP_PROBABILITY = env_float("SIMULATOR_SEQUENCE_GAP_PROBABILITY", 0.0)
SEQUENCE_RESTART_RESET_THRESHOLD = env_int("SEQUENCE_RESTART_RESET_THRESHOLD", 1000)
SCHEMA_VERSION = "1.1"

# Feed source selection: synthetic | coinbase | databento | replay
FEED_SOURCE = env("FEED_SOURCE", "synthetic")
FEED_RAW_TOPIC_NAME = env("FEED_RAW_TOPIC_NAME", "feed.raw.v1")

# Coinbase WebSocket feed
COINBASE_WS_URL = env("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
COINBASE_PRODUCTS = [p.strip() for p in env("COINBASE_PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",") if p.strip()]
COINBASE_CHANNELS = [c.strip() for c in env("COINBASE_CHANNELS", "matches,ticker,heartbeat").split(",") if c.strip()]

# Feed ingestor resilience
FEED_STALL_SECONDS = env_float("FEED_STALL_SECONDS", 30.0)
FEED_TIMEOUT_SECONDS_STR = os.getenv("FEED_TIMEOUT_SECONDS")
FEED_TIMEOUT_SECONDS = float(FEED_TIMEOUT_SECONDS_STR) if FEED_TIMEOUT_SECONDS_STR else None
FEED_INGESTOR_HEALTH_PORT = env_int("FEED_INGESTOR_HEALTH_PORT", 8020)

# Replay feed
FEED_REPLAY_FILE = env("FEED_REPLAY_FILE", "/app/market_platform/fixtures/coinbase-sample.jsonl")
FEED_REPLAY_SPEED = env_float("FEED_REPLAY_SPEED", 1.0)
FEED_REPLAY_LOOP = env("FEED_REPLAY_LOOP", "false").lower() == "true"

# Market data API security (comma-separated keys; empty = open mode)
API_KEYS_RAW = env("API_KEYS", "")
API_KEYS: set[str] = {k.strip() for k in API_KEYS_RAW.split(",") if k.strip()}
WS_MAX_CONNECTIONS = env_int("WS_MAX_CONNECTIONS", 500)
CORS_ORIGINS = [o.strip() for o in env("CORS_ORIGINS", "*").split(",") if o.strip()]
