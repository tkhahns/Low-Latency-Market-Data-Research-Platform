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

# Research intelligence pipeline
RESEARCH_SOURCES = [s.strip() for s in env("RESEARCH_SOURCES", "arxiv,rss").split(",") if s.strip()]
RESEARCH_POLL_SECONDS = env_int("RESEARCH_POLL_SECONDS", 900)
RESEARCH_RSS_FEEDS = [
    f.strip()
    for f in env(
        "RESEARCH_RSS_FEEDS",
        "https://feeds.feedburner.com/CoinDesk,"
        "https://cointelegraph.com/rss,"
        "https://decrypt.co/feed",
    ).split(",")
    if f.strip()
]
RESEARCH_SYMBOL_MAP_JSON = os.getenv("RESEARCH_SYMBOL_MAP")  # JSON override; None = use built-in
RESEARCH_EXTRACTOR = env("RESEARCH_EXTRACTOR", "auto")  # auto | rule_based | claude
RESEARCH_LLM_MODEL = env("RESEARCH_LLM_MODEL", "claude-opus-4-8")
RESEARCH_LLM_DAILY_BUDGET_USD = env_float("RESEARCH_LLM_DAILY_BUDGET_USD", 1.00)
RESEARCH_EDGAR_USER_AGENT = os.getenv("RESEARCH_EDGAR_USER_AGENT")  # "name email" format; None = edgar disabled
RESEARCH_INGESTOR_HEALTH_PORT = env_int("RESEARCH_INGESTOR_HEALTH_PORT", 8030)
RESEARCH_REPLAY_FILE = env("RESEARCH_REPLAY_FILE", "/app/market_platform/fixtures/research-sample.jsonl")
RESEARCH_TIMEOUT_SECONDS_STR = os.getenv("RESEARCH_TIMEOUT_SECONDS")
RESEARCH_TIMEOUT_SECONDS = float(RESEARCH_TIMEOUT_SECONDS_STR) if RESEARCH_TIMEOUT_SECONDS_STR else None
