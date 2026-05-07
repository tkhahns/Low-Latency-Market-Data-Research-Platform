from __future__ import annotations

import os


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = env("REDIS_URL", "redis://localhost:6379/0")
EXCHANGE = env("EXCHANGE", "XNAS")
SYMBOLS = [value.strip().upper() for value in env("SYMBOLS", "AAPL,MSFT,NVDA").split(",") if value.strip()]
SCHEMA_VERSION = "1.0"

