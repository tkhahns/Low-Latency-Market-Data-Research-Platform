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
SCHEMA_VERSION = "1.0"
