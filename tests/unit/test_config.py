from __future__ import annotations

from market_platform.config import env_float, env_int


def test_env_float_reads_default_and_environment(monkeypatch) -> None:
    monkeypatch.delenv("TEST_FLOAT_VALUE", raising=False)
    assert env_float("TEST_FLOAT_VALUE", 0.0) == 0.0

    monkeypatch.setenv("TEST_FLOAT_VALUE", "0.03")
    assert env_float("TEST_FLOAT_VALUE", 0.0) == 0.03


def test_env_int_reads_default_and_environment(monkeypatch) -> None:
    monkeypatch.delenv("TEST_INT_VALUE", raising=False)
    assert env_int("TEST_INT_VALUE", 1000) == 1000

    monkeypatch.setenv("TEST_INT_VALUE", "25")
    assert env_int("TEST_INT_VALUE", 1000) == 25
