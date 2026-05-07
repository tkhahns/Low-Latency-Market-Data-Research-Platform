from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timezone
from math import log, sqrt
from typing import Any

from .events import quality_alert
from .time import parse_utc, utc_now_iso


@dataclass
class BarState:
    window_start: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    notional: float

    def update(self, price: float, size: int) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += size
        self.notional += price * size

    def as_event(self, trade: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "event_type": "bar_1s",
            "symbol": trade["symbol"],
            "exchange": trade["exchange"],
            "event_time": self.window_start,
            "ingest_time": utc_now_iso(),
            "sequence_number": trade["sequence_number"],
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "vwap": self.notional / self.volume if self.volume else self.close,
        }


@dataclass
class SymbolWindow:
    prices: list[tuple[str, float]] = field(default_factory=list)
    volume: int = 0
    notional: float = 0.0

    def add_trade(self, event_time: str, price: float, size: int, max_points: int = 120) -> None:
        self.prices.append((event_time, price))
        self.prices = self.prices[-max_points:]
        self.volume += size
        self.notional += price * size

    def metrics(self, trade: dict[str, Any]) -> dict[str, Any]:
        returns = []
        for index in range(1, len(self.prices)):
            previous = self.prices[index - 1][1]
            current = self.prices[index][1]
            if previous > 0 and current > 0:
                returns.append(log(current / previous))
        if len(returns) > 1:
            mean = sum(returns) / len(returns)
            variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
            volatility_bps = sqrt(variance) * 10000
        else:
            volatility_bps = 0.0
        return {
            "schema_version": "1.0",
            "event_type": "rolling_metrics",
            "symbol": trade["symbol"],
            "exchange": trade["exchange"],
            "event_time": trade["event_time"],
            "ingest_time": utc_now_iso(),
            "sequence_number": trade["sequence_number"],
            "sample_count": len(self.prices),
            "rolling_volume": self.volume,
            "rolling_vwap": self.notional / self.volume if self.volume else float(trade["price"]),
            "volatility_bps": round(volatility_bps, 6),
        }


@dataclass
class StreamState:
    bars: dict[str, BarState] = field(default_factory=dict)
    windows: dict[str, SymbolWindow] = field(default_factory=dict)

    def quote_to_top_of_book(self, quote: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        spread = round(float(quote["ask_price"]) - float(quote["bid_price"]), 6)
        mid = round((float(quote["ask_price"]) + float(quote["bid_price"])) / 2, 6)
        now = utc_now_iso()
        top = {
            "schema_version": "1.0",
            "event_type": "top_of_book",
            "symbol": quote["symbol"],
            "exchange": quote["exchange"],
            "event_time": quote["event_time"],
            "ingest_time": quote["ingest_time"],
            "processed_time": now,
            "sequence_number": quote["sequence_number"],
            "bid_price": quote["bid_price"],
            "bid_size": quote["bid_size"],
            "ask_price": quote["ask_price"],
            "ask_size": quote["ask_size"],
            "spread": spread,
            "mid_price": mid,
        }
        alerts = []
        if spread < 0:
            alerts.append(
                quality_alert(
                    symbol=quote["symbol"],
                    exchange=quote["exchange"],
                    alert_type="crossed_market",
                    severity="critical",
                    message="Ask price is below bid price.",
                    observed_sequence=quote["sequence_number"],
                )
            )
        elif spread > max(0.05, mid * 0.002):
            alerts.append(
                quality_alert(
                    symbol=quote["symbol"],
                    exchange=quote["exchange"],
                    alert_type="wide_spread",
                    severity="warning",
                    message=f"Spread {spread} exceeded configured threshold.",
                    observed_sequence=quote["sequence_number"],
                )
            )
        return top, alerts

    def trade_to_bar(self, trade: dict[str, Any]) -> dict[str, Any]:
        event_time = parse_utc(trade["event_time"]).astimezone(timezone.utc).replace(microsecond=0)
        window_start = event_time.isoformat().replace("+00:00", "Z")
        key = f"{trade['symbol']}:{window_start}"
        price = float(trade["price"])
        size = int(trade["size"])
        if key not in self.bars:
            self.bars[key] = BarState(
                window_start=window_start,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=size,
                notional=price * size,
            )
        else:
            self.bars[key].update(price, size)
        return self.bars[key].as_event(trade)

    def trade_to_metrics(self, trade: dict[str, Any]) -> dict[str, Any]:
        key = f"{trade['symbol']}:{trade['exchange']}"
        price = float(trade["price"])
        size = int(trade["size"])
        if key not in self.windows:
            self.windows[key] = SymbolWindow()
        self.windows[key].add_trade(trade["event_time"], price, size)
        return self.windows[key].metrics(trade)
