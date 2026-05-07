from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timezone
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
class StreamState:
    bars: dict[str, BarState] = field(default_factory=dict)

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
