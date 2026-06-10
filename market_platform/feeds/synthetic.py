from __future__ import annotations

import asyncio
import random
from typing import Any, AsyncIterator
from uuid import uuid4

from market_platform.config import EXCHANGE, SIMULATOR_SEQUENCE_GAP_PROBABILITY, SYMBOLS
from market_platform.time import utc_now_iso


class SyntheticFeedSource:
    name = "synthetic"

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        prices = {symbol: random.uniform(120, 500) for symbol in SYMBOLS}
        sequences = {symbol: 0 for symbol in SYMBOLS}
        while True:
            for symbol in SYMBOLS:
                sequences[symbol] += 1
                if random.random() < SIMULATOR_SEQUENCE_GAP_PROBABILITY:
                    sequences[symbol] += 1

                prices[symbol] = max(1.0, prices[symbol] + random.uniform(-0.25, 0.25))
                mid = round(prices[symbol], 2)
                spread = random.choice([0.01, 0.02, 0.03, 0.08])
                event_time = utc_now_iso()

                yield {
                    "event_type": "quote",
                    "symbol": symbol,
                    "exchange": EXCHANGE,
                    "event_time": event_time,
                    "sequence_number": sequences[symbol],
                    "bid_price": round(mid - spread / 2, 2),
                    "bid_size": random.randint(100, 2000),
                    "ask_price": round(mid + spread / 2, 2),
                    "ask_size": random.randint(100, 2000),
                }

                if random.random() < 0.65:
                    sequences[symbol] += 1
                    yield {
                        "event_type": "trade",
                        "symbol": symbol,
                        "exchange": EXCHANGE,
                        "event_time": event_time,
                        "sequence_number": sequences[symbol],
                        "price": round(mid + random.uniform(-spread / 2, spread / 2), 2),
                        "size": random.randint(1, 1000),
                        "trade_id": str(uuid4()),
                        "conditions": [],
                    }
            await asyncio.sleep(0.25)
