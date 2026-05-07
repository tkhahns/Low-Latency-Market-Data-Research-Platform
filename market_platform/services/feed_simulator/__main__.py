from __future__ import annotations

import asyncio
import random
from uuid import uuid4

from aiokafka import AIOKafkaProducer

from market_platform.config import EXCHANGE, KAFKA_BOOTSTRAP_SERVERS, SYMBOLS
from market_platform.serde import dumps
from market_platform.time import utc_now_iso
from market_platform.topics import SYNTHETIC_RAW_TOPIC


async def main() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    prices = {symbol: random.uniform(120, 500) for symbol in SYMBOLS}
    sequences = {symbol: 0 for symbol in SYMBOLS}
    try:
        while True:
            for symbol in SYMBOLS:
                sequences[symbol] += 1
                if random.random() < 0.03:
                    sequences[symbol] += 1

                prices[symbol] = max(1.0, prices[symbol] + random.uniform(-0.25, 0.25))
                mid = round(prices[symbol], 2)
                spread = random.choice([0.01, 0.02, 0.03, 0.08])
                event_time = utc_now_iso()

                quote = {
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
                await producer.send_and_wait(SYNTHETIC_RAW_TOPIC, dumps(quote), key=symbol.encode())

                if random.random() < 0.65:
                    sequences[symbol] += 1
                    trade = {
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
                    await producer.send_and_wait(SYNTHETIC_RAW_TOPIC, dumps(trade), key=symbol.encode())
            await asyncio.sleep(0.25)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
