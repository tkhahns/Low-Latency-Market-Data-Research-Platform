from __future__ import annotations

import argparse
import statistics
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch(url: str) -> float:
    started = time.perf_counter()
    with urllib.request.urlopen(url, timeout=5) as response:
        response.read()
    return (time.perf_counter() - started) * 1000


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((pct / 100) * (len(ordered) - 1))))
    return ordered[index]


def main() -> None:
    parser = argparse.ArgumentParser(description="Load test the local Redis-backed market data API.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--requests", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=25)
    args = parser.parse_args()

    url = f"{args.base_url.rstrip('/')}/latest/{args.symbol.upper()}"
    started = time.perf_counter()
    latencies = []
    failures = 0
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [executor.submit(fetch, url) for _ in range(args.requests)]
        for future in as_completed(futures):
            try:
                latencies.append(future.result())
            except Exception:
                failures += 1
    elapsed = time.perf_counter() - started
    print(f"requests={args.requests} concurrency={args.concurrency} failures={failures}")
    print(f"throughput_rps={(args.requests - failures) / elapsed:.2f}")
    print(f"mean_ms={statistics.mean(latencies) if latencies else 0:.2f}")
    print(f"p95_ms={percentile(latencies, 95):.2f}")
    print(f"p99_ms={percentile(latencies, 99):.2f}")


if __name__ == "__main__":
    main()
