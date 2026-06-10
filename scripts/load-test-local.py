from __future__ import annotations

import argparse
import json
import statistics
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


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
    parser.add_argument("--json-out", metavar="FILE", help="Write results as JSON to this path")
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

    results = {
        "requests": args.requests,
        "concurrency": args.concurrency,
        "failures": failures,
        "throughput_rps": round((args.requests - failures) / elapsed, 2),
        "mean_ms": round(statistics.mean(latencies) if latencies else 0, 2),
        "p95_ms": round(percentile(latencies, 95), 2),
        "p99_ms": round(percentile(latencies, 99), 2),
    }

    print(f"requests={results['requests']} concurrency={results['concurrency']} failures={results['failures']}")
    print(f"throughput_rps={results['throughput_rps']}")
    print(f"mean_ms={results['mean_ms']}")
    print(f"p95_ms={results['p95_ms']}")
    print(f"p99_ms={results['p99_ms']}")

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(results, indent=2))
        print(f"Results written to {args.json_out}")


if __name__ == "__main__":
    main()
