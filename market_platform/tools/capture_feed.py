"""Capture N seconds of live feed events to a JSONL fixture for deterministic replay."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path


async def capture(source_name: str, seconds: float, out: Path, scrub: bool) -> None:
    from market_platform.feeds import make_feed_source

    source = make_feed_source(source_name)
    events = []
    start_wall = time.time()
    start_mono = time.monotonic()

    print(f"Capturing {seconds}s from source={source_name} ...")
    try:
        async with asyncio.timeout(seconds):
            async for event in source.stream():
                offset_ms = (time.monotonic() - start_mono) * 1000
                event["_capture_offset_ms"] = round(offset_ms, 1)
                events.append(event)
    except TimeoutError:
        pass

    if scrub:
        for event in events:
            event["event_time"] = "2026-01-01T00:00:00.000Z"

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        f.write("# Captured fixture. Timestamps rebased by ReplayFeedSource.\n")
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"Wrote {len(events)} events to {out}  (wall time: {time.time() - start_wall:.1f}s)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture feed events to a JSONL replay fixture.")
    parser.add_argument("--source", default="coinbase", help="Feed source name")
    parser.add_argument("--seconds", type=float, default=30.0, help="Duration to capture")
    parser.add_argument("--out", default="market_platform/fixtures/coinbase-sample.jsonl", help="Output JSONL path")
    parser.add_argument("--scrub", action="store_true", help="Normalize timestamps to a fixed baseline")
    args = parser.parse_args()
    asyncio.run(capture(args.source, args.seconds, Path(args.out), args.scrub))


if __name__ == "__main__":
    main()
