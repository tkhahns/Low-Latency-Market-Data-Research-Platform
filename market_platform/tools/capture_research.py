"""Capture research documents to JSONL fixture for CI/offline determinism.

Usage:
    python -m market_platform.tools.capture_research --source arxiv --limit 10
    python -m market_platform.tools.capture_research --source rss --limit 5
    python -m market_platform.tools.capture_research --source all --limit 5 --output market_platform/fixtures/research-sample.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


async def capture(source_name: str, limit: int) -> list[dict]:
    from market_platform.config import RESEARCH_RSS_FEEDS
    from market_platform.research.sources.arxiv import ArxivSource
    from market_platform.research.sources.rss import RssSource

    sources = []
    if source_name in ("arxiv", "all"):
        sources.append(ArxivSource(max_results=limit))
    if source_name in ("rss", "all"):
        sources.append(RssSource(RESEARCH_RSS_FEEDS))

    if not sources:
        print(f"Unknown source: {source_name!r}. Choose: arxiv, rss, all", file=sys.stderr)
        sys.exit(1)

    rows = []
    for source in sources:
        docs, _ = await source.fetch_since(None)
        for doc in docs[:limit]:
            rows.append(doc.to_dict())
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture research documents to JSONL fixture.")
    parser.add_argument("--source", default="all", choices=["arxiv", "rss", "all"])
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--output", default=None, help="Output file path (default: stdout)")
    args = parser.parse_args()

    rows = asyncio.run(capture(args.source, args.limit))
    lines = "\n".join(json.dumps(row) for row in rows)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(lines + "\n", encoding="utf-8")
        print(f"Wrote {len(rows)} documents to {out}", file=sys.stderr)
    else:
        print(lines)


if __name__ == "__main__":
    main()
