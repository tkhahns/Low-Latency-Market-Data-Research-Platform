from __future__ import annotations

import argparse
from pathlib import Path

from market_platform.ops.documents import iter_markdown_chunks
from market_platform.ops.vector_store import JsonVectorStore, PostgresVectorStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Index an Obsidian vault or markdown directory for ops RAG.")
    parser.add_argument("paths", nargs="+", help="Markdown files or directories to index.")
    parser.add_argument("--postgres-dsn", help="Postgres DSN with pgvector enabled.")
    parser.add_argument("--json-store", default="var/rag/vector-store.json", help="Local JSON vector store for demos/tests.")
    parser.add_argument("--source-type", default="obsidian")
    args = parser.parse_args()

    chunks = iter_markdown_chunks([Path(path) for path in args.paths], source_type=args.source_type)
    store = PostgresVectorStore(args.postgres_dsn) if args.postgres_dsn else JsonVectorStore(Path(args.json_store))
    count = store.upsert_chunks(chunks)
    print(f"indexed_chunks={count} source_type={args.source_type}")


if __name__ == "__main__":
    main()
