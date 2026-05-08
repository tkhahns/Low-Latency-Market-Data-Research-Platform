from pathlib import Path

from market_platform.ops.documents import iter_markdown_chunks
from market_platform.ops.embedding import cosine_similarity, deterministic_embedding
from market_platform.ops.vector_store import InMemoryVectorStore


def test_deterministic_embeddings_are_stable_and_searchable():
    left = deterministic_embedding("AAPL Redis freshness stale symbol")
    right = deterministic_embedding("AAPL Redis freshness stale symbol")
    unrelated = deterministic_embedding("Databricks gold bars volatility")

    assert left == right
    assert cosine_similarity(left, right) > cosine_similarity(left, unrelated)


def test_obsidian_markdown_chunks_are_indexed_with_source_references(tmp_path: Path):
    vault = tmp_path / "Vault"
    vault.mkdir()
    note = vault / "Flink Window Tuning.md"
    note.write_text(
        """---
title: Flink Window Tuning
tags: [flink, latency]
---

Redis caching and Flink windows reduced tick-to-signal latency during local benchmark notes.
""",
        encoding="utf-8",
    )

    chunks = iter_markdown_chunks([vault], source_type="obsidian")
    store = InMemoryVectorStore()
    assert store.upsert_chunks(chunks) == 1

    results = store.search("Flink Redis tick-to-signal latency", limit=1)
    assert results[0].source.source_uri == "obsidian:Flink Window Tuning.md"
    assert results[0].source.title == "Flink Window Tuning"
    assert results[0].metadata["tags"] == ["flink", "latency"]
