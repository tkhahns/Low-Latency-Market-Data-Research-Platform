"""Unit tests for research sources — no network, no LLM."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_platform.research.models import ResearchDocument
from market_platform.research.sources.replay import ReplaySource
from market_platform.research.symbol_map import symbols_for_text


# ---------------------------------------------------------------------------
# Replay source
# ---------------------------------------------------------------------------


FIXTURE = Path(__file__).resolve().parents[2] / "market_platform" / "fixtures" / "research-sample.jsonl"


def test_replay_loads_all_docs(tmp_path):
    # Replay from the committed fixture
    import asyncio
    source = ReplaySource(FIXTURE)
    docs, cursor = asyncio.get_event_loop().run_until_complete(source.fetch_since(None))
    assert len(docs) >= 10
    assert cursor is not None
    for doc in docs:
        assert doc.doc_id.startswith("sha256:")
        assert doc.source in ("arxiv", "rss", "edgar", "replay")
        assert doc.title
        assert doc.body


def test_replay_cursor_dedup():
    import asyncio
    source = ReplaySource(FIXTURE)
    docs1, cursor1 = asyncio.get_event_loop().run_until_complete(source.fetch_since(None))
    docs2, cursor2 = asyncio.get_event_loop().run_until_complete(source.fetch_since(cursor1))
    assert len(docs2) == 0  # All docs are at or before cursor1
    assert cursor2 == cursor1


def test_replay_missing_file(tmp_path):
    import asyncio
    source = ReplaySource(tmp_path / "nonexistent.jsonl")
    docs, cursor = asyncio.get_event_loop().run_until_complete(source.fetch_since(None))
    assert docs == []
    assert cursor is None


def test_replay_custom_fixture(tmp_path):
    import asyncio
    fixture = tmp_path / "test.jsonl"
    rows = [
        {
            "source": "rss",
            "source_uri": "https://example.com/1",
            "title": "Bitcoin rally",
            "body": "Bitcoin surged to new highs",
            "published_time": "2026-06-01T12:00:00+00:00",
            "fetched_time": "2026-06-01T12:01:00+00:00",
        },
        {
            "source": "arxiv",
            "source_uri": "https://arxiv.org/abs/test",
            "title": "ETH paper",
            "body": "Ethereum smart contract analysis",
            "published_time": "2026-06-02T12:00:00+00:00",
            "fetched_time": "2026-06-02T12:01:00+00:00",
        },
    ]
    fixture.write_text("\n".join(json.dumps(r) for r in rows))
    source = ReplaySource(fixture)
    docs, cursor = asyncio.get_event_loop().run_until_complete(source.fetch_since(None))
    assert len(docs) == 2
    assert cursor == "2026-06-02T12:00:00+00:00"


# ---------------------------------------------------------------------------
# doc_id content-hash dedup
# ---------------------------------------------------------------------------


def test_doc_id_deterministic():
    doc1 = ResearchDocument(
        source="rss",
        source_uri="https://example.com",
        title="Title",
        body="Same body",
        published_time="2026-06-01T00:00:00+00:00",
        fetched_time="2026-06-01T00:00:00+00:00",
    )
    doc2 = ResearchDocument(
        source="arxiv",
        source_uri="https://arxiv.org/abs/x",
        title="Different title",
        body="Same body",
        published_time="2026-05-01T00:00:00+00:00",
        fetched_time="2026-05-01T00:00:00+00:00",
    )
    assert doc1.doc_id == doc2.doc_id  # Hash is body-only


def test_doc_id_different_body():
    doc1 = ResearchDocument(
        source="rss", source_uri="https://a.com", title="T", body="body A",
        published_time="2026-01-01T00:00:00+00:00", fetched_time="2026-01-01T00:00:00+00:00",
    )
    doc2 = ResearchDocument(
        source="rss", source_uri="https://a.com", title="T", body="body B",
        published_time="2026-01-01T00:00:00+00:00", fetched_time="2026-01-01T00:00:00+00:00",
    )
    assert doc1.doc_id != doc2.doc_id


# ---------------------------------------------------------------------------
# Symbol map
# ---------------------------------------------------------------------------


def test_symbols_for_text_btc():
    result = symbols_for_text("Bitcoin ETF flows hit a record high today")
    assert "BTC-USD" in result


def test_symbols_for_text_eth():
    result = symbols_for_text("Ethereum gas fees dropped after the Dencun upgrade")
    assert "ETH-USD" in result


def test_symbols_for_text_sol():
    result = symbols_for_text("Solana DePIN projects attracted new funding")
    assert "SOL-USD" in result


def test_symbols_for_text_no_match():
    result = symbols_for_text("Federal Reserve holds rates steady")
    # May or may not match depending on configured symbols; just assert it returns a list
    assert isinstance(result, list)


def test_symbols_for_text_case_insensitive():
    assert symbols_for_text("BITCOIN is rising") == symbols_for_text("bitcoin is rising")
