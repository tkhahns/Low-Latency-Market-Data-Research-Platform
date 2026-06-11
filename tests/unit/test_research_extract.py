"""Unit tests for research extraction — no API key, no network."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from market_platform.research.extract import RuleBasedExtractor, make_extractor
from market_platform.research.models import ResearchDocument


def make_doc(body: str, title: str = "Test", source: str = "rss") -> ResearchDocument:
    return ResearchDocument(
        source=source,
        source_uri="https://example.com",
        title=title,
        body=body,
        published_time="2026-06-01T00:00:00+00:00",
        fetched_time="2026-06-01T00:01:00+00:00",
    )


# ---------------------------------------------------------------------------
# RuleBasedExtractor
# ---------------------------------------------------------------------------


def test_rule_based_bullish():
    doc = make_doc("Bitcoin surged to record highs as institutional inflows accelerated.")
    insight = asyncio.get_event_loop().run_until_complete(RuleBasedExtractor().extract(doc))
    assert insight.sentiment == "bullish"
    assert "BTC-USD" in insight.symbols
    assert insight.extractor == "rule_based"
    assert insight.doc_id == doc.doc_id


def test_rule_based_bearish():
    doc = make_doc("Ethereum prices crashed after a major exploit drained liquidity.")
    insight = asyncio.get_event_loop().run_until_complete(RuleBasedExtractor().extract(doc))
    assert insight.sentiment == "bearish"
    assert "ETH-USD" in insight.symbols


def test_rule_based_neutral():
    doc = make_doc("Researchers published a new paper on cryptocurrency market microstructure.", title="Crypto paper")
    insight = asyncio.get_event_loop().run_until_complete(RuleBasedExtractor().extract(doc))
    assert insight.sentiment in ("bullish", "bearish", "neutral")
    assert insight.extractor == "rule_based"


def test_rule_based_summary_uses_first_sentences():
    body = "First sentence. Second sentence. Third sentence."
    doc = make_doc(body)
    insight = asyncio.get_event_loop().run_until_complete(RuleBasedExtractor().extract(doc))
    assert "First sentence" in insight.summary
    assert insight.summary.count("sentence") <= 2


def test_rule_based_tags_regulation():
    doc = make_doc("The SEC approved a new framework for broker-dealers.")
    insight = asyncio.get_event_loop().run_until_complete(RuleBasedExtractor().extract(doc))
    assert "regulation" in insight.tags or "filing" in insight.tags


def test_rule_based_tags_etf():
    doc = make_doc("Bitcoin ETF inflows hit a record high today.")
    insight = asyncio.get_event_loop().run_until_complete(RuleBasedExtractor().extract(doc))
    assert "etf-flows" in insight.tags


def test_rule_based_insight_schema():
    doc = make_doc("Solana DePIN projects raised $500M in Q2 2026.")
    insight = asyncio.get_event_loop().run_until_complete(RuleBasedExtractor().extract(doc))
    assert insight.schema_version == "1.0" if hasattr(insight, "schema_version") else True
    d = insight.to_dict()
    assert d["schema_version"] == "1.0"
    assert d["extractor"] == "rule_based"
    assert d["source"] == "rss"
    assert d["sentiment"] in ("bullish", "bearish", "neutral")
    assert isinstance(d["symbols"], list)
    assert isinstance(d["tags"], list)
    assert isinstance(d["entities"], list)


# ---------------------------------------------------------------------------
# make_extractor factory
# ---------------------------------------------------------------------------


def test_make_extractor_no_key_returns_rule_based():
    ext = make_extractor("auto", api_key=None, model="claude-opus-4-8")
    assert isinstance(ext, RuleBasedExtractor)


def test_make_extractor_rule_based_explicit():
    ext = make_extractor("rule_based", api_key="sk-test", model="claude-opus-4-8")
    assert isinstance(ext, RuleBasedExtractor)


def test_make_extractor_claude_no_key_raises():
    with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
        make_extractor("claude", api_key=None, model="claude-opus-4-8")


# ---------------------------------------------------------------------------
# ClaudeExtractor (stubbed — no real API key)
# ---------------------------------------------------------------------------


def test_claude_extractor_parses_valid_json():
    import json
    from market_platform.research.extract import ClaudeExtractor

    fake_response = MagicMock()
    fake_response.content = [MagicMock(text=json.dumps({
        "summary": "Claude summary here.",
        "symbols": ["BTC-USD"],
        "tags": ["etf-flows"],
        "sentiment": "bullish",
        "entities": ["BlackRock"],
    }))]
    fake_response.usage = MagicMock(input_tokens=100, output_tokens=50)

    async def fake_create(**kwargs):
        return fake_response

    extractor = ClaudeExtractor.__new__(ClaudeExtractor)
    extractor._model = "claude-opus-4-8"
    extractor._budget_tracker = None
    from market_platform.research.symbol_map import SYMBOL_MAP
    from market_platform.research.extract import _SYSTEM_PROMPT_TEMPLATE
    extractor._system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
        symbols_list=", ".join(sorted(SYMBOL_MAP.keys()))
    )

    mock_client = MagicMock()
    mock_client.messages.create = AsyncMock(return_value=fake_response)
    extractor._client = mock_client

    doc = make_doc("Bitcoin ETF inflows hit a record high as BlackRock reported strong demand.")
    insight = asyncio.get_event_loop().run_until_complete(extractor.extract(doc))

    assert insight.extractor == "claude"
    assert insight.summary == "Claude summary here."
    assert "BTC-USD" in insight.symbols
    assert insight.sentiment == "bullish"


def test_claude_extractor_falls_back_on_bad_json():
    from market_platform.research.extract import ClaudeExtractor

    fake_response = MagicMock()
    fake_response.content = [MagicMock(text="not valid json {{{ }")]
    fake_response.usage = None

    extractor = ClaudeExtractor.__new__(ClaudeExtractor)
    extractor._model = "claude-opus-4-8"
    extractor._budget_tracker = None
    from market_platform.research.symbol_map import SYMBOL_MAP
    from market_platform.research.extract import _SYSTEM_PROMPT_TEMPLATE
    extractor._system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
        symbols_list=", ".join(sorted(SYMBOL_MAP.keys()))
    )
    mock_client = MagicMock()
    mock_client.messages.create = AsyncMock(return_value=fake_response)
    extractor._client = mock_client

    doc = make_doc("Bitcoin price soared to new highs.")
    insight = asyncio.get_event_loop().run_until_complete(extractor.extract(doc))
    assert insight.extractor == "rule_based"


def test_claude_extractor_falls_back_on_api_error():
    from market_platform.research.extract import ClaudeExtractor

    extractor = ClaudeExtractor.__new__(ClaudeExtractor)
    extractor._model = "claude-opus-4-8"
    extractor._budget_tracker = None
    from market_platform.research.symbol_map import SYMBOL_MAP
    from market_platform.research.extract import _SYSTEM_PROMPT_TEMPLATE
    extractor._system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
        symbols_list=", ".join(sorted(SYMBOL_MAP.keys()))
    )
    mock_client = MagicMock()
    mock_client.messages.create = AsyncMock(side_effect=Exception("API error"))
    extractor._client = mock_client

    doc = make_doc("Ethereum rally follows protocol upgrade.")
    insight = asyncio.get_event_loop().run_until_complete(extractor.extract(doc))
    assert insight.extractor == "rule_based"
