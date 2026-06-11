"""Extractor protocol plus RuleBasedExtractor and ClaudeExtractor."""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from market_platform.research.models import ResearchDocument, ResearchInsight
from market_platform.research.symbol_map import SYMBOL_MAP, symbols_for_text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Extractor(Protocol):
    async def extract(self, doc: ResearchDocument) -> ResearchInsight: ...


# ---------------------------------------------------------------------------
# Sentiment lexicon
# ---------------------------------------------------------------------------

_BULLISH_TERMS = frozenset(
    [
        "surge", "soar", "rally", "record high", "all-time high", "ath", "gains",
        "bullish", "buy", "outperform", "inflows", "adoption", "approval", "milestone",
        "growth", "rise", "rose", "upside", "breakout", "accumulation",
    ]
)
_BEARISH_TERMS = frozenset(
    [
        "crash", "plunge", "drop", "fall", "decline", "bearish", "sell", "outflows",
        "ban", "regulation", "hack", "exploit", "collapse", "loss", "downside",
        "capitulation", "fear", "liquidation", "contagion",
    ]
)

_TAG_KEYWORDS: Dict[str, List[str]] = {
    "regulation": ["sec", "cftc", "regulation", "regulatory", "framework", "compliance", "law"],
    "etf-flows": ["etf", "fund inflow", "fund outflow", "inflows", "outflows", "blackrock", "fidelity"],
    "defi": ["defi", "decentralized finance", "uniswap", "aave", "liquidity pool", "amm", "yield"],
    "institutional": ["institutional", "hedge fund", "asset manager", "pension", "endowment"],
    "mining": ["mining", "miner", "hash rate", "proof of work", "asic"],
    "layer2": ["layer 2", "l2", "arbitrum", "optimism", "rollup", "zkp"],
    "staking": ["staking", "stake", "validator", "restaking", "eigen"],
    "macro": ["inflation", "interest rate", "fed", "federal reserve", "macro", "gdp", "recession"],
    "research": ["arxiv", "paper", "model", "neural", "transformer", "deep learning", "ml", "ai"],
    "filing": ["sec", "8-k", "10-k", "10-q", "filing", "disclosure"],
}


def _sentiment(text: str) -> str:
    lower = text.lower()
    bull = sum(1 for t in _BULLISH_TERMS if t in lower)
    bear = sum(1 for t in _BEARISH_TERMS if t in lower)
    if bull > bear:
        return "bullish"
    if bear > bull:
        return "bearish"
    return "neutral"


def _tags(text: str) -> List[str]:
    lower = text.lower()
    return [tag for tag, kws in _TAG_KEYWORDS.items() if any(kw in lower for kw in kws)]


def _entities(text: str) -> List[str]:
    found = re.findall(r"\b[A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)*\b", text)
    seen: Dict[str, None] = {}
    result = []
    for e in found:
        if len(e) > 2 and e not in seen:
            seen[e] = None
            result.append(e)
    return result[:20]


def _summary(body: str) -> str:
    sentences = re.split(r"(?<=[.!?])\s+", body.strip())
    return " ".join(sentences[:2])


# ---------------------------------------------------------------------------
# RuleBasedExtractor
# ---------------------------------------------------------------------------


class RuleBasedExtractor:
    async def extract(self, doc: ResearchDocument) -> ResearchInsight:
        combined = f"{doc.title} {doc.body}"
        return ResearchInsight(
            doc_id=doc.doc_id,
            summary=_summary(doc.body) or doc.title,
            symbols=symbols_for_text(combined),
            tags=_tags(combined),
            sentiment=_sentiment(combined),
            entities=_entities(combined),
            extractor="rule_based",
            source=doc.source,
            source_uri=doc.source_uri,
            title=doc.title,
            published_time=doc.published_time,
            extracted_time=datetime.now(timezone.utc).isoformat(),
        )


# ---------------------------------------------------------------------------
# ClaudeExtractor
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """You are a financial research analyst extracting structured information from documents.

Return a JSON object with these exact fields:
- summary: 2-3 sentence summary (string)
- symbols: array of relevant symbols from this list only: {symbols_list}
- tags: array of topic tags from: regulation, etf-flows, defi, institutional, mining, layer2, staking, macro, research, filing
- sentiment: one of "bullish", "bearish", "neutral"
- entities: array of named entities (organizations, people, products) up to 10

Return only valid JSON. Do not add any other text."""


class ClaudeExtractor:
    def __init__(self, api_key: str, model: str, budget_tracker: Optional[Any] = None) -> None:
        import anthropic

        self._client = anthropic.AsyncAnthropic(api_key=api_key)
        self._model = model
        self._budget_tracker = budget_tracker
        self._system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
            symbols_list=", ".join(sorted(SYMBOL_MAP.keys()))
        )

    async def extract(self, doc: ResearchDocument) -> ResearchInsight:
        user_content = f"Title: {doc.title}\n\nBody: {doc.body}"
        try:
            response = await self._client.messages.create(
                model=self._model,
                max_tokens=512,
                system=[
                    {
                        "type": "text",
                        "text": self._system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_content}],
            )
        except Exception as exc:
            logger.warning("claude extractor failed for %s: %s — falling back to rule_based", doc.doc_id, exc)
            return await RuleBasedExtractor().extract(doc)

        if self._budget_tracker:
            usage = getattr(response, "usage", None)
            if usage:
                await self._budget_tracker.record_usage(self._model, usage)

        raw_text = response.content[0].text.strip()
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            logger.warning("claude returned non-JSON for %s — falling back", doc.doc_id)
            return await RuleBasedExtractor().extract(doc)

        valid_symbols = set(SYMBOL_MAP.keys())
        symbols = [s for s in parsed.get("symbols", []) if s in valid_symbols]
        sentiment = parsed.get("sentiment", "neutral")
        if sentiment not in ("bullish", "bearish", "neutral"):
            sentiment = "neutral"

        return ResearchInsight(
            doc_id=doc.doc_id,
            summary=parsed.get("summary") or _summary(doc.body),
            symbols=symbols,
            tags=parsed.get("tags", []),
            sentiment=sentiment,
            entities=parsed.get("entities", [])[:10],
            extractor="claude",
            source=doc.source,
            source_uri=doc.source_uri,
            title=doc.title,
            published_time=doc.published_time,
            extracted_time=datetime.now(timezone.utc).isoformat(),
        )


# ---------------------------------------------------------------------------
# Budget tracker
# ---------------------------------------------------------------------------

_PRICING: Dict[str, Dict[str, float]] = {
    "claude-opus-4-8": {"input": 5.0 / 1e6, "output": 25.0 / 1e6},
    "claude-haiku-4-5": {"input": 1.0 / 1e6, "output": 5.0 / 1e6},
    "claude-haiku-4-5-20251001": {"input": 1.0 / 1e6, "output": 5.0 / 1e6},
}


class BudgetTracker:
    def __init__(self, redis_client: Any, daily_budget_usd: float) -> None:
        self._redis = redis_client
        self._budget = daily_budget_usd

    async def record_usage(self, model: str, usage: Any) -> float:
        from datetime import date
        from market_platform import redis_keys

        pricing = _PRICING.get(model, {"input": 5.0 / 1e6, "output": 25.0 / 1e6})
        input_tokens = getattr(usage, "input_tokens", 0) or 0
        output_tokens = getattr(usage, "output_tokens", 0) or 0
        cost = input_tokens * pricing["input"] + output_tokens * pricing["output"]

        key = redis_keys.research_llm_spend(date.today().isoformat())
        new_total = await self._redis.incrbyfloat(key, cost)
        await self._redis.expire(key, 86400 * 2)
        return new_total

    async def is_budget_exhausted(self) -> bool:
        from datetime import date
        from market_platform import redis_keys

        key = redis_keys.research_llm_spend(date.today().isoformat())
        val = await self._redis.get(key)
        if val is None:
            return False
        return float(val) >= self._budget


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_extractor(
    mode: str,
    api_key: Optional[str],
    model: str,
    budget_tracker: Optional[BudgetTracker] = None,
) -> Extractor:
    if mode == "rule_based":
        return RuleBasedExtractor()
    if mode == "claude":
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY required for mode=claude")
        return ClaudeExtractor(api_key, model, budget_tracker)
    # auto: use Claude if key present
    if api_key:
        return ClaudeExtractor(api_key, model, budget_tracker)
    return RuleBasedExtractor()
