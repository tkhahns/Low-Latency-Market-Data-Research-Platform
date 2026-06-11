"""Data models for the research intelligence pipeline."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ResearchDocument:
    """A raw document fetched from a research source."""

    source: str  # arxiv | rss | edgar | replay
    source_uri: str
    title: str
    body: str
    published_time: str  # ISO-8601
    fetched_time: str    # ISO-8601

    @property
    def doc_id(self) -> str:
        digest = hashlib.sha256(self.body.encode()).hexdigest()
        return f"sha256:{digest}"

    def to_dict(self) -> dict:
        return {
            "schema_version": "1.0",
            "doc_id": self.doc_id,
            "source": self.source,
            "source_uri": self.source_uri,
            "title": self.title,
            "body": self.body,
            "published_time": self.published_time,
            "fetched_time": self.fetched_time,
        }


@dataclass
class ResearchInsight:
    """Structured insight extracted from a research document."""

    doc_id: str
    summary: str
    symbols: List[str]
    tags: List[str]
    sentiment: str  # bullish | bearish | neutral
    entities: List[str]
    extractor: str  # rule_based | claude
    source: str
    source_uri: str
    title: str
    published_time: str   # ISO-8601
    extracted_time: str   # ISO-8601

    def to_dict(self) -> dict:
        return {
            "schema_version": "1.0",
            "doc_id": self.doc_id,
            "summary": self.summary,
            "symbols": self.symbols,
            "tags": self.tags,
            "sentiment": self.sentiment,
            "entities": self.entities,
            "extractor": self.extractor,
            "source": self.source,
            "source_uri": self.source_uri,
            "title": self.title,
            "published_time": self.published_time,
            "extracted_time": self.extracted_time,
        }
