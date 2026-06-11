"""EmbeddingProvider abstraction — swappable backend, deterministic default."""
from __future__ import annotations

import os
from typing import List, Protocol, runtime_checkable

from market_platform.ops.embedding import deterministic_embedding


@runtime_checkable
class EmbeddingProvider(Protocol):
    def embed(self, text: str) -> List[float]: ...


class DeterministicEmbeddingProvider:
    """Default: free, deterministic, CI-safe. Upgrade via EMBEDDING_PROVIDER."""

    def embed(self, text: str) -> List[float]:
        return deterministic_embedding(text)


def get_embedding_provider() -> EmbeddingProvider:
    provider = os.getenv("EMBEDDING_PROVIDER", "deterministic")
    if provider == "deterministic":
        return DeterministicEmbeddingProvider()
    raise ValueError(f"Unknown EMBEDDING_PROVIDER: {provider!r}. Supported: deterministic")
