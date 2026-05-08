from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol

from market_platform.ops.documents import DocumentChunk
from market_platform.ops.embedding import cosine_similarity, deterministic_embedding


@dataclass(frozen=True)
class SourceReference:
    source_uri: str
    title: str
    source_type: str
    chunk_index: int
    score: float


@dataclass(frozen=True)
class SearchResult:
    content: str
    source: SourceReference
    metadata: dict[str, Any]


class EvidenceStore(Protocol):
    def upsert_chunks(self, chunks: Iterable[DocumentChunk]) -> int:
        ...

    def search(self, query: str, limit: int = 5) -> list[SearchResult]:
        ...


class InMemoryVectorStore:
    def __init__(self) -> None:
        self.rows: list[tuple[DocumentChunk, list[float]]] = []

    def upsert_chunks(self, chunks: Iterable[DocumentChunk]) -> int:
        count = 0
        existing = {(chunk.source_uri, chunk.chunk_index): index for index, (chunk, _) in enumerate(self.rows)}
        for chunk in chunks:
            row = (chunk, deterministic_embedding(chunk.content))
            key = (chunk.source_uri, chunk.chunk_index)
            if key in existing:
                self.rows[existing[key]] = row
            else:
                self.rows.append(row)
            count += 1
        return count

    def search(self, query: str, limit: int = 5) -> list[SearchResult]:
        query_vector = deterministic_embedding(query)
        ranked = sorted(
            ((cosine_similarity(query_vector, embedding), chunk) for chunk, embedding in self.rows),
            key=lambda row: row[0],
            reverse=True,
        )
        return [
            SearchResult(
                content=chunk.content,
                source=SourceReference(
                    source_uri=chunk.source_uri,
                    title=chunk.title,
                    source_type=chunk.source_type,
                    chunk_index=chunk.chunk_index,
                    score=round(score, 6),
                ),
                metadata={"tags": list(chunk.tags), "indexed_at": chunk.indexed_at},
            )
            for score, chunk in ranked[:limit]
            if score > 0
        ]


class JsonVectorStore(InMemoryVectorStore):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        self.rows = [
            (
                DocumentChunk(
                    source_uri=row["chunk"]["source_uri"],
                    title=row["chunk"]["title"],
                    content=row["chunk"]["content"],
                    tags=tuple(row["chunk"].get("tags", [])),
                    source_type=row["chunk"]["source_type"],
                    chunk_index=row["chunk"]["chunk_index"],
                    indexed_at=row["chunk"]["indexed_at"],
                ),
                row["embedding"],
            )
            for row in payload
        ]

    def persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {"chunk": {**asdict(chunk), "tags": list(chunk.tags)}, "embedding": embedding}
            for chunk, embedding in self.rows
        ]
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def upsert_chunks(self, chunks: Iterable[DocumentChunk]) -> int:
        count = super().upsert_chunks(chunks)
        self.persist()
        return count


class PostgresVectorStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def upsert_chunks(self, chunks: Iterable[DocumentChunk]) -> int:
        import psycopg

        rows = list(chunks)
        with psycopg.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                for chunk in rows:
                    cursor.execute(
                        """
                        INSERT INTO rag_documents (
                          source_uri, title, content, tags, source_type, chunk_index, indexed_at, embedding
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::vector)
                        ON CONFLICT (source_uri, chunk_index) DO UPDATE SET
                          title = EXCLUDED.title,
                          content = EXCLUDED.content,
                          tags = EXCLUDED.tags,
                          source_type = EXCLUDED.source_type,
                          indexed_at = EXCLUDED.indexed_at,
                          embedding = EXCLUDED.embedding
                        """,
                        (
                            chunk.source_uri,
                            chunk.title,
                            chunk.content,
                            list(chunk.tags),
                            chunk.source_type,
                            chunk.chunk_index,
                            chunk.indexed_at,
                            vector_literal(deterministic_embedding(chunk.content)),
                        ),
                    )
        return len(rows)

    def search(self, query: str, limit: int = 5) -> list[SearchResult]:
        import psycopg

        query_vector = deterministic_embedding(query)
        with psycopg.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT source_uri, title, content, tags, source_type, chunk_index,
                           indexed_at, 1 - (embedding <=> %s::vector) AS score
                    FROM rag_documents
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                    """,
                    (vector_literal(query_vector), vector_literal(query_vector), limit),
                )
                return [
                    SearchResult(
                        content=row[2],
                        source=SourceReference(
                            source_uri=row[0],
                            title=row[1],
                            source_type=row[4],
                            chunk_index=row[5],
                            score=round(float(row[7]), 6),
                        ),
                        metadata={"tags": row[3], "indexed_at": row[6]},
                    )
                    for row in cursor.fetchall()
                ]


def vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{value:.8f}" for value in vector) + "]"
