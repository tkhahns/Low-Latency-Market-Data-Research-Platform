from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from market_platform.time import utc_now_iso


FRONTMATTER_PATTERN = re.compile(r"\A---\n(?P<frontmatter>.*?)\n---\n(?P<body>.*)\Z", re.DOTALL)


@dataclass(frozen=True)
class DocumentChunk:
    source_uri: str
    title: str
    content: str
    tags: tuple[str, ...]
    source_type: str
    chunk_index: int
    indexed_at: str


def parse_markdown(path: Path, root: Path, source_type: str) -> list[DocumentChunk]:
    text = path.read_text(encoding="utf-8")
    title = path.stem
    tags: tuple[str, ...] = ()
    match = FRONTMATTER_PATTERN.match(text)
    if match:
        frontmatter = match.group("frontmatter")
        text = match.group("body")
        for line in frontmatter.splitlines():
            if line.startswith("title:"):
                title = line.split(":", 1)[1].strip().strip('"')
            elif line.startswith("tags:"):
                raw = line.split(":", 1)[1].strip().strip("[]")
                tags = tuple(tag.strip().strip('"') for tag in raw.split(",") if tag.strip())
    chunks = chunk_text(text)
    relative = path.relative_to(root).as_posix()
    indexed_at = utc_now_iso()
    return [
        DocumentChunk(
            source_uri=f"{source_type}:{relative}",
            title=title,
            content=chunk,
            tags=tags,
            source_type=source_type,
            chunk_index=index,
            indexed_at=indexed_at,
        )
        for index, chunk in enumerate(chunks)
    ]


def chunk_text(text: str, max_words: int = 220, overlap_words: int = 30) -> list[str]:
    words = text.split()
    if not words:
        return []
    chunks = []
    step = max(1, max_words - overlap_words)
    for start in range(0, len(words), step):
        chunk = " ".join(words[start : start + max_words]).strip()
        if chunk:
            chunks.append(chunk)
        if start + max_words >= len(words):
            break
    return chunks


def iter_markdown_chunks(paths: Iterable[Path], source_type: str) -> list[DocumentChunk]:
    all_chunks: list[DocumentChunk] = []
    for root in paths:
        if not root.exists():
            continue
        files = [root] if root.is_file() else sorted(root.rglob("*.md"))
        for path in files:
            if path.name.startswith("."):
                continue
            all_chunks.extend(parse_markdown(path, root if root.is_dir() else root.parent, source_type))
    return all_chunks

