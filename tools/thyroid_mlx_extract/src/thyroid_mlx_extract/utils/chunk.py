"""Long-text chunking with overlap, for notes that exceed model context."""
from __future__ import annotations


def chunk_text(text: str, *, max_chars: int = 32_000, overlap_chars: int = 2_000) -> list[str]:
    """Split a long string into overlapping chunks.

    Defaults assume a 128K-token context window (~32K chars budget for the document
    portion of the prompt after system + schema + reserve). Tune per model.
    """
    if len(text) <= max_chars:
        return [text]
    chunks = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = end - overlap_chars
    return chunks
