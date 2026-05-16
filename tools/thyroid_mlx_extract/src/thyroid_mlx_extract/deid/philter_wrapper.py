"""Philter-lite wrapper.

Philter (UCSF) hit 99.46% recall in published evaluation. Use it as the FIRST PASS
on any clinical note before any LLM step. LLMs alone miss >50% of clinical PHI
(2026 published evidence) and are not a substitute.

Install:
    pip install philter-lite
"""
from __future__ import annotations

from pathlib import Path


def scrub(text: str, *, philter_config_path: Path | str | None = None) -> str:
    """Return a de-identified copy of `text`.

    Philter replaces detected PHI with `**`. If `philter_config_path` is None
    we use philter-lite's default config.
    """
    try:
        from philter_lite import Philter
    except ImportError as e:
        raise ImportError(
            "philter-lite not installed. Run: pip install philter-lite"
        ) from e

    philter = Philter(config_path=str(philter_config_path)) if philter_config_path else Philter()
    return philter.filter_text(text)


def is_safe(text: str) -> bool:
    """Quick post-check: returns True if no obvious PHI patterns remain.

    Heuristic guardrail — not a replacement for proper validation. Use it for
    sanity-checking that scrubbing actually ran.
    """
    import re

    suspects = [
        r"\b\d{3}-\d{2}-\d{4}\b",            # SSN
        r"\b\d{3}-\d{3}-\d{4}\b",            # phone
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",      # MM/DD/YYYY dates
        r"\bMRN[: ]*\d+\b",                  # MRN
        r"\bDOB[: ]*\d{1,2}/\d{1,2}/\d{2,4}\b",
    ]
    return not any(re.search(p, text) for p in suspects)
