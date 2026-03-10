"""
Optional LLM-based entity extractor.

Gated behind the OPENAI_API_KEY environment variable.
If the key is not set, all methods return empty results with a warning.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from notes_extraction.base import BaseExtractor, EntityMatch
from notes_extraction.vocab import ENTITY_SCHEMA_COLUMNS

log = logging.getLogger(__name__)

MAX_CHUNK_CHARS = 6000


class LLMExtractor(BaseExtractor):
    """Structured LLM extraction with evidence-span enforcement."""

    entity_domain = "llm"

    def __init__(self, api_key_env: str = "OPENAI_API_KEY") -> None:
        self._api_key = os.getenv(api_key_env)
        if not self._api_key:
            log.warning(
                f"LLMExtractor: {api_key_env} not set — LLM extraction disabled. "
                "Set the env var to enable."
            )

    @property
    def available(self) -> bool:
        return self._api_key is not None

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        if not self.available:
            return []

        chunk = note_text[:MAX_CHUNK_CHARS]

        try:
            return self._call_llm(note_row_id, research_id, note_type, chunk)
        except Exception as exc:
            log.error(f"LLM extraction failed for {note_row_id}: {exc}")
            return []

    def _call_llm(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        text: str,
    ) -> list[EntityMatch]:
        """Call the LLM API and parse structured output.

        This is a stub implementation.  Replace the body with actual API
        calls (e.g. openai.ChatCompletion) when ready.
        """
        log.debug("LLM extraction stub called — returning empty (implement API call)")
        return []

    @staticmethod
    def _validate_evidence_span(span: str, full_text: str) -> bool:
        """Evidence span must be an exact substring of the source text."""
        return span in full_text
