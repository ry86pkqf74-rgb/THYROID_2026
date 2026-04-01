"""
Base classes for the entity-extraction framework.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from notes_extraction.vocab import ENTITY_SCHEMA_COLUMNS, NEGATION_CUES

NEGATION_WINDOW = 40


@dataclass
class EntityMatch:
    """A single extracted entity with provenance."""

    research_id: int
    note_row_id: str
    note_type: str
    entity_type: str
    entity_value_raw: str
    entity_value_norm: str
    present_or_negated: str = "present"
    confidence: float = 0.9
    evidence_span: str = ""
    evidence_start: int = 0
    evidence_end: int = 0
    entity_date: str | None = None
    note_date: str | None = None
    extraction_method: str = "regex"
    extracted_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    date_confidence: float | None = None
    source_line: int | None = None
    chunk_index: int = 0
    chunk_char_start: int = 0
    chunk_char_end: int = 0
    evidence_global_start: int = 0
    evidence_global_end: int = 0
    raw_response_sha256: str | None = None
    verification_status: str = "unverified"
    verification_step: str = "none"
    extraction_run_id: str | None = None
    extractor_name: str | None = None
    extractor_version: str | None = None
    model_name: str | None = None
    model_version: str | None = None
    prompt_version: str | None = None
    verifier_name: str | None = None
    verifier_version: str | None = None

    def to_dict(self) -> dict:
        return {
            "research_id": self.research_id,
            "note_row_id": self.note_row_id,
            "note_type": self.note_type,
            "entity_type": self.entity_type,
            "entity_value_raw": self.entity_value_raw,
            "entity_value_norm": self.entity_value_norm,
            "present_or_negated": self.present_or_negated,
            "confidence": self.confidence,
            "evidence_span": self.evidence_span,
            "evidence_start": self.evidence_start,
            "evidence_end": self.evidence_end,
            "entity_date": self.entity_date,
            "note_date": self.note_date,
            "extraction_method": self.extraction_method,
            "extracted_at": self.extracted_at,
            "date_confidence": self.date_confidence,
            "source_line": self.source_line,
            "chunk_index": self.chunk_index,
            "chunk_char_start": self.chunk_char_start,
            "chunk_char_end": self.chunk_char_end,
            "evidence_global_start": self.evidence_global_start,
            "evidence_global_end": self.evidence_global_end,
            "raw_response_sha256": self.raw_response_sha256,
            "verification_status": self.verification_status,
            "verification_step": self.verification_step,
            "extraction_run_id": self.extraction_run_id,
            "extractor_name": self.extractor_name,
            "extractor_version": self.extractor_version,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "prompt_version": self.prompt_version,
            "verifier_name": self.verifier_name,
            "verifier_version": self.verifier_version,
        }


class BaseExtractor(abc.ABC):
    """Abstract base for all entity extractors."""

    entity_domain: str = ""

    @abc.abstractmethod
    def extract(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        note_text: str,
        note_date: str | None = None,
    ) -> list[EntityMatch]:
        ...

    @staticmethod
    def check_negation(text: str, match_start: int) -> str:
        """Look for negation cues in the window before the match."""
        window_start = max(0, match_start - NEGATION_WINDOW)
        window = text[window_start:match_start].lower()
        for cue in NEGATION_CUES:
            if cue in window:
                return "negated"
        return "present"

    @staticmethod
    def validate_output(df: pd.DataFrame) -> None:
        """Raise if the output DataFrame violates the required schema."""
        missing = set(ENTITY_SCHEMA_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Output missing required columns: {missing}")
