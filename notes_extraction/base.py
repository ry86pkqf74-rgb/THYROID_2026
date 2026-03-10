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
    extraction_method: str = "regex"
    extracted_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

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
            "extraction_method": self.extraction_method,
            "extracted_at": self.extracted_at,
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
