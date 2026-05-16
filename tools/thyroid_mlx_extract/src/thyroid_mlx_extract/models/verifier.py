"""Second-pass verification of existing note_entities_* rows.

The note_entities_* tables in pub_canonical have provenance columns
(verification_status, date_confidence, verifier_name, verifier_version,
verification_step) that are 100% NULL across all 76,641 extracted entities
as of 2026-05-16. This module runs a second model (typically R1-distill-70B)
over the SAME source note text plus the original extraction, and writes
verdicts back to a sibling workspace table.

OUTPUT TABLE shape: see bq/verify_io.py VERIFIED_SCHEMA.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel, Field, confloat

from .extractor import extract as run_model


class VerificationVerdict(BaseModel):
    """Second-pass model's verdict on the original extraction."""

    agrees_with_original: bool = Field(
        ..., description="True if the second-pass model agrees with the original extraction"
    )
    corrected_value: Optional[str] = Field(
        None, description="If disagrees, what should entity_value_norm be? Null if agrees."
    )
    corrected_present_or_negated: Optional[str] = Field(
        None, description="If disagrees on present/negated, the corrected value"
    )
    date_confidence: confloat(ge=0, le=1) = Field(
        ...,
        description="0=no date attribution support in evidence, 1=unambiguous.",
    )
    evidence_present_in_source: bool = Field(
        ...,
        description="Does the evidence_span literally appear in the source text? Hallucination check.",
    )
    reasoning: str = Field(
        ..., description="One short sentence (<200 chars) citing source language."
    )


VERIFIER_PROMPT = """You are a clinical adjudicator. A primary extractor produced an entity from a clinical note. Your job is to verify whether that extraction is correct, using only the evidence in the note.

PRIMARY EXTRACTION:
- Entity type: {entity_type}
- Entity value (raw): {entity_value_raw}
- Entity value (normalized): {entity_value_norm}
- Present or negated: {present_or_negated}
- Primary model's confidence: {original_confidence}
- Primary model's evidence span: "{evidence_span}"
- Primary model's claimed entity date: {entity_date}

SOURCE NOTE EXCERPT (the full text the primary model saw):
---
{source_text}
---

YOUR JOB:
1. Does the evidence_span actually appear in the source note text? (literal substring check). If not, the extraction is hallucinated.
2. Given the source text, does the primary extraction correctly capture {entity_type}?
3. If you disagree, what should the value be?
4. How confident are you in the entity_date attribution from the evidence? (0 = no date support, 1 = unambiguous date).

Be conservative: if the source text doesn't clearly support the original extraction, set agrees_with_original=False.

Output: single JSON object matching the schema. No prose before or after.
"""


@dataclass
class VerificationInput:
    research_id: str
    source_pk: str
    entity_type: str
    entity_value_raw: str
    entity_value_norm: str
    present_or_negated: str
    original_confidence: float
    evidence_span: str
    entity_date: Optional[str]
    source_text: str


@dataclass
class VerificationResult:
    success: bool
    verdict: Optional[VerificationVerdict]
    verification_status: str
    raw_text: str
    error: Optional[str]
    elapsed_seconds: float
    verifier_model: str


def verify_one(
    item: VerificationInput,
    *,
    verifier_model: str = "r1-distill-70b",
    prompt_version: str = "v1",
    temperature: float = 0.0,
    max_tokens: int = 1024,
) -> VerificationResult:
    """Verify a single extracted entity. Returns a structured verdict."""
    prompt = VERIFIER_PROMPT.format(
        entity_type=item.entity_type,
        entity_value_raw=item.entity_value_raw,
        entity_value_norm=item.entity_value_norm,
        present_or_negated=item.present_or_negated,
        original_confidence=item.original_confidence,
        evidence_span=item.evidence_span,
        entity_date=item.entity_date or "(no date attributed)",
        source_text=_truncate(item.source_text, 24_000),
    )

    t0 = time.time()
    result = run_model(
        model_key=verifier_model,
        schema_cls=VerificationVerdict,
        prompt_template=prompt,
        source_text="",
        prompt_version=prompt_version,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    elapsed = time.time() - t0

    if not result.success or result.payload is None:
        return VerificationResult(
            success=False, verdict=None, verification_status="both_failed",
            raw_text=result.raw_text, error=result.error,
            elapsed_seconds=elapsed, verifier_model=verifier_model,
        )

    verdict: VerificationVerdict = result.payload
    status = "agreed" if verdict.agrees_with_original else "disagreed"
    return VerificationResult(
        success=True, verdict=verdict, verification_status=status,
        raw_text=result.raw_text, error=None,
        elapsed_seconds=elapsed, verifier_model=verifier_model,
    )


def _truncate(text: str, max_chars: int) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 200] + "\n\n[...TRUNCATED — original text was longer...]"
