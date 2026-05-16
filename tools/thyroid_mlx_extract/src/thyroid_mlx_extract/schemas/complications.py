"""Complications subtyping — convert flat 'complication' tag to typed events.

Source: clinical_notes_long (OPNOTE, HP, ENDOCRINE_FM)
Target: typed event per (research_id, complication_type, timing_window)
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


ComplicationType = Literal[
    "hypoparathyroidism_transient",
    "hypoparathyroidism_persistent",
    "hypoparathyroidism_permanent",
    "rln_injury_unilateral_transient",
    "rln_injury_unilateral_permanent",
    "rln_injury_bilateral_transient",
    "rln_injury_bilateral_permanent",
    "voice_change_subjective",
    "voice_change_stroboscopy_confirmed",
    "dysphagia_transient",
    "dysphagia_persistent",
    "hematoma_postop",
    "hematoma_late",
    "seroma",
    "chyle_leak",
    "ssi_superficial",
    "ssi_deep",
    "tracheostomy_perioperative",
    "tracheostomy_late",
    "death_perioperative",
    "death_late",
    "other",
]


class ComplicationEvent(BaseModel):
    complication_type: ComplicationType
    laterality: Optional[Literal["left", "right", "bilateral", "not_applicable", "not_specified"]] = (
        "not_specified"
    )
    timing_days_post_op: Optional[int] = Field(
        None, description="If a date is mentioned relative to surgery"
    )
    severity: Optional[Literal["mild", "moderate", "severe", "not_specified"]] = "not_specified"
    treatment_required: Optional[bool] = None
    treatment_summary: Optional[str] = None

    # For hypoparathyroidism / hypocalcemia
    pth_nadir: Optional[float] = Field(None, description="pg/mL if mentioned")
    ca_nadir: Optional[float] = Field(None, description="mg/dL if mentioned")

    # Quality scales
    vhi_score: Optional[float] = Field(None, description="VHI-10 or VHI-30 if mentioned")
    eat10_score: Optional[float] = Field(None, description="EAT-10 if mentioned")

    resolved: Optional[bool] = None
    resolution_date_offset_days: Optional[int] = None

    evidence_text: str
    confidence: confloat(ge=0, le=1) = 1.0


class Extraction(BaseModel):
    """Root schema for complications extraction."""

    events: list[ComplicationEvent] = []
    no_complications_documented: bool = Field(
        False, description="True if note explicitly states an uncomplicated course"
    )
