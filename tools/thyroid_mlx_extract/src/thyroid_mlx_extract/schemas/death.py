"""Cause-of-death adjudication for the survival cohort.

Source: clinical_notes_long where note_type='DEATH'
Target: cancer_specific vs non_cancer vs uncertain; proximate cause; date
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class Extraction(BaseModel):
    """Root schema for death attribution."""

    attribution: Literal["cancer_specific", "non_cancer", "uncertain"] = "uncertain"
    proximate_cause: Optional[str] = Field(
        None, description="ICD-style brief description, e.g. 'progressive metastatic disease'"
    )
    contributing_factors: list[str] = []
    death_date: Optional[str] = Field(None, description="ISO date if present")
    location_of_death: Optional[Literal["hospital", "hospice", "home", "other", "not_specified"]] = (
        "not_specified"
    )
    last_treatment_active: Optional[bool] = Field(
        None, description="Was the patient on active thyroid-cancer therapy at time of death?"
    )
    evidence_text: str = Field(..., description="Exact substring(s) supporting the attribution")
    confidence: confloat(ge=0, le=1) = 1.0
