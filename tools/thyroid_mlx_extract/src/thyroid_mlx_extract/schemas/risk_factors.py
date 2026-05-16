"""Pre-existing risk factors from HP notes.

Source: clinical_notes_long where note_type='HP'
Target: childhood neck radiation, family hx, smoking, BMI, environmental exposures
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class RadiationExposure(BaseModel):
    childhood_neck_radiation: Optional[bool] = None
    indication: Optional[str] = None
    age_at_exposure: Optional[int] = None
    dose_gy: Optional[float] = None
    evidence_text: Optional[str] = None


class FamilyHistory(BaseModel):
    thyroid_cancer_first_degree: Optional[bool] = None
    thyroid_cancer_count_first_degree: Optional[int] = None
    men2_men2a_men2b: Optional[bool] = None
    fap_or_gardner: Optional[bool] = None
    cowden_syndrome: Optional[bool] = None
    carney_complex: Optional[bool] = None
    werner_syndrome: Optional[bool] = None
    evidence_text: Optional[str] = None


class Lifestyle(BaseModel):
    smoking_status: Optional[Literal["never", "former", "current", "not_specified"]] = None
    pack_years: Optional[float] = None
    alcohol_status: Optional[Literal["none", "occasional", "moderate", "heavy", "not_specified"]] = None
    bmi: Optional[confloat(ge=10, le=80)] = None
    height_cm: Optional[float] = None
    weight_kg: Optional[float] = None
    evidence_text: Optional[str] = None


class Extraction(BaseModel):
    """Root schema for pre-existing risk factors."""

    radiation: RadiationExposure = RadiationExposure()
    family_history: FamilyHistory = FamilyHistory()
    lifestyle: Lifestyle = Lifestyle()
    autoimmune_thyroid_history: Optional[Literal["hashimoto", "graves", "none", "not_specified"]] = (
        "not_specified"
    )
    prior_neck_surgery: Optional[bool] = None
    overall_confidence: confloat(ge=0, le=1) = 1.0
