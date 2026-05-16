"""Re-extraction schema for llm_dynamic_risk_response (current 97.7% empty).

ATA 2015 dynamic risk classification — captures the clinician's stated
assessment plus contributing evidence.
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class Extraction(BaseModel):
    response_category: Literal[
        "excellent",
        "indeterminate",
        "biochemical_incomplete",
        "structural_incomplete",
        "not_assessed",
        "not_specified",
    ] = "not_specified"

    tg_level_ng_ml: Optional[confloat(ge=0)] = None
    tg_stimulated: Optional[bool] = None
    antitg_present: Optional[bool] = None
    antitg_level: Optional[float] = None

    structural_disease_present: Optional[bool] = None
    structural_disease_site: Optional[str] = None
    structural_modality: Optional[
        Literal["us", "ct", "mri", "pet", "rai_wbs", "physical_exam", "biopsy", "not_specified"]
    ] = "not_specified"

    clinician_assessment_statement: Optional[str] = Field(
        None, description="Verbatim statement from clinician on response category"
    )
    evidence_text: str = Field(..., description="Source substring supporting the classification")
    confidence: confloat(ge=0, le=1) = 1.0
