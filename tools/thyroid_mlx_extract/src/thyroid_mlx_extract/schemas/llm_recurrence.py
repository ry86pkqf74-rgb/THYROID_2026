"""Re-extraction schema for llm_recurrence (current table 92.3% empty).

Source: clinical_notes_long (any note type that mentions recurrence/persistent
disease/structural disease found in follow-up).
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


RecurrenceSite = Literal[
    "cervical_local_thyroid_bed",
    "cervical_nodal_central_compartment",
    "cervical_nodal_lateral_compartment",
    "cervical_nodal_unspecified",
    "mediastinal",
    "pulmonary",
    "bone",
    "brain",
    "hepatic",
    "other_distant",
    "biochemical_only",
    "not_specified",
]


class RecurrenceEvent(BaseModel):
    recurrence_type: Literal["structural", "biochemical", "indeterminate", "no_recurrence"]
    site: RecurrenceSite
    laterality: Optional[Literal["left", "right", "bilateral", "central", "not_applicable"]] = None
    detected_date: Optional[str] = Field(None, description="ISO date if present")
    detection_modality: Optional[
        Literal[
            "ultrasound", "ct", "mri", "pet", "rai_scan", "biopsy_fna",
            "biopsy_core", "surgical_pathology", "tg_lab", "antitg_lab",
            "physical_exam", "other", "not_specified",
        ]
    ] = "not_specified"
    biopsy_confirmed: Optional[bool] = None
    size_largest_cm: Optional[confloat(ge=0)] = None
    tg_level_ng_ml: Optional[confloat(ge=0)] = None
    antitg_present: Optional[bool] = None
    treatment_decision_made: Optional[bool] = Field(
        None, description="Did the note describe a treatment decision (surgery, RAI, observation)?"
    )
    evidence_text: str = Field(..., description="Verbatim source substring supporting this event")
    confidence: confloat(ge=0, le=1) = 1.0


class Extraction(BaseModel):
    """Root schema for recurrence event extraction."""
    events: list[RecurrenceEvent] = []
    no_recurrence_documented: bool = Field(
        False, description="True if the note explicitly states no evidence of disease"
    )
