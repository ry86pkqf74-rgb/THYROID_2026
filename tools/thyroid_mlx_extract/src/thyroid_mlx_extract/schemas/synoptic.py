"""Synoptic pathology enrichment — Tier 1's highest-leverage extraction.

Source: path_synoptics.synoptic_diagnosis + .path_diagnosis_comment + clinical_notes_long
Target: Ki-67%, mitoses, capsule, capsular invasion, angio, PNI, ETE, ENE per tumor
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class TumorFinding(BaseModel):
    tumor_index: int = Field(..., ge=1, le=10, description="1-indexed; matches path_synoptics tumor_N")
    histologic_type: Optional[str] = Field(
        None, description="e.g. 'papillary thyroid carcinoma, classical variant'"
    )
    histologic_grade: Optional[Literal["low", "intermediate", "high", "not_described"]] = None

    # Proliferation indices
    mitotic_count_per_2mm2: Optional[int] = Field(
        None, description="Integer mitoses per 2 mm² (10 HPF in modern WHO)"
    )
    mitotic_count_per_2mm2_evidence: Optional[str] = None
    ki67_labeling_index_pct: Optional[confloat(ge=0, le=100)] = None
    ki67_labeling_index_pct_evidence: Optional[str] = None

    # Capsule
    capsule_status: Optional[
        Literal["encapsulated", "partially_encapsulated", "unencapsulated", "not_described"]
    ] = None
    capsular_invasion: Optional[
        Literal["absent", "focal", "extensive", "not_described"]
    ] = None
    capsular_invasion_evidence: Optional[str] = None

    # Angio / lymphatic / perineural
    angioinvasion_present: Optional[bool] = None
    angioinvasion_vessels_count: Optional[int] = Field(
        None, description="WHO/Turin quantification: number of involved vessels"
    )
    angioinvasion_evidence: Optional[str] = None
    lymphatic_invasion: Optional[Literal["present", "absent", "not_described"]] = None
    perineural_invasion: Optional[Literal["present", "absent", "not_described"]] = None
    perineural_invasion_evidence: Optional[str] = None

    # Margins
    margin_status: Optional[
        Literal["negative", "positive", "close_lt_1mm", "not_described"]
    ] = None
    margin_distance_mm: Optional[confloat(ge=0)] = None

    # Extrathyroidal extension — the hard one
    ete_grade: Optional[
        Literal[
            "none",
            "microscopic_only",       # T3a (WHO 2022)
            "gross_minimal",          # T3b
            "gross_beyond_strap",     # T4a
            "aerodigestive_or_vascular",  # T4b
            "not_described",
        ]
    ] = None
    ete_evidence: Optional[str] = None

    # Extranodal extension (when LN positive)
    extranodal_extension_present: Optional[bool] = None
    extranodal_extension_largest_deposit_cm: Optional[confloat(ge=0)] = None
    extranodal_extension_evidence: Optional[str] = None


class Extraction(BaseModel):
    """Root schema for synoptic pathology enrichment."""

    tumors: list[TumorFinding] = []
    ihc_panel: dict[str, str] = Field(
        default_factory=dict,
        description="Marker → result, e.g. {'TTF1': 'positive', 'thyroglobulin': 'positive', 'calcitonin': 'negative'}",
    )
    overall_path_confidence: confloat(ge=0, le=1) = 1.0
    notes_for_review: Optional[str] = Field(
        None,
        description="Free text for ambiguities the model wants flagged. <500 chars.",
    )
