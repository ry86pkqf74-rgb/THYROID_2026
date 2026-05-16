"""Re-extraction schema for llm_us_nodule_dynamics (current table 87% empty).

Captures the temporal/dynamic findings radiologists make in follow-up US:
size change, new nodules, suspicious change in features, vascular pattern shift,
lymph node interval change.
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class NoduleDynamics(BaseModel):
    nodule_index: Optional[int] = Field(None, ge=1, le=20)
    size_change: Optional[
        Literal["new", "growing", "stable", "shrinking", "resolved", "not_specified"]
    ] = "not_specified"
    size_change_pct: Optional[float] = Field(
        None, description="If quantified: percent change since prior"
    )
    new_features_concerning: Optional[bool] = None
    new_features_detail: Optional[str] = None
    intervention_recommended: Optional[
        Literal["fna", "core_biopsy", "follow_up_imaging", "no_action", "not_specified"]
    ] = "not_specified"
    evidence_text: str


class LymphNodeDynamics(BaseModel):
    compartment: Literal["central", "lateral_left", "lateral_right", "mediastinal", "not_specified"]
    interval_change: Literal["new", "growing", "stable", "shrinking", "resolved", "not_specified"]
    size_largest_short_axis_mm: Optional[float] = None
    suspicious_features: list[
        Literal[
            "microcalcifications", "cystic_change", "loss_of_hilum", "round_shape",
            "increased_vascularity", "rim_enhancement"
        ]
    ] = []
    evidence_text: str


class Extraction(BaseModel):
    """Root schema for US dynamic findings."""
    nodules: list[NoduleDynamics] = []
    lymph_nodes: list[LymphNodeDynamics] = []
    overall_assessment: Optional[
        Literal[
            "stable", "improved", "worsened",
            "indeterminate", "no_prior_for_comparison", "not_specified",
        ]
    ] = "not_specified"
    overall_confidence: confloat(ge=0, le=1) = 1.0
