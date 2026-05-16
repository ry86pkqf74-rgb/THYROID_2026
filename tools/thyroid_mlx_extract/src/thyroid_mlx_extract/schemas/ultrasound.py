"""US nodule feature enrichment beyond ACR TI-RADS structured fields.

Source: ultrasound_reports.nodule_N_source_description
Target: halo, vascularity, microcalc subtype, taller-than-wide, spongiform, capsule contour
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class NoduleFeatures(BaseModel):
    nodule_index: int = Field(..., ge=1, le=20)

    halo: Optional[Literal["present", "thin", "thick_irregular", "absent", "not_described"]] = None
    halo_evidence: Optional[str] = None

    vascularity: Optional[
        Literal["peripheral", "internal", "mixed", "absent", "not_described"]
    ] = None
    vascularity_evidence: Optional[str] = None

    microcalcification_subtype: Optional[
        Literal[
            "punctate_echogenic_foci",
            "comet_tail_artifacts",
            "coarse",
            "dystrophic",
            "rim",
            "peripheral_macrocalcification",
            "absent",
            "not_described",
        ]
    ] = None
    microcalcification_evidence: Optional[str] = None

    shape_ratio: Optional[
        Literal["taller_than_wide", "wider_than_tall", "round", "not_described"]
    ] = None
    shape_evidence: Optional[str] = None

    spongiform_pct: Optional[confloat(ge=0, le=100)] = Field(
        None, description="Percent of nodule with spongiform appearance"
    )
    spongiform_evidence: Optional[str] = None

    us_ete_suspected: Optional[bool] = Field(
        None, description="Radiologist suggests extrathyroidal extension by US"
    )
    us_ete_evidence: Optional[str] = None

    tracheal_involvement_suspected: Optional[bool] = None
    tracheal_involvement_evidence: Optional[str] = None

    capsule_contour: Optional[
        Literal["abuts", "deforms", "extends_beyond", "not_described"]
    ] = None
    capsule_contour_evidence: Optional[str] = None


class Extraction(BaseModel):
    """Root schema for US nodule feature enrichment."""

    nodules: list[NoduleFeatures] = []
    lymph_node_concern: Optional[Literal["high", "intermediate", "low", "not_described"]] = None
    lymph_node_evidence: Optional[str] = None
