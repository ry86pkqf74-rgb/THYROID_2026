"""FNA cytology subtype + nuclear features.

Source: fna_cytology.path_text
Target: Bethesda subcategorization, nuclear/architectural features, adequacy
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class NuclearFeature(BaseModel):
    feature: Literal[
        "nuclear_grooves",
        "intranuclear_pseudoinclusions",
        "powdery_chromatin",
        "nuclear_overlap",
        "irregular_nuclear_contours",
        "enlarged_nuclei",
        "nuclear_clearing",
    ]
    severity: Literal["focal", "diffuse", "rare", "not_specified"] = "not_specified"
    evidence_text: str
    confidence: confloat(ge=0, le=1) = 1.0


class ArchitecturalFeature(BaseModel):
    feature: Literal[
        "papillary_fronds",
        "microfollicular",
        "macrofollicular",
        "hurthle_cell",
        "oncocytic",
        "solid",
        "trabecular",
        "three_dimensional_clusters",
    ]
    evidence_text: str
    confidence: confloat(ge=0, le=1) = 1.0


class Extraction(BaseModel):
    """Root schema for FNA cytology subtype extraction."""

    bethesda_category: Optional[Literal["I", "II", "III", "IV", "V", "VI"]] = None
    bethesda_subcategory: Optional[
        Literal[
            "I-cyst-fluid-only",
            "I-scant-cellularity",
            "I-obscuring-blood",
            "I-other",
            "II-benign-follicular",
            "II-benign-hashimoto",
            "II-cyst-degenerative",
            "II-other",
            "III-AUS-nuclear",
            "III-AUS-other",
            "III-AUS-oncocytic",
            "IV-FN-conventional",
            "IV-FN-oncocytic",
            "V-suspicious-PTC",
            "V-suspicious-MTC",
            "V-suspicious-other",
            "VI-PTC",
            "VI-PDTC",
            "VI-ATC",
            "VI-MTC",
            "VI-metastatic",
            "VI-other",
            "not_specified",
        ]
    ] = None

    nuclear_features: list[NuclearFeature] = []
    architectural_features: list[ArchitecturalFeature] = []

    adequacy: Optional[
        Literal[
            "satisfactory",
            "less_than_optimal",
            "scant_cellularity",
            "obscuring_blood",
            "cyst_fluid_only",
            "not_specified",
        ]
    ] = None
    molecular_testing_recommended: Optional[bool] = None
    notes_for_review: Optional[str] = None
