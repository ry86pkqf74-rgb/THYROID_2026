"""Cross-sectional imaging (CT/MRI/NM) — T4a/T4b staging and distant disease.

Source: ct_imaging.original_report, mri_imaging.original_report, nuclear_med.findings_text
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class AerodigestiveInvolvement(BaseModel):
    tracheal_cartilage_erosion: Optional[bool] = None
    tracheal_lumen_invasion: Optional[bool] = None
    esophageal_muscularis_invasion: Optional[bool] = None
    esophageal_lumen_invasion: Optional[bool] = None
    rln_groove_abutment: Optional[bool] = None
    rln_groove_invasion: Optional[bool] = None
    evidence_text: Optional[str] = None


class VascularInvolvement(BaseModel):
    carotid_encasement_degrees: Optional[Literal["<180", "180-270", ">=270", "absent", "not_described"]] = None
    jugular_invasion: Optional[bool] = None
    innominate_or_subclavian_involvement: Optional[bool] = None
    evidence_text: Optional[str] = None


class LymphNodeFindings(BaseModel):
    central_compartment_concerning: Optional[bool] = None
    lateral_compartment_concerning: Optional[bool] = None
    mediastinal_level_vii: Optional[bool] = None
    necrotic_nodes: Optional[bool] = None
    calcified_nodes: Optional[bool] = None
    largest_short_axis_mm: Optional[float] = None
    levels_involved: list[Literal["I", "II", "III", "IV", "V", "VI", "VII"]] = []
    evidence_text: Optional[str] = None


class DistantMetastases(BaseModel):
    pulmonary_present: Optional[bool] = None
    pulmonary_pattern: Optional[Literal["miliary", "macronodular", "mixed", "not_described"]] = None
    pulmonary_count_category: Optional[Literal["solitary", "few_<5", "many_>=5", "not_described"]] = None
    bone_present: Optional[bool] = None
    bone_pattern: Optional[Literal["lytic", "blastic", "mixed", "not_described"]] = None
    bone_sites: list[str] = []
    brain_present: Optional[bool] = None
    hepatic_present: Optional[bool] = None
    evidence_text: Optional[str] = None


class Extraction(BaseModel):
    """Root schema for cross-sectional imaging extraction."""

    modality: Literal["CT", "MRI", "NM", "PET", "other"]
    contrast_administered: Optional[bool] = None
    aerodigestive: AerodigestiveInvolvement = AerodigestiveInvolvement()
    vascular: VascularInvolvement = VascularInvolvement()
    lymph_nodes: LymphNodeFindings = LymphNodeFindings()
    distant_mets: DistantMetastases = DistantMetastases()

    # NM-specific
    uptake_pattern: Optional[Literal["diffuse", "focal", "multifocal", "absent", "not_described"]] = None
    mediastinal_uptake: Optional[bool] = None
    retrosternal_extension: Optional[bool] = None
    uptake_evidence: Optional[str] = None

    overall_t_stage_suggested: Optional[
        Literal["T0", "T1", "T2", "T3a", "T3b", "T4a", "T4b", "Tx", "not_specified"]
    ] = None
    overall_confidence: confloat(ge=0, le=1) = 1.0
