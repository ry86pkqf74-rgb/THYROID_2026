"""Molecular result parsing (ThyroSeq, Afirma, Castle, etc).

Source: molecular_results.raw_payload_json
Target: variants, fusions, CNVs, expression alterations, risk call
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat


class Variant(BaseModel):
    gene: str = Field(..., description="HGNC gene symbol, e.g. BRAF, NRAS")
    protein_change: Optional[str] = Field(None, description="HGVS.p notation, e.g. p.V600E or V600E")
    cdna_change: Optional[str] = Field(None, description="HGVS.c notation, e.g. c.1799T>A")
    variant_allele_frequency_pct: Optional[confloat(ge=0, le=100)] = None
    classification: Literal[
        "pathogenic",
        "likely_pathogenic",
        "uncertain_significance",
        "likely_benign",
        "benign",
        "not_specified",
    ] = "not_specified"
    zygosity: Optional[Literal["heterozygous", "homozygous", "hemizygous", "not_specified"]] = (
        "not_specified"
    )
    evidence_text: str = Field(..., description="Exact substring from source confirming this variant")
    confidence: confloat(ge=0, le=1) = 1.0


class Fusion(BaseModel):
    gene_5_prime: str
    gene_3_prime: str
    breakpoint: Optional[str] = None
    supporting_reads: Optional[int] = None
    evidence_text: str
    confidence: confloat(ge=0, le=1) = 1.0


class CopyNumberAlteration(BaseModel):
    gene: str
    alteration_type: Literal["gain", "loss", "amplification", "deletion", "loh", "not_specified"]
    copy_number: Optional[float] = None
    evidence_text: str
    confidence: confloat(ge=0, le=1) = 1.0


class ExpressionAlteration(BaseModel):
    gene: str
    direction: Literal["overexpressed", "underexpressed", "altered", "not_specified"]
    magnitude: Optional[str] = None
    evidence_text: str
    confidence: confloat(ge=0, le=1) = 1.0


class RiskCall(BaseModel):
    band: Literal["positive", "negative", "low", "moderate", "high", "intermediate", "not_called"]
    risk_pct: Optional[confloat(ge=0, le=100)] = None
    classifier: Optional[str] = Field(
        None, description="e.g. 'ThyroSeq GC v3', 'Afirma GSC', 'Castle DiffDx'"
    )
    evidence_text: str
    confidence: confloat(ge=0, le=1) = 1.0


class Extraction(BaseModel):
    """Root schema for molecular extraction."""

    assay_name: Optional[str] = Field(
        None, description="e.g. 'ThyroSeq GC v3', 'Afirma GSC', 'Castle DiffDx'"
    )
    test_date: Optional[str] = Field(None, description="ISO date if present in report")
    variants: list[Variant] = []
    fusions: list[Fusion] = []
    cnvs: list[CopyNumberAlteration] = []
    expression_alterations: list[ExpressionAlteration] = []
    risk_call: Optional[RiskCall] = None
    interpretation_summary: Optional[str] = Field(
        None, description="1–2 sentence narrative; <300 chars"
    )
    no_alterations_detected: bool = Field(
        False, description="True if the report explicitly states a negative result"
    )
