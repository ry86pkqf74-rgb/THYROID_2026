"""
Extraction Audit Engine v5 — Phase 7 Preoperative & Full Molecular Panel Refinement
====================================================================================
Extends v4 with specialized parsers for:
  1. FNABethesdaParser       – Bethesda normalization across all sources with source linking
  2. MolecularPanelCleaner   – BRAF/TERT/RAS/RET/NTRK/ALK: ordered vs positive + method
  3. PreopImagingReconciler  – Date-aligned imaging-path size/ETE concordance

Source hierarchy: fna_cytology (1.0) > path_synoptics (0.95) > molecular_testing (0.85)
                  > genetic_testing (0.80) > note_entities (0.6)

Usage:
    from notes_extraction.extraction_audit_engine_v5 import audit_and_refine_phase7
    results = audit_and_refine_phase7(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v5.py --md --variable all
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from notes_extraction.extraction_audit_engine_v4 import (
    _extract_table_name,
)
from notes_extraction.extraction_audit_engine_v3 import _get_connection
from notes_extraction.vocab import (
    MOLECULAR_VARIANT_NORM,
    GENE_FUSION_NORM,
)

# ---------------------------------------------------------------------------
# Source hierarchy constants — Phase 7
# ---------------------------------------------------------------------------
PHASE7_SOURCE_HIERARCHY = {
    "fna_cytology": 1.0,
    "path_synoptic": 0.95,
    "molecular_testing": 0.85,
    "genetic_testing": 0.80,
    "molecular_test_episode_v2": 0.90,
    "imaging_nodule_long_v2": 0.88,
    "recurrence_risk_features_mv": 0.75,
    "note_entities": 0.60,
    "h_p_consent": 0.20,
}

# ---------------------------------------------------------------------------
# Bethesda normalization map
# ---------------------------------------------------------------------------
BETHESDA_NORM = {
    "i": 1, "1": 1, "nondiagnostic": 1, "unsatisfactory": 1, "nd": 1,
    "non-diagnostic": 1, "inadequate": 1,
    "ii": 2, "2": 2, "benign": 2,
    "iii": 3, "3": 3, "aus": 3, "flus": 3,
    "atypia of undetermined significance": 3,
    "follicular lesion of undetermined significance": 3, "aus/flus": 3,
    "iv": 4, "4": 4, "fn": 4, "sfn": 4,
    "follicular neoplasm": 4, "suspicious for follicular neoplasm": 4,
    "hürthle cell neoplasm": 4, "hurthle cell neoplasm": 4, "hcn": 4,
    "v": 5, "5": 5, "suspicious for malignancy": 5, "sfm": 5, "suspicious": 5,
    "vi": 6, "6": 6, "malignant": 6, "positive for malignancy": 6,
}

BETHESDA_NAMES = {
    1: "Nondiagnostic/Unsatisfactory",
    2: "Benign",
    3: "AUS/FLUS",
    4: "Follicular Neoplasm/SFN",
    5: "Suspicious for Malignancy",
    6: "Malignant",
}

# ---------------------------------------------------------------------------
# Molecular method detection patterns
# ---------------------------------------------------------------------------
_METHOD_NGS_PATTERNS = [
    re.compile(r"\b(?:ThyroSeq|next[\s-]*generation\s+sequencing|NGS|Illumina)\b", re.I),
    re.compile(r"\bThyroSeq\s+v[23]\b", re.I),
]
_METHOD_GSC_PATTERNS = [
    re.compile(r"\bAfirma\s+(?:GSC|GEC|Genomic\s+Sequencing\s+Classifier)\b", re.I),
    re.compile(r"\bAfirma\b", re.I),
]
_METHOD_IHC_PATTERNS = [
    re.compile(r"\b(?:immunohistochemistry|IHC|VE1\s+antibody|anti-BRAF)\b", re.I),
]
_METHOD_PCR_PATTERNS = [
    re.compile(r"\b(?:PCR|real[\s-]*time\s+PCR|allele[\s-]*specific\s+PCR|Cobas|idylla)\b", re.I),
]
_METHOD_FISH_PATTERNS = [
    re.compile(r"\b(?:FISH|fluorescence\s+in\s+situ)\b", re.I),
]

# BRAF specific
_BRAF_V600E_POSITIVE = [
    re.compile(r"\bBRAF\s+(?:p\.?\s*)?V600E?\b.*?(?:positive|detected|present|identified|mutated)", re.I),
    re.compile(r"\bBRAF\s+(?:p\.?\s*)?V600E?\s+c\.\s*1799T>A\b", re.I),
    re.compile(r"(?:positive|detected).*?\bBRAF\s+(?:p\.?\s*)?V600E?\b", re.I),
    re.compile(r"\bBRAF\s+mutation\s+(?:was\s+)?(?:positive|detected|identified)\b", re.I),
]
_BRAF_V600E_NEGATIVE = [
    re.compile(r"\bBRAF\s+(?:p\.?\s*)?V600E?\b.*?(?:negative|not\s+detected|wild[\s-]*type)", re.I),
    re.compile(r"\bno\s+BRAF\s+(?:p\.?\s*)?V600E?\s+mutation\b", re.I),
    re.compile(r"\bBRAF\b.*?(?:negative|not\s+detected|wild[\s-]*type)", re.I),
]
_BRAF_ORDERED_ONLY = [
    re.compile(r"\bBRAF\s+(?:testing|analysis|test)\s+(?:was\s+)?(?:ordered|sent|submitted|pending)\b", re.I),
    re.compile(r"\bpending\s+BRAF\b", re.I),
]

# TERT promoter specific variants
_TERT_C228T = re.compile(r"\bTERT\s+(?:promoter\s+)?C228T\b", re.I)
_TERT_C250T = re.compile(r"\bTERT\s+(?:promoter\s+)?C250T\b", re.I)
_TERT_POSITIVE = [
    re.compile(r"\bTERT\s+(?:promoter\s+)?(?:mutation\s+)?(?:positive|detected|present|identified|found)\b", re.I),
    re.compile(r"\bTERT\s+(?:C228T|C250T)\b", re.I),
]
_TERT_NEGATIVE = [
    re.compile(r"\bTERT\b.*?(?:negative|not\s+detected|wild[\s-]*type)\b", re.I),
]


# ---------------------------------------------------------------------------
# 1. FNABethesdaParser
# ---------------------------------------------------------------------------
class FNABethesdaParser:
    """Parse and reconcile Bethesda categories across all source tables."""

    def normalize_bethesda(self, raw: str | None) -> int | None:
        if not raw:
            return None
        v = str(raw).strip().lower()
        v = re.sub(r"[.\-_/\\()]", " ", v).strip()
        if v in ("", "x", "null", "n/a", "n/s", "c/a", "none", "see other"):
            return None
        direct = BETHESDA_NORM.get(v)
        if direct:
            return direct
        num_match = re.search(r"\b([1-6])\b", v)
        if num_match:
            return int(num_match.group(1))
        roman_match = re.search(r"\b(i{1,3}|iv|v|vi)\b", v)
        if roman_match:
            roman_map = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6}
            return roman_map.get(roman_match.group(1))
        for key, val in BETHESDA_NORM.items():
            if key in v:
                return val
        return None

    def classify_bethesda(self, raw: str | None, source: str = "unknown") -> dict:
        num = self.normalize_bethesda(raw)
        return {
            "bethesda_num": num,
            "bethesda_name": BETHESDA_NAMES.get(num) if num else None,
            "raw_value": raw,
            "source": source,
            "reliability": PHASE7_SOURCE_HIERARCHY.get(source, 0.5),
            "confidence": 0.95 if num else 0.0,
        }


# ---------------------------------------------------------------------------
# 2. MolecularPanelCleaner
# ---------------------------------------------------------------------------
class MolecularPanelCleaner:
    """Distinguish ordered vs tested vs positive for each molecular marker,
    with method detection (NGS/GSC/IHC/PCR)."""

    def detect_method(self, text: str | None) -> str:
        if not text:
            return "unknown"
        for p in _METHOD_NGS_PATTERNS:
            if p.search(text):
                return "NGS"
        for p in _METHOD_GSC_PATTERNS:
            if p.search(text):
                return "Afirma_GSC"
        for p in _METHOD_IHC_PATTERNS:
            if p.search(text):
                return "IHC"
        for p in _METHOD_PCR_PATTERNS:
            if p.search(text):
                return "PCR"
        for p in _METHOD_FISH_PATTERNS:
            if p.search(text):
                return "FISH"
        return "unknown"

    def classify_braf(self, mutation_text: str | None,
                      detailed_text: str | None,
                      result_text: str | None,
                      braf_flag: bool | None = None) -> dict:
        combined = " ".join(filter(None, [mutation_text, detailed_text, result_text]))
        if not combined.strip() and braf_flag is None:
            return self._empty("BRAF")

        method = self.detect_method(combined)
        if braf_flag is True:
            return {
                "gene": "BRAF", "variant": "V600E",
                "status": "positive", "method": method,
                "tested": True, "confidence": 0.95,
            }
        for p in _BRAF_V600E_POSITIVE:
            if p.search(combined):
                return {
                    "gene": "BRAF", "variant": "V600E",
                    "status": "positive", "method": method,
                    "tested": True, "confidence": 0.92,
                }
        for p in _BRAF_V600E_NEGATIVE:
            if p.search(combined):
                return {
                    "gene": "BRAF", "variant": "V600E",
                    "status": "negative", "method": method,
                    "tested": True, "confidence": 0.90,
                }
        for p in _BRAF_ORDERED_ONLY:
            if p.search(combined):
                return {
                    "gene": "BRAF", "variant": None,
                    "status": "ordered_not_resulted", "method": method,
                    "tested": False, "confidence": 0.70,
                }
        if re.search(r"\bBRAF\b", combined, re.I):
            return {
                "gene": "BRAF", "variant": "V600E",
                "status": "tested_indeterminate", "method": method,
                "tested": True, "confidence": 0.60,
            }
        if braf_flag is False:
            return {
                "gene": "BRAF", "variant": "V600E",
                "status": "negative", "method": method,
                "tested": True, "confidence": 0.85,
            }
        return self._empty("BRAF")

    def classify_tert(self, mutation_text: str | None,
                      detailed_text: str | None,
                      tert_flag: bool | None = None) -> dict:
        combined = " ".join(filter(None, [mutation_text, detailed_text]))
        if not combined.strip() and tert_flag is None:
            return self._empty("TERT")

        method = self.detect_method(combined)
        promoter_type = None
        if _TERT_C228T.search(combined):
            promoter_type = "C228T"
        elif _TERT_C250T.search(combined):
            promoter_type = "C250T"

        if tert_flag is True or any(p.search(combined) for p in _TERT_POSITIVE):
            return {
                "gene": "TERT", "variant": promoter_type or "promoter_mutation",
                "status": "positive", "method": method,
                "tested": True, "confidence": 0.92,
            }
        if tert_flag is False or any(p.search(combined) for p in _TERT_NEGATIVE):
            return {
                "gene": "TERT", "variant": None,
                "status": "negative", "method": method,
                "tested": True, "confidence": 0.90,
            }
        if re.search(r"\bTERT\b", combined, re.I):
            return {
                "gene": "TERT", "variant": promoter_type,
                "status": "tested_indeterminate", "method": method,
                "tested": True, "confidence": 0.60,
            }
        return self._empty("TERT")

    def classify_gene(self, gene: str, mutation_text: str | None,
                      detailed_text: str | None,
                      flag: bool | None = None) -> dict:
        """Generic classifier for RET, RAS, NTRK, ALK, TP53."""
        combined = " ".join(filter(None, [mutation_text, detailed_text]))
        gene_upper = gene.upper()
        gene_re = re.compile(rf"\b{re.escape(gene_upper)}\b", re.I)
        pos_re = re.compile(
            rf"\b{re.escape(gene_upper)}\b.*?(?:positive|detected|present|identified|mutated|mutation\s+positive)",
            re.I,
        )
        neg_re = re.compile(
            rf"\b{re.escape(gene_upper)}\b.*?(?:negative|not\s+detected|wild[\s-]*type|no\s+mutation)",
            re.I,
        )
        fusion_re = re.compile(
            rf"\b{re.escape(gene_upper)}[\s/-]+\w+\s+fusion\b|"
            rf"\b\w+[\s/-]+{re.escape(gene_upper)}\s+fusion\b",
            re.I,
        )

        if not combined.strip() and flag is None:
            return self._empty(gene_upper)

        method = self.detect_method(combined)
        variant = None
        for raw_key, norm_val in MOLECULAR_VARIANT_NORM.items():
            if gene_upper in norm_val.upper() and raw_key.lower() in combined.lower():
                variant = norm_val
                break
        if not variant:
            for raw_key, norm_val in GENE_FUSION_NORM.items():
                if gene_upper in norm_val.upper() and raw_key.lower() in combined.lower():
                    variant = norm_val
                    break

        if flag is True or pos_re.search(combined):
            return {
                "gene": gene_upper, "variant": variant,
                "status": "positive", "method": method,
                "tested": True, "confidence": 0.90,
            }
        is_fusion = fusion_re.search(combined)
        if is_fusion:
            return {
                "gene": gene_upper, "variant": variant or f"{gene_upper}_fusion",
                "status": "positive", "method": method,
                "tested": True, "confidence": 0.88,
            }
        if flag is False or neg_re.search(combined):
            return {
                "gene": gene_upper, "variant": None,
                "status": "negative", "method": method,
                "tested": True, "confidence": 0.88,
            }
        if gene_re.search(combined):
            return {
                "gene": gene_upper, "variant": variant,
                "status": "tested_indeterminate", "method": method,
                "tested": True, "confidence": 0.55,
            }
        return self._empty(gene_upper)

    def _empty(self, gene: str) -> dict:
        return {
            "gene": gene, "variant": None,
            "status": "not_tested", "method": None,
            "tested": False, "confidence": 0.0,
        }


# ---------------------------------------------------------------------------
# 3. PreopImagingReconciler
# ---------------------------------------------------------------------------
class PreopImagingReconciler:
    """Date-aligned preop imaging vs pathology concordance."""

    def reconcile_sizes(self, imaging_size_cm: float | None,
                        path_size_cm: float | None) -> dict:
        if imaging_size_cm is None or path_size_cm is None:
            return {
                "size_discrepancy_cm": None,
                "size_concordance": "insufficient_data",
                "confidence": 0.0,
            }
        diff = abs(imaging_size_cm - path_size_cm)
        ratio = max(imaging_size_cm, path_size_cm) / max(min(imaging_size_cm, path_size_cm), 0.01)
        if diff <= 0.5 and ratio <= 1.5:
            concordance = "concordant"
        elif diff <= 1.0 and ratio <= 2.0:
            concordance = "minor_discrepancy"
        else:
            concordance = "major_discrepancy"
        return {
            "size_discrepancy_cm": round(diff, 2),
            "size_ratio": round(ratio, 2),
            "size_concordance": concordance,
            "imaging_size_cm": imaging_size_cm,
            "path_size_cm": path_size_cm,
            "confidence": 0.90 if concordance == "concordant" else 0.75,
        }

    def reconcile_ete(self, imaging_suspicious_ete: bool | None,
                      path_ete_confirmed: bool | None) -> dict:
        if imaging_suspicious_ete is None and path_ete_confirmed is None:
            return {"ete_concordance": "insufficient_data", "confidence": 0.0}
        if imaging_suspicious_ete and path_ete_confirmed:
            return {"ete_concordance": "concordant_positive", "confidence": 0.95}
        if not imaging_suspicious_ete and not path_ete_confirmed:
            return {"ete_concordance": "concordant_negative", "confidence": 0.90}
        if imaging_suspicious_ete and not path_ete_confirmed:
            return {"ete_concordance": "imaging_overcall", "confidence": 0.80}
        return {"ete_concordance": "imaging_undercall", "confidence": 0.85}


# ---------------------------------------------------------------------------
# SQL Builders
# ---------------------------------------------------------------------------
def build_fna_by_source_sql() -> str:
    """Per-patient FNA Bethesda from ALL sources, source-linked."""
    return """
CREATE OR REPLACE TABLE extracted_fna_bethesda_v1 AS
WITH
-- Source 1: fna_cytology (gold standard, structured)
src_fna_cytology AS (
    SELECT
        research_id,
        fna_index,
        TRY_CAST(fna_date AS DATE) AS fna_date,
        bethesda_2023_num AS bethesda_num,
        bethesda_2023_name AS bethesda_name,
        confidence AS fna_confidence,
        specimen_location,
        SUBSTRING(path_text, 1, 200) AS path_text_snippet,
        'fna_cytology' AS source_table,
        1.0 AS source_reliability
    FROM fna_cytology
    WHERE bethesda_2023_num IS NOT NULL
),

-- Source 2: fna_episode_master_v2
src_fna_episode AS (
    SELECT
        research_id,
        fna_episode_id AS fna_index,
        TRY_CAST(resolved_fna_date AS DATE) AS fna_date,
        TRY_CAST(bethesda_category AS INTEGER) AS bethesda_num,
        NULL AS bethesda_name,
        fna_confidence,
        NULL AS specimen_location,
        NULL AS path_text_snippet,
        'fna_episode_master_v2' AS source_table,
        0.92 AS source_reliability
    FROM fna_episode_master_v2
    WHERE bethesda_category IS NOT NULL
      AND TRY_CAST(bethesda_category AS INTEGER) BETWEEN 1 AND 6
),

-- Source 3: molecular_testing.fna_bethesda
src_mol_testing AS (
    SELECT
        research_id,
        test_index AS fna_index,
        TRY_CAST("date" AS DATE) AS fna_date,
        TRY_CAST(
            CASE
                WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('i','1') THEN 1
                WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('ii','2','benign') THEN 2
                WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('iii','3','aus','flus','aus/flus') THEN 3
                WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('iv','4','fn','sfn') THEN 4
                WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('v','5','suspicious','sfm') THEN 5
                WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('vi','6','malignant') THEN 6
                WHEN regexp_matches(TRIM(CAST(fna_bethesda AS VARCHAR)), '^[1-6]$')
                    THEN CAST(TRIM(CAST(fna_bethesda AS VARCHAR)) AS INTEGER)
                ELSE NULL
            END AS INTEGER) AS bethesda_num,
        CAST(fna_bethesda AS VARCHAR) AS bethesda_name,
        NULL AS fna_confidence,
        NULL AS specimen_location,
        NULL AS path_text_snippet,
        'molecular_testing' AS source_table,
        0.85 AS source_reliability
    FROM molecular_testing
    WHERE fna_bethesda IS NOT NULL
      AND TRIM(CAST(fna_bethesda AS VARCHAR)) NOT IN ('', 'x', 'X', 'null', 'N/A', 'see other')
),

-- Union all sources
all_fna AS (
    SELECT * FROM src_fna_cytology
    UNION ALL
    SELECT * FROM src_fna_episode
    UNION ALL
    SELECT * FROM src_mol_testing
),

-- Per-patient: worst (highest) Bethesda, best source
per_patient AS (
    SELECT
        research_id,
        MAX(bethesda_num) AS worst_bethesda_num,
        MIN(bethesda_num) FILTER (WHERE bethesda_num IS NOT NULL) AS best_bethesda_num,
        COUNT(DISTINCT fna_index) AS n_fna_episodes,
        COUNT(DISTINCT source_table) AS n_sources,
        STRING_AGG(DISTINCT source_table, ', ') AS source_tables,
        MIN(fna_date) FILTER (WHERE fna_date IS NOT NULL) AS first_fna_date,
        MAX(fna_date) FILTER (WHERE fna_date IS NOT NULL) AS last_fna_date,
        MAX(source_reliability) AS best_source_reliability,
        -- Prefer highest-reliability source's Bethesda
        MAX(bethesda_num) FILTER (WHERE source_reliability >= 0.95) AS gold_bethesda_num,
        COUNT(*) AS total_records
    FROM all_fna
    WHERE bethesda_num BETWEEN 1 AND 6
    GROUP BY research_id
)

SELECT
    pp.research_id,
    COALESCE(pp.gold_bethesda_num, pp.worst_bethesda_num) AS bethesda_final,
    pp.worst_bethesda_num,
    pp.best_bethesda_num,
    pp.n_fna_episodes,
    pp.n_sources,
    pp.source_tables,
    pp.first_fna_date,
    pp.last_fna_date,
    pp.best_source_reliability,
    pp.total_records,
    CASE COALESCE(pp.gold_bethesda_num, pp.worst_bethesda_num)
        WHEN 1 THEN 'Nondiagnostic/Unsatisfactory'
        WHEN 2 THEN 'Benign'
        WHEN 3 THEN 'AUS/FLUS'
        WHEN 4 THEN 'Follicular Neoplasm/SFN'
        WHEN 5 THEN 'Suspicious for Malignancy'
        WHEN 6 THEN 'Malignant'
        ELSE 'Unknown'
    END AS bethesda_final_name,
    CASE
        WHEN pp.worst_bethesda_num = pp.best_bethesda_num THEN 'uniform'
        WHEN pp.worst_bethesda_num - pp.best_bethesda_num = 1 THEN 'adjacent'
        ELSE 'discordant'
    END AS cross_fna_concordance,
    CASE
        WHEN pp.gold_bethesda_num IS NOT NULL THEN 0.95
        WHEN pp.best_source_reliability >= 0.90 THEN 0.90
        ELSE 0.80
    END AS confidence,
    CURRENT_TIMESTAMP AS refined_at
FROM per_patient pp
ORDER BY pp.research_id;
"""


def build_molecular_panel_sql() -> str:
    """Full molecular panel: per-patient, per-gene status with method and source."""
    return """
CREATE OR REPLACE TABLE extracted_molecular_panel_v1 AS
WITH
-- Primary: molecular_test_episode_v2 (richest structured data)
mol_ep AS (
    SELECT
        research_id,
        molecular_episode_id,
        platform,
        platform_raw,
        result,
        mutation,
        detailed_findings_raw,
        resolved_test_date AS test_date,
        LOWER(CAST(braf_flag AS VARCHAR)) = 'true' AS braf_pos,
        braf_variant,
        LOWER(CAST(ras_flag AS VARCHAR)) = 'true' AS ras_pos,
        ras_subtype,
        LOWER(CAST(ret_flag AS VARCHAR)) = 'true' AS ret_pos,
        LOWER(CAST(ret_fusion_flag AS VARCHAR)) = 'true' AS ret_fusion,
        LOWER(CAST(tert_flag AS VARCHAR)) = 'true' AS tert_pos,
        LOWER(CAST(ntrk_flag AS VARCHAR)) = 'true' AS ntrk_pos,
        LOWER(CAST(alk_flag AS VARCHAR)) = 'true' AS alk_pos,
        LOWER(CAST(tp53_flag AS VARCHAR)) = 'true' AS tp53_pos,
        LOWER(CAST(eif1ax_flag AS VARCHAR)) = 'true' AS eif1ax_pos,
        LOWER(CAST(pax8_pparg_flag AS VARCHAR)) = 'true' AS pax8_pparg_pos,
        LOWER(CAST(fusion_flag AS VARCHAR)) = 'true' AS any_fusion,
        LOWER(CAST(high_risk_marker_flag AS VARCHAR)) = 'true' AS high_risk,
        LOWER(CAST(inadequate_flag AS VARCHAR)) = 'true' AS inadequate,
        LOWER(CAST(cancelled_flag AS VARCHAR)) = 'true' AS cancelled,
        bethesda_category,
        'molecular_test_episode_v2' AS source_table
    FROM molecular_test_episode_v2
    WHERE LOWER(CAST(inadequate_flag AS VARCHAR)) <> 'true'
      AND LOWER(CAST(cancelled_flag AS VARCHAR)) <> 'true'
),

-- Method detection from detailed_findings_raw
mol_with_method AS (
    SELECT
        *,
        CASE
            WHEN platform IN ('ThyroSeq', 'ThyroSeq_v2', 'ThyroSeq_v3') THEN 'NGS'
            WHEN platform IN ('Afirma', 'Afirma_GSC', 'Afirma_GEC') THEN 'Afirma_GSC'
            WHEN regexp_matches(COALESCE(detailed_findings_raw,''), '(?i)\\bIHC\\b|immunohistochemistry|VE1') THEN 'IHC'
            WHEN regexp_matches(COALESCE(detailed_findings_raw,''), '(?i)\\bPCR\\b|real.time.PCR|Cobas|idylla') THEN 'PCR'
            WHEN regexp_matches(COALESCE(detailed_findings_raw,''), '(?i)\\bFISH\\b') THEN 'FISH'
            ELSE 'unknown'
        END AS test_method,
        -- BRAF variant extraction
        CASE
            WHEN braf_pos AND (braf_variant IS NOT NULL AND TRIM(braf_variant) <> '')
                THEN braf_variant
            WHEN braf_pos AND regexp_matches(COALESCE(mutation,'') || COALESCE(detailed_findings_raw,''),
                '(?i)V600E|c\\.\\s*1799T>A')
                THEN 'V600E'
            WHEN braf_pos THEN 'V600E_presumed'
            ELSE NULL
        END AS braf_variant_refined,
        -- TERT promoter type
        CASE
            WHEN tert_pos AND regexp_matches(COALESCE(mutation,'') || COALESCE(detailed_findings_raw,''),
                '(?i)C228T')
                THEN 'C228T'
            WHEN tert_pos AND regexp_matches(COALESCE(mutation,'') || COALESCE(detailed_findings_raw,''),
                '(?i)C250T')
                THEN 'C250T'
            WHEN tert_pos THEN 'promoter_unspecified'
            ELSE NULL
        END AS tert_promoter_type,
        -- RAS subtype refinement
        CASE
            WHEN ras_pos AND ras_subtype IS NOT NULL AND TRIM(ras_subtype) <> ''
                THEN ras_subtype
            WHEN ras_pos AND regexp_matches(COALESCE(mutation,''), '(?i)NRAS')
                THEN 'NRAS'
            WHEN ras_pos AND regexp_matches(COALESCE(mutation,''), '(?i)HRAS')
                THEN 'HRAS'
            WHEN ras_pos AND regexp_matches(COALESCE(mutation,''), '(?i)KRAS')
                THEN 'KRAS'
            WHEN ras_pos THEN 'RAS_unspecified'
            ELSE NULL
        END AS ras_subtype_refined
    FROM mol_ep
),

-- Per-patient aggregation
per_patient AS (
    SELECT
        research_id,
        -- BRAF
        BOOL_OR(braf_pos) AS braf_positive,
        COUNT(CASE WHEN braf_pos THEN 1 END) AS braf_positive_count,
        BOOL_OR(NOT braf_pos AND NOT inadequate AND NOT cancelled) AS braf_tested_negative,
        STRING_AGG(DISTINCT braf_variant_refined, ', ')
            FILTER (WHERE braf_variant_refined IS NOT NULL) AS braf_variants,
        -- TERT
        BOOL_OR(tert_pos) AS tert_positive,
        STRING_AGG(DISTINCT tert_promoter_type, ', ')
            FILTER (WHERE tert_promoter_type IS NOT NULL) AS tert_promoter_types,
        -- RAS
        BOOL_OR(ras_pos) AS ras_positive,
        STRING_AGG(DISTINCT ras_subtype_refined, ', ')
            FILTER (WHERE ras_subtype_refined IS NOT NULL) AS ras_subtypes,
        -- RET
        BOOL_OR(ret_pos) AS ret_positive,
        BOOL_OR(ret_fusion) AS ret_fusion_positive,
        -- NTRK
        BOOL_OR(ntrk_pos) AS ntrk_positive,
        -- ALK
        BOOL_OR(alk_pos) AS alk_positive,
        -- TP53
        BOOL_OR(tp53_pos) AS tp53_positive,
        -- EIF1AX
        BOOL_OR(eif1ax_pos) AS eif1ax_positive,
        -- PAX8-PPARG
        BOOL_OR(pax8_pparg_pos) AS pax8_pparg_positive,
        -- Any fusion
        BOOL_OR(any_fusion) AS any_fusion_positive,
        -- High risk
        BOOL_OR(high_risk) AS high_risk_marker,
        -- Method & Platform
        STRING_AGG(DISTINCT test_method, ', ')
            FILTER (WHERE test_method <> 'unknown') AS methods_used,
        STRING_AGG(DISTINCT platform, ', ')
            FILTER (WHERE platform IS NOT NULL) AS platforms_used,
        COUNT(*) AS n_molecular_tests,
        MIN(test_date) AS first_test_date,
        MAX(test_date) AS last_test_date,
        -- Tested flag (had at least one valid test)
        TRUE AS molecular_tested
    FROM mol_with_method
    GROUP BY research_id
)

SELECT
    pp.*,
    -- Composite status
    CASE
        WHEN pp.braf_positive OR pp.tert_positive THEN 'high_risk_molecular'
        WHEN pp.ras_positive THEN 'intermediate_risk_molecular'
        WHEN pp.ret_positive OR pp.ntrk_positive OR pp.alk_positive THEN 'fusion_positive'
        WHEN pp.molecular_tested THEN 'tested_negative'
        ELSE 'not_tested'
    END AS molecular_risk_category,
    -- BRAF status
    CASE
        WHEN pp.braf_positive THEN 'positive'
        WHEN pp.braf_tested_negative THEN 'negative'
        ELSE 'not_tested'
    END AS braf_status,
    -- TERT status
    CASE
        WHEN pp.tert_positive THEN 'positive'
        WHEN pp.molecular_tested AND NOT pp.tert_positive THEN 'negative'
        ELSE 'not_tested'
    END AS tert_status,
    0.92 AS confidence,
    CURRENT_TIMESTAMP AS refined_at
FROM per_patient pp
ORDER BY pp.research_id;
"""


def build_preop_imaging_reconciliation_sql() -> str:
    """Preop imaging vs pathology size and ETE concordance."""
    return """
CREATE OR REPLACE TABLE extracted_preop_imaging_concordance_v1 AS
WITH
-- Per-patient preop imaging: closest exam before surgery
surgery_dates AS (
    SELECT research_id, MIN(TRY_CAST(surg_date AS DATE)) AS first_surg_date
    FROM path_synoptics
    WHERE surg_date IS NOT NULL
    GROUP BY research_id
),

preop_imaging AS (
    SELECT
        i.research_id,
        i.size_cm_max AS imaging_size_cm,
        i.tirads_score,
        i.tirads_category,
        i.composition,
        i.echogenicity,
        i.margins AS imaging_margins,
        i.calcifications,
        i.laterality,
        i.modality,
        TRY_CAST(i.resolved_exam_date AS DATE) AS exam_date,
        i.suspicious_node_flag,
        i.nodule_id,
        s.first_surg_date,
        CASE WHEN TRY_CAST(i.resolved_exam_date AS DATE) IS NOT NULL
             AND s.first_surg_date IS NOT NULL
             THEN s.first_surg_date - TRY_CAST(i.resolved_exam_date AS DATE)
             ELSE NULL
        END AS days_before_surgery,
        ROW_NUMBER() OVER (
            PARTITION BY i.research_id
            ORDER BY
                CASE WHEN TRY_CAST(i.resolved_exam_date AS DATE) <= s.first_surg_date
                          AND TRY_CAST(i.resolved_exam_date AS DATE) >= s.first_surg_date - INTERVAL '365 days'
                     THEN 0 ELSE 1 END,
                ABS(s.first_surg_date - TRY_CAST(i.resolved_exam_date AS DATE)),
                i.size_cm_max DESC NULLS LAST
        ) AS rn
    FROM imaging_nodule_long_v2 i
    JOIN surgery_dates s ON i.research_id = s.research_id
    WHERE i.size_cm_max IS NOT NULL
),

best_preop AS (
    SELECT * FROM preop_imaging WHERE rn = 1
),

-- Path size
path_size AS (
    SELECT
        research_id,
        TRY_CAST(TRIM(REPLACE(COALESCE(tumor_1_size_greatest_dimension_cm,''), ';', '')) AS DOUBLE)
            AS path_size_cm,
        CASE
            WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension,''))
                 IN ('', 'no', 'none', 'absent', 'not identified', 'negative') THEN FALSE
            WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension,'')) IN ('x') THEN TRUE
            WHEN tumor_1_extrathyroidal_extension IS NOT NULL THEN TRUE
            ELSE NULL
        END AS path_ete_present,
        CASE
            WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension,'')) LIKE '%gross%'
                 OR LOWER(COALESCE(tumor_1_extrathyroidal_extension,'')) LIKE '%extensive%'
                THEN 'gross'
            WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension,'')) LIKE '%microscopic%'
                 OR LOWER(COALESCE(tumor_1_extrathyroidal_extension,'')) LIKE '%minimal%'
                 OR LOWER(COALESCE(tumor_1_extrathyroidal_extension,'')) LIKE '%focal%'
                THEN 'microscopic'
            WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension,''))
                 IN ('x', 'yes', 'present', 'identified') THEN 'present_ungraded'
            ELSE 'none'
        END AS path_ete_grade,
        ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY surg_date DESC) AS rn
    FROM path_synoptics
    WHERE tumor_1_size_greatest_dimension_cm IS NOT NULL
),

best_path AS (SELECT * FROM path_size WHERE rn = 1)

SELECT
    COALESCE(bp.research_id, bpath.research_id) AS research_id,

    -- Imaging
    bp.imaging_size_cm,
    bp.tirads_score,
    bp.tirads_category,
    bp.composition,
    bp.echogenicity,
    bp.imaging_margins,
    bp.calcifications,
    bp.modality,
    bp.exam_date AS preop_imaging_date,
    bp.days_before_surgery,
    bp.suspicious_node_flag AS imaging_suspicious_node,

    -- Path
    bpath.path_size_cm,
    bpath.path_ete_present,
    bpath.path_ete_grade,

    -- Size concordance
    CASE
        WHEN bp.imaging_size_cm IS NOT NULL AND bpath.path_size_cm IS NOT NULL THEN
            ROUND(ABS(bp.imaging_size_cm - bpath.path_size_cm), 2)
        ELSE NULL
    END AS size_discrepancy_cm,
    CASE
        WHEN bp.imaging_size_cm IS NOT NULL AND bpath.path_size_cm IS NOT NULL THEN
            ROUND(GREATEST(bp.imaging_size_cm, bpath.path_size_cm) /
                  GREATEST(LEAST(bp.imaging_size_cm, bpath.path_size_cm), 0.01), 2)
        ELSE NULL
    END AS size_ratio,
    CASE
        WHEN bp.imaging_size_cm IS NULL OR bpath.path_size_cm IS NULL THEN 'insufficient_data'
        WHEN ABS(bp.imaging_size_cm - bpath.path_size_cm) <= 0.5 THEN 'concordant'
        WHEN ABS(bp.imaging_size_cm - bpath.path_size_cm) <= 1.0 THEN 'minor_discrepancy'
        ELSE 'major_discrepancy'
    END AS size_concordance,

    -- ETE concordance
    CASE
        WHEN bp.research_id IS NULL OR bpath.research_id IS NULL THEN 'insufficient_data'
        WHEN bp.tirads_category IN ('TR5', '5') AND bpath.path_ete_present THEN 'concordant_suspicious'
        WHEN bp.tirads_score >= 4 AND bpath.path_ete_present THEN 'concordant_suspicious'
        WHEN (bp.tirads_category IN ('TR1','TR2','1','2') OR bp.tirads_score <= 2)
             AND NOT COALESCE(bpath.path_ete_present, FALSE) THEN 'concordant_benign'
        WHEN bp.tirads_score >= 4 AND NOT COALESCE(bpath.path_ete_present, FALSE) THEN 'imaging_overcall'
        WHEN (bp.tirads_score IS NULL OR bp.tirads_score < 4)
             AND bpath.path_ete_present THEN 'imaging_undercall'
        ELSE 'indeterminate'
    END AS ete_imaging_path_concordance,

    -- Source/confidence
    CASE
        WHEN bp.research_id IS NOT NULL AND bpath.research_id IS NOT NULL THEN 'imaging+path'
        WHEN bp.research_id IS NOT NULL THEN 'imaging_only'
        WHEN bpath.research_id IS NOT NULL THEN 'path_only'
        ELSE 'none'
    END AS data_completeness,
    CASE
        WHEN bp.research_id IS NOT NULL AND bpath.research_id IS NOT NULL THEN 0.92
        WHEN bp.research_id IS NOT NULL THEN 0.75
        WHEN bpath.research_id IS NOT NULL THEN 0.80
        ELSE 0.0
    END AS confidence,
    CURRENT_TIMESTAMP AS refined_at
FROM best_preop bp
FULL OUTER JOIN best_path bpath ON bp.research_id = bpath.research_id
ORDER BY COALESCE(bp.research_id, bpath.research_id);
"""


def build_vw_fna_by_source_sql() -> str:
    """Summary view: FNA Bethesda distribution by source."""
    return """
CREATE OR REPLACE TABLE vw_fna_by_source AS
WITH all_sources AS (
    SELECT 'fna_cytology' AS source, bethesda_2023_num AS bethesda_num, COUNT(*) AS n
    FROM fna_cytology
    WHERE bethesda_2023_num IS NOT NULL
    GROUP BY bethesda_2023_num
    UNION ALL
    SELECT 'fna_episode_master_v2' AS source,
           TRY_CAST(bethesda_category AS INTEGER) AS bethesda_num, COUNT(*) AS n
    FROM fna_episode_master_v2
    WHERE bethesda_category IS NOT NULL
      AND TRY_CAST(bethesda_category AS INTEGER) BETWEEN 1 AND 6
    GROUP BY TRY_CAST(bethesda_category AS INTEGER)
    UNION ALL
    SELECT 'molecular_testing' AS source,
           CASE
               WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('i','1') THEN 1
               WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('ii','2','benign') THEN 2
               WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('iii','3','aus','flus','aus/flus') THEN 3
               WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('iv','4','fn','sfn') THEN 4
               WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('v','5','suspicious','sfm') THEN 5
               WHEN LOWER(TRIM(CAST(fna_bethesda AS VARCHAR))) IN ('vi','6','malignant') THEN 6
               ELSE NULL
           END AS bethesda_num, COUNT(*) AS n
    FROM molecular_testing
    WHERE fna_bethesda IS NOT NULL
      AND TRIM(CAST(fna_bethesda AS VARCHAR)) NOT IN ('', 'x', 'X', 'null', 'N/A', 'see other')
    GROUP BY 2
)
SELECT source, bethesda_num,
    CASE bethesda_num
        WHEN 1 THEN 'Nondiagnostic'
        WHEN 2 THEN 'Benign'
        WHEN 3 THEN 'AUS/FLUS'
        WHEN 4 THEN 'FN/SFN'
        WHEN 5 THEN 'Suspicious'
        WHEN 6 THEN 'Malignant'
        ELSE 'Unknown'
    END AS bethesda_name,
    n,
    ROUND(100.0 * n / SUM(n) OVER (PARTITION BY source), 1) AS pct
FROM all_sources
WHERE bethesda_num IS NOT NULL
ORDER BY source, bethesda_num;
"""


def build_vw_preop_molecular_panel_sql() -> str:
    """Summary view: molecular positivity rates by gene and method."""
    return """
CREATE OR REPLACE TABLE vw_preop_molecular_panel AS
WITH gene_stats AS (
    SELECT 'BRAF' AS gene,
        COUNT(*) AS total_patients,
        COUNT(CASE WHEN braf_positive THEN 1 END) AS positive,
        COUNT(CASE WHEN braf_status = 'negative' THEN 1 END) AS negative,
        COUNT(CASE WHEN braf_status = 'not_tested' THEN 1 END) AS not_tested,
        STRING_AGG(DISTINCT braf_variants, ' | ')
            FILTER (WHERE braf_variants IS NOT NULL) AS variants_seen
    FROM extracted_molecular_panel_v1
    UNION ALL
    SELECT 'TERT' AS gene,
        COUNT(*),
        COUNT(CASE WHEN tert_positive THEN 1 END),
        COUNT(CASE WHEN tert_status = 'negative' THEN 1 END),
        COUNT(CASE WHEN tert_status = 'not_tested' THEN 1 END),
        STRING_AGG(DISTINCT tert_promoter_types, ' | ')
            FILTER (WHERE tert_promoter_types IS NOT NULL)
    FROM extracted_molecular_panel_v1
    UNION ALL
    SELECT 'RAS',
        COUNT(*),
        COUNT(CASE WHEN ras_positive THEN 1 END),
        COUNT(CASE WHEN NOT ras_positive AND molecular_tested THEN 1 END),
        COUNT(CASE WHEN NOT molecular_tested THEN 1 END),
        STRING_AGG(DISTINCT ras_subtypes, ' | ')
            FILTER (WHERE ras_subtypes IS NOT NULL)
    FROM extracted_molecular_panel_v1
    UNION ALL
    SELECT 'RET',
        COUNT(*),
        COUNT(CASE WHEN ret_positive THEN 1 END),
        COUNT(CASE WHEN NOT ret_positive AND molecular_tested THEN 1 END),
        COUNT(CASE WHEN NOT molecular_tested THEN 1 END),
        NULL
    FROM extracted_molecular_panel_v1
    UNION ALL
    SELECT 'NTRK',
        COUNT(*),
        COUNT(CASE WHEN ntrk_positive THEN 1 END),
        COUNT(CASE WHEN NOT ntrk_positive AND molecular_tested THEN 1 END),
        COUNT(CASE WHEN NOT molecular_tested THEN 1 END),
        NULL
    FROM extracted_molecular_panel_v1
    UNION ALL
    SELECT 'ALK',
        COUNT(*),
        COUNT(CASE WHEN alk_positive THEN 1 END),
        COUNT(CASE WHEN NOT alk_positive AND molecular_tested THEN 1 END),
        COUNT(CASE WHEN NOT molecular_tested THEN 1 END),
        NULL
    FROM extracted_molecular_panel_v1
    UNION ALL
    SELECT 'TP53',
        COUNT(*),
        COUNT(CASE WHEN tp53_positive THEN 1 END),
        COUNT(CASE WHEN NOT tp53_positive AND molecular_tested THEN 1 END),
        COUNT(CASE WHEN NOT molecular_tested THEN 1 END),
        NULL
    FROM extracted_molecular_panel_v1
)
SELECT
    gene,
    total_patients,
    positive,
    negative,
    not_tested,
    positive + negative AS tested,
    CASE WHEN positive + negative > 0
         THEN ROUND(100.0 * positive / (positive + negative), 1)
         ELSE 0.0
    END AS positivity_rate_pct,
    variants_seen
FROM gene_stats
ORDER BY
    CASE gene
        WHEN 'BRAF' THEN 1 WHEN 'TERT' THEN 2 WHEN 'RAS' THEN 3
        WHEN 'RET' THEN 4 WHEN 'NTRK' THEN 5 WHEN 'ALK' THEN 6
        WHEN 'TP53' THEN 7 ELSE 8
    END;
"""


def build_fna_path_concordance_sql() -> str:
    """FNA Bethesda vs surgical pathology outcome concordance."""
    return """
CREATE OR REPLACE TABLE extracted_fna_path_concordance_v1 AS
WITH
fna AS (
    SELECT research_id, bethesda_final, bethesda_final_name, first_fna_date
    FROM extracted_fna_bethesda_v1
),

path_outcome AS (
    SELECT
        research_id,
        CASE
            WHEN histology_1_type IN ('PTC','FTC','MTC','ATC','PDTC') THEN 'malignant'
            WHEN histology_1_type = 'HCC' THEN 'malignant'
            WHEN histology_1_type ILIKE '%papillary%' THEN 'malignant'
            WHEN histology_1_type ILIKE '%carcinoma%' THEN 'malignant'
            WHEN histology_1_type ILIKE '%NIFTP%' THEN 'borderline'
            WHEN histology_1_type ILIKE '%adenoma%' THEN 'benign'
            WHEN histology_1_type ILIKE '%hyperplasia%' THEN 'benign'
            WHEN histology_1_type ILIKE '%benign%' THEN 'benign'
            WHEN histology_1_type = 'other' THEN 'other'
            WHEN histology_1_type IS NOT NULL THEN 'other'
            ELSE 'unknown'
        END AS path_outcome,
        histology_1_type
    FROM patient_level_summary_mv
)

SELECT
    f.research_id,
    f.bethesda_final,
    f.bethesda_final_name,
    p.path_outcome,
    p.histology_1_type AS final_histology,
    -- Concordance classification
    CASE
        WHEN f.bethesda_final = 6 AND p.path_outcome = 'malignant' THEN 'true_positive'
        WHEN f.bethesda_final = 2 AND p.path_outcome = 'benign' THEN 'true_negative'
        WHEN f.bethesda_final >= 5 AND p.path_outcome = 'benign' THEN 'false_positive'
        WHEN f.bethesda_final <= 2 AND p.path_outcome = 'malignant' THEN 'false_negative'
        WHEN f.bethesda_final IN (3,4) AND p.path_outcome = 'malignant' THEN 'indeterminate_malignant'
        WHEN f.bethesda_final IN (3,4) AND p.path_outcome = 'benign' THEN 'indeterminate_benign'
        WHEN p.path_outcome = 'borderline' THEN 'borderline_niftp'
        ELSE 'unclassifiable'
    END AS concordance_category,
    CASE
        WHEN f.bethesda_final >= 5 AND p.path_outcome = 'malignant' THEN TRUE
        WHEN f.bethesda_final <= 2 AND p.path_outcome = 'benign' THEN TRUE
        ELSE FALSE
    END AS concordant,
    CURRENT_TIMESTAMP AS refined_at
FROM fna f
JOIN path_outcome p ON f.research_id = p.research_id
ORDER BY f.research_id;
"""


def build_master_clinical_v6_sql() -> str:
    """patient_refined_master_clinical_v6 — v5 + Phase 7 preop & molecular panel."""
    return """
CREATE OR REPLACE TABLE patient_refined_master_clinical_v6 AS
SELECT
    v5.*,

    -- Phase 7: FNA Bethesda
    fb.bethesda_final,
    fb.bethesda_final_name,
    fb.worst_bethesda_num,
    fb.n_fna_episodes,
    fb.n_sources AS fna_n_sources,
    fb.source_tables AS fna_source_tables,
    fb.first_fna_date,
    fb.last_fna_date,
    fb.cross_fna_concordance,
    fb.confidence AS fna_confidence,

    -- Phase 7: Molecular panel (replaces v5 molecular)
    mp.braf_positive AS braf_positive_v7,
    mp.braf_status AS braf_status_v7,
    mp.braf_variants,
    mp.tert_positive AS tert_positive_v7,
    mp.tert_status AS tert_status_v7,
    mp.tert_promoter_types,
    mp.ras_positive AS ras_positive_v7,
    mp.ras_subtypes,
    mp.ret_positive AS ret_positive_v7,
    mp.ret_fusion_positive,
    mp.ntrk_positive AS ntrk_positive_v7,
    mp.alk_positive AS alk_positive_v7,
    mp.tp53_positive AS tp53_positive_v7,
    mp.eif1ax_positive,
    mp.pax8_pparg_positive,
    mp.any_fusion_positive,
    mp.high_risk_marker AS high_risk_molecular_v7,
    mp.methods_used AS molecular_methods,
    mp.platforms_used AS molecular_platforms_v7,
    mp.n_molecular_tests AS n_molecular_tests_v7,
    mp.molecular_risk_category,
    mp.molecular_tested AS molecular_tested_v7,

    -- Phase 7: FNA-path concordance
    fc.path_outcome AS fna_path_outcome,
    fc.concordance_category AS fna_path_concordance_category,
    fc.concordant AS fna_path_concordant,

    -- Phase 7: Preop imaging concordance
    ic.imaging_size_cm AS preop_imaging_size_cm,
    ic.tirads_score AS preop_tirads_score,
    ic.tirads_category AS preop_tirads_category,
    ic.composition AS preop_composition,
    ic.echogenicity AS preop_echogenicity,
    ic.preop_imaging_date,
    ic.days_before_surgery AS imaging_days_before_surgery,
    ic.imaging_suspicious_node,
    ic.size_discrepancy_cm,
    ic.size_concordance,
    ic.ete_imaging_path_concordance,
    ic.data_completeness AS imaging_data_completeness

FROM patient_refined_master_clinical_v5 v5
LEFT JOIN extracted_fna_bethesda_v1 fb ON v5.research_id = fb.research_id
LEFT JOIN extracted_molecular_panel_v1 mp ON v5.research_id = mp.research_id
LEFT JOIN extracted_fna_path_concordance_v1 fc ON v5.research_id = fc.research_id
LEFT JOIN extracted_preop_imaging_concordance_v1 ic ON v5.research_id = ic.research_id
ORDER BY v5.research_id;
"""


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
_PHASE7_STEPS = [
    ("fna_bethesda", build_fna_by_source_sql),
    ("molecular_panel", build_molecular_panel_sql),
    ("preop_imaging", build_preop_imaging_reconciliation_sql),
    ("fna_path_concordance", build_fna_path_concordance_sql),
    ("vw_fna_by_source", build_vw_fna_by_source_sql),
    ("vw_preop_molecular_panel", build_vw_preop_molecular_panel_sql),
    ("master_v6", build_master_clinical_v6_sql),
]


def audit_and_refine_phase7(
    con,
    variables: list[str] | None = None,
    verbose: bool = True,
) -> dict[str, dict]:
    steps = _PHASE7_STEPS
    if variables:
        steps = [(n, fn) for n, fn in _PHASE7_STEPS if n in variables or "all" in variables]

    results = {}

    for step_name, sql_builder in steps:
        if verbose:
            print(f"\n{'='*70}")
            print(f"  Phase 7: {step_name}")
            print(f"{'='*70}")

        sql = sql_builder()
        table_name = _extract_table_name(sql)

        try:
            con.execute(sql)
            n = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if verbose:
                print(f"  {table_name}: {n} rows")
            results[step_name] = {"table": table_name, "rows": n, "status": "ok"}

            if step_name == "fna_bethesda":
                results[step_name].update(_fna_stats(con))
            elif step_name == "molecular_panel":
                results[step_name].update(_molecular_panel_stats(con))
            elif step_name == "preop_imaging":
                results[step_name].update(_imaging_stats(con))
            elif step_name == "fna_path_concordance":
                results[step_name].update(_fna_path_stats(con))

        except Exception as e:
            if verbose:
                print(f"  [ERROR] {step_name}: {e}")
            results[step_name] = {"error": str(e), "status": "failed"}

    return results


def _fna_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(bethesda_final) AS with_bethesda,
            COUNT(CASE WHEN bethesda_final = 6 THEN 1 END) AS malignant,
            COUNT(CASE WHEN bethesda_final = 5 THEN 1 END) AS suspicious,
            COUNT(CASE WHEN bethesda_final = 4 THEN 1 END) AS fn_sfn,
            COUNT(CASE WHEN bethesda_final = 3 THEN 1 END) AS aus_flus,
            COUNT(CASE WHEN bethesda_final = 2 THEN 1 END) AS benign,
            COUNT(CASE WHEN bethesda_final = 1 THEN 1 END) AS nondiag,
            AVG(n_fna_episodes) AS avg_fna_episodes,
            COUNT(CASE WHEN n_sources > 1 THEN 1 END) AS multi_source,
            COUNT(CASE WHEN cross_fna_concordance = 'discordant' THEN 1 END) AS discordant
        FROM extracted_fna_bethesda_v1
    """).fetchone()
    return {
        "with_bethesda": row[1], "malignant": row[2], "suspicious": row[3],
        "fn_sfn": row[4], "aus_flus": row[5], "benign": row[6], "nondiag": row[7],
        "avg_fna_episodes": round(row[8], 1) if row[8] else 0,
        "multi_source_patients": row[9], "discordant_patients": row[10],
    }


def _molecular_panel_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN braf_positive THEN 1 END) AS braf_pos,
            COUNT(CASE WHEN braf_status = 'negative' THEN 1 END) AS braf_neg,
            COUNT(CASE WHEN tert_positive THEN 1 END) AS tert_pos,
            COUNT(CASE WHEN tert_status = 'negative' THEN 1 END) AS tert_neg,
            COUNT(CASE WHEN ras_positive THEN 1 END) AS ras_pos,
            COUNT(CASE WHEN ret_positive THEN 1 END) AS ret_pos,
            COUNT(CASE WHEN ntrk_positive THEN 1 END) AS ntrk_pos,
            COUNT(CASE WHEN alk_positive THEN 1 END) AS alk_pos,
            COUNT(CASE WHEN tp53_positive THEN 1 END) AS tp53_pos,
            COUNT(CASE WHEN molecular_risk_category = 'high_risk_molecular' THEN 1 END) AS high_risk,
            STRING_AGG(DISTINCT methods_used, ' | ')
                FILTER (WHERE methods_used IS NOT NULL) AS all_methods
        FROM extracted_molecular_panel_v1
    """).fetchone()
    tested = (row[1] or 0) + (row[2] or 0)
    return {
        "total_patients": row[0],
        "braf_positive": row[1], "braf_negative": row[2],
        "braf_positivity_pct": round(100 * row[1] / max(tested, 1), 1),
        "tert_positive": row[3], "tert_negative": row[4],
        "ras_positive": row[5], "ret_positive": row[6],
        "ntrk_positive": row[7], "alk_positive": row[8],
        "tp53_positive": row[9], "high_risk_molecular": row[10],
        "methods_detected": row[11],
    }


def _imaging_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(imaging_size_cm) AS with_imaging_size,
            COUNT(path_size_cm) AS with_path_size,
            COUNT(CASE WHEN size_concordance = 'concordant' THEN 1 END) AS concordant,
            COUNT(CASE WHEN size_concordance = 'minor_discrepancy' THEN 1 END) AS minor_disc,
            COUNT(CASE WHEN size_concordance = 'major_discrepancy' THEN 1 END) AS major_disc,
            AVG(size_discrepancy_cm) FILTER (WHERE size_discrepancy_cm IS NOT NULL) AS avg_disc,
            COUNT(tirads_score) AS with_tirads,
            AVG(tirads_score) FILTER (WHERE tirads_score IS NOT NULL) AS avg_tirads,
            COUNT(CASE WHEN ete_imaging_path_concordance = 'imaging_overcall' THEN 1 END) AS overcall,
            COUNT(CASE WHEN ete_imaging_path_concordance = 'imaging_undercall' THEN 1 END) AS undercall
        FROM extracted_preop_imaging_concordance_v1
    """).fetchone()
    size_total = (row[3] or 0) + (row[4] or 0) + (row[5] or 0)
    return {
        "with_imaging_size": row[1], "with_path_size": row[2],
        "size_concordant": row[3], "size_minor_disc": row[4], "size_major_disc": row[5],
        "avg_discrepancy_cm": round(row[6], 2) if row[6] else None,
        "with_tirads": row[7], "avg_tirads": round(row[8], 1) if row[8] else None,
        "ete_overcall": row[9], "ete_undercall": row[10],
        "size_concordance_pct": round(100 * (row[3] or 0) / max(size_total, 1), 1),
    }


def _fna_path_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN concordance_category = 'true_positive' THEN 1 END) AS tp,
            COUNT(CASE WHEN concordance_category = 'true_negative' THEN 1 END) AS tn,
            COUNT(CASE WHEN concordance_category = 'false_positive' THEN 1 END) AS fp,
            COUNT(CASE WHEN concordance_category = 'false_negative' THEN 1 END) AS fn,
            COUNT(CASE WHEN concordance_category = 'indeterminate_malignant' THEN 1 END) AS indet_mal,
            COUNT(CASE WHEN concordance_category = 'indeterminate_benign' THEN 1 END) AS indet_ben,
            COUNT(CASE WHEN concordant THEN 1 END) AS concordant_total
        FROM extracted_fna_path_concordance_v1
    """).fetchone()
    return {
        "total_evaluated": row[0],
        "true_positive": row[1], "true_negative": row[2],
        "false_positive": row[3], "false_negative": row[4],
        "indeterminate_malignant": row[5], "indeterminate_benign": row[6],
        "concordance_pct": round(100 * (row[7] or 0) / max(row[0], 1), 1),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Phase 7 Preoperative & Full Molecular Panel Refinement")
    parser.add_argument("--variable", default="all",
                        choices=["all", "fna_bethesda", "molecular_panel",
                                 "preop_imaging", "fna_path_concordance",
                                 "vw_fna_by_source", "vw_preop_molecular_panel",
                                 "master_v6"])
    parser.add_argument("--md", action="store_true", default=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default="notes_extraction")
    args = parser.parse_args()

    use_md = not args.local

    if args.dry_run:
        print(f"[dry-run] Would run phase7 step={args.variable}")
        return

    con = _get_connection(use_md)

    variables = None if args.variable == "all" else [args.variable]
    results = audit_and_refine_phase7(con, variables=variables, verbose=True)

    out_dir = PROJECT_ROOT / args.output_dir
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    lines = ["# Phase 7 Preoperative & Full Molecular Panel Refinement Report",
             f"_Generated: {timestamp}_", ""]
    for step, rpt in results.items():
        lines.append(f"## {step}")
        for k, v in rpt.items():
            lines.append(f"- **{k}:** {v}")
        lines.append("")

    report_path = out_dir / f"phase7_preop_molecular_refinement_{timestamp}.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[phase7] Report saved: {report_path}")

    con.close()


if __name__ == "__main__":
    main()
