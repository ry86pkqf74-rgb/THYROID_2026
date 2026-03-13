"""
Extraction Audit Engine v9 — Phase 11: Final Sweep (Imaging, RAS, BRAF, Pre-op Excel)
======================================================================================
Final closure engine. Extends v8 with:
  1. USImagingTIRADSParser       – extract TIRADS scores/categories from clinical notes
  2. NoduleSizeExtractor         – extract nodule sizes (cm/mm) from clinical notes
  3. RASMolecularSubtyper        – parse RAS subtypes from mutation text + NLP entities
  4. BRAF_IHC_NLP_Recovery       – cross-source BRAF recovery (IHC, NLP, mutation text)
  5. PreOpExcelFinalSweep        – mine genetic_testing & molecular_testing text fields

Source hierarchy: molecular_testing_structured (1.0) > genetic_testing (0.95) >
                  nlp_path_report (0.90) > nlp_entities (0.85) > nlp_clinical_note (0.75)

Usage:
    from notes_extraction.extraction_audit_engine_v9 import audit_and_refine_phase11
    results = audit_and_refine_phase11(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v9.py --md --variable all
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from notes_extraction.extraction_audit_engine_v4 import _extract_table_name
from notes_extraction.extraction_audit_engine_v3 import _get_connection

PHASE11_SOURCE_HIERARCHY = {
    "molecular_testing_structured": 1.0,
    "genetic_testing_structured": 0.95,
    "molecular_test_episode_v2": 0.92,
    "nlp_path_report": 0.90,
    "nlp_entities_genetics": 0.85,
    "ihc_pathology_note": 0.82,
    "nlp_clinical_note": 0.75,
    "nlp_us_report": 0.70,
    "excel_preop_sweep": 0.65,
}

# ---------------------------------------------------------------------------
# 1. USImagingTIRADSParser — TIRADS from clinical note NLP
# ---------------------------------------------------------------------------
_TIRADS_SCORE_RE = re.compile(
    r"(?:TI-?RADS|ACR\s*TI-?RADS)\s*(?:score\s*(?:of\s*)?)?([1-5])(?:\s*([a-c]))?",
    re.IGNORECASE,
)
_TIRADS_CATEGORY_MAP = {
    "1": "TR1_Benign",
    "2": "TR2_Not_Suspicious",
    "3": "TR3_Mildly_Suspicious",
    "4": "TR4_Moderately_Suspicious",
    "5": "TR5_Highly_Suspicious",
}

_NODULE_SIZE_CM_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:x\s*(\d+(?:\.\d+)?)\s*(?:x\s*(\d+(?:\.\d+)?)\s*)?)?cm",
    re.IGNORECASE,
)
_NODULE_SIZE_MM_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:x\s*(\d+(?:\.\d+)?)\s*(?:x\s*(\d+(?:\.\d+)?)\s*)?)?mm",
    re.IGNORECASE,
)
_NODULE_CONTEXT_RE = re.compile(
    r"(?:nodule|thyroid|lobe|isthmus).{0,80}?(\d+(?:\.\d+)?)\s*(?:x\s*(\d+(?:\.\d+)?)\s*(?:x\s*(\d+(?:\.\d+)?)\s*)?)?(?:cm|mm)",
    re.IGNORECASE,
)

_CONSENT_SKIP_RE = re.compile(
    r"\b(?:risks?\s+(?:include|of)|informed\s+consent|complications?\s+(?:include|such))\b",
    re.IGNORECASE,
)


class USImagingTIRADSParser:
    """Extract TIRADS score, category, and nodule size from clinical notes."""

    def extract_tirads(self, note_text: str, note_type: str) -> dict | None:
        if not note_text or len(note_text) < 20:
            return None
        text = note_text
        if note_type in ("h_p", "op_note") and _CONSENT_SKIP_RE.search(text[:600]):
            consent_end = text.find("\n\n", 500)
            if consent_end > 0:
                text = text[consent_end:]

        m = _TIRADS_SCORE_RE.search(text)
        if not m:
            return None
        score = int(m.group(1))
        subletter = m.group(2) or ""
        category = _TIRADS_CATEGORY_MAP.get(str(score), f"TR{score}")

        return {
            "tirads_score": score,
            "tirads_subletter": subletter.lower() if subletter else None,
            "tirads_category": category,
            "source": f"nlp_{note_type}",
            "confidence": 0.85 if note_type in ("other_history", "endocrine_note") else 0.70,
        }

    def extract_nodule_size(self, note_text: str, note_type: str) -> dict | None:
        if not note_text or len(note_text) < 20:
            return None
        text = note_text
        if note_type in ("h_p", "op_note") and _CONSENT_SKIP_RE.search(text[:600]):
            consent_end = text.find("\n\n", 500)
            if consent_end > 0:
                text = text[consent_end:]

        sizes = []
        for m in _NODULE_CONTEXT_RE.finditer(text):
            dims = [float(m.group(i)) for i in (1, 2, 3) if m.group(i)]
            unit = "mm" if "mm" in m.group(0).lower() else "cm"
            if unit == "mm":
                dims = [d / 10.0 for d in dims]
            if dims and 0.1 <= max(dims) <= 15.0:
                sizes.append({"max_cm": max(dims), "dims": dims})

        if not sizes:
            for m in _NODULE_SIZE_CM_RE.finditer(text):
                dims = [float(m.group(i)) for i in (1, 2, 3) if m.group(i)]
                if dims and 0.1 <= max(dims) <= 15.0:
                    sizes.append({"max_cm": max(dims), "dims": dims})
            for m in _NODULE_SIZE_MM_RE.finditer(text):
                dims = [float(m.group(i)) / 10.0 for i in (1, 2, 3) if m.group(i)]
                if dims and 0.1 <= max(dims) <= 15.0:
                    sizes.append({"max_cm": max(dims), "dims": dims})

        if not sizes:
            return None
        best = max(sizes, key=lambda s: s["max_cm"])
        return {
            "size_cm_max": round(best["max_cm"], 2),
            "size_cm_x": round(best["dims"][0], 2) if len(best["dims"]) >= 1 else None,
            "size_cm_y": round(best["dims"][1], 2) if len(best["dims"]) >= 2 else None,
            "size_cm_z": round(best["dims"][2], 2) if len(best["dims"]) >= 3 else None,
            "n_dimensions": len(best["dims"]),
            "source": f"nlp_{note_type}",
            "confidence": 0.75 if note_type in ("h_p", "endocrine_note") else 0.60,
        }


# ---------------------------------------------------------------------------
# 2. RASMolecularSubtyper — parse RAS from mutation text + entities
# ---------------------------------------------------------------------------
_RAS_GENE_RE = re.compile(
    r"\b(N|H|K)RAS\b", re.IGNORECASE,
)
_RAS_VARIANT_RE = re.compile(
    r"\b([NHK]RAS)\s*"
    r"(?:mutation\s+)?(?:POSITIVE\s*)?"
    r"(?:\(?p\.?\s*)?([A-Z]\d+[A-Z](?:/[A-Z])?)?"
    r"(?:\s*,?\s*c\.?\s*([\d_]+(?:del)?(?:ins)?[A-Z>]+\d*))?"
    r"(?:\s*,?\s*(?:AF:?\s*)?(\d+(?:\.\d+)?)\s*%)?",
    re.IGNORECASE,
)
_RAS_POSITIVE_RE = re.compile(
    r"(?:positive\s+(?:for\s+)?)?([NHK]RAS)\s*(?:mutation)?\s*(?:POSITIVE|detected|identified)",
    re.IGNORECASE,
)


class RASMolecularSubtyper:
    """Parse RAS gene subtypes and variants from mutation text fields."""

    def parse_mutation_text(self, mutation_text: str, detailed_findings: str = None) -> list[dict]:
        results = []
        seen = set()
        for text in [mutation_text, detailed_findings]:
            if not text or str(text).strip() in ("", "x", "nan", "None", "none"):
                continue
            for m in _RAS_VARIANT_RE.finditer(str(text)):
                gene = m.group(1).upper() + "RAS"
                protein = m.group(2) if m.group(2) else None
                cdna = m.group(3) if m.group(3) else None
                af = float(m.group(4)) if m.group(4) else None
                key = (gene, protein or "")
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "ras_gene": gene,
                        "ras_protein_change": protein,
                        "ras_cdna_change": cdna,
                        "allele_frequency_pct": af,
                        "source": "mutation_text",
                        "confidence": 0.95,
                    })
            for m in _RAS_POSITIVE_RE.finditer(str(text)):
                gene = m.group(1).upper() + "RAS"
                key = (gene, "")
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "ras_gene": gene,
                        "ras_protein_change": None,
                        "ras_cdna_change": None,
                        "allele_frequency_pct": None,
                        "source": "mutation_text_positive",
                        "confidence": 0.90,
                    })
        return results

    def parse_entity(self, entity_value_norm: str, present_or_negated: str) -> dict | None:
        if present_or_negated != "present":
            return None
        val = str(entity_value_norm).strip().upper()
        if val in ("NRAS", "HRAS", "KRAS"):
            return {
                "ras_gene": val,
                "ras_protein_change": None,
                "ras_cdna_change": None,
                "allele_frequency_pct": None,
                "source": "nlp_entities_genetics",
                "confidence": 0.80,
            }
        if val == "RAS":
            return {
                "ras_gene": "RAS_unspecified",
                "ras_protein_change": None,
                "ras_cdna_change": None,
                "allele_frequency_pct": None,
                "source": "nlp_entities_genetics",
                "confidence": 0.70,
            }
        return None


# ---------------------------------------------------------------------------
# 3. BRAF_IHC_NLP_Recovery — cross-source BRAF validation/recovery
# ---------------------------------------------------------------------------
_BRAF_V600E_RE = re.compile(
    r"\bBRAF\s*(?:p\.?\s*)?V600E\b", re.IGNORECASE,
)
_BRAF_POSITIVE_RE = re.compile(
    r"\bBRAF\s*(?:mutation\s*)?(?:is\s+)?(?:POSITIVE|detected|identified|present)\b",
    re.IGNORECASE,
)
_BRAF_NEGATIVE_RE = re.compile(
    r"\bBRAF\s*(?:mutation\s*)?(?:is\s+)?(?:NEGATIVE|not\s+detected|wild[- ]?type|negative)\b",
    re.IGNORECASE,
)
_BRAF_IHC_RE = re.compile(
    r"\b(?:VE1|BRAF\s*V600E)\s*(?:immunohistochem|immunostain|IHC|stain)",
    re.IGNORECASE,
)
_BRAF_IHC_POS_RE = re.compile(
    r"(?:VE1|BRAF\s*(?:V600E\s*)?(?:IHC|immunostain|stain))\s*(?:is\s+)?(?:positive|detected|present)",
    re.IGNORECASE,
)


class BRAFIHCNLPRecovery:
    """Cross-source BRAF recovery: molecular testing, IHC, NLP entities, note text."""

    def parse_mutation_text(self, mutation_text: str, detailed_findings: str = None) -> dict | None:
        for text in [mutation_text, detailed_findings]:
            if not text or str(text).strip() in ("", "x", "nan", "None", "none"):
                continue
            t = str(text)
            if _BRAF_V600E_RE.search(t):
                return {
                    "braf_status": "positive",
                    "braf_variant": "V600E",
                    "detection_method": "NGS",
                    "source": "mutation_text",
                    "confidence": 0.98,
                }
            if _BRAF_POSITIVE_RE.search(t) and not _BRAF_NEGATIVE_RE.search(t):
                return {
                    "braf_status": "positive",
                    "braf_variant": "V600E_presumed",
                    "detection_method": "NGS",
                    "source": "mutation_text",
                    "confidence": 0.92,
                }
        return None

    def parse_ihc_from_note(self, note_text: str, note_type: str) -> dict | None:
        if not note_text or len(note_text) < 30:
            return None
        t = str(note_text)
        if _BRAF_IHC_RE.search(t):
            if _BRAF_IHC_POS_RE.search(t):
                return {
                    "braf_status": "positive",
                    "braf_variant": "V600E",
                    "detection_method": "IHC_VE1",
                    "source": f"ihc_{note_type}",
                    "confidence": 0.88,
                }
        if _BRAF_V600E_RE.search(t) and _BRAF_POSITIVE_RE.search(t):
            if not _BRAF_NEGATIVE_RE.search(t):
                return {
                    "braf_status": "positive",
                    "braf_variant": "V600E",
                    "detection_method": "NGS_or_IHC",
                    "source": f"nlp_{note_type}",
                    "confidence": 0.82,
                }
        return None

    def parse_entity(self, entity_value_norm: str, present_or_negated: str,
                      note_text: str | None = None) -> dict | None:
        """NLP entity 'present' only means non-negated mention.
        Require explicit positive qualifier in surrounding note text."""
        if present_or_negated != "present":
            return None
        if "BRAF" not in str(entity_value_norm).upper():
            return None
        if note_text:
            t = str(note_text).lower()
            has_positive = bool(re.search(
                r'braf.{0,30}(positive|pos\b|detected|mutation\s+(identified|detected|present)|v600e)', t))
            has_negative = bool(re.search(
                r'braf.{0,15}(negative|neg\b|not\s+detected|wild.?type)', t))
            if not has_positive or has_negative:
                return None
        return {
            "braf_status": "positive",
            "braf_variant": None,
            "detection_method": "NLP_entity_confirmed",
            "source": "nlp_entities_genetics",
            "confidence": 0.82,
        }


# ---------------------------------------------------------------------------
# 4. PreOpExcelFinalSweep — mine genetic_testing & molecular_testing text
# ---------------------------------------------------------------------------
_GENE_MUTATION_RE = re.compile(
    r"\b(BRAF|NRAS|HRAS|KRAS|TERT|RET|NTRK|ALK|TP53|EIF1AX|DICER1|PTEN|PIK3CA|TSHR|GNAS|PAX8|PPARG|MET)\b"
    r"(?:\s*(?:p\.?\s*)?([A-Z]\d+[A-Z_/]+(?:splice)?))?"
    r"(?:\s*(?:c\.?\s*)?([0-9_\->A-Za-z]+))?"
    r"(?:\s*(?:AF:?\s*)?(\d+(?:\.\d+)?)\s*%)?",
    re.IGNORECASE,
)
_FUSION_RE = re.compile(
    r"\b((?:PAX8|NTRK[123]?|RET|ALK|BRAF|PPARG|THADA|CREB3L2|ETV6|NCOA4)[-/](?:PPARG|GLIS[123]|PTC[123]|ALK|BRAF|ROS1|MET|NTRK|THADA|STRN|EML4))\b",
    re.IGNORECASE,
)
_RESULT_POSITIVE_RE = re.compile(
    r"\b(POSITIVE|Suspicious|>?\s*(?:50|80|90|95|99)\s*%)\b",
    re.IGNORECASE,
)


class PreOpExcelFinalSweep:
    """Mine genetic_testing and molecular_testing text fields for missed variants."""

    def parse_excel_row(self, row: dict) -> list[dict]:
        results = []
        seen = set()
        text_fields = []
        for k in ("MUTATION_1", "MUTATION-2", "MUTATION_3",
                   "Detailed findings_1", "Detailed findings_2", "Detailed findings_3",
                   "mutation", "detailed_findings", "result"):
            val = row.get(k)
            if val and str(val).strip() not in ("", "nan", "None", "x", "none", "see other"):
                text_fields.append((k, str(val)))

        for field_name, text in text_fields:
            for m in _GENE_MUTATION_RE.finditer(text):
                gene = m.group(1).upper()
                protein = m.group(2) if m.group(2) else None
                cdna = m.group(3) if m.group(3) else None
                af = float(m.group(4)) if m.group(4) else None
                key = (gene, protein or "")
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "gene": gene,
                        "protein_change": protein,
                        "cdna_change": cdna,
                        "allele_frequency_pct": af,
                        "source_field": field_name,
                        "source": "excel_preop_sweep",
                        "confidence": 0.90 if "Detailed" in field_name else 0.85,
                    })
            for m in _FUSION_RE.finditer(text):
                fusion = m.group(1).upper()
                key = ("FUSION", fusion)
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "gene": "FUSION",
                        "protein_change": fusion,
                        "cdna_change": None,
                        "allele_frequency_pct": None,
                        "source_field": field_name,
                        "source": "excel_preop_sweep",
                        "confidence": 0.88,
                    })
        return results


# ---------------------------------------------------------------------------
# SQL Builders
# ---------------------------------------------------------------------------

def build_us_tirads_sql() -> str:
    """TIRADS + nodule sizes from NLP on clinical notes."""
    return """
CREATE OR REPLACE TABLE extracted_us_tirads_v1 AS
WITH tirads_raw AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        note_type,
        TRY_CAST(NULLIF(regexp_extract(note_text, '(?i)TI-?RADS\\s*([1-5])', 1), '') AS INTEGER) AS tirads_score,
        LOWER(NULLIF(regexp_extract(note_text, '(?i)TI-?RADS\\s*[1-5]([a-c])', 1), '')) AS tirads_subletter,
        CASE
            WHEN note_type IN ('other_history','history_summary','endocrine_note') THEN 0.85
            ELSE 0.70
        END AS confidence,
        'nlp_' || note_type AS source
    FROM clinical_notes_long
    WHERE regexp_matches(LOWER(note_text), 'ti-?rads\\s*[1-5]')
),
tirads_scored AS (
    SELECT *,
        CASE tirads_score
            WHEN 1 THEN 'TR1_Benign'
            WHEN 2 THEN 'TR2_Not_Suspicious'
            WHEN 3 THEN 'TR3_Mildly_Suspicious'
            WHEN 4 THEN 'TR4_Moderately_Suspicious'
            WHEN 5 THEN 'TR5_Highly_Suspicious'
        END AS tirads_category
    FROM tirads_raw
    WHERE tirads_score IS NOT NULL AND tirads_score BETWEEN 1 AND 5
),
tirads_deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY tirads_score DESC, confidence DESC
        ) AS rn
    FROM tirads_scored
)
SELECT
    research_id,
    tirads_score,
    tirads_subletter,
    tirads_category,
    confidence,
    source,
    CURRENT_TIMESTAMP AS extracted_at
FROM tirads_deduped
WHERE rn = 1
"""


def build_nodule_sizes_sql() -> str:
    """Nodule sizes from NLP on clinical notes — largest per patient."""
    return """
CREATE OR REPLACE TABLE extracted_nodule_sizes_v1 AS
WITH size_raw AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        note_type,
        regexp_extract(note_text,
            '(?i)(?:nodule|thyroid|lobe).{0,80}?(\\d+(?:\\.\\d+)?)\\s*(?:x\\s*\\d+(?:\\.\\d+)?\\s*(?:x\\s*\\d+(?:\\.\\d+)?\\s*)?)?cm',
            1
        ) AS size_raw_str,
        'nlp_' || note_type AS source,
        CASE WHEN note_type IN ('h_p','endocrine_note') THEN 0.70 ELSE 0.60 END AS confidence
    FROM clinical_notes_long
    WHERE regexp_matches(LOWER(note_text),
        '(?:nodule|thyroid|lobe).{0,80}?\\d+(?:\\.\\d+)?\\s*(?:x\\s*\\d+(?:\\.\\d+)?\\s*(?:x\\s*\\d+(?:\\.\\d+)?\\s*)?)?cm')
),
size_parsed AS (
    SELECT *,
        TRY_CAST(size_raw_str AS DOUBLE) AS size_cm
    FROM size_raw
    WHERE size_raw_str IS NOT NULL
),
size_valid AS (
    SELECT *
    FROM size_parsed
    WHERE size_cm IS NOT NULL AND size_cm BETWEEN 0.1 AND 15.0
),
size_deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY size_cm DESC, confidence DESC
        ) AS rn
    FROM size_valid
)
SELECT
    research_id,
    size_cm AS nodule_size_cm_max,
    source,
    confidence,
    CURRENT_TIMESTAMP AS extracted_at
FROM size_deduped
WHERE rn = 1
"""


def build_ras_subtypes_sql() -> str:
    """RAS subtypes from molecular_testing.mutation + molecular_test_episode_v2.ras_subtype + NLP entities."""
    return """
CREATE OR REPLACE TABLE extracted_ras_subtypes_v1 AS
WITH
src_episode AS (
    SELECT DISTINCT
        CAST(research_id AS INTEGER) AS research_id,
        ras_subtype AS ras_gene,
        NULL AS ras_protein_change,
        NULL AS ras_cdna_change,
        NULL AS allele_frequency_pct,
        'molecular_test_episode_v2' AS source,
        0.95 AS confidence
    FROM molecular_test_episode_v2
    WHERE ras_subtype IS NOT NULL AND TRIM(CAST(ras_subtype AS VARCHAR)) <> ''
),
src_mutation_nras AS (
    SELECT DISTINCT
        CAST(research_id AS INTEGER) AS research_id,
        'NRAS' AS ras_gene,
        regexp_extract(CAST(mutation AS VARCHAR), '(?i)NRAS\\s*(?:p\\.?\\s*)?([A-Z]\\d+[A-Z])', 1) AS ras_protein_change,
        regexp_extract(CAST(mutation AS VARCHAR), '(?i)NRAS.{0,30}c\\.?\\s*([\\d_A-Za-z>]+)', 1) AS ras_cdna_change,
        TRY_CAST(regexp_extract(CAST(mutation AS VARCHAR), '(?i)NRAS.{0,60}(\\d+(?:\\.\\d+)?)\\s*%', 1) AS DOUBLE) AS allele_frequency_pct,
        'molecular_testing_mutation' AS source,
        0.95 AS confidence
    FROM molecular_testing
    WHERE regexp_matches(LOWER(CAST(mutation AS VARCHAR)), 'nras')
       OR regexp_matches(LOWER(CAST(detailed_findings AS VARCHAR)), 'nras')
),
src_mutation_hras AS (
    SELECT DISTINCT
        CAST(research_id AS INTEGER) AS research_id,
        'HRAS' AS ras_gene,
        regexp_extract(CAST(mutation AS VARCHAR), '(?i)HRAS\\s*(?:p\\.?\\s*)?([A-Z]\\d+[A-Z])', 1) AS ras_protein_change,
        regexp_extract(CAST(mutation AS VARCHAR), '(?i)HRAS.{0,30}c\\.?\\s*([\\d_A-Za-z>]+)', 1) AS ras_cdna_change,
        TRY_CAST(regexp_extract(CAST(mutation AS VARCHAR), '(?i)HRAS.{0,60}(\\d+(?:\\.\\d+)?)\\s*%', 1) AS DOUBLE) AS allele_frequency_pct,
        'molecular_testing_mutation' AS source,
        0.95 AS confidence
    FROM molecular_testing
    WHERE regexp_matches(LOWER(CAST(mutation AS VARCHAR)), 'hras')
       OR regexp_matches(LOWER(CAST(detailed_findings AS VARCHAR)), 'hras')
),
src_mutation_kras AS (
    SELECT DISTINCT
        CAST(research_id AS INTEGER) AS research_id,
        'KRAS' AS ras_gene,
        regexp_extract(CAST(mutation AS VARCHAR), '(?i)KRAS\\s*(?:p\\.?\\s*)?([A-Z]\\d+[A-Z])', 1) AS ras_protein_change,
        regexp_extract(CAST(mutation AS VARCHAR), '(?i)KRAS.{0,30}c\\.?\\s*([\\d_A-Za-z>]+)', 1) AS ras_cdna_change,
        TRY_CAST(regexp_extract(CAST(mutation AS VARCHAR), '(?i)KRAS.{0,60}(\\d+(?:\\.\\d+)?)\\s*%', 1) AS DOUBLE) AS allele_frequency_pct,
        'molecular_testing_mutation' AS source,
        0.95 AS confidence
    FROM molecular_testing
    WHERE regexp_matches(LOWER(CAST(mutation AS VARCHAR)), 'kras')
       OR regexp_matches(LOWER(CAST(detailed_findings AS VARCHAR)), 'kras')
),
src_entities AS (
    -- NLP entity 'present' only means non-negated mention, NOT a positive test result.
    -- Require explicit positive qualifier in the clinical note text.
    SELECT DISTINCT
        e.research_id,
        e.ras_gene,
        NULL AS ras_protein_change,
        NULL AS ras_cdna_change,
        NULL AS allele_frequency_pct,
        'nlp_entities_confirmed' AS source,
        0.82 AS confidence
    FROM (
        SELECT DISTINCT CAST(research_id AS INTEGER) as research_id,
               CASE
                   WHEN UPPER(entity_value_norm) IN ('NRAS','HRAS','KRAS') THEN UPPER(entity_value_norm)
                   WHEN UPPER(entity_value_norm) = 'RAS' THEN 'RAS_unspecified'
                   ELSE UPPER(entity_value_norm)
               END AS ras_gene,
               UPPER(entity_value_norm) as raw_gene
        FROM note_entities_genetics
        WHERE present_or_negated = 'present'
          AND UPPER(entity_value_norm) IN ('NRAS','HRAS','KRAS','RAS')
    ) e
    WHERE EXISTS (
        SELECT 1 FROM clinical_notes_long n
        WHERE n.research_id = e.research_id
          AND (regexp_matches(LOWER(n.note_text), LOWER(e.raw_gene) || '.{0,30}(positive|pos\b|detected|mutation)')
               OR regexp_matches(LOWER(n.note_text), '(positive|detected|mutation).{0,15}' || LOWER(e.raw_gene)))
          AND NOT regexp_matches(LOWER(n.note_text), LOWER(e.raw_gene) || '.{0,15}(negative|neg\\b|not\\s+detected|wild.?type)')
    )
),
all_ras AS (
    SELECT * FROM src_episode
    UNION ALL SELECT * FROM src_mutation_nras
    UNION ALL SELECT * FROM src_mutation_hras
    UNION ALL SELECT * FROM src_mutation_kras
    UNION ALL SELECT * FROM src_entities
),
ras_deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, ras_gene
            ORDER BY confidence DESC
        ) AS rn
    FROM all_ras
)
SELECT
    research_id,
    ras_gene,
    ras_protein_change,
    ras_cdna_change,
    allele_frequency_pct,
    source,
    confidence,
    CURRENT_TIMESTAMP AS extracted_at
FROM ras_deduped
WHERE rn = 1
"""


def build_ras_patient_summary_sql() -> str:
    """Per-patient RAS summary: best subtype, any positive flag."""
    return """
CREATE OR REPLACE TABLE extracted_ras_patient_summary_v1 AS
WITH per_patient AS (
    SELECT
        research_id,
        BOOL_OR(ras_gene IN ('NRAS','HRAS','KRAS')) AS ras_positive,
        BOOL_OR(ras_gene = 'NRAS') AS nras_positive,
        BOOL_OR(ras_gene = 'HRAS') AS hras_positive,
        BOOL_OR(ras_gene = 'KRAS') AS kras_positive,
        MAX(CASE WHEN ras_gene NOT IN ('RAS_unspecified') THEN ras_gene END) AS ras_primary_subtype,
        MAX(ras_protein_change) AS ras_best_protein_change,
        MAX(allele_frequency_pct) AS ras_max_allele_freq,
        MAX(confidence) AS ras_confidence,
        STRING_AGG(DISTINCT source, '; ') AS ras_sources,
        COUNT(DISTINCT ras_gene) AS ras_n_genes
    FROM extracted_ras_subtypes_v1
    GROUP BY research_id
)
SELECT *,
    CURRENT_TIMESTAMP AS extracted_at
FROM per_patient
"""


def build_braf_recovery_sql() -> str:
    """BRAF recovery from all sources with cross-validation."""
    return """
CREATE OR REPLACE TABLE extracted_braf_recovery_v1 AS
WITH
src_mol_episode AS (
    SELECT DISTINCT
        CAST(research_id AS INTEGER) AS research_id,
        'positive' AS braf_status,
        COALESCE(braf_variant, 'V600E_presumed') AS braf_variant,
        'NGS' AS detection_method,
        'molecular_test_episode_v2' AS source,
        0.98 AS confidence
    FROM molecular_test_episode_v2
    WHERE LOWER(CAST(braf_flag AS VARCHAR)) = 'true'
),
src_genetic AS (
    SELECT DISTINCT
        CAST(research_id AS INTEGER) AS research_id,
        'positive' AS braf_status,
        'V600E_presumed' AS braf_variant,
        'structured' AS detection_method,
        'genetic_testing' AS source,
        0.95 AS confidence
    FROM genetic_testing
    WHERE LOWER(CAST(any_braf_positive AS VARCHAR)) = 'true'
),
src_mutation_text AS (
    SELECT DISTINCT
        CAST(research_id AS INTEGER) AS research_id,
        'positive' AS braf_status,
        CASE
            WHEN regexp_matches(CAST(mutation AS VARCHAR), '(?i)V600E') THEN 'V600E'
            ELSE 'V600E_presumed'
        END AS braf_variant,
        CASE
            WHEN regexp_matches(LOWER(CAST(detailed_findings AS VARCHAR)), 'afirma|ihc|immunohistochem') THEN 'IHC_or_Afirma'
            ELSE 'NGS'
        END AS detection_method,
        'molecular_testing_mutation' AS source,
        0.93 AS confidence
    FROM molecular_testing
    WHERE (regexp_matches(LOWER(CAST(mutation AS VARCHAR)), 'braf.{0,20}(positive|v600|detected)')
       OR regexp_matches(LOWER(CAST(detailed_findings AS VARCHAR)), 'braf.{0,20}(positive|v600|detected)'))
      AND NOT regexp_matches(LOWER(CAST(mutation AS VARCHAR)), 'braf.{0,10}negative')
      AND NOT regexp_matches(LOWER(CAST(detailed_findings AS VARCHAR)), 'braf.{0,20}(negative|not detected|wild)')
),
src_nlp_entities AS (
    -- NLP entity 'present' only means non-negated mention, NOT a positive test result.
    -- Require explicit positive qualifier (positive/detected/V600E) in the note text
    -- to avoid counting mentions like "tested for BRAF" or "BRAF panel" as positive.
    SELECT DISTINCT
        e.research_id,
        'positive' AS braf_status,
        NULL AS braf_variant,
        'NLP_entity_confirmed' AS detection_method,
        'nlp_entities_genetics' AS source,
        0.82 AS confidence
    FROM (
        SELECT DISTINCT CAST(research_id AS INTEGER) as research_id
        FROM note_entities_genetics
        WHERE UPPER(entity_value_norm) = 'BRAF'
          AND present_or_negated = 'present'
    ) e
    WHERE EXISTS (
        SELECT 1 FROM clinical_notes_long n
        WHERE n.research_id = e.research_id
          AND LOWER(n.note_text) LIKE '%braf%'
          AND (regexp_matches(LOWER(n.note_text), 'braf.{0,30}(positive|pos\\b|detected|mutation\\s+(identified|detected|present)|v600e)')
               OR regexp_matches(LOWER(n.note_text), '(positive|detected).{0,15}braf'))
          AND NOT regexp_matches(LOWER(n.note_text), 'braf.{0,15}(negative|neg\\b|not\\s+detected|wild.?type)')
    )
),
src_clinical_notes AS (
    SELECT DISTINCT
        CAST(research_id AS INTEGER) AS research_id,
        'positive' AS braf_status,
        CASE
            WHEN regexp_matches(note_text, '(?i)V600E') THEN 'V600E'
            ELSE 'V600E_presumed'
        END AS braf_variant,
        CASE
            WHEN regexp_matches(LOWER(note_text), '(?:ve1|ihc|immunohistochem|immunostain)') THEN 'IHC_VE1'
            ELSE 'NGS_or_unknown'
        END AS detection_method,
        'nlp_clinical_note' AS source,
        0.75 AS confidence
    FROM clinical_notes_long
    WHERE regexp_matches(LOWER(note_text), 'braf.{0,20}(positive|detected|present|v600e|mutation identified)')
      AND NOT regexp_matches(LOWER(note_text), 'braf.{0,20}(negative|not detected|wild.?type)')
      AND note_type NOT IN ('h_p')
),
all_braf AS (
    SELECT * FROM src_mol_episode
    UNION ALL SELECT * FROM src_genetic
    UNION ALL SELECT * FROM src_mutation_text
    UNION ALL SELECT * FROM src_nlp_entities
    UNION ALL SELECT * FROM src_clinical_notes
),
braf_deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY confidence DESC
        ) AS rn
    FROM all_braf
)
SELECT
    research_id,
    braf_status,
    braf_variant,
    detection_method,
    source,
    confidence,
    CURRENT_TIMESTAMP AS extracted_at
FROM braf_deduped
WHERE rn = 1
"""


def build_braf_audit_sql() -> str:
    """BRAF cross-source audit view."""
    return """
CREATE OR REPLACE TABLE vw_braf_audit AS
SELECT
    b.research_id,
    b.braf_status AS braf_recovered_status,
    b.braf_variant AS braf_recovered_variant,
    b.detection_method,
    b.source AS recovery_source,
    b.confidence AS recovery_confidence,
    CASE WHEN m.braf_flag IS NOT NULL AND LOWER(CAST(m.braf_flag AS VARCHAR)) = 'true' THEN TRUE ELSE FALSE END AS braf_episode_flag,
    CASE WHEN g.any_braf_positive IS NOT NULL AND LOWER(CAST(g.any_braf_positive AS VARCHAR)) = 'true' THEN TRUE ELSE FALSE END AS braf_genetic_flag,
    CASE
        WHEN b.braf_status = 'positive' AND LOWER(CAST(m.braf_flag AS VARCHAR)) = 'true' THEN 'concordant_positive'
        WHEN b.braf_status = 'positive' AND (m.braf_flag IS NULL OR LOWER(CAST(m.braf_flag AS VARCHAR)) <> 'true') THEN 'recovered_new'
        ELSE 'other'
    END AS audit_status
FROM extracted_braf_recovery_v1 b
LEFT JOIN (
    SELECT CAST(research_id AS INTEGER) AS research_id, MAX(braf_flag) AS braf_flag
    FROM molecular_test_episode_v2 GROUP BY 1
) m ON b.research_id = m.research_id
LEFT JOIN (
    SELECT CAST(research_id AS INTEGER) AS research_id, MAX(any_braf_positive) AS any_braf_positive
    FROM genetic_testing GROUP BY 1
) g ON b.research_id = g.research_id
"""


def build_preop_excel_sweep_sql() -> str:
    """Final pre-op Excel sweep: mine all text fields for missed mutations."""
    return """
CREATE OR REPLACE TABLE extracted_preop_sweep_v1 AS
WITH mutation_text AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        CAST(mutation AS VARCHAR) AS mutation_text,
        CAST(detailed_findings AS VARCHAR) AS detailed_text,
        CAST(result AS VARCHAR) AS result_text,
        'molecular_testing' AS source_table
    FROM molecular_testing
    WHERE (mutation IS NOT NULL AND TRIM(CAST(mutation AS VARCHAR)) NOT IN ('','x','nan','none','see other'))
       OR (detailed_findings IS NOT NULL AND TRIM(CAST(detailed_findings AS VARCHAR)) NOT IN ('','x','nan'))
),
genes_found AS (
    SELECT
        research_id,
        UPPER(regexp_extract(mutation_text, '(?i)(BRAF|NRAS|HRAS|KRAS|TERT|RET|NTRK|ALK|TP53|EIF1AX|DICER1|PTEN|PIK3CA|TSHR|GNAS)', 1)) AS gene,
        regexp_extract(mutation_text, '(?i)(?:BRAF|NRAS|HRAS|KRAS|TERT|RET|NTRK|ALK|TP53|EIF1AX|DICER1|PTEN|PIK3CA|TSHR|GNAS)\\s*(?:p\\.?\\s*)?([A-Z]\\d+[A-Z_/]+)', 1) AS protein_change,
        TRY_CAST(regexp_extract(mutation_text, '(?i)(?:BRAF|NRAS|HRAS|KRAS|TERT|RET|NTRK|ALK|TP53|EIF1AX|DICER1|PTEN|PIK3CA|TSHR|GNAS).{0,80}?(\\d+(?:\\.\\d+)?)\\s*%', 1) AS DOUBLE) AS allele_freq_pct,
        CASE
            WHEN regexp_matches(LOWER(mutation_text), '(positive|detected|identified|mutation\\s+positive)') THEN 'positive'
            WHEN regexp_matches(LOWER(mutation_text), '(negative|not detected|wild.?type)') THEN 'negative'
            ELSE 'mentioned'
        END AS assertion,
        source_table,
        0.85 AS confidence
    FROM mutation_text
    WHERE regexp_matches(mutation_text, '(?i)(BRAF|NRAS|HRAS|KRAS|TERT|RET|NTRK|ALK|TP53|EIF1AX|DICER1|PTEN|PIK3CA|TSHR|GNAS)')
),
genes_deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY research_id, gene ORDER BY confidence DESC) AS rn
    FROM genes_found
    WHERE gene IS NOT NULL AND gene <> ''
)
SELECT
    research_id,
    gene,
    protein_change,
    allele_freq_pct,
    assertion,
    source_table,
    confidence,
    CURRENT_TIMESTAMP AS extracted_at
FROM genes_deduped
WHERE rn = 1
"""


def build_vw_us_tirads_sql() -> str:
    """TIRADS summary view."""
    return """
CREATE OR REPLACE TABLE vw_us_tirads AS
SELECT
    tirads_category,
    COUNT(*) AS n_patients,
    AVG(tirads_score) AS avg_score,
    AVG(confidence) AS avg_confidence
FROM extracted_us_tirads_v1
GROUP BY tirads_category
ORDER BY AVG(tirads_score)
"""


def build_vw_molecular_subtypes_sql() -> str:
    """Molecular subtypes summary."""
    return """
CREATE OR REPLACE TABLE vw_molecular_subtypes AS
SELECT
    'RAS' AS gene_family,
    ras_gene AS subtype,
    COUNT(DISTINCT research_id) AS n_patients,
    COUNT(DISTINCT CASE WHEN ras_protein_change IS NOT NULL THEN research_id END) AS has_protein_change,
    MAX(confidence) AS max_confidence
FROM extracted_ras_subtypes_v1
GROUP BY ras_gene
UNION ALL
SELECT
    'BRAF' AS gene_family,
    detection_method AS subtype,
    COUNT(DISTINCT research_id) AS n_patients,
    COUNT(DISTINCT CASE WHEN braf_variant = 'V600E' THEN research_id END) AS has_protein_change,
    MAX(confidence) AS max_confidence
FROM extracted_braf_recovery_v1
GROUP BY detection_method
ORDER BY gene_family, n_patients DESC
"""


def build_imaging_molecular_final_sql() -> str:
    """Consolidated imaging + molecular final table."""
    return """
CREATE OR REPLACE TABLE extracted_imaging_molecular_final_v1 AS
SELECT
    COALESCE(t.research_id, s.research_id, r.research_id, b.research_id) AS research_id,
    t.tirads_score,
    t.tirads_category,
    t.confidence AS tirads_confidence,
    t.source AS tirads_source,
    s.nodule_size_cm_max AS imaging_nodule_size_cm,
    s.confidence AS nodule_size_confidence,
    s.source AS nodule_size_source,
    r.ras_positive,
    r.nras_positive,
    r.hras_positive,
    r.kras_positive,
    r.ras_primary_subtype,
    r.ras_best_protein_change,
    r.ras_max_allele_freq,
    r.ras_confidence,
    r.ras_sources,
    b.braf_status AS braf_recovered_status,
    b.braf_variant AS braf_recovered_variant,
    b.detection_method AS braf_detection_method,
    b.source AS braf_source,
    b.confidence AS braf_confidence,
    CURRENT_TIMESTAMP AS extracted_at
FROM extracted_us_tirads_v1 t
FULL OUTER JOIN extracted_nodule_sizes_v1 s ON t.research_id = s.research_id
FULL OUTER JOIN extracted_ras_patient_summary_v1 r ON COALESCE(t.research_id, s.research_id) = r.research_id
FULL OUTER JOIN extracted_braf_recovery_v1 b ON COALESCE(t.research_id, s.research_id, r.research_id) = b.research_id
"""


def build_master_clinical_v10_sql() -> str:
    """Patient master clinical v10: extends v9 with Phase 11 imaging + molecular."""
    return """
CREATE OR REPLACE TABLE patient_refined_master_clinical_v10 AS
SELECT
    v9.*,
    -- Phase 11: TIRADS/Imaging
    t.tirads_score AS tirads_score_v11,
    t.tirads_category AS tirads_category_v11,
    t.confidence AS tirads_confidence_v11,
    ns.nodule_size_cm_max AS imaging_nodule_size_cm_v11,
    -- Phase 11: RAS subtypes
    r.ras_positive AS ras_positive_v11,
    r.nras_positive AS nras_positive_v11,
    r.hras_positive AS hras_positive_v11,
    r.kras_positive AS kras_positive_v11,
    r.ras_primary_subtype AS ras_primary_subtype_v11,
    r.ras_best_protein_change AS ras_protein_change_v11,
    r.ras_max_allele_freq AS ras_allele_freq_v11,
    -- Phase 11: BRAF recovery
    b.braf_status AS braf_recovered_status_v11,
    b.braf_variant AS braf_recovered_variant_v11,
    b.detection_method AS braf_detection_method_v11,
    -- Phase 11: Pre-op sweep gene count
    COALESCE(ps.n_genes_found, 0) AS preop_sweep_genes_found_v11,
    -- Phase 11: combined flags
    COALESCE(r.ras_positive, FALSE) OR COALESCE(v9.ras_positive_v7, FALSE) AS ras_positive_final,
    COALESCE(b.braf_status = 'positive', FALSE) OR COALESCE(v9.braf_positive_v7, FALSE) AS braf_positive_final
FROM patient_refined_master_clinical_v9 v9
LEFT JOIN extracted_us_tirads_v1 t ON v9.research_id = t.research_id
LEFT JOIN extracted_nodule_sizes_v1 ns ON v9.research_id = ns.research_id
LEFT JOIN extracted_ras_patient_summary_v1 r ON v9.research_id = r.research_id
LEFT JOIN extracted_braf_recovery_v1 b ON v9.research_id = b.research_id
LEFT JOIN (
    SELECT research_id, COUNT(DISTINCT gene) AS n_genes_found
    FROM extracted_preop_sweep_v1
    WHERE assertion = 'positive'
    GROUP BY research_id
) ps ON v9.research_id = ps.research_id
"""


# ---------------------------------------------------------------------------
# Phase 11 Steps Registry
# ---------------------------------------------------------------------------
_PHASE11_STEPS = [
    {"name": "us_tirads", "sql_builder": build_us_tirads_sql, "table": "extracted_us_tirads_v1"},
    {"name": "nodule_sizes", "sql_builder": build_nodule_sizes_sql, "table": "extracted_nodule_sizes_v1"},
    {"name": "ras_subtypes", "sql_builder": build_ras_subtypes_sql, "table": "extracted_ras_subtypes_v1"},
    {"name": "ras_patient_summary", "sql_builder": build_ras_patient_summary_sql, "table": "extracted_ras_patient_summary_v1"},
    {"name": "braf_recovery", "sql_builder": build_braf_recovery_sql, "table": "extracted_braf_recovery_v1"},
    {"name": "braf_audit", "sql_builder": build_braf_audit_sql, "table": "vw_braf_audit"},
    {"name": "preop_sweep", "sql_builder": build_preop_excel_sweep_sql, "table": "extracted_preop_sweep_v1"},
    {"name": "vw_us_tirads", "sql_builder": build_vw_us_tirads_sql, "table": "vw_us_tirads"},
    {"name": "vw_molecular_subtypes", "sql_builder": build_vw_molecular_subtypes_sql, "table": "vw_molecular_subtypes"},
    {"name": "imaging_molecular_final", "sql_builder": build_imaging_molecular_final_sql, "table": "extracted_imaging_molecular_final_v1"},
    {"name": "master_clinical_v10", "sql_builder": build_master_clinical_v10_sql, "table": "patient_refined_master_clinical_v10"},
]


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------
def _tirads_stats(con) -> dict:
    df = con.execute("SELECT tirads_category, COUNT(*) AS n FROM extracted_us_tirads_v1 GROUP BY tirads_category ORDER BY tirads_category").df()
    return {"distribution": df.to_dict("records"), "total_patients": int(df["n"].sum())}


def _nodule_size_stats(con) -> dict:
    df = con.execute("SELECT COUNT(*) AS n, AVG(nodule_size_cm_max) AS avg_cm, MEDIAN(nodule_size_cm_max) AS median_cm, MIN(nodule_size_cm_max) AS min_cm, MAX(nodule_size_cm_max) AS max_cm FROM extracted_nodule_sizes_v1").df()
    return df.to_dict("records")[0]


def _ras_stats(con) -> dict:
    df = con.execute("SELECT ras_gene, COUNT(DISTINCT research_id) AS n FROM extracted_ras_subtypes_v1 GROUP BY ras_gene ORDER BY n DESC").df()
    summary = con.execute("SELECT COUNT(*) AS total, SUM(CASE WHEN ras_positive THEN 1 ELSE 0 END) AS positive FROM extracted_ras_patient_summary_v1").df()
    return {"subtypes": df.to_dict("records"), "summary": summary.to_dict("records")[0]}


def _braf_stats(con) -> dict:
    df = con.execute("SELECT audit_status, COUNT(*) AS n FROM vw_braf_audit GROUP BY audit_status ORDER BY n DESC").df()
    total = con.execute("SELECT COUNT(*) AS n FROM extracted_braf_recovery_v1").df()
    methods = con.execute("SELECT detection_method, COUNT(*) AS n FROM extracted_braf_recovery_v1 GROUP BY detection_method ORDER BY n DESC").df()
    return {
        "audit": df.to_dict("records"),
        "total_braf_positive": int(total["n"].iloc[0]),
        "by_method": methods.to_dict("records"),
    }


def _preop_stats(con) -> dict:
    df = con.execute("SELECT gene, assertion, COUNT(DISTINCT research_id) AS n FROM extracted_preop_sweep_v1 GROUP BY gene, assertion ORDER BY n DESC LIMIT 20").df()
    total = con.execute("SELECT COUNT(DISTINCT research_id) AS n FROM extracted_preop_sweep_v1 WHERE assertion='positive'").df()
    return {"top_genes": df.to_dict("records"), "patients_with_positive": int(total["n"].iloc[0])}


def _master_v10_stats(con) -> dict:
    df = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN tirads_score_v11 IS NOT NULL THEN 1 END) AS has_tirads,
            COUNT(CASE WHEN imaging_nodule_size_cm_v11 IS NOT NULL THEN 1 END) AS has_nodule_size,
            COUNT(CASE WHEN ras_positive_final IS TRUE THEN 1 END) AS ras_positive_final,
            COUNT(CASE WHEN braf_positive_final IS TRUE THEN 1 END) AS braf_positive_final,
            COUNT(CASE WHEN nras_positive_v11 IS TRUE THEN 1 END) AS nras_positive,
            COUNT(CASE WHEN hras_positive_v11 IS TRUE THEN 1 END) AS hras_positive,
            COUNT(CASE WHEN kras_positive_v11 IS TRUE THEN 1 END) AS kras_positive
        FROM patient_refined_master_clinical_v10
    """).df()
    return df.to_dict("records")[0]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def audit_and_refine_phase11(
    con,
    variables: list[str] | None = None,
    verbose: bool = True,
) -> dict[str, dict]:
    """Run Phase 11 extraction, audit, and materialization."""
    results = {}
    steps = _PHASE11_STEPS
    if variables:
        steps = [s for s in steps if s["name"] in variables]

    for step in steps:
        name = step["name"]
        sql = step["sql_builder"]()
        table = step["table"]
        if verbose:
            print(f"  [{name}] Creating {table}...")
        try:
            con.execute(sql)
            cnt = con.execute(f"SELECT COUNT(*) AS n FROM {table}").df()["n"].iloc[0]
            results[name] = {"table": table, "rows": int(cnt), "status": "ok"}
            if verbose:
                print(f"    -> {cnt:,} rows")
        except Exception as e:
            results[name] = {"table": table, "rows": 0, "status": "error", "error": str(e)}
            if verbose:
                print(f"    -> ERROR: {e}")

    if verbose:
        print("\n  Collecting stats...")
    stats_map = {
        "us_tirads": _tirads_stats,
        "nodule_sizes": _nodule_size_stats,
        "ras_subtypes": _ras_stats,
        "braf_recovery": _braf_stats,
        "preop_sweep": _preop_stats,
        "master_clinical_v10": _master_v10_stats,
    }
    for name, fn in stats_map.items():
        if name in results and results[name]["status"] == "ok":
            try:
                results[name]["stats"] = fn(con)
            except Exception as e:
                results[name]["stats_error"] = str(e)

    return results


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------
def generate_report(results: dict, output_path: Path) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# Phase 11 — Final Sweep: Imaging, RAS, BRAF, Pre-op Excel",
        f"Generated: {ts}",
        "",
        "## Summary",
        "",
        "| Step | Table | Rows | Status |",
        "|------|-------|------|--------|",
    ]
    for name, info in results.items():
        lines.append(f"| {name} | `{info['table']}` | {info.get('rows', 0):,} | {info['status']} |")

    lines.extend(["", "## TIRADS Extraction"])
    if "us_tirads" in results and "stats" in results["us_tirads"]:
        s = results["us_tirads"]["stats"]
        lines.append(f"- Total patients with TIRADS: **{s['total_patients']}**")
        for d in s.get("distribution", []):
            lines.append(f"  - {d['tirads_category']}: {d['n']}")

    lines.extend(["", "## Nodule Sizes"])
    if "nodule_sizes" in results and "stats" in results["nodule_sizes"]:
        s = results["nodule_sizes"]["stats"]
        lines.append(f"- Patients: **{s.get('n', 0):,}**, Avg: {s.get('avg_cm', 0):.1f} cm, Median: {s.get('median_cm', 0):.1f} cm")

    lines.extend(["", "## RAS Subtypes"])
    if "ras_subtypes" in results and "stats" in results["ras_subtypes"]:
        s = results["ras_subtypes"]["stats"]
        lines.append(f"- Total RAS positive: **{s['summary'].get('positive', 0)}** patients")
        for d in s.get("subtypes", []):
            lines.append(f"  - {d['ras_gene']}: {d['n']} patients")

    lines.extend(["", "## BRAF Recovery"])
    if "braf_recovery" in results and "stats" in results["braf_recovery"]:
        s = results["braf_recovery"]["stats"]
        lines.append(f"- Total BRAF positive (all sources): **{s['total_braf_positive']}**")
        lines.append("- Audit breakdown:")
        for d in s.get("audit", []):
            lines.append(f"  - {d['audit_status']}: {d['n']}")
        lines.append("- By detection method:")
        for d in s.get("by_method", []):
            lines.append(f"  - {d['detection_method']}: {d['n']}")

    lines.extend(["", "## Pre-op Excel Sweep"])
    if "preop_sweep" in results and "stats" in results["preop_sweep"]:
        s = results["preop_sweep"]["stats"]
        lines.append(f"- Patients with positive mutations found: **{s['patients_with_positive']}**")
        lines.append("- Top genes:")
        for d in s.get("top_genes", [])[:10]:
            lines.append(f"  - {d['gene']} ({d['assertion']}): {d['n']} patients")

    lines.extend(["", "## Master Clinical v10 Fill Rates"])
    if "master_clinical_v10" in results and "stats" in results["master_clinical_v10"]:
        s = results["master_clinical_v10"]["stats"]
        total = s.get("total", 1)
        lines.append(f"- Total patients: **{total:,}**")
        lines.append(f"- TIRADS: {s.get('has_tirads', 0):,} ({100*s.get('has_tirads',0)/total:.1f}%)")
        lines.append(f"- Nodule size: {s.get('has_nodule_size', 0):,} ({100*s.get('has_nodule_size',0)/total:.1f}%)")
        lines.append(f"- BRAF final: {s.get('braf_positive_final', 0):,} ({100*s.get('braf_positive_final',0)/total:.1f}%)")
        lines.append(f"- RAS final: {s.get('ras_positive_final', 0):,} ({100*s.get('ras_positive_final',0)/total:.1f}%)")
        lines.append(f"  - NRAS: {s.get('nras_positive', 0):,}")
        lines.append(f"  - HRAS: {s.get('hras_positive', 0):,}")
        lines.append(f"  - KRAS: {s.get('kras_positive', 0):,}")

    report = "\n".join(lines)
    output_path.write_text(report)
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Phase 11 Final Sweep")
    parser.add_argument("--variable", default="all", help="Step name or 'all'")
    parser.add_argument("--md", action="store_true", default=True, help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    parser.add_argument("--output-dir", default="notes_extraction", help="Report output dir")
    args = parser.parse_args()

    if args.local:
        args.md = False

    con = _get_connection(use_md=args.md)
    variables = None if args.variable == "all" else [args.variable]

    if args.dry_run:
        steps = _PHASE11_STEPS
        if variables:
            steps = [s for s in steps if s["name"] in variables]
        for s in steps:
            print(f"\n-- {s['name']} --")
            print(s["sql_builder"]())
        con.close()
        return

    print(f"Phase 11 — Final Sweep ({'MotherDuck' if args.md else 'local'})")
    print("=" * 60)
    results = audit_and_refine_phase11(con, variables=variables)

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    report_path = output_dir / "master_refinement_report_phase11.md"
    report = generate_report(results, report_path)
    print(f"\nReport: {report_path}")

    results_path = output_dir / f"phase11_results_{ts}.json"
    serializable = {}
    for k, v in results.items():
        serializable[k] = {kk: vv for kk, vv in v.items() if isinstance(vv, (str, int, float, bool, list, dict, type(None)))}
    results_path.write_text(json.dumps(serializable, indent=2, default=str))
    print(f"Results: {results_path}")

    con.close()


if __name__ == "__main__":
    main()
