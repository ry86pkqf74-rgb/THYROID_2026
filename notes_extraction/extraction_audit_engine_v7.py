"""
Extraction Audit Engine v7 — Phase 9 Targeted Refinement
==========================================================
Extends v6 with:
  1. LabExpansionPipeline    – PTH/calcium from extracted_clinical_events_v4 + enhanced NLP
  2. RAIDoseParser           – RAI dose from all note types with source linking
  3. GradingRuleEngine       – ETE microscopic auto-assignment, TERT C228T/C250T HGVS,
                               ENE extent grading from path free-text

Source hierarchy: structured_db (1.0) > extracted_events (0.95) > endocrine_note (0.90)
                  > path_report (0.85) > op_note (0.80) > dc_sum (0.70) > h_p (0.20)

Usage:
    from notes_extraction.extraction_audit_engine_v7 import audit_and_refine_phase9
    results = audit_and_refine_phase9(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v7.py --md --variable all
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from notes_extraction.extraction_audit_engine_v4 import _extract_table_name
from notes_extraction.extraction_audit_engine_v3 import _get_connection

PHASE9_SOURCE_HIERARCHY = {
    "structured_db": 1.0,
    "extracted_clinical_events_v4": 0.95,
    "thyroglobulin_lab": 0.95,
    "endocrine_note": 0.90,
    "path_synoptic": 0.90,
    "op_note": 0.80,
    "dc_sum": 0.70,
    "other_history": 0.60,
    "history_summary": 0.55,
    "h_p": 0.20,
}

# ---------------------------------------------------------------------------
# 1. LabExpansionPipeline — PTH/calcium from clinical events + expanded NLP
# ---------------------------------------------------------------------------
_PTH_PATTERNS_EXPANDED = [
    re.compile(r"\b(?:PTH|parathyroid\s+hormone|intact\s+PTH|iPTH)\s*"
               r"(?:level\s*)?(?:was|of|:|=|\s)\s*(\d+(?:\.\d+)?)\s*(?:pg/m[lL])?\b", re.I),
    re.compile(r"\b(?:PTH|iPTH)\s*(\d+(?:\.\d+)?)\b", re.I),
    re.compile(r"\bPTH\s+result\s*:?\s*(\d+(?:\.\d+)?)\b", re.I),
    re.compile(r"\bintact\s+parathyroid\s+(?:hormone\s+)?(?:level\s+)?(\d+(?:\.\d+)?)\b", re.I),
]

_CALCIUM_PATTERNS_EXPANDED = [
    re.compile(r"\b(?:calcium|Ca|Ca2\+)\s*(?:level\s*)?(?:was|of|:|=|\s)?\s*(\d+(?:\.\d+)?)"
               r"\s*(?:mg/d[lL])?\b", re.I),
    re.compile(r"\b(?:calcium|Ca)\s*[:=]\s*(\d+(?:\.\d+)?)\b", re.I),
    re.compile(r"\bCa\s+(\d+(?:\.\d+)?)\s*mg/dL\b", re.I),
    re.compile(r"\btotal\s+calcium\s*:?\s*(\d+(?:\.\d+)?)\b", re.I),
    re.compile(r"\bcalcium\s+level\s+(?:was\s+)?(\d+(?:\.\d+)?)\b", re.I),
]

_IONIZED_CA_EXPANDED = re.compile(
    r"\b(?:ionized\s+(?:calcium|Ca)|iCa)\s*(?:was|of|:|=|\s)?\s*(\d+(?:\.\d+)?)\s*(?:mmol/[lL])?\b", re.I
)


class LabExpansionPipeline:
    """Expand PTH/calcium capture via extracted_clinical_events_v4 + enhanced NLP."""

    def extract_pth_value(self, text: str) -> list[dict]:
        results = []
        for pat in _PTH_PATTERNS_EXPANDED:
            for m in pat.finditer(text):
                try:
                    val = float(m.group(1))
                except (ValueError, IndexError):
                    continue
                if 0.5 <= val <= 500:
                    results.append({
                        "value": val, "unit": "pg/mL", "lab_type": "pth",
                        "match_text": m.group(0)[:80],
                    })
        return results

    def extract_calcium_value(self, text: str) -> list[dict]:
        results = []
        is_ionized = bool(_IONIZED_CA_EXPANDED.search(text))
        for pat in _CALCIUM_PATTERNS_EXPANDED:
            for m in pat.finditer(text):
                try:
                    val = float(m.group(1))
                except (ValueError, IndexError):
                    continue
                if is_ionized and 0.5 <= val <= 2.0:
                    results.append({
                        "value": val, "unit": "mmol/L", "lab_type": "ionized_calcium",
                        "match_text": m.group(0)[:80],
                    })
                elif not is_ionized and 4.0 <= val <= 15.0:
                    results.append({
                        "value": val, "unit": "mg/dL", "lab_type": "total_calcium",
                        "match_text": m.group(0)[:80],
                    })
        return results


# ---------------------------------------------------------------------------
# 2. RAIDoseParser — extract dose from all note types
# ---------------------------------------------------------------------------
_RAI_DOSE_PATTERNS = [
    re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:mCi|millicuries?)\b", re.I),
    re.compile(r"\b(?:dose|administered|received|given|treated\s+with)\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*mCi\b", re.I),
    re.compile(r"\b(?:I-?131|RAI|radioactive\s+iodine|radioiodine)\s+(?:dose\s+(?:of\s+)?)?(\d+(?:\.\d+)?)\s*mCi\b", re.I),
    re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:mCi|GBq)\s+(?:of\s+)?(?:I-?131|RAI|radioiodine)\b", re.I),
    re.compile(r"\b(?:ablation|remnant\s+ablation|adjuvant)\s+(?:with\s+)?(\d+(?:\.\d+)?)\s*mCi\b", re.I),
    re.compile(r"\b(\d{2,3}(?:\.\d+)?)\s*mCi\s+(?:I-?131|RAI|ablation|treatment)\b", re.I),
]

_RAI_NEGATION_CUES = [
    "not received", "declined", "deferred", "not a candidate",
    "not recommended", "no RAI", "without RAI",
]


class RAIDoseParser:
    """Extract RAI dose from clinical notes with source reliability scoring."""

    def extract_dose(self, text: str) -> list[dict]:
        if not text or len(text.strip()) < 20:
            return []
        results = []
        for pat in _RAI_DOSE_PATTERNS:
            for m in pat.finditer(text):
                window = text[max(0, m.start() - 80):m.start()].lower()
                if any(cue in window for cue in _RAI_NEGATION_CUES):
                    continue
                try:
                    val = float(m.group(1))
                except (ValueError, IndexError):
                    continue
                if 10 <= val <= 1000:
                    results.append({
                        "dose_mci": val,
                        "match_text": m.group(0)[:80],
                        "span_start": m.start(),
                    })
        return results

    def is_rai_context(self, text: str) -> bool:
        rai_keywords = re.compile(
            r"\b(?:RAI|I-?131|radioactive\s+iodine|radioiodine|ablation|remnant|millicurie|mCi)\b", re.I
        )
        return bool(rai_keywords.search(text or ""))


# ---------------------------------------------------------------------------
# 3. GradingRuleEngine — ETE microscopic, TERT sub-typing, ENE extent
# ---------------------------------------------------------------------------
_TERT_HGVS_C228T = [
    re.compile(r"\bTERT\s+(?:promoter\s+)?C228T\b", re.I),
    re.compile(r"\bc\.\s*-?124C>T\b", re.I),
    re.compile(r"\bTERT\s+p\.?\s*C228T\b", re.I),
    re.compile(r"\bc\.\s*1-124C>T\b", re.I),
]

_TERT_HGVS_C250T = [
    re.compile(r"\bTERT\s+(?:promoter\s+)?C250T\b", re.I),
    re.compile(r"\bc\.\s*-?146C>T\b", re.I),
    re.compile(r"\bTERT\s+p\.?\s*C250T\b", re.I),
    re.compile(r"\bc\.\s*1-146C>T\b", re.I),
]

_ENE_EXTENT_PATTERNS = [
    (re.compile(r"\bextensive\s+extranodal\b", re.I), "extensive"),
    (re.compile(r"\bgross\s+extranodal\b", re.I), "extensive"),
    (re.compile(r"\bmacroscopic\s+extranodal\b", re.I), "extensive"),
    (re.compile(r"\b>?\s*2\s*mm\s+(?:of\s+)?extranodal\b", re.I), "extensive"),
    (re.compile(r"\bfocal\s+extranodal\b", re.I), "focal"),
    (re.compile(r"\bminimal\s+extranodal\b", re.I), "focal"),
    (re.compile(r"\bmicroscopic\s+extranodal\b", re.I), "focal"),
    (re.compile(r"\b<=?\s*2\s*mm\s+(?:of\s+)?extranodal\b", re.I), "focal"),
]

_ETE_GROSS_KEYWORDS = re.compile(
    r"\b(?:gross|extensive|macroscopic)\s+(?:extrathyroidal\s+)?extension\b|"
    r"\binvad(?:ing|ed|es?)\s+(?:the\s+)?(?:strap\s+muscles?|trachea|esophag\w+|RLN|"
    r"recurrent\s+laryngeal|skeletal\s+muscle)\b|"
    r"\b(?:strap\s+muscle|tracheal?|esophageal?)\s+(?:invasion|involvement)\b|"
    r"\bpT4[ab]?\b",
    re.I,
)


class GradingRuleEngine:
    """Rule-based grading for ETE, TERT, and ENE."""

    def classify_tert_variant(self, mutation_text: str | None,
                               detailed_text: str | None) -> str | None:
        combined = " ".join(filter(None, [mutation_text, detailed_text]))
        if not combined.strip():
            return None
        for pat in _TERT_HGVS_C228T:
            if pat.search(combined):
                return "C228T"
        for pat in _TERT_HGVS_C250T:
            if pat.search(combined):
                return "C250T"
        if re.search(r"\bTERT\b", combined, re.I):
            return "promoter_unspecified"
        return None

    def apply_ete_microscopic_rule(self, path_ete_raw: str | None,
                                    op_note_text: str | None = None) -> dict:
        """If path says 'x' (present) and op-note has no gross invasion → microscopic."""
        if not path_ete_raw:
            return {"ete_grade_v9": None, "ete_rule_applied": None}

        raw = path_ete_raw.strip().lower()
        if raw in ("no", "none", "absent", "not identified", "negative", ""):
            return {"ete_grade_v9": "none", "ete_rule_applied": None}
        if raw in ("x",):
            has_gross_op = False
            if op_note_text and _ETE_GROSS_KEYWORDS.search(op_note_text):
                has_gross_op = True
            if has_gross_op:
                return {"ete_grade_v9": "gross", "ete_rule_applied": "op_note_gross"}
            return {"ete_grade_v9": "microscopic", "ete_rule_applied": "x_to_microscopic"}
        if "gross" in raw or "extensive" in raw:
            return {"ete_grade_v9": "gross", "ete_rule_applied": None}
        if "microscopic" in raw or "minimal" in raw or "focal" in raw:
            return {"ete_grade_v9": "microscopic", "ete_rule_applied": None}
        if raw in ("yes", "present", "identified"):
            return {"ete_grade_v9": "microscopic", "ete_rule_applied": "present_to_microscopic"}
        return {"ete_grade_v9": "present_ungraded", "ete_rule_applied": None}

    def classify_ene_extent(self, raw_ene: str | None,
                             path_text: str | None = None) -> dict:
        if not raw_ene and not path_text:
            return {"ene_grade_v9": None, "ene_levels": None}

        combined = " ".join(filter(None, [raw_ene, path_text]))
        v = (raw_ene or "").strip().lower()

        if v in ("absent", "no", "none", "negative", "not identified"):
            return {"ene_grade_v9": "absent", "ene_levels": None}

        for pat, grade in _ENE_EXTENT_PATTERNS:
            if pat.search(combined):
                levels = self._extract_ene_levels(combined)
                return {"ene_grade_v9": grade, "ene_levels": levels}

        levels = self._extract_ene_levels(combined)
        if v in ("x", "present", "identified", "yes") or v.startswith("present"):
            return {"ene_grade_v9": "present_ungraded", "ene_levels": levels}
        if v in ("indeterminate",):
            return {"ene_grade_v9": "indeterminate", "ene_levels": levels}

        return {"ene_grade_v9": "present_ungraded", "ene_levels": levels}

    def _extract_ene_levels(self, text: str) -> str | None:
        level_re = re.compile(
            r"\blevel\s+([IViv1-6]+[abAB]?(?:\s*[-–]\s*[IViv1-6]+[abAB]?)?)\b|"
            r"\b(central|lateral|supraclavicular|paratracheal|pretracheal|"
            r"delphian|prelaryngeal|perithyroidal|IJ)\b",
            re.I,
        )
        found = [m.group(0).strip() for m in level_re.finditer(text)]
        return "; ".join(sorted(set(found))) if found else None


# ---------------------------------------------------------------------------
# SQL builders — Phase 9 tables
# ---------------------------------------------------------------------------

def build_postop_labs_expanded_sql() -> str:
    """extracted_postop_labs_expanded_v1 — expanded PTH/calcium from events + NLP."""
    return """
CREATE OR REPLACE TABLE extracted_postop_labs_expanded_v1 AS
WITH
-- Source 1: existing extracted_postop_labs_v1 (NLP from notes)
existing_labs AS (
    SELECT
        research_id,
        TRY_CAST(lab_date AS DATE) AS lab_date,
        lab_type,
        value,
        unit,
        source_note_type,
        days_postop,
        'nlp_v1' AS extraction_method,
        0.85 AS source_reliability
    FROM extracted_postop_labs_v1
),

-- Source 2: extracted_clinical_events_v4 — PTH events (event_value is DOUBLE)
event_pth AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(event_date AS DATE) AS lab_date,
        'pth' AS lab_type,
        event_value AS value,
        'pg/mL' AS unit,
        COALESCE(source_column, 'clinical_events') AS source_note_type,
        NULL AS days_postop,
        'clinical_events_v4' AS extraction_method,
        0.92 AS source_reliability
    FROM extracted_clinical_events_v4
    WHERE LOWER(event_type) = 'lab'
      AND LOWER(event_subtype) = 'pth'
      AND event_value IS NOT NULL
      AND event_value BETWEEN 0.5 AND 500
),

-- Source 3: extracted_clinical_events_v4 — calcium events (event_value is DOUBLE)
event_calcium AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(event_date AS DATE) AS lab_date,
        'total_calcium' AS lab_type,
        event_value AS value,
        'mg/dL' AS unit,
        COALESCE(source_column, 'clinical_events') AS source_note_type,
        NULL AS days_postop,
        'clinical_events_v4' AS extraction_method,
        0.92 AS source_reliability
    FROM extracted_clinical_events_v4
    WHERE LOWER(event_type) = 'lab'
      AND LOWER(event_subtype) = 'calcium'
      AND event_value IS NOT NULL
      AND event_value BETWEEN 4.0 AND 15.0
),

-- Source 4: enhanced NLP from ALL note types (expanded patterns)
enhanced_nlp AS (
    SELECT
        CAST(c.research_id AS INTEGER) AS research_id,
        TRY_CAST(c.note_date AS DATE) AS lab_date,
        CASE
            WHEN regexp_matches(c.note_text, '(?i)\\b(?:PTH|parathyroid\\s+hormone|iPTH)\\s*(?:level\\s*)?(?:was|of|:|=|\\s)\\s*\\d')
                THEN 'pth'
            WHEN regexp_matches(c.note_text, '(?i)\\b(?:ionized\\s+calcium|iCa)\\s*(?:was|of|:|=|\\s)\\s*\\d')
                THEN 'ionized_calcium'
            WHEN regexp_matches(c.note_text, '(?i)\\b(?:calcium|Ca)\\s*(?:level\\s*)?(?:was|of|:|=|\\s)\\s*\\d')
                THEN 'total_calcium'
        END AS lab_type,
        CASE
            WHEN regexp_matches(c.note_text, '(?i)\\b(?:PTH|parathyroid\\s+hormone|iPTH)\\s*(?:level\\s*)?(?:was|of|:|=|\\s)\\s*(\\d+\\.?\\d*)')
                THEN TRY_CAST(regexp_extract(c.note_text, '(?:PTH|parathyroid hormone|iPTH)\\s*(?:level\\s*)?(?:was|of|:|=|\\s)\\s*(\\d+\\.?\\d*)', 1) AS DOUBLE)
            WHEN regexp_matches(c.note_text, '(?i)\\b(?:ionized\\s+calcium|iCa)\\s*(?:was|of|:|=|\\s)\\s*(\\d+\\.?\\d*)')
                THEN TRY_CAST(regexp_extract(c.note_text, '(?:ionized calcium|iCa)\\s*(?:was|of|:|=|\\s)\\s*(\\d+\\.?\\d*)', 1) AS DOUBLE)
            WHEN regexp_matches(c.note_text, '(?i)\\b(?:calcium|Ca)\\s*(?:level\\s*)?(?:was|of|:|=|\\s)\\s*(\\d+\\.?\\d*)')
                THEN TRY_CAST(regexp_extract(c.note_text, '(?:calcium|Ca)\\s*(?:level\\s*)?(?:was|of|:|=|\\s)\\s*(\\d+\\.?\\d*)', 1) AS DOUBLE)
        END AS value,
        CASE
            WHEN regexp_matches(c.note_text, '(?i)\\bionized') THEN 'mmol/L'
            WHEN regexp_matches(c.note_text, '(?i)\\bPTH') THEN 'pg/mL'
            ELSE 'mg/dL'
        END AS unit,
        c.note_type AS source_note_type,
        NULL AS days_postop,
        'nlp_v9_expanded' AS extraction_method,
        CASE c.note_type
            WHEN 'endocrine_note' THEN 0.90
            WHEN 'dc_sum' THEN 0.70
            WHEN 'op_note' THEN 0.80
            WHEN 'ed_note' THEN 0.65
            ELSE 0.55
        END AS source_reliability
    FROM clinical_notes_long c
    WHERE c.note_type NOT IN ('h_p')
      AND (regexp_matches(c.note_text, '(?i)\\b(?:PTH|parathyroid\\s+hormone|iPTH)\\s*(?:level\\s*)?(?:was|of|:|=|\\s)\\s*\\d')
           OR regexp_matches(c.note_text, '(?i)\\b(?:calcium|Ca)\\s*(?:level\\s*)?(?:was|of|:|=|\\s)\\s*\\d'))
),

-- Union all sources and deduplicate
all_labs AS (
    SELECT * FROM existing_labs
    UNION ALL
    SELECT * FROM event_pth
    UNION ALL
    SELECT * FROM event_calcium
    UNION ALL
    SELECT research_id, lab_date, lab_type, value, unit, source_note_type,
           days_postop, extraction_method, source_reliability
    FROM enhanced_nlp
    WHERE value IS NOT NULL
      AND lab_type IS NOT NULL
),

-- Add days_postop from surgery date where missing
with_surgery AS (
    SELECT
        a.*,
        s.first_surg_date,
        COALESCE(a.days_postop,
            CASE WHEN a.lab_date IS NOT NULL AND s.first_surg_date IS NOT NULL
                 THEN DATEDIFF('day', TRY_CAST(s.first_surg_date AS DATE), a.lab_date)
            END
        ) AS days_postop_resolved
    FROM all_labs a
    LEFT JOIN (
        SELECT research_id, MIN(surg_date) AS first_surg_date
        FROM path_synoptics GROUP BY research_id
    ) s ON a.research_id = s.research_id
),

-- Deduplicate: same patient + lab_type + date → keep highest reliability
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, lab_type, lab_date
            ORDER BY source_reliability DESC, extraction_method
        ) AS rn
    FROM with_surgery
    WHERE value IS NOT NULL
)

SELECT
    research_id,
    lab_date,
    lab_type,
    value,
    unit,
    source_note_type,
    days_postop_resolved AS days_postop,
    extraction_method,
    source_reliability,
    first_surg_date,
    CASE
        WHEN lab_type = 'pth' AND value BETWEEN 0.5 AND 500 THEN TRUE
        WHEN lab_type = 'total_calcium' AND value BETWEEN 4.0 AND 15.0 THEN TRUE
        WHEN lab_type = 'ionized_calcium' AND value BETWEEN 0.5 AND 2.0 THEN TRUE
        ELSE FALSE
    END AS value_in_range,
    CURRENT_TIMESTAMP AS refined_at
FROM deduped
WHERE rn = 1
ORDER BY research_id, lab_type, lab_date;
"""


def build_vw_postop_lab_expanded_sql() -> str:
    """vw_postop_lab_expanded — per-patient lab summary with nadirs."""
    return """
CREATE OR REPLACE TABLE vw_postop_lab_expanded AS
WITH per_patient_lab AS (
    SELECT
        research_id,
        lab_type,
        COUNT(*) AS n_values,
        MIN(value) AS nadir,
        MAX(value) AS peak,
        AVG(value) AS mean_value,
        MIN(lab_date) AS first_lab_date,
        MAX(lab_date) AS last_lab_date,
        MIN(days_postop) FILTER (WHERE days_postop >= 0 AND days_postop <= 30) AS earliest_postop_day,
        MIN(value) FILTER (WHERE days_postop BETWEEN 0 AND 30) AS nadir_within_30d,
        COUNT(*) FILTER (WHERE days_postop BETWEEN 0 AND 30) AS n_within_30d,
        MAX(source_reliability) AS best_source_reliability,
        STRING_AGG(DISTINCT extraction_method, ', ') AS extraction_methods
    FROM extracted_postop_labs_expanded_v1
    WHERE value_in_range
    GROUP BY research_id, lab_type
)
SELECT
    p.research_id,
    -- PTH
    MAX(CASE WHEN lab_type = 'pth' THEN n_values END) AS pth_n_values,
    MAX(CASE WHEN lab_type = 'pth' THEN nadir END) AS pth_nadir,
    MAX(CASE WHEN lab_type = 'pth' THEN nadir_within_30d END) AS pth_nadir_30d,
    MAX(CASE WHEN lab_type = 'pth' THEN mean_value END) AS pth_mean,
    MAX(CASE WHEN lab_type = 'pth' THEN n_within_30d END) AS pth_n_within_30d,
    CASE WHEN MAX(CASE WHEN lab_type = 'pth' THEN nadir_within_30d END) < 15 THEN TRUE
         ELSE FALSE END AS hypoparathyroidism_flag,
    -- Total calcium
    MAX(CASE WHEN lab_type = 'total_calcium' THEN n_values END) AS calcium_n_values,
    MAX(CASE WHEN lab_type = 'total_calcium' THEN nadir END) AS calcium_nadir,
    MAX(CASE WHEN lab_type = 'total_calcium' THEN nadir_within_30d END) AS calcium_nadir_30d,
    MAX(CASE WHEN lab_type = 'total_calcium' THEN mean_value END) AS calcium_mean,
    MAX(CASE WHEN lab_type = 'total_calcium' THEN n_within_30d END) AS calcium_n_within_30d,
    CASE WHEN MAX(CASE WHEN lab_type = 'total_calcium' THEN nadir_within_30d END) < 8.0 THEN TRUE
         ELSE FALSE END AS hypocalcemia_flag,
    -- Ionized calcium
    MAX(CASE WHEN lab_type = 'ionized_calcium' THEN n_values END) AS ica_n_values,
    MAX(CASE WHEN lab_type = 'ionized_calcium' THEN nadir END) AS ica_nadir,
    -- Aggregate
    SUM(n_values) AS total_lab_values,
    COUNT(DISTINCT lab_type) AS n_lab_types,
    MAX(best_source_reliability) AS best_reliability,
    STRING_AGG(DISTINCT extraction_methods, ', ') AS all_extraction_methods
FROM (SELECT DISTINCT research_id FROM path_synoptics) spine
LEFT JOIN per_patient_lab p ON spine.research_id = p.research_id
GROUP BY p.research_id
HAVING p.research_id IS NOT NULL
ORDER BY p.research_id;
"""


def build_rai_dose_refined_sql() -> str:
    """extracted_rai_dose_refined_v1 — RAI dose from all note types + existing episodes."""
    return """
CREATE OR REPLACE TABLE extracted_rai_dose_refined_v1 AS
WITH
-- Source 1: existing rai_treatment_episode_v2 with dose
existing_dose AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        resolved_rai_date AS rai_date,
        dose_mci,
        rai_intent,
        scan_findings_raw,
        iodine_avidity_flag,
        stimulated_tg,
        stimulated_tsh,
        'rai_treatment_episode_v2' AS source_table,
        1.0 AS source_reliability
    FROM rai_treatment_episode_v2
    WHERE dose_mci IS NOT NULL
),

-- Source 2: NLP dose from clinical notes (all types with RAI keywords)
nlp_dose AS (
    SELECT
        CAST(c.research_id AS INTEGER) AS research_id,
        TRY_CAST(c.note_date AS DATE) AS rai_date,
        TRY_CAST(regexp_extract(c.note_text, '(\\d{2,3}(?:\\.\\d+)?)\\s*(?:mCi|millicurie)', 1) AS DOUBLE) AS dose_mci,
        CASE
            WHEN c.note_text ILIKE '%ablat%' THEN 'ablation'
            WHEN c.note_text ILIKE '%adjuvant%' THEN 'adjuvant'
            WHEN c.note_text ILIKE '%therapeutic%' OR c.note_text ILIKE '%treatment dose%' THEN 'therapeutic'
            WHEN c.note_text ILIKE '%remnant%' THEN 'remnant_ablation'
            ELSE 'unknown'
        END AS rai_intent,
        NULL AS scan_findings_raw,
        NULL AS iodine_avidity_flag,
        NULL AS stimulated_tg,
        NULL AS stimulated_tsh,
        c.note_type AS source_table,
        CASE c.note_type
            WHEN 'endocrine_note' THEN 0.90
            WHEN 'dc_sum' THEN 0.70
            WHEN 'op_note' THEN 0.65
            WHEN 'other_history' THEN 0.60
            WHEN 'history_summary' THEN 0.55
            ELSE 0.40
        END AS source_reliability
    FROM clinical_notes_long c
    WHERE (c.note_text ILIKE '%mCi%' OR c.note_text ILIKE '%millicurie%')
      AND regexp_matches(c.note_text, '\\d{2,3}(?:\\.\\d+)?\\s*(?:mCi|millicurie)')
      AND c.note_type NOT IN ('h_p')
),

-- Union and filter
all_doses AS (
    SELECT * FROM existing_dose
    UNION ALL
    SELECT * FROM nlp_dose
    WHERE dose_mci IS NOT NULL AND dose_mci BETWEEN 10 AND 1000
),

-- Per-patient: link to RAI episodes for dose backfill
rai_patients AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        resolved_rai_date,
        rai_intent AS episode_intent,
        dose_mci AS episode_dose,
        scan_findings_raw AS episode_scan,
        iodine_avidity_flag AS episode_avid,
        stimulated_tg AS episode_stim_tg,
        stimulated_tsh AS episode_stim_tsh
    FROM rai_treatment_episode_v2
),

-- Join NLP doses to nearest RAI episode
matched AS (
    SELECT
        ad.research_id,
        ad.rai_date,
        ad.dose_mci,
        COALESCE(ad.rai_intent,
            rp.episode_intent) AS rai_intent_resolved,
        COALESCE(ad.scan_findings_raw,
            rp.episode_scan) AS scan_findings_resolved,
        ad.iodine_avidity_flag,
        ad.stimulated_tg,
        ad.stimulated_tsh,
        ad.source_table,
        ad.source_reliability,
        rp.resolved_rai_date AS matched_episode_date,
        rp.episode_dose,
        CASE
            WHEN ad.source_table = 'rai_treatment_episode_v2' THEN 'structured'
            WHEN rp.research_id IS NOT NULL THEN 'nlp_linked_to_episode'
            ELSE 'nlp_standalone'
        END AS linkage_status,
        ROW_NUMBER() OVER (
            PARTITION BY ad.research_id, ad.rai_date, ad.dose_mci
            ORDER BY ad.source_reliability DESC
        ) AS rn
    FROM all_doses ad
    LEFT JOIN rai_patients rp ON ad.research_id = rp.research_id
        AND ABS(DATEDIFF('day', TRY_CAST(ad.rai_date AS DATE),
                TRY_CAST(rp.resolved_rai_date AS DATE))) <= 90
)

SELECT
    research_id,
    rai_date,
    dose_mci,
    rai_intent_resolved AS rai_intent,
    scan_findings_resolved AS scan_findings,
    iodine_avidity_flag,
    stimulated_tg,
    stimulated_tsh,
    source_table,
    source_reliability,
    linkage_status,
    matched_episode_date,
    CURRENT_TIMESTAMP AS refined_at
FROM matched
WHERE rn = 1
ORDER BY research_id, rai_date;
"""


def build_vw_rai_dose_by_source_sql() -> str:
    """vw_rai_dose_by_source — RAI dose coverage by source."""
    return """
CREATE OR REPLACE TABLE vw_rai_dose_by_source AS
SELECT
    source_table,
    linkage_status,
    COUNT(*) AS n_doses,
    COUNT(DISTINCT research_id) AS n_patients,
    AVG(dose_mci) AS avg_dose_mci,
    MIN(dose_mci) AS min_dose,
    MAX(dose_mci) AS max_dose,
    AVG(source_reliability) AS avg_reliability
FROM extracted_rai_dose_refined_v1
GROUP BY source_table, linkage_status
ORDER BY n_doses DESC;
"""


def build_ete_ene_tert_refined_sql() -> str:
    """extracted_ete_ene_tert_refined_v1 — ETE microscopic rule + TERT sub-typing + ENE extent."""
    return """
CREATE OR REPLACE TABLE extracted_ete_ene_tert_refined_v1 AS
WITH
-- ETE: apply microscopic rule to all 'x' placeholders
ete_refined AS (
    SELECT
        ps.research_id,
        ps.tumor_1_extrathyroidal_extension AS ete_raw,
        -- Existing grade from Phase 5 sub-grading
        COALESCE(esg.refined_ete_grade, staging.ete_grade) AS ete_prior_grade,
        -- Phase 9 rule: 'x' → microscopic unless op-note says gross
        CASE
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,''))) = 'x' THEN
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM clinical_notes_long cn
                        WHERE CAST(cn.research_id AS INTEGER) = ps.research_id
                          AND cn.note_type = 'op_note'
                          AND (cn.note_text ILIKE '%gross%extrathyroidal%'
                               OR cn.note_text ILIKE '%invading%strap%'
                               OR cn.note_text ILIKE '%invading%trachea%'
                               OR cn.note_text ILIKE '%tracheal invasion%'
                               OR cn.note_text ILIKE '%esophageal invasion%'
                               OR cn.note_text ILIKE '%pT4%')
                    ) THEN 'gross'
                    ELSE 'microscopic'
                END
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,'')))
                 IN ('no','none','absent','not identified','negative','') THEN 'none'
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,'')))
                 LIKE '%gross%' OR LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,'')))
                 LIKE '%extensive%' THEN 'gross'
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,'')))
                 LIKE '%microscopic%' OR LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,'')))
                 LIKE '%minimal%' OR LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,'')))
                 LIKE '%focal%' THEN 'microscopic'
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,'')))
                 IN ('yes','present','identified') THEN 'microscopic'
            ELSE 'present_ungraded'
        END AS ete_grade_v9,
        CASE
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,''))) = 'x' THEN 'x_to_microscopic'
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extrathyroidal_extension,'')))
                 IN ('yes','present','identified') THEN 'present_to_microscopic'
            ELSE NULL
        END AS ete_rule_applied
    FROM path_synoptics ps
    LEFT JOIN extracted_ete_subgraded_v1 esg ON ps.research_id = esg.research_id
    LEFT JOIN patient_refined_staging_flags_v3 staging ON ps.research_id = staging.research_id
    WHERE ps.tumor_1_extrathyroidal_extension IS NOT NULL
      AND TRIM(ps.tumor_1_extrathyroidal_extension) <> ''
),

-- Deduplicate to one row per patient (most aggressive ETE)
ete_per_patient AS (
    SELECT research_id,
        MAX(CASE ete_grade_v9
            WHEN 'gross' THEN 4
            WHEN 'microscopic' THEN 3
            WHEN 'present_ungraded' THEN 2
            WHEN 'none' THEN 1
            ELSE 0 END) AS ete_rank,
        MAX(ete_grade_v9) FILTER (WHERE ete_grade_v9 = 'gross') AS has_gross,
        MAX(ete_grade_v9) FILTER (WHERE ete_grade_v9 = 'microscopic') AS has_micro,
        MAX(ete_rule_applied) AS rule_applied
    FROM ete_refined
    GROUP BY research_id
),

ete_final AS (
    SELECT research_id,
        CASE ete_rank
            WHEN 4 THEN 'gross'
            WHEN 3 THEN 'microscopic'
            WHEN 2 THEN 'present_ungraded'
            WHEN 1 THEN 'none'
            ELSE 'none'
        END AS ete_grade_v9,
        rule_applied AS ete_rule_applied
    FROM ete_per_patient
),

-- TERT: enhanced sub-typing with HGVS patterns
tert_refined AS (
    SELECT
        CAST(m.research_id AS INTEGER) AS research_id,
        CASE
            WHEN regexp_matches(COALESCE(m.mutation,'') || ' ' || COALESCE(m.detailed_findings_raw,''),
                '(?i)C228T|c\\.\\s*-?124C>T|c\\.1-124C>T') THEN 'C228T'
            WHEN regexp_matches(COALESCE(m.mutation,'') || ' ' || COALESCE(m.detailed_findings_raw,''),
                '(?i)C250T|c\\.\\s*-?146C>T|c\\.1-146C>T') THEN 'C250T'
            WHEN LOWER(CAST(m.tert_flag AS VARCHAR)) = 'true' THEN 'promoter_unspecified'
            ELSE NULL
        END AS tert_variant_v9,
        LOWER(CAST(m.tert_flag AS VARCHAR)) = 'true' AS tert_positive_v9,
        m.platform AS tert_platform,
        m.mutation AS tert_mutation_raw,
        SUBSTRING(m.detailed_findings_raw, 1, 200) AS tert_detailed_snippet
    FROM molecular_test_episode_v2 m
    WHERE LOWER(CAST(m.tert_flag AS VARCHAR)) = 'true'
),

tert_per_patient AS (
    SELECT
        research_id,
        BOOL_OR(tert_positive_v9) AS tert_positive_v9,
        COALESCE(
            MAX(CASE WHEN tert_variant_v9 = 'C228T' THEN 'C228T' END),
            MAX(CASE WHEN tert_variant_v9 = 'C250T' THEN 'C250T' END),
            MAX(CASE WHEN tert_variant_v9 = 'promoter_unspecified' THEN 'promoter_unspecified' END)
        ) AS tert_variant_v9,
        STRING_AGG(DISTINCT tert_platform, ', ') AS tert_platforms,
        STRING_AGG(DISTINCT tert_mutation_raw, ' | ') FILTER (WHERE tert_mutation_raw IS NOT NULL) AS tert_mutations_raw,
        COUNT(*) AS tert_test_count
    FROM tert_refined
    GROUP BY research_id
),

-- ENE: extent grading from path_synoptics
ene_refined AS (
    SELECT
        ps.research_id,
        ps.tumor_1_extranodal_extension AS ene_raw,
        CASE
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 IN ('absent','no','none','negative','not identified','') THEN 'absent'
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 LIKE '%extensive%' OR LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 LIKE '%gross%' OR LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 LIKE '%macroscopic%' THEN 'extensive'
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 LIKE '%focal%' OR LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 LIKE '%minimal%' OR LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 LIKE '%microscopic%' THEN 'focal'
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 IN ('x','present','identified','yes')
                 OR LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 LIKE 'present%' THEN 'present_ungraded'
            WHEN LOWER(TRIM(COALESCE(ps.tumor_1_extranodal_extension,'')))
                 = 'indeterminate' THEN 'indeterminate'
            ELSE 'present_ungraded'
        END AS ene_grade_v9,
        -- Extract LN levels from multi-line ENE text
        regexp_extract(ps.tumor_1_extranodal_extension,
            '(?i)(?:level\\s+[IViv1-6]+[abAB]?(?:\\s*[-]\\s*[IViv1-6]+)?|'
            'central|lateral|supraclavicular|paratracheal|pretracheal|delphian|prelaryngeal|IJ)',
            0) AS ene_levels_raw
    FROM path_synoptics ps
    WHERE ps.tumor_1_extranodal_extension IS NOT NULL
      AND TRIM(ps.tumor_1_extranodal_extension) <> ''
),

ene_per_patient AS (
    SELECT research_id,
        MAX(CASE ene_grade_v9
            WHEN 'extensive' THEN 5
            WHEN 'present_ungraded' THEN 3
            WHEN 'focal' THEN 2
            WHEN 'indeterminate' THEN 1
            WHEN 'absent' THEN 0
            ELSE 3 END) AS ene_rank,
        STRING_AGG(DISTINCT ene_levels_raw, '; ') FILTER (WHERE ene_levels_raw IS NOT NULL AND ene_levels_raw <> '') AS ene_levels_combined,
        COUNT(*) AS ene_record_count
    FROM ene_refined
    GROUP BY research_id
),

ene_final AS (
    SELECT research_id,
        CASE ene_rank
            WHEN 5 THEN 'extensive'
            WHEN 3 THEN 'present_ungraded'
            WHEN 2 THEN 'focal'
            WHEN 1 THEN 'indeterminate'
            WHEN 0 THEN 'absent'
            ELSE 'present_ungraded'
        END AS ene_grade_v9,
        ene_levels_combined AS ene_levels_v9,
        ene_record_count
    FROM ene_per_patient
)

-- Combine all three into single output table
SELECT
    COALESCE(e.research_id, t.research_id, en.research_id) AS research_id,
    -- ETE columns
    e.ete_grade_v9,
    e.ete_rule_applied,
    -- TERT columns
    t.tert_positive_v9,
    t.tert_variant_v9,
    t.tert_platforms,
    t.tert_mutations_raw,
    t.tert_test_count,
    -- ENE columns
    en.ene_grade_v9,
    en.ene_levels_v9,
    en.ene_record_count,
    CURRENT_TIMESTAMP AS refined_at
FROM ete_final e
FULL OUTER JOIN tert_per_patient t ON e.research_id = t.research_id
FULL OUTER JOIN ene_final en ON COALESCE(e.research_id, t.research_id) = en.research_id
ORDER BY COALESCE(e.research_id, t.research_id, en.research_id);
"""


def build_vw_ete_microscopic_rule_sql() -> str:
    """vw_ete_microscopic_rule — ETE grading distribution before/after rule."""
    return """
CREATE OR REPLACE TABLE vw_ete_microscopic_rule AS
WITH before_rule AS (
    SELECT ete_grade, COUNT(*) AS n_before
    FROM patient_refined_staging_flags_v3
    WHERE ete_path_confirmed IS NOT NULL OR ete_grade IS NOT NULL
    GROUP BY ete_grade
),
after_rule AS (
    SELECT ete_grade_v9, COUNT(*) AS n_after
    FROM extracted_ete_ene_tert_refined_v1
    WHERE ete_grade_v9 IS NOT NULL
    GROUP BY ete_grade_v9
)
SELECT
    COALESCE(b.ete_grade, a.ete_grade_v9) AS grade,
    COALESCE(b.n_before, 0) AS n_before_phase9,
    COALESCE(a.n_after, 0) AS n_after_phase9,
    COALESCE(a.n_after, 0) - COALESCE(b.n_before, 0) AS delta
FROM before_rule b
FULL OUTER JOIN after_rule a ON b.ete_grade = a.ete_grade_v9
ORDER BY grade;
"""


def build_master_clinical_v8_sql() -> str:
    """patient_refined_master_clinical_v8 — extends v7 with Phase 9 columns."""
    return """
CREATE OR REPLACE TABLE patient_refined_master_clinical_v8 AS
SELECT
    v7.*,

    -- Phase 9: Expanded labs (8 columns)
    lab.pth_n_values,
    lab.pth_nadir,
    lab.pth_nadir_30d,
    lab.hypoparathyroidism_flag AS hypoparathyroidism_lab_flag,
    lab.calcium_n_values,
    lab.calcium_nadir,
    lab.calcium_nadir_30d,
    lab.hypocalcemia_flag AS hypocalcemia_lab_flag,
    lab.ica_n_values,
    lab.ica_nadir,
    lab.total_lab_values AS total_postop_lab_values,
    lab.all_extraction_methods AS lab_extraction_methods,

    -- Phase 9: RAI dose refined (6 columns)
    rd.dose_mci AS rai_dose_v9,
    rd.rai_intent AS rai_intent_v9,
    rd.source_table AS rai_dose_source,
    rd.linkage_status AS rai_dose_linkage,
    rd.scan_findings AS rai_scan_findings_v9,
    rd.iodine_avidity_flag AS rai_avid_v9,

    -- Phase 9: ETE grading (2 columns)
    etg.ete_grade_v9,
    etg.ete_rule_applied,

    -- Phase 9: TERT sub-typing (4 columns)
    etg.tert_positive_v9,
    etg.tert_variant_v9,
    etg.tert_platforms AS tert_platforms_v9,
    etg.tert_test_count AS tert_test_count_v9,

    -- Phase 9: ENE grading (3 columns)
    etg.ene_grade_v9,
    etg.ene_levels_v9,
    etg.ene_record_count AS ene_record_count_v9

FROM patient_refined_master_clinical_v7 v7
LEFT JOIN vw_postop_lab_expanded lab ON v7.research_id = lab.research_id
LEFT JOIN (
    SELECT research_id,
        MAX(dose_mci) AS dose_mci,
        MAX(rai_intent) AS rai_intent,
        MAX(source_table) AS source_table,
        MAX(linkage_status) AS linkage_status,
        STRING_AGG(DISTINCT scan_findings, '; ') FILTER (WHERE scan_findings IS NOT NULL) AS scan_findings,
        BOOL_OR(iodine_avidity_flag IS TRUE) AS iodine_avidity_flag
    FROM extracted_rai_dose_refined_v1
    GROUP BY research_id
) rd ON v7.research_id = rd.research_id
LEFT JOIN extracted_ete_ene_tert_refined_v1 etg ON v7.research_id = etg.research_id
ORDER BY v7.research_id;
"""


# ---------------------------------------------------------------------------
# Step registry
# ---------------------------------------------------------------------------
_PHASE9_STEPS = [
    ("postop_labs_expanded", build_postop_labs_expanded_sql),
    ("vw_postop_lab_expanded", build_vw_postop_lab_expanded_sql),
    ("rai_dose_refined", build_rai_dose_refined_sql),
    ("vw_rai_dose_by_source", build_vw_rai_dose_by_source_sql),
    ("ete_ene_tert_refined", build_ete_ene_tert_refined_sql),
    ("vw_ete_microscopic_rule", build_vw_ete_microscopic_rule_sql),
    ("master_v8", build_master_clinical_v8_sql),
]


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------
def _lab_expanded_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total_values,
            COUNT(DISTINCT research_id) AS total_patients,
            COUNT(CASE WHEN lab_type = 'pth' THEN 1 END) AS pth_values,
            COUNT(DISTINCT CASE WHEN lab_type = 'pth' THEN research_id END) AS pth_patients,
            COUNT(CASE WHEN lab_type = 'total_calcium' THEN 1 END) AS ca_values,
            COUNT(DISTINCT CASE WHEN lab_type = 'total_calcium' THEN research_id END) AS ca_patients,
            COUNT(CASE WHEN lab_type = 'ionized_calcium' THEN 1 END) AS ica_values,
            COUNT(DISTINCT CASE WHEN lab_type = 'ionized_calcium' THEN research_id END) AS ica_patients,
            COUNT(DISTINCT extraction_method) AS n_methods,
            STRING_AGG(DISTINCT extraction_method, ', ') AS methods
        FROM extracted_postop_labs_expanded_v1
    """).fetchone()
    return {
        "total_values": row[0], "total_patients": row[1],
        "pth_values": row[2], "pth_patients": row[3],
        "calcium_values": row[4], "calcium_patients": row[5],
        "ionized_ca_values": row[6], "ionized_ca_patients": row[7],
        "extraction_methods": row[9],
    }


def _rai_dose_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total_doses,
            COUNT(DISTINCT research_id) AS patients_with_dose,
            AVG(dose_mci) AS avg_dose,
            MIN(dose_mci) AS min_dose,
            MAX(dose_mci) AS max_dose,
            COUNT(CASE WHEN linkage_status = 'structured' THEN 1 END) AS structured,
            COUNT(CASE WHEN linkage_status = 'nlp_linked_to_episode' THEN 1 END) AS nlp_linked,
            COUNT(CASE WHEN linkage_status = 'nlp_standalone' THEN 1 END) AS nlp_standalone
        FROM extracted_rai_dose_refined_v1
    """).fetchone()
    return {
        "total_doses": row[0], "patients_with_dose": row[1],
        "avg_dose_mci": round(row[2], 1) if row[2] else 0,
        "min_dose": row[3], "max_dose": row[4],
        "structured": row[5], "nlp_linked": row[6], "nlp_standalone": row[7],
    }


def _ete_ene_tert_stats(con) -> dict:
    ete = con.execute("""
        SELECT ete_grade_v9, COUNT(*) AS n
        FROM extracted_ete_ene_tert_refined_v1
        WHERE ete_grade_v9 IS NOT NULL
        GROUP BY ete_grade_v9 ORDER BY n DESC
    """).fetchall()
    tert = con.execute("""
        SELECT tert_variant_v9, COUNT(*) AS n
        FROM extracted_ete_ene_tert_refined_v1
        WHERE tert_positive_v9
        GROUP BY tert_variant_v9 ORDER BY n DESC
    """).fetchall()
    ene = con.execute("""
        SELECT ene_grade_v9, COUNT(*) AS n
        FROM extracted_ete_ene_tert_refined_v1
        WHERE ene_grade_v9 IS NOT NULL
        GROUP BY ene_grade_v9 ORDER BY n DESC
    """).fetchall()
    rule = con.execute("""
        SELECT ete_rule_applied, COUNT(*) AS n
        FROM extracted_ete_ene_tert_refined_v1
        WHERE ete_rule_applied IS NOT NULL
        GROUP BY ete_rule_applied ORDER BY n DESC
    """).fetchall()
    return {
        "ete_distribution": {r[0]: r[1] for r in ete},
        "tert_variants": {r[0]: r[1] for r in tert},
        "ene_distribution": {r[0]: r[1] for r in ene},
        "ete_rules_applied": {r[0]: r[1] for r in rule},
    }


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
def audit_and_refine_phase9(
    con,
    variables: list[str] | None = None,
    verbose: bool = True,
) -> dict[str, dict]:
    steps = _PHASE9_STEPS
    if variables:
        steps = [(n, fn) for n, fn in _PHASE9_STEPS if n in variables or "all" in variables]

    results = {}
    stat_fns = {
        "postop_labs_expanded": _lab_expanded_stats,
        "rai_dose_refined": _rai_dose_stats,
        "ete_ene_tert_refined": _ete_ene_tert_stats,
    }

    for step_name, sql_builder in steps:
        if verbose:
            print(f"\n{'='*70}")
            print(f"  Phase 9: {step_name}")
            print(f"{'='*70}")

        sql = sql_builder()
        table_name = _extract_table_name(sql)

        try:
            con.execute(sql)
            n = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if verbose:
                print(f"  {table_name}: {n} rows")
            results[step_name] = {"table": table_name, "rows": n, "status": "ok"}

            if step_name in stat_fns:
                results[step_name].update(stat_fns[step_name](con))

        except Exception as e:
            if verbose:
                print(f"  [ERROR] {step_name}: {e}")
            results[step_name] = {"error": str(e), "status": "failed"}

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Phase 9 Targeted Refinement: Labs, RAI Dose, Deep Grading")
    parser.add_argument("--variable", default="all",
                        choices=["all"] + [s[0] for s in _PHASE9_STEPS])
    parser.add_argument("--md", action="store_true", default=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default="notes_extraction")
    args = parser.parse_args()

    use_md = not args.local

    if args.dry_run:
        print(f"[dry-run] Would run phase9 step={args.variable}")
        return

    con = _get_connection(use_md)

    variables = None if args.variable == "all" else [args.variable]
    results = audit_and_refine_phase9(con, variables=variables, verbose=True)

    out_dir = PROJECT_ROOT / args.output_dir
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    lines = [
        "# Phase 9 Targeted Refinement Report",
        f"_Generated: {timestamp}_",
        "",
        "## Priorities Addressed",
        "1. Calcium/PTH lab expansion (extracted_clinical_events_v4 + enhanced NLP)",
        "2. RAI dose NLP (all note types with source linking)",
        "3. ETE x→microscopic auto-assignment rule",
        "4. TERT C228T/C250T sub-typing with HGVS patterns",
        "5. ENE extent grading from path free-text",
        "",
    ]
    for step, rpt in results.items():
        lines.append(f"## {step}")
        for k, v in rpt.items():
            lines.append(f"- **{k}:** {v}")
        lines.append("")

    report_path = out_dir / f"master_refinement_report_phase9.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[phase9] Report saved: {report_path}")

    json_path = out_dir / f"phase9_results_{timestamp}.json"
    json_path.write_text(json.dumps(results, default=str, indent=2), encoding="utf-8")
    print(f"[phase9] JSON results: {json_path}")

    con.close()


if __name__ == "__main__":
    main()
