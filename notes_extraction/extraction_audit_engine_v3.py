"""
Extraction Audit Engine v3 — Phase 5 Top-5 Variable Refinement
================================================================
Extends v2 with specialized parsers for:
  1. GradingParser       – ETE sub-grading (gross/microscopic/minimal/none)
  2. MolecularMarkerCleaner – TERT/BRAF/RAS decontamination
  3. NumericValueParser   – PTH, calcium, RAI dose extraction from free text
  4. LabIngestionPipeline – structured lab table creation from notes + Excel
  5. CrossSourceReconciler_v2 – numeric trends + grading hierarchies

Usage:
    from notes_extraction.extraction_audit_engine_v3 import audit_and_refine_top5
    results = audit_and_refine_top5(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v3.py \
        --md --variable all
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from notes_extraction.extraction_audit_engine_v2 import (
    SourceClassifier,
    SourceWeightedClassifier,
    CrossSourceReconciler,
    SourcedAuditResult,
    SourcedMentionResult,
    PatientSourceProfile,
    VARIABLE_CONFIGS,
    SOURCE_RELIABILITY,
)
from notes_extraction.extraction_audit_engine import CONSENT_BOILERPLATE_PATTERNS

# ---------------------------------------------------------------------------
# 1. GradingParser — ETE sub-grading from operative notes
# ---------------------------------------------------------------------------
_ETE_GROSS_PATTERNS = [
    re.compile(r"\b(?:gross|extensive|macroscopic)\s+(?:extrathyroidal\s+)?extension\b", re.I),
    re.compile(r"\binvad(?:ing|ed|es?)\s+(?:the\s+)?(?:strap\s+muscles?|trachea|esophag\w+|RLN"
               r"|recurrent\s+laryngeal|skeletal\s+muscle|cricothyroid)", re.I),
    re.compile(r"\b(?:strap\s+muscle|tracheal?|esophageal?)\s+(?:invasion|involvement|infiltration)\b", re.I),
    re.compile(r"\bpT4[ab]?\b", re.I),
    re.compile(r"\b(?:excision\s+of\s+the\s+strap\s+muscles?|strap\s+muscle\s+excis\w+)\b", re.I),
    re.compile(r"\btumor\s+(?:was\s+)?(?:adherent|fixed)\s+to\s+(?:the\s+)?(?:trachea|strap)", re.I),
]

_ETE_MICROSCOPIC_PATTERNS = [
    re.compile(r"\b(?:minimal|microscopic|focal|minor)\s+(?:extrathyroidal\s+)?extension\b", re.I),
    re.compile(r"\b(?:extension|extends?)\s+(?:into|through)\s+(?:the\s+)?(?:perithyroidal\s+"
               r"(?:fat|adipose|soft\s+tissue)|thyroid\s+capsule)\b", re.I),
    re.compile(r"\bpT3b\b", re.I),
    re.compile(r"\bsingle\s+(?:microscopic\s+)?focus\s+of\s+extension\b", re.I),
    re.compile(r"\bperithyroidal\s+(?:fat|tissue)\s+(?:involved|invaded|infiltrated)\b", re.I),
]

_ETE_ABSENT_PATTERNS = [
    re.compile(r"\bno\s+(?:gross\s+)?(?:evidence\s+of\s+)?extrathyroidal\s+extension\b", re.I),
    re.compile(r"\bno\s+(?:gross\s+)?(?:evidence\s+of\s+)?(?:ETE|extra-thyroidal)\b", re.I),
    re.compile(r"\bno\s+adherence\s+to\s+adjacent\s+structures\b", re.I),
    re.compile(r"\bextrathyroidal\s+extension\s*:\s*(?:absent|no|none|not\s+identified|negative)\b", re.I),
    re.compile(r"\bwithout\s+(?:any\s+)?(?:signs?\s+of\s+)?extrathyroidal\s+extension\b", re.I),
    re.compile(r"\bconfined\s+to\s+(?:the\s+)?thyroid\b", re.I),
]

_ETE_CONSENT_FP = [
    re.compile(r"\brisk(?:s)?\s+(?:of|include|including)\b.*?extrathyroidal", re.I),
    re.compile(r"\bdiscussed\s+(?:with|the)\b.*?extrathyroidal", re.I),
    re.compile(r"\bpossib(?:le|ility)\s+of\b.*?extrathyroidal", re.I),
]


class GradingParser:
    """Parse free-text operative/path note context into ETE grade."""

    def grade_ete_context(self, context: str) -> dict:
        """
        Returns: {
            'ete_grade': 'gross'|'microscopic'|'none'|'present_ungraded'|None,
            'evidence': str,
            'confidence': float (0-1),
            'is_consent_fp': bool,
        }
        """
        if not context or len(context.strip()) < 10:
            return {"ete_grade": None, "evidence": "", "confidence": 0.0, "is_consent_fp": False}

        if any(p.search(context) for p in _ETE_CONSENT_FP):
            if any(p.search(context) for p in CONSENT_BOILERPLATE_PATTERNS):
                return {"ete_grade": None, "evidence": "consent_boilerplate",
                        "confidence": 0.0, "is_consent_fp": True}

        if any(p.search(context) for p in _ETE_ABSENT_PATTERNS):
            return {"ete_grade": "none", "evidence": "negation_pattern",
                    "confidence": 0.85, "is_consent_fp": False}

        if any(p.search(context) for p in _ETE_GROSS_PATTERNS):
            return {"ete_grade": "gross", "evidence": "gross_pattern",
                    "confidence": 0.90, "is_consent_fp": False}

        if any(p.search(context) for p in _ETE_MICROSCOPIC_PATTERNS):
            return {"ete_grade": "microscopic", "evidence": "microscopic_pattern",
                    "confidence": 0.88, "is_consent_fp": False}

        return {"ete_grade": "present_ungraded", "evidence": "no_specific_pattern",
                "confidence": 0.3, "is_consent_fp": False}


# ---------------------------------------------------------------------------
# 2. MolecularMarkerCleaner — TERT decontamination
# ---------------------------------------------------------------------------
_TERT_TESTED_PATTERNS = [
    re.compile(r"\bTERT\s+(?:promoter\s+)?(?:mutation\s+)?(?:detected|positive|present|identified|found)\b", re.I),
    re.compile(r"\bTERT\s+(?:C228T|C250T)\b", re.I),
    re.compile(r"\bTERT\s+promoter\s+(?:mutation\s+)?(?:is\s+)?positive\b", re.I),
]

_TERT_NEGATIVE_PATTERNS = [
    re.compile(r"\bTERT\s+(?:promoter\s+)?(?:mutation\s+)?(?:not\s+detected|negative|absent|wild[\s-]*type)\b", re.I),
    re.compile(r"\bno\s+TERT\s+(?:promoter\s+)?mutation\b", re.I),
]

_TERT_CONSENT_FP = [
    re.compile(r"\brisk(?:s)?\s+(?:of|for|include)\b.*?TERT", re.I),
    re.compile(r"\bTERT\s+(?:promoter\s+)?(?:mutation\s+)?(?:testing|analysis|may|could|should|recommend)\b", re.I),
    re.compile(r"\b(?:if|when)\s+TERT\b", re.I),
    re.compile(r"\bTERT\s+(?:inhibitor|therapy|targeted)\b", re.I),
]

_MOLECULAR_PLATFORM_SIGNAL = [
    re.compile(r"\b(?:ThyroSeq|Afirma|molecular\s+(?:testing|panel|analysis)|NGS)\b", re.I),
]


class MolecularMarkerCleaner:
    """Clean and validate TERT/BRAF molecular marker mentions."""

    def classify_tert_mention(self, context: str, note_type: str = "") -> dict:
        """
        Returns: {
            'tert_status': 'positive'|'negative'|'tested_unknown'|'mentioned_only'|'consent_fp',
            'evidence': str,
            'platform_detected': str|None,
            'confidence': float,
        }
        """
        if not context:
            return {"tert_status": None, "evidence": "", "platform_detected": None, "confidence": 0.0}

        if any(p.search(context) for p in _TERT_CONSENT_FP):
            if any(p.search(context) for p in CONSENT_BOILERPLATE_PATTERNS):
                return {"tert_status": "consent_fp", "evidence": "consent_boilerplate",
                        "platform_detected": None, "confidence": 0.0}

        platform = None
        for p in _MOLECULAR_PLATFORM_SIGNAL:
            m = p.search(context)
            if m:
                platform = m.group(0)
                break

        if any(p.search(context) for p in _TERT_TESTED_PATTERNS):
            return {"tert_status": "positive", "evidence": "positive_pattern",
                    "platform_detected": platform, "confidence": 0.92}

        if any(p.search(context) for p in _TERT_NEGATIVE_PATTERNS):
            return {"tert_status": "negative", "evidence": "negative_pattern",
                    "platform_detected": platform, "confidence": 0.90}

        if platform:
            return {"tert_status": "tested_unknown", "evidence": "platform_context",
                    "platform_detected": platform, "confidence": 0.5}

        return {"tert_status": "mentioned_only", "evidence": "generic_mention",
                "platform_detected": None, "confidence": 0.2}


# ---------------------------------------------------------------------------
# 3. NumericValueParser — lab values and RAI dose from free text
# ---------------------------------------------------------------------------
_PTH_PATTERNS = [
    re.compile(r"\b(?:PTH|parathyroid\s+hormone|intact\s+PTH|iPTH)\s*"
               r"(?:level\s*)?(?:was|of|:|\s)?\s*(\d+(?:\.\d+)?)\s*(?:pg/m[lL]|ng/[lL])?\b", re.I),
    re.compile(r"\b(?:PTH|parathyroid\s+hormone)\s*[:=]\s*(\d+(?:\.\d+)?)\b", re.I),
    re.compile(r"\bPTH\s+(\d+(?:\.\d+)?)\b", re.I),
]

_CALCIUM_PATTERNS = [
    re.compile(r"\b(?:calcium|Ca|Ca2\+|ionized\s+calcium|iCa)\s*"
               r"(?:level\s*)?(?:was|of|:|\s)?\s*(\d+(?:\.\d+)?)\s*(?:mg/d[lL]|mmol/[lL])?\b", re.I),
    re.compile(r"\b(?:calcium|Ca)\s*[:=]\s*(\d+(?:\.\d+)?)\b", re.I),
]

_IONIZED_CA_PATTERN = re.compile(
    r"\b(?:ionized\s+(?:calcium|Ca)|iCa)\s*(?:was|of|:|\s)?\s*(\d+(?:\.\d+)?)\s*(?:mmol/[lL])?\b", re.I
)

_RAI_DOSE_PATTERNS = [
    re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:mCi|millicuries?)\b", re.I),
    re.compile(r"\b(?:dose|administered|received|given)\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*mCi\b", re.I),
    re.compile(r"\b(?:I-?131|RAI|radioactive\s+iodine)\s+(?:dose\s+(?:of\s+)?)?(\d+(?:\.\d+)?)\s*mCi\b", re.I),
    re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:mCi|GBq)\s+(?:of\s+)?(?:I-?131|RAI|radioiodine)\b", re.I),
]


class NumericValueParser:
    """Extract numeric lab values (PTH, calcium, RAI dose) from free text."""

    def extract_pth(self, text: str) -> list[dict]:
        results = []
        for pat in _PTH_PATTERNS:
            for m in pat.finditer(text):
                val = float(m.group(1))
                if 0.5 <= val <= 500:
                    results.append({
                        "value": val,
                        "unit": "pg/mL",
                        "lab_type": "pth",
                        "span_start": m.start(),
                        "span_end": m.end(),
                        "match_text": m.group(0),
                    })
        return results

    def extract_calcium(self, text: str) -> list[dict]:
        results = []
        is_ionized = bool(_IONIZED_CA_PATTERN.search(text))
        for pat in _CALCIUM_PATTERNS:
            for m in pat.finditer(text):
                val = float(m.group(1))
                if is_ionized and 0.5 <= val <= 2.0:
                    results.append({
                        "value": val, "unit": "mmol/L", "lab_type": "ionized_calcium",
                        "span_start": m.start(), "span_end": m.end(),
                        "match_text": m.group(0),
                    })
                elif not is_ionized and 4.0 <= val <= 15.0:
                    results.append({
                        "value": val, "unit": "mg/dL", "lab_type": "total_calcium",
                        "span_start": m.start(), "span_end": m.end(),
                        "match_text": m.group(0),
                    })
        return results

    def extract_rai_dose(self, text: str) -> list[dict]:
        results = []
        for pat in _RAI_DOSE_PATTERNS:
            for m in pat.finditer(text):
                val = float(m.group(1))
                if 10 <= val <= 1000:
                    results.append({
                        "value": val, "unit": "mCi", "lab_type": "rai_dose",
                        "span_start": m.start(), "span_end": m.end(),
                        "match_text": m.group(0),
                    })
        return results

    def extract_all_labs(self, text: str) -> list[dict]:
        return self.extract_pth(text) + self.extract_calcium(text) + self.extract_rai_dose(text)


# ---------------------------------------------------------------------------
# 4. LabIngestionPipeline — build structured lab table from notes
# ---------------------------------------------------------------------------
_DATE_PATTERNS_IN_TEXT = [
    re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b"),
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),
    re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2})\b"),
]


class LabIngestionPipeline:
    """Scan clinical notes for PTH/calcium lab values and create structured table."""

    def __init__(self):
        self.parser = NumericValueParser()

    def extract_labs_from_note(self, research_id: int, note_text: str,
                                note_type: str, note_date: str | None,
                                surgery_date: str | None) -> list[dict]:
        """Extract all lab values from a single note."""
        if not note_text or len(note_text.strip()) < 20:
            return []

        if any(p.search(note_text[:500]) for p in CONSENT_BOILERPLATE_PATTERNS):
            if note_type == "h_p":
                return []

        labs = self.parser.extract_pth(note_text) + self.parser.extract_calcium(note_text)
        results = []
        for lab in labs:
            lab_date = self._find_nearest_date(note_text, lab["span_start"])
            if not lab_date and note_date:
                lab_date = note_date

            days_postop = None
            if lab_date and surgery_date:
                try:
                    from datetime import datetime as dt
                    ld = pd.to_datetime(lab_date, errors="coerce")
                    sd = pd.to_datetime(surgery_date, errors="coerce")
                    if pd.notna(ld) and pd.notna(sd):
                        days_postop = (ld - sd).days
                except Exception:
                    pass

            results.append({
                "research_id": research_id,
                "lab_date": lab_date,
                "lab_type": lab["lab_type"],
                "value": lab["value"],
                "unit": lab["unit"],
                "source_note_type": note_type,
                "days_postop": days_postop,
                "match_text": lab["match_text"][:80],
            })
        return results

    def _find_nearest_date(self, text: str, position: int) -> str | None:
        """Find the closest date mention to a lab value in text."""
        best_date = None
        best_dist = float("inf")
        for pat in _DATE_PATTERNS_IN_TEXT:
            for m in pat.finditer(text):
                dist = abs(m.start() - position)
                if dist < best_dist:
                    best_dist = dist
                    try:
                        date_str = m.group(0)
                        pd.to_datetime(date_str, errors="raise")
                        best_date = date_str
                    except Exception:
                        pass
        return best_date if best_dist < 500 else None


# ---------------------------------------------------------------------------
# 5. CrossSourceReconciler_v2 — numeric + grading
# ---------------------------------------------------------------------------
_GRADE_HIERARCHY = {"gross": 4, "microscopic": 3, "present_ungraded": 2, "none": 1}


class CrossSourceReconciler_v2(CrossSourceReconciler):
    """Extended reconciler with numeric trend detection and grading hierarchies."""

    def reconcile_numeric(self, values: list[dict]) -> dict:
        """Reconcile multiple numeric lab values for the same patient/lab type."""
        if not values:
            return {"nadir": None, "peak": None, "trend": "unknown", "n_values": 0}

        sorted_vals = sorted(values, key=lambda x: x.get("lab_date") or "")
        nums = [v["value"] for v in sorted_vals if v.get("value") is not None]
        if not nums:
            return {"nadir": None, "peak": None, "trend": "unknown", "n_values": 0}

        return {
            "nadir": min(nums),
            "peak": max(nums),
            "trend": "decreasing" if len(nums) > 1 and nums[-1] < nums[0]
                     else "increasing" if len(nums) > 1 and nums[-1] > nums[0]
                     else "stable",
            "n_values": len(nums),
            "first_value": nums[0] if nums else None,
            "last_value": nums[-1] if nums else None,
        }

    def reconcile_grading(self, grades: list[str]) -> str:
        """Reconcile multiple ETE grades — highest wins (path > op)."""
        if not grades:
            return "present_ungraded"
        ranked = sorted(grades, key=lambda g: _GRADE_HIERARCHY.get(g, 0), reverse=True)
        return ranked[0]


# ---------------------------------------------------------------------------
# 6. Extranodal Extension Parser
# ---------------------------------------------------------------------------
_ENE_PATTERNS = [
    re.compile(r"\bextranodal\s+extension\s*:\s*(present|absent|yes|no|identified|not\s+identified)\b", re.I),
    re.compile(r"\bextranodal\s+extension\s+(?:is\s+)?(present|identified)\b", re.I),
    re.compile(r"\bno\s+extranodal\s+extension\b", re.I),
    re.compile(r"\b(?:ENE|extracapsular\s+(?:extension|spread))\s*:\s*(present|absent)\b", re.I),
    re.compile(r"\bmetastat\w+\s+(?:lymph\s+)?nodes?\s+with\s+extranodal\s+extension\b", re.I),
]

_ENE_LEVEL_PATTERN = re.compile(
    r"\b(?:level|compartment)\s+(?:VI|6|II|2|III|3|IV|4|V|5|[IViv]+[abAB]?)\b", re.I
)


class ExtranodaParser:
    """Parse extranodal extension from pathology text."""

    def parse(self, text: str) -> dict:
        if not text:
            return {"ene_status": None, "ene_level": None, "confidence": 0.0}

        for p in _ENE_PATTERNS:
            m = p.search(text)
            if m:
                val = m.group(0).lower()
                if any(neg in val for neg in ("no ", "absent", "not identified", "not present")):
                    status = "absent"
                else:
                    status = "present"

                level = None
                level_m = _ENE_LEVEL_PATTERN.search(text)
                if level_m:
                    level = level_m.group(0)

                return {"ene_status": status, "ene_level": level, "confidence": 0.85}

        return {"ene_status": None, "ene_level": None, "confidence": 0.0}


# ---------------------------------------------------------------------------
# SQL Builders
# ---------------------------------------------------------------------------
def build_ete_subgrading_sql() -> str:
    """SQL to create extracted_ete_subgraded_v1 by parsing op note context."""
    return """
CREATE OR REPLACE TABLE extracted_ete_subgraded_v1 AS
WITH

ungraded_patients AS (
    SELECT DISTINCT research_id
    FROM extracted_ete_refined_v1
    WHERE ete_grade = 'present_ungraded'
),

op_note_context AS (
    SELECT
        n.research_id,
        n.note_type,
        CASE
            WHEN POSITION('extrathyroidal' IN LOWER(n.note_text)) > 0
            THEN SUBSTRING(n.note_text,
                 GREATEST(1, POSITION('extrathyroidal' IN LOWER(n.note_text)) - 200),
                 600)
            WHEN POSITION('extension' IN LOWER(n.note_text)) > 0
                 AND POSITION('extrathyroid' IN LOWER(n.note_text)) > 0
            THEN SUBSTRING(n.note_text,
                 GREATEST(1, POSITION('extension' IN LOWER(n.note_text)) - 200),
                 600)
            WHEN POSITION('invad' IN LOWER(n.note_text)) > 0
                 AND (LOWER(n.note_text) LIKE '%strap%' OR LOWER(n.note_text) LIKE '%trachea%'
                      OR LOWER(n.note_text) LIKE '%esophag%')
            THEN SUBSTRING(n.note_text,
                 GREATEST(1, POSITION('invad' IN LOWER(n.note_text)) - 200),
                 600)
            ELSE NULL
        END AS ete_context
    FROM clinical_notes_long n
    JOIN ungraded_patients u ON n.research_id = u.research_id
    WHERE n.note_type IN ('op_note', 'endocrine_note', 'dc_sum')
),

parsed_grades AS (
    SELECT
        research_id,
        note_type,
        ete_context,
        CASE
            -- GROSS: strap muscle / trachea / esophagus invasion
            WHEN regexp_matches(ete_context,
                '(?i)\\b(?:gross|extensive|macroscopic)\\s+(?:extrathyroidal\\s+)?extension')
                THEN 'gross'
            WHEN regexp_matches(ete_context,
                '(?i)invad(?:ing|ed|es?)\\s+(?:the\\s+)?(?:strap\\s+muscles?|trachea|esophag|RLN|cricothyroid|skeletal)')
                THEN 'gross'
            WHEN regexp_matches(ete_context,
                '(?i)\\b(?:strap\\s+muscle|tracheal?|esophageal?)\\s+(?:invasion|involvement|infiltration)')
                THEN 'gross'
            WHEN regexp_matches(ete_context, '(?i)excision\\s+of\\s+the\\s+strap\\s+muscles?')
                THEN 'gross'
            WHEN regexp_matches(ete_context, '(?i)pT4[ab]?')
                THEN 'gross'

            -- MICROSCOPIC: perithyroidal fat / focal / minimal
            WHEN regexp_matches(ete_context,
                '(?i)\\b(?:minimal|microscopic|focal|minor)\\s+(?:extrathyroidal\\s+)?extension')
                THEN 'microscopic'
            WHEN regexp_matches(ete_context,
                '(?i)(?:extension|extends?)\\s+(?:into|through)\\s+(?:the\\s+)?(?:perithyroidal|thyroid\\s+capsule)')
                THEN 'microscopic'
            WHEN regexp_matches(ete_context, '(?i)pT3b')
                THEN 'microscopic'
            WHEN regexp_matches(ete_context,
                '(?i)perithyroidal\\s+(?:fat|tissue)\\s+(?:involved|invaded|infiltrated)')
                THEN 'microscopic'

            -- NONE: explicit negation in operative context
            WHEN regexp_matches(ete_context,
                '(?i)no\\s+(?:gross\\s+)?(?:evidence\\s+of\\s+)?extrathyroidal\\s+extension')
                THEN 'op_note_none'
            WHEN regexp_matches(ete_context,
                '(?i)no\\s+adherence\\s+to\\s+adjacent\\s+structures')
                THEN 'op_note_none'
            WHEN regexp_matches(ete_context,
                '(?i)without\\s+(?:any\\s+)?(?:signs?\\s+of\\s+)?extrathyroidal')
                THEN 'op_note_none'
            WHEN regexp_matches(ete_context,
                '(?i)confined\\s+to\\s+(?:the\\s+)?thyroid')
                THEN 'op_note_none'

            ELSE NULL
        END AS op_note_grade,
        -- Confidence: higher for specific patterns
        CASE
            WHEN regexp_matches(ete_context,
                '(?i)\\b(?:gross|extensive|macroscopic)\\s+(?:extrathyroidal\\s+)?extension')
                THEN 0.90
            WHEN regexp_matches(ete_context,
                '(?i)invad(?:ing|ed|es?)\\s+(?:the\\s+)?(?:strap|trachea|esophag)')
                THEN 0.88
            WHEN regexp_matches(ete_context,
                '(?i)\\b(?:minimal|microscopic|focal)\\s+(?:extrathyroidal\\s+)?extension')
                THEN 0.88
            WHEN regexp_matches(ete_context,
                '(?i)no\\s+(?:gross\\s+)?(?:evidence\\s+of\\s+)?extrathyroidal')
                THEN 0.85
            ELSE 0.0
        END AS op_note_confidence
    FROM op_note_context
    WHERE ete_context IS NOT NULL
),

-- Rank: prefer gross > microscopic > op_note_none > NULL per patient
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY
                CASE op_note_grade
                    WHEN 'gross' THEN 1
                    WHEN 'microscopic' THEN 2
                    WHEN 'op_note_none' THEN 3
                    ELSE 4
                END,
                op_note_confidence DESC
        ) AS rn
    FROM parsed_grades
    WHERE op_note_grade IS NOT NULL
),

best_per_patient AS (
    SELECT research_id, op_note_grade, op_note_confidence, note_type
    FROM ranked
    WHERE rn = 1
)

SELECT
    e.research_id,
    e.ete_grade AS original_grade,
    e.ete_source_of_truth AS original_source,
    b.op_note_grade,
    b.op_note_confidence,
    b.note_type AS grading_source_note,
    -- Final refined grade: path microscopic/gross trumps; op note overrides 'present_ungraded' only
    CASE
        WHEN e.ete_grade IN ('gross', 'microscopic') THEN e.ete_grade
        WHEN b.op_note_grade = 'gross' THEN 'gross'
        WHEN b.op_note_grade = 'microscopic' THEN 'microscopic'
        WHEN b.op_note_grade = 'op_note_none' THEN 'op_note_none_path_positive'
        ELSE 'present_ungraded'
    END AS refined_ete_grade,
    CASE
        WHEN b.op_note_grade IS NOT NULL THEN 'op_note_subgraded'
        ELSE 'no_subgrade_evidence'
    END AS subgrade_method,
    CURRENT_TIMESTAMP AS refined_at
FROM extracted_ete_refined_v1 e
LEFT JOIN best_per_patient b ON e.research_id = b.research_id
WHERE e.ete_grade = 'present_ungraded'
ORDER BY e.research_id;
"""


def build_tert_refined_sql() -> str:
    """SQL to create extracted_molecular_refined_v1 with correct TERT from molecular episodes."""
    return """
CREATE OR REPLACE TABLE extracted_molecular_refined_v1 AS
WITH

-- Source 1: Structured molecular_test_episode_v2 (gold standard)
mol_tert AS (
    SELECT
        research_id,
        LOWER(CAST(tert_flag AS VARCHAR)) = 'true' AS tert_positive,
        LOWER(CAST(braf_flag AS VARCHAR)) = 'true' AS braf_positive_mol,
        LOWER(CAST(ras_flag AS VARCHAR)) = 'true' AS ras_positive_mol,
        LOWER(CAST(ret_flag AS VARCHAR)) = 'true' AS ret_positive_mol,
        LOWER(CAST(ntrk_flag AS VARCHAR)) = 'true' AS ntrk_positive_mol,
        LOWER(CAST(tp53_flag AS VARCHAR)) = 'true' AS tp53_positive_mol,
        LOWER(CAST(high_risk_marker_flag AS VARCHAR)) = 'true' AS high_risk_marker,
        platform,
        resolved_test_date AS test_date,
        'molecular_test_episode_v2' AS source_table
    FROM molecular_test_episode_v2
    WHERE LOWER(CAST(inadequate_flag AS VARCHAR)) <> 'true'
      AND LOWER(CAST(cancelled_flag AS VARCHAR)) <> 'true'
),

-- Per-patient: ANY positive across multiple tests
patient_mol AS (
    SELECT
        research_id,
        BOOL_OR(tert_positive) AS tert_positive_any,
        BOOL_OR(braf_positive_mol) AS braf_positive_any,
        BOOL_OR(ras_positive_mol) AS ras_positive_any,
        BOOL_OR(ret_positive_mol) AS ret_positive_any,
        BOOL_OR(ntrk_positive_mol) AS ntrk_positive_any,
        BOOL_OR(tp53_positive_mol) AS tp53_positive_any,
        BOOL_OR(high_risk_marker) AS high_risk_any,
        BOOL_OR(tert_positive) OR BOOL_OR(braf_positive_mol) OR BOOL_OR(ras_positive_mol)
            AS any_molecular_tested,
        STRING_AGG(DISTINCT platform, ', ') AS platforms_used,
        MIN(test_date) AS first_test_date,
        COUNT(*) AS n_molecular_tests
    FROM mol_tert
    GROUP BY research_id
),

-- Source 2: recurrence_risk_features_mv (older structured)
rrf AS (
    SELECT
        research_id,
        LOWER(CAST(braf_positive AS VARCHAR)) = 'true' AS braf_positive_rrf,
        LOWER(CAST(ras_positive AS VARCHAR)) = 'true' AS ras_positive_rrf,
        LOWER(CAST(tert_positive AS VARCHAR)) = 'true' AS tert_positive_rrf,
        LOWER(CAST(ret_positive AS VARCHAR)) = 'true' AS ret_positive_rrf
    FROM recurrence_risk_features_mv
),

-- Full patient spine
spine AS (
    SELECT DISTINCT research_id FROM patient_mol
    UNION
    SELECT DISTINCT research_id FROM rrf
)

SELECT
    s.research_id,
    -- TERT: molecular episodes (tert_flag) are authoritative
    COALESCE(pm.tert_positive_any, rr.tert_positive_rrf, FALSE) AS tert_positive_refined,
    CASE
        WHEN pm.tert_positive_any IS NOT NULL THEN 'molecular_test_episode_v2'
        WHEN rr.tert_positive_rrf IS NOT NULL THEN 'recurrence_risk_features_mv'
        ELSE NULL
    END AS tert_source,
    CASE WHEN pm.any_molecular_tested OR rr.research_id IS NOT NULL
         THEN TRUE ELSE FALSE END AS tert_tested,

    -- BRAF: prefer molecular episodes, fall back to rrf
    COALESCE(pm.braf_positive_any, rr.braf_positive_rrf, FALSE) AS braf_positive_refined,
    CASE
        WHEN pm.braf_positive_any IS NOT NULL THEN 'molecular_test_episode_v2'
        WHEN rr.braf_positive_rrf IS NOT NULL THEN 'recurrence_risk_features_mv'
        ELSE NULL
    END AS braf_source,

    -- RAS
    COALESCE(pm.ras_positive_any, rr.ras_positive_rrf, FALSE) AS ras_positive_refined,

    -- RET
    COALESCE(pm.ret_positive_any, rr.ret_positive_rrf, FALSE) AS ret_positive_refined,

    -- NTRK / TP53 (molecular episodes only)
    COALESCE(pm.ntrk_positive_any, FALSE) AS ntrk_positive_refined,
    COALESCE(pm.tp53_positive_any, FALSE) AS tp53_positive_refined,
    COALESCE(pm.high_risk_any, FALSE) AS high_risk_marker_any,

    pm.platforms_used,
    pm.first_test_date,
    pm.n_molecular_tests,

    CURRENT_TIMESTAMP AS refined_at
FROM spine s
LEFT JOIN patient_mol pm ON s.research_id = pm.research_id
LEFT JOIN rrf rr ON s.research_id = rr.research_id
ORDER BY s.research_id;
"""


def build_postop_labs_sql() -> str:
    """SQL to create extracted_postop_labs_v1 from clinical notes."""
    return """
CREATE OR REPLACE TABLE extracted_postop_labs_v1 AS
WITH

surgery_dates AS (
    SELECT research_id, MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
    FROM path_synoptics
    WHERE surg_date IS NOT NULL
    GROUP BY research_id
),

note_labs AS (
    SELECT
        n.research_id,
        n.note_type,
        TRY_CAST(n.note_date AS DATE) AS note_date,
        n.note_text,
        s.first_surgery_date
    FROM clinical_notes_long n
    JOIN surgery_dates s ON n.research_id = s.research_id
    WHERE n.note_type IN ('endocrine_note', 'dc_sum', 'op_note', 'other_notes', 'ed_note')
      AND (LOWER(n.note_text) LIKE '%pth%'
           OR LOWER(n.note_text) LIKE '%parathyroid hormone%'
           OR LOWER(n.note_text) LIKE '%calcium level%'
           OR LOWER(n.note_text) LIKE '%calcium was%'
           OR LOWER(n.note_text) LIKE '%ionized calcium%'
           OR LOWER(n.note_text) LIKE '%ca2+%'
           OR LOWER(n.note_text) LIKE '%hypocalcemia%')
)

-- Placeholder: Python-side extraction will populate via INSERT
SELECT
    NULL::INTEGER AS research_id,
    NULL::VARCHAR AS lab_date,
    NULL::VARCHAR AS lab_type,
    NULL::DOUBLE AS value,
    NULL::VARCHAR AS unit,
    NULL::VARCHAR AS source_note_type,
    NULL::INTEGER AS days_postop,
    NULL::BOOLEAN AS nadir_flag,
    NULL::VARCHAR AS match_text
WHERE FALSE;
"""


def build_postop_lab_nadir_sql() -> str:
    """SQL view for per-patient PTH/calcium nadir."""
    return """
CREATE OR REPLACE VIEW vw_postop_lab_nadir AS
WITH lab_with_rank AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, lab_type
            ORDER BY value ASC, days_postop ASC
        ) AS rn
    FROM extracted_postop_labs_v1
    WHERE (days_postop BETWEEN 0 AND 30 OR days_postop IS NULL)
      AND value IS NOT NULL
)
SELECT
    research_id,
    lab_type,
    value AS nadir_value,
    unit,
    lab_date AS nadir_date,
    days_postop AS nadir_days_postop,
    source_note_type
FROM lab_with_rank
WHERE rn = 1;
"""


def build_ene_refined_sql() -> str:
    """SQL for extranodal extension refinement."""
    return """
CREATE OR REPLACE TABLE extracted_ene_refined_v1 AS
WITH

structured_ene AS (
    SELECT
        research_id,
        tumor_1_extranodal_extension AS raw_value,
        CASE
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,''))
                 IN ('','none','no','absent','not identified','negative','n/a','n/s','c/a','null')
                THEN 'absent'
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,'')) = 'x'
                THEN 'present_ungraded'
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,''))
                 LIKE '%present%' OR LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%identified%'
                THEN 'present'
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%focal%'
                THEN 'focal'
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%extensive%'
                THEN 'extensive'
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%microscopic%'
                THEN 'microscopic'
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%indeterminate%'
                THEN 'indeterminate'
            ELSE 'present_ungraded'
        END AS ene_status,
        -- Extract level info from freetext
        CASE
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%level 6%'
                 OR LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%level vi%'
                THEN 'level_VI'
            WHEN regexp_matches(LOWER(COALESCE(tumor_1_extranodal_extension,'')),
                 'level\\s+(?:2|ii|3|iii|4|iv|5|v)')
                THEN regexp_extract(LOWER(tumor_1_extranodal_extension),
                     '(level\\s+(?:2|ii|3|iii|4|iv|5|v)[ab]?)', 1)
            WHEN LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%left neck%'
                 OR LOWER(COALESCE(tumor_1_extranodal_extension,'')) LIKE '%right neck%'
                THEN 'lateral'
            ELSE NULL
        END AS ene_level,
        'path_synoptics' AS source_table,
        1.0 AS source_reliability,
        surg_date AS detection_date
    FROM path_synoptics
    WHERE tumor_1_extranodal_extension IS NOT NULL
      AND LOWER(COALESCE(tumor_1_extranodal_extension,'')) NOT IN ('', 'null')
),

per_patient AS (
    SELECT
        research_id,
        -- Prefer worst grade
        MAX(CASE ene_status
            WHEN 'extensive' THEN 5
            WHEN 'present' THEN 4
            WHEN 'focal' THEN 3
            WHEN 'microscopic' THEN 3
            WHEN 'present_ungraded' THEN 2
            WHEN 'indeterminate' THEN 1
            WHEN 'absent' THEN 0
            ELSE 0 END) AS grade_rank,
        STRING_AGG(DISTINCT ene_level, ', ') FILTER (WHERE ene_level IS NOT NULL) AS ene_levels,
        COUNT(*) AS n_records,
        MAX(detection_date) AS latest_date
    FROM structured_ene
    GROUP BY research_id
)

SELECT
    pp.research_id,
    CASE pp.grade_rank
        WHEN 5 THEN 'extensive'
        WHEN 4 THEN 'present'
        WHEN 3 THEN 'focal'
        WHEN 2 THEN 'present_ungraded'
        WHEN 1 THEN 'indeterminate'
        WHEN 0 THEN 'absent'
        ELSE 'unknown'
    END AS ene_status_refined,
    pp.ene_levels,
    pp.n_records,
    pp.latest_date,
    'path_synoptics' AS source_table,
    CASE WHEN pp.grade_rank >= 2 THEN TRUE ELSE FALSE END AS ene_positive,
    CURRENT_TIMESTAMP AS refined_at
FROM per_patient pp
ORDER BY pp.research_id;
"""


def build_rai_source_validation_sql() -> str:
    """SQL to create RAI dose/avidity source-attributed table."""
    return """
CREATE OR REPLACE TABLE extracted_rai_validated_v1 AS
WITH rai_with_source AS (
    SELECT
        r.research_id,
        r.rai_episode_id,
        r.resolved_rai_date,
        r.dose_mci,
        r.dose_text_raw,
        r.rai_assertion_status,
        r.rai_intent,
        r.source_note_type,
        r.iodine_avidity_flag,
        r.stimulated_tg,
        r.stimulated_tsh,
        r.scan_findings_raw,
        r.pre_scan_flag,
        r.post_therapy_scan_flag,
        CASE
            WHEN r.source_note_type = 'endocrine_note' THEN 0.8
            WHEN r.source_note_type = 'dc_sum' THEN 0.7
            WHEN r.source_note_type = 'op_note' THEN 0.6
            WHEN r.source_note_type = 'other_notes' THEN 0.5
            WHEN r.source_note_type = 'h_p' THEN 0.2
            ELSE 0.5
        END AS source_reliability,
        CASE
            WHEN r.rai_assertion_status = 'definite_received' THEN 1.0
            WHEN r.rai_assertion_status = 'likely_received' THEN 0.8
            WHEN r.rai_assertion_status = 'planned' THEN 0.4
            WHEN r.rai_assertion_status = 'historical' THEN 0.6
            WHEN r.rai_assertion_status = 'negated' THEN 0.0
            WHEN r.rai_assertion_status = 'ambiguous' THEN 0.3
            ELSE 0.5
        END AS assertion_confidence
    FROM rai_treatment_episode_v2 r
),

per_patient AS (
    SELECT
        research_id,
        COUNT(*) AS n_rai_episodes,
        COUNT(CASE WHEN rai_assertion_status IN ('definite_received','likely_received') THEN 1 END)
            AS confirmed_rai_episodes,
        MAX(dose_mci) AS max_dose_mci,
        MIN(resolved_rai_date) AS first_rai_date,
        MAX(resolved_rai_date) AS last_rai_date,
        BOOL_OR(LOWER(CAST(iodine_avidity_flag AS VARCHAR)) = 'true') AS any_avid,
        MAX(stimulated_tg) AS max_stimulated_tg,
        STRING_AGG(DISTINCT source_note_type, ', ') AS source_types,
        STRING_AGG(DISTINCT rai_intent, ', ') FILTER (WHERE rai_intent IS NOT NULL) AS intents,
        MAX(source_reliability) AS best_source_reliability,
        MAX(assertion_confidence) AS best_assertion_confidence
    FROM rai_with_source
    GROUP BY research_id
)

SELECT
    pp.*,
    CASE
        WHEN pp.confirmed_rai_episodes > 0 AND pp.max_dose_mci IS NOT NULL THEN 'confirmed_with_dose'
        WHEN pp.confirmed_rai_episodes > 0 THEN 'confirmed_no_dose'
        WHEN pp.n_rai_episodes > 0 AND pp.max_dose_mci IS NOT NULL THEN 'unconfirmed_with_dose'
        WHEN pp.n_rai_episodes > 0 THEN 'unconfirmed_no_dose'
        ELSE 'no_rai'
    END AS rai_validation_tier,
    CURRENT_TIMESTAMP AS refined_at
FROM per_patient pp
ORDER BY pp.research_id;
"""


def build_master_clinical_v4_sql() -> str:
    """SQL for patient_refined_master_clinical_v4 — unified master table."""
    return """
CREATE OR REPLACE TABLE patient_refined_master_clinical_v4 AS
SELECT
    sf.research_id,

    -- Phase 4 staging flags
    sf.ete_path_confirmed,
    sf.ete_grade AS ete_grade_v3,
    sf.margin_status_refined,
    sf.closest_margin_mm,
    sf.vascular_invasion_refined,
    sf.lvi_refined,
    sf.perineural_invasion_refined,
    sf.capsular_invasion_refined,
    sf.tumor_size_path_cm,
    sf.tumor_size_imaging_cm,
    sf.braf_positive_refined AS braf_positive_v3,
    sf.ras_positive_refined AS ras_positive_v3,
    sf.tert_positive_refined AS tert_positive_v3,
    sf.molecular_platform AS molecular_platform_v3,
    sf.recurrence_confirmed,
    sf.recurrence_risk_band,

    -- Phase 5 Variable 1: ETE sub-grading
    COALESCE(es.refined_ete_grade, sf.ete_grade) AS ete_grade_v5,
    es.op_note_grade AS ete_op_note_subgrade,
    es.subgrade_method,

    -- Phase 5 Variable 2: TERT refined
    COALESCE(mr.tert_positive_refined, sf.tert_positive_refined, FALSE) AS tert_positive_v5,
    mr.tert_source,
    mr.tert_tested,
    COALESCE(mr.braf_positive_refined, sf.braf_positive_refined, FALSE) AS braf_positive_v5,
    mr.braf_source,
    COALESCE(mr.ras_positive_refined, sf.ras_positive_refined, FALSE) AS ras_positive_v5,
    mr.ret_positive_refined,
    mr.ntrk_positive_refined,
    mr.tp53_positive_refined,
    mr.high_risk_marker_any,
    mr.platforms_used,
    mr.n_molecular_tests,

    -- Phase 5 Variable 3: Post-op labs (nadir)
    pth_nadir.nadir_value AS pth_nadir_value,
    pth_nadir.nadir_days_postop AS pth_nadir_days_postop,
    ca_nadir.nadir_value AS calcium_nadir_value,
    ca_nadir.nadir_days_postop AS calcium_nadir_days_postop,

    -- Phase 5 Variable 4: RAI validated
    rv.n_rai_episodes,
    rv.confirmed_rai_episodes,
    rv.max_dose_mci,
    rv.first_rai_date,
    rv.any_avid AS rai_avidity,
    rv.max_stimulated_tg,
    rv.rai_validation_tier,
    rv.best_source_reliability AS rai_source_reliability,

    -- Phase 5 Variable 5: Extranodal extension
    ene.ene_status_refined,
    ene.ene_levels,
    ene.ene_positive,

    -- Complications (Phase 2)
    cf.refined_rln_injury,
    cf.confirmed_rln_injury,
    cf.refined_hypocalcemia,
    cf.confirmed_hypocalcemia,
    cf.refined_hypoparathyroidism,
    cf.confirmed_hypoparathyroidism,
    cf.refined_chyle_leak,
    cf.refined_seroma,
    cf.refined_hematoma,

    CURRENT_TIMESTAMP AS refined_at

FROM patient_refined_staging_flags_v3 sf
LEFT JOIN extracted_ete_subgraded_v1 es ON sf.research_id = es.research_id
LEFT JOIN extracted_molecular_refined_v1 mr ON sf.research_id = mr.research_id
LEFT JOIN vw_postop_lab_nadir pth_nadir
    ON sf.research_id = pth_nadir.research_id AND pth_nadir.lab_type = 'pth'
LEFT JOIN vw_postop_lab_nadir ca_nadir
    ON sf.research_id = ca_nadir.research_id AND ca_nadir.lab_type = 'total_calcium'
LEFT JOIN extracted_rai_validated_v1 rv ON sf.research_id = rv.research_id
LEFT JOIN extracted_ene_refined_v1 ene ON sf.research_id = ene.research_id
LEFT JOIN patient_refined_complication_flags_v2 cf ON sf.research_id = cf.research_id
ORDER BY sf.research_id;
"""


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
def audit_and_refine_top5(
    con,
    variables: list[str] | None = None,
    verbose: bool = True,
    sample_size: int = 250,
) -> dict[str, dict]:
    """
    Run the full Phase 5 top-5 variable refinement pipeline.
    Returns dict mapping variable name → mini-report dict.
    """
    all_vars = variables or ["ete_subgrade", "tert", "postop_labs", "rai_validation", "ene"]
    results = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    for var in all_vars:
        if verbose:
            print(f"\n{'='*70}")
            print(f"  Phase 5 Refinement: {var}")
            print(f"{'='*70}")

        if var == "ete_subgrade":
            results[var] = _refine_ete_subgrade(con, verbose)
        elif var == "tert":
            results[var] = _refine_tert(con, verbose)
        elif var == "postop_labs":
            results[var] = _refine_postop_labs(con, verbose, sample_size)
        elif var == "rai_validation":
            results[var] = _refine_rai(con, verbose)
        elif var == "ene":
            results[var] = _refine_ene(con, verbose)
        else:
            if verbose:
                print(f"  [skip] Unknown variable: {var}")

    if verbose:
        print(f"\n{'='*70}")
        print(f"  Building master clinical table v4")
        print(f"{'='*70}")
    try:
        con.execute(build_master_clinical_v4_sql())
        n = con.execute("SELECT COUNT(*) FROM patient_refined_master_clinical_v4").fetchone()[0]
        if verbose:
            print(f"  patient_refined_master_clinical_v4: {n} rows")
        results["master_table"] = {"table": "patient_refined_master_clinical_v4", "rows": n}
    except Exception as e:
        if verbose:
            print(f"  [warn] Master table build: {e}")
        results["master_table"] = {"error": str(e)}

    return results


def _refine_ete_subgrade(con, verbose: bool) -> dict:
    """Variable 1: ETE sub-grading from op notes."""
    if verbose:
        print("  Deploying ETE sub-grading SQL...")
    con.execute(build_ete_subgrading_sql())

    stats = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN refined_ete_grade = 'gross' THEN 1 END) AS gross,
            COUNT(CASE WHEN refined_ete_grade = 'microscopic' THEN 1 END) AS microscopic,
            COUNT(CASE WHEN refined_ete_grade = 'op_note_none_path_positive' THEN 1 END) AS op_none_path_pos,
            COUNT(CASE WHEN refined_ete_grade = 'present_ungraded' THEN 1 END) AS still_ungraded,
            COUNT(CASE WHEN subgrade_method = 'op_note_subgraded' THEN 1 END) AS subgraded
        FROM extracted_ete_subgraded_v1
    """).fetchone()

    report = {
        "total_ungraded_input": stats[0],
        "newly_gross": stats[1],
        "newly_microscopic": stats[2],
        "op_note_none_path_positive": stats[3],
        "still_ungraded": stats[4],
        "total_subgraded": stats[5],
        "subgrade_rate": f"{100*stats[5]/max(stats[0],1):.1f}%",
    }
    if verbose:
        print(f"  Results: {report}")
    return report


def _refine_tert(con, verbose: bool) -> dict:
    """Variable 2: TERT from molecular episodes."""
    if verbose:
        print("  Deploying TERT refined SQL...")
    con.execute(build_tert_refined_sql())

    stats = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN tert_positive_refined THEN 1 END) AS tert_pos,
            COUNT(CASE WHEN tert_tested THEN 1 END) AS tert_tested,
            COUNT(CASE WHEN braf_positive_refined THEN 1 END) AS braf_pos,
            COUNT(CASE WHEN ras_positive_refined THEN 1 END) AS ras_pos,
            COUNT(CASE WHEN tp53_positive_refined THEN 1 END) AS tp53_pos
        FROM extracted_molecular_refined_v1
    """).fetchone()

    report = {
        "total_patients": stats[0],
        "tert_positive": stats[1],
        "tert_tested": stats[2],
        "braf_positive": stats[3],
        "ras_positive": stats[4],
        "tp53_positive": stats[5],
        "tert_positivity_rate": f"{100*stats[1]/max(stats[2],1):.1f}%" if stats[2] else "N/A",
    }
    if verbose:
        print(f"  Results: {report}")
    return report


def _refine_postop_labs(con, verbose: bool, sample_size: int = 250) -> dict:
    """Variable 3: Post-op PTH/calcium from clinical notes."""
    if verbose:
        print("  Creating extracted_postop_labs_v1 table...")
    con.execute(build_postop_labs_sql())

    pipeline = LabIngestionPipeline()

    notes_df = con.execute(f"""
        WITH surgery_dates AS (
            SELECT research_id, MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
            FROM path_synoptics WHERE surg_date IS NOT NULL
            GROUP BY research_id
        )
        SELECT
            n.research_id,
            n.note_type,
            n.note_date,
            s.first_surgery_date,
            SUBSTRING(n.note_text, 1, 5000) AS note_text
        FROM clinical_notes_long n
        JOIN surgery_dates s ON n.research_id = s.research_id
        WHERE n.note_type IN ('endocrine_note', 'dc_sum', 'op_note', 'other_notes', 'ed_note')
          AND (LOWER(n.note_text) LIKE '%pth%'
               OR LOWER(n.note_text) LIKE '%parathyroid hormone%'
               OR LOWER(n.note_text) LIKE '%calcium level%'
               OR LOWER(n.note_text) LIKE '%calcium was%'
               OR LOWER(n.note_text) LIKE '%ionized calcium%'
               OR LOWER(n.note_text) LIKE '%hypocalcemia%')
    """).fetchdf()

    if verbose:
        print(f"  Processing {len(notes_df)} notes with PTH/calcium mentions...")

    all_labs = []
    for _, row in notes_df.iterrows():
        labs = pipeline.extract_labs_from_note(
            research_id=int(row["research_id"]),
            note_text=str(row.get("note_text", "")),
            note_type=str(row.get("note_type", "")),
            note_date=str(row.get("note_date", "")) if row.get("note_date") else None,
            surgery_date=str(row.get("first_surgery_date", "")) if row.get("first_surgery_date") else None,
        )
        all_labs.extend(labs)

    if verbose:
        print(f"  Extracted {len(all_labs)} lab values from {len(notes_df)} notes")

    if all_labs:
        labs_df = pd.DataFrame(all_labs)
        labs_df["nadir_flag"] = False
        labs_df["lab_date"] = labs_df["lab_date"].astype(str).replace({"None": None, "nan": None, "NaT": None})
        labs_df["days_postop"] = pd.to_numeric(labs_df["days_postop"], errors="coerce")
        col_order = ["research_id", "lab_date", "lab_type", "value", "unit",
                     "source_note_type", "days_postop", "nadir_flag", "match_text"]
        labs_df = labs_df[col_order]
        con.execute("DELETE FROM extracted_postop_labs_v1")
        con.execute("INSERT INTO extracted_postop_labs_v1 SELECT * FROM labs_df")

        con.execute(build_postop_lab_nadir_sql())

        stats = con.execute("""
            SELECT
                COUNT(*) AS total_labs,
                COUNT(DISTINCT research_id) AS patients,
                COUNT(CASE WHEN lab_type = 'pth' THEN 1 END) AS pth_values,
                COUNT(CASE WHEN lab_type = 'total_calcium' THEN 1 END) AS calcium_values,
                COUNT(CASE WHEN lab_type = 'ionized_calcium' THEN 1 END) AS ionized_ca,
                COUNT(DISTINCT CASE WHEN lab_type = 'pth' THEN research_id END) AS pth_patients,
                COUNT(DISTINCT CASE WHEN lab_type LIKE '%calcium%' THEN research_id END) AS ca_patients
            FROM extracted_postop_labs_v1
        """).fetchone()

        report = {
            "total_lab_values": stats[0],
            "unique_patients": stats[1],
            "pth_values": stats[2],
            "calcium_values": stats[3],
            "ionized_calcium_values": stats[4],
            "patients_with_pth": stats[5],
            "patients_with_calcium": stats[6],
        }
    else:
        report = {"total_lab_values": 0, "error": "No lab values extracted"}

    if verbose:
        print(f"  Results: {report}")
    return report


def _refine_rai(con, verbose: bool) -> dict:
    """Variable 4: RAI dose/avidity validation."""
    if verbose:
        print("  Deploying RAI validation SQL...")
    con.execute(build_rai_source_validation_sql())

    stats = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN rai_validation_tier = 'confirmed_with_dose' THEN 1 END) AS confirmed_dose,
            COUNT(CASE WHEN rai_validation_tier = 'confirmed_no_dose' THEN 1 END) AS confirmed_no_dose,
            COUNT(CASE WHEN rai_validation_tier = 'unconfirmed_with_dose' THEN 1 END) AS unconf_dose,
            COUNT(CASE WHEN rai_validation_tier = 'unconfirmed_no_dose' THEN 1 END) AS unconf_no_dose,
            COUNT(CASE WHEN any_avid THEN 1 END) AS avid_patients,
            AVG(max_dose_mci) FILTER (WHERE max_dose_mci IS NOT NULL) AS avg_dose
        FROM extracted_rai_validated_v1
    """).fetchone()

    report = {
        "total_rai_patients": stats[0],
        "confirmed_with_dose": stats[1],
        "confirmed_no_dose": stats[2],
        "unconfirmed_with_dose": stats[3],
        "unconfirmed_no_dose": stats[4],
        "avid_patients": stats[5],
        "avg_dose_mci": round(stats[6], 1) if stats[6] else None,
    }
    if verbose:
        print(f"  Results: {report}")
    return report


def _refine_ene(con, verbose: bool) -> dict:
    """Variable 5: Extranodal extension refinement."""
    if verbose:
        print("  Deploying ENE refinement SQL...")
    con.execute(build_ene_refined_sql())

    stats = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN ene_positive THEN 1 END) AS ene_positive,
            COUNT(CASE WHEN ene_status_refined = 'present' THEN 1 END) AS present,
            COUNT(CASE WHEN ene_status_refined = 'extensive' THEN 1 END) AS extensive,
            COUNT(CASE WHEN ene_status_refined = 'focal' THEN 1 END) AS focal,
            COUNT(CASE WHEN ene_status_refined = 'present_ungraded' THEN 1 END) AS ungraded,
            COUNT(CASE WHEN ene_status_refined = 'absent' THEN 1 END) AS absent,
            COUNT(CASE WHEN ene_levels IS NOT NULL THEN 1 END) AS with_level
        FROM extracted_ene_refined_v1
    """).fetchone()

    report = {
        "total_patients": stats[0],
        "ene_positive": stats[1],
        "present": stats[2],
        "extensive": stats[3],
        "focal": stats[4],
        "ungraded": stats[5],
        "absent": stats[6],
        "with_level_detail": stats[7],
        "positivity_rate": f"{100*stats[1]/max(stats[0],1):.1f}%",
    }
    if verbose:
        print(f"  Results: {report}")
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _get_connection(use_md: bool, local_path: str = "thyroid_master.duckdb"):
    import duckdb
    if use_md:
        try:
            import toml
            secrets = toml.load(PROJECT_ROOT / ".streamlit/secrets.toml")
            token = secrets["MOTHERDUCK_TOKEN"]
        except Exception:
            import os
            token = os.environ.get("MOTHERDUCK_TOKEN", "")
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(str(PROJECT_ROOT / local_path))


def main():
    parser = argparse.ArgumentParser(description="Phase 5 Top-5 Variable Refinement")
    parser.add_argument("--variable", default="all",
                        choices=["all", "ete_subgrade", "tert", "postop_labs",
                                 "rai_validation", "ene"],
                        help="Which variable to refine")
    parser.add_argument("--sample", type=int, default=250)
    parser.add_argument("--md", action="store_true", default=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default="notes_extraction")
    args = parser.parse_args()

    use_md = not args.local

    if args.dry_run:
        print(f"[dry-run] Would refine variable={args.variable}")
        return

    con = _get_connection(use_md)

    if args.variable == "all":
        variables = None
    else:
        variables = [args.variable]

    results = audit_and_refine_top5(con, variables=variables,
                                      verbose=True, sample_size=args.sample)

    out_dir = PROJECT_ROOT / args.output_dir
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    report_path = out_dir / f"phase5_refinement_{timestamp}.md"

    lines = ["# Phase 5 Top-5 Variable Refinement Report", f"_Generated: {timestamp}_", ""]
    for var, rpt in results.items():
        lines.append(f"## {var}")
        for k, v in rpt.items():
            lines.append(f"- **{k}:** {v}")
        lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[phase5] Report saved: {report_path}")

    con.close()


if __name__ == "__main__":
    main()
