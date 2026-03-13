"""
Extraction Audit Engine v4 — Phase 6 Source-Linked Staging Refinement
=====================================================================
Extends v3 with specialized parsers for:
  1. MarginDistanceParser   – R0/R1/R2 status + closest margin mm
  2. InvasionGrader         – Vascular (focal/extensive), LVI, PNI grading
  3. LNYieldCalculator      – Total examined, positive, ratio, levels dissected
  4. ENEDeepener            – Extranodal extension with source hierarchy

Source hierarchy: path_synoptic (1.0) > op_note (0.9) > imaging (0.7) > consent (0.2)

Usage:
    from notes_extraction.extraction_audit_engine_v4 import audit_and_refine_phase6
    results = audit_and_refine_phase6(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v4.py --md --variable all
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

from notes_extraction.extraction_audit_engine_v3 import (
    GradingParser,
    CrossSourceReconciler_v2,
    ExtranodaParser,
    _get_connection,
    build_master_clinical_v4_sql,
)
from notes_extraction.extraction_audit_engine_v2 import (
    SOURCE_RELIABILITY,
    VARIABLE_CONFIGS,
)
from notes_extraction.extraction_audit_engine import CONSENT_BOILERPLATE_PATTERNS

# ---------------------------------------------------------------------------
# Source hierarchy constants
# ---------------------------------------------------------------------------
PHASE6_SOURCE_HIERARCHY = {
    "path_synoptic": 1.0,
    "structured_db": 1.0,
    "op_note": 0.9,
    "endocrine": 0.8,
    "discharge": 0.7,
    "imaging": 0.7,
    "other": 0.5,
    "h_p_consent": 0.2,
}

# ---------------------------------------------------------------------------
# 1. MarginDistanceParser
# ---------------------------------------------------------------------------
_MARGIN_INVOLVED_VALS = {
    "x", "involved", "involvd", "positive", "present", "yes",
}
_MARGIN_NEGATIVE_VALS = {"negative", "uninvolved", "free", "no", "clear"}
_MARGIN_CLOSE_VALS = {"close", "less than 1mm", "<1mm", "close margin"}
_MARGIN_INDETERMINATE_VALS = {"indeterminate", "c/a", "n/s", "cannot assess",
                               "not applicable", "n/a"}

_DISTANCE_NUMERIC_RE = re.compile(r"^<?\.?(\d+(?:\.\d+)?)$")
_DISTANCE_LT_RE = re.compile(r"^<\s*\.?(\d+(?:\.\d+)?)$")


class MarginDistanceParser:
    """Parse margin status to R0/R1/R2 and closest distance in mm."""

    R_CLASSIFICATION = {
        "R0": "negative margins (no residual tumor)",
        "R1": "microscopic residual (margin involved or < 1mm)",
        "R2": "macroscopic residual (gross tumor at margin)",
    }

    def classify_margin(self, raw_status: str | None, raw_distance: str | None,
                        ete_grade: str | None = None) -> dict:
        if not raw_status and not raw_distance:
            return self._empty()

        status_norm = self._normalize_status(raw_status)
        distance_mm = self._parse_distance(raw_distance)
        r_class = self._derive_r_class(status_norm, distance_mm, ete_grade)

        return {
            "margin_status_refined": status_norm,
            "margin_r_classification": r_class,
            "closest_margin_mm": distance_mm,
            "margin_source": "path_synoptic",
            "margin_reliability": 1.0,
            "confidence": self._confidence(status_norm, distance_mm),
        }

    def _normalize_status(self, raw: str | None) -> str:
        if not raw:
            return "unknown"
        v = raw.strip().lower()
        if v in _MARGIN_INVOLVED_VALS:
            return "involved"
        if v in _MARGIN_NEGATIVE_VALS:
            return "negative"
        if v in _MARGIN_CLOSE_VALS:
            return "close"
        if v in _MARGIN_INDETERMINATE_VALS:
            return "indeterminate"
        d = self._parse_distance(v)
        if d is not None:
            return "close" if d < 1.0 else "negative"
        return "involved" if v else "unknown"

    def _parse_distance(self, raw: str | None) -> float | None:
        if not raw:
            return None
        v = raw.strip().lower().rstrip(";").strip()
        if v in ("null", "n/s", "c/a", "n/a", "x", ""):
            return None
        m = _DISTANCE_LT_RE.match(v)
        if m:
            return float(m.group(1))
        m = _DISTANCE_NUMERIC_RE.match(v)
        if m:
            return float(m.group(1))
        try:
            return float(v)
        except ValueError:
            return None

    def _derive_r_class(self, status: str, distance_mm: float | None,
                        ete_grade: str | None) -> str:
        if ete_grade == "gross" and status == "involved":
            return "R2"
        if status == "involved":
            return "R1"
        if status == "close":
            return "R1" if distance_mm is not None and distance_mm < 0.1 else "R0_close"
        if status == "negative":
            return "R0"
        return "Rx"

    def _confidence(self, status: str, distance_mm: float | None) -> float:
        if status in ("involved", "negative") and distance_mm is not None:
            return 0.95
        if status in ("involved", "negative"):
            return 0.90
        if status == "close":
            return 0.85
        return 0.50

    def _empty(self) -> dict:
        return {
            "margin_status_refined": None,
            "margin_r_classification": None,
            "closest_margin_mm": None,
            "margin_source": None,
            "margin_reliability": 0.0,
            "confidence": 0.0,
        }


# ---------------------------------------------------------------------------
# NLP patterns for margins in free text
# ---------------------------------------------------------------------------
_MARGIN_NLP_POSITIVE = [
    re.compile(r"\bmargin(?:s)?\s+(?:are\s+)?(?:positive|involved|invaded)\b", re.I),
    re.compile(r"\btumor\s+(?:at|extends?\s+to)\s+(?:the\s+)?(?:inked\s+)?margin\b", re.I),
    re.compile(r"\bR1\s+resection\b", re.I),
    re.compile(r"\bink(?:ed)?\s+margin\s+positive\b", re.I),
]

_MARGIN_NLP_NEGATIVE = [
    re.compile(r"\bmargin(?:s)?\s+(?:are\s+)?(?:negative|free|clear|uninvolved)\b", re.I),
    re.compile(r"\bR0\s+resection\b", re.I),
    re.compile(r"\bno\s+(?:tumor\s+)?(?:at|involving)\s+(?:the\s+)?margin\b", re.I),
]

_MARGIN_NLP_DISTANCE = re.compile(
    r"\bclosest\s+margin\s*(?:is\s+)?(?:of\s+)?(\d+(?:\.\d+)?)\s*(?:mm|millimeter)", re.I
)


# ---------------------------------------------------------------------------
# 2. InvasionGrader
# ---------------------------------------------------------------------------
_INVASION_PRESENT_VALS = {"x", "present", "presnt", "preesent", "identified", "yes"}
_INVASION_FOCAL_VALS = {"focal", "foacl", "minimal", "limited", "1 focus"}
_INVASION_EXTENSIVE_VALS = {"extensive", "estensive", "extrensive", "extensivre",
                             "extensiver", "prominent", "multifocal"}
_INVASION_INDETERMINATE_VALS = {"indeterminate", "indeeterminate", "indeterminent",
                                 "indetermiante", "suspicious", "c/a", "n/s"}
_INVASION_ABSENT_VALS = {"absent", "no", "none", "negative", "not identified"}

_ANGIO_QUANTIFY_FOCAL_RE = re.compile(r"^<\s*4")
_ANGIO_QUANTIFY_EXTENSIVE_RE = re.compile(r"^>?\s*=?\s*4|^[5-9]|^\d{2,}")
_ANGIO_QUANTIFY_NUMERIC_RE = re.compile(r"^(\d+)")


class InvasionGrader:
    """Grade vascular invasion (with WHO 2022 cutoffs), LVI, and PNI."""

    def grade_vascular(self, raw_angio: str | None, raw_quantify: str | None) -> dict:
        if not raw_angio:
            return self._empty("vascular_invasion")

        v = raw_angio.strip().lower()
        quantify = (raw_quantify or "").strip().lower()

        if v in _INVASION_ABSENT_VALS:
            return self._result("vascular_invasion", "absent", "none", 0.90)

        if v in _INVASION_INDETERMINATE_VALS:
            return self._result("vascular_invasion", "indeterminate", None, 0.50)

        grade = self._grade_from_text(v)
        if grade == "present_ungraded" and quantify:
            grade = self._grade_from_quantify(quantify)
        if grade == "present_ungraded" and v in _INVASION_FOCAL_VALS:
            grade = "focal"
        if grade == "present_ungraded" and v in _INVASION_EXTENSIVE_VALS:
            grade = "extensive"

        vessel_count = self._parse_vessel_count(quantify)

        return {
            "entity_name": "vascular_invasion",
            "refined_value": grade,
            "vessel_count": vessel_count,
            "who_2022_grade": "focal (<4 vessels)" if grade == "focal"
                             else "extensive (>=4 vessels)" if grade == "extensive"
                             else grade,
            "source": "path_synoptic",
            "reliability": 1.0,
            "confidence": 0.92 if grade in ("focal", "extensive") else 0.80,
        }

    def grade_lvi(self, raw_lvi: str | None) -> dict:
        if not raw_lvi:
            return self._empty("lvi")
        v = raw_lvi.strip().lower()
        if v in _INVASION_ABSENT_VALS:
            return self._result("lvi", "absent", "none", 0.90)
        if v in _INVASION_INDETERMINATE_VALS:
            return self._result("lvi", "indeterminate", None, 0.50)
        grade = self._grade_from_text(v)
        return self._result("lvi", grade, None, 0.88 if grade != "present_ungraded" else 0.75)

    def grade_pni(self, raw_pni: str | None) -> dict:
        if not raw_pni:
            return self._empty("pni")
        v = raw_pni.strip().lower()
        if v in _INVASION_ABSENT_VALS:
            return self._result("pni", "absent", "none", 0.90)
        if v in _INVASION_INDETERMINATE_VALS:
            return self._result("pni", "indeterminate", None, 0.50)
        grade = self._grade_from_text(v)
        return self._result("pni", grade, None, 0.88 if grade != "present_ungraded" else 0.75)

    def _grade_from_text(self, v: str) -> str:
        if v in _INVASION_FOCAL_VALS:
            return "focal"
        if v in _INVASION_EXTENSIVE_VALS:
            return "extensive"
        if v in _INVASION_PRESENT_VALS:
            return "present_ungraded"
        return "present_ungraded"

    def _grade_from_quantify(self, q: str) -> str:
        if _ANGIO_QUANTIFY_FOCAL_RE.match(q):
            return "focal"
        if _ANGIO_QUANTIFY_EXTENSIVE_RE.match(q):
            return "extensive"
        m = _ANGIO_QUANTIFY_NUMERIC_RE.match(q)
        if m:
            n = int(m.group(1))
            return "focal" if n < 4 else "extensive"
        return "present_ungraded"

    def _parse_vessel_count(self, q: str) -> int | None:
        if not q:
            return None
        m = _ANGIO_QUANTIFY_NUMERIC_RE.match(q)
        if m:
            return int(m.group(1))
        return None

    def _result(self, entity: str, value: str, grade: str | None, conf: float) -> dict:
        return {
            "entity_name": entity,
            "refined_value": value,
            "vessel_count": None,
            "who_2022_grade": grade,
            "source": "path_synoptic",
            "reliability": 1.0,
            "confidence": conf,
        }

    def _empty(self, entity: str) -> dict:
        return {
            "entity_name": entity,
            "refined_value": None,
            "vessel_count": None,
            "who_2022_grade": None,
            "source": None,
            "reliability": 0.0,
            "confidence": 0.0,
        }


# ---------------------------------------------------------------------------
# NLP patterns for invasion in free text
# ---------------------------------------------------------------------------
_VASC_NLP_PRESENT = [
    re.compile(r"\bvascular\s+(?:space\s+)?invasion\s*(?::\s*)?(present|identified|focal|extensive)\b", re.I),
    re.compile(r"\bangioinvasion\s*(?::\s*)?(present|focal|extensive)\b", re.I),
    re.compile(r"\b(\d+)\s+(?:foci?\s+of\s+)?(?:vascular|angio)\s*invasion\b", re.I),
]
_LVI_NLP_PRESENT = [
    re.compile(r"\blymphatic\s+(?:space\s+)?invasion\s*(?::\s*)?(present|identified|focal|extensive)\b", re.I),
    re.compile(r"\blymphovascular\s+invasion\s*(?::\s*)?(present|identified)\b", re.I),
]
_PNI_NLP_PRESENT = [
    re.compile(r"\bperineural\s+invasion\s*(?::\s*)?(present|identified|focal)\b", re.I),
]


# ---------------------------------------------------------------------------
# 3. LNYieldCalculator
# ---------------------------------------------------------------------------
_SEMICOLON_STRIP_RE = re.compile(r"[;\s]+$")
_LN_LOCATION_PARSE_RE = re.compile(
    r"(\d+)\s*/\s*(\d+)\s+([\w\s/]+?)(?:;|$)", re.I
)
_LN_LEVEL_RE = re.compile(
    r"\b(?:level\s+)?([IViv]+[abAB]?|[2-7][abAB]?|vi|central|lateral)\b", re.I
)

LEVEL_NORMALIZATION = {
    "vi": "VI", "6": "VI", "central": "VI",
    "ii": "II", "2": "II", "iia": "IIA", "2a": "IIA", "iib": "IIB", "2b": "IIB",
    "iii": "III", "3": "III",
    "iv": "IV", "4": "IV", "iva": "IVA", "4a": "IVA", "ivb": "IVB", "4b": "IVB",
    "v": "V", "5": "V", "va": "VA", "5a": "VA", "vb": "VB", "5b": "VB",
    "vii": "VII", "7": "VII",
}

LOCATION_NORMALIZATION = {
    "perithyroidal": "central_VI",
    "pretracheal": "central_VI",
    "paratracheal": "central_VI",
    "prelaryngeal": "central_VI",
    "delphian": "central_VI",
    "central neck": "central_VI",
    "pyramidal": "central_VI",
    "jugular": "lateral_II_IV",
    "ij": "lateral_II_IV",
    "submandibular": "lateral_I",
    "supraclavicular": "lateral_V",
    "retrocarotid": "lateral_III_IV",
}


class LNYieldCalculator:
    """Parse lymph node yield: examined, positive, ratio, levels dissected."""

    def parse_yield(self, raw_examined: str | None, raw_involved: str | None,
                    raw_location: str | None, raw_other_dissection: str | None) -> dict:
        examined = self._parse_count(raw_examined)
        involved = self._parse_involved(raw_involved)
        locations = self._parse_locations(raw_location, raw_other_dissection)

        ratio = None
        if examined is not None and examined > 0 and involved is not None:
            ratio = round(involved / examined, 3)

        positive_flag = None
        if involved is not None:
            positive_flag = involved > 0
        elif raw_involved and raw_involved.strip().lower() == "x":
            positive_flag = True

        return {
            "ln_total_examined": examined,
            "ln_total_positive": involved,
            "ln_ratio": ratio,
            "ln_positive_flag": positive_flag,
            "ln_levels_dissected": locations.get("levels", []),
            "ln_compartments": locations.get("compartments", []),
            "ln_central_dissected": locations.get("central_dissected", False),
            "ln_lateral_dissected": locations.get("lateral_dissected", False),
            "ln_location_raw": raw_location,
            "ln_source": "path_synoptic",
            "ln_reliability": 1.0,
            "confidence": self._confidence(examined, involved, locations),
        }

    def _parse_count(self, raw: str | None) -> int | None:
        if not raw:
            return None
        v = _SEMICOLON_STRIP_RE.sub("", raw.strip()).lower()
        if v in ("null", "n/s", "c/a", "n/a", "x", ""):
            return None
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None

    def _parse_involved(self, raw: str | None) -> int | None:
        if not raw:
            return None
        v = _SEMICOLON_STRIP_RE.sub("", raw.strip()).lower()
        if v in ("null", "n/s", "c/a", "n/a", ""):
            return None
        if v == "x":
            return None  # positive flag only, no count
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None

    def _parse_locations(self, raw_location: str | None,
                         raw_other: str | None) -> dict:
        levels = set()
        compartments = set()
        central = False
        lateral = False

        for raw in [raw_location, raw_other]:
            if not raw:
                continue
            text = raw.strip().lower()

            for loc_key, compartment in LOCATION_NORMALIZATION.items():
                if loc_key in text:
                    compartments.add(compartment)
                    if compartment.startswith("central"):
                        central = True
                        levels.add("VI")
                    else:
                        lateral = True

            for m in _LN_LOCATION_PARSE_RE.finditer(text):
                loc_text = m.group(3).strip()
                for loc_key, compartment in LOCATION_NORMALIZATION.items():
                    if loc_key in loc_text:
                        compartments.add(compartment)
                        if compartment.startswith("central"):
                            central = True
                            levels.add("VI")
                        else:
                            lateral = True

            for m in _LN_LEVEL_RE.finditer(text):
                lvl = m.group(1).lower()
                norm = LEVEL_NORMALIZATION.get(lvl, lvl.upper())
                levels.add(norm)
                if norm == "VI":
                    central = True
                else:
                    lateral = True

        return {
            "levels": sorted(levels),
            "compartments": sorted(compartments),
            "central_dissected": central,
            "lateral_dissected": lateral,
        }

    def _confidence(self, examined, involved, locations) -> float:
        if examined is not None and involved is not None:
            return 0.95
        if examined is not None:
            return 0.85
        if locations.get("levels"):
            return 0.80
        return 0.50


# ---------------------------------------------------------------------------
# NLP patterns for LN yield in free text
# ---------------------------------------------------------------------------
_LN_YIELD_NLP = [
    re.compile(r"(\d+)\s*(?:of\s+)?(\d+)\s+(?:lymph\s+)?nodes?\s+(?:are\s+)?positive", re.I),
    re.compile(r"(\d+)\s+(?:lymph\s+)?nodes?\s+(?:examined|submitted|received)"
               r"(?:.*?)(\d+)\s+(?:with|show\w*)\s+(?:metastat\w+|carcinoma|tumor)", re.I),
    re.compile(r"(?:metastat\w+|carcinoma|positive)\s+(?:in\s+)?(\d+)\s*(?:of|/)\s*(\d+)"
               r"\s+(?:lymph\s+)?nodes?", re.I),
]


# ---------------------------------------------------------------------------
# 4. ENEDeepener — extend v3 ENE with op-note NLP
# ---------------------------------------------------------------------------
_ENE_NLP_POSITIVE = [
    re.compile(r"\bextranodal\s+extension\s*(?:is\s+)?(present|identified|seen)\b", re.I),
    re.compile(r"\bextracapsular\s+(?:spread|extension|invasion)\s*(?:is\s+)?(present|identified)\b", re.I),
    re.compile(r"\bENE\s*[:\s]+(present|positive|yes)\b", re.I),
    re.compile(r"\b(?:soft\s+tissue|perinodal|extranodal)\s+(?:invasion|extension|infiltration)\s*"
               r"(?:is\s+)?(present|identified)\b", re.I),
]

_ENE_NLP_NEGATIVE = [
    re.compile(r"\bno\s+(?:evidence\s+of\s+)?extranodal\s+extension\b", re.I),
    re.compile(r"\bextranodal\s+extension\s*:\s*(?:absent|no|none|not\s+identified)\b", re.I),
    re.compile(r"\bENE\s*[:\s]+(absent|negative|no|none)\b", re.I),
]

_ENE_NLP_GRADE = [
    re.compile(r"\bextranodal\s+extension.*?(?:focal|limited|microscopic)\b", re.I),
    re.compile(r"\bextranodal\s+extension.*?(?:extensive|gross|macroscopic)\b", re.I),
]


class ENEDeepener:
    """Deepen ENE refinement with NLP from op_notes and path_reports."""

    def classify_ene_nlp(self, context: str, note_type: str) -> dict:
        if not context:
            return {"ene_status": None, "ene_grade": None, "ene_level": None,
                    "confidence": 0.0, "source": None}

        if any(p.search(context[:500]) for p in CONSENT_BOILERPLATE_PATTERNS):
            if note_type in ("h_p", "h_p_consent"):
                return {"ene_status": None, "ene_grade": None, "ene_level": None,
                        "confidence": 0.0, "source": "consent_fp"}

        source_rel = PHASE6_SOURCE_HIERARCHY.get(note_type, 0.5)

        for p in _ENE_NLP_NEGATIVE:
            if p.search(context):
                return {"ene_status": "absent", "ene_grade": "none",
                        "ene_level": self._find_level(context),
                        "confidence": 0.85 * source_rel, "source": note_type}

        for p in _ENE_NLP_POSITIVE:
            if p.search(context):
                grade = self._classify_grade(context)
                return {"ene_status": "present", "ene_grade": grade,
                        "ene_level": self._find_level(context),
                        "confidence": 0.88 * source_rel, "source": note_type}

        return {"ene_status": None, "ene_grade": None, "ene_level": None,
                "confidence": 0.0, "source": None}

    def _classify_grade(self, context: str) -> str:
        text_lower = context.lower()
        if any(w in text_lower for w in ("extensive", "gross", "macroscopic")):
            return "extensive"
        if any(w in text_lower for w in ("focal", "limited", "microscopic", "minimal")):
            return "focal"
        return "present_ungraded"

    def _find_level(self, context: str) -> str | None:
        m = _LN_LEVEL_RE.search(context)
        if m:
            return LEVEL_NORMALIZATION.get(m.group(1).lower(), m.group(1).upper())
        return None


# ---------------------------------------------------------------------------
# SQL Builders
# ---------------------------------------------------------------------------
def build_margins_refined_sql() -> str:
    """Per-patient margin status with R-classification and distance."""
    return """
CREATE OR REPLACE TABLE extracted_margins_refined_v1 AS
WITH
raw_margins AS (
    SELECT
        research_id,
        tumor_1_margin_status AS raw_status,
        tumor_1_distance_to_closest_margin_mm AS raw_distance,
        tumor_1_margin_angiolymphatic_invasion_comment AS margin_comment,
        COALESCE(tumor_1_extrathyroidal_extension, '') AS ete_raw,
        surg_date
    FROM path_synoptics
),

normalized AS (
    SELECT
        research_id,
        raw_status,
        raw_distance,
        margin_comment,
        ete_raw,
        surg_date,
        -- Normalize status
        CASE
            WHEN LOWER(COALESCE(raw_status,'')) IN ('x','involved','involvd','positive','present','yes')
                THEN 'involved'
            WHEN LOWER(COALESCE(raw_status,'')) IN ('negative','uninvolved','free','clear','no')
                THEN 'negative'
            WHEN LOWER(COALESCE(raw_status,'')) IN ('close')
                THEN 'close'
            WHEN LOWER(COALESCE(raw_status,'')) IN ('indeterminate','c/a','n/s','n/a','cannot assess')
                THEN 'indeterminate'
            WHEN raw_status IS NULL OR TRIM(raw_status) = '' THEN NULL
            ELSE 'involved'
        END AS margin_status_norm,
        -- Parse distance
        CASE
            WHEN raw_distance IS NULL OR TRIM(COALESCE(raw_distance,'')) IN ('','null','n/s','c/a','n/a','x')
                THEN NULL
            WHEN regexp_matches(TRIM(raw_distance), '^<?\\.?\\d+(\\.\\d+)?$')
                THEN TRY_CAST(regexp_replace(TRIM(raw_distance), '^<', '') AS DOUBLE)
            ELSE TRY_CAST(TRIM(REPLACE(raw_distance, ';', '')) AS DOUBLE)
        END AS distance_mm,
        -- ETE for R2 derivation
        CASE
            WHEN LOWER(ete_raw) LIKE '%gross%' OR LOWER(ete_raw) LIKE '%extensive%'
                 OR LOWER(ete_raw) LIKE '%strap%' OR LOWER(ete_raw) LIKE '%trachea%'
                THEN 'gross'
            ELSE 'non_gross'
        END AS ete_grade_for_r
    FROM raw_margins
),

per_patient AS (
    SELECT
        research_id,
        -- Worst margin across tumors: involved > close > indeterminate > negative
        MAX(CASE margin_status_norm
            WHEN 'involved' THEN 4
            WHEN 'close' THEN 3
            WHEN 'indeterminate' THEN 2
            WHEN 'negative' THEN 1
            ELSE 0
        END) AS worst_status_rank,
        MIN(distance_mm) FILTER (WHERE distance_mm IS NOT NULL) AS closest_margin_mm,
        BOOL_OR(ete_grade_for_r = 'gross') AS has_gross_ete,
        COUNT(*) AS n_records,
        MAX(surg_date) AS latest_surg_date
    FROM normalized
    WHERE margin_status_norm IS NOT NULL
    GROUP BY research_id
)

SELECT
    pp.research_id,
    CASE pp.worst_status_rank
        WHEN 4 THEN 'involved'
        WHEN 3 THEN 'close'
        WHEN 2 THEN 'indeterminate'
        WHEN 1 THEN 'negative'
        ELSE 'unknown'
    END AS margin_status_refined,
    -- R classification
    CASE
        WHEN pp.worst_status_rank = 4 AND pp.has_gross_ete THEN 'R2'
        WHEN pp.worst_status_rank = 4 THEN 'R1'
        WHEN pp.worst_status_rank = 3 AND pp.closest_margin_mm IS NOT NULL
             AND pp.closest_margin_mm < 0.1 THEN 'R1'
        WHEN pp.worst_status_rank = 3 THEN 'R0_close'
        WHEN pp.worst_status_rank = 1 THEN 'R0'
        WHEN pp.worst_status_rank = 2 THEN 'Rx'
        ELSE 'Rx'
    END AS margin_r_classification,
    pp.closest_margin_mm,
    pp.has_gross_ete AS margin_with_gross_ete,
    pp.n_records,
    'path_synoptic' AS source_category,
    1.0 AS source_reliability,
    CASE
        WHEN pp.worst_status_rank IN (4,1) AND pp.closest_margin_mm IS NOT NULL THEN 0.95
        WHEN pp.worst_status_rank IN (4,1) THEN 0.90
        WHEN pp.worst_status_rank = 3 THEN 0.85
        ELSE 0.50
    END AS confidence,
    CURRENT_TIMESTAMP AS refined_at
FROM per_patient pp
ORDER BY pp.research_id;
"""


def build_invasion_profile_sql() -> str:
    """Per-patient invasion profile: vascular (WHO 2022), LVI, PNI."""
    return """
CREATE OR REPLACE TABLE extracted_invasion_profile_v1 AS
WITH
raw_invasion AS (
    SELECT
        research_id,
        tumor_1_angioinvasion AS raw_vasc,
        tumor_1_angioinvasion_quantify AS raw_vasc_quant,
        tumor_1_lymphatic_invasion AS raw_lvi,
        tumor_1_perineural_invasion AS raw_pni,
        tumor_1_capsular_invasion AS raw_capsular,
        surg_date
    FROM path_synoptics
),

normalized AS (
    SELECT
        research_id,
        surg_date,
        -- Vascular invasion normalization
        CASE
            WHEN LOWER(COALESCE(raw_vasc,'')) IN ('','null') THEN NULL
            WHEN LOWER(raw_vasc) IN ('x','present','presnt','identified','yes') THEN 'present_ungraded'
            WHEN LOWER(raw_vasc) IN ('focal','foacl','minimal','limited','1 focus') THEN 'focal'
            WHEN LOWER(raw_vasc) IN ('extensive','estensive','extrensive','extensivre',
                                      'extensiver','prominent','multifocal') THEN 'extensive'
            WHEN LOWER(raw_vasc) IN ('indeterminate','suspicious','c/a','n/s','s') THEN 'indeterminate'
            WHEN LOWER(raw_vasc) IN ('absent','no','none','negative','not identified') THEN 'absent'
            ELSE 'present_ungraded'
        END AS vasc_status,
        -- Quantify → WHO 2022 grade
        CASE
            WHEN raw_vasc_quant IS NOT NULL AND regexp_matches(LOWER(TRIM(raw_vasc_quant)), '^<\\s*4')
                THEN 'focal'
            WHEN raw_vasc_quant IS NOT NULL AND (
                regexp_matches(LOWER(TRIM(raw_vasc_quant)), '^>?\\s*=?\\s*4')
                OR regexp_matches(LOWER(TRIM(raw_vasc_quant)), '^[5-9]')
                OR regexp_matches(LOWER(TRIM(raw_vasc_quant)), '^\\d{2,}'))
                THEN 'extensive'
            WHEN raw_vasc_quant IS NOT NULL AND regexp_matches(TRIM(raw_vasc_quant), '^\\d+$')
                AND TRY_CAST(TRIM(raw_vasc_quant) AS INTEGER) < 4 THEN 'focal'
            WHEN raw_vasc_quant IS NOT NULL AND regexp_matches(TRIM(raw_vasc_quant), '^\\d+$')
                AND TRY_CAST(TRIM(raw_vasc_quant) AS INTEGER) >= 4 THEN 'extensive'
            ELSE NULL
        END AS vasc_who_grade,
        TRY_CAST(regexp_extract(COALESCE(raw_vasc_quant,''), '(\d+)', 1) AS INTEGER) AS vessel_count,
        -- LVI normalization
        CASE
            WHEN LOWER(COALESCE(raw_lvi,'')) IN ('','null') THEN NULL
            WHEN LOWER(raw_lvi) IN ('x','present','preesent','identified','yes') THEN 'present_ungraded'
            WHEN LOWER(raw_lvi) IN ('focal','1 focus') THEN 'focal'
            WHEN LOWER(raw_lvi) IN ('extensive','extensivre','extensiver') THEN 'extensive'
            WHEN LOWER(raw_lvi) IN ('indeterminate','indeeterminate','indeterminent',
                                     'indetermiante','suspicious','c/a','n/s') THEN 'indeterminate'
            WHEN LOWER(raw_lvi) IN ('absent','no','none','negative','not identified') THEN 'absent'
            ELSE 'present_ungraded'
        END AS lvi_status,
        -- PNI normalization
        CASE
            WHEN LOWER(COALESCE(raw_pni,'')) IN ('','null') THEN NULL
            WHEN LOWER(raw_pni) IN ('x','present','identified','yes') THEN 'present_ungraded'
            WHEN LOWER(raw_pni) IN ('focal') THEN 'focal'
            WHEN LOWER(raw_pni) IN ('indeterminate','c/a') THEN 'indeterminate'
            WHEN LOWER(raw_pni) IN ('absent','no','none','negative','not identified') THEN 'absent'
            ELSE 'present_ungraded'
        END AS pni_status,
        -- Capsular invasion
        CASE
            WHEN LOWER(COALESCE(raw_capsular,'')) IN ('','null') THEN NULL
            WHEN LOWER(raw_capsular) IN ('x','present','identified','yes') THEN 'present'
            WHEN LOWER(raw_capsular) IN ('absent','no','none','negative') THEN 'absent'
            WHEN LOWER(raw_capsular) IN ('indeterminate','c/a') THEN 'indeterminate'
            ELSE 'present'
        END AS capsular_status
    FROM raw_invasion
),

per_patient AS (
    SELECT
        research_id,
        -- Vascular: worst grade, refined by WHO quantify
        MAX(CASE COALESCE(vasc_who_grade, vasc_status)
            WHEN 'extensive' THEN 5
            WHEN 'focal' THEN 3
            WHEN 'present_ungraded' THEN 2
            WHEN 'indeterminate' THEN 1
            WHEN 'absent' THEN 0
            ELSE -1
        END) AS vasc_rank,
        MAX(vessel_count) AS max_vessel_count,
        -- Override: if quantify says focal/extensive, that trumps raw text
        MAX(CASE WHEN vasc_who_grade IS NOT NULL THEN
            CASE vasc_who_grade WHEN 'extensive' THEN 5 WHEN 'focal' THEN 3 ELSE -1 END
            ELSE -1 END) AS vasc_who_rank,

        -- LVI: worst grade
        MAX(CASE lvi_status
            WHEN 'extensive' THEN 5 WHEN 'focal' THEN 3
            WHEN 'present_ungraded' THEN 2 WHEN 'indeterminate' THEN 1
            WHEN 'absent' THEN 0 ELSE -1 END) AS lvi_rank,

        -- PNI: worst grade
        MAX(CASE pni_status
            WHEN 'focal' THEN 3 WHEN 'present_ungraded' THEN 2
            WHEN 'indeterminate' THEN 1 WHEN 'absent' THEN 0 ELSE -1 END) AS pni_rank,

        -- Capsular
        MAX(CASE capsular_status
            WHEN 'present' THEN 2 WHEN 'indeterminate' THEN 1
            WHEN 'absent' THEN 0 ELSE -1 END) AS capsular_rank,

        COUNT(*) AS n_records
    FROM normalized
    WHERE vasc_status IS NOT NULL OR lvi_status IS NOT NULL
          OR pni_status IS NOT NULL OR capsular_status IS NOT NULL
    GROUP BY research_id
)

SELECT
    pp.research_id,
    -- Vascular: WHO quantify overrides raw text when available
    CASE GREATEST(pp.vasc_rank, pp.vasc_who_rank)
        WHEN 5 THEN 'extensive'
        WHEN 3 THEN 'focal'
        WHEN 2 THEN 'present_ungraded'
        WHEN 1 THEN 'indeterminate'
        WHEN 0 THEN 'absent'
        ELSE NULL
    END AS vascular_invasion_refined,
    CASE GREATEST(pp.vasc_rank, pp.vasc_who_rank)
        WHEN 5 THEN 'extensive (>=4 vessels)'
        WHEN 3 THEN 'focal (<4 vessels)'
        ELSE NULL
    END AS vascular_who_2022_grade,
    pp.max_vessel_count AS vessel_count,
    CASE GREATEST(pp.vasc_rank, pp.vasc_who_rank) WHEN 0 THEN FALSE WHEN -1 THEN NULL ELSE TRUE END AS vascular_positive,

    -- LVI
    CASE pp.lvi_rank
        WHEN 5 THEN 'extensive'
        WHEN 3 THEN 'focal'
        WHEN 2 THEN 'present_ungraded'
        WHEN 1 THEN 'indeterminate'
        WHEN 0 THEN 'absent'
        ELSE NULL
    END AS lvi_refined,
    CASE pp.lvi_rank WHEN 0 THEN FALSE WHEN -1 THEN NULL ELSE TRUE END AS lvi_positive,

    -- PNI
    CASE pp.pni_rank
        WHEN 3 THEN 'focal'
        WHEN 2 THEN 'present_ungraded'
        WHEN 1 THEN 'indeterminate'
        WHEN 0 THEN 'absent'
        ELSE NULL
    END AS pni_refined,
    CASE pp.pni_rank WHEN 0 THEN FALSE WHEN -1 THEN NULL ELSE TRUE END AS pni_positive,

    -- Capsular
    CASE pp.capsular_rank
        WHEN 2 THEN 'present'
        WHEN 1 THEN 'indeterminate'
        WHEN 0 THEN 'absent'
        ELSE NULL
    END AS capsular_invasion_refined,

    pp.n_records,
    'path_synoptic' AS source_category,
    1.0 AS source_reliability,
    CASE
        WHEN GREATEST(pp.vasc_rank, pp.vasc_who_rank) >= 3 THEN 0.92
        WHEN pp.vasc_rank >= 0 THEN 0.88
        ELSE 0.75
    END AS confidence,
    CURRENT_TIMESTAMP AS refined_at
FROM per_patient pp
ORDER BY pp.research_id;
"""


def build_ln_yield_sql() -> str:
    """Per-patient lymph node yield with levels and ratio."""
    return """
CREATE OR REPLACE TABLE extracted_ln_yield_v1 AS
WITH
raw_ln AS (
    SELECT
        research_id,
        tumor_1_ln_examined AS raw_examined,
        tumor_1_ln_involved AS raw_involved,
        tumor_1_ln_location AS raw_location,
        tumor_1_ln_examined_comment AS ln_comment,
        other_ln_dissection AS other_dissection,
        central_compartment_dissection,
        surg_date
    FROM path_synoptics
),

parsed AS (
    SELECT
        research_id,
        surg_date,
        raw_examined,
        raw_involved,
        raw_location,
        other_dissection,
        central_compartment_dissection,
        -- Parse examined (strip semicolons, cast)
        TRY_CAST(TRIM(REPLACE(COALESCE(raw_examined,''), ';', '')) AS INTEGER) AS ln_examined,
        -- Parse involved (x = positive flag, numeric = count)
        CASE
            WHEN LOWER(TRIM(COALESCE(raw_involved,''))) = 'x' THEN NULL
            ELSE TRY_CAST(TRIM(REPLACE(COALESCE(raw_involved,''), ';', '')) AS INTEGER)
        END AS ln_involved,
        LOWER(TRIM(COALESCE(raw_involved,''))) = 'x' AS ln_positive_flag_x,
        -- Location contains positive/examined counts like "0/1 perithyroidal"
        raw_location AS location_detail,
        -- Central dissection flag
        CASE
            WHEN central_compartment_dissection IS NOT NULL THEN TRUE
            WHEN LOWER(COALESCE(raw_location,'')) LIKE '%perithyroidal%'
                 OR LOWER(COALESCE(raw_location,'')) LIKE '%pretracheal%'
                 OR LOWER(COALESCE(raw_location,'')) LIKE '%paratracheal%'
                 OR LOWER(COALESCE(raw_location,'')) LIKE '%prelaryngeal%'
                 OR LOWER(COALESCE(raw_location,'')) LIKE '%delphian%'
                 OR LOWER(COALESCE(raw_location,'')) LIKE '%central neck%'
                 OR LOWER(COALESCE(raw_location,'')) LIKE '%pyramidal%'
                 OR LOWER(COALESCE(raw_location,'')) LIKE '%level 6%'
                 OR LOWER(COALESCE(raw_location,'')) LIKE '%level vi%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%central%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%level 6%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%level vi%'
                THEN TRUE
            ELSE FALSE
        END AS central_dissected,
        -- Lateral dissection flag
        CASE
            WHEN LOWER(COALESCE(other_dissection,'')) LIKE '%lateral%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%level ii%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%level iii%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%level iv%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%level v%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%jugular%'
                 OR LOWER(COALESCE(other_dissection,'')) LIKE '%neck dissection%'
                THEN TRUE
            ELSE FALSE
        END AS lateral_dissected
    FROM raw_ln
),

per_patient AS (
    SELECT
        research_id,
        SUM(ln_examined) AS total_examined,
        SUM(ln_involved) AS total_involved,
        BOOL_OR(ln_positive_flag_x) AS has_x_positive,
        BOOL_OR(central_dissected) AS central_dissected,
        BOOL_OR(lateral_dissected) AS lateral_dissected,
        STRING_AGG(DISTINCT location_detail, ' | ')
            FILTER (WHERE location_detail IS NOT NULL AND TRIM(location_detail) <> '') AS location_summary,
        COUNT(*) AS n_records,
        MAX(surg_date) AS latest_surg_date
    FROM parsed
    WHERE ln_examined IS NOT NULL OR ln_involved IS NOT NULL
          OR ln_positive_flag_x OR central_dissected OR lateral_dissected
    GROUP BY research_id
)

SELECT
    pp.research_id,
    pp.total_examined AS ln_total_examined,
    pp.total_involved AS ln_total_positive,
    CASE WHEN pp.total_examined > 0 AND pp.total_involved IS NOT NULL
         THEN ROUND(pp.total_involved::DOUBLE / pp.total_examined, 3)
         ELSE NULL
    END AS ln_ratio,
    CASE
        WHEN pp.total_involved IS NOT NULL AND pp.total_involved > 0 THEN TRUE
        WHEN pp.has_x_positive THEN TRUE
        WHEN pp.total_involved = 0 THEN FALSE
        ELSE NULL
    END AS ln_positive_flag,
    pp.central_dissected,
    pp.lateral_dissected,
    pp.location_summary AS ln_levels_raw,
    pp.n_records,
    'path_synoptic' AS source_category,
    1.0 AS source_reliability,
    CASE
        WHEN pp.total_examined IS NOT NULL AND pp.total_involved IS NOT NULL THEN 0.95
        WHEN pp.total_examined IS NOT NULL THEN 0.85
        ELSE 0.70
    END AS confidence,
    CURRENT_TIMESTAMP AS refined_at
FROM per_patient pp
ORDER BY pp.research_id;
"""


def build_ene_deepened_sql() -> str:
    """Deepen ENE from v3 with source hierarchy and grading."""
    return """
CREATE OR REPLACE TABLE extracted_ene_refined_v2 AS
WITH
-- Start from v1 (path_synoptic gold standard)
v1_ene AS (
    SELECT
        research_id,
        ene_status_refined,
        ene_levels,
        n_records,
        source_table,
        ene_positive,
        CASE ene_status_refined
            WHEN 'extensive' THEN 5
            WHEN 'present' THEN 4
            WHEN 'focal' THEN 3
            WHEN 'present_ungraded' THEN 2
            WHEN 'indeterminate' THEN 1
            WHEN 'absent' THEN 0
            ELSE -1
        END AS grade_rank
    FROM extracted_ene_refined_v1
),

-- NLP from op_notes (source reliability 0.9)
op_note_ene AS (
    SELECT
        n.research_id,
        CASE
            WHEN regexp_matches(LOWER(n.note_text),
                'extranodal\\s+extension\\s*(?:is\\s+)?(present|identified|seen)')
                THEN 'present'
            WHEN regexp_matches(LOWER(n.note_text),
                'no\\s+(?:evidence\\s+of\\s+)?extranodal\\s+extension')
                THEN 'absent'
            WHEN regexp_matches(LOWER(n.note_text),
                'extranodal\\s+extension\\s*:\\s*(?:absent|no|none|not\\s+identified)')
                THEN 'absent'
            ELSE NULL
        END AS op_note_ene_status,
        CASE
            WHEN LOWER(n.note_text) LIKE '%extensive%extranodal%'
                 OR LOWER(n.note_text) LIKE '%extranodal%extensive%'
                THEN 'extensive'
            WHEN LOWER(n.note_text) LIKE '%focal%extranodal%'
                 OR LOWER(n.note_text) LIKE '%extranodal%focal%'
                THEN 'focal'
            ELSE NULL
        END AS op_note_ene_grade,
        n.note_type,
        ROW_NUMBER() OVER (
            PARTITION BY n.research_id
            ORDER BY CASE n.note_type
                WHEN 'op_note' THEN 1 WHEN 'endocrine_note' THEN 2
                WHEN 'dc_sum' THEN 3 ELSE 4 END
        ) AS rn
    FROM clinical_notes_long n
    WHERE n.note_type IN ('op_note', 'endocrine_note', 'dc_sum')
      AND (LOWER(n.note_text) LIKE '%extranodal%' OR LOWER(n.note_text) LIKE '%extracapsular%'
           OR LOWER(n.note_text) LIKE '%ene%')
      AND NOT (LOWER(n.note_text) LIKE '%risk%' AND n.note_type = 'h_p')
),

best_op_note AS (
    SELECT research_id, op_note_ene_status, op_note_ene_grade, note_type
    FROM op_note_ene WHERE rn = 1 AND op_note_ene_status IS NOT NULL
),

-- Union spine
all_patients AS (
    SELECT DISTINCT research_id FROM v1_ene
    UNION
    SELECT DISTINCT research_id FROM best_op_note
)

SELECT
    a.research_id,
    -- Path synoptic is authoritative; op note fills gaps or deepens grading
    COALESCE(v1.ene_status_refined,
             bon.op_note_ene_status) AS ene_status_refined,
    CASE
        WHEN v1.grade_rank >= 3 THEN v1.ene_status_refined
        WHEN v1.grade_rank = 2 AND bon.op_note_ene_grade IS NOT NULL THEN bon.op_note_ene_grade
        WHEN v1.grade_rank IS NULL AND bon.op_note_ene_grade IS NOT NULL THEN bon.op_note_ene_grade
        WHEN v1.ene_status_refined IS NOT NULL THEN v1.ene_status_refined
        WHEN bon.op_note_ene_status IS NOT NULL THEN COALESCE(bon.op_note_ene_grade, bon.op_note_ene_status)
        ELSE NULL
    END AS ene_grade_refined,
    v1.ene_levels,
    COALESCE(v1.n_records, 0) + CASE WHEN bon.research_id IS NOT NULL THEN 1 ELSE 0 END AS n_sources,
    CASE
        WHEN v1.research_id IS NOT NULL AND bon.research_id IS NOT NULL THEN 'path_synoptic+op_note'
        WHEN v1.research_id IS NOT NULL THEN 'path_synoptic'
        WHEN bon.research_id IS NOT NULL THEN 'op_note'
        ELSE 'none'
    END AS source_chain,
    CASE
        WHEN v1.ene_positive IS TRUE THEN TRUE
        WHEN bon.op_note_ene_status = 'present' THEN TRUE
        WHEN v1.ene_status_refined = 'absent' AND bon.op_note_ene_status IS NULL THEN FALSE
        WHEN v1.ene_status_refined = 'absent' AND bon.op_note_ene_status = 'absent' THEN FALSE
        ELSE NULL
    END AS ene_positive,
    -- Concordance
    CASE
        WHEN v1.research_id IS NOT NULL AND bon.research_id IS NOT NULL THEN
            CASE
                WHEN (v1.ene_positive IS TRUE AND bon.op_note_ene_status = 'present') THEN 'concordant_positive'
                WHEN (v1.ene_status_refined = 'absent' AND bon.op_note_ene_status = 'absent') THEN 'concordant_negative'
                WHEN (v1.ene_positive IS TRUE AND bon.op_note_ene_status = 'absent') THEN 'discordant_path_pos_op_neg'
                WHEN (v1.ene_status_refined = 'absent' AND bon.op_note_ene_status = 'present') THEN 'discordant_path_neg_op_pos'
                ELSE 'partial'
            END
        ELSE 'single_source'
    END AS concordance_status,
    CASE
        WHEN v1.research_id IS NOT NULL AND bon.research_id IS NOT NULL THEN 0.95
        WHEN v1.research_id IS NOT NULL THEN 0.90
        WHEN bon.research_id IS NOT NULL THEN 0.80
        ELSE 0.0
    END AS confidence,
    CURRENT_TIMESTAMP AS refined_at
FROM all_patients a
LEFT JOIN v1_ene v1 ON a.research_id = v1.research_id
LEFT JOIN best_op_note bon ON a.research_id = bon.research_id
ORDER BY a.research_id;
"""


def build_staging_details_sql() -> str:
    """Consolidated staging details table: margins + invasion + LN + ENE."""
    return """
CREATE OR REPLACE TABLE extracted_staging_details_refined_v1 AS
SELECT
    COALESCE(m.research_id, ip.research_id, ly.research_id, e.research_id) AS research_id,

    -- Margins
    m.margin_status_refined,
    m.margin_r_classification,
    m.closest_margin_mm,
    m.margin_with_gross_ete,
    m.source_category AS margin_source,
    m.confidence AS margin_confidence,

    -- Vascular invasion (WHO 2022)
    ip.vascular_invasion_refined,
    ip.vascular_who_2022_grade,
    ip.vessel_count,
    ip.vascular_positive,

    -- LVI
    ip.lvi_refined,
    ip.lvi_positive,

    -- PNI
    ip.pni_refined,
    ip.pni_positive,

    -- Capsular
    ip.capsular_invasion_refined,
    ip.source_category AS invasion_source,
    ip.confidence AS invasion_confidence,

    -- LN yield
    ly.ln_total_examined,
    ly.ln_total_positive,
    ly.ln_ratio,
    ly.ln_positive_flag,
    ly.central_dissected,
    ly.lateral_dissected,
    ly.ln_levels_raw,
    ly.source_category AS ln_source,
    ly.confidence AS ln_confidence,

    -- ENE
    e.ene_status_refined,
    e.ene_grade_refined,
    e.ene_positive,
    e.ene_levels,
    e.source_chain AS ene_source,
    e.concordance_status AS ene_concordance,
    e.confidence AS ene_confidence,

    CURRENT_TIMESTAMP AS refined_at
FROM extracted_margins_refined_v1 m
FULL OUTER JOIN extracted_invasion_profile_v1 ip ON m.research_id = ip.research_id
FULL OUTER JOIN extracted_ln_yield_v1 ly ON COALESCE(m.research_id, ip.research_id) = ly.research_id
FULL OUTER JOIN extracted_ene_refined_v2 e ON COALESCE(m.research_id, ip.research_id, ly.research_id) = e.research_id
ORDER BY COALESCE(m.research_id, ip.research_id, ly.research_id, e.research_id);
"""


def build_master_clinical_v5_sql() -> str:
    """patient_refined_master_clinical_v5 — v4 + Phase 6 staging details."""
    return """
CREATE OR REPLACE TABLE patient_refined_master_clinical_v5 AS
SELECT
    v4.*,

    -- Phase 6: Margins
    sd.margin_r_classification,
    sd.margin_with_gross_ete,
    sd.margin_source,
    sd.margin_confidence,

    -- Phase 6: Vascular (WHO 2022)
    sd.vascular_who_2022_grade,
    sd.vessel_count AS vascular_vessel_count,
    sd.vascular_positive,

    -- Phase 6: LVI (replace v3 simple flag)
    sd.lvi_positive,

    -- Phase 6: PNI (replace v3 simple flag)
    sd.pni_refined AS pni_refined_v6,
    sd.pni_positive,

    -- Phase 6: Capsular
    sd.capsular_invasion_refined AS capsular_invasion_v6,

    -- Phase 6: LN yield
    sd.ln_total_examined,
    sd.ln_total_positive,
    sd.ln_ratio,
    sd.ln_positive_flag AS ln_positive_v6,
    sd.central_dissected AS ln_central_dissected,
    sd.lateral_dissected AS ln_lateral_dissected,
    sd.ln_levels_raw,
    sd.ln_source,
    sd.ln_confidence,

    -- Phase 6: ENE deepened
    sd.ene_grade_refined AS ene_grade_v6,
    sd.ene_source AS ene_source_v6,
    sd.ene_concordance AS ene_concordance_v6,
    sd.ene_confidence AS ene_confidence_v6

FROM patient_refined_master_clinical_v4 v4
LEFT JOIN extracted_staging_details_refined_v1 sd ON v4.research_id = sd.research_id
ORDER BY v4.research_id;
"""


def build_margins_by_source_sql() -> str:
    """Summary view: margins breakdown by source and R-classification."""
    return """
CREATE OR REPLACE TABLE vw_margins_by_source AS
SELECT
    margin_r_classification,
    margin_status_refined,
    COUNT(*) AS n_patients,
    AVG(closest_margin_mm) FILTER (WHERE closest_margin_mm IS NOT NULL) AS avg_margin_mm,
    MIN(closest_margin_mm) FILTER (WHERE closest_margin_mm IS NOT NULL) AS min_margin_mm,
    COUNT(closest_margin_mm) AS n_with_distance,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM extracted_margins_refined_v1
GROUP BY margin_r_classification, margin_status_refined
ORDER BY n_patients DESC;
"""


def build_invasion_profile_summary_sql() -> str:
    """Summary view: invasion profile rates."""
    return """
CREATE OR REPLACE TABLE vw_invasion_profile AS
WITH total AS (SELECT COUNT(DISTINCT research_id) AS n FROM extracted_invasion_profile_v1)
SELECT
    'vascular_invasion' AS entity,
    vascular_invasion_refined AS grade,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / (SELECT n FROM total), 1) AS pct
FROM extracted_invasion_profile_v1
WHERE vascular_invasion_refined IS NOT NULL
GROUP BY vascular_invasion_refined
UNION ALL
SELECT
    'lvi' AS entity,
    lvi_refined AS grade,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / (SELECT n FROM total), 1) AS pct
FROM extracted_invasion_profile_v1
WHERE lvi_refined IS NOT NULL
GROUP BY lvi_refined
UNION ALL
SELECT
    'pni' AS entity,
    pni_refined AS grade,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / (SELECT n FROM total), 1) AS pct
FROM extracted_invasion_profile_v1
WHERE pni_refined IS NOT NULL
GROUP BY pni_refined
ORDER BY entity, n DESC;
"""


def build_ln_yield_summary_sql() -> str:
    """Summary view: LN yield statistics."""
    return """
CREATE OR REPLACE TABLE vw_ln_yield_summary AS
SELECT
    COUNT(*) AS total_patients,
    COUNT(ln_total_examined) AS with_examined,
    COUNT(ln_total_positive) AS with_positive,
    AVG(ln_total_examined) FILTER (WHERE ln_total_examined > 0) AS avg_examined,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ln_total_examined)
        FILTER (WHERE ln_total_examined > 0) AS median_examined,
    AVG(ln_total_positive) FILTER (WHERE ln_total_positive IS NOT NULL) AS avg_positive,
    AVG(ln_ratio) FILTER (WHERE ln_ratio IS NOT NULL) AS avg_ratio,
    COUNT(CASE WHEN ln_positive_flag IS TRUE THEN 1 END) AS n_ln_positive,
    COUNT(CASE WHEN central_dissected THEN 1 END) AS n_central_dissected,
    COUNT(CASE WHEN lateral_dissected THEN 1 END) AS n_lateral_dissected,
    ROUND(100.0 * COUNT(CASE WHEN ln_positive_flag IS TRUE THEN 1 END) / COUNT(*), 1) AS ln_positive_rate_pct
FROM extracted_ln_yield_v1;
"""


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
_PHASE6_STEPS = [
    ("margins", build_margins_refined_sql),
    ("invasion_profile", build_invasion_profile_sql),
    ("ln_yield", build_ln_yield_sql),
    ("ene_deepened", build_ene_deepened_sql),
    ("staging_details", build_staging_details_sql),
    ("master_v5", build_master_clinical_v5_sql),
    ("vw_margins_by_source", build_margins_by_source_sql),
    ("vw_invasion_profile", build_invasion_profile_summary_sql),
    ("vw_ln_yield_summary", build_ln_yield_summary_sql),
]


def audit_and_refine_phase6(
    con,
    variables: list[str] | None = None,
    verbose: bool = True,
) -> dict[str, dict]:
    """
    Run the full Phase 6 staging refinement pipeline.
    Returns dict mapping step name -> {table, rows, ...}.
    """
    steps = _PHASE6_STEPS
    if variables:
        steps = [(n, fn) for n, fn in _PHASE6_STEPS if n in variables or "all" in variables]

    results = {}

    for step_name, sql_builder in steps:
        if verbose:
            print(f"\n{'='*70}")
            print(f"  Phase 6: {step_name}")
            print(f"{'='*70}")

        sql = sql_builder()
        table_name = _extract_table_name(sql)

        try:
            con.execute(sql)
            n = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if verbose:
                print(f"  {table_name}: {n} rows")
            results[step_name] = {"table": table_name, "rows": n, "status": "ok"}

            if step_name == "margins":
                results[step_name].update(_margins_stats(con))
            elif step_name == "invasion_profile":
                results[step_name].update(_invasion_stats(con))
            elif step_name == "ln_yield":
                results[step_name].update(_ln_stats(con))
            elif step_name == "ene_deepened":
                results[step_name].update(_ene_stats(con))

        except Exception as e:
            if verbose:
                print(f"  [ERROR] {step_name}: {e}")
            results[step_name] = {"error": str(e), "status": "failed"}

    return results


def _extract_table_name(sql: str) -> str:
    """Extract table/view name from CREATE OR REPLACE TABLE/VIEW statement."""
    import re as _re
    m = _re.search(r"CREATE\s+OR\s+REPLACE\s+(?:TABLE|VIEW)\s+(\w+)", sql, _re.I)
    return m.group(1) if m else "unknown"


def _margins_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN margin_r_classification = 'R0' THEN 1 END) AS r0,
            COUNT(CASE WHEN margin_r_classification = 'R0_close' THEN 1 END) AS r0_close,
            COUNT(CASE WHEN margin_r_classification = 'R1' THEN 1 END) AS r1,
            COUNT(CASE WHEN margin_r_classification = 'R2' THEN 1 END) AS r2,
            COUNT(CASE WHEN margin_r_classification = 'Rx' THEN 1 END) AS rx,
            AVG(closest_margin_mm) FILTER (WHERE closest_margin_mm IS NOT NULL) AS avg_dist,
            COUNT(closest_margin_mm) AS n_with_distance
        FROM extracted_margins_refined_v1
    """).fetchone()
    return {
        "R0": row[1], "R0_close": row[2], "R1": row[3], "R2": row[4], "Rx": row[5],
        "avg_margin_mm": round(row[6], 2) if row[6] else None,
        "n_with_distance": row[7],
    }


def _invasion_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN vascular_positive THEN 1 END) AS vasc_pos,
            COUNT(CASE WHEN vascular_who_2022_grade = 'focal (<4 vessels)' THEN 1 END) AS vasc_focal,
            COUNT(CASE WHEN vascular_who_2022_grade = 'extensive (>=4 vessels)' THEN 1 END) AS vasc_ext,
            COUNT(CASE WHEN lvi_positive THEN 1 END) AS lvi_pos,
            COUNT(CASE WHEN pni_positive THEN 1 END) AS pni_pos,
            COUNT(CASE WHEN capsular_invasion_refined = 'present' THEN 1 END) AS capsular_pos
        FROM extracted_invasion_profile_v1
    """).fetchone()
    return {
        "vasc_positive": row[1], "vasc_focal": row[2], "vasc_extensive": row[3],
        "lvi_positive": row[4], "pni_positive": row[5], "capsular_positive": row[6],
    }


def _ln_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN ln_positive_flag THEN 1 END) AS ln_pos,
            AVG(ln_total_examined) FILTER (WHERE ln_total_examined > 0) AS avg_examined,
            AVG(ln_ratio) FILTER (WHERE ln_ratio IS NOT NULL) AS avg_ratio,
            COUNT(CASE WHEN central_dissected THEN 1 END) AS n_central,
            COUNT(CASE WHEN lateral_dissected THEN 1 END) AS n_lateral
        FROM extracted_ln_yield_v1
    """).fetchone()
    return {
        "ln_positive": row[1],
        "avg_examined": round(row[2], 1) if row[2] else None,
        "avg_ratio": round(row[3], 3) if row[3] else None,
        "n_central_dissected": row[4],
        "n_lateral_dissected": row[5],
    }


def _ene_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN ene_positive THEN 1 END) AS ene_pos,
            COUNT(CASE WHEN ene_grade_refined = 'extensive' THEN 1 END) AS extensive,
            COUNT(CASE WHEN ene_grade_refined = 'focal' THEN 1 END) AS focal,
            COUNT(CASE WHEN ene_grade_refined = 'present_ungraded' THEN 1 END) AS ungraded,
            COUNT(CASE WHEN concordance_status = 'concordant_positive' THEN 1 END) AS concordant_pos,
            COUNT(CASE WHEN concordance_status LIKE 'discordant%' THEN 1 END) AS discordant,
            COUNT(CASE WHEN source_chain = 'path_synoptic+op_note' THEN 1 END) AS dual_source
        FROM extracted_ene_refined_v2
    """).fetchone()
    return {
        "ene_positive": row[1], "extensive": row[2], "focal": row[3],
        "ungraded": row[4], "concordant_positive": row[5],
        "discordant": row[6], "dual_source": row[7],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Phase 6 Source-Linked Staging Refinement")
    parser.add_argument("--variable", default="all",
                        choices=["all", "margins", "invasion_profile", "ln_yield",
                                 "ene_deepened", "staging_details", "master_v5",
                                 "vw_margins_by_source", "vw_invasion_profile",
                                 "vw_ln_yield_summary"],
                        help="Which step to run")
    parser.add_argument("--md", action="store_true", default=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default="notes_extraction")
    args = parser.parse_args()

    use_md = not args.local

    if args.dry_run:
        print(f"[dry-run] Would run phase6 step={args.variable}")
        return

    con = _get_connection(use_md)

    variables = None if args.variable == "all" else [args.variable]
    results = audit_and_refine_phase6(con, variables=variables, verbose=True)

    out_dir = PROJECT_ROOT / args.output_dir
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    lines = ["# Phase 6 Source-Linked Staging Refinement Report",
             f"_Generated: {timestamp}_", ""]
    for step, rpt in results.items():
        lines.append(f"## {step}")
        for k, v in rpt.items():
            lines.append(f"- **{k}:** {v}")
        lines.append("")

    report_path = out_dir / f"phase6_staging_refinement_{timestamp}.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[phase6] Report saved: {report_path}")

    con.close()


if __name__ == "__main__":
    main()
