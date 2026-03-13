"""
Extraction Audit Engine v8 — Phase 10: Source-Linked Recovery
=============================================================
Extends v7 with:
  1. MarginR0RecoveryParser    – recover R0 from 334 cancer patients w/ NULL margin
  2. InvasionGradingResolver   – resolve ~3,255 present_ungraded vascular/LVI via op note NLP
  3. LateralNeckDissectionDetector – expand lateral neck from 25 → ~250 via op note + levels
  4. MultiTumorAggregator      – aggregate tumor 2–5 invasion/margin/ETE fields
  5. MICEImputer               – multiple imputation for 65% missing LN/margin/size

Source hierarchy: path_synoptic (1.0) > op_note (0.85) > multi_tumor (0.80) > nlp (0.75)

Usage:
    from notes_extraction.extraction_audit_engine_v8 import audit_and_refine_phase10
    results = audit_and_refine_phase10(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v8.py --md --variable all
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

PHASE10_SOURCE_HIERARCHY = {
    "path_synoptic": 1.0,
    "path_synoptic_multi_tumor": 0.95,
    "op_note": 0.85,
    "multi_tumor_aggregate": 0.80,
    "nlp_op_note": 0.75,
    "nlp_dc_sum": 0.65,
    "structured_inferred": 0.60,
    "mice_imputed": 0.40,
}

# ---------------------------------------------------------------------------
# 1. MarginR0RecoveryParser — R0 from NULL-margin cancer patients
# ---------------------------------------------------------------------------
_MARGIN_NEG_NLP = [
    re.compile(r"\bmargin(?:s)?\s+(?:are\s+)?(?:negative|free|clear|uninvolved)\b", re.I),
    re.compile(r"\bR0\s+resection\b", re.I),
    re.compile(r"\bno\s+(?:tumor\s+)?(?:at|involving|reaching)\s+(?:the\s+)?margin\b", re.I),
    re.compile(r"\b(?:inked\s+)?margin(?:s)?\s+(?:are\s+)?negative\b", re.I),
    re.compile(r"\bwidely\s+(?:clear|free)\s+margin\b", re.I),
    re.compile(r"\bmargin\s+(?:>|greater\s+than)\s*\d+\s*mm\b", re.I),
]

_MARGIN_POS_NLP = [
    re.compile(r"\bmargin(?:s)?\s+(?:are\s+)?(?:positive|involved|invaded)\b", re.I),
    re.compile(r"\btumor\s+(?:at|extends?\s+to|reaching)\s+(?:the\s+)?(?:inked\s+)?margin\b", re.I),
    re.compile(r"\bR1\s+resection\b", re.I),
    re.compile(r"\bink(?:ed)?\s+margin\s+positive\b", re.I),
    re.compile(r"\bmargin\s+(?:<|less\s+than)\s*0\.?1\s*mm\b", re.I),
]

_MARGIN_CLOSE_NLP = [
    re.compile(r"\b(?:close|near|approaching)\s+margin\b", re.I),
    re.compile(r"\bmargin\s+(?:<|less\s+than)\s*[12]\s*mm\b", re.I),
]

_MARGIN_DISTANCE_NLP = re.compile(
    r"\bclosest\s+margin\s*(?:is\s+)?(?:of\s+)?(\d+(?:\.\d+)?)\s*(?:mm|millimeter)", re.I
)

_CONSENT_BOILERPLATE_RE = re.compile(
    r"\b(?:risk(?:s)?|complication(?:s)?|include|including|consent|informed)\b.*"
    r"\b(?:bleeding|infection|scar|hoarse|numbness)\b", re.I | re.DOTALL
)


class MarginR0RecoveryParser:
    """Recover R0/margin status from op notes for cancer patients with NULL margin."""

    def classify_from_note(self, note_text: str, note_type: str) -> dict | None:
        if not note_text or len(note_text) < 20:
            return None
        text = str(note_text)

        if _CONSENT_BOILERPLATE_RE.search(text[:500]):
            text_clean = text[500:]
        else:
            text_clean = text

        if not text_clean.strip():
            return None

        dist_match = _MARGIN_DISTANCE_NLP.search(text_clean)
        distance_mm = float(dist_match.group(1)) if dist_match else None

        for pat in _MARGIN_NEG_NLP:
            if pat.search(text_clean):
                return {
                    "margin_status_recovered": "negative",
                    "r_classification_recovered": "R0" if not distance_mm or distance_mm >= 1.0 else "R0_close",
                    "closest_margin_mm": distance_mm,
                    "source": f"nlp_{note_type}",
                    "confidence": 0.85 if note_type == "op_note" else 0.70,
                }

        for pat in _MARGIN_CLOSE_NLP:
            if pat.search(text_clean):
                return {
                    "margin_status_recovered": "close",
                    "r_classification_recovered": "R0_close",
                    "closest_margin_mm": distance_mm,
                    "source": f"nlp_{note_type}",
                    "confidence": 0.80 if note_type == "op_note" else 0.65,
                }

        for pat in _MARGIN_POS_NLP:
            if pat.search(text_clean):
                return {
                    "margin_status_recovered": "positive",
                    "r_classification_recovered": "R1",
                    "closest_margin_mm": distance_mm,
                    "source": f"nlp_{note_type}",
                    "confidence": 0.85 if note_type == "op_note" else 0.70,
                }

        return None


# ---------------------------------------------------------------------------
# 2. InvasionGradingResolver — resolve present_ungraded vascular/LVI
# ---------------------------------------------------------------------------
_VASC_FOCAL_NLP = [
    re.compile(r"\bfocal\s+(?:vascular\s+(?:space\s+)?)?invasion\b", re.I),
    re.compile(r"\b(?:rare|single|isolated)\s+(?:focus|foci)\s+(?:of\s+)?(?:vascular|angio)\s*invasion\b", re.I),
    re.compile(r"\b(?:1|2|3)\s+(?:foci?\s+of\s+)?(?:vascular|angio)\s*invasion\b", re.I),
    re.compile(r"\bvascular\s+(?:space\s+)?invasion\s*(?::\s*)?focal\b", re.I),
    re.compile(r"\b(?:limited|minimal)\s+(?:vascular|angio)\s*invasion\b", re.I),
]

_VASC_EXTENSIVE_NLP = [
    re.compile(r"\b(?:extensive|widespread|prominent|multifocal)\s+(?:vascular\s+(?:space\s+)?)?invasion\b", re.I),
    re.compile(r"\b(?:vascular|angio)\s*invasion\s*(?::\s*)?(?:extensive|multifocal)\b", re.I),
    re.compile(r"\b(?:[4-9]|\d{2,})\s+(?:foci?\s+of\s+)?(?:vascular|angio)\s*invasion\b", re.I),
    re.compile(r"\b(?:>|>=|greater\s+than)\s*4\s+(?:foci|vessels)\b", re.I),
]

_LVI_FOCAL_NLP = [
    re.compile(r"\bfocal\s+lymph(?:o)?vascular\s+(?:space\s+)?invasion\b", re.I),
    re.compile(r"\blymphovascular\s+invasion\s*(?::\s*)?focal\b", re.I),
]

_LVI_EXTENSIVE_NLP = [
    re.compile(r"\b(?:extensive|prominent|multifocal)\s+lymph(?:o)?vascular\s+(?:space\s+)?invasion\b", re.I),
    re.compile(r"\blymphovascular\s+invasion\s*(?::\s*)?(?:extensive|multifocal)\b", re.I),
]

_VESSEL_COUNT_NLP = re.compile(
    r"\b(\d+)\s+(?:foci?\s+of\s+)?(?:vascular|angio)\s*(?:space\s+)?invasion\b", re.I
)


class InvasionGradingResolver:
    """Resolve present_ungraded vascular/LVI via op note NLP and quantify fields."""

    def grade_from_note(self, note_text: str, entity: str) -> dict | None:
        if not note_text or len(note_text) < 20:
            return None
        text = str(note_text)

        if _CONSENT_BOILERPLATE_RE.search(text[:500]):
            text = text[500:]

        if entity == "vascular":
            count_m = _VESSEL_COUNT_NLP.search(text)
            if count_m:
                count = int(count_m.group(1))
                grade = "focal" if count < 4 else "extensive"
                return {"grade": grade, "vessel_count": count, "source": "nlp_op_note", "confidence": 0.80}

            for pat in _VASC_EXTENSIVE_NLP:
                if pat.search(text):
                    return {"grade": "extensive", "vessel_count": None, "source": "nlp_op_note", "confidence": 0.75}
            for pat in _VASC_FOCAL_NLP:
                if pat.search(text):
                    return {"grade": "focal", "vessel_count": None, "source": "nlp_op_note", "confidence": 0.75}

        elif entity == "lvi":
            for pat in _LVI_EXTENSIVE_NLP:
                if pat.search(text):
                    return {"grade": "extensive", "vessel_count": None, "source": "nlp_op_note", "confidence": 0.75}
            for pat in _LVI_FOCAL_NLP:
                if pat.search(text):
                    return {"grade": "focal", "vessel_count": None, "source": "nlp_op_note", "confidence": 0.75}

        return None

    def grade_from_quantify(self, quantify_val: str) -> dict | None:
        if not quantify_val:
            return None
        val = str(quantify_val).strip().lower()
        if val in ("", "x", "n/s", "not specified"):
            return None

        if re.match(r"^<\s*4", val):
            return {"grade": "focal", "vessel_count": 3, "source": "quantify_field", "confidence": 0.95}
        if re.match(r"^(>\s*=?\s*4|>=\s*4|>\s*4)", val):
            return {"grade": "extensive", "vessel_count": 4, "source": "quantify_field", "confidence": 0.95}
        if re.match(r"^<\s*4\s*vessels", val):
            return {"grade": "focal", "vessel_count": 3, "source": "quantify_field", "confidence": 0.95}

        m = re.match(r"^(\d+)", val)
        if m:
            count = int(m.group(1))
            if 0 < count < 100:
                grade = "focal" if count < 4 else "extensive"
                return {"grade": grade, "vessel_count": count, "source": "quantify_field", "confidence": 0.95}

        return None


# ---------------------------------------------------------------------------
# 3. LateralNeckDissectionDetector
# ---------------------------------------------------------------------------
_LATERAL_DISSECTION_NLP = [
    re.compile(r"\blateral\s+neck\s+dissection\b", re.I),
    re.compile(r"\bmodified\s+radical\s+neck\s+dissection\b", re.I),
    re.compile(r"\bselective\s+neck\s+dissection\b", re.I),
    re.compile(r"\b(?:right|left|bilateral)\s+(?:lateral\s+)?(?:neck|cervical)\s+dissection\b", re.I),
    re.compile(r"\b(?:level|levels)\s+(?:II|III|IV|V|2|3|4|5)(?:\s*[-–]\s*(?:II|III|IV|V|2|3|4|5))?\s+(?:dissection|clearance)\b", re.I),
    re.compile(r"\bcompartment\s+(?:II|III|IV|V|2|3|4|5)\b", re.I),
    re.compile(r"\bjugular\s+(?:chain\s+)?(?:dissection|nodes?|lymphadenectomy)\b", re.I),
    re.compile(r"\bfunctional\s+neck\s+dissection\b", re.I),
    re.compile(r"\bradical\s+neck\s+dissection\b", re.I),
]

_LATERAL_LEVELS_RE = re.compile(
    r"\b(?:level\s+)?([IViv]+[abAB]?|[2-5][abAB]?)\b", re.I
)

_LEVEL_NORM = {
    "ii": "II", "2": "II", "iia": "IIA", "2a": "IIA", "iib": "IIB", "2b": "IIB",
    "iii": "III", "3": "III", "iv": "IV", "4": "IV",
    "iva": "IVA", "4a": "IVA", "ivb": "IVB", "4b": "IVB",
    "v": "V", "5": "V", "va": "VA", "5a": "VA", "vb": "VB", "5b": "VB",
}

_LATERAL_LEVEL_SET = {"II", "IIA", "IIB", "III", "IV", "IVA", "IVB", "V", "VA", "VB"}

_SIDE_RE = re.compile(r"\b(right|left|bilateral)\b", re.I)


class LateralNeckDissectionDetector:
    """Detect lateral neck dissections from structured fields and op notes."""

    def detect_from_levels(self, level_examined: str | None, other_dissection: str | None) -> dict | None:
        combined = " ".join(filter(None, [str(level_examined or ""), str(other_dissection or "")]))
        if not combined.strip():
            return None

        levels = set()
        for m in _LATERAL_LEVELS_RE.finditer(combined):
            norm = _LEVEL_NORM.get(m.group(1).lower())
            if norm and norm in _LATERAL_LEVEL_SET:
                levels.add(norm)

        if not levels:
            if any(kw in combined.lower() for kw in ["lateral", "jugular", "modified radical"]):
                return {
                    "lateral_dissection_detected": True,
                    "levels_identified": None,
                    "side": self._extract_side(combined),
                    "source": "structured_text",
                    "confidence": 0.80,
                }
            return None

        return {
            "lateral_dissection_detected": True,
            "levels_identified": sorted(levels),
            "side": self._extract_side(combined),
            "source": "structured_levels",
            "confidence": 0.90,
        }

    def detect_from_note(self, note_text: str) -> dict | None:
        if not note_text or len(note_text) < 20:
            return None
        text = str(note_text)

        if _CONSENT_BOILERPLATE_RE.search(text[:500]):
            text = text[500:]

        for pat in _LATERAL_DISSECTION_NLP:
            m = pat.search(text)
            if m:
                start = max(0, m.start() - 100)
                end = min(len(text), m.end() + 200)
                ctx = text[start:end]

                levels = set()
                for lm in _LATERAL_LEVELS_RE.finditer(ctx):
                    norm = _LEVEL_NORM.get(lm.group(1).lower())
                    if norm and norm in _LATERAL_LEVEL_SET:
                        levels.add(norm)

                return {
                    "lateral_dissection_detected": True,
                    "levels_identified": sorted(levels) if levels else None,
                    "side": self._extract_side(ctx),
                    "source": "nlp_op_note",
                    "confidence": 0.80,
                }

        return None

    @staticmethod
    def _extract_side(text: str) -> str | None:
        m = _SIDE_RE.search(text)
        return m.group(1).lower() if m else None


# ---------------------------------------------------------------------------
# 4. MultiTumorAggregator
# ---------------------------------------------------------------------------
class MultiTumorAggregator:
    """Aggregate worst-case invasion/margin/ETE across tumors 1–5."""

    INVASION_HIERARCHY = {"extensive": 4, "focal": 3, "present_ungraded": 2, "present": 2,
                          "x": 2, "absent": 1, "negative": 1, "no": 1, "none": 1}
    MARGIN_HIERARCHY = {"involved": 3, "positive": 3, "x": 3, "close": 2, "negative": 1, "free": 1}
    ETE_HIERARCHY = {"gross": 4, "extensive": 4, "microscopic": 3, "minimal": 3,
                     "present": 2, "x": 2, "none": 1, "no": 1, "absent": 1, "negative": 1}

    def aggregate(self, row: dict) -> dict:
        worst_angio = self._worst_across_tumors(row, "angioinvasion", self.INVASION_HIERARCHY)
        worst_margin = self._worst_across_tumors(row, "margin_status", self.MARGIN_HIERARCHY)
        worst_ete = self._worst_across_tumors(row, "extrathyroidal_extension", self.ETE_HIERARCHY)
        n_tumors = sum(1 for i in range(1, 6) if row.get(f"tumor_{i}_histologic_type"))
        has_multitumor = n_tumors > 1

        worst_quantify = None
        for i in range(1, 6):
            q = row.get(f"tumor_{i}_angioinvasion_quantify")
            if q and str(q).strip().lower() not in ("", "x", "n/s", "not specified"):
                val = self._parse_count(str(q))
                if val is not None:
                    worst_quantify = max(worst_quantify or 0, val)

        return {
            "n_tumors": n_tumors,
            "has_multitumor_data": has_multitumor,
            "worst_angioinvasion": worst_angio,
            "worst_margin_status": worst_margin,
            "worst_ete": worst_ete,
            "worst_vessel_count": worst_quantify,
            "worst_angio_who_grade": (
                "focal" if worst_quantify and worst_quantify < 4
                else "extensive" if worst_quantify and worst_quantify >= 4
                else None
            ),
        }

    def _worst_across_tumors(self, row: dict, field_suffix: str, hierarchy: dict) -> str | None:
        worst = None
        worst_rank = 0
        for i in range(1, 6):
            val = row.get(f"tumor_{i}_{field_suffix}")
            if val:
                norm = str(val).strip().lower()
                rank = hierarchy.get(norm, 0)
                if rank > worst_rank:
                    worst_rank = rank
                    worst = norm
        return worst

    @staticmethod
    def _parse_count(raw: str) -> int | None:
        raw = raw.strip().lower()
        if re.match(r"^<\s*4", raw):
            return 3
        m_gte = re.match(r"^>?\s*=?\s*(\d+)", raw)
        if m_gte:
            return int(m_gte.group(1))
        m_num = re.match(r"^(\d+)", raw)
        if m_num:
            return int(m_num.group(1))
        return None


# ---------------------------------------------------------------------------
# 5. MICEImputer wrapper
# ---------------------------------------------------------------------------
class MICEImputer:
    """MICE imputation using sklearn IterativeImputer for publication-grade models."""

    def __init__(self, m_imputations: int = 20, max_iter: int = 10, random_state: int = 42):
        self.m = m_imputations
        self.max_iter = max_iter
        self.random_state = random_state

    def impute(self, df: pd.DataFrame, target_cols: list[str],
               covariate_cols: list[str]) -> tuple[pd.DataFrame, dict]:
        from sklearn.experimental import enable_iterative_imputer  # noqa: F401
        from sklearn.impute import IterativeImputer

        all_cols = list(set(target_cols + covariate_cols))
        sub = df[["research_id"] + all_cols].copy()

        for c in all_cols:
            sub[c] = pd.to_numeric(sub[c], errors="coerce")

        before_missing = {c: sub[c].isna().sum() for c in target_cols}
        before_pct = {c: round(100 * v / len(sub), 1) for c, v in before_missing.items()}

        imputed_datasets = []
        for i in range(self.m):
            imp = IterativeImputer(
                max_iter=self.max_iter,
                random_state=self.random_state + i,
                sample_posterior=True,
            )
            arr = imp.fit_transform(sub[all_cols].to_numpy(dtype=float, na_value=np.nan))
            imp_df = pd.DataFrame(arr, columns=all_cols, index=sub.index)
            imp_df["research_id"] = sub["research_id"].values
            imp_df["imputation_id"] = i
            imputed_datasets.append(imp_df)

        pooled = imputed_datasets[0][["research_id"] + target_cols].copy()
        for c in target_cols:
            stacked = np.column_stack([d[c].values for d in imputed_datasets])
            pooled[c] = np.nanmean(stacked, axis=1)

        after_missing = {c: pooled[c].isna().sum() for c in target_cols}
        after_pct = {c: round(100 * v / len(pooled), 1) for c, v in after_missing.items()}

        meta = {
            "m_imputations": self.m,
            "max_iter": self.max_iter,
            "n_patients": len(sub),
            "before_missing_pct": before_pct,
            "after_missing_pct": after_pct,
            "target_cols": target_cols,
            "covariate_cols": covariate_cols,
        }

        return pooled, meta


# ===========================================================================
# SQL Builders
# ===========================================================================

def build_margin_r0_recovery_sql() -> str:
    """extracted_margin_r0_recovery_v1 — recover margins for cancer+NULL patients."""
    return """
CREATE OR REPLACE TABLE extracted_margin_r0_recovery_v1 AS
WITH
cancer_null_margin AS (
    SELECT DISTINCT ps.research_id
    FROM path_synoptics ps
    WHERE (ps.tumor_1_margin_status IS NULL
           OR TRIM(LOWER(CAST(ps.tumor_1_margin_status AS VARCHAR))) IN ('', 'null'))
      AND ps.tumor_1_histologic_type IS NOT NULL
      AND TRIM(LOWER(CAST(ps.tumor_1_histologic_type AS VARCHAR))) NOT IN ('', 'null', 'none')
),

op_note_margin AS (
    SELECT
        CAST(c.research_id AS INTEGER) AS research_id,
        'op_note' AS source_type,
        0.85 AS source_reliability,
        CASE
            WHEN c.note_text ILIKE '%margin%negative%'
                 OR c.note_text ILIKE '%margin%free%'
                 OR c.note_text ILIKE '%margin%clear%'
                 OR c.note_text ILIKE '%margin%uninvolved%'
                 OR c.note_text ILIKE '%R0 resection%'
                 OR c.note_text ILIKE '%widely clear margin%'
                 OR c.note_text ILIKE '%no tumor at margin%'
                 OR c.note_text ILIKE '%no tumor involving margin%' THEN 'negative'
            WHEN c.note_text ILIKE '%close margin%'
                 OR c.note_text ILIKE '%near margin%'
                 OR c.note_text ILIKE '%approaching margin%' THEN 'close'
            WHEN c.note_text ILIKE '%margin%positive%'
                 OR c.note_text ILIKE '%margin%involved%'
                 OR c.note_text ILIKE '%R1 resection%'
                 OR c.note_text ILIKE '%tumor at margin%'
                 OR c.note_text ILIKE '%tumor extends to margin%' THEN 'positive'
            ELSE NULL
        END AS margin_status_recovered,
        CASE
            WHEN c.note_text ILIKE '%margin%negative%'
                 OR c.note_text ILIKE '%margin%free%'
                 OR c.note_text ILIKE '%margin%clear%'
                 OR c.note_text ILIKE '%margin%uninvolved%'
                 OR c.note_text ILIKE '%R0 resection%'
                 OR c.note_text ILIKE '%no tumor at margin%' THEN 'R0'
            WHEN c.note_text ILIKE '%close margin%'
                 OR c.note_text ILIKE '%near margin%' THEN 'R0_close'
            WHEN c.note_text ILIKE '%margin%positive%'
                 OR c.note_text ILIKE '%margin%involved%'
                 OR c.note_text ILIKE '%R1 resection%'
                 OR c.note_text ILIKE '%tumor at margin%' THEN 'R1'
            ELSE NULL
        END AS r_classification_recovered,
        TRY_CAST(regexp_extract(c.note_text,
            '(?i)closest\\s+margin\\s*(?:is\\s+)?(?:of\\s+)?(\\d+(?:\\.\\d+)?)\\s*(?:mm|millimeter)', 1
        ) AS DOUBLE) AS closest_margin_mm,
        LEFT(c.note_text, 200) AS raw_snippet
    FROM clinical_notes_long c
    INNER JOIN cancer_null_margin cnm ON c.research_id = cnm.research_id
    WHERE c.note_type = 'op_note'
      AND c.note_text ILIKE '%margin%'
      AND c.note_text NOT ILIKE '%surgical risk%margin%'
      AND c.note_text NOT ILIKE '%consent%margin%'
),

dc_sum_margin AS (
    SELECT
        CAST(c.research_id AS INTEGER) AS research_id,
        'dc_sum' AS source_type,
        0.65 AS source_reliability,
        CASE
            WHEN c.note_text ILIKE '%margin%negative%'
                 OR c.note_text ILIKE '%margin%free%'
                 OR c.note_text ILIKE '%margin%clear%' THEN 'negative'
            WHEN c.note_text ILIKE '%margin%positive%'
                 OR c.note_text ILIKE '%margin%involved%' THEN 'positive'
            ELSE NULL
        END AS margin_status_recovered,
        CASE
            WHEN c.note_text ILIKE '%margin%negative%'
                 OR c.note_text ILIKE '%margin%free%' THEN 'R0'
            WHEN c.note_text ILIKE '%margin%positive%'
                 OR c.note_text ILIKE '%margin%involved%' THEN 'R1'
            ELSE NULL
        END AS r_classification_recovered,
        NULL AS closest_margin_mm,
        LEFT(c.note_text, 200) AS raw_snippet
    FROM clinical_notes_long c
    INNER JOIN cancer_null_margin cnm ON c.research_id = cnm.research_id
    WHERE c.note_type = 'dc_sum'
      AND c.note_text ILIKE '%margin%'
),

benign_not_applicable AS (
    SELECT DISTINCT
        ps.research_id,
        'benign_inferred' AS source_type,
        0.60 AS source_reliability,
        'not_applicable' AS margin_status_recovered,
        'NA_benign' AS r_classification_recovered,
        CAST(NULL AS DOUBLE) AS closest_margin_mm,
        CAST(NULL AS VARCHAR) AS raw_snippet
    FROM path_synoptics ps
    WHERE (ps.tumor_1_margin_status IS NULL
           OR TRIM(LOWER(CAST(ps.tumor_1_margin_status AS VARCHAR))) IN ('', 'null'))
      AND (ps.tumor_1_histologic_type IS NULL
           OR TRIM(LOWER(CAST(ps.tumor_1_histologic_type AS VARCHAR))) IN ('', 'null', 'none'))
),

all_sources AS (
    SELECT * FROM op_note_margin WHERE margin_status_recovered IS NOT NULL
    UNION ALL
    SELECT * FROM dc_sum_margin WHERE margin_status_recovered IS NOT NULL
    UNION ALL
    SELECT * FROM benign_not_applicable
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY source_reliability DESC,
                CASE margin_status_recovered
                    WHEN 'positive' THEN 3 WHEN 'close' THEN 2
                    WHEN 'negative' THEN 1 WHEN 'not_applicable' THEN 0
                    ELSE 0
                END DESC
        ) AS rn
    FROM all_sources
)

SELECT
    research_id,
    source_type,
    source_reliability,
    margin_status_recovered,
    r_classification_recovered,
    closest_margin_mm,
    SUBSTRING(raw_snippet, 1, 150) AS raw_snippet,
    CURRENT_TIMESTAMP AS refined_at
FROM deduped
WHERE rn = 1
ORDER BY research_id;
"""


def build_vw_margin_r0_recovery_sql() -> str:
    """vw_margin_r0_recovery — summary of margin recovery by source and classification."""
    return """
CREATE OR REPLACE TABLE vw_margin_r0_recovery AS
SELECT
    source_type,
    r_classification_recovered,
    margin_status_recovered,
    COUNT(*) AS n_patients,
    ROUND(AVG(source_reliability), 2) AS avg_reliability,
    COUNT(closest_margin_mm) AS n_with_distance
FROM extracted_margin_r0_recovery_v1
GROUP BY source_type, r_classification_recovered, margin_status_recovered
ORDER BY source_type, n_patients DESC;
"""


def build_invasion_grading_recovery_sql() -> str:
    """extracted_invasion_grading_recovery_v1 — resolve present_ungraded vascular/LVI."""
    return """
CREATE OR REPLACE TABLE extracted_invasion_grading_recovery_v1 AS
WITH
ungraded_patients AS (
    SELECT DISTINCT research_id
    FROM patient_refined_staging_flags_v3
    WHERE vascular_invasion_refined = 'present_ungraded'
       OR lvi_refined = 'present'
),

quantify_source AS (
    SELECT
        ps.research_id,
        'quantify_field' AS source_type,
        0.95 AS source_reliability,
        'vascular' AS entity_name,
        CASE
            WHEN LOWER(TRIM(CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR))) LIKE '<%4%'
                 OR CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR) IN ('1','2','3') THEN 'focal'
            WHEN LOWER(TRIM(CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR))) LIKE '>%4%'
                 OR LOWER(TRIM(CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR))) LIKE '>%=%4%'
                 OR CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR) IN ('4','5','6','>5','>6','>30','>/=4','>/= 6') THEN 'extensive'
            ELSE NULL
        END AS grade_recovered,
        TRY_CAST(regexp_extract(CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR), '(\\d+)', 1) AS INTEGER) AS vessel_count,
        CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR) AS raw_value
    FROM path_synoptics ps
    INNER JOIN ungraded_patients up ON ps.research_id = up.research_id
    WHERE ps.tumor_1_angioinvasion_quantify IS NOT NULL
      AND TRIM(LOWER(CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR))) NOT IN ('', 'x', 'n/s', 'not specified')
),

multi_tumor_quantify AS (
    SELECT
        ps.research_id,
        'multi_tumor' AS source_type,
        0.80 AS source_reliability,
        'vascular' AS entity_name,
        CASE
            WHEN MAX(TRY_CAST(regexp_extract(
                COALESCE(CAST(ps.tumor_2_angioinvasion_quantify AS VARCHAR),
                         CAST(ps.tumor_3_angioinvasion_quantify AS VARCHAR)),
                '(\\d+)', 1) AS INTEGER)) >= 4 THEN 'extensive'
            WHEN MAX(TRY_CAST(regexp_extract(
                COALESCE(CAST(ps.tumor_2_angioinvasion_quantify AS VARCHAR),
                         CAST(ps.tumor_3_angioinvasion_quantify AS VARCHAR)),
                '(\\d+)', 1) AS INTEGER)) > 0 THEN 'focal'
            ELSE NULL
        END AS grade_recovered,
        MAX(TRY_CAST(regexp_extract(
            COALESCE(CAST(ps.tumor_2_angioinvasion_quantify AS VARCHAR),
                     CAST(ps.tumor_3_angioinvasion_quantify AS VARCHAR)),
            '(\\d+)', 1) AS INTEGER)) AS vessel_count,
        'multi_tumor_aggregate' AS raw_value
    FROM path_synoptics ps
    INNER JOIN ungraded_patients up ON ps.research_id = up.research_id
    WHERE (ps.tumor_2_angioinvasion_quantify IS NOT NULL
           OR ps.tumor_3_angioinvasion_quantify IS NOT NULL)
    GROUP BY ps.research_id
    HAVING grade_recovered IS NOT NULL
),

op_note_vascular AS (
    SELECT
        CAST(c.research_id AS INTEGER) AS research_id,
        'nlp_op_note' AS source_type,
        0.75 AS source_reliability,
        'vascular' AS entity_name,
        CASE
            WHEN c.note_text ILIKE '%extensive%vascular%invasion%'
                 OR c.note_text ILIKE '%vascular%invasion%extensive%'
                 OR c.note_text ILIKE '%multifocal%vascular%invasion%'
                 OR c.note_text ILIKE '%prominent%vascular%invasion%' THEN 'extensive'
            WHEN c.note_text ILIKE '%focal%vascular%invasion%'
                 OR c.note_text ILIKE '%vascular%invasion%focal%'
                 OR c.note_text ILIKE '%limited%vascular%invasion%'
                 OR c.note_text ILIKE '%minimal%vascular%invasion%'
                 OR c.note_text ILIKE '%rare%vascular%invasion%' THEN 'focal'
            ELSE NULL
        END AS grade_recovered,
        TRY_CAST(regexp_extract(c.note_text,
            '(?i)(\\d+)\\s+(?:foci?\\s+of\\s+)?(?:vascular|angio)\\s*invasion', 1) AS INTEGER) AS vessel_count,
        LEFT(c.note_text, 200) AS raw_value
    FROM clinical_notes_long c
    INNER JOIN ungraded_patients up ON c.research_id = up.research_id
    WHERE c.note_type = 'op_note'
      AND (c.note_text ILIKE '%vascular%invasion%focal%'
           OR c.note_text ILIKE '%focal%vascular%invasion%'
           OR c.note_text ILIKE '%extensive%vascular%invasion%'
           OR c.note_text ILIKE '%vascular%invasion%extensive%'
           OR c.note_text ILIKE '%multifocal%vascular%invasion%'
           OR c.note_text ILIKE '%limited%vascular%invasion%'
           OR c.note_text ILIKE '%minimal%vascular%invasion%'
           OR c.note_text ILIKE '%prominent%vascular%invasion%'
           OR c.note_text ILIKE '%rare%vascular%invasion%')
      AND c.note_text NOT ILIKE '%risk%complication%vascular%'
      AND c.note_text NOT ILIKE '%consent%vascular%'
),

op_note_lvi AS (
    SELECT
        CAST(c.research_id AS INTEGER) AS research_id,
        'nlp_op_note' AS source_type,
        0.75 AS source_reliability,
        'lvi' AS entity_name,
        CASE
            WHEN c.note_text ILIKE '%extensive%lymphovascular%'
                 OR c.note_text ILIKE '%lymphovascular%invasion%extensive%'
                 OR c.note_text ILIKE '%multifocal%lymphovascular%' THEN 'extensive'
            WHEN c.note_text ILIKE '%focal%lymphovascular%'
                 OR c.note_text ILIKE '%lymphovascular%invasion%focal%'
                 OR c.note_text ILIKE '%limited%lymphovascular%' THEN 'focal'
            ELSE NULL
        END AS grade_recovered,
        CAST(NULL AS INTEGER) AS vessel_count,
        LEFT(c.note_text, 200) AS raw_value
    FROM clinical_notes_long c
    INNER JOIN ungraded_patients up ON c.research_id = up.research_id
    WHERE c.note_type = 'op_note'
      AND (c.note_text ILIKE '%focal%lymphovascular%'
           OR c.note_text ILIKE '%lymphovascular%invasion%focal%'
           OR c.note_text ILIKE '%extensive%lymphovascular%'
           OR c.note_text ILIKE '%lymphovascular%invasion%extensive%'
           OR c.note_text ILIKE '%multifocal%lymphovascular%'
           OR c.note_text ILIKE '%limited%lymphovascular%')
),

all_invasions AS (
    SELECT * FROM quantify_source WHERE grade_recovered IS NOT NULL
    UNION ALL
    SELECT * FROM multi_tumor_quantify WHERE grade_recovered IS NOT NULL
    UNION ALL
    SELECT * FROM op_note_vascular WHERE grade_recovered IS NOT NULL
    UNION ALL
    SELECT * FROM op_note_lvi WHERE grade_recovered IS NOT NULL
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, entity_name
            ORDER BY source_reliability DESC,
                CASE grade_recovered WHEN 'extensive' THEN 2 WHEN 'focal' THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM all_invasions
)

SELECT
    research_id,
    source_type,
    source_reliability,
    entity_name,
    grade_recovered,
    vessel_count,
    SUBSTRING(CAST(raw_value AS VARCHAR), 1, 150) AS raw_snippet,
    CURRENT_TIMESTAMP AS refined_at
FROM deduped
WHERE rn = 1
ORDER BY research_id, entity_name;
"""


def build_lateral_neck_detection_sql() -> str:
    """extracted_lateral_neck_v1 — detect lateral neck dissections from all sources."""
    return """
CREATE OR REPLACE TABLE extracted_lateral_neck_v1 AS
WITH
structured_lateral AS (
    SELECT DISTINCT
        ps.research_id,
        'structured_levels' AS source_type,
        0.95 AS source_reliability,
        CAST(ps.tumor_1_level_examined AS VARCHAR) AS level_field,
        CAST(ps.other_ln_dissection AS VARCHAR) AS dissection_field,
        STRING_AGG(DISTINCT
            CASE
                WHEN regexp_matches(CAST(ps.tumor_1_level_examined AS VARCHAR), '(?:^|[^0-9])(2|II)(?:[^0-9]|$)')
                     OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level 2%'
                     OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level ii%' THEN 'II'
                ELSE NULL
            END, ', ') AS detected_levels,
        CASE
            WHEN LOWER(COALESCE(CAST(ps.other_ln_dissection AS VARCHAR), '')) LIKE '%right%'
                 OR LOWER(COALESCE(CAST(ps.tumor_1_level_examined AS VARCHAR), '')) LIKE '%right%' THEN 'right'
            WHEN LOWER(COALESCE(CAST(ps.other_ln_dissection AS VARCHAR), '')) LIKE '%left%'
                 OR LOWER(COALESCE(CAST(ps.tumor_1_level_examined AS VARCHAR), '')) LIKE '%left%' THEN 'left'
            WHEN LOWER(COALESCE(CAST(ps.other_ln_dissection AS VARCHAR), '')) LIKE '%bilateral%' THEN 'bilateral'
            ELSE NULL
        END AS side,
        'level_or_text_match' AS detection_method
    FROM path_synoptics ps
    WHERE (
        ps.tumor_1_level_examined LIKE '%2%'
        OR ps.tumor_1_level_examined LIKE '%3%'
        OR ps.tumor_1_level_examined LIKE '%4%'
        OR ps.tumor_1_level_examined LIKE '%5%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%lateral%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level 2%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level ii%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level 3%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level iii%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level 4%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level iv%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level 5%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%level v%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%jugular%'
        OR LOWER(CAST(ps.other_ln_dissection AS VARCHAR)) LIKE '%modified radical%'
    )
    GROUP BY ps.research_id, ps.tumor_1_level_examined, ps.other_ln_dissection
),

op_note_lateral AS (
    SELECT DISTINCT ON (c.research_id)
        CAST(c.research_id AS INTEGER) AS research_id,
        'nlp_op_note' AS source_type,
        0.80 AS source_reliability,
        CAST(NULL AS VARCHAR) AS level_field,
        CAST(NULL AS VARCHAR) AS dissection_field,
        regexp_extract(c.note_text,
            '(?i)(level(?:s)?\\s+(?:II|III|IV|V|2|3|4|5)(?:\\s*[-–,]\\s*(?:II|III|IV|V|2|3|4|5))*)', 0
        ) AS detected_levels,
        CASE
            WHEN c.note_text ILIKE '%right%lateral%' OR c.note_text ILIKE '%right%neck dissection%' THEN 'right'
            WHEN c.note_text ILIKE '%left%lateral%' OR c.note_text ILIKE '%left%neck dissection%' THEN 'left'
            WHEN c.note_text ILIKE '%bilateral%' THEN 'bilateral'
            ELSE NULL
        END AS side,
        CASE
            WHEN c.note_text ILIKE '%lateral neck dissection%' THEN 'lateral_neck_dissection'
            WHEN c.note_text ILIKE '%modified radical neck dissection%' THEN 'modified_radical'
            WHEN c.note_text ILIKE '%selective neck dissection%' THEN 'selective_neck'
            WHEN c.note_text ILIKE '%radical neck dissection%' THEN 'radical_neck'
            WHEN c.note_text ILIKE '%functional neck dissection%' THEN 'functional_neck'
            WHEN c.note_text ILIKE '%level II%' OR c.note_text ILIKE '%level III%'
                 OR c.note_text ILIKE '%level IV%' OR c.note_text ILIKE '%level V%' THEN 'level_mention'
            WHEN c.note_text ILIKE '%lateral compartment%' THEN 'lateral_compartment'
            WHEN c.note_text ILIKE '%jugular%dissection%' OR c.note_text ILIKE '%jugular%nodes%' THEN 'jugular'
            ELSE 'other_lateral'
        END AS detection_method
    FROM clinical_notes_long c
    WHERE c.note_type = 'op_note'
      AND (c.note_text ILIKE '%lateral neck dissection%'
           OR c.note_text ILIKE '%modified radical neck dissection%'
           OR c.note_text ILIKE '%selective neck dissection%'
           OR c.note_text ILIKE '%radical neck dissection%'
           OR c.note_text ILIKE '%functional neck dissection%'
           OR c.note_text ILIKE '%lateral compartment%'
           OR c.note_text ILIKE '%jugular%dissection%'
           OR c.note_text ILIKE '%jugular%lymphadenectomy%'
           OR (c.note_text ILIKE '%level II%' AND (c.note_text ILIKE '%dissection%' OR c.note_text ILIKE '%clearance%'))
           OR (c.note_text ILIKE '%level III%' AND (c.note_text ILIKE '%dissection%' OR c.note_text ILIKE '%clearance%'))
           OR (c.note_text ILIKE '%level IV%' AND (c.note_text ILIKE '%dissection%' OR c.note_text ILIKE '%clearance%'))
           OR (c.note_text ILIKE '%level V%' AND (c.note_text ILIKE '%dissection%' OR c.note_text ILIKE '%clearance%')))
      AND c.note_text NOT ILIKE '%risk%lateral%'
      AND c.note_text NOT ILIKE '%consent%lateral%'
    ORDER BY c.research_id, source_reliability DESC
),

all_lateral AS (
    SELECT research_id, source_type, source_reliability, level_field,
           dissection_field, detected_levels, side, detection_method
    FROM structured_lateral
    UNION ALL
    SELECT research_id, source_type, source_reliability, level_field,
           dissection_field, detected_levels, side, detection_method
    FROM op_note_lateral
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY source_reliability DESC
        ) AS rn
    FROM all_lateral
)

SELECT
    research_id,
    source_type,
    source_reliability,
    level_field,
    dissection_field,
    detected_levels,
    side,
    detection_method,
    CURRENT_TIMESTAMP AS refined_at
FROM deduped
WHERE rn = 1
ORDER BY research_id;
"""


def build_vw_lateral_neck_sql() -> str:
    """vw_lateral_neck — summary of lateral neck dissection detection."""
    return """
CREATE OR REPLACE TABLE vw_lateral_neck AS
SELECT
    source_type,
    detection_method,
    side,
    COUNT(*) AS n_patients,
    ROUND(AVG(source_reliability), 2) AS avg_reliability
FROM extracted_lateral_neck_v1
GROUP BY source_type, detection_method, side
ORDER BY source_type, n_patients DESC;
"""


def build_multi_tumor_aggregate_sql() -> str:
    """extracted_multi_tumor_aggregate_v1 — worst-case invasion across tumors 1–5."""
    return """
CREATE OR REPLACE TABLE extracted_multi_tumor_aggregate_v1 AS
WITH multi_tumor_pts AS (
    SELECT research_id
    FROM path_synoptics
    WHERE tumor_2_histologic_type IS NOT NULL
    GROUP BY research_id
),

per_patient AS (
    SELECT
        ps.research_id,

        -- Tumor count
        (CASE WHEN ps.tumor_1_histologic_type IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN ps.tumor_2_histologic_type IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN ps.tumor_3_histologic_type IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN ps.tumor_4_histologic_type IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN ps.tumor_5_histologic_type IS NOT NULL THEN 1 ELSE 0 END) AS n_tumors,

        -- Worst angioinvasion (hierarchy: extensive > focal > present > absent)
        CASE
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_angioinvasion AS VARCHAR), '')) IN ('extensive','extensivre','estensive','extrensive')
                 OR LOWER(COALESCE(CAST(ps.tumor_2_angioinvasion AS VARCHAR), '')) IN ('extensive','extensivre')
                 OR LOWER(COALESCE(CAST(ps.tumor_3_angioinvasion AS VARCHAR), '')) IN ('extensive','extensivre') THEN 'extensive'
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_angioinvasion AS VARCHAR), '')) IN ('focal','foacl','minimal','limited')
                 OR LOWER(COALESCE(CAST(ps.tumor_2_angioinvasion AS VARCHAR), '')) IN ('focal','foacl')
                 OR LOWER(COALESCE(CAST(ps.tumor_3_angioinvasion AS VARCHAR), '')) IN ('focal','foacl') THEN 'focal'
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_angioinvasion AS VARCHAR), '')) IN ('x','present','identified','yes')
                 OR LOWER(COALESCE(CAST(ps.tumor_2_angioinvasion AS VARCHAR), '')) IN ('x','present','identified')
                 OR LOWER(COALESCE(CAST(ps.tumor_3_angioinvasion AS VARCHAR), '')) IN ('x','present','identified') THEN 'present_ungraded'
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_angioinvasion AS VARCHAR), '')) IN ('absent','no','none','negative')
                 AND (ps.tumor_2_angioinvasion IS NULL OR LOWER(CAST(ps.tumor_2_angioinvasion AS VARCHAR)) IN ('absent','no','none','negative',''))
                 THEN 'absent'
            ELSE NULL
        END AS worst_angioinvasion,

        -- Worst margin (hierarchy: involved > close > negative)
        CASE
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_margin_status AS VARCHAR), '')) IN ('x','involved','involvd','positive','present')
                 OR LOWER(COALESCE(CAST(ps.tumor_2_margin_status AS VARCHAR), '')) IN ('x','involved','positive')
                 OR LOWER(COALESCE(CAST(ps.tumor_3_margin_status AS VARCHAR), '')) IN ('x','involved','positive') THEN 'involved'
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_margin_status AS VARCHAR), '')) IN ('close','<1mm')
                 OR LOWER(COALESCE(CAST(ps.tumor_2_margin_status AS VARCHAR), '')) IN ('close','<1mm') THEN 'close'
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_margin_status AS VARCHAR), '')) IN ('negative','free','uninvolved','clear')
                 OR LOWER(COALESCE(CAST(ps.tumor_2_margin_status AS VARCHAR), '')) IN ('negative','free') THEN 'negative'
            ELSE NULL
        END AS worst_margin,

        -- Worst ETE (hierarchy: gross > microscopic > present > absent)
        CASE
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_extrathyroidal_extension AS VARCHAR), '')) LIKE '%extensive%'
                 OR LOWER(COALESCE(CAST(ps.tumor_1_extrathyroidal_extension AS VARCHAR), '')) LIKE '%gross%'
                 OR LOWER(COALESCE(CAST(ps.tumor_2_extrathyroidal_extension AS VARCHAR), '')) LIKE '%extensive%'
                 OR LOWER(COALESCE(CAST(ps.tumor_2_extrathyroidal_extension AS VARCHAR), '')) LIKE '%gross%' THEN 'gross'
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_extrathyroidal_extension AS VARCHAR), '')) LIKE '%microscopic%'
                 OR LOWER(COALESCE(CAST(ps.tumor_1_extrathyroidal_extension AS VARCHAR), '')) LIKE '%minimal%'
                 OR LOWER(COALESCE(CAST(ps.tumor_2_extrathyroidal_extension AS VARCHAR), '')) LIKE '%microscopic%' THEN 'microscopic'
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_extrathyroidal_extension AS VARCHAR), '')) IN ('x','yes','present','yes, minimal','yes, extensive')
                 OR LOWER(COALESCE(CAST(ps.tumor_2_extrathyroidal_extension AS VARCHAR), '')) IN ('x','yes','present') THEN 'present_ungraded'
            WHEN LOWER(COALESCE(CAST(ps.tumor_1_extrathyroidal_extension AS VARCHAR), '')) IN ('no','none','absent','negative','not identified')
                 AND (ps.tumor_2_extrathyroidal_extension IS NULL
                      OR LOWER(CAST(ps.tumor_2_extrathyroidal_extension AS VARCHAR)) IN ('no','none','absent','negative','')) THEN 'absent'
            ELSE NULL
        END AS worst_ete,

        -- Max vessel count across all tumors
        GREATEST(
            COALESCE(TRY_CAST(regexp_extract(CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR), '(\\d+)', 1) AS INTEGER), 0),
            COALESCE(TRY_CAST(regexp_extract(CAST(ps.tumor_2_angioinvasion_quantify AS VARCHAR), '(\\d+)', 1) AS INTEGER), 0),
            COALESCE(TRY_CAST(regexp_extract(CAST(ps.tumor_3_angioinvasion_quantify AS VARCHAR), '(\\d+)', 1) AS INTEGER), 0)
        ) AS max_vessel_count,

        -- Max tumor size across all tumors
        GREATEST(
            COALESCE(TRY_CAST(REPLACE(CAST(ps.tumor_1_size_greatest_dimension_cm AS VARCHAR), ';', '') AS DOUBLE), 0),
            COALESCE(TRY_CAST(REPLACE(CAST(ps.tumor_2_size_greatest_dimension_cm AS VARCHAR), ';', '') AS DOUBLE), 0),
            COALESCE(TRY_CAST(REPLACE(CAST(ps.tumor_3_size_greatest_dimension_cm AS VARCHAR), ';', '') AS DOUBLE), 0),
            COALESCE(TRY_CAST(REPLACE(CAST(ps.tumor_4_size_greatest_dimension_cm AS VARCHAR), ';', '') AS DOUBLE), 0)
        ) AS max_tumor_size_cm,

        -- Total LN burden across all tumors
        COALESCE(TRY_CAST(REPLACE(CAST(ps.tumor_1_ln_involved AS VARCHAR), ';', '') AS INTEGER), 0)
        + COALESCE(TRY_CAST(REPLACE(CAST(ps.tumor_2_lns_involved AS VARCHAR), ';', '') AS INTEGER), 0) AS total_ln_positive

    FROM path_synoptics ps
    INNER JOIN multi_tumor_pts mt ON ps.research_id = mt.research_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.research_id ORDER BY ps.surg_date DESC NULLS LAST) = 1
)

SELECT
    research_id,
    n_tumors,
    worst_angioinvasion,
    worst_margin,
    worst_ete,
    max_vessel_count,
    CASE WHEN max_vessel_count > 0 AND max_vessel_count < 4 THEN 'focal'
         WHEN max_vessel_count >= 4 THEN 'extensive'
         ELSE NULL
    END AS worst_who_grade,
    max_tumor_size_cm,
    total_ln_positive,
    CURRENT_TIMESTAMP AS refined_at
FROM per_patient
ORDER BY research_id;
"""


def build_staging_recovery_sql() -> str:
    """extracted_staging_recovery_v1 — consolidated Phase 10 recovery table."""
    return """
CREATE OR REPLACE TABLE extracted_staging_recovery_v1 AS
WITH
existing_margins AS (
    SELECT sf.research_id, mr.margin_r_classification, sf.margin_status_refined, sf.closest_margin_mm
    FROM patient_refined_staging_flags_v3 sf
    LEFT JOIN extracted_margins_refined_v1 mr ON sf.research_id = mr.research_id
),

existing_invasions AS (
    SELECT sf.research_id, sf.vascular_invasion_refined,
           ip.vascular_who_2022_grade, sf.lvi_refined, ip.vascular_positive
    FROM patient_refined_staging_flags_v3 sf
    LEFT JOIN extracted_invasion_profile_v1 ip ON sf.research_id = ip.research_id
),

margin_recovery AS (
    SELECT research_id, margin_status_recovered, r_classification_recovered,
           closest_margin_mm AS recovered_margin_mm, source_type AS margin_source
    FROM extracted_margin_r0_recovery_v1
    WHERE margin_status_recovered NOT IN ('not_applicable')
),

invasion_recovery AS (
    SELECT research_id, entity_name, grade_recovered, vessel_count,
           source_type AS invasion_source
    FROM extracted_invasion_grading_recovery_v1
),

lateral_neck AS (
    SELECT research_id, detection_method, detected_levels, side,
           source_type AS lateral_source
    FROM extracted_lateral_neck_v1
),

multi_tumor AS (
    SELECT research_id, n_tumors, worst_angioinvasion, worst_margin, worst_ete,
           max_vessel_count, worst_who_grade, max_tumor_size_cm, total_ln_positive
    FROM extracted_multi_tumor_aggregate_v1
),

spine AS (
    SELECT DISTINCT research_id FROM path_synoptics
)

SELECT
    s.research_id,

    -- Margin: existing → recovered → multi-tumor
    COALESCE(em.margin_r_classification,
             mr.r_classification_recovered,
             CASE mt.worst_margin
                 WHEN 'involved' THEN 'R1'
                 WHEN 'close' THEN 'R0_close'
                 WHEN 'negative' THEN 'R0'
                 ELSE NULL
             END) AS margin_r_class_v10,
    COALESCE(em.margin_status_refined,
             mr.margin_status_recovered,
             mt.worst_margin) AS margin_status_v10,
    COALESCE(em.closest_margin_mm, mr.recovered_margin_mm) AS closest_margin_mm_v10,
    CASE
        WHEN em.margin_r_classification IS NOT NULL THEN 'phase6_existing'
        WHEN mr.margin_status_recovered IS NOT NULL THEN mr.margin_source
        WHEN mt.worst_margin IS NOT NULL THEN 'multi_tumor'
        ELSE NULL
    END AS margin_source_v10,

    -- Vascular: existing grade → recovered grade → multi-tumor
    COALESCE(
        ei.vascular_who_2022_grade,
        iv.grade_recovered,
        mt.worst_who_grade
    ) AS vascular_who_grade_v10,
    COALESCE(
        CASE WHEN ei.vascular_who_2022_grade IS NOT NULL THEN ei.vascular_invasion_refined END,
        iv.grade_recovered,
        mt.worst_angioinvasion
    ) AS vascular_invasion_v10,
    COALESCE(iv.vessel_count, mt.max_vessel_count) AS vessel_count_v10,
    CASE
        WHEN ei.vascular_who_2022_grade IS NOT NULL THEN 'phase6_existing'
        WHEN iv.grade_recovered IS NOT NULL THEN iv.invasion_source
        WHEN mt.worst_who_grade IS NOT NULL THEN 'multi_tumor'
        ELSE NULL
    END AS vascular_source_v10,

    -- LVI: existing → recovered
    COALESCE(
        CASE WHEN ei.lvi_refined IN ('focal','extensive') THEN ei.lvi_refined END,
        lvi_rec.grade_recovered
    ) AS lvi_grade_v10,
    CASE
        WHEN ei.lvi_refined IN ('focal','extensive') THEN 'phase6_existing'
        WHEN lvi_rec.grade_recovered IS NOT NULL THEN lvi_rec.invasion_source
        ELSE NULL
    END AS lvi_source_v10,

    -- Lateral neck
    CASE WHEN ln.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS lateral_neck_dissected_v10,
    ln.detection_method AS lateral_detection_method,
    ln.detected_levels AS lateral_levels_v10,
    ln.side AS lateral_side_v10,
    ln.lateral_source AS lateral_source_v10,

    -- Multi-tumor aggregate
    mt.n_tumors AS n_tumors_v10,
    mt.worst_ete AS worst_ete_v10,
    mt.max_tumor_size_cm AS max_tumor_size_cm_v10,
    mt.total_ln_positive AS total_ln_positive_v10,

    CURRENT_TIMESTAMP AS refined_at

FROM spine s
LEFT JOIN existing_margins em ON s.research_id = em.research_id
LEFT JOIN existing_invasions ei ON s.research_id = ei.research_id
LEFT JOIN margin_recovery mr ON s.research_id = mr.research_id
LEFT JOIN (SELECT * FROM invasion_recovery WHERE entity_name = 'vascular') iv ON s.research_id = iv.research_id
LEFT JOIN (SELECT * FROM invasion_recovery WHERE entity_name = 'lvi') lvi_rec ON s.research_id = lvi_rec.research_id
LEFT JOIN lateral_neck ln ON s.research_id = ln.research_id
LEFT JOIN multi_tumor mt ON s.research_id = mt.research_id
ORDER BY s.research_id;
"""


def build_mice_imputation_summary_sql() -> str:
    """extracted_mice_summary_v1 — MICE imputation metadata (populated by Python)."""
    return """
CREATE OR REPLACE TABLE extracted_mice_summary_v1 (
    variable VARCHAR,
    before_missing_pct DOUBLE,
    after_missing_pct DOUBLE,
    m_imputations INTEGER,
    max_iter INTEGER,
    n_patients INTEGER,
    imputation_method VARCHAR DEFAULT 'MICE_IterativeImputer',
    refined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def build_master_clinical_v9_sql() -> str:
    """patient_refined_master_clinical_v9 — extends v8 with Phase 10 columns."""
    return """
CREATE OR REPLACE TABLE patient_refined_master_clinical_v9 AS
SELECT
    v8.*,

    -- Phase 10: Margin recovery (4 columns)
    sr.margin_r_class_v10,
    sr.margin_status_v10,
    sr.closest_margin_mm_v10,
    sr.margin_source_v10,

    -- Phase 10: Vascular grading recovery (4 columns)
    sr.vascular_who_grade_v10,
    sr.vascular_invasion_v10,
    sr.vessel_count_v10,
    sr.vascular_source_v10,

    -- Phase 10: LVI grading recovery (2 columns)
    sr.lvi_grade_v10,
    sr.lvi_source_v10,

    -- Phase 10: Lateral neck (5 columns)
    sr.lateral_neck_dissected_v10,
    sr.lateral_detection_method,
    sr.lateral_levels_v10,
    sr.lateral_side_v10,
    sr.lateral_source_v10,

    -- Phase 10: Multi-tumor aggregate (4 columns)
    sr.n_tumors_v10,
    sr.worst_ete_v10,
    sr.max_tumor_size_cm_v10,
    sr.total_ln_positive_v10

FROM patient_refined_master_clinical_v8 v8
LEFT JOIN extracted_staging_recovery_v1 sr ON v8.research_id = sr.research_id
ORDER BY v8.research_id;
"""


# ---------------------------------------------------------------------------
# Step registry
# ---------------------------------------------------------------------------
_PHASE10_STEPS = [
    ("margin_r0_recovery", build_margin_r0_recovery_sql),
    ("vw_margin_r0_recovery", build_vw_margin_r0_recovery_sql),
    ("invasion_grading_recovery", build_invasion_grading_recovery_sql),
    ("lateral_neck_detection", build_lateral_neck_detection_sql),
    ("vw_lateral_neck", build_vw_lateral_neck_sql),
    ("multi_tumor_aggregate", build_multi_tumor_aggregate_sql),
    ("staging_recovery", build_staging_recovery_sql),
    ("mice_summary", build_mice_imputation_summary_sql),
    ("master_v9", build_master_clinical_v9_sql),
]


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------
def _margin_recovery_stats(con) -> dict:
    rows = con.execute("""
        SELECT r_classification_recovered, source_type, COUNT(*) AS n
        FROM extracted_margin_r0_recovery_v1
        GROUP BY r_classification_recovered, source_type
        ORDER BY n DESC
    """).fetchall()
    total = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM extracted_margin_r0_recovery_v1").fetchone()
    return {
        "total_rows": total[0], "unique_patients": total[1],
        "by_class_source": {f"{r[0]}_{r[1]}": r[2] for r in rows},
    }


def _invasion_grading_stats(con) -> dict:
    rows = con.execute("""
        SELECT entity_name, grade_recovered, source_type, COUNT(*) AS n
        FROM extracted_invasion_grading_recovery_v1
        GROUP BY entity_name, grade_recovered, source_type
        ORDER BY entity_name, n DESC
    """).fetchall()
    total = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM extracted_invasion_grading_recovery_v1").fetchone()
    return {
        "total_rows": total[0], "unique_patients": total[1],
        "by_entity_grade_source": {f"{r[0]}_{r[1]}_{r[2]}": r[3] for r in rows},
    }


def _lateral_neck_stats(con) -> dict:
    rows = con.execute("""
        SELECT source_type, detection_method, COUNT(*) AS n
        FROM extracted_lateral_neck_v1
        GROUP BY source_type, detection_method
        ORDER BY n DESC
    """).fetchall()
    total = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM extracted_lateral_neck_v1").fetchone()
    return {
        "total_rows": total[0], "unique_patients": total[1],
        "by_source_method": {f"{r[0]}_{r[1]}": r[2] for r in rows},
    }


def _multi_tumor_stats(con) -> dict:
    rows = con.execute("""
        SELECT n_tumors, COUNT(*) AS n,
            SUM(CASE WHEN worst_angioinvasion IS NOT NULL THEN 1 ELSE 0 END) AS has_angio,
            SUM(CASE WHEN worst_margin IS NOT NULL THEN 1 ELSE 0 END) AS has_margin,
            SUM(CASE WHEN worst_ete IS NOT NULL THEN 1 ELSE 0 END) AS has_ete
        FROM extracted_multi_tumor_aggregate_v1
        GROUP BY n_tumors ORDER BY n_tumors
    """).fetchall()
    total = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM extracted_multi_tumor_aggregate_v1").fetchone()
    return {
        "total_rows": total[0], "unique_patients": total[1],
        "by_n_tumors": {r[0]: {"n": r[1], "has_angio": r[2], "has_margin": r[3], "has_ete": r[4]} for r in rows},
    }


def run_mice_imputation(con, verbose: bool = True) -> dict:
    """Run MICE imputation on key variables and store summary."""
    if verbose:
        print("\n  Running MICE imputation (m=20)...")

    df = con.execute("""
        SELECT
            ps.research_id,
            TRY_CAST(REPLACE(CAST(ps.tumor_1_size_greatest_dimension_cm AS VARCHAR), ';', '') AS DOUBLE) AS tumor_size_cm,
            TRY_CAST(REPLACE(CAST(ps.tumor_1_ln_involved AS VARCHAR), ';', '') AS DOUBLE) AS ln_positive,
            TRY_CAST(REPLACE(CAST(ps.tumor_1_ln_examined AS VARCHAR), ';', '') AS DOUBLE) AS ln_examined,
            CASE LOWER(COALESCE(CAST(ps.tumor_1_margin_status AS VARCHAR), ''))
                WHEN 'x' THEN 1.0 WHEN 'involved' THEN 1.0 WHEN 'involvd' THEN 1.0 WHEN 'positive' THEN 1.0 WHEN 'present' THEN 1.0
                WHEN 'negative' THEN 0.0 WHEN 'free' THEN 0.0 WHEN 'uninvolved' THEN 0.0 WHEN 'clear' THEN 0.0
                WHEN 'close' THEN 0.5
                ELSE NULL
            END AS margin_binary,
            ps.age AS age_at_surgery,
            CASE WHEN LOWER(COALESCE(CAST(ps.gender AS VARCHAR), '')) LIKE '%male%'
                 AND LOWER(COALESCE(CAST(ps.gender AS VARCHAR), '')) NOT LIKE '%female%' THEN 1.0 ELSE 0.0 END AS sex_male,
            CASE WHEN pls.histology_1_type IN ('PTC','FTC','MTC','PDTC','ATC','HCC') THEN 1.0 ELSE 0.0 END AS cancer_flag,
            TRY_CAST(REPLACE(CAST(ps.weight_total AS VARCHAR), ';', '') AS DOUBLE) AS specimen_weight_g
        FROM path_synoptics ps
        LEFT JOIN patient_level_summary_mv pls ON ps.research_id = pls.research_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.research_id ORDER BY ps.surg_date DESC NULLS LAST) = 1
    """).fetchdf()

    target_cols = ["tumor_size_cm", "ln_positive", "ln_examined", "margin_binary", "specimen_weight_g"]
    covariate_cols = ["age_at_surgery", "sex_male", "cancer_flag"]

    try:
        imputer = MICEImputer(m_imputations=20, max_iter=10, random_state=42)
        pooled, meta = imputer.impute(df, target_cols, covariate_cols)

        summary_rows = []
        for c in target_cols:
            summary_rows.append({
                "variable": c,
                "before_missing_pct": meta["before_missing_pct"].get(c, 0),
                "after_missing_pct": meta["after_missing_pct"].get(c, 0),
                "m_imputations": 20,
                "max_iter": 10,
                "n_patients": meta["n_patients"],
            })

        summary_df = pd.DataFrame(summary_rows)
        con.execute("DELETE FROM extracted_mice_summary_v1")
        con.register("mice_summary_df", summary_df)
        con.execute("""
            INSERT INTO extracted_mice_summary_v1 (variable, before_missing_pct, after_missing_pct,
                m_imputations, max_iter, n_patients)
            SELECT variable, before_missing_pct, after_missing_pct, m_imputations, max_iter, n_patients
            FROM mice_summary_df
        """)
        con.unregister("mice_summary_df")

        if verbose:
            print(f"  MICE complete: {meta['n_patients']} patients, {len(target_cols)} variables")
            for c in target_cols:
                print(f"    {c}: {meta['before_missing_pct'][c]}% → {meta['after_missing_pct'][c]}%")

        return meta

    except ImportError as e:
        if verbose:
            print(f"  [WARN] sklearn not available for MICE: {e}")
        return {"error": str(e)}
    except Exception as e:
        if verbose:
            print(f"  [ERROR] MICE failed: {e}")
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
def audit_and_refine_phase10(
    con,
    variables: list[str] | None = None,
    verbose: bool = True,
    run_mice: bool = True,
) -> dict[str, dict]:
    steps = _PHASE10_STEPS
    if variables:
        steps = [(n, fn) for n, fn in _PHASE10_STEPS if n in variables or "all" in variables]

    results = {}
    stat_fns = {
        "margin_r0_recovery": _margin_recovery_stats,
        "invasion_grading_recovery": _invasion_grading_stats,
        "lateral_neck_detection": _lateral_neck_stats,
        "multi_tumor_aggregate": _multi_tumor_stats,
    }

    for step_name, sql_builder in steps:
        if verbose:
            print(f"\n{'='*70}")
            print(f"  Phase 10: {step_name}")
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

    if run_mice and (not variables or "all" in variables or "mice_summary" in variables):
        mice_meta = run_mice_imputation(con, verbose=verbose)
        results["mice_imputation"] = mice_meta

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Phase 10: Source-Linked Recovery of Margins, Invasions, Lateral Neck, MICE")
    parser.add_argument("--variable", default="all",
                        choices=["all"] + [s[0] for s in _PHASE10_STEPS])
    parser.add_argument("--md", action="store_true", default=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-mice", action="store_true")
    parser.add_argument("--output-dir", default="notes_extraction")
    args = parser.parse_args()

    use_md = not args.local

    if args.dry_run:
        print(f"[dry-run] Would run phase10 step={args.variable}")
        return

    con = _get_connection(use_md)

    variables = None if args.variable == "all" else [args.variable]
    results = audit_and_refine_phase10(con, variables=variables, verbose=True,
                                        run_mice=not args.no_mice)

    out_dir = PROJECT_ROOT / args.output_dir
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    lines = [
        "# Phase 10: Source-Linked Recovery Report",
        f"_Generated: {timestamp}_",
        "",
        "## Priorities Addressed",
        "1. R0 margin recovery from NULL-margin cancer patients (op note NLP + dc_sum)",
        "2. Vascular/LVI present_ungraded → focal/extensive (quantify + op note NLP)",
        "3. Lateral neck dissection expansion (structured levels + op note NLP)",
        "4. Multi-tumor aggregation (worst-case across tumors 1–5)",
        "5. MICE imputation for publication-grade models",
        "",
    ]
    for step, rpt in results.items():
        lines.append(f"## {step}")
        for k, v in rpt.items():
            if isinstance(v, dict):
                lines.append(f"- **{k}:**")
                for kk, vv in v.items():
                    lines.append(f"  - {kk}: {vv}")
            else:
                lines.append(f"- **{k}:** {v}")
        lines.append("")

    report_path = out_dir / "master_refinement_report_phase10.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[phase10] Report saved: {report_path}")

    json_path = out_dir / f"phase10_results_{timestamp}.json"
    json_path.write_text(json.dumps(results, default=str, indent=2), encoding="utf-8")
    print(f"[phase10] JSON results: {json_path}")

    con.close()


if __name__ == "__main__":
    main()
