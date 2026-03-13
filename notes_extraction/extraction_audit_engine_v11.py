"""
Extraction Audit Engine v11 — Phase 13: Final 3 Gaps Closure
=============================================================
FINAL extraction engine. Closes three remaining data quality gaps:

  1. VascularInvasionGrader  – WHO 2022 grading for 3,409 ungraded vascular invasion
     records: vessel-count recovery from quantify field, multi-tumor aggregation,
     LVI cross-reference, op-note NLP, typo normalization
  2. IHC_BRAF_Recovery       – mine IHC/VE1/immunohistochemistry notes for BRAF protein
     positivity (14 notes identified)
  3. RAS_SubtypeResolver     – resolve 65 RAS_unspecified to NRAS/HRAS/KRAS via deeper
     text mining of molecular_testing, ThyroSeq, NLP entities, genetic_testing

Source hierarchy:
  path_synoptic_quantify (1.0) > path_synoptic_text (0.95) >
  multi_tumor_aggregate (0.90) > op_note_nlp (0.85) >
  lvi_cross_reference (0.80) > typo_normalization (0.75) >
  molecular_structured (1.0) > thyroseq_enrichment (0.95) >
  nlp_entity (0.85) > molecular_text (0.80) >
  ihc_pathology (0.90) > nlp_clinical (0.75)

Usage:
    from notes_extraction.extraction_audit_engine_v11 import audit_and_refine_phase13
    results = audit_and_refine_phase13(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v11.py --md
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

from notes_extraction.extraction_audit_engine_v3 import _get_connection

PHASE13_SOURCE_HIERARCHY = {
    "path_synoptic_quantify": 1.0,
    "path_synoptic_text": 0.95,
    "multi_tumor_aggregate": 0.90,
    "op_note_nlp": 0.85,
    "lvi_cross_reference": 0.80,
    "typo_normalization": 0.75,
    "molecular_structured": 1.0,
    "thyroseq_enrichment": 0.95,
    "ihc_pathology": 0.90,
    "nlp_entity_genetics": 0.85,
    "molecular_text_mining": 0.80,
    "nlp_clinical_note": 0.75,
}

WHO_2022_FOCAL_THRESHOLD = 4

_VASC_TYPO_MAP = {
    "presnt": "present",
    "foacl": "focal",
    "extrensive": "extensive",
    "estensive": "extensive",
    "extensivre": "extensive",
    "extensiver": "extensive",
    "preesent": "present",
    "c/a": "cannot_assess",
    "s": "suspicious",
    "indetermiante": "indeterminate",
    "indeeterminate": "indeterminate",
    "indeterminent": "indeterminate",
    "n/s": "not_specified",
}

_LVI_TYPO_MAP = {
    "preesent": "present",
    "extensivre": "extensive",
    "extensiver": "extensive",
    "indeeterminate": "indeterminate",
    "indeterminent": "indeterminate",
    "indetermiante": "indeterminate",
    "c/a": "cannot_assess",
    "n/s": "not_specified",
}

_VASC_FOCAL_NLP = [
    re.compile(r"\bfocal\s+(?:vascular|angio)\s*invasion\b", re.I),
    re.compile(r"\b(?:limited|minimal|single|one|1)\s+(?:focus|foci|vessel)\s+(?:of\s+)?(?:vascular|angio)", re.I),
    re.compile(r"\b(?:vascular|angio)\s*invasion.{0,30}focal\b", re.I),
    re.compile(r"\b(?:<\s*4|fewer\s+than\s+4|1|2|3)\s+(?:foci|vessels?)\s+(?:of\s+)?(?:vascular|angio)", re.I),
]

_VASC_EXTENSIVE_NLP = [
    re.compile(r"\bextensive\s+(?:vascular|angio)\s*invasion\b", re.I),
    re.compile(r"\b(?:widespread|diffuse|numerous|many|multiple)\s+(?:foci|vessels?)\s+(?:of\s+)?(?:vascular|angio)", re.I),
    re.compile(r"\b(?:vascular|angio)\s*invasion.{0,30}extensive\b", re.I),
    re.compile(r"\b(?:>\s*4|4\s+or\s+more|>=\s*4|\d{2,})\s+(?:foci|vessels?)\s+(?:of\s+)?(?:vascular|angio)", re.I),
]

_VASC_PRESENT_NLP = [
    re.compile(r"\b(?:vascular|angio)\s*(?:lymphatic\s+)?invasion\s+(?:is\s+)?(?:present|identified|noted|seen)\b", re.I),
    re.compile(r"\bpresent.{0,15}(?:vascular|angio)\s*invasion\b", re.I),
]

_VESSEL_COUNT_NLP = re.compile(
    r"(\d+)\s*(?:foci|focus|vessels?)\s+(?:of\s+)?(?:vascular|angio)", re.I
)

_CONSENT_SKIP_RE = re.compile(
    r"\b(?:risk(?:s)?|complication(?:s)?|include|informed|consent|Valsalva)\b",
    re.I,
)

_IHC_BRAF_POSITIVE = [
    re.compile(r"\bVE1\s+(?:is\s+)?(?:positive|strong|diffuse|reactive)\b", re.I),
    re.compile(r"\bBRAF\s+(?:V600E\s+)?(?:protein|IHC|immunostain)\s+(?:is\s+)?(?:positive|detected|reactive)\b", re.I),
    re.compile(r"\bimmunohistochem\S*\s+(?:for\s+)?BRAF\s*(?:V600E)?\s*(?:is\s+)?(?:positive|reactive)\b", re.I),
    re.compile(r"\bBRAF\s+(?:V600E\s+)?immunohistochem\S*\s*(?:is\s+)?(?:positive|reactive)\b", re.I),
    re.compile(r"\bpositive\s+(?:for\s+)?BRAF\s+(?:by\s+)?(?:IHC|immunohistochem|VE1)\b", re.I),
]

_IHC_BRAF_NEGATIVE = [
    re.compile(r"\bVE1\s+(?:is\s+)?(?:negative|absent|non-reactive)\b", re.I),
    re.compile(r"\bBRAF\s+(?:V600E\s+)?(?:protein|IHC|immunostain)\s+(?:is\s+)?(?:negative|not\s+detected)\b", re.I),
]

_RAS_SUBTYPE_PATTERNS = {
    "NRAS": [
        re.compile(r"\bNRAS\b", re.I),
        re.compile(r"\bN-RAS\b", re.I),
    ],
    "HRAS": [
        re.compile(r"\bHRAS\b", re.I),
        re.compile(r"\bH-RAS\b", re.I),
    ],
    "KRAS": [
        re.compile(r"\bKRAS\b", re.I),
        re.compile(r"\bK-RAS\b", re.I),
    ],
}

_RAS_VARIANT_RE = re.compile(
    r"\b([NHK])-?RAS\s*(?:p\.?)?\s*((?:Q61[RKL]|G12[DVCS]|G13[DRV]|A146[TV]|K117[NR]|Q22K))\b",
    re.I,
)

_RAS_ALLELE_FREQ_RE = re.compile(
    r"\b([NHK])-?RAS\b.{0,60}?(?:(?:VAF|allele\s*freq\S*|AF)\s*[:=]?\s*)?(\d+(?:\.\d+)?)\s*%",
    re.I,
)


# ---------------------------------------------------------------------------
# 1. VascularInvasionGrader
# ---------------------------------------------------------------------------
class VascularInvasionGrader:
    """Grade vascular invasion using WHO 2022 criteria across multiple sources."""

    def grade_from_synoptics(self, con) -> pd.DataFrame:
        """
        Tier 1: Grade from path_synoptics structured fields.
        Uses vessel count when available, text normalization + typo correction.
        """
        df = con.execute("""
            SELECT
                research_id,
                LOWER(CAST(tumor_1_angioinvasion AS VARCHAR)) as vasc_raw,
                TRIM(CAST(tumor_1_angioinvasion_quantify AS VARCHAR)) as vessel_count_raw,
                LOWER(CAST(tumor_1_lymphatic_invasion AS VARCHAR)) as lvi_raw
            FROM path_synoptics
            WHERE tumor_1_angioinvasion IS NOT NULL
              AND LOWER(CAST(tumor_1_angioinvasion AS VARCHAR)) NOT IN ('null','')
        """).fetchdf()

        rows = []
        for _, r in df.iterrows():
            rid = int(r["research_id"])
            raw = str(r["vasc_raw"]).strip()
            vc_raw = r["vessel_count_raw"]
            lvi_raw = str(r.get("lvi_raw", "")).strip()

            norm = _VASC_TYPO_MAP.get(raw, raw)
            vessel_count = None
            grade = None
            source = "path_synoptic_text"
            confidence = 0.95

            if vc_raw and str(vc_raw).strip() not in ("", "nan", "None", "NaN"):
                try:
                    vessel_count = int(float(str(vc_raw).strip()))
                    if vessel_count < WHO_2022_FOCAL_THRESHOLD:
                        grade = "focal"
                    else:
                        grade = "extensive"
                    source = "path_synoptic_quantify"
                    confidence = 1.0
                except (ValueError, TypeError):
                    pass

            if grade is None:
                if norm in ("focal", "limited", "minimal"):
                    grade = "focal"
                elif norm in ("extensive", "prominent", "multifocal", "diffuse"):
                    grade = "extensive"
                elif norm in ("1 focus",):
                    grade = "focal"
                    vessel_count = 1
                elif norm in ("x", "present", "yes", "identified"):
                    grade = "present_ungraded"
                    confidence = 0.75
                elif norm in ("indeterminate", "suspicious", "cannot_assess", "not_specified"):
                    grade = "indeterminate"
                    confidence = 0.50
                elif norm == "no":
                    grade = "absent"
                    confidence = 1.0
                else:
                    grade = "present_ungraded"
                    confidence = 0.60

            lvi_grade = None
            lvi_norm = _LVI_TYPO_MAP.get(lvi_raw, lvi_raw)
            if lvi_norm in ("focal", "limited"):
                lvi_grade = "focal"
            elif lvi_norm in ("extensive", "prominent", "multifocal"):
                lvi_grade = "extensive"
            elif lvi_norm in ("x", "present", "yes", "identified", "preesent"):
                lvi_grade = "present_ungraded"
            elif lvi_norm in ("no", "absent", "negative"):
                lvi_grade = "absent"
            elif lvi_norm and lvi_norm not in ("null", "", "nan", "none"):
                if "focus" in lvi_norm:
                    lvi_grade = "focal"
                elif "cannot" in lvi_norm or "indeter" in lvi_norm:
                    lvi_grade = "indeterminate"
                else:
                    lvi_grade = "present_ungraded"

            rows.append({
                "research_id": rid,
                "vasc_raw": raw,
                "vasc_normalized": norm,
                "vasc_grade_v13": grade,
                "vessel_count_v13": vessel_count,
                "vasc_source_v13": source,
                "vasc_confidence_v13": confidence,
                "lvi_raw_v13": lvi_raw if lvi_raw not in ("null", "", "nan") else None,
                "lvi_grade_v13": lvi_grade,
            })

        return pd.DataFrame(rows)

    def grade_from_multi_tumor(self, con) -> pd.DataFrame:
        """Tier 2: Aggregate worst-case vascular grading across tumors 2-5."""
        rows = []
        for tumor_num in range(2, 6):
            vasc_col = f"tumor_{tumor_num}_angioinvasion"
            quant_col = f"tumor_{tumor_num}_angioinvasion_quantify"
            try:
                df = con.execute(f"""
                    SELECT research_id,
                           LOWER(CAST({vasc_col} AS VARCHAR)) as vasc,
                           TRIM(CAST({quant_col} AS VARCHAR)) as quant
                    FROM path_synoptics
                    WHERE {vasc_col} IS NOT NULL
                      AND LOWER(CAST({vasc_col} AS VARCHAR)) NOT IN ('null','','no','none','absent')
                """).fetchdf()
                for _, r in df.iterrows():
                    grade = None
                    vc = None
                    raw = str(r["vasc"]).strip()
                    q = str(r["quant"]).strip()
                    if q not in ("", "nan", "None", "NaN"):
                        try:
                            vc = int(float(q))
                            grade = "focal" if vc < 4 else "extensive"
                        except (ValueError, TypeError):
                            pass
                    if grade is None:
                        norm = _VASC_TYPO_MAP.get(raw, raw)
                        if norm in ("focal", "limited"):
                            grade = "focal"
                        elif norm in ("extensive", "prominent"):
                            grade = "extensive"
                        elif norm in ("x", "present"):
                            grade = "present_ungraded"
                        else:
                            grade = "present_ungraded"
                    rows.append({
                        "research_id": int(r["research_id"]),
                        "tumor_number": tumor_num,
                        "vasc_grade": grade,
                        "vessel_count": vc,
                    })
            except Exception:
                continue

        if not rows:
            return pd.DataFrame()

        mt_df = pd.DataFrame(rows)
        grade_rank = {"extensive": 3, "focal": 2, "present_ungraded": 1, "indeterminate": 0}
        mt_df["rank"] = mt_df["vasc_grade"].map(grade_rank).fillna(0)

        agg = mt_df.groupby("research_id").agg(
            worst_grade=("rank", "max"),
            max_vessel_count=("vessel_count", "max"),
            n_tumors_with_vasc=("research_id", "count"),
        ).reset_index()

        rank_to_grade = {v: k for k, v in grade_rank.items()}
        agg["mt_vasc_grade"] = agg["worst_grade"].map(rank_to_grade)
        return agg[["research_id", "mt_vasc_grade", "max_vessel_count", "n_tumors_with_vasc"]]

    def grade_from_op_notes(self, con, sample_size: int = 300) -> pd.DataFrame:
        """Tier 3: NLP extraction from op notes for vascular grading keywords."""
        ungraded_rids = con.execute("""
            SELECT DISTINCT ps.research_id
            FROM path_synoptics ps
            WHERE LOWER(CAST(ps.tumor_1_angioinvasion AS VARCHAR)) IN ('x','present','yes')
              AND (ps.tumor_1_angioinvasion_quantify IS NULL
                   OR TRIM(CAST(ps.tumor_1_angioinvasion_quantify AS VARCHAR)) IN ('','nan','None'))
            ORDER BY ps.research_id
        """).fetchdf()

        if len(ungraded_rids) == 0:
            return pd.DataFrame()

        rid_list = ",".join(str(r) for r in ungraded_rids["research_id"].tolist()[:sample_size])
        notes = con.execute(f"""
            SELECT research_id, note_text
            FROM clinical_notes_long
            WHERE research_id IN ({rid_list})
              AND note_type IN ('op_note','path_report','endocrine_note')
            ORDER BY research_id
        """).fetchdf()

        rows = []
        for _, n in notes.iterrows():
            text = str(n["note_text"])[:5000]
            if _CONSENT_SKIP_RE.search(text[:300]):
                text_start = text.find("OPERATIVE FINDINGS")
                if text_start < 0:
                    text_start = text.find("PROCEDURE")
                if text_start < 0:
                    text_start = 300
                text = text[text_start:]

            rid = int(n["research_id"])

            vc_match = _VESSEL_COUNT_NLP.search(text)
            if vc_match:
                try:
                    count = int(vc_match.group(1))
                    grade = "focal" if count < 4 else "extensive"
                    rows.append({
                        "research_id": rid,
                        "nlp_vasc_grade": grade,
                        "nlp_vessel_count": count,
                        "nlp_source": "op_note_vessel_count",
                    })
                    continue
                except (ValueError, TypeError):
                    pass

            for pat in _VASC_FOCAL_NLP:
                if pat.search(text):
                    rows.append({
                        "research_id": rid,
                        "nlp_vasc_grade": "focal",
                        "nlp_vessel_count": None,
                        "nlp_source": "op_note_nlp",
                    })
                    break
            else:
                for pat in _VASC_EXTENSIVE_NLP:
                    if pat.search(text):
                        rows.append({
                            "research_id": rid,
                            "nlp_vasc_grade": "extensive",
                            "nlp_vessel_count": None,
                            "nlp_source": "op_note_nlp",
                        })
                        break

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).drop_duplicates(subset=["research_id"], keep="first")

    def reconcile(self, synoptic_df: pd.DataFrame, mt_df: pd.DataFrame, nlp_df: pd.DataFrame) -> pd.DataFrame:
        """Merge tiers: synoptic > multi-tumor > op note NLP."""
        result = synoptic_df.copy()

        if len(mt_df) > 0:
            mt_lookup = dict(zip(mt_df["research_id"], mt_df["mt_vasc_grade"]))
            mt_vc = dict(zip(mt_df["research_id"], mt_df["max_vessel_count"]))
            for i, row in result.iterrows():
                if row["vasc_grade_v13"] == "present_ungraded":
                    rid = row["research_id"]
                    if rid in mt_lookup and mt_lookup[rid] in ("focal", "extensive"):
                        result.at[i, "vasc_grade_v13"] = mt_lookup[rid]
                        result.at[i, "vasc_source_v13"] = "multi_tumor_aggregate"
                        result.at[i, "vasc_confidence_v13"] = 0.90
                        if mt_vc.get(rid) is not None:
                            result.at[i, "vessel_count_v13"] = mt_vc[rid]

        if len(nlp_df) > 0:
            nlp_lookup = dict(zip(nlp_df["research_id"], nlp_df["nlp_vasc_grade"]))
            nlp_vc = dict(zip(nlp_df["research_id"], nlp_df.get("nlp_vessel_count", {})))
            for i, row in result.iterrows():
                if row["vasc_grade_v13"] == "present_ungraded":
                    rid = row["research_id"]
                    if rid in nlp_lookup and nlp_lookup[rid] in ("focal", "extensive"):
                        result.at[i, "vasc_grade_v13"] = nlp_lookup[rid]
                        result.at[i, "vasc_source_v13"] = "op_note_nlp"
                        result.at[i, "vasc_confidence_v13"] = 0.85
                        vc = nlp_vc.get(rid)
                        if vc is not None and not (isinstance(vc, float) and np.isnan(vc)):
                            result.at[i, "vessel_count_v13"] = int(vc)

        return result


# ---------------------------------------------------------------------------
# 2. IHC_BRAF_Recovery
# ---------------------------------------------------------------------------
class IHC_BRAF_Recovery:
    """Mine IHC/VE1 pathology mentions for BRAF protein positivity."""

    def extract(self, con) -> pd.DataFrame:
        """Search clinical notes for IHC BRAF mentions."""
        notes = con.execute("""
            SELECT research_id, note_type, note_text
            FROM clinical_notes_long
            WHERE (LOWER(note_text) LIKE '%ve1%'
                   OR LOWER(note_text) LIKE '%immunohistochem%braf%'
                   OR LOWER(note_text) LIKE '%braf%immunohistochem%'
                   OR LOWER(note_text) LIKE '%braf protein%'
                   OR LOWER(note_text) LIKE '%braf ihc%'
                   OR LOWER(note_text) LIKE '%braf immunostain%'
                   OR LOWER(note_text) LIKE '%braf v600e%immunostain%'
                   OR LOWER(note_text) LIKE '%immunostain%braf%')
        """).fetchdf()

        rows = []
        for _, n in notes.iterrows():
            text = str(n["note_text"])
            rid = int(n["research_id"])
            note_type = str(n["note_type"])

            if _CONSENT_SKIP_RE.search(text[:200]) and "pathol" not in text[:500].lower():
                continue

            ihc_positive = False
            ihc_negative = False
            for pat in _IHC_BRAF_POSITIVE:
                if pat.search(text):
                    ihc_positive = True
                    break
            for pat in _IHC_BRAF_NEGATIVE:
                if pat.search(text):
                    ihc_negative = True
                    break

            if ihc_positive or ihc_negative:
                rows.append({
                    "research_id": rid,
                    "ihc_braf_result": "positive" if ihc_positive else "negative",
                    "ihc_note_type": note_type,
                    "ihc_source": "ihc_pathology",
                    "ihc_confidence": 0.90 if note_type == "path_report" else 0.80,
                })

        if not rows:
            return pd.DataFrame(columns=[
                "research_id", "ihc_braf_result", "ihc_note_type",
                "ihc_source", "ihc_confidence",
            ])
        return pd.DataFrame(rows).drop_duplicates(subset=["research_id"], keep="first")


# ---------------------------------------------------------------------------
# 3. RAS_SubtypeResolver
# ---------------------------------------------------------------------------
class RAS_SubtypeResolver:
    """Resolve RAS_unspecified patients to NRAS/HRAS/KRAS."""

    def resolve(self, con) -> pd.DataFrame:
        """
        Multi-source resolution:
        1. molecular_testing mutation + detailed_findings text
        2. ThyroSeq enrichment mutation_raw
        3. NLP genetics entities
        4. genetic_testing Excel columns
        """
        unspec_rids = con.execute("""
            SELECT DISTINCT research_id
            FROM extracted_ras_subtypes_v1
            WHERE LOWER(ras_gene) IN ('ras','ras_unspecified','unspecified')
        """).fetchdf()

        if len(unspec_rids) == 0:
            return pd.DataFrame(columns=[
                "research_id", "resolved_ras_gene", "resolved_variant",
                "resolved_allele_freq", "resolution_source", "resolution_confidence",
            ])

        rid_list = ",".join(str(r) for r in unspec_rids["research_id"].tolist())
        rows = []

        mol_df = con.execute(f"""
            SELECT research_id,
                   CAST(mutation AS VARCHAR) as mutation,
                   CAST(detailed_findings AS VARCHAR) as findings
            FROM molecular_testing
            WHERE research_id IN ({rid_list})
        """).fetchdf()

        for _, r in mol_df.iterrows():
            rid = int(r["research_id"])
            text = f"{r.get('mutation', '')} {r.get('findings', '')}"
            gene, variant, af = self._parse_ras_text(text)
            if gene:
                rows.append({
                    "research_id": rid,
                    "resolved_ras_gene": gene,
                    "resolved_variant": variant,
                    "resolved_allele_freq": af,
                    "resolution_source": "molecular_testing_text",
                    "resolution_confidence": 0.95,
                })

        resolved_rids = {r["research_id"] for r in rows}
        remaining = [r for r in unspec_rids["research_id"].tolist() if r not in resolved_rids]

        if remaining:
            rem_list = ",".join(str(r) for r in remaining)
            try:
                tsq = con.execute(f"""
                    SELECT research_id,
                           CAST(mutation_raw AS VARCHAR) as mutation_raw
                    FROM thyroseq_molecular_enrichment
                    WHERE research_id IN ({rem_list})
                """).fetchdf()
                for _, r in tsq.iterrows():
                    rid = int(r["research_id"])
                    gene, variant, af = self._parse_ras_text(str(r.get("mutation_raw", "")))
                    if gene:
                        rows.append({
                            "research_id": rid,
                            "resolved_ras_gene": gene,
                            "resolved_variant": variant,
                            "resolved_allele_freq": af,
                            "resolution_source": "thyroseq_enrichment",
                            "resolution_confidence": 0.92,
                        })
            except Exception:
                pass

        resolved_rids = {r["research_id"] for r in rows}
        remaining = [r for r in unspec_rids["research_id"].tolist() if r not in resolved_rids]

        if remaining:
            rem_list = ",".join(str(r) for r in remaining)
            nlp = con.execute(f"""
                SELECT research_id, entity_value_norm
                FROM note_entities_genetics
                WHERE research_id IN ({rem_list})
                  AND present_or_negated = 'present'
                  AND (LOWER(entity_value_norm) LIKE '%nras%'
                       OR LOWER(entity_value_norm) LIKE '%hras%'
                       OR LOWER(entity_value_norm) LIKE '%kras%')
            """).fetchdf()
            for _, r in nlp.iterrows():
                rid = int(r["research_id"])
                if rid in resolved_rids:
                    continue
                entity = str(r["entity_value_norm"]).upper()
                gene = None
                if "NRAS" in entity:
                    gene = "NRAS"
                elif "HRAS" in entity:
                    gene = "HRAS"
                elif "KRAS" in entity:
                    gene = "KRAS"
                if gene:
                    rows.append({
                        "research_id": rid,
                        "resolved_ras_gene": gene,
                        "resolved_variant": None,
                        "resolved_allele_freq": None,
                        "resolution_source": "nlp_entity_genetics",
                        "resolution_confidence": 0.85,
                    })
                    resolved_rids.add(rid)

        resolved_rids = {r["research_id"] for r in rows}
        remaining = [r for r in unspec_rids["research_id"].tolist() if r not in resolved_rids]

        if remaining:
            rem_list = ",".join(str(r) for r in remaining)
            try:
                gen = con.execute(f"""
                    SELECT research_id,
                           CAST(MUTATION_1 AS VARCHAR) as m1,
                           CAST(\"MUTATION-2\" AS VARCHAR) as m2,
                           CAST(MUTATION_3 AS VARCHAR) as m3,
                           CAST(\"Detailed findings_1\" AS VARCHAR) as df1,
                           CAST(\"Detailed findings_2\" AS VARCHAR) as df2,
                           CAST(\"Detailed findings_3\" AS VARCHAR) as df3
                    FROM genetic_testing
                    WHERE research_id IN ({rem_list})
                """).fetchdf()
                for _, r in gen.iterrows():
                    rid = int(r["research_id"])
                    if rid in resolved_rids:
                        continue
                    text = " ".join(str(v) for v in r.values if v is not None and str(v) != "nan")
                    gene, variant, af = self._parse_ras_text(text)
                    if gene:
                        rows.append({
                            "research_id": rid,
                            "resolved_ras_gene": gene,
                            "resolved_variant": variant,
                            "resolved_allele_freq": af,
                            "resolution_source": "genetic_testing_excel",
                            "resolution_confidence": 0.88,
                        })
                        resolved_rids.add(rid)
            except Exception:
                pass

        resolved_rids = {r["research_id"] for r in rows}
        remaining = [r for r in unspec_rids["research_id"].tolist() if r not in resolved_rids]

        if remaining:
            rem_list = ",".join(str(r) for r in remaining)
            clin = con.execute(f"""
                SELECT research_id, note_text
                FROM clinical_notes_long
                WHERE research_id IN ({rem_list})
                  AND note_type IN ('path_report','endocrine_note','op_note','other_history')
                  AND (LOWER(note_text) LIKE '%nras%'
                       OR LOWER(note_text) LIKE '%hras%'
                       OR LOWER(note_text) LIKE '%kras%')
            """).fetchdf()
            for _, r in clin.iterrows():
                rid = int(r["research_id"])
                if rid in resolved_rids:
                    continue
                text = str(r["note_text"])[:3000]
                gene, variant, af = self._parse_ras_text(text)
                if gene:
                    rows.append({
                        "research_id": rid,
                        "resolved_ras_gene": gene,
                        "resolved_variant": variant,
                        "resolved_allele_freq": af,
                        "resolution_source": "clinical_note_text",
                        "resolution_confidence": 0.78,
                    })
                    resolved_rids.add(rid)

        if not rows:
            return pd.DataFrame(columns=[
                "research_id", "resolved_ras_gene", "resolved_variant",
                "resolved_allele_freq", "resolution_source", "resolution_confidence",
            ])
        return pd.DataFrame(rows).drop_duplicates(subset=["research_id"], keep="first")

    def _parse_ras_text(self, text: str) -> tuple[str | None, str | None, float | None]:
        """Parse RAS subtype, variant, and allele frequency from text."""
        if not text or text.strip() in ("", "nan", "None"):
            return None, None, None

        vm = _RAS_VARIANT_RE.search(text)
        if vm:
            gene = f"{vm.group(1).upper()}RAS"
            variant = vm.group(2).upper()
            af_match = _RAS_ALLELE_FREQ_RE.search(text)
            af = float(af_match.group(2)) if af_match else None
            return gene, variant, af

        for gene, patterns in _RAS_SUBTYPE_PATTERNS.items():
            for pat in patterns:
                if pat.search(text):
                    af_match = _RAS_ALLELE_FREQ_RE.search(text)
                    af = float(af_match.group(2)) if af_match else None
                    return gene, None, af

        return None, None, None


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def audit_and_refine_phase13(con, dry_run: bool = False) -> dict:
    """Full Phase 13 pipeline: 3 gaps closure + FINAL master table."""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    results: dict = {"timestamp": ts, "phase": 13, "tables_deployed": []}

    # ── 1. Vascular Invasion Grading ────────────────────────────────────
    print("\n[Phase 13] === Vascular Invasion Grading ===")
    grader = VascularInvasionGrader()

    print("  Tier 1: path_synoptics structured fields ...")
    synoptic_df = grader.grade_from_synoptics(con)
    print(f"    → {len(synoptic_df)} rows")
    grade_dist = synoptic_df["vasc_grade_v13"].value_counts()
    print(f"    → Grade distribution: {dict(grade_dist)}")
    results["vasc_synoptic_rows"] = len(synoptic_df)
    results["vasc_grade_dist_tier1"] = dict(grade_dist)

    print("  Tier 2: multi-tumor aggregate ...")
    mt_df = grader.grade_from_multi_tumor(con)
    print(f"    → {len(mt_df)} patients with multi-tumor vascular data")
    results["vasc_multi_tumor_patients"] = len(mt_df)

    print("  Tier 3: op note NLP ...")
    nlp_df = grader.grade_from_op_notes(con, sample_size=5000)
    print(f"    → {len(nlp_df)} patients with NLP vascular grading")
    results["vasc_nlp_patients"] = len(nlp_df)

    print("  Reconciling across tiers ...")
    vasc_final = grader.reconcile(synoptic_df, mt_df, nlp_df)
    grade_dist_final = vasc_final["vasc_grade_v13"].value_counts()
    print(f"    → Final grade distribution: {dict(grade_dist_final)}")
    results["vasc_grade_dist_final"] = dict(grade_dist_final)

    resolved = vasc_final[vasc_final["vasc_grade_v13"].isin(["focal", "extensive"])]
    ungraded = vasc_final[vasc_final["vasc_grade_v13"] == "present_ungraded"]
    print(f"    → Graded (focal+extensive): {len(resolved)}")
    print(f"    → Still ungraded: {len(ungraded)}")
    results["vasc_graded_count"] = len(resolved)
    results["vasc_still_ungraded"] = len(ungraded)

    # ── 2. IHC BRAF Recovery ────────────────────────────────────────────
    print("\n[Phase 13] === IHC BRAF Recovery ===")
    ihc = IHC_BRAF_Recovery()
    ihc_df = ihc.extract(con)
    print(f"    → {len(ihc_df)} IHC BRAF results from clinical notes")
    if len(ihc_df) > 0:
        ihc_dist = ihc_df["ihc_braf_result"].value_counts()
        print(f"    → Result distribution: {dict(ihc_dist)}")
        results["ihc_braf_results"] = dict(ihc_dist)
    else:
        print("    → No IHC BRAF results found (IHC reports not in clinical_notes_long)")
        results["ihc_braf_results"] = {}
    results["ihc_braf_patients"] = len(ihc_df)

    # ── 3. RAS Subtype Resolution ───────────────────────────────────────
    print("\n[Phase 13] === RAS Subtype Resolution ===")
    ras_resolver = RAS_SubtypeResolver()
    ras_df = ras_resolver.resolve(con)
    print(f"    → {len(ras_df)} patients resolved from RAS_unspecified")
    if len(ras_df) > 0:
        ras_dist = ras_df["resolved_ras_gene"].value_counts()
        src_dist = ras_df["resolution_source"].value_counts()
        print(f"    → Gene distribution: {dict(ras_dist)}")
        print(f"    → Source distribution: {dict(src_dist)}")
        results["ras_resolved_dist"] = dict(ras_dist)
        results["ras_source_dist"] = dict(src_dist)

    remaining_unspec = 65 - len(ras_df)
    print(f"    → Remaining unspecified: {max(remaining_unspec, 0)}")
    results["ras_resolved_count"] = len(ras_df)
    results["ras_remaining_unspec"] = max(remaining_unspec, 0)

    if dry_run:
        print("\n[Phase 13] DRY RUN — skipping table deployment.")
        return results

    # ── 4. Deploy tables ────────────────────────────────────────────────
    print("\n[Phase 13] Deploying tables ...")

    con.execute("DROP TABLE IF EXISTS extracted_vascular_grading_v13")
    con.register("_tmp_vasc", vasc_final)
    con.execute("CREATE TABLE extracted_vascular_grading_v13 AS SELECT * FROM _tmp_vasc")
    n = con.execute("SELECT COUNT(*) FROM extracted_vascular_grading_v13").fetchone()[0]
    print(f"  → extracted_vascular_grading_v13: {n} rows")
    results["tables_deployed"].append(("extracted_vascular_grading_v13", n))

    con.execute("""
        DROP TABLE IF EXISTS vw_vascular_invasion_grade;
        CREATE TABLE vw_vascular_invasion_grade AS
        SELECT
            vasc_grade_v13 as grade,
            COUNT(*) as n_patients,
            ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 1) as pct,
            AVG(vasc_confidence_v13) as avg_confidence,
            COUNT(vessel_count_v13) as with_vessel_count,
            vasc_source_v13 as primary_source
        FROM extracted_vascular_grading_v13
        GROUP BY vasc_grade_v13, vasc_source_v13
        ORDER BY n_patients DESC
    """)
    n = con.execute("SELECT COUNT(*) FROM vw_vascular_invasion_grade").fetchone()[0]
    print(f"  → vw_vascular_invasion_grade: {n} rows")
    results["tables_deployed"].append(("vw_vascular_invasion_grade", n))

    if len(ihc_df) > 0:
        con.execute("DROP TABLE IF EXISTS extracted_ihc_braf_v13")
        con.register("_tmp_ihc", ihc_df)
        con.execute("CREATE TABLE extracted_ihc_braf_v13 AS SELECT * FROM _tmp_ihc")
        n = con.execute("SELECT COUNT(*) FROM extracted_ihc_braf_v13").fetchone()[0]
        print(f"  → extracted_ihc_braf_v13: {n} rows")
        results["tables_deployed"].append(("extracted_ihc_braf_v13", n))
    else:
        con.execute("DROP TABLE IF EXISTS extracted_ihc_braf_v13")
        con.execute("""
            CREATE TABLE extracted_ihc_braf_v13 (
                research_id INTEGER, ihc_braf_result VARCHAR,
                ihc_note_type VARCHAR, ihc_source VARCHAR, ihc_confidence DOUBLE
            )
        """)
        print("  → extracted_ihc_braf_v13: 0 rows (empty — IHC reports not in notes)")
        results["tables_deployed"].append(("extracted_ihc_braf_v13", 0))

    con.execute("""
        DROP TABLE IF EXISTS vw_molecular_ihc_braf;
        CREATE TABLE vw_molecular_ihc_braf AS
        SELECT
            ihc_braf_result as result,
            ihc_note_type as note_type,
            COUNT(*) as n_patients,
            AVG(ihc_confidence) as avg_confidence
        FROM extracted_ihc_braf_v13
        GROUP BY 1, 2
        ORDER BY n_patients DESC
    """)
    n = con.execute("SELECT COUNT(*) FROM vw_molecular_ihc_braf").fetchone()[0]
    print(f"  → vw_molecular_ihc_braf: {n} rows")
    results["tables_deployed"].append(("vw_molecular_ihc_braf", n))

    if len(ras_df) > 0:
        con.execute("DROP TABLE IF EXISTS extracted_ras_resolved_v13")
        con.register("_tmp_ras", ras_df)
        con.execute("CREATE TABLE extracted_ras_resolved_v13 AS SELECT * FROM _tmp_ras")
        n = con.execute("SELECT COUNT(*) FROM extracted_ras_resolved_v13").fetchone()[0]
        print(f"  → extracted_ras_resolved_v13: {n} rows")
        results["tables_deployed"].append(("extracted_ras_resolved_v13", n))
    else:
        con.execute("DROP TABLE IF EXISTS extracted_ras_resolved_v13")
        con.execute("""
            CREATE TABLE extracted_ras_resolved_v13 (
                research_id INTEGER, resolved_ras_gene VARCHAR,
                resolved_variant VARCHAR, resolved_allele_freq DOUBLE,
                resolution_source VARCHAR, resolution_confidence DOUBLE
            )
        """)
        print("  → extracted_ras_resolved_v13: 0 rows")
        results["tables_deployed"].append(("extracted_ras_resolved_v13", 0))

    con.execute("""
        DROP TABLE IF EXISTS vw_ras_subtypes;
        CREATE TABLE vw_ras_subtypes AS
        SELECT
            resolved_ras_gene as gene,
            COUNT(*) as n_resolved,
            COUNT(resolved_variant) as with_variant,
            COUNT(resolved_allele_freq) as with_af,
            AVG(resolution_confidence) as avg_confidence,
            resolution_source
        FROM extracted_ras_resolved_v13
        GROUP BY 1, resolution_source
        ORDER BY n_resolved DESC
    """)
    n = con.execute("SELECT COUNT(*) FROM vw_ras_subtypes").fetchone()[0]
    print(f"  → vw_ras_subtypes: {n} rows")
    results["tables_deployed"].append(("vw_ras_subtypes", n))

    # ── 5. Build FINAL master table: patient_refined_master_clinical_v12 ─
    print("\n[Phase 13] Building FINAL patient_refined_master_clinical_v12 ...")
    _build_master_v12(con)
    n = con.execute("SELECT COUNT(*) FROM patient_refined_master_clinical_v12").fetchone()[0]
    print(f"  → patient_refined_master_clinical_v12: {n} rows")
    results["tables_deployed"].append(("patient_refined_master_clinical_v12", n))

    # ── 6. Update advanced_features_v5 with Phase 13 columns ─────────────
    print("\n[Phase 13] Updating advanced_features_v5 with Phase 13 columns ...")
    _update_advanced_features(con)
    n = con.execute("SELECT COUNT(*) FROM advanced_features_v5").fetchone()[0]
    print(f"  → advanced_features_v5 (updated): {n} rows")

    # ── 7. Validation view ──────────────────────────────────────────────
    _deploy_validation_view(con)
    results["tables_deployed"].append(("val_phase13_final_gaps", 4))

    # ── 8. Final fill rates ─────────────────────────────────────────────
    print("\n[Phase 13] === FINAL FILL RATES ===")
    fill = con.execute("""
        SELECT
            COUNT(*) as total,
            ROUND(COUNT(CASE WHEN vasc_grade_final_v13 IN ('focal','extensive') THEN 1 END)*100.0
                  / NULLIF(COUNT(CASE WHEN vasc_grade_final_v13 IS NOT NULL
                                      AND vasc_grade_final_v13 != 'absent' THEN 1 END), 0), 1)
                as vasc_graded_pct_of_positive,
            COUNT(CASE WHEN vasc_grade_final_v13 IN ('focal','extensive') THEN 1 END) as vasc_graded,
            COUNT(CASE WHEN vasc_grade_final_v13 = 'present_ungraded' THEN 1 END) as vasc_ungraded,
            COUNT(CASE WHEN LOWER(CAST(braf_positive_final AS VARCHAR)) IN ('true','1') THEN 1 END) as braf_pos,
            COUNT(CASE WHEN ihc_braf_result_v13 IS NOT NULL THEN 1 END) as ihc_braf,
            COUNT(CASE WHEN LOWER(CAST(ras_positive_final AS VARCHAR)) IN ('true','1') THEN 1 END) as ras_pos,
            COUNT(CASE WHEN ras_resolved_gene_v13 IS NOT NULL THEN 1 END) as ras_resolved
        FROM patient_refined_master_clinical_v12
    """).fetchone()
    print(f"  Total patients: {fill[0]}")
    print(f"  Vascular graded % (of positive): {fill[1]}%")
    print(f"  Vascular graded (focal+extensive): {fill[2]}")
    print(f"  Vascular still ungraded: {fill[3]}")
    print(f"  BRAF positive (final): {fill[4]}")
    print(f"  IHC BRAF results: {fill[5]}")
    print(f"  RAS positive (final): {fill[6]}")
    print(f"  RAS newly resolved: {fill[7]}")
    results["final_fill_rates"] = {
        "total_patients": fill[0],
        "vasc_graded_pct_of_positive": float(fill[1]) if fill[1] else 0,
        "vasc_graded": fill[2],
        "vasc_still_ungraded": fill[3],
        "braf_positive_final": fill[4],
        "ihc_braf_results": fill[5],
        "ras_positive_final": fill[6],
        "ras_newly_resolved": fill[7],
    }

    # ── 9. Save results ─────────────────────────────────────────────────
    out_dir = PROJECT_ROOT / "notes_extraction"
    results_path = out_dir / f"phase13_results_{ts}.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n[Phase 13] Results saved to {results_path}")

    return results


def _build_master_v12(con):
    """Build FINAL master clinical table extending v11 with Phase 13 columns."""
    con.execute("""
        DROP TABLE IF EXISTS patient_refined_master_clinical_v12;
        CREATE TABLE patient_refined_master_clinical_v12 AS
        SELECT
            m.*,
            v.vasc_grade_v13       AS vasc_grade_final_v13,
            v.vessel_count_v13     AS vasc_vessel_count_v13,
            v.vasc_source_v13      AS vasc_source_final_v13,
            v.vasc_confidence_v13  AS vasc_confidence_final_v13,
            v.lvi_grade_v13        AS lvi_grade_final_v13,
            ihc.ihc_braf_result    AS ihc_braf_result_v13,
            ihc.ihc_note_type      AS ihc_braf_note_type_v13,
            ihc.ihc_confidence     AS ihc_braf_confidence_v13,
            ras.resolved_ras_gene  AS ras_resolved_gene_v13,
            ras.resolved_variant   AS ras_resolved_variant_v13,
            ras.resolved_allele_freq AS ras_resolved_af_v13,
            ras.resolution_source  AS ras_resolution_source_v13,
            ras.resolution_confidence AS ras_resolution_confidence_v13
        FROM patient_refined_master_clinical_v11 m
        LEFT JOIN (
            SELECT research_id,
                   vasc_grade_v13, vessel_count_v13,
                   vasc_source_v13, vasc_confidence_v13, lvi_grade_v13
            FROM extracted_vascular_grading_v13
            QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id
                                       ORDER BY vasc_confidence_v13 DESC) = 1
        ) v ON m.research_id = v.research_id
        LEFT JOIN extracted_ihc_braf_v13 ihc ON m.research_id = ihc.research_id
        LEFT JOIN extracted_ras_resolved_v13 ras ON m.research_id = ras.research_id
    """)


def _update_advanced_features(con):
    """Add Phase 13 vascular + molecular columns to advanced_features_v5."""
    try:
        con.execute("SELECT 1 FROM advanced_features_v5 LIMIT 1")
    except Exception:
        print("  WARNING: advanced_features_v5 not found, skipping update")
        return

    con.execute("""
        CREATE OR REPLACE TABLE advanced_features_v5 AS
        SELECT
            a.*,
            v.vasc_grade_v13       AS vasc_grade_final_v13,
            v.vessel_count_v13     AS vasc_vessel_count_v13,
            v.lvi_grade_v13        AS lvi_grade_final_v13
        FROM advanced_features_v5 a
        LEFT JOIN (
            SELECT research_id,
                   vasc_grade_v13, vessel_count_v13, lvi_grade_v13
            FROM extracted_vascular_grading_v13
            QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id
                                       ORDER BY vasc_confidence_v13 DESC) = 1
        ) v ON a.research_id = v.research_id
    """)


def _deploy_validation_view(con):
    """Create val_phase13_final_gaps validation table."""
    con.execute("""
        DROP TABLE IF EXISTS val_phase13_final_gaps;
        CREATE TABLE val_phase13_final_gaps AS
        SELECT 'vascular_graded' AS variable,
               (SELECT COUNT(*) FROM extracted_vascular_grading_v13
                WHERE vasc_grade_v13 IN ('focal','extensive')) AS refined_count,
               (SELECT COUNT(*) FROM extracted_vascular_grading_v13
                WHERE vasc_grade_v13 = 'present_ungraded') AS still_ungraded,
               (SELECT COUNT(*) FROM extracted_vascular_grading_v13) AS total_input
        UNION ALL
        SELECT 'ihc_braf',
               (SELECT COUNT(*) FROM extracted_ihc_braf_v13
                WHERE ihc_braf_result = 'positive'),
               (SELECT COUNT(*) FROM extracted_ihc_braf_v13
                WHERE ihc_braf_result = 'negative'),
               (SELECT COUNT(*) FROM extracted_ihc_braf_v13)
        UNION ALL
        SELECT 'ras_resolved',
               (SELECT COUNT(*) FROM extracted_ras_resolved_v13),
               65 - (SELECT COUNT(*) FROM extracted_ras_resolved_v13),
               65
        UNION ALL
        SELECT 'master_v12_total',
               (SELECT COUNT(*) FROM patient_refined_master_clinical_v12),
               0,
               (SELECT COUNT(*) FROM patient_refined_master_clinical_v12)
    """)
    print("  → val_phase13_final_gaps deployed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Phase 13: Final 3 Gaps Closure")
    parser.add_argument("--md", action="store_true", help="Deploy to MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    args = parser.parse_args()

    if args.local:
        import duckdb
        con = duckdb.connect(str(PROJECT_ROOT / "thyroid_master.duckdb"))
    else:
        con = _get_connection(use_md=args.md)
    results = audit_and_refine_phase13(con, dry_run=args.dry_run)

    print("\n" + "=" * 80)
    print("  PHASE 13 COMPLETE — FINAL GAPS CLOSURE")
    print("=" * 80)
    fr = results.get("final_fill_rates", {})
    print(f"  Vascular graded (focal+extensive): {fr.get('vasc_graded', 'N/A')}")
    print(f"  Vascular graded % of positive: {fr.get('vasc_graded_pct_of_positive', 'N/A')}%")
    print(f"  IHC BRAF results: {fr.get('ihc_braf_results', 'N/A')}")
    print(f"  RAS newly resolved: {fr.get('ras_newly_resolved', 'N/A')} / 65")
    print(f"  Tables deployed: {len(results.get('tables_deployed', []))}")
    print()

    con.close()


if __name__ == "__main__":
    main()
