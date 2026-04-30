#!/usr/bin/env python3
"""Build mig_184_v2 ratified R1 AJCC read-only artifacts.

This script performs only read-only SELECTs against the locked MotherDuck
publication database and writes local artifacts requested by
cursor_prompts/CURSOR_PROMPT_mig184_v2_r1_ajcc_RATIFIED_20260430.md.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

RUN_ID = "mig_184_v2_r1_ajcc_derivation_ratified_20260430"
REPORT_NAME = "mig_184_v2_r1_ajcc_derivation_ratified_20260430.md"
SQL_NAME = "184_v2_r1_ajcc_derivation_ratified_20260430.sql"
ADJ_DIR = REPO_ROOT / "exports" / "mig184_r1_adjudication_20260430"
MANIFEST_DIR = REPO_ROOT / "exports" / RUN_ID

T_SEVERITY = {"T4B": 8, "T4A": 7, "T4": 6.5, "T3B": 5, "T3A": 4, "T3": 3, "T2": 2, "T1B": 1.2, "T1A": 1.1, "T1": 1, "TX": 0}
N_SEVERITY = {"N1B": 3, "N1A": 2, "N1": 1, "NX": 0, "N0B": 0, "N0A": 0, "N0": 0}
M_SEVERITY = {"M1": 1, "MX": 0, "M0": 0}
STAGE_SEVERITY = {"IVC": 9, "IVB": 8, "IVA": 7, "IV": 6, "III": 5, "II": 4, "I": 3}

MICRO_ETE_RE = re.compile(r"micro|minimal|focal", re.I)
GROSS_ETE_RE = re.compile(r"gross|macroscopic|strap|skeletal\s+muscle|sternothyroid|sternohyoid|omohyoid|thyrohyoid", re.I)
ABSENT_ETE_RE = re.compile(r"^(no|none|negative|absent|not\s+identified)$", re.I)
T4A_RE = re.compile(r"laryn|trache|esophag|recurrent\s+laryngeal|\brln\b|subcutaneous|airway", re.I)
T4B_RE = re.compile(r"prevertebral|mediastinal|carotid|encas", re.I)
MTC_RE = re.compile(r"medullary|\bmtc\b", re.I)
PTC_RE = re.compile(r"papillary|\bptc\b", re.I)
FTC_RE = re.compile(r"follicular|\bftc\b|h[üu]rthle|hurthle|oncocytic|\bhcc\b", re.I)
ATC_RE = re.compile(r"anaplastic|\batc\b", re.I)
NIFTP_RE = re.compile(r"niftp|non[- ]?invasive follicular thyroid neoplasm", re.I)
PTMC_RE = re.compile(r"microcarcinoma|\bptmc\b", re.I)

RATIFIED_RULES = [
    ("AJCC version", "AJCC 8 (2018 revision)"),
    ("gross_ete=1 + microscopic-text contradiction", "Trust qualifier → no upgrade"),
    ("N1 unspecified", "Keep as N1 at path-event grain; split only at PM grain from upstream central/lateral evidence"),
    ("Stage-group computation grain", "PM grain only; path-event grain holds T/N/M only"),
    ("Mixed histology", "Track components separately; manuscript-default stage_group_resolved uses more aggressive component MTC > PTC > FTC"),
    ("T4 invasion rules", "gross_ete=1 → T3b; laryngeal/tracheal/esophageal/RLN → T4a; prevertebral/mediastinal/carotid → T4b"),
    ("Size-unavailable", "COALESCE(size_greatest_dimension_cm, tumor_size_cm_per_surgery); PTMC without size → T1a; NIFTP exclude; anaplastic → T4; residual stays pending"),
    ("Age-unknown", "No issue; age_at_surgery is complete"),
]


def norm_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "<na>"}:
        return None
    return text


def norm_stage(value: Any) -> str | None:
    text = norm_text(value)
    if text is None:
        return None
    text = text.upper().replace(" ", "")
    if text.startswith("STAGE"):
        text = text.replace("STAGE", "")
    return text


def safe_float(value: Any) -> float | None:
    text = norm_text(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        return float(match.group(0)) if match else None


def is_true(value: Any) -> bool:
    text = norm_text(value)
    if text is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value) != 0.0
    return text.lower() in {"true", "t", "1", "yes", "y", "x", "present", "positive"}


def has_positive_num(value: Any) -> bool:
    num = safe_float(value)
    return num is not None and num > 0


def clean_csv_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if is_object_dtype(df[col]) or is_string_dtype(df[col]):
            df[col] = df[col].map(lambda v: re.sub(r"[\r\n]+", " ", v) if isinstance(v, str) else v)
    return df


def add_review_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_csv_text(df)
    if "logan_decision" not in df.columns:
        df["logan_decision"] = ""
    if "logan_notes" not in df.columns:
        df["logan_notes"] = ""
    return df


def fetch_path_events(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            surgery_episode_id,
            tumor_ordinal,
            surgery_date,
            path_surgery_id,
            specimen_id,
            synoptic_row_ix,
            laterality,
            size_greatest_dimension_cm,
            tumor_size_cm_per_surgery,
            primary_histology,
            histology_variant,
            extrathyroidal_extension,
            gross_ete,
            ln_examined,
            ln_involved,
            nodal_disease_positive_count,
            nodal_disease_total_count,
            extranodal_extension,
            t_stage_ajcc7,
            n_stage_ajcc7,
            m_stage_ajcc7,
            stage_group_ajcc7,
            overall_stage_ajcc7,
            t_stage_ajcc8,
            n_stage_ajcc8,
            m_stage_ajcc8,
            stage_group_ajcc8,
            overall_stage_ajcc8,
            staging_source_note,
            linkage_confidence_tier,
            linkage_score
        FROM main.canonical_path_malignant_events_v1
        """
    ).fetchdf()


def fetch_cpm(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            age_at_surgery,
            is_malignant,
            histology_final,
            histologic_types_all,
            ajcc7_t_stage,
            ajcc7_n_stage,
            ajcc7_m_stage,
            ajcc7_stage_group,
            ajcc8_t_stage,
            ajcc8_n_stage,
            ajcc8_m_stage,
            ajcc8_stage_group,
            dominant_tumor_ajcc8_t_stage,
            dominant_tumor_ajcc8_n_stage,
            dominant_tumor_ajcc8_m_stage,
            dominant_tumor_ajcc8_stage_group,
            cnln_img_central_present,
            cnln_img_lateral_neck_present,
            cnln_img_left_present,
            cnln_img_right_present,
            cnln_img_bilateral_present,
            cnln_img_levels_mentioned,
            cnln_surg_levels_mentioned,
            lateral_neck_dissected_structured_or_nlp,
            lateral_neck_dissected,
            ln_lateral_dissected,
            ln_rollup_central_positive,
            ln_rollup_lateral_left_positive,
            ln_rollup_lateral_right_positive,
            ln_rollup_bilateral_lateral_positive,
            tp_ln_central_positive,
            tp_ln_lateral_positive,
            tp_central_positive_total
        FROM main.canonical_patient_master
        """
    ).fetchdf()


def fetch_invasion(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            invasion_event_id,
            CAST(research_id AS VARCHAR) AS research_id,
            linked_surgery_episode_id AS surgery_episode_id,
            linked_path_malignant_event_id,
            invasion_type,
            finding_status,
            evidence_qualifier,
            source_modality,
            source_kind,
            source_table,
            finding_date,
            confidence,
            linkage_method,
            linkage_ambiguous_multi_finding,
            n_candidate_episodes
        FROM main.canonical_invasion_events_v1
        """
    ).fetchdf()


def fetch_registry_cf87_count(con) -> int:
    return int(
        con.execute(
            """
            SELECT COUNT(*)
            FROM main.canonical_column_verification_registry_v1
            WHERE notes ILIKE '%CF-87-AJCC%'
               OR notes ILIKE '%CF-87%AJCC%'
               OR batch_id ILIKE '%CF-87%'
            """
        ).fetchone()[0]
    )


def invasion_class(invasion_type: Any, qualifier: Any, status: Any) -> str | None:
    status_text = (norm_text(status) or "").lower()
    if status_text in {"absent", "negative", "negated", "not_present", "not present"}:
        return None
    text = " ".join(t for t in [norm_text(invasion_type), norm_text(qualifier)] if t)
    if not text:
        return None
    if T4B_RE.search(text):
        return "T4b"
    if T4A_RE.search(text):
        return "T4a"
    return None


def build_invasion_lookup(inv: pd.DataFrame) -> pd.DataFrame:
    if inv.empty:
        return pd.DataFrame(columns=["research_id", "surgery_episode_id", "t4_invasion_stage", "t4_invasion_evidence"])
    tmp = inv.copy()
    tmp["t4_invasion_stage"] = tmp.apply(lambda r: invasion_class(r.get("invasion_type"), r.get("evidence_qualifier"), r.get("finding_status")), axis=1)
    tmp = tmp[tmp["t4_invasion_stage"].notna()].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["research_id", "surgery_episode_id", "t4_invasion_stage", "t4_invasion_evidence"])
    tmp["sev"] = tmp["t4_invasion_stage"].map(lambda x: T_SEVERITY.get(str(x).upper(), 0))
    tmp["evidence_piece"] = tmp.apply(lambda r: f"{r.get('invasion_type')}:{r.get('finding_status')}:{r.get('evidence_qualifier')}:{r.get('source_table')}", axis=1)
    agg = (
        tmp.sort_values(["research_id", "surgery_episode_id", "sev"], ascending=[True, True, False])
        .groupby(["research_id", "surgery_episode_id"], dropna=False)
        .agg(
            t4_invasion_stage=("t4_invasion_stage", "first"),
            t4_invasion_evidence=("evidence_piece", lambda s: " | ".join(list(dict.fromkeys(map(str, s)))[:6])),
        )
        .reset_index()
    )
    return agg


def histology_components(value: Any) -> list[str]:
    text = norm_text(value) or ""
    parts = [p.strip() for p in re.split(r"\||;|,", text) if p.strip()]
    if not parts and text:
        parts = [text]
    out: list[str] = []
    for part in parts:
        if ATC_RE.search(part):
            out.append("ATC")
        elif MTC_RE.search(part):
            out.append("MTC")
        elif PTC_RE.search(part):
            out.append("PTC")
        elif FTC_RE.search(part):
            out.append("FTC")
        elif NIFTP_RE.search(part):
            out.append("NIFTP")
        elif part:
            out.append(part.upper())
    return list(dict.fromkeys(out))


def aggressive_component(components: list[str]) -> str | None:
    for comp in ["ATC", "MTC", "PTC", "FTC", "NIFTP"]:
        if comp in components:
            return comp
    return components[0] if components else None


def classify_histology(row: pd.Series) -> str:
    text = " | ".join(t for t in [norm_text(row.get("primary_histology")), norm_text(row.get("histology_variant"))] if t)
    if NIFTP_RE.search(text):
        return "NIFTP"
    if ATC_RE.search(text):
        return "ATC"
    if MTC_RE.search(text):
        return "MTC"
    if PTC_RE.search(text):
        return "PTC"
    if FTC_RE.search(text):
        return "FTC"
    return "UNKNOWN"


def derive_t_stage(row: pd.Series) -> tuple[str | None, str, str]:
    hist_text = " | ".join(t for t in [norm_text(row.get("primary_histology")), norm_text(row.get("histology_variant"))] if t)
    ete_text = norm_text(row.get("extrathyroidal_extension")) or ""
    size = safe_float(row.get("size_greatest_dimension_cm"))
    size_source = "size_greatest_dimension_cm"
    if size is None or size <= 0:
        size = safe_float(row.get("tumor_size_cm_per_surgery"))
        size_source = "tumor_size_cm_per_surgery" if size is not None and size > 0 else "size_unavailable"

    if NIFTP_RE.search(hist_text):
        return None, "niftp_excluded", "uncalculable"
    inv_stage = norm_stage(row.get("t4_invasion_stage"))
    if inv_stage in {"T4A", "T4B"}:
        return inv_stage.title().replace("a", "a").replace("b", "b"), f"canonical_invasion_events_v1:{inv_stage}", "high"
    if ATC_RE.search(hist_text):
        return "T4", "anaplastic_default_T4", "high"
    if T4B_RE.search(ete_text):
        return "T4b", "path_event_ete_text_T4b", "high"
    if T4A_RE.search(ete_text):
        return "T4a", "path_event_ete_text_T4a", "high"
    if MICRO_ETE_RE.search(ete_text):
        # Ratified Rule #2: microscopic/minimal/focal qualifier wins over gross_ete flag.
        pass
    elif is_true(row.get("gross_ete")) or GROSS_ETE_RE.search(ete_text):
        return "T3b", "gross_ete_to_T3b_strap_assumption", "medium"

    if size is not None and size > 0:
        if size <= 1.0:
            return "T1a", size_source, "high"
        if size <= 2.0:
            return "T1b", size_source, "high"
        if size <= 4.0:
            return "T2", size_source, "high"
        return "T3a", size_source, "high"
    if PTMC_RE.search(hist_text):
        return "T1a", "microcarcinoma_without_size_default_T1a", "medium"
    return None, "size_residual_logan_pending", "uncalculable"


def derive_n_event(row: pd.Series, edition: int) -> str | None:
    col = f"n_stage_ajcc{edition}"
    current = norm_stage(row.get(col))
    if current:
        return current
    if has_positive_num(row.get("ln_involved")) or has_positive_num(row.get("nodal_disease_positive_count")):
        return "N1"
    if has_positive_num(row.get("ln_examined")) or has_positive_num(row.get("nodal_disease_total_count")):
        return "N0"
    return None


def has_lateral_evidence(row: pd.Series) -> bool:
    bool_cols = [
        "cnln_img_lateral_neck_present", "cnln_img_left_present", "cnln_img_right_present",
        "cnln_img_bilateral_present", "lateral_neck_dissected_structured_or_nlp",
        "lateral_neck_dissected", "ln_lateral_dissected",
    ]
    num_cols = ["ln_rollup_lateral_left_positive", "ln_rollup_lateral_right_positive", "ln_rollup_bilateral_lateral_positive", "tp_ln_lateral_positive"]
    text = " ".join(t for t in [norm_text(row.get("cnln_img_levels_mentioned")), norm_text(row.get("cnln_surg_levels_mentioned"))] if t).lower()
    return any(is_true(row.get(c)) for c in bool_cols) or any(has_positive_num(row.get(c)) for c in num_cols) or bool(re.search(r"lateral|level\s*[1-5ivx]+|jugular|retropharyngeal", text))


def has_central_evidence(row: pd.Series) -> bool:
    bool_cols = ["cnln_img_central_present"]
    num_cols = ["ln_rollup_central_positive", "tp_ln_central_positive", "tp_central_positive_total"]
    text = " ".join(t for t in [norm_text(row.get("cnln_img_levels_mentioned")), norm_text(row.get("cnln_surg_levels_mentioned"))] if t).lower()
    return any(is_true(row.get(c)) for c in bool_cols) or any(has_positive_num(row.get(c)) for c in num_cols) or bool(re.search(r"central|level\s*(vi|6|vii|7)|paratracheal|pretracheal|delphian|prelaryngeal", text))


def resolve_pm_n(row: pd.Series) -> tuple[str | None, str]:
    current = norm_stage(row.get("ajcc8_n_stage"))
    lateral = has_lateral_evidence(row)
    central = has_central_evidence(row)
    if current == "N1":
        if lateral:
            return "N1b", "pm_n1_split_lateral_evidence"
        if central:
            return "N1a", "pm_n1_split_central_evidence"
        return "N1", "pm_n1_unspecified_no_split_evidence"
    return current, "pm_existing_n_stage"


def stage_group_ajcc8(component: str | None, t: Any, n: Any, m: Any, age: Any) -> str | None:
    comp = component or "DTC"
    t_s = norm_stage(t)
    n_s = norm_stage(n)
    m_s = norm_stage(m) or "M0"
    age_f = safe_float(age)
    if comp == "NIFTP":
        return None
    if comp == "ATC":
        if m_s == "M1":
            return "IVB"
        if t_s == "T4B":
            return "IVB"
        return "IVA" if t_s else "IV"
    if comp == "MTC":
        return stage_group_mtc(t_s, n_s, m_s)
    if t_s is None or age_f is None:
        return None
    if m_s == "M1":
        return "II" if age_f < 55 else "IVB"
    if age_f < 55:
        return "I"
    if t_s in {"T1", "T1A", "T1B", "T2"} and (n_s in {None, "N0", "N0A", "N0B", "NX"}):
        return "I"
    if t_s in {"T1", "T1A", "T1B", "T2"} and n_s and n_s.startswith("N1"):
        return "II"
    if t_s in {"T3", "T3A", "T3B"}:
        return "II"
    if t_s == "T4A":
        return "III"
    if t_s == "T4B":
        return "IVA"
    if t_s == "T4":
        return "IVA"
    return None


def stage_group_ajcc7(component: str | None, t: Any, n: Any, m: Any, age: Any) -> str | None:
    comp = component or "DTC"
    t_s = norm_stage(t)
    n_s = norm_stage(n)
    m_s = norm_stage(m) or "M0"
    age_f = safe_float(age)
    if comp == "NIFTP":
        return None
    if comp == "ATC":
        if m_s == "M1":
            return "IVC"
        if t_s == "T4B":
            return "IVB"
        return "IVA" if t_s else "IV"
    if comp == "MTC":
        return stage_group_mtc(t_s, n_s, m_s)
    if t_s is None or age_f is None:
        return None
    if m_s == "M1":
        return "II" if age_f < 45 else "IVC"
    if age_f < 45:
        return "I"
    if t_s in {"T1", "T1A", "T1B"} and (n_s in {None, "N0", "NX"}):
        return "I"
    if t_s == "T2" and (n_s in {None, "N0", "NX"}):
        return "II"
    if (t_s == "T3" and (n_s in {None, "N0", "NX"})) or (t_s in {"T1", "T1A", "T1B", "T2", "T3"} and n_s == "N1A"):
        return "III"
    if t_s == "T4A" or (t_s in {"T1", "T1A", "T1B", "T2", "T3"} and n_s in {"N1", "N1B"}):
        return "IVA"
    if t_s == "T4B":
        return "IVB"
    return None


def stage_group_mtc(t: Any, n: Any, m: Any) -> str | None:
    t_s = norm_stage(t)
    n_s = norm_stage(n)
    m_s = norm_stage(m) or "M0"
    if t_s is None:
        return None
    if m_s == "M1":
        return "IVC"
    if t_s in {"T1", "T1A", "T1B"} and n_s in {None, "N0", "NX"}:
        return "I"
    if t_s in {"T2", "T3", "T3A", "T3B"} and n_s in {None, "N0", "NX"}:
        return "II"
    if t_s in {"T1", "T1A", "T1B", "T2", "T3", "T3A", "T3B"} and n_s == "N1A":
        return "III"
    if t_s == "T4A" or (t_s in {"T1", "T1A", "T1B", "T2", "T3", "T3A", "T3B"} and n_s in {"N1", "N1B"}):
        return "IVA"
    if t_s == "T4B":
        return "IVB"
    return None


def worst_stage(values: pd.Series, mapping: dict[str, float | int]) -> str | None:
    best: tuple[float, str] | None = None
    for value in values.tolist():
        stage = norm_stage(value)
        if not stage:
            continue
        sev = float(mapping.get(stage, -1))
        if best is None or sev > best[0]:
            best = (sev, stage)
    return best[1] if best else None


def derive_events(path_df: pd.DataFrame, invasion_lookup: pd.DataFrame) -> pd.DataFrame:
    df = path_df.merge(invasion_lookup, on=["research_id", "surgery_episode_id"], how="left")
    t_parts = df.apply(derive_t_stage, axis=1, result_type="expand")
    df["t_stage_ajcc8_resolved"] = t_parts[0]
    df["t_resolution_source"] = t_parts[1]
    df["t_resolution_confidence"] = t_parts[2]
    df["t_stage_ajcc7_resolved"] = df.apply(lambda r: "T3" if norm_stage(r.get("t_stage_ajcc8_resolved")) == "T3B" else r.get("t_stage_ajcc8_resolved"), axis=1)
    df["n_stage_ajcc8_resolved"] = df.apply(lambda r: derive_n_event(r, 8), axis=1)
    df["n_stage_ajcc7_resolved"] = df.apply(lambda r: derive_n_event(r, 7), axis=1)
    df["m_stage_ajcc8_resolved"] = df["m_stage_ajcc8"].map(norm_stage).fillna("M0")
    df["m_stage_ajcc7_resolved"] = df["m_stage_ajcc7"].map(norm_stage).fillna(df["m_stage_ajcc8_resolved"])
    df["histology_component"] = df.apply(classify_histology, axis=1)
    return df


def derive_patients(event_df: pd.DataFrame, cpm: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for rid, sub in event_df.groupby("research_id", dropna=False):
        rows.append(
            {
                "research_id": rid,
                "n_path_events": len(sub),
                "t_stage_ajcc8_resolved": worst_stage(sub["t_stage_ajcc8_resolved"], T_SEVERITY),
                "t_stage_ajcc7_resolved": worst_stage(sub["t_stage_ajcc7_resolved"], T_SEVERITY),
                "n_stage_event_coarse": worst_stage(sub["n_stage_ajcc8_resolved"], N_SEVERITY),
                "m_stage_ajcc8_resolved": worst_stage(sub["m_stage_ajcc8_resolved"], M_SEVERITY) or "M0",
                "m_stage_ajcc7_resolved": worst_stage(sub["m_stage_ajcc7_resolved"], M_SEVERITY) or "M0",
                "path_components": " | ".join(sorted(set(c for c in sub["histology_component"].dropna().astype(str) if c != "UNKNOWN"))),
            }
        )
    roll = pd.DataFrame(rows)
    out = cpm.merge(roll, on="research_id", how="left")
    n_parts = out.apply(resolve_pm_n, axis=1, result_type="expand")
    out["n_stage_ajcc8_resolved"] = n_parts[0]
    out["n_resolution_source"] = n_parts[1]
    out["n_stage_ajcc7_resolved"] = out["n_stage_ajcc8_resolved"]
    out["components"] = out["histologic_types_all"].map(histology_components)
    out["component_for_stage"] = out["components"].map(aggressive_component)
    out["component_for_stage"] = out["component_for_stage"].where(out["component_for_stage"].notna(), out["path_components"].map(lambda x: aggressive_component(histology_components(x))))
    out["stage_group_ajcc8_resolved"] = out.apply(lambda r: stage_group_ajcc8(r.get("component_for_stage"), r.get("t_stage_ajcc8_resolved"), r.get("n_stage_ajcc8_resolved"), r.get("m_stage_ajcc8_resolved"), r.get("age_at_surgery")), axis=1)
    out["stage_group_ajcc7_resolved"] = out.apply(lambda r: stage_group_ajcc7(r.get("component_for_stage"), r.get("t_stage_ajcc7_resolved"), r.get("n_stage_ajcc7_resolved"), r.get("m_stage_ajcc7_resolved"), r.get("age_at_surgery")), axis=1)
    return out


def compare_stage(df: pd.DataFrame, legacy_col: str, resolved_col: str) -> dict[str, Any]:
    legacy = df[legacy_col].map(norm_stage) if legacy_col in df else pd.Series([None] * len(df), index=df.index)
    resolved = df[resolved_col].map(norm_stage) if resolved_col in df else pd.Series([None] * len(df), index=df.index)
    paired = legacy.notna() & resolved.notna()
    changed = paired & (legacy != resolved)
    return {
        "legacy_column": legacy_col,
        "resolved_column": resolved_col,
        "rows_total": int(len(df)),
        "legacy_non_null": int(legacy.notna().sum()),
        "resolved_non_null": int(resolved.notna().sum()),
        "paired_non_null": int(paired.sum()),
        "paired_changes": int(changed.sum()),
        "paired_change_pct": round(float(changed.sum()) / float(paired.sum()) * 100, 2) if int(paired.sum()) else 0.0,
        "resolved_null": int(resolved.isna().sum()),
    }


def build_shift_summaries(event_df: pd.DataFrame, patient_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    malignant = patient_df[patient_df["is_malignant"].map(is_true)].copy()
    event_shift = pd.DataFrame(
        [
            compare_stage(event_df, "t_stage_ajcc8", "t_stage_ajcc8_resolved"),
            compare_stage(event_df, "n_stage_ajcc8", "n_stage_ajcc8_resolved"),
            compare_stage(event_df, "m_stage_ajcc8", "m_stage_ajcc8_resolved"),
            compare_stage(event_df, "t_stage_ajcc7", "t_stage_ajcc7_resolved"),
            compare_stage(event_df, "n_stage_ajcc7", "n_stage_ajcc7_resolved"),
            compare_stage(event_df, "m_stage_ajcc7", "m_stage_ajcc7_resolved"),
        ]
    )
    patient_shift = pd.DataFrame(
        [
            compare_stage(malignant, "ajcc8_t_stage", "t_stage_ajcc8_resolved"),
            compare_stage(malignant, "ajcc8_n_stage", "n_stage_ajcc8_resolved"),
            compare_stage(malignant, "ajcc8_m_stage", "m_stage_ajcc8_resolved"),
            compare_stage(malignant, "ajcc8_stage_group", "stage_group_ajcc8_resolved"),
            compare_stage(malignant, "ajcc7_t_stage", "t_stage_ajcc7_resolved"),
            compare_stage(malignant, "ajcc7_n_stage", "n_stage_ajcc7_resolved"),
            compare_stage(malignant, "ajcc7_m_stage", "m_stage_ajcc7_resolved"),
            compare_stage(malignant, "ajcc7_stage_group", "stage_group_ajcc7_resolved"),
        ]
    )
    return patient_shift, event_shift


def write_csvs(patient_df: pd.DataFrame, event_df: pd.DataFrame, invasion_df: pd.DataFrame) -> dict[str, int]:
    ADJ_DIR.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    n1 = patient_df[patient_df["ajcc8_n_stage"].map(norm_stage).eq("N1")].copy()
    n1["central_evidence_available"] = n1.apply(has_central_evidence, axis=1)
    n1["lateral_evidence_available"] = n1.apply(has_lateral_evidence, axis=1)
    n1 = n1[n1["central_evidence_available"] | n1["lateral_evidence_available"]].copy()
    n1["proposed_n_stage_ajcc8_resolved"] = n1.apply(lambda r: "N1b" if r["lateral_evidence_available"] else "N1a", axis=1)
    n1["proposed_action"] = "split_PM_grain_N1_using_upstream_central_lateral_evidence"
    n1_cols = [
        "research_id", "ajcc8_n_stage", "proposed_n_stage_ajcc8_resolved", "central_evidence_available",
        "lateral_evidence_available", "cnln_img_central_present", "cnln_img_lateral_neck_present",
        "cnln_img_left_present", "cnln_img_right_present", "cnln_img_bilateral_present",
        "lateral_neck_dissected_structured_or_nlp", "ln_rollup_central_positive",
        "ln_rollup_lateral_left_positive", "ln_rollup_lateral_right_positive",
        "ln_rollup_bilateral_lateral_positive", "cnln_img_levels_mentioned", "cnln_surg_levels_mentioned",
        "proposed_action",
    ]
    n1_out = add_review_cols(n1[n1_cols].sort_values(["proposed_n_stage_ajcc8_resolved", "research_id"]))
    n1_out.to_csv(ADJ_DIR / "r1b_n1_unspecified_pm_grain.csv", index=False)
    counts["r1b_n1_unspecified_pm_grain.csv"] = len(n1_out)

    inv = invasion_df.copy()
    inv["proposed_t_stage_ajcc8_resolved"] = inv.apply(lambda r: invasion_class(r.get("invasion_type"), r.get("evidence_qualifier"), r.get("finding_status")), axis=1)
    inv = inv[inv["proposed_t_stage_ajcc8_resolved"].notna()].copy()
    inv["proposed_action"] = "review_T4a_T4b_candidate_from_canonical_invasion_events_v1"
    inv_cols = [
        "research_id", "surgery_episode_id", "linked_path_malignant_event_id", "invasion_type",
        "finding_status", "evidence_qualifier", "proposed_t_stage_ajcc8_resolved", "source_modality",
        "source_kind", "source_table", "finding_date", "confidence", "linkage_method",
        "linkage_ambiguous_multi_finding", "n_candidate_episodes", "proposed_action",
    ]
    inv_out = add_review_cols(inv[inv_cols].sort_values(["proposed_t_stage_ajcc8_resolved", "research_id", "surgery_episode_id"], na_position="last"))
    inv_out.to_csv(ADJ_DIR / "r1d_t4_invasion_evidence_review.csv", index=False)
    counts["r1d_t4_invasion_evidence_review.csv"] = len(inv_out)

    mixed = patient_df[patient_df["histologic_types_all"].fillna("").astype(str).str.contains("\\|", regex=True)].copy()
    mixed = mixed[mixed["components"].map(lambda xs: len([x for x in xs if x in {"MTC", "PTC", "FTC", "ATC"}]) >= 2)].copy()
    mixed["components_pipe"] = mixed["components"].map(lambda xs: " | ".join(xs))
    mixed["proposed_stage_component"] = mixed["components"].map(aggressive_component)
    mixed["proposed_action"] = "use_more_aggressive_component_for_manuscript_default_stage_group_resolved"
    mixed_cols = [
        "research_id", "histology_final", "histologic_types_all", "components_pipe", "proposed_stage_component",
        "ajcc8_t_stage", "ajcc8_n_stage", "ajcc8_m_stage", "ajcc8_stage_group",
        "t_stage_ajcc8_resolved", "n_stage_ajcc8_resolved", "m_stage_ajcc8_resolved",
        "stage_group_ajcc8_resolved", "stage_group_ajcc7_resolved", "proposed_action",
    ]
    mixed_out = add_review_cols(mixed[mixed_cols].sort_values(["proposed_stage_component", "research_id"]))
    mixed_out.to_csv(ADJ_DIR / "r1e_mixed_histology_stage_group.csv", index=False)
    counts["r1e_mixed_histology_stage_group.csv"] = len(mixed_out)
    return counts


def markdown_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    view = df.head(max_rows) if max_rows else df
    if view.empty:
        return "_(no rows)_"
    return view.to_markdown(index=False)


def count_existing_csv_rows(path: Path) -> int | str:
    if not path.exists():
        return "missing"
    with path.open(newline="") as f:
        return max(sum(1 for _ in csv.reader(f)) - 1, 0)


def write_sql(path: Path) -> None:
    sql = r"""-- mig_184_v2 R1 AJCC derivation RATIFIED (Logan-ratified 8 rules; supersedes 17b5d8a)
-- Target DB: thyroid_canonical_publication_v1_0
-- Posture: APPLY SKELETON ONLY; authored for Cowork Path-C review/apply. Cursor did NOT execute this file.
-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY

USE thyroid_canonical_publication_v1_0;

-- §0 pre-flight invariants
SELECT 'cpm_row_count' AS invariant_name, COUNT(*) AS observed_value, 10871 AS expected_value
FROM main.canonical_patient_master;
SELECT 'cpm_distinct_research_id' AS invariant_name, COUNT(DISTINCT research_id) AS observed_value, 10871 AS expected_value
FROM main.canonical_patient_master;
SELECT 'preexisting_mig184_v2_registry_rows' AS invariant_name, COUNT(*) AS observed_value, 0 AS expected_value
FROM main.canonical_column_verification_registry_v1
WHERE batch_id = 'mig_184_v2_r1_ajcc_derivation_ratified_20260430';

-- §A pre-snapshot tables for 36 path-malignant CFs + 9 ETE event_resolved CFs + PM AJCC columns.
CREATE SCHEMA IF NOT EXISTS manuscript_workspace;
CREATE TABLE IF NOT EXISTS manuscript_workspace.mig184_v2_r1_pre_snapshot_path_malignant AS
SELECT
    CURRENT_TIMESTAMP AS snapshot_ts,
    'mig_184_v2_r1_ajcc_derivation_ratified_20260430' AS batch_id,
    research_id,
    surgery_episode_id,
    tumor_ordinal,
    t_stage_ajcc7,
    n_stage_ajcc7,
    m_stage_ajcc7,
    overall_stage_ajcc7,
    stage_group_ajcc7,
    t_stage_ajcc8,
    n_stage_ajcc8,
    m_stage_ajcc8,
    overall_stage_ajcc8,
    stage_group_ajcc8,
    staging_source_note
FROM main.canonical_path_malignant_events_v1;

CREATE TABLE IF NOT EXISTS manuscript_workspace.mig184_v2_r1_pre_snapshot_patient_master AS
SELECT
    CURRENT_TIMESTAMP AS snapshot_ts,
    'mig_184_v2_r1_ajcc_derivation_ratified_20260430' AS batch_id,
    research_id,
    ajcc7_t_stage,
    ajcc7_n_stage,
    ajcc7_m_stage,
    ajcc7_stage_group,
    ajcc8_t_stage,
    ajcc8_n_stage,
    ajcc8_m_stage,
    ajcc8_stage_group,
    dominant_tumor_ajcc8_t_stage,
    dominant_tumor_ajcc8_n_stage,
    dominant_tumor_ajcc8_m_stage,
    dominant_tumor_ajcc8_stage_group,
    age_at_surgery,
    histology_final,
    histologic_types_all
FROM main.canonical_patient_master;

CREATE TABLE IF NOT EXISTS manuscript_workspace.mig184_v2_r1_pre_snapshot_registry AS
SELECT CURRENT_TIMESTAMP AS snapshot_ts, *
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-87-AJCC%'
   OR notes ILIKE '%CF-87%AJCC%'
   OR batch_id ILIKE '%CF-87%';

-- §B canonical_path_malignant_events_v1 resolved columns (path-event grain holds T/N/M only; no stage group update here).
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS t_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS n_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS m_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS t_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS n_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS m_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_resolution_source VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_resolution_confidence VARCHAR;

-- §C canonical_patient_master resolved columns (PM grain computes stage group).
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_t_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_n_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_m_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_stage_group_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_t_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_n_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_m_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_stage_group_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc_resolution_source VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc_resolution_confidence VARCHAR;

-- §D T-stage UPDATE — Rules #1, #2, #6, #7.
WITH t4_invasion AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        linked_surgery_episode_id AS surgery_episode_id,
        MAX(CASE
            WHEN regexp_matches(LOWER(COALESCE(invasion_type,'') || ' ' || COALESCE(evidence_qualifier,'')), 'prevertebral|mediastinal|carotid|encas') THEN 2
            WHEN regexp_matches(LOWER(COALESCE(invasion_type,'') || ' ' || COALESCE(evidence_qualifier,'')), 'laryn|trache|esophag|recurrent\s+laryngeal|\brln\b|subcutaneous|airway') THEN 1
            ELSE 0
        END) AS t4_rank
    FROM main.canonical_invasion_events_v1
    WHERE LOWER(COALESCE(finding_status,'')) NOT IN ('absent','negative','negated','not_present','not present')
    GROUP BY 1, 2
), event_derivation AS (
    SELECT
        CAST(e.research_id AS VARCHAR) AS research_id,
        e.surgery_episode_id,
        e.tumor_ordinal,
        CASE
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'niftp|non[- ]?invasive follicular thyroid neoplasm') THEN NULL
            WHEN COALESCE(t4.t4_rank, 0) = 2 THEN 'T4b'
            WHEN COALESCE(t4.t4_rank, 0) = 1 THEN 'T4a'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'anaplastic|\batc\b') THEN 'T4'
            WHEN regexp_matches(LOWER(COALESCE(e.extrathyroidal_extension,'')), 'prevertebral|mediastinal|carotid|encas') THEN 'T4b'
            WHEN regexp_matches(LOWER(COALESCE(e.extrathyroidal_extension,'')), 'laryn|trache|esophag|recurrent\s+laryngeal|\brln\b|subcutaneous|airway') THEN 'T4a'
            WHEN regexp_matches(LOWER(COALESCE(e.extrathyroidal_extension,'')), 'micro|minimal|focal') THEN
                CASE
                    WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 1 THEN 'T1a'
                    WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 2 THEN 'T1b'
                    WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 4 THEN 'T2'
                    WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) > 4 THEN 'T3a'
                    WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b') THEN 'T1a'
                    ELSE NULL
                END
            WHEN COALESCE(e.gross_ete, 0) = 1 OR regexp_matches(LOWER(COALESCE(e.extrathyroidal_extension,'')), 'gross|macroscopic|strap|skeletal\s+muscle|sternothyroid|sternohyoid|omohyoid|thyrohyoid') THEN 'T3b'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 1 THEN 'T1a'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 2 THEN 'T1b'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 4 THEN 'T2'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) > 4 THEN 'T3a'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b') THEN 'T1a'
            ELSE NULL
        END AS t_stage_ajcc8_resolved,
        CASE
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'niftp|non[- ]?invasive follicular thyroid neoplasm') THEN 'niftp_excluded'
            WHEN COALESCE(t4.t4_rank, 0) > 0 THEN 'canonical_invasion_events_v1'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'anaplastic|\batc\b') THEN 'anaplastic_default_T4'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NOT NULL THEN 'coalesce_size_greatest_dimension_cm_tumor_size_cm_per_surgery'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b') THEN 'microcarcinoma_without_size_default_T1a'
            ELSE 'size_residual_logan_pending'
        END AS t_resolution_source,
        CASE
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NOT NULL OR COALESCE(t4.t4_rank, 0) > 0 THEN 'high'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b|anaplastic|\batc\b') THEN 'medium'
            ELSE 'uncalculable'
        END AS t_resolution_confidence
    FROM main.canonical_path_malignant_events_v1 e
    LEFT JOIN t4_invasion t4
      ON CAST(e.research_id AS VARCHAR) = t4.research_id
     AND e.surgery_episode_id IS NOT DISTINCT FROM t4.surgery_episode_id
)
UPDATE main.canonical_path_malignant_events_v1 AS tgt
SET
    t_stage_ajcc8_resolved = src.t_stage_ajcc8_resolved,
    t_stage_ajcc7_resolved = CASE WHEN src.t_stage_ajcc8_resolved = 'T3b' THEN 'T3' ELSE src.t_stage_ajcc8_resolved END,
    ajcc_resolution_source = src.t_resolution_source,
    ajcc_resolution_confidence = src.t_resolution_confidence
FROM event_derivation src
WHERE CAST(tgt.research_id AS VARCHAR) = src.research_id
  AND tgt.surgery_episode_id IS NOT DISTINCT FROM src.surgery_episode_id
  AND tgt.tumor_ordinal IS NOT DISTINCT FROM src.tumor_ordinal;

-- §E N-stage UPDATE — Rule #3.
UPDATE main.canonical_path_malignant_events_v1
SET
    n_stage_ajcc8_resolved = COALESCE(n_stage_ajcc8,
        CASE
            WHEN COALESCE(ln_involved, 0) > 0 OR COALESCE(nodal_disease_positive_count, 0) > 0 THEN 'N1'
            WHEN COALESCE(ln_examined, 0) > 0 OR COALESCE(nodal_disease_total_count, 0) > 0 THEN 'N0'
            ELSE NULL
        END),
    n_stage_ajcc7_resolved = COALESCE(n_stage_ajcc7, n_stage_ajcc8,
        CASE
            WHEN COALESCE(ln_involved, 0) > 0 OR COALESCE(nodal_disease_positive_count, 0) > 0 THEN 'N1'
            WHEN COALESCE(ln_examined, 0) > 0 OR COALESCE(nodal_disease_total_count, 0) > 0 THEN 'N0'
            ELSE NULL
        END);

-- §F M-stage UPDATE — copied from current verified patient/event stage where available; M0 default at PM grain unless M1 evidence exists.
UPDATE main.canonical_path_malignant_events_v1
SET
    m_stage_ajcc8_resolved = COALESCE(m_stage_ajcc8, 'M0'),
    m_stage_ajcc7_resolved = COALESCE(m_stage_ajcc7, m_stage_ajcc8, 'M0');

-- §E/F/G PM rollup + PM N split + stage group update.
WITH event_rollup AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        arg_max(t_stage_ajcc8_resolved, CASE upper(t_stage_ajcc8_resolved) WHEN 'T4B' THEN 8 WHEN 'T4A' THEN 7 WHEN 'T4' THEN 6.5 WHEN 'T3B' THEN 5 WHEN 'T3A' THEN 4 WHEN 'T3' THEN 3 WHEN 'T2' THEN 2 WHEN 'T1B' THEN 1.2 WHEN 'T1A' THEN 1.1 ELSE 0 END) AS t8,
        arg_max(t_stage_ajcc7_resolved, CASE upper(t_stage_ajcc7_resolved) WHEN 'T4B' THEN 8 WHEN 'T4A' THEN 7 WHEN 'T4' THEN 6.5 WHEN 'T3B' THEN 5 WHEN 'T3A' THEN 4 WHEN 'T3' THEN 3 WHEN 'T2' THEN 2 WHEN 'T1B' THEN 1.2 WHEN 'T1A' THEN 1.1 ELSE 0 END) AS t7,
        arg_max(n_stage_ajcc8_resolved, CASE upper(n_stage_ajcc8_resolved) WHEN 'N1B' THEN 3 WHEN 'N1A' THEN 2 WHEN 'N1' THEN 1 ELSE 0 END) AS event_n,
        CASE WHEN SUM(CASE WHEN upper(COALESCE(m_stage_ajcc8_resolved, 'M0')) = 'M1' THEN 1 ELSE 0 END) > 0 THEN 'M1' ELSE 'M0' END AS m8,
        CASE WHEN SUM(CASE WHEN upper(COALESCE(m_stage_ajcc7_resolved, 'M0')) = 'M1' THEN 1 ELSE 0 END) > 0 THEN 'M1' ELSE 'M0' END AS m7
    FROM main.canonical_path_malignant_events_v1
    GROUP BY 1
), pm_derivation AS (
    SELECT
        CAST(pm.research_id AS VARCHAR) AS research_id,
        er.t8,
        er.t7,
        CASE
            WHEN upper(pm.ajcc8_n_stage) = 'N1' AND (
                COALESCE(pm.cnln_img_lateral_neck_present, FALSE)
                OR COALESCE(pm.cnln_img_left_present, FALSE)
                OR COALESCE(pm.cnln_img_right_present, FALSE)
                OR COALESCE(pm.cnln_img_bilateral_present, FALSE)
                OR COALESCE(pm.lateral_neck_dissected_structured_or_nlp, FALSE)
                OR COALESCE(pm.lateral_neck_dissected, FALSE)
                OR COALESCE(pm.ln_lateral_dissected, FALSE)
                OR COALESCE(pm.ln_rollup_lateral_left_positive, 0) > 0
                OR COALESCE(pm.ln_rollup_lateral_right_positive, 0) > 0
                OR COALESCE(pm.ln_rollup_bilateral_lateral_positive, 0) > 0
                OR COALESCE(pm.tp_ln_lateral_positive, 0) > 0
                OR regexp_matches(LOWER(COALESCE(pm.cnln_img_levels_mentioned,'') || ' ' || COALESCE(pm.cnln_surg_levels_mentioned,'')), 'lateral|level\s*[1-5ivx]+|jugular|retropharyngeal')
            ) THEN 'N1b'
            WHEN upper(pm.ajcc8_n_stage) = 'N1' AND (
                COALESCE(pm.cnln_img_central_present, FALSE)
                OR COALESCE(pm.ln_rollup_central_positive, 0) > 0
                OR COALESCE(pm.tp_ln_central_positive, 0) > 0
                OR COALESCE(pm.tp_central_positive_total, 0) > 0
                OR regexp_matches(LOWER(COALESCE(pm.cnln_img_levels_mentioned,'') || ' ' || COALESCE(pm.cnln_surg_levels_mentioned,'')), 'central|level\s*(vi|6|vii|7)|paratracheal|pretracheal|delphian|prelaryngeal')
            ) THEN 'N1a'
            ELSE COALESCE(pm.ajcc8_n_stage, er.event_n)
        END AS n8,
        COALESCE(pm.ajcc7_n_stage, pm.ajcc8_n_stage, er.event_n) AS n7,
        COALESCE(pm.ajcc8_m_stage, er.m8, 'M0') AS m8,
        COALESCE(pm.ajcc7_m_stage, pm.ajcc8_m_stage, er.m7, 'M0') AS m7,
        pm.age_at_surgery,
        CASE
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'anaplastic|\batc\b') THEN 'ATC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'medullary|\bmtc\b') THEN 'MTC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'papillary|\bptc\b') THEN 'PTC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'follicular|\bftc\b|hurthle|hürthle|oncocytic|\bhcc\b') THEN 'FTC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'niftp') THEN 'NIFTP'
            ELSE 'DTC'
        END AS stage_component
    FROM main.canonical_patient_master pm
    LEFT JOIN event_rollup er ON CAST(pm.research_id AS VARCHAR) = er.research_id
), stage_derivation AS (
    SELECT
        *,
        CASE
            WHEN stage_component = 'NIFTP' THEN NULL
            WHEN stage_component = 'ATC' AND m8 = 'M1' THEN 'IVB'
            WHEN stage_component = 'ATC' AND t8 = 'T4b' THEN 'IVB'
            WHEN stage_component = 'ATC' THEN 'IVA'
            WHEN stage_component = 'MTC' AND m8 = 'M1' THEN 'IVC'
            WHEN stage_component = 'MTC' AND t8 IN ('T1','T1a','T1b') AND COALESCE(n8,'N0') IN ('N0','NX') THEN 'I'
            WHEN stage_component = 'MTC' AND t8 IN ('T2','T3','T3a','T3b') AND COALESCE(n8,'N0') IN ('N0','NX') THEN 'II'
            WHEN stage_component = 'MTC' AND t8 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n8 = 'N1a' THEN 'III'
            WHEN stage_component = 'MTC' AND (t8 = 'T4a' OR (t8 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n8 IN ('N1','N1b'))) THEN 'IVA'
            WHEN stage_component = 'MTC' AND t8 = 'T4b' THEN 'IVB'
            WHEN age_at_surgery < 55 AND m8 = 'M1' THEN 'II'
            WHEN age_at_surgery < 55 THEN 'I'
            WHEN m8 = 'M1' THEN 'IVB'
            WHEN t8 IN ('T1','T1a','T1b','T2') AND COALESCE(n8,'N0') IN ('N0','N0a','N0b','NX') THEN 'I'
            WHEN t8 IN ('T1','T1a','T1b','T2') AND n8 LIKE 'N1%' THEN 'II'
            WHEN t8 IN ('T3','T3a','T3b') THEN 'II'
            WHEN t8 = 'T4a' THEN 'III'
            WHEN t8 = 'T4b' THEN 'IVA'
            WHEN t8 = 'T4' THEN 'IVA'
            ELSE NULL
        END AS sg8,
        CASE
            WHEN stage_component = 'NIFTP' THEN NULL
            WHEN stage_component = 'ATC' AND m7 = 'M1' THEN 'IVC'
            WHEN stage_component = 'ATC' AND t7 = 'T4b' THEN 'IVB'
            WHEN stage_component = 'ATC' THEN 'IVA'
            WHEN stage_component = 'MTC' AND m7 = 'M1' THEN 'IVC'
            WHEN stage_component = 'MTC' AND t7 IN ('T1','T1a','T1b') AND COALESCE(n7,'N0') IN ('N0','NX') THEN 'I'
            WHEN stage_component = 'MTC' AND t7 IN ('T2','T3','T3a','T3b') AND COALESCE(n7,'N0') IN ('N0','NX') THEN 'II'
            WHEN stage_component = 'MTC' AND t7 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n7 = 'N1a' THEN 'III'
            WHEN stage_component = 'MTC' AND (t7 = 'T4a' OR (t7 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n7 IN ('N1','N1b'))) THEN 'IVA'
            WHEN stage_component = 'MTC' AND t7 = 'T4b' THEN 'IVB'
            WHEN age_at_surgery < 45 AND m7 = 'M1' THEN 'II'
            WHEN age_at_surgery < 45 THEN 'I'
            WHEN m7 = 'M1' THEN 'IVC'
            WHEN t7 IN ('T1','T1a','T1b') AND COALESCE(n7,'N0') IN ('N0','NX') THEN 'I'
            WHEN t7 = 'T2' AND COALESCE(n7,'N0') IN ('N0','NX') THEN 'II'
            WHEN (t7 = 'T3' AND COALESCE(n7,'N0') IN ('N0','NX')) OR (t7 IN ('T1','T1a','T1b','T2','T3') AND n7 = 'N1a') THEN 'III'
            WHEN t7 = 'T4a' OR (t7 IN ('T1','T1a','T1b','T2','T3') AND n7 IN ('N1','N1b')) THEN 'IVA'
            WHEN t7 = 'T4b' THEN 'IVB'
            ELSE NULL
        END AS sg7
    FROM pm_derivation
)
UPDATE main.canonical_patient_master AS pm
SET
    ajcc8_t_stage_resolved = src.t8,
    ajcc8_n_stage_resolved = src.n8,
    ajcc8_m_stage_resolved = src.m8,
    ajcc8_stage_group_resolved = src.sg8,
    ajcc7_t_stage_resolved = src.t7,
    ajcc7_n_stage_resolved = src.n7,
    ajcc7_m_stage_resolved = src.m7,
    ajcc7_stage_group_resolved = src.sg7,
    ajcc_resolution_source = 'mig184_v2_logan_ratified_R1_rules',
    ajcc_resolution_confidence = CASE WHEN src.t8 IS NULL OR src.sg8 IS NULL THEN 'uncalculable_or_pending' ELSE 'high' END,
    cpm_built_at = CURRENT_TIMESTAMP
FROM stage_derivation src
WHERE CAST(pm.research_id AS VARCHAR) = src.research_id;

-- §H Registry note appendix closing CF-87-AJCC on 45 CF rows.
UPDATE main.canonical_column_verification_registry_v1
SET
    verification_status = 'verified',
    verified_by = 'mig_184_v2_r1_ajcc_derivation_ratified_20260430',
    verified_ts = CURRENT_TIMESTAMP,
    verification_method = 'logan_ratified_AJCC8_R1_resolved_derivation_legacy_columns_preserved',
    batch_id = 'mig_184_v2_r1_ajcc_derivation_ratified_20260430',
    notes = COALESCE(notes, '') || ' | mig184_v2: CF-87-AJCC closed by Logan-ratified 8-rule R1 resolved AJCC derivation; legacy stored columns preserved; manuscript SQL should prefer *_resolved.'
WHERE notes ILIKE '%CF-87-AJCC%'
   OR notes ILIKE '%CF-87%AJCC%'
   OR batch_id ILIKE '%CF-87%';

-- §I cpm_reconciliation_provenance_v1 row insert.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
    (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
    ('mig_184_v2_r1_ajcc_derivation_ratified_20260430', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
     'pre_snapshot_add_resolved_cols_event_tnm_pm_stage_group_registry_provenance_post_state',
     'CF-87-AJCC', '45_registry_rows_targeted', 'resolved_columns_authored', 'size_residual_and_csv_review_items');

-- §J Post-state probes.
SELECT 'path_event_t8_resolved' AS metric, t_stage_ajcc8_resolved AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1 GROUP BY 1,2 ORDER BY 1,3 DESC;
SELECT 'path_event_n8_resolved' AS metric, n_stage_ajcc8_resolved AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1 GROUP BY 1,2 ORDER BY 1,3 DESC;
SELECT 'pm_stage_group_ajcc8_resolved' AS metric, ajcc8_stage_group_resolved AS value, COUNT(*) AS n
FROM main.canonical_patient_master GROUP BY 1,2 ORDER BY 1,3 DESC;
SELECT
    COUNT(*) AS paired_pm_ajcc8_stage_group,
    SUM(CASE WHEN ajcc8_stage_group IS DISTINCT FROM ajcc8_stage_group_resolved THEN 1 ELSE 0 END) AS drifted_pm_ajcc8_stage_group
FROM main.canonical_patient_master
WHERE ajcc8_stage_group IS NOT NULL AND ajcc8_stage_group_resolved IS NOT NULL;
"""
    path.write_text(sql)


def write_report(path: Path, patient_shift: pd.DataFrame, event_shift: pd.DataFrame, counts: dict[str, int], event_df: pd.DataFrame, patient_df: pd.DataFrame, registry_count: int) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    existing = {
        "r1a_ete_t_stage_upgrade_review.csv": count_existing_csv_rows(ADJ_DIR / "r1a_ete_t_stage_upgrade_review.csv"),
        "r1c_size_unavailable_residual_121events.csv": count_existing_csv_rows(ADJ_DIR / "r1c_size_unavailable_residual_121events.csv"),
    }
    r1c_path = ADJ_DIR / "r1c_size_unavailable_residual_121events.csv"
    if r1c_path.exists():
        r1c = pd.read_csv(r1c_path)
        r1c_counts = r1c["suggested_disposition"].value_counts(dropna=False).reset_index()
        r1c_counts.columns = ["suggested_disposition", "n_rows"]
    else:
        r1c_counts = pd.DataFrame(columns=["suggested_disposition", "n_rows"])
    t_counts = event_df["t_resolution_source"].value_counts(dropna=False).reset_index()
    t_counts.columns = ["t_resolution_source", "n_events"]
    n_counts = patient_df["n_resolution_source"].value_counts(dropna=False).reset_index()
    n_counts.columns = ["n_resolution_source", "n_patients"]
    hist_counts = patient_df["component_for_stage"].fillna("UNKNOWN_OR_NO_PATH_EVENT").value_counts(dropna=False).reset_index()
    hist_counts.columns = ["component_for_stage", "n_patients"]

    lines = [
        "# mig_184_v2 — R1 AJCC derivation RATIFIED",
        "",
        f"**Run ID:** `{RUN_ID}`",
        f"**Run timestamp (UTC):** `{ts}`",
        "**Posture:** read-only MotherDuck SELECTs + local artifact authoring only; no MotherDuck DDL/DML executed.",
        "**Target DB:** `thyroid_canonical_publication_v1_0`",
        "**Supersedes:** `17b5d8a` / old mig_184 scoping artifacts.",
        "",
        "## 1. Ratified 8-rule spec (Logan-locked)",
        "",
        "| # | Rule | Decision |",
        "|---:|---|---|",
        *[f"| {idx} | {rule} | {decision} |" for idx, (rule, decision) in enumerate(RATIFIED_RULES, start=1)],
        "",
        "Implementation posture in the SQL artifact is marked `LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY`. The SQL is authored but not executed by this lane.",
        "",
        "## 2. Cross-source drift cohort under R1 derivation",
        "",
        "Baseline from mig_182: path-event stored-vs-findings AJCC8 T-stage mismatch was 28.81%; malignant patient-grain CPM vs dominant AJCC8 stage-group shifts were 2.97%. The v2 R1 dry derivation below uses the ratified rules and reports legacy→resolved shifts without mutating source tables.",
        "",
        "### Patient grain (malignant CPM patients)",
        "",
        markdown_table(patient_shift),
        "",
        "### Path-event grain",
        "",
        markdown_table(event_shift),
        "",
        "### T-resolution source distribution",
        "",
        markdown_table(t_counts),
        "",
        "### PM N-resolution source distribution",
        "",
        markdown_table(n_counts),
        "",
        "### Stage component distribution",
        "",
        markdown_table(hist_counts),
        "",
        "## 3. Adjudication CSV inventory",
        "",
        "| CSV | rows | status | purpose |",
        "|---|---:|---|---|",
        f"| r1a_ete_t_stage_upgrade_review.csv | {existing['r1a_ete_t_stage_upgrade_review.csv']} | pre-existing, preserved | ETE/T-stage upgrade review resolved by Rules #1/#2/#6. |",
        f"| r1c_size_unavailable_residual_121events.csv | {existing['r1c_size_unavailable_residual_121events.csv']} | pre-existing, preserved | Size-unavailable residuals under Rule #7. |",
        f"| r1b_n1_unspecified_pm_grain.csv | {counts.get('r1b_n1_unspecified_pm_grain.csv', 0):,} | generated v2 | PM-grain N1 split candidates with central/lateral evidence. |",
        f"| r1d_t4_invasion_evidence_review.csv | {counts.get('r1d_t4_invasion_evidence_review.csv', 0):,} | generated v2 | T4a/T4b invasion candidates from canonical invasion events. |",
        f"| r1e_mixed_histology_stage_group.csv | {counts.get('r1e_mixed_histology_stage_group.csv', 0):,} | generated v2 | Mixed-component histology cases for aggressive-component stage grouping. |",
        "",
        "### r1c residual disposition breakdown",
        "",
        markdown_table(r1c_counts),
        "",
        "## 4. Remaining row-level decisions",
        "",
        f"- **Size residuals:** the preserved r1c CSV has {existing['r1c_size_unavailable_residual_121events.csv']} rows. Per Logan Rule #7, PTMC rows default to T1a, NIFTP is excluded, anaplastic defaults T4, and the residual hand-curation subset remains `size_residual_logan_pending` in the SQL logic.",
        f"- **N1 PM split candidates:** {counts.get('r1b_n1_unspecified_pm_grain.csv', 0):,} patients have N1 plus central/lateral evidence and need final review before Path-C apply.",
        f"- **T4 invasion candidates:** {counts.get('r1d_t4_invasion_evidence_review.csv', 0):,} rows have T4a/T4b candidate evidence from `canonical_invasion_events_v1`.",
        f"- **Mixed histology:** {counts.get('r1e_mixed_histology_stage_group.csv', 0):,} patients have multi-component histology and are staged by the aggressive-component rule (MTC > PTC > FTC; ATC highest if present).",
        f"- **Registry closure target:** live read-only probe found {registry_count:,} registry rows carrying CF-87/AJCC notes or batch IDs. The skeleton SQL updates matching rows and appends the v2 closure note.",
        "",
        "## 5. Unblocking checklist",
        "",
        "1. Logan reviews the five CSVs, especially r1b/r1d/r1e plus the r1c residual hand-curation subset.",
        "2. Cowork applies `qc_framework_v1/migrations/184_v2_r1_ajcc_derivation_ratified_20260430.sql` via Path C if review passes.",
        "3. Post-apply, rerun §J probes, CPM invariants, and registry checks.",
        "4. Manuscript SQL should prefer the new `*_resolved` columns while preserving legacy stored columns unchanged.",
        "",
        "## Governance boundary",
        "",
        "This run did not execute `ALTER`, `UPDATE`, `CREATE`, `DROP`, registry mutation, or provenance insert against MotherDuck. All database interactions were SELECT-only via `connect_locked()`. The SQL file is an apply skeleton for Cowork Path-C.",
    ]
    path.write_text("\n".join(lines) + "\n")


def write_manifest(counts: dict[str, int], patient_shift: pd.DataFrame, event_shift: pd.DataFrame) -> None:
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "posture": "read_only_motherduck_selects_local_artifacts_only",
        "supersedes_commit": "17b5d8a",
        "deliverables": {
            "sql": str((REPO_ROOT / "qc_framework_v1" / "migrations" / SQL_NAME).relative_to(REPO_ROOT)),
            "report": str((REPO_ROOT / "qc_framework_v1" / "reports" / REPORT_NAME).relative_to(REPO_ROOT)),
            "csvs": [str((ADJ_DIR / name).relative_to(REPO_ROOT)) for name in sorted(counts)],
        },
        "adjudication_counts": counts,
        "patient_shift": patient_shift.to_dict(orient="records"),
        "event_shift": event_shift.to_dict(orient="records"),
    }
    (MANIFEST_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    con = connect_locked()
    path_df = fetch_path_events(con)
    cpm = fetch_cpm(con)
    invasion = fetch_invasion(con)
    registry_count = fetch_registry_cf87_count(con)

    invasion_lookup = build_invasion_lookup(invasion)
    event_df = derive_events(path_df, invasion_lookup)
    patient_df = derive_patients(event_df, cpm)
    patient_shift, event_shift = build_shift_summaries(event_df, patient_df)
    counts = write_csvs(patient_df, event_df, invasion)

    sql_path = REPO_ROOT / "qc_framework_v1" / "migrations" / SQL_NAME
    report_path = REPO_ROOT / "qc_framework_v1" / "reports" / REPORT_NAME
    write_sql(sql_path)
    write_report(report_path, patient_shift, event_shift, counts, event_df, patient_df, registry_count)
    write_manifest(counts, patient_shift, event_shift)

    print(f"Wrote {sql_path.relative_to(REPO_ROOT)}")
    print(f"Wrote {report_path.relative_to(REPO_ROOT)}")
    for name, count in sorted(counts.items()):
        print(f"Wrote {(ADJ_DIR / name).relative_to(REPO_ROOT)}: {count:,} rows")
    print(f"Wrote {(MANIFEST_DIR / 'manifest.json').relative_to(REPO_ROOT)}")
    print("Patient shift summary:")
    print(patient_shift.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
