#!/usr/bin/env python3
"""mig_184 R1 AJCC derivation rule scoping.

Read-only MotherDuck scoping lane for CF-87-AJCC closure. The script only
runs SELECT statements against the locked publication database and writes local
artifacts: a full AJCC7/8 derivation report, four Logan-adjudication CSVs, and a
non-executable placeholder apply SQL skeleton.
"""

from __future__ import annotations

import argparse
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

RUN_ID = "mig184_r1_ajcc_derivation_scoping_20260430"
REPORT_NAME = "mig_184_r1_ajcc_derivation_scoping_20260430.md"
SQL_NAME = "184_r1_ajcc_derivation_skeleton_20260430.sql"
ADJ_DIR = REPO_ROOT / "exports" / "mig184_r1_adjudication_20260430"

T_SEVERITY = {"T4B": 8, "T4A": 7, "T4": 6, "T3B": 5, "T3A": 4, "T3": 3, "T2": 2, "T1B": 1.2, "T1A": 1.1, "T1": 1, "TX": 0}
N_SEVERITY = {"N1B": 3, "N1A": 2, "N1": 1, "NX": 0, "N0B": 0, "N0A": 0, "N0": 0}
M_SEVERITY = {"M1": 1, "MX": 0, "M0": 0}
STAGE_SEVERITY = {"IVC": 9, "IVB": 8, "IVA": 7, "IV": 6, "III": 5, "II": 4, "I": 3}

T4A_TERMS = re.compile(r"trache|laryn|cricoid|esophag|recurrent\s+laryngeal|\brln\b|subcutaneous|airway", re.I)
T4B_TERMS = re.compile(r"prevertebral|carotid|mediastinal\s+vessel|encas", re.I)
GROSS_ETE_TERMS = re.compile(r"gross|macroscopic|strap|skeletal\s+muscle|sternothyroid|sternohyoid|omohyoid|thyrohyoid", re.I)
MICRO_ETE_TERMS = re.compile(r"microscopic|minimal|focal", re.I)
DTC_TERMS = re.compile(r"papillary|ptc|follicular|ftc|hurthle|h[üu]rthle|hcc|oncocytic|poorly|pdtc", re.I)
MTC_TERMS = re.compile(r"medullary|\bmtc\b", re.I)
ATC_TERMS = re.compile(r"anaplastic|\batc\b", re.I)


def norm_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
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


def is_true(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "t", "1", "yes", "y", "x", "present", "positive"}


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        match = re.search(r"\d+(?:\.\d+)?", str(value))
        return float(match.group(0)) if match else None


def severity(value: Any, mapping: dict[str, float | int]) -> float:
    stage = norm_stage(value)
    if stage is None:
        return -1
    return float(mapping.get(stage, -1))


def normalize_histology(value: Any) -> str:
    text = norm_text(value) or ""
    has_mtc = bool(MTC_TERMS.search(text))
    has_atc = bool(ATC_TERMS.search(text))
    has_dtc = bool(DTC_TERMS.search(text))
    if has_atc:
        return "ATC"
    if has_mtc and has_dtc:
        return "MIXED_MTC_DTC_REVIEW"
    if has_mtc:
        return "MTC"
    if has_dtc:
        return "DTC"
    return "UNKNOWN_REVIEW"


def classify_ete(row: pd.Series) -> str:
    text = norm_text(row.get("extrathyroidal_extension")) or ""
    if T4B_TERMS.search(text):
        return "t4b_anatomic_text"
    if T4A_TERMS.search(text):
        return "t4a_anatomic_text"
    if is_true(row.get("gross_ete")) or GROSS_ETE_TERMS.search(text):
        return "gross_unlocalized"
    if MICRO_ETE_TERMS.search(text):
        return "microscopic"
    if text.lower() in {"no", "none", "negative", "absent", "not identified"}:
        return "absent"
    if text:
        return "present_ungraded"
    return "missing"


def derive_t8(row: pd.Series) -> str | None:
    ete = classify_ete(row)
    size = safe_float(row.get("size_greatest_dimension_cm"))
    if ete == "t4b_anatomic_text":
        return "T4b"
    if ete == "t4a_anatomic_text":
        return "T4a"
    if ete == "gross_unlocalized":
        return "T3b"
    if size is None or size <= 0:
        return None
    if size <= 1.0:
        return "T1a"
    if size <= 2.0:
        return "T1b"
    if size <= 4.0:
        return "T2"
    return "T3a"


def derive_t7(row: pd.Series) -> str | None:
    ete = classify_ete(row)
    size = safe_float(row.get("size_greatest_dimension_cm"))
    if ete == "t4b_anatomic_text":
        return "T4b"
    if ete == "t4a_anatomic_text":
        return "T4a"
    if ete in {"gross_unlocalized", "microscopic", "present_ungraded"}:
        return "T3"
    if size is None or size <= 0:
        return None
    if size <= 1.0:
        return "T1a"
    if size <= 2.0:
        return "T1b"
    if size <= 4.0:
        return "T2"
    return "T3"


def derive_n_coarse(row: pd.Series) -> str | None:
    involved = safe_float(row.get("ln_involved"))
    examined = safe_float(row.get("ln_examined"))
    positive_count = safe_float(row.get("nodal_disease_positive_count"))
    total_count = safe_float(row.get("nodal_disease_total_count"))
    if (involved is not None and involved > 0) or (positive_count is not None and positive_count > 0):
        return "N1"
    if (examined is not None and examined > 0) or (total_count is not None and total_count > 0):
        return "N0"
    return None


def worst_stage(values: pd.Series, mapping: dict[str, float | int]) -> str | None:
    pairs = [(severity(v, mapping), norm_stage(v)) for v in values.tolist()]
    pairs = [p for p in pairs if p[1] is not None]
    if not pairs:
        return None
    return max(pairs, key=lambda p: p[0])[1]


def stage_group_ajcc8_dtc(t: Any, n: Any, m: Any, age: Any) -> str | None:
    t_s = norm_stage(t)
    n_s = norm_stage(n)
    m_s = norm_stage(m) or "M0"
    age_f = safe_float(age)
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
    return None


def stage_group_ajcc7_dtc(t: Any, n: Any, m: Any, age: Any) -> str | None:
    t_s = norm_stage(t)
    n_s = norm_stage(n)
    m_s = norm_stage(m) or "M0"
    age_f = safe_float(age)
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


def derive_stage_group(histology_class: str, t8: Any, t7: Any, n: Any, m: Any, age: Any, edition: int) -> str | None:
    if histology_class == "ATC":
        m_s = norm_stage(m) or "M0"
        if m_s == "M1":
            return "IVC" if edition == 7 else "IVB"
        t_s = norm_stage(t8 if edition == 8 else t7)
        return "IVA" if t_s in {"T1", "T1A", "T1B", "T2", "T3", "T3A", "T3B"} else "IVB"
    if histology_class == "MTC":
        return stage_group_mtc(t8 if edition == 8 else t7, n, m)
    if histology_class == "DTC":
        return stage_group_ajcc8_dtc(t8, n, m, age) if edition == 8 else stage_group_ajcc7_dtc(t7, n, m, age)
    return None


def fetch_path_events(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            surgery_episode_id,
            tumor_ordinal,
            surgery_date,
            size_greatest_dimension_cm,
            extrathyroidal_extension,
            gross_ete,
            ln_examined,
            ln_involved,
            nodal_disease_positive_count,
            nodal_disease_total_count,
            extranodal_extension,
            primary_histology,
            t_stage_ajcc7,
            n_stage_ajcc7,
            m_stage_ajcc7,
            stage_group_ajcc7,
            t_stage_ajcc8,
            n_stage_ajcc8,
            m_stage_ajcc8,
            stage_group_ajcc8,
            overall_stage_ajcc7,
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
            dominant_tumor_ajcc8_stage_group
        FROM main.canonical_patient_master
        """
    ).fetchdf()


def fetch_invasion(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            linked_surgery_episode_id AS surgery_episode_id,
            linked_path_malignant_event_id,
            invasion_type,
            finding_status,
            evidence_qualifier,
            confidence,
            source_kind,
            source_modality,
            source_table,
            linkage_method,
            linkage_ambiguous_multi_finding,
            n_candidate_episodes
        FROM main.canonical_invasion_events_v1
        """
    ).fetchdf()


def derive_event_level(path_df: pd.DataFrame) -> pd.DataFrame:
    df = path_df.copy()
    df["ete_rule_bucket"] = df.apply(classify_ete, axis=1)
    df["t_stage_ajcc8_resolved_draft"] = df.apply(derive_t8, axis=1)
    df["t_stage_ajcc7_resolved_draft"] = df.apply(derive_t7, axis=1)
    df["n_stage_resolved_coarse_draft"] = df.apply(derive_n_coarse, axis=1)
    df["m_stage_resolved_draft"] = df["m_stage_ajcc8"].map(norm_stage)
    df["histology_class_draft"] = df["primary_histology"].map(normalize_histology)
    return df


def patient_rollup(event_df: pd.DataFrame, cpm: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for rid, sub in event_df.groupby("research_id", dropna=False):
        row = {"research_id": rid, "n_path_event_rows": int(len(sub))}
        row["t_stage_ajcc8_resolved_draft"] = worst_stage(sub["t_stage_ajcc8_resolved_draft"], T_SEVERITY)
        row["t_stage_ajcc7_resolved_draft"] = worst_stage(sub["t_stage_ajcc7_resolved_draft"], T_SEVERITY)
        row["n_stage_resolved_coarse_draft"] = worst_stage(sub["n_stage_resolved_coarse_draft"], N_SEVERITY)
        row["m_stage_resolved_draft"] = worst_stage(sub["m_stage_resolved_draft"], M_SEVERITY) or "M0"
        classes = [c for c in sub["histology_class_draft"].dropna().tolist() if c != "UNKNOWN_REVIEW"]
        if "ATC" in classes:
            row["histology_class_draft"] = "ATC"
        elif "MIXED_MTC_DTC_REVIEW" in classes:
            row["histology_class_draft"] = "MIXED_MTC_DTC_REVIEW"
        elif "MTC" in classes:
            row["histology_class_draft"] = "MTC"
        elif "DTC" in classes:
            row["histology_class_draft"] = "DTC"
        else:
            row["histology_class_draft"] = "UNKNOWN_REVIEW"
        rows.append(row)
    roll = pd.DataFrame(rows)
    merged = cpm.merge(roll, on="research_id", how="left")
    # Use CPM histology as fallback/override when path-event primary_histology is unavailable.
    cpm_class = (merged["histology_final"].fillna("") + " | " + merged["histologic_types_all"].fillna("")).map(normalize_histology)
    merged["histology_class_for_stage_draft"] = merged["histology_class_draft"].where(
        merged["histology_class_draft"].notna() & (merged["histology_class_draft"] != "UNKNOWN_REVIEW"), cpm_class
    )
    merged["stage_group_ajcc8_resolved_draft"] = merged.apply(
        lambda r: derive_stage_group(
            r.get("histology_class_for_stage_draft"),
            r.get("t_stage_ajcc8_resolved_draft"),
            r.get("t_stage_ajcc7_resolved_draft"),
            r.get("n_stage_resolved_coarse_draft"),
            r.get("m_stage_resolved_draft"),
            r.get("age_at_surgery"),
            8,
        ),
        axis=1,
    )
    merged["stage_group_ajcc7_resolved_draft"] = merged.apply(
        lambda r: derive_stage_group(
            r.get("histology_class_for_stage_draft"),
            r.get("t_stage_ajcc8_resolved_draft"),
            r.get("t_stage_ajcc7_resolved_draft"),
            r.get("n_stage_resolved_coarse_draft"),
            r.get("m_stage_resolved_draft"),
            r.get("age_at_surgery"),
            7,
        ),
        axis=1,
    )
    return merged


def compare_stage(df: pd.DataFrame, current: str, draft: str) -> dict[str, Any]:
    cur = df[current].map(norm_stage) if current in df else pd.Series([None] * len(df), index=df.index)
    new = df[draft].map(norm_stage) if draft in df else pd.Series([None] * len(df), index=df.index)
    paired = cur.notna() & new.notna()
    changed = paired & (cur != new)
    return {
        "component": draft,
        "current_column": current,
        "rows_total": int(len(df)),
        "current_non_null": int(cur.notna().sum()),
        "draft_non_null": int(new.notna().sum()),
        "paired_non_null": int(paired.sum()),
        "paired_changes": int(changed.sum()),
        "paired_change_pct": round((int(changed.sum()) / int(paired.sum()) * 100) if int(paired.sum()) else 0, 2),
        "draft_uncalculable": int(new.isna().sum()),
    }


def build_shift_summary(patient_df: pd.DataFrame, event_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    malignant = patient_df[patient_df["is_malignant"].map(is_true)].copy()
    patient_summary = pd.DataFrame(
        [
            compare_stage(malignant, "ajcc8_t_stage", "t_stage_ajcc8_resolved_draft"),
            compare_stage(malignant, "ajcc8_n_stage", "n_stage_resolved_coarse_draft"),
            compare_stage(malignant, "ajcc8_m_stage", "m_stage_resolved_draft"),
            compare_stage(malignant, "ajcc8_stage_group", "stage_group_ajcc8_resolved_draft"),
            compare_stage(malignant, "ajcc7_t_stage", "t_stage_ajcc7_resolved_draft"),
            compare_stage(malignant, "ajcc7_n_stage", "n_stage_resolved_coarse_draft"),
            compare_stage(malignant, "ajcc7_m_stage", "m_stage_resolved_draft"),
            compare_stage(malignant, "ajcc7_stage_group", "stage_group_ajcc7_resolved_draft"),
        ]
    )
    event_summary = pd.DataFrame(
        [
            compare_stage(event_df, "t_stage_ajcc8", "t_stage_ajcc8_resolved_draft"),
            compare_stage(event_df, "n_stage_ajcc8", "n_stage_resolved_coarse_draft"),
            compare_stage(event_df, "m_stage_ajcc8", "m_stage_resolved_draft"),
            compare_stage(event_df, "t_stage_ajcc7", "t_stage_ajcc7_resolved_draft"),
            compare_stage(event_df, "n_stage_ajcc7", "n_stage_resolved_coarse_draft"),
            compare_stage(event_df, "m_stage_ajcc7", "m_stage_resolved_draft"),
        ]
    )
    return patient_summary, event_summary


def blank_adjudication_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["logan_decision"] = ""
    df["logan_notes"] = ""
    # Reviewer CSVs should be one physical line per logical row. Some source
    # staging notes contain embedded newlines, which are legal CSV but make
    # wc/head validation and spreadsheet review cumbersome.
    text_cols = [col for col in df.columns if is_object_dtype(df[col]) or is_string_dtype(df[col])]
    for col in text_cols:
        df[col] = df[col].map(lambda v: re.sub(r"[\r\n]+", " ", v) if isinstance(v, str) else v)
    return df


def write_adjudication_csvs(event_df: pd.DataFrame, cpm: pd.DataFrame, invasion_df: pd.DataFrame) -> dict[str, int]:
    ADJ_DIR.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    n1 = event_df[event_df["n_stage_ajcc8"].map(norm_stage).eq("N1")].copy()
    n1["proposed_action"] = "logan_ratify_defer_as_N1_or_split_with_cervical_ln_sources"
    n1["staging_source_note"] = n1["staging_source_note"].fillna("n_stage_ajcc8=N1 without anatomic location in path event")
    n1_cols = [
        "research_id", "surgery_episode_id", "tumor_ordinal", "surgery_date", "primary_histology",
        "n_stage_ajcc8", "ln_examined", "ln_involved", "nodal_disease_positive_count",
        "nodal_disease_total_count", "extranodal_extension", "proposed_action", "staging_source_note",
    ]
    n1_out = blank_adjudication_cols(n1[n1_cols])
    n1_out.to_csv(ADJ_DIR / "r1b_n1_unspecified_no_location.csv", index=False)
    counts["r1b_n1_unspecified_no_location.csv"] = len(n1_out)

    size_missing = event_df[event_df["size_greatest_dimension_cm"].isna() | (event_df["size_greatest_dimension_cm"].map(lambda v: (safe_float(v) or 0) <= 0))].copy()
    size_missing["proposed_action"] = "review_size_source_or_leave_t_stage_resolved_null"
    size_missing["staging_source_note"] = size_missing["staging_source_note"].fillna("size_greatest_dimension_cm unavailable or non-positive")
    size_cols = [
        "research_id", "surgery_episode_id", "tumor_ordinal", "surgery_date", "primary_histology",
        "size_greatest_dimension_cm", "extrathyroidal_extension", "gross_ete", "t_stage_ajcc7",
        "t_stage_ajcc8", "t_stage_ajcc8_resolved_draft", "proposed_action", "staging_source_note",
    ]
    size_out = blank_adjudication_cols(size_missing[size_cols])
    size_out.to_csv(ADJ_DIR / "r1c_size_unavailable_t_uncalculable.csv", index=False)
    counts["r1c_size_unavailable_t_uncalculable.csv"] = len(size_out)

    inv = invasion_df[
        invasion_df["invasion_type"].astype(str).str.lower().isin(["airway", "tracheal", "esophageal", "soft_tissue"])
        & ~invasion_df["finding_status"].astype(str).str.lower().eq("absent")
    ].copy()
    if not inv.empty:
        agg = (
            inv.groupby(["research_id", "surgery_episode_id", "invasion_type", "finding_status", "evidence_qualifier"], dropna=False)
            .agg(
                n_events=("invasion_type", "size"),
                max_confidence=("confidence", "max"),
                source_tables=("source_table", lambda s: " | ".join(sorted({str(x) for x in s.dropna()}))[:500]),
                source_modalities=("source_modality", lambda s: " | ".join(sorted({str(x) for x in s.dropna()}))[:500]),
                any_ambiguous=("linkage_ambiguous_multi_finding", lambda s: bool(pd.Series(s).map(is_true).any())),
                max_candidate_episodes=("n_candidate_episodes", "max"),
            )
            .reset_index()
        )
    else:
        agg = pd.DataFrame(columns=["research_id", "surgery_episode_id", "invasion_type", "finding_status", "evidence_qualifier", "n_events", "max_confidence", "source_tables", "source_modalities", "any_ambiguous", "max_candidate_episodes"])
    path_t4 = event_df[event_df["t_stage_ajcc8"].map(norm_stage).isin(["T4A", "T4B"]) | event_df["ete_rule_bucket"].isin(["t4a_anatomic_text", "t4b_anatomic_text"])].copy()
    path_t4 = path_t4[["research_id", "surgery_episode_id", "tumor_ordinal", "surgery_date", "t_stage_ajcc8", "extrathyroidal_extension", "gross_ete", "ete_rule_bucket", "staging_source_note"]]
    path_t4["invasion_type"] = "path_event_t4_or_anatomic_ete_text"
    path_t4["finding_status"] = "review"
    path_t4["evidence_qualifier"] = path_t4["extrathyroidal_extension"]
    path_t4["n_events"] = 1
    path_t4["max_confidence"] = None
    path_t4["source_tables"] = "canonical_path_malignant_events_v1"
    path_t4["source_modalities"] = "path_event"
    path_t4["any_ambiguous"] = False
    path_t4["max_candidate_episodes"] = None
    path_t4["path_current_t_stage_ajcc8"] = path_t4["t_stage_ajcc8"]
    path_t4["path_ete_rule_bucket"] = path_t4["ete_rule_bucket"]
    for col in ["path_current_t_stage_ajcc8", "path_ete_rule_bucket"]:
        if col not in agg:
            agg[col] = None
    agg["tumor_ordinal"] = None
    agg["surgery_date"] = None
    agg["path_current_t_stage_ajcc8"] = None
    agg["path_ete_rule_bucket"] = None
    common_cols = [
        "research_id", "surgery_episode_id", "tumor_ordinal", "surgery_date", "invasion_type",
        "finding_status", "evidence_qualifier", "n_events", "max_confidence", "source_tables",
        "source_modalities", "any_ambiguous", "max_candidate_episodes", "path_current_t_stage_ajcc8",
        "path_ete_rule_bucket",
    ]
    t4_out = pd.concat([agg[common_cols], path_t4[common_cols]], ignore_index=True)
    t4_out["proposed_action"] = "logan_map_invasion_type_to_T4a_T4b_or_exclude"
    t4_out["staging_source_note"] = "T4 anatomic evidence review from canonical_invasion_events_v1 and path-event T4/anatomic ETE text"
    t4_out = blank_adjudication_cols(t4_out.sort_values(["research_id", "surgery_episode_id", "invasion_type", "evidence_qualifier"], na_position="last"))
    t4_out.to_csv(ADJ_DIR / "r1d_t4_invasion_evidence_review.csv", index=False)
    counts["r1d_t4_invasion_evidence_review.csv"] = len(t4_out)

    age = cpm[cpm["age_at_surgery"].map(lambda v: (safe_float(v) is not None) and (53 <= safe_float(v) <= 57))].copy()
    age["histology_class_draft"] = (age["histology_final"].fillna("") + " | " + age["histologic_types_all"].fillna("")).map(normalize_histology)
    age["proposed_action"] = "logan_ratify_broadcast_PM_age_to_patient_stage_group_only"
    age["staging_source_note"] = "age_at_surgery within +/-2 years of AJCC8 DTC cutoff 55"
    age_cols = [
        "research_id", "age_at_surgery", "is_malignant", "histology_final", "histologic_types_all",
        "histology_class_draft", "ajcc8_t_stage", "ajcc8_n_stage", "ajcc8_m_stage", "ajcc8_stage_group",
        "ajcc7_stage_group", "proposed_action", "staging_source_note",
    ]
    age_out = blank_adjudication_cols(age[age_cols].sort_values(["age_at_surgery", "research_id"]))
    age_out.to_csv(ADJ_DIR / "r1e_stage_group_age_cutoff_review.csv", index=False)
    counts["r1e_stage_group_age_cutoff_review.csv"] = len(age_out)

    return counts


def rule_gap_table(event_df: pd.DataFrame, invasion_df: pd.DataFrame, cpm: pd.DataFrame) -> pd.DataFrame:
    size_missing = int(event_df["size_greatest_dimension_cm"].isna().sum())
    ete_distinct = int(event_df["extrathyroidal_extension"].dropna().astype(str).str.strip().nunique())
    n1_unspec = int(event_df["n_stage_ajcc8"].map(norm_stage).eq("N1").sum())
    t4_inv = int(
        invasion_df[
            invasion_df["invasion_type"].astype(str).str.lower().isin(["airway", "tracheal", "esophageal", "soft_tissue"])
            & ~invasion_df["finding_status"].astype(str).str.lower().eq("absent")
        ].shape[0]
    )
    age_boundary = int(cpm["age_at_surgery"].map(lambda v: (safe_float(v) is not None) and (53 <= safe_float(v) <= 57)).sum())
    hist_review = int(cpm[(cpm["histology_final"].fillna("") + " | " + cpm["histologic_types_all"].fillna("")).map(normalize_histology).isin(["MIXED_MTC_DTC_REVIEW", "UNKNOWN_REVIEW"])].shape[0])
    return pd.DataFrame(
        [
            {"rule": "T1/T2/T3 size cutoffs", "required_inputs": "size_greatest_dimension_cm", "availability": "available as DOUBLE on canonical_path_malignant_events_v1", "live_gap_count": size_missing, "adjudication_needed": "YES for size-unavailable rows", "disposition": "Export r1c; leave resolved T NULL unless Logan ratifies feeder fallback."},
            {"rule": "ETE text to none/micro/gross/T4", "required_inputs": "extrathyroidal_extension, gross_ete", "availability": f"available but messy ({ete_distinct} distinct non-null ETE strings)", "live_gap_count": int(event_df[event_df["ete_rule_bucket"].isin(["present_ungraded", "gross_unlocalized"])].shape[0]), "adjudication_needed": "YES", "disposition": "Use r1a and report vocabulary; Logan ratifies text mapping before apply."},
            {"rule": "T3b gross ETE strap muscles", "required_inputs": "muscle-specific gross ETE evidence", "availability": "not directly available on path-event; gross_ete is unlocalized", "live_gap_count": int(event_df[event_df["ete_rule_bucket"].eq("gross_unlocalized")].shape[0]), "adjudication_needed": "YES", "disposition": "Logan decides gross_ete -> T3b vs ambiguous bucket."},
            {"rule": "T4a/T4b anatomic invasion", "required_inputs": "airway/trachea/esophagus/RLN/prevertebral/carotid/mediastinal", "availability": "partially available via canonical_invasion_events_v1 plus path-event ETE text", "live_gap_count": t4_inv, "adjudication_needed": "YES", "disposition": "Export r1d; Logan maps invasion_type/status/qualifier to T4a/T4b/exclude."},
            {"rule": "N1a/N1b split", "required_inputs": "nodal level/location", "availability": "not available on path-event; only ln_involved/counts and ENE", "live_gap_count": n1_unspec, "adjudication_needed": "YES", "disposition": "Export r1b; ratify defer-as-N1 or join separate cervical LN sources."},
            {"rule": "Stage group age cutoff", "required_inputs": "age_at_surgery", "availability": "available on CPM only", "live_gap_count": age_boundary, "adjudication_needed": "YES", "disposition": "Export r1e; recommend patient-grain stage group only using PM age."},
            {"rule": "DTC vs MTC vs ATC stage grouping", "required_inputs": "primary_histology / histology_final", "availability": "available but mixed/unknown cases remain", "live_gap_count": hist_review, "adjudication_needed": "YES", "disposition": "Logan ratifies mixed histology precedence; use DTC/MTC/ATC-specific CASE branches."},
        ]
    )


def markdown_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    if max_rows is not None:
        df = df.head(max_rows)
    if df.empty:
        return "_(no rows)_"
    return df.to_markdown(index=False)


def write_skeleton_sql(path: Path) -> None:
    path.write_text(
        "-- mig_184 — R1 AJCC derivation skeleton (PLACEHOLDER; DO NOT EXECUTE)\n"
        "-- Target DB: thyroid_canonical_publication_v1_0\n"
        "-- Governance: LOGAN MUST RATIFY RULES BEFORE EXECUTION.\n"
        "-- This file is intentionally non-runnable: <TBD_LOGAN_RATIFIED_RULE_*> placeholders remain.\n\n"
        "-- §0 pre-flight (apply lane only)\n"
        "SELECT COUNT(*) AS cpm_rows, COUNT(DISTINCT research_id) AS cpm_distinct_research_id\n"
        "FROM main.canonical_patient_master;\n\n"
        "-- §A pre-snapshot of future write targets\n"
        "-- LOGAN MUST RATIFY RULES BEFORE EXECUTION.\n"
        "-- CREATE TABLE \"Thyroid 2026 UPdated\".archive_pub_v1_0.canonical_patient_master_pre_mig184_<UTC> AS\n"
        "-- SELECT research_id, ajcc7_t_stage, ajcc7_n_stage, ajcc7_m_stage, ajcc7_stage_group,\n"
        "--        ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group\n"
        "-- FROM main.canonical_patient_master;\n\n"
        "-- §B add resolved columns (PLACEHOLDER)\n"
        "-- LOGAN MUST RATIFY RULES BEFORE EXECUTION.\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS t_stage_ajcc8_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS n_stage_ajcc8_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS m_stage_ajcc8_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS t_stage_ajcc7_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS n_stage_ajcc7_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS m_stage_ajcc7_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_resolution_source VARCHAR;\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_resolution_confidence VARCHAR;\n"
        "-- ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_logan_adjudicated_flag BOOLEAN;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_t_stage_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_n_stage_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_m_stage_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_stage_group_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_t_stage_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_n_stage_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_m_stage_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_stage_group_resolved VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc_resolution_source VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc_resolution_confidence VARCHAR;\n"
        "-- ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc_logan_adjudicated_flag BOOLEAN;\n\n"
        "-- §C update from ratified derivation rules (PLACEHOLDER)\n"
        "-- LOGAN MUST RATIFY RULES BEFORE EXECUTION.\n"
        "-- WITH event_derivation AS (\n"
        "--   SELECT research_id, surgery_episode_id, tumor_ordinal,\n"
        "--          CASE\n"
        "--            WHEN <TBD_LOGAN_RATIFIED_RULE_T4B_ANATOMIC_INVASION> THEN 'T4b'\n"
        "--            WHEN <TBD_LOGAN_RATIFIED_RULE_T4A_ANATOMIC_INVASION> THEN 'T4a'\n"
        "--            WHEN <TBD_LOGAN_RATIFIED_RULE_GROSS_ETE_TO_T3B> THEN 'T3b'\n"
        "--            WHEN size_greatest_dimension_cm <= 1 THEN 'T1a'\n"
        "--            WHEN size_greatest_dimension_cm <= 2 THEN 'T1b'\n"
        "--            WHEN size_greatest_dimension_cm <= 4 THEN 'T2'\n"
        "--            WHEN size_greatest_dimension_cm > 4 THEN 'T3a'\n"
        "--          END AS t_stage_ajcc8_resolved,\n"
        "--          CASE\n"
        "--            WHEN <TBD_LOGAN_RATIFIED_RULE_N1B_LOCATION> THEN 'N1b'\n"
        "--            WHEN <TBD_LOGAN_RATIFIED_RULE_N1A_LOCATION> THEN 'N1a'\n"
        "--            WHEN ln_involved > 0 THEN 'N1'\n"
        "--            WHEN ln_examined > 0 AND COALESCE(ln_involved, 0) = 0 THEN 'N0'\n"
        "--          END AS n_stage_ajcc8_resolved,\n"
        "--          <TBD_LOGAN_RATIFIED_RULE_M_STAGE_SOURCE> AS m_stage_ajcc8_resolved\n"
        "--   FROM main.canonical_path_malignant_events_v1\n"
        "-- )\n"
        "-- UPDATE main.canonical_path_malignant_events_v1 AS tgt\n"
        "-- SET t_stage_ajcc8_resolved = src.t_stage_ajcc8_resolved,\n"
        "--     n_stage_ajcc8_resolved = src.n_stage_ajcc8_resolved,\n"
        "--     m_stage_ajcc8_resolved = src.m_stage_ajcc8_resolved,\n"
        "--     ajcc_resolution_source = 'mig184_r1_ratified_findings_derivation',\n"
        "--     ajcc_resolution_confidence = <TBD_LOGAN_RATIFIED_CONFIDENCE_RULE>,\n"
        "--     ajcc_logan_adjudicated_flag = <TBD_LOGAN_RATIFIED_FLAG_RULE>\n"
        "-- FROM event_derivation AS src\n"
        "-- WHERE CAST(tgt.research_id AS VARCHAR) = CAST(src.research_id AS VARCHAR)\n"
        "--   AND tgt.surgery_episode_id IS NOT DISTINCT FROM src.surgery_episode_id\n"
        "--   AND tgt.tumor_ordinal IS NOT DISTINCT FROM src.tumor_ordinal;\n\n"
        "-- §D registry note appendix closing CF-87-AJCC (PLACEHOLDER)\n"
        "-- LOGAN MUST RATIFY RULES BEFORE EXECUTION.\n"
        "-- UPDATE main.canonical_column_verification_registry_v1\n"
        "-- SET notes = notes || ' | mig184: CF-87-AJCC closed by Logan-ratified R1 resolved AJCC derivation; legacy columns preserved.'\n"
        "-- WHERE notes ILIKE '%CF-87-AJCC%' OR notes ILIKE '%CF-87%AJCC%';\n\n"
        "-- §E provenance row insert (PLACEHOLDER)\n"
        "-- LOGAN MUST RATIFY RULES BEFORE EXECUTION.\n"
        "-- INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1\n"
        "--   (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)\n"
        "-- VALUES\n"
        "--   ('mig_184_r1_ajcc_apply_<DATE>', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'ajcc7_8_resolved_derivation', 'CF-87-AJCC', 'TBD', 'TBD', 'TBD');\n"
    )


def write_report(
    report_path: Path,
    event_df: pd.DataFrame,
    patient_df: pd.DataFrame,
    gap_df: pd.DataFrame,
    patient_shift: pd.DataFrame,
    event_shift: pd.DataFrame,
    adj_counts: dict[str, int],
) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    ete_counts = event_df["ete_rule_bucket"].value_counts(dropna=False).reset_index()
    ete_counts.columns = ["ete_rule_bucket", "n_events"]
    hist_counts = patient_df["histology_class_for_stage_draft"].value_counts(dropna=False).reset_index()
    hist_counts.columns = ["histology_class_for_stage_draft", "n_patients"]
    lines = [
        "# mig_184 — R1 AJCC derivation rule scoping",
        "",
        f"**Run ID:** `{RUN_ID}`  ",
        f"**Run timestamp (UTC):** `{timestamp}`  ",
        "**Posture:** read-only MotherDuck scoping; no production DDL/DML executed.  ",
        "**Target DB:** `thyroid_canonical_publication_v1_0`  ",
        "**Carry-forward:** `CF-87-AJCC` closure track; Logan ratified R1 as a future apply strategy, but rules remain pending ratification.  ",
        "",
        "## Executive summary",
        "",
        f"- Queried **{len(event_df):,}** rows from `main.canonical_path_malignant_events_v1` and **{len(patient_df):,}** rows from `main.canonical_patient_master` with read-only SELECTs.",
        "- Authored a non-executable skeleton apply SQL with explicit `LOGAN MUST RATIFY RULES BEFORE EXECUTION` markers and `<TBD_LOGAN_RATIFIED_RULE_*>` placeholders.",
        f"- Exported four new Logan-adjudication CSVs in `exports/mig184_r1_adjudication_20260430/`; existing r1a was preserved. New row counts: {', '.join(f'{k}={v:,}' for k, v in sorted(adj_counts.items()))}.",
        "- Draft shift counts below are **scoping estimates only** using conservative placeholder derivation rules; they are not apply instructions.",
        "",
        "## 1. Full AJCC7/8 rule spec",
        "",
        "### AJCC8 T component (DTC/MTC path-event derivation)",
        "",
        "- `T1a`: tumor ≤1.0 cm, limited to thyroid, no gross/anatomic ETE.",
        "- `T1b`: tumor >1.0 cm and ≤2.0 cm, limited to thyroid, no gross/anatomic ETE.",
        "- `T2`: tumor >2.0 cm and ≤4.0 cm, limited to thyroid, no gross/anatomic ETE.",
        "- `T3a`: tumor >4.0 cm limited to thyroid. Prompt text also listed a strap-muscle alternate under T3a, but the AJCC8 operational rule assigns gross strap-muscle ETE to `T3b`; Logan should ratify final wording.",
        "- `T3b`: gross ETE invading only strap muscles (sternohyoid, sternothyroid, thyrohyoid, omohyoid) regardless of size.",
        "- `T4a`: gross ETE invading subcutaneous soft tissues, larynx, trachea, esophagus, or recurrent laryngeal nerve.",
        "- `T4b`: gross ETE invading prevertebral fascia, mediastinal vessels, or encasing carotid artery.",
        "- Microscopic/minimal ETE does **not** upstage T1/T2 in AJCC8.",
        "",
        "### AJCC7 T component (DTC/MTC path-event derivation)",
        "",
        "- `T1a`: tumor ≤1.0 cm, limited to thyroid.",
        "- `T1b`: tumor >1.0 cm and ≤2.0 cm, limited to thyroid.",
        "- `T2`: tumor >2.0 cm and ≤4.0 cm, limited to thyroid.",
        "- `T3`: tumor >4.0 cm limited to thyroid **or** minimal/microscopic ETE into sternothyroid muscle/perithyroid soft tissues.",
        "- `T4a`: moderately advanced disease with gross ETE into subcutaneous soft tissues, larynx, trachea, esophagus, or recurrent laryngeal nerve.",
        "- `T4b`: very advanced disease invading prevertebral fascia, mediastinal vessels, or encasing carotid artery.",
        "",
        "### AJCC8 N component",
        "",
        "- `N0`: no nodal metastases; path-event proxy is `ln_examined > 0` and `ln_involved = 0`.",
        "- `N0a`: cytologically/histologically confirmed benign nodes.",
        "- `N0b`: no radiologic/clinical evidence of nodal disease.",
        "- `N1a`: level VI or VII central-compartment/upper-mediastinal nodal metastases.",
        "- `N1b`: unilateral, bilateral, contralateral lateral cervical levels I–V or retropharyngeal nodal metastases.",
        "- Current path-event grain lacks anatomic nodal level/location, so `N1a`/`N1b` splitting requires Logan-ratified supplemental-source rules.",
        "",
        "### AJCC7 N component",
        "",
        "- `N0`: no regional nodal metastases.",
        "- `N1a`: level VI central compartment metastases (pretracheal, paratracheal, prelaryngeal/Delphian).",
        "- `N1b`: unilateral/bilateral/contralateral cervical or superior mediastinal metastases.",
        "- AJCC7 vs AJCC8 differs for upper mediastinal/level VII handling; ratification must explicitly encode edition-specific mapping.",
        "",
        "### M component",
        "",
        "- `M0`: no distant metastases.",
        "- `M1`: distant metastases present.",
        "- Path-event rows currently carry copied `m_stage_ajcc7`/`m_stage_ajcc8`; the verified-finding source for a true re-derivation must be ratified before apply.",
        "",
        "### AJCC8 stage group — differentiated thyroid carcinoma (DTC)",
        "",
        "- Age <55: `I` for any T/any N/M0; `II` for any T/any N/M1.",
        "- Age ≥55: `I` for T1–T2/N0 or NX/M0; `II` for T1–T2/N1/M0 or T3a–T3b/any N/M0; `III` for T4a/any N/M0; `IVA` for T4b/any N/M0; `IVB` for any T/any N/M1.",
        "",
        "### AJCC7 stage group — differentiated thyroid carcinoma (DTC)",
        "",
        "- Age <45: `I` for any T/any N/M0; `II` for any T/any N/M1.",
        "- Age ≥45: `I` for T1/N0/M0; `II` for T2/N0/M0; `III` for T3/N0/M0 or T1–T3/N1a/M0; `IVA` for T4a/any N/M0 or T1–T3/N1b/M0; `IVB` for T4b/any N/M0; `IVC` for any T/any N/M1.",
        "",
        "### MTC and ATC stage grouping",
        "",
        "- MTC uses age-independent stage grouping: I=T1/N0/M0; II=T2–T3/N0/M0; III=T1–T3/N1a/M0; IVA=T4a/any N/M0 or T1–T3/N1b/M0; IVB=T4b/any N/M0; IVC=M1. Logan must ratify mixed `MTC | PTC` precedence.",
        "- ATC is stage IV by definition; broad grouping should preserve IVA/IVB/IVC where T/M detail is available, otherwise `IV`.",
        "",
        "## 2. Adjudication-gap enumeration",
        "",
        markdown_table(gap_df),
        "",
        "### ETE draft bucket distribution",
        "",
        markdown_table(ete_counts),
        "",
        "### Patient histology-class draft distribution",
        "",
        markdown_table(hist_counts),
        "",
        "## 3. Cross-source drift cohort under proposed R1 draft derivation",
        "",
        "### Patient grain (malignant CPM patients only)",
        "",
        markdown_table(patient_shift),
        "",
        "### Path-event grain",
        "",
        markdown_table(event_shift),
        "",
        "Interpretation: these are draft estimates with unratified rules. The apply lane must not use them until Logan resolves the adjudication CSVs and ratifies source precedence for ETE, anatomic invasion, nodal location, M-stage source, age broadcast, and histology class.",
        "",
        "## 4. Logan adjudication CSVs",
        "",
        "| CSV | rows | purpose |",
        "|---|---:|---|",
        "| r1a_ete_t_stage_upgrade_review.csv | preserved existing | Cowork-generated ETE→T-stage upgrade candidates. |",
        *[f"| {name} | {count:,} | Generated by this mig_184 scoping lane. |" for name, count in sorted(adj_counts.items())],
        "",
        "Minimum common columns include domain identifiers, relevant clinical inputs, `proposed_action`, blank `logan_decision`, blank `logan_notes`, and `staging_source_note`.",
        "",
        "## 5. Recommended Logan dispositions before apply lane",
        "",
        "1. **Ratify ETE text mapping**: decide which raw `extrathyroidal_extension` values map to absent/microscopic/gross/T4 and whether unlocalized `gross_ete=1` should default to AJCC8 `T3b` or remain ambiguous.",
        "2. **Ratify T4 source mapping**: map `canonical_invasion_events_v1.invasion_type` + `finding_status` + `evidence_qualifier` to `T4a`, `T4b`, or exclude. Do not count absent/boilerplate entries.",
        "3. **Ratify N-stage policy**: either keep positive path-event rows as coarse `N1` when location is absent, or define a governed supplemental-source join for central/lateral level mapping.",
        "4. **Ratify M-stage source**: identify verified distant-metastasis source; copied legacy `m_stage_*` is acceptable only if Logan declares it the source of truth for R1.",
        "5. **Ratify stage-group grain**: compute stage group at patient grain using CPM `age_at_surgery`; avoid broadcasting age-derived stage groups back to tumor rows unless explicitly needed.",
        "6. **Ratify histology precedence**: define DTC/MTC/ATC class for mixed histologies such as `MTC | PTC`; route unresolved classes to manual review.",
        "",
        "## Governance boundary",
        "",
        "This lane did not run `ALTER`, `UPDATE`, `CREATE`, `DROP`, registry mutation, or provenance insert against MotherDuck. The skeleton SQL is a placeholder artifact only and intentionally contains unresolved `<TBD_LOGAN_RATIFIED_RULE_*>` markers.",
    ]
    report_path.write_text("\n".join(lines) + "\n")


def write_manifest(out_dir: Path, adj_counts: dict[str, int], patient_shift: pd.DataFrame, event_shift: pd.DataFrame) -> None:
    manifest = {
        "run_id": RUN_ID,
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "posture": "read_only_motherduck_local_artifacts_only",
        "target_db": "thyroid_canonical_publication_v1_0",
        "deliverables": {
            "report": str((REPO_ROOT / "qc_framework_v1" / "reports" / REPORT_NAME).relative_to(REPO_ROOT)),
            "skeleton_sql": str((REPO_ROOT / "qc_framework_v1" / "migrations" / SQL_NAME).relative_to(REPO_ROOT)),
            "adjudication_csv_dir": str(ADJ_DIR.relative_to(REPO_ROOT)),
        },
        "adjudication_counts": adj_counts,
        "patient_shift_summary": patient_shift.to_dict(orient="records"),
        "event_shift_summary": event_shift.to_dict(orient="records"),
        "governance": "No production DDL/DML executed; Logan ratification required before apply lane.",
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "exports" / RUN_ID), help="Output directory for manifest/local summaries")
    args = parser.parse_args()

    con = connect_locked()
    path_df = fetch_path_events(con)
    cpm = fetch_cpm(con)
    invasion = fetch_invasion(con)

    event_df = derive_event_level(path_df)
    patient_df = patient_rollup(event_df, cpm)
    patient_shift, event_shift = build_shift_summary(patient_df, event_df)
    gap_df = rule_gap_table(event_df, invasion, cpm)
    adj_counts = write_adjudication_csvs(event_df, cpm, invasion)

    report_path = REPO_ROOT / "qc_framework_v1" / "reports" / REPORT_NAME
    sql_path = REPO_ROOT / "qc_framework_v1" / "migrations" / SQL_NAME
    write_skeleton_sql(sql_path)
    write_report(report_path, event_df, patient_df, gap_df, patient_shift, event_shift, adj_counts)
    write_manifest(Path(args.out_dir), adj_counts, patient_shift, event_shift)

    print(f"Wrote {report_path.relative_to(REPO_ROOT)}")
    print(f"Wrote {sql_path.relative_to(REPO_ROOT)}")
    for name, count in sorted(adj_counts.items()):
        print(f"Wrote {ADJ_DIR.relative_to(REPO_ROOT) / name}: {count:,} rows")
    print(f"Wrote {Path(args.out_dir).relative_to(REPO_ROOT) / 'manifest.json'}")
    print("Patient shift summary:")
    print(patient_shift.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
