#!/usr/bin/env python3
"""
M036: 2025 ATA initial risk stratification comparison.

The classifier is intentionally implemented as pure row-wise Python logic so the
clinical rules can be unit tested without a MotherDuck connection. The database
layer only pulls source columns, writes study artifacts, and optionally uploads
the manuscript workspace table.

Run from repo root:
  .venv/bin/python scripts/m036_ata_2025_rss.py --dry-run
  .venv/bin/python scripts/m036_ata_2025_rss.py
"""
from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motherduck_client import get_token  # noqa: E402

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
OUT_DIR = ROOT / "studies" / "m036_ata_rss_comparison"
OLD_UPLOAD_TABLE = "manuscript_workspace.m036_ata_2025_rss_v1"
UPLOAD_TABLE = "manuscript_workspace.m036_ata_2025_rss_v2"

RISK_ORDER = {"uncalculable": 0, "low": 1, "intermediate": 2, "high": 3}
RISK_LABELS = ["low", "intermediate", "high", "uncalculable"]


@dataclass(frozen=True)
class Ata2025Result:
    category: str
    rule_triggered: str
    missing_inputs: str = ""


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def as_lower(value: Any) -> str:
    if is_missing(value):
        return ""
    return str(value).strip().lower()


def as_float(value: Any) -> float | None:
    if is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> int | None:
    val = as_float(value)
    if val is None or math.isnan(val):
        return None
    return int(val)


def is_true(value: Any) -> bool:
    if value is True:
        return True
    if is_missing(value):
        return False
    return str(value).strip().lower() in {"true", "t", "1", "yes", "y", "x"}


def normalize_ata_2015(value: Any) -> str:
    v = as_lower(value)
    return v if v in {"low", "intermediate", "high"} else "uncalculable"


def is_niftp(histology: str) -> bool:
    return "niftp" in histology or "non-invasive follicular" in histology


def is_ptc(histology: str) -> bool:
    return "ptc" in histology or "papillary" in histology


def is_ftc_like(histology: str) -> bool:
    terms = ("follicular carcinoma", "ftc", "hcc", "hurthle", "hürthle", "oncocytic", "ftump")
    return any(term in histology for term in terms)


def is_non_dtc_histology(histology: str) -> bool:
    non_dtc_terms = (
        "medullary",
        "mtc",
        "anaplastic",
        "poorly differentiated",
        "nut carcinoma",
        "angiosarcoma",
    )
    return histology.startswith("metastatic") or any(term in histology for term in non_dtc_terms)


def is_dtc_or_niftp(histology: str) -> bool:
    if is_niftp(histology) or is_ptc(histology) or is_ftc_like(histology):
        return True
    return False


def is_aggressive_histology(histology: str) -> bool:
    aggressive_terms = (
        "tall cell",
        "hobnail",
        "columnar",
        "diffuse sclerosing",
        "solid",
        "trabecular",
        "poorly differentiated",
        "differentiated high grade",
        "widely invasive",
    )
    return any(term in histology for term in aggressive_terms)


def has_ete(row: dict[str, Any]) -> bool:
    ete = as_lower(row.get("ete_grade_final")) or as_lower(row.get("ete_grade_final_v2"))
    return is_true(row.get("gross_ete_flag")) or ete in {
        "microscopic",
        "gross",
        "minimal",
        "present",
        "present_ungraded",
        "true",
    }


def has_microscopic_ete(row: dict[str, Any]) -> bool:
    ete = as_lower(row.get("ete_grade_final")) or as_lower(row.get("ete_grade_final_v2"))
    return ete in {"microscopic", "minimal", "present", "present_ungraded", "true"}


def has_gross_ete_or_t4(row: dict[str, Any]) -> bool:
    t_stage = as_lower(row.get("ajcc8_t_stage"))
    ete = as_lower(row.get("ete_grade_final")) or as_lower(row.get("ete_grade_final_v2"))
    return is_true(row.get("gross_ete_flag")) or ete == "gross" or t_stage in {"t4a", "t4b"}


def has_any_vascular_invasion(row: dict[str, Any]) -> bool:
    vasc = " ".join(
        as_lower(row.get(c))
        for c in ("vascular_invasion_final", "vascular_invasion_grade", "vascular_who_2022_grade")
    )
    if any(term in vasc for term in ("focal", "extensive", "present", "vessel")):
        return True
    vessel_count = as_float(row.get("vascular_vessel_count"))
    if vessel_count is None:
        vessel_count = as_float(row.get("vasc_vessel_count_v13"))
    return vessel_count is not None and vessel_count > 0


def has_extensive_vascular_invasion(row: dict[str, Any]) -> bool:
    vasc = " ".join(
        as_lower(row.get(c))
        for c in ("vascular_invasion_final", "vascular_invasion_grade", "vascular_who_2022_grade")
    )
    vessel_count = as_float(row.get("vascular_vessel_count"))
    if vessel_count is None:
        vessel_count = as_float(row.get("vasc_vessel_count_v13"))
    return "extensive" in vasc or (vessel_count is not None and vessel_count > 4)


def has_minor_vascular_invasion(row: dict[str, Any], histology: str) -> bool:
    if has_extensive_vascular_invasion(row):
        return False
    if not has_any_vascular_invasion(row):
        return False
    vessel_count = as_float(row.get("vascular_vessel_count"))
    if vessel_count is None:
        vessel_count = as_float(row.get("vasc_vessel_count_v13"))
    if is_ftc_like(histology):
        return vessel_count is None or vessel_count <= 4
    return True


def ln_positive_count(row: dict[str, Any]) -> int | None:
    for col in ("ln_positive_final", "ln_rollup_total_positive", "ln_total_positive"):
        val = as_int(row.get(col))
        if val is not None:
            return val
    return None


def ln_examined_count(row: dict[str, Any]) -> int | None:
    for col in ("ln_rollup_total_examined", "ln_total_examined"):
        val = as_int(row.get(col))
        if val is not None:
            return val
    return None


def ln_largest_deposit_cm(row: dict[str, Any]) -> float | None:
    for col in ("ln_rollup_largest_deposit_cm", "tp_ln_largest_deposit_cm"):
        val = as_float(row.get(col))
        if val is not None:
            return val
    return None


def is_n0(row: dict[str, Any]) -> bool:
    n_stage = as_lower(row.get("ajcc8_n_stage"))
    ln_pos = ln_positive_count(row)
    ln_exam = ln_examined_count(row)
    return n_stage == "n0" or ln_pos == 0 or (ln_pos is None and ln_exam is not None and ln_exam > 0)


def has_distant_mets(row: dict[str, Any]) -> bool:
    return as_lower(row.get("ajcc8_m_stage")) == "m1"


def has_incomplete_resection(row: dict[str, Any]) -> bool:
    margin_classes = {
        as_lower(row.get(c))
        for c in ("margin_r_class_v10", "margin_r_classification", "margin_status_final")
    }
    return "r2" in margin_classes


def has_high_risk_molecular(row: dict[str, Any]) -> bool:
    braf = is_true(row.get("braf_positive_final"))
    tert = (
        is_true(row.get("tert_positive_final"))
        or is_true(row.get("tert_positive_v7"))
        or is_true(row.get("tert_positive"))
    )
    tp53 = is_true(row.get("tp53_positive_v7"))
    tier = as_lower(row.get("molecular_risk_tier"))
    return tert or tp53 or (braf and tert) or tier == "high" or is_true(row.get("high_risk_molecular_v7"))


def has_braf_alone(row: dict[str, Any]) -> bool:
    return (
        is_true(row.get("braf_positive_final"))
        and not has_high_risk_molecular(row)
        and not is_true(row.get("tert_positive_final"))
        and not is_true(row.get("tp53_positive_v7"))
    )


def classify_ata_2025(row_like: dict[str, Any] | pd.Series) -> Ata2025Result:
    row = dict(row_like)
    histology = as_lower(row.get("histology_final"))
    if not histology:
        return Ata2025Result("uncalculable", "uncalculable:missing_histology", "histology_final")
    if is_non_dtc_histology(histology) or not is_dtc_or_niftp(histology):
        return Ata2025Result("uncalculable", "uncalculable:non_dtc_histology", "")

    size = as_float(row.get("tumor_size_cm_dominant"))
    if size is None:
        size = as_float(row.get("tumor_size_cm"))
    ln_pos = ln_positive_count(row)
    ln_size = ln_largest_deposit_cm(row)

    if has_distant_mets(row):
        return Ata2025Result("high", "high:distant_metastasis")
    if has_incomplete_resection(row):
        return Ata2025Result("high", "high:incomplete_resection_r2")
    if has_gross_ete_or_t4(row):
        return Ata2025Result("high", "high:gross_ete_or_t4")
    if has_extensive_vascular_invasion(row):
        return Ata2025Result("high", "high:extensive_vascular_invasion")
    if ln_pos is not None and ln_pos >= 5:
        return Ata2025Result("high", "high:five_or_more_positive_ln")
    if ln_size is not None and ln_size > 3.0:
        return Ata2025Result("high", "high:lymph_node_deposit_gt3cm")
    if has_high_risk_molecular(row):
        return Ata2025Result("high", "high:high_risk_molecular")

    if has_microscopic_ete(row):
        return Ata2025Result("intermediate", "intermediate:microscopic_ete")
    if has_minor_vascular_invasion(row, histology):
        return Ata2025Result("intermediate", "intermediate:minor_vascular_invasion")
    if ln_pos is not None and 1 <= ln_pos <= 4 and (ln_size is None or ln_size <= 3.0):
        return Ata2025Result("intermediate", "intermediate:limited_nodal_metastases")
    if is_ptc(histology) and size is not None and size <= 1.0 and is_true(row.get("multifocal_flag_path")) and has_ete(row):
        return Ata2025Result("intermediate", "intermediate:multifocal_microptc_with_ete")
    if has_braf_alone(row):
        return Ata2025Result("intermediate", "intermediate:braf_v600e_alone")
    if is_aggressive_histology(histology):
        return Ata2025Result("intermediate", "intermediate:aggressive_histology")

    if is_niftp(histology):
        return Ata2025Result("low", "low:niftp")
    if is_ptc(histology) and size is not None and size <= 1.0 and not is_true(row.get("multifocal_flag_path")) and not has_ete(row) and not has_any_vascular_invasion(row) and is_n0(row):
        return Ata2025Result("low", "low:unifocal_papillary_microcarcinoma")
    if is_ptc(histology) and size is not None and size <= 4.0 and not is_aggressive_histology(histology) and not has_ete(row) and not has_any_vascular_invasion(row) and is_n0(row):
        return Ata2025Result("low", "low:intrathyroidal_ptc_le4cm_n0")
    if is_ftc_like(histology) and not has_any_vascular_invasion(row) and not is_aggressive_histology(histology):
        return Ata2025Result("low", "low:minimally_invasive_ftc_no_vascular_invasion")

    missing: list[str] = []
    if size is None:
        missing.append("tumor_size_cm_dominant")
    if ln_pos is None and as_lower(row.get("ajcc8_n_stage")) not in {"n0", "n1a", "n1b"}:
        missing.append("ln_positive_final")
    if not missing:
        missing.append("risk_defining_feature_absent_or_ambiguous")
    return Ata2025Result(
        "uncalculable",
        "uncalculable:insufficient_anatomic_risk_data",
        ";".join(missing),
    )


def reclassification_direction(old: Any, new: Any) -> str:
    old_cat = normalize_ata_2015(old)
    new_cat = as_lower(new) if as_lower(new) in RISK_ORDER else "uncalculable"
    delta = RISK_ORDER[new_cat] - RISK_ORDER[old_cat]
    if delta > 0:
        return "up"
    if delta < 0:
        return "down"
    return "same"


def wilson_ci(events: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n <= 0:
        return (np.nan, np.nan)
    p = events / n
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    half = z * math.sqrt((p * (1 - p) + z**2 / (4 * n)) / n) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def connect_publication_db() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise RuntimeError("No MotherDuck token resolved from env or motherduck.local.toml")
    con = duckdb.connect(f"md:{PUBLICATION_DB}?motherduck_token={quote_plus(tok)}")
    con.execute(f'USE "{PUBLICATION_DB}"')
    return con


def actual_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = con.execute(f"DESCRIBE main.{table_name}").fetchall()
    return {str(r[0]) for r in rows}


def first_existing_expr(cols: set[str], aliases: list[str], output_name: str, default_sql: str = "NULL") -> str:
    for col in aliases:
        if col in cols:
            return f"{col} AS {output_name}"
    return f"{default_sql} AS {output_name}"


def coalesce_existing_expr(cols: set[str], aliases: list[str], output_name: str, default_sql: str = "NULL") -> str:
    existing = [c for c in aliases if c in cols]
    if not existing:
        return f"{default_sql} AS {output_name}"
    if len(existing) == 1:
        return f"{existing[0]} AS {output_name}"
    return f"COALESCE({', '.join(existing)}) AS {output_name}"


def pull_malignant_cohort(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    cols = actual_columns(con, "canonical_patient_master")
    select_exprs = [
        "CAST(research_id AS VARCHAR) AS research_id",
        first_existing_expr(cols, ["ata_risk_category", "ata_initial_risk"], "ata_2015_category"),
        first_existing_expr(cols, ["histology_final"], "histology_final"),
        coalesce_existing_expr(cols, ["tumor_size_cm_dominant", "tumor_size_cm_max", "path_tumor_size_cm"], "tumor_size_cm_dominant"),
        first_existing_expr(cols, ["multifocal_flag_path"], "multifocal_flag_path", "FALSE"),
        coalesce_existing_expr(cols, ["ete_grade_final_v2", "ete_grade_final", "ete_grade"], "ete_grade_final"),
        first_existing_expr(cols, ["gross_ete_flag"], "gross_ete_flag", "FALSE"),
        coalesce_existing_expr(cols, ["vascular_invasion_final", "vascular_invasion_grade", "vascular_who_2022_grade"], "vascular_invasion_final"),
        coalesce_existing_expr(cols, ["vascular_invasion_grade", "vascular_who_2022_grade"], "vascular_invasion_grade"),
        coalesce_existing_expr(cols, ["vascular_vessel_count", "vasc_vessel_count_v13", "vessel_count", "vi_vessels_max"], "vascular_vessel_count"),
        coalesce_existing_expr(cols, ["margin_r_class_v10", "margin_r_classification", "margin_status_final"], "margin_status_final"),
        coalesce_existing_expr(cols, ["margin_r_class_v10", "margin_r_classification"], "margin_r_class_v10"),
        first_existing_expr(cols, ["margin_involved_any"], "margin_involved_any", "FALSE"),
        coalesce_existing_expr(cols, ["ln_positive_final", "ln_rollup_total_positive", "ln_total_positive", "path_ln_positive_raw"], "ln_positive_final"),
        coalesce_existing_expr(cols, ["ln_rollup_total_examined", "ln_total_examined", "path_ln_examined_raw"], "ln_rollup_total_examined"),
        coalesce_existing_expr(cols, ["ln_rollup_total_positive", "ln_positive_final", "ln_total_positive"], "ln_rollup_total_positive"),
        coalesce_existing_expr(cols, ["ln_rollup_largest_deposit_cm", "tp_ln_largest_deposit_cm"], "ln_rollup_largest_deposit_cm"),
        first_existing_expr(cols, ["ln_rollup_has_per_level_data"], "ln_rollup_has_per_level_data"),
        first_existing_expr(cols, ["braf_positive_final", "braf_positive"], "braf_positive_final", "FALSE"),
        first_existing_expr(cols, ["ras_positive_final", "ras_positive"], "ras_positive_final", "FALSE"),
        first_existing_expr(cols, ["tert_positive_final", "tert_positive_v7", "tert_positive"], "tert_positive_final", "FALSE"),
        first_existing_expr(cols, ["tert_positive_v7"], "tert_positive_v7", "FALSE"),
        first_existing_expr(cols, ["tert_positive"], "tert_positive", "FALSE"),
        first_existing_expr(cols, ["tp53_positive_v7"], "tp53_positive_v7", "FALSE"),
        first_existing_expr(cols, ["molecular_risk_tier"], "molecular_risk_tier"),
        first_existing_expr(cols, ["high_risk_molecular_v7"], "high_risk_molecular_v7", "FALSE"),
        first_existing_expr(cols, ["mol_has_fusion"], "mol_has_fusion", "FALSE"),
        first_existing_expr(cols, ["ajcc8_stage_group"], "ajcc8_stage_group"),
        first_existing_expr(cols, ["ajcc8_t_stage"], "ajcc8_t_stage"),
        first_existing_expr(cols, ["ajcc8_n_stage"], "ajcc8_n_stage"),
        first_existing_expr(cols, ["ajcc8_m_stage"], "ajcc8_m_stage"),
        first_existing_expr(cols, ["distant_mets_proxy"], "distant_mets_proxy", "FALSE"),
        first_existing_expr(cols, ["rai_received_reconciled", "rai_received_flag"], "rai_received_reconciled", "FALSE"),
        first_existing_expr(cols, ["any_recurrence_flag"], "any_recurrence_flag", "FALSE"),
        first_existing_expr(cols, ["recurrence_data_confidence"], "recurrence_data_confidence"),
        first_existing_expr(cols, ["time_to_recurrence_days"], "time_to_recurrence_days"),
        first_existing_expr(cols, ["followup_years"], "followup_years"),
    ]
    sql = f"""
        SELECT {", ".join(select_exprs)}
        FROM main.canonical_patient_master
        WHERE is_malignant IS TRUE
        ORDER BY CAST(research_id AS BIGINT)
    """
    df = con.execute(sql).fetchdf()
    if len(df) != 4019:
        raise RuntimeError(f"Expected 4,019 malignant patients, pulled {len(df):,}")
    return df


def classify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    results = [classify_ata_2025(row) for row in df.to_dict(orient="records")]
    out = df.copy()
    out["ata_2015_category"] = out["ata_2015_category"].map(normalize_ata_2015)
    out["ata_2025_category"] = [r.category for r in results]
    out["ata_2025_rule_triggered"] = [r.rule_triggered for r in results]
    out["ata_2025_missing_inputs"] = [r.missing_inputs for r in results]
    out["reclassified_flag"] = out["ata_2015_category"] != out["ata_2025_category"]
    out["reclassification_direction"] = [
        reclassification_direction(old, new)
        for old, new in zip(out["ata_2015_category"], out["ata_2025_category"], strict=True)
    ]
    out["recurrence_event"] = out["any_recurrence_flag"].map(is_true)
    out["ata_2015_score"] = out["ata_2015_category"].map(RISK_ORDER)
    out["ata_2025_score"] = out["ata_2025_category"].map(RISK_ORDER)
    return out


def build_crosstab(df: pd.DataFrame) -> pd.DataFrame:
    table = pd.crosstab(df["ata_2015_category"], df["ata_2025_category"], dropna=False)
    row_order = ["low", "intermediate", "high", "uncalculable"]
    col_order = [c for c in row_order if c in table.columns]
    table = table.reindex(row_order, fill_value=0)
    table = table.reindex(columns=col_order, fill_value=0)
    table.index.name = "ata_2015_category"
    return table.reset_index()


def build_outcome_validation(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for system, col in (("2015", "ata_2015_category"), ("2025", "ata_2025_category")):
        for cat in ["low", "intermediate", "high", "uncalculable"]:
            sub = df[df[col] == cat]
            n = int(len(sub))
            events = int(sub["recurrence_event"].sum()) if n else 0
            lo, hi = wilson_ci(events, n)
            rows.append(
                {
                    "system": system,
                    "category": cat,
                    "n": n,
                    "recurred": events,
                    "recurrence_rate": events / n if n else np.nan,
                    "recurrence_rate_pct": round(100 * events / n, 1) if n else np.nan,
                    "ci95_low_pct": round(100 * lo, 1) if n else np.nan,
                    "ci95_high_pct": round(100 * hi, 1) if n else np.nan,
                }
            )
    return pd.DataFrame(rows)


def safe_auc(y_true: pd.Series, scores: pd.Series) -> float | None:
    try:
        from sklearn.metrics import roc_auc_score

        y = y_true.astype(int)
        if y.nunique() < 2:
            return None
        return float(roc_auc_score(y, scores.astype(float)))
    except Exception:
        return None


def build_performance(df: pd.DataFrame) -> pd.DataFrame:
    event_mask = df["recurrence_event"]
    nonevent_mask = ~df["recurrence_event"]
    up = df["reclassification_direction"] == "up"
    down = df["reclassification_direction"] == "down"

    event_n = int(event_mask.sum())
    nonevent_n = int(nonevent_mask.sum())
    event_up = int((event_mask & up).sum())
    event_down = int((event_mask & down).sum())
    nonevent_down = int((nonevent_mask & down).sum())
    nonevent_up = int((nonevent_mask & up).sum())
    nri_events = (event_up - event_down) / event_n if event_n else np.nan
    nri_nonevents = (nonevent_down - nonevent_up) / nonevent_n if nonevent_n else np.nan
    nri = nri_events + nri_nonevents

    rows = [
        {
            "metric": "nri_events_component",
            "value": nri_events,
            "numerator": event_up - event_down,
            "denominator": event_n,
        },
        {
            "metric": "nri_nonevents_component",
            "value": nri_nonevents,
            "numerator": nonevent_down - nonevent_up,
            "denominator": nonevent_n,
        },
        {"metric": "nri_total", "value": nri, "numerator": np.nan, "denominator": len(df)},
        {
            "metric": "c_statistic_2015_including_uncalculable",
            "value": safe_auc(df["recurrence_event"], df["ata_2015_score"]),
            "numerator": np.nan,
            "denominator": len(df),
        },
        {
            "metric": "c_statistic_2025_including_uncalculable",
            "value": safe_auc(df["recurrence_event"], df["ata_2025_score"]),
            "numerator": np.nan,
            "denominator": len(df),
        },
    ]
    return pd.DataFrame(rows)


def build_km_summary_and_plot(df: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    work = df.copy()
    work["event_time_days"] = np.where(
        work["recurrence_event"] & work["time_to_recurrence_days"].notna(),
        pd.to_numeric(work["time_to_recurrence_days"], errors="coerce"),
        pd.to_numeric(work["followup_years"], errors="coerce") * 365.25,
    )
    work = work[(work["event_time_days"].notna()) & (work["event_time_days"] > 0)]
    rows: list[dict[str, Any]] = []
    if work.empty:
        return pd.DataFrame(rows)

    for system, col in (("2015", "ata_2015_category"), ("2025", "ata_2025_category")):
        for cat in ["low", "intermediate", "high", "uncalculable"]:
            sub = work[work[col] == cat]
            if sub.empty:
                continue
            rows.append(
                {
                    "system": system,
                    "category": cat,
                    "n_km": int(len(sub)),
                    "events_km": int(sub["recurrence_event"].sum()),
                    "median_followup_years": round(float(sub["event_time_days"].median() / 365.25), 2),
                }
            )

    try:
        import matplotlib.pyplot as plt
        from lifelines import KaplanMeierFitter

        fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
        for ax, (system, col) in zip(axes, (("2015", "ata_2015_category"), ("2025", "ata_2025_category")), strict=True):
            for cat in ["low", "intermediate", "high", "uncalculable"]:
                sub = work[work[col] == cat]
                if len(sub) < 5:
                    continue
                kmf = KaplanMeierFitter()
                kmf.fit(
                    durations=sub["event_time_days"] / 365.25,
                    event_observed=sub["recurrence_event"],
                    label=cat,
                )
                kmf.plot_survival_function(ax=ax, ci_show=False)
            ax.set_title(f"ATA {system}")
            ax.set_xlabel("Years from surgery")
            ax.set_ylabel("Recurrence-free survival")
        fig.tight_layout()
        fig.savefig(out_dir / "ata_2025_km_curves.png", dpi=300)
        plt.close(fig)
    except Exception as exc:  # noqa: BLE001
        rows.append({"system": "KM", "category": "plot_skipped", "n_km": 0, "events_km": 0, "median_followup_years": str(exc)})

    return pd.DataFrame(rows)


def latex_escape(value: Any) -> str:
    s = "" if is_missing(value) else str(value)
    replacements = {
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    return s.replace("–", "--")


def write_latex(df: pd.DataFrame, path: Path, caption: str, label: str) -> None:
    col_spec = "l" + "r" * max(0, len(df.columns) - 1)
    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        f"\\caption{{{latex_escape(caption)}}}",
        f"\\label{{tab:{latex_escape(label)}}}",
        f"\\begin{{tabular}}{{{col_spec}}}",
        r"\toprule",
        " & ".join(latex_escape(c) for c in df.columns) + r" \\",
        r"\midrule",
    ]
    for _, row in df.iterrows():
        lines.append(" & ".join(latex_escape(v) for v in row.tolist()) + r" \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_outputs(df: pd.DataFrame, out_dir: Path) -> dict[str, pd.DataFrame]:
    out_dir.mkdir(parents=True, exist_ok=True)
    patient_cols = [
        "research_id",
        "ata_2015_category",
        "ata_2025_category",
        "ata_2025_rule_triggered",
        "ata_2025_missing_inputs",
        "reclassified_flag",
        "reclassification_direction",
        "any_recurrence_flag",
        "time_to_recurrence_days",
        "followup_years",
    ]
    patient = df[patient_cols].copy()
    audit_cols = patient_cols + [
        "histology_final",
        "tumor_size_cm_dominant",
        "ete_grade_final",
        "gross_ete_flag",
        "vascular_invasion_final",
        "vascular_vessel_count",
        "margin_status_final",
        "ln_positive_final",
        "ln_rollup_total_examined",
        "ln_rollup_largest_deposit_cm",
        "molecular_risk_tier",
        "ajcc8_t_stage",
        "ajcc8_n_stage",
        "ajcc8_m_stage",
    ]
    audit = df[[c for c in audit_cols if c in df.columns]].copy()
    crosstab = build_crosstab(df)
    outcomes = build_outcome_validation(df)
    performance = build_performance(df)
    km_summary = build_km_summary_and_plot(df, out_dir)

    patient.to_csv(out_dir / "ata_2025_rss_classification.csv", index=False)
    crosstab.to_csv(out_dir / "reclassification_crosstab.csv", index=False)
    outcomes.to_csv(out_dir / "outcome_validation.csv", index=False)
    audit.to_csv(out_dir / "ata_2025_rules_audit.csv", index=False)
    performance.to_csv(out_dir / "model_performance.csv", index=False)
    km_summary.to_csv(out_dir / "km_summary.csv", index=False)

    write_latex(crosstab, out_dir / "reclassification_crosstab.tex", "ATA 2015 versus 2025 risk reclassification", "m036_ata_reclassification")
    latex_outcomes = outcomes.copy()
    latex_outcomes["recurrence_rate_display"] = latex_outcomes.apply(
        lambda r: f"{int(r['recurred'])}/{int(r['n'])} ({r['recurrence_rate_pct']}\\%; 95\\% CI {r['ci95_low_pct']}--{r['ci95_high_pct']})"
        if pd.notna(r["recurrence_rate_pct"]) else "",
        axis=1,
    )
    write_latex(
        latex_outcomes[["system", "category", "n", "recurrence_rate_display"]],
        out_dir / "outcome_validation.tex",
        "Recurrence by ATA risk category",
        "m036_ata_outcomes",
    )
    write_latex(performance, out_dir / "model_performance.tex", "ATA 2025 reclassification performance", "m036_ata_performance")

    return {
        "patient": patient,
        "audit": audit,
        "crosstab": crosstab,
        "outcomes": outcomes,
        "performance": performance,
        "km_summary": km_summary,
    }


def upload_results(con: duckdb.DuckDBPyConnection, patient_df: pd.DataFrame) -> None:
    upload_cols = [
        "research_id",
        "ata_2015_category",
        "ata_2025_category",
        "ata_2025_rule_triggered",
        "ata_2025_missing_inputs",
        "reclassified_flag",
        "reclassification_direction",
    ]
    upload = patient_df[upload_cols].copy()
    con.execute("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")
    con.execute(f"DROP TABLE IF EXISTS {OLD_UPLOAD_TABLE}")
    con.register("_m036_ata_2025_upload", upload)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {UPLOAD_TABLE} AS
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            CAST(ata_2015_category AS VARCHAR) AS ata_2015_category,
            CAST(ata_2025_category AS VARCHAR) AS ata_2025_category,
            CAST(ata_2025_rule_triggered AS VARCHAR) AS ata_2025_rule_triggered,
            CAST(ata_2025_missing_inputs AS VARCHAR) AS ata_2025_missing_inputs,
            CAST(reclassified_flag AS BOOLEAN) AS reclassified_flag,
            CAST(reclassification_direction AS VARCHAR) AS reclassification_direction
        FROM _m036_ata_2025_upload
        """
    )
    con.unregister("_m036_ata_2025_upload")
    row = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {UPLOAD_TABLE}").fetchone()
    if row != (4019, 4019):
        raise RuntimeError(f"Upload invariant failed for {UPLOAD_TABLE}: {row}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Implement M036 2025 ATA RSS comparison.")
    ap.add_argument("--dry-run", action="store_true", help="Generate files but skip MotherDuck upload.")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    con = connect_publication_db()
    df = classify_dataframe(pull_malignant_cohort(con))
    outputs = write_outputs(df, args.out_dir)
    if not args.dry_run:
        upload_results(con, outputs["patient"])

    counts = df["ata_2025_category"].value_counts(dropna=False).to_dict()
    print(f"Pulled and classified {len(df):,} malignant patients")
    print(f"2025 ATA distribution: {counts}")
    print(f"Outputs written to {args.out_dir}")
    if args.dry_run:
        print("Dry run: MotherDuck upload skipped")
    else:
        print(f"Uploaded {UPLOAD_TABLE}")


if __name__ == "__main__":
    main()
