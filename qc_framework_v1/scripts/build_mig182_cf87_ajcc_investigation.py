#!/usr/bin/env python3
"""mig_182 read-only investigation for CF-87-AJCC.

This script connects to the locked publication MotherDuck database, runs only
SELECT probes, and writes local CSV/JSON/Markdown artifacts. It does not execute
MotherDuck DDL/DML. The goal is to surface the CF-87-AJCC inventory, quantify
AJCC staging drift, and propose R1/R2/R3 follow-up options for Logan ratification.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

RUN_ID = "mig182_cf87_ajcc_investigation_20260429"
REPORT_NAME = "mig_182_cf_87_ajcc_investigation_20260429.md"

MIG87_CONTEXT = """CF-87-AJCC: AJCC7/8 staging values inherited from CTC pre361 are verified
    as faithful copies. The findings-vs-staging derivation correctness
    (Logan's airway-invasion rule extended to ETE/multifocality/nodal) is a
    separate validation question that operates UPSTREAM of canonical (in
    CTC's build pipeline, scripts 251/266). Defer to a future round that
    either (a) restores CTC and validates its staging derivation against
    findings, or (b) re-derives staging post-canonical from the verified
    finding columns and audits diff vs current values."""

PATH_STAGE_COLS = [
    "t_stage_ajcc7",
    "n_stage_ajcc7",
    "m_stage_ajcc7",
    "overall_stage_ajcc7",
    "stage_group_ajcc7",
    "t_stage_ajcc8",
    "n_stage_ajcc8",
    "m_stage_ajcc8",
    "overall_stage_ajcc8",
    "stage_group_ajcc8",
]

PATH_FINDING_COLS = [
    "size_greatest_dimension_cm",
    "extrathyroidal_extension",
    "gross_ete",
    "ln_examined",
    "ln_involved",
    "extranodal_extension",
    "multifocality_flag",
]

CPM_STAGE_COLS = [
    "ajcc7_t_stage",
    "ajcc7_n_stage",
    "ajcc7_m_stage",
    "ajcc7_stage_group",
    "ajcc8_t_stage",
    "ajcc8_t_stage_with_microete_t3b_DEPRECATED",
    "ajcc8_t_stage_v2",
    "ajcc8_n_stage",
    "ajcc8_n_stage_v2",
    "ajcc8_m_stage",
    "ajcc8_m_stage_v2",
    "ajcc8_stage_group",
    "ajcc8_stage_group_corrected",
    "ajcc8_stage_group_v2",
    "dominant_tumor_ajcc8_t_stage",
    "dominant_tumor_ajcc8_n_stage",
    "dominant_tumor_ajcc8_m_stage",
    "dominant_tumor_ajcc8_stage_group",
    "n_tumors_ajcc8_staged",
    "tumor_stage_heterogeneous_t_ajcc8_flag",
    "tumor_stage_heterogeneous_overall_ajcc8_flag",
    "is_malignant",
]

T_SEVERITY = {
    "T4B": 8,
    "T4A": 7,
    "T4": 6,
    "T3B": 5,
    "T3A": 4,
    "T3": 3,
    "T2": 2,
    "T1B": 1.2,
    "T1A": 1.1,
    "T1": 1,
    "TX": 0,
}
N_SEVERITY = {"N1B": 3, "N1A": 2, "N1": 1, "NX": 0, "N0": 0}
M_SEVERITY = {"M1": 1, "MX": 0, "M0": 0}
STAGE_SEVERITY = {
    "IVB": 8,
    "IVA": 7,
    "IV": 6,
    "IIIC": 5.3,
    "IIIB": 5.2,
    "IIIA": 5.1,
    "III": 5,
    "IIC": 4.3,
    "IIB": 4.2,
    "IIA": 4.1,
    "II": 4,
    "IC": 3.3,
    "IB": 3.2,
    "IA": 3.1,
    "I": 3,
}


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def norm_stage(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL"}:
        return None
    text = text.replace(" ", "")
    if text.startswith("STAGE"):
        text = text.replace("STAGE", "")
    return text


def severity(value: object, mapping: dict[str, float | int]) -> float:
    text = norm_stage(value)
    if text is None:
        return -1
    return float(mapping.get(text, -1))


def is_true(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "t", "1", "yes", "y", "x", "present", "positive"}


def safe_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        match = re.search(r"\d+(?:\.\d+)?", str(value))
        return float(match.group(0)) if match else None


def infer_ajcc8_t_from_findings(row: pd.Series) -> str | None:
    text = str(row.get("extrathyroidal_extension") or "").lower()
    size = safe_float(row.get("size_greatest_dimension_cm"))
    gross = is_true(row.get("gross_ete")) or any(
        token in text for token in ["gross", "macroscopic", "strap", "skeletal muscle"]
    )
    t4a = any(token in text for token in ["trache", "larynx", "laryngeal", "esophag", "recurrent laryngeal", "cricoid"])
    t4b = any(token in text for token in ["prevertebral", "carotid", "mediastinal vessel"])
    if t4b:
        return "T4b"
    if t4a:
        return "T4a"
    if gross:
        return "T3b"
    if size is None or size <= 0:
        return None
    if size <= 1:
        return "T1a"
    if size <= 2:
        return "T1b"
    if size <= 4:
        return "T2"
    return "T3a"


def infer_ajcc8_n_positive_group(row: pd.Series) -> str | None:
    involved = safe_float(row.get("ln_involved"))
    examined = safe_float(row.get("ln_examined"))
    if involved is not None and involved > 0:
        return "N_positive"
    if examined is not None and examined > 0:
        return "N0"
    return None


def extract_original_cf_context() -> str:
    migration = REPO_ROOT / "qc_framework_v1" / "migrations" / "87_path_malignant_ctc_equivalence_batch_verify.sql"
    text = migration.read_text()
    match = re.search(r"CF-87-AJCC:.*?values\.\n", text, re.DOTALL)
    if match:
        return match.group(0).strip()
    return MIG87_CONTEXT


def fetch_registry_inventory(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT schema_name, table_name, column_name, batch_id, verification_status,
               SUBSTR(notes, 1, 400) AS notes_excerpt
        FROM main.canonical_column_verification_registry_v1
        WHERE notes ILIKE '%CF-87-AJCC%'
           OR notes ILIKE '%CF-87%AJCC%'
           OR notes ILIKE '%AJCC%drift%'
        ORDER BY table_name, column_name
        """
    ).fetchdf()


def fetch_path_events(con) -> pd.DataFrame:
    cols = ["surgery_episode_id", "tumor_ordinal", "surgery_date"] + PATH_FINDING_COLS + PATH_STAGE_COLS
    select_cols = ", ".join(qident(c) for c in cols)
    return con.execute(f"SELECT CAST(research_id AS VARCHAR) AS research_id, {select_cols} FROM main.canonical_path_malignant_events_v1").fetchdf()


def fetch_cpm(con) -> pd.DataFrame:
    available = con.execute(
        """
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_schema='main' AND table_name='canonical_patient_master'
        """
    ).fetchdf()["column_name"].tolist()
    cols = [c for c in CPM_STAGE_COLS if c in available]
    select_cols = ", ".join(qident(c) for c in cols)
    return con.execute(f"SELECT CAST(research_id AS VARCHAR) AS research_id, {select_cols} FROM main.canonical_patient_master").fetchdf()


def registry_family_summary(registry: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for table_name, sub in registry.groupby("table_name", dropna=False):
        cols = set(sub["column_name"].astype(str))
        rows.append(
            {
                "table_name": table_name,
                "n_columns_tagged": len(cols),
                "ajcc_stage_columns": sum("ajcc" in c.lower() or "stage" in c.lower() for c in cols),
                "finding_or_input_columns": sum(c in PATH_FINDING_COLS or c in {"histology_variant", "laterality", "primary_histology", "t_stage_discordance_flag", "reported_t_stage_ajcc8", "derived_t_stage_ajcc8"} for c in cols),
                "columns": ", ".join(sorted(cols)),
            }
        )
    return pd.DataFrame(rows)


def path_internal_drift(path_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = path_df.copy()
    df["inferred_ajcc8_t_from_findings"] = df.apply(infer_ajcc8_t_from_findings, axis=1)
    df["inferred_ajcc8_n_positive_group"] = df.apply(infer_ajcc8_n_positive_group, axis=1)
    df["stored_ajcc8_n_positive_group"] = df["n_stage_ajcc8"].map(lambda x: "N_positive" if severity(x, N_SEVERITY) > 0 else ("N0" if norm_stage(x) == "N0" else None))

    summary_rows = []
    comparisons = [
        ("ajcc8_t_stage", "t_stage_ajcc8", "inferred_ajcc8_t_from_findings"),
        ("ajcc8_n_positive_group", "stored_ajcc8_n_positive_group", "inferred_ajcc8_n_positive_group"),
    ]
    for label, stored, inferred in comparisons:
        paired = df[df[stored].map(norm_stage).notna() & df[inferred].map(norm_stage).notna()].copy()
        mismatched = paired[paired[stored].map(norm_stage) != paired[inferred].map(norm_stage)]
        summary_rows.append(
            {
                "dimension": label,
                "rows_total": len(df),
                "stored_non_null": int(df[stored].map(norm_stage).notna().sum()),
                "inferred_non_null": int(df[inferred].map(norm_stage).notna().sum()),
                "paired_non_null": len(paired),
                "paired_mismatches": len(mismatched),
                "paired_mismatch_pct": round((len(mismatched) / len(paired) * 100) if len(paired) else 0, 2),
            }
        )
    examples = df[
        df["t_stage_ajcc8"].map(norm_stage).notna()
        & df["inferred_ajcc8_t_from_findings"].map(norm_stage).notna()
        & (df["t_stage_ajcc8"].map(norm_stage) != df["inferred_ajcc8_t_from_findings"].map(norm_stage))
    ][
        [
            "research_id",
            "surgery_episode_id",
            "tumor_ordinal",
            "size_greatest_dimension_cm",
            "extrathyroidal_extension",
            "gross_ete",
            "t_stage_ajcc8",
            "inferred_ajcc8_t_from_findings",
        ]
    ].head(100)
    distribution = (
        df.assign(
            stored_t=df["t_stage_ajcc8"].map(norm_stage),
            inferred_t=df["inferred_ajcc8_t_from_findings"].map(norm_stage),
        )
        .groupby(["stored_t", "inferred_t"], dropna=False)
        .size()
        .reset_index(name="n_rows")
        .sort_values("n_rows", ascending=False)
    )
    return pd.DataFrame(summary_rows), distribution, examples


def patient_level_rollup(path_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for rid, sub in path_df.groupby("research_id", dropna=False):
        row: dict[str, object] = {"research_id": rid, "n_path_event_rows": len(sub)}
        for col, mapping, prefix in [
            ("t_stage_ajcc8", T_SEVERITY, "path_worst_ajcc8_t_stage"),
            ("n_stage_ajcc8", N_SEVERITY, "path_worst_ajcc8_n_stage"),
            ("m_stage_ajcc8", M_SEVERITY, "path_worst_ajcc8_m_stage"),
            ("stage_group_ajcc8", STAGE_SEVERITY, "path_worst_ajcc8_stage_group"),
            ("t_stage_ajcc7", T_SEVERITY, "path_worst_ajcc7_t_stage"),
            ("n_stage_ajcc7", N_SEVERITY, "path_worst_ajcc7_n_stage"),
            ("m_stage_ajcc7", M_SEVERITY, "path_worst_ajcc7_m_stage"),
            ("stage_group_ajcc7", STAGE_SEVERITY, "path_worst_ajcc7_stage_group"),
        ]:
            vals = [(severity(v, mapping), norm_stage(v)) for v in sub[col].tolist()]
            vals = [v for v in vals if v[1] is not None]
            row[prefix] = max(vals, key=lambda x: x[0])[1] if vals else None
        inferred = sub.copy()
        inferred["inferred_t"] = inferred.apply(infer_ajcc8_t_from_findings, axis=1)
        vals = [(severity(v, T_SEVERITY), norm_stage(v)) for v in inferred["inferred_t"].tolist()]
        vals = [v for v in vals if v[1] is not None]
        row["path_worst_inferred_ajcc8_t_from_findings"] = max(vals, key=lambda x: x[0])[1] if vals else None
        rows.append(row)
    return pd.DataFrame(rows)


def compare_pair(df: pd.DataFrame, left: str, right: str, cohort_mask: pd.Series | None = None) -> dict[str, object]:
    sub = df if cohort_mask is None else df[cohort_mask].copy()
    if left not in sub.columns or right not in sub.columns:
        return {"comparison": f"{left} vs {right}", "available": False}
    left_norm = sub[left].map(norm_stage)
    right_norm = sub[right].map(norm_stage)
    paired = left_norm.notna() & right_norm.notna()
    mismatch = paired & (left_norm != right_norm)
    return {
        "comparison": f"{left} vs {right}",
        "available": True,
        "n_rows": int(len(sub)),
        "left_non_null": int(left_norm.notna().sum()),
        "right_non_null": int(right_norm.notna().sum()),
        "paired_non_null": int(paired.sum()),
        "mismatches": int(mismatch.sum()),
        "mismatch_pct_of_paired": round((int(mismatch.sum()) / int(paired.sum()) * 100) if int(paired.sum()) else 0, 2),
    }


def patient_cross_source_drift(path_rollup: pd.DataFrame, cpm: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    merged = cpm.merge(path_rollup, on="research_id", how="left")
    malignant_mask = merged["is_malignant"].map(is_true) if "is_malignant" in merged.columns else pd.Series([True] * len(merged), index=merged.index)
    comparisons = [
        ("ajcc8_t_stage", "dominant_tumor_ajcc8_t_stage"),
        ("ajcc8_t_stage", "ajcc8_t_stage_with_microete_t3b_DEPRECATED"),
        ("ajcc8_t_stage", "ajcc8_t_stage_v2"),
        ("ajcc8_stage_group", "dominant_tumor_ajcc8_stage_group"),
        ("ajcc8_stage_group", "ajcc8_stage_group_corrected"),
        ("ajcc8_stage_group", "ajcc8_stage_group_v2"),
        ("ajcc8_t_stage", "path_worst_ajcc8_t_stage"),
        ("dominant_tumor_ajcc8_t_stage", "path_worst_ajcc8_t_stage"),
        ("ajcc8_t_stage", "path_worst_inferred_ajcc8_t_from_findings"),
        ("path_worst_ajcc8_t_stage", "path_worst_inferred_ajcc8_t_from_findings"),
        ("ajcc8_n_stage", "path_worst_ajcc8_n_stage"),
        ("ajcc8_m_stage", "path_worst_ajcc8_m_stage"),
        ("ajcc7_t_stage", "path_worst_ajcc7_t_stage"),
        ("ajcc7_stage_group", "path_worst_ajcc7_stage_group"),
    ]
    summary = pd.DataFrame([compare_pair(merged, left, right, malignant_mask) for left, right in comparisons])

    stage_cols = [c for c in ["ajcc8_t_stage", "dominant_tumor_ajcc8_t_stage", "path_worst_ajcc8_t_stage", "path_worst_inferred_ajcc8_t_from_findings", "ajcc8_stage_group", "dominant_tumor_ajcc8_stage_group", "path_worst_ajcc8_stage_group"] if c in merged.columns]
    discordant_mask = pd.Series(False, index=merged.index)
    for left, right in comparisons:
        if left in merged.columns and right in merged.columns:
            l = merged[left].map(norm_stage)
            r = merged[right].map(norm_stage)
            discordant_mask |= l.notna() & r.notna() & (l != r)
    examples = merged[malignant_mask & discordant_mask][["research_id", "is_malignant", "n_path_event_rows"] + stage_cols].head(200)

    def mismatch_count(left: str, right: str) -> int:
        if left not in merged.columns or right not in merged.columns:
            return 0
        l = merged[left].map(norm_stage)
        r = merged[right].map(norm_stage)
        return int((malignant_mask & l.notna() & r.notna() & (l != r)).sum())

    rows = [
        {"metric": "cpm_total_rows", "n": len(merged), "denominator": "all_cpm_rows"},
        {"metric": "cpm_malignant_rows", "n": int(malignant_mask.sum()), "denominator": "all_cpm_rows"},
        {"metric": "malignant_with_cpm_ajcc8_t_stage", "n": int((malignant_mask & merged.get("ajcc8_t_stage", pd.Series(index=merged.index)).map(norm_stage).notna()).sum()), "denominator": "cpm_malignant_rows"},
        {"metric": "malignant_with_dominant_tumor_ajcc8_t_stage", "n": int((malignant_mask & merged.get("dominant_tumor_ajcc8_t_stage", pd.Series(index=merged.index)).map(norm_stage).notna()).sum()), "denominator": "cpm_malignant_rows"},
        {"metric": "malignant_with_path_event_ajcc8_t_stage", "n": int((malignant_mask & merged.get("path_worst_ajcc8_t_stage", pd.Series(index=merged.index)).map(norm_stage).notna()).sum()), "denominator": "cpm_malignant_rows"},
        {"metric": "malignant_cpm_vs_dominant_ajcc8_t_stage_diff", "n": mismatch_count("ajcc8_t_stage", "dominant_tumor_ajcc8_t_stage"), "denominator": "cpm_malignant_rows"},
        {"metric": "malignant_cpm_vs_dominant_ajcc8_stage_group_shift", "n": mismatch_count("ajcc8_stage_group", "dominant_tumor_ajcc8_stage_group"), "denominator": "cpm_malignant_rows"},
        {"metric": "malignant_path_stored_vs_findings_inferred_ajcc8_t_stage_diff", "n": mismatch_count("path_worst_ajcc8_t_stage", "path_worst_inferred_ajcc8_t_from_findings"), "denominator": "cpm_malignant_rows"},
        {"metric": "malignant_with_any_scoped_cross_source_stage_discordance", "n": int((malignant_mask & discordant_mask).sum()), "denominator": "cpm_malignant_rows"},
    ]
    impact = pd.DataFrame(rows)
    denom_map = {
        "all_cpm_rows": len(merged),
        "cpm_malignant_rows": int(malignant_mask.sum()),
    }
    impact["pct_of_denominator"] = impact.apply(
        lambda r: round((r["n"] / denom_map.get(r["denominator"], 0) * 100) if denom_map.get(r["denominator"], 0) else 0, 2),
        axis=1,
    )
    return summary, impact, examples


def coverage_tables(path_df: pd.DataFrame, cpm: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    path_rows = []
    for col in PATH_STAGE_COLS + PATH_FINDING_COLS:
        path_rows.append({"source_table": "canonical_path_malignant_events_v1", "column_name": col, "non_null_rows": int(path_df[col].notna().sum()), "total_rows": len(path_df), "non_null_pct": round(path_df[col].notna().mean() * 100, 2)})
    cpm_rows = []
    for col in [c for c in CPM_STAGE_COLS if c in cpm.columns and c != "is_malignant"]:
        cpm_rows.append({"source_table": "canonical_patient_master", "column_name": col, "non_null_rows": int(cpm[col].notna().sum()), "total_rows": len(cpm), "non_null_pct": round(cpm[col].notna().mean() * 100, 2)})
    return pd.DataFrame(path_rows), pd.DataFrame(cpm_rows)


def option_matrix() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "option": "R1_rederive_from_verified_findings",
                "scope": "Recompute AJCC staging from verified finding columns into new *_resolved columns; keep legacy cols unchanged until sign-off.",
                "pros": "Directly addresses CF-87's findings-vs-staging question; auditable row-level diff; avoids overwriting ratified legacy copies.",
                "cons": "Requires formal AJCC7/8 derivation spec for T/N/M/stage group, including nodal location and M-stage rules; current path-event findings cannot fully distinguish N1a vs N1b.",
                "recommendation": "Best long-term manuscript-grade closure after Logan ratifies exact rules.",
            },
            {
                "option": "R2_priority_resolved_stage",
                "scope": "Create patient-level resolved AJCC columns with priority dominant-tumor/heterogeneity layer > canonical patient master > path-event rollup, plus discordance flags.",
                "pros": "Lowest practical manuscript disruption; leverages 266b/266c dominant-tumor layer already aligned to canonical CPM at high concordance.",
                "cons": "Does not prove every per-tumor path-event stage is derivationally correct; it is a patient-level resolution strategy.",
                "recommendation": "Recommended immediate follow-up if the manuscript needs a stable patient-level stage source.",
            },
            {
                "option": "R3_flag_only_no_stage_mutation",
                "scope": "Leave all stage columns unchanged; add/export discordance and manuscript-impact review queues only.",
                "pros": "Safest governance posture; no production staging mutation; preserves CF as transparent caveat.",
                "cons": "Does not close the semantic CF; downstream analysts must choose among stage columns manually.",
                "recommendation": "Acceptable only if Logan wants no apply lane before manuscript lock.",
            },
        ]
    )


def write_sql_artifact(path: Path) -> None:
    path.write_text(
        "-- mig_182 — CF-87-AJCC investigation probes (READ-ONLY)\n"
        "-- Target DB: thyroid_canonical_publication_v1_0\n"
        "-- No DDL/DML in this artifact.\n\n"
        "SELECT schema_name, table_name, column_name, batch_id, SUBSTR(notes, 1, 400) AS notes_excerpt\n"
        "FROM main.canonical_column_verification_registry_v1\n"
        "WHERE notes ILIKE '%CF-87-AJCC%' OR notes ILIKE '%CF-87%AJCC%' OR notes ILIKE '%AJCC%drift%'\n"
        "ORDER BY table_name, column_name;\n\n"
        "SELECT t_stage_ajcc8, n_stage_ajcc8, m_stage_ajcc8, stage_group_ajcc8, COUNT(*) AS n_rows\n"
        "FROM main.canonical_path_malignant_events_v1\n"
        "GROUP BY 1,2,3,4\n"
        "ORDER BY n_rows DESC;\n\n"
        "SELECT ajcc8_t_stage, dominant_tumor_ajcc8_t_stage, ajcc8_stage_group, dominant_tumor_ajcc8_stage_group, COUNT(*) AS n_patients\n"
        "FROM main.canonical_patient_master\n"
        "GROUP BY 1,2,3,4\n"
        "ORDER BY n_patients DESC;\n"
    )


def write_report(
    report_path: Path,
    registry: pd.DataFrame,
    family_summary: pd.DataFrame,
    path_internal: pd.DataFrame,
    path_distribution: pd.DataFrame,
    patient_summary: pd.DataFrame,
    impact: pd.DataFrame,
    path_coverage: pd.DataFrame,
    cpm_coverage: pd.DataFrame,
    options: pd.DataFrame,
    out_dir: Path,
) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    path_cf = registry[registry["table_name"] == "canonical_path_malignant_events_v1"]
    lines = [
        "# mig_182 — CF-87-AJCC investigation",
        "",
        f"**Run ID:** `{RUN_ID}`  ",
        f"**Run timestamp (UTC):** `{timestamp}`  ",
        "**Posture:** read-only MotherDuck investigation; no production DDL/DML.  ",
        "**Target DB:** `thyroid_canonical_publication_v1_0`  ",
        "**Carry-forward:** `CF-87-AJCC` (36-column original path-malignant col-impact; 45 current registry mentions including downstream ETE-event replay columns).  ",
        "",
        "## Executive summary",
        "",
        f"- Registry lookup found **{len(registry):,}** current `CF-87-AJCC`/AJCC-drift mentions across **{registry['table_name'].nunique():,}** tables.",
        f"- The original mig_87 path-malignant scope is **{len(path_cf):,}** columns on `canonical_path_malignant_events_v1`; this is the 36-column col-impact referenced in the handoff.",
        "- Original mig_87 proved faithful-copy equivalence to CTC pre-361; it did **not** prove that the copied AJCC values are derivationally correct from findings.",
        "- The highest-risk manuscript question is patient-level AJCC8 source choice: canonical CPM, dominant-tumor/heterogeneity layer, path-event rollup, or fresh findings-derived stage.",
        "- Recommended follow-up is **R2 now** for patient-level manuscript stability plus **R1 later** for full per-tumor derivation closure; no apply SQL is included in this lane.",
        "",
        "## 1. CF-87-AJCC inventory",
        "",
        family_summary.to_markdown(index=False),
        "",
        "### Complete registry rows",
        "",
        registry.to_markdown(index=False),
        "",
        "## 2. Original mig_87 context (verbatim)",
        "",
        "> " + extract_original_cf_context().replace("\n", "\n> "),
        "",
        "Interpretation: mig_87 verified copied values, not clinical derivation correctness. AJCC7 and AJCC8 columns involved in the original 36-column batch are `t/n/m/overall/stage_group_ajcc7` and `t/n/m/overall/stage_group_ajcc8`, plus the calculability flags and staging-source metadata.",
        "",
        "## 3. Drift quantification on live data",
        "",
        "### Path-event internal findings-vs-stored-stage probes",
        "",
        path_internal.to_markdown(index=False),
        "",
        "Top stored-vs-inferred AJCC8 T distributions are exported in CSV; top rows are shown here:",
        "",
        path_distribution.head(30).to_markdown(index=False),
        "",
        "### Patient-level cross-source stage comparisons",
        "",
        patient_summary.to_markdown(index=False),
        "",
        "## 4. Cross-source reconciliation coverage",
        "",
        "### Path-event coverage",
        "",
        path_coverage.to_markdown(index=False),
        "",
        "### Canonical patient master coverage",
        "",
        cpm_coverage.to_markdown(index=False),
        "",
        "## 5. Manuscript-impact assessment",
        "",
        impact.to_markdown(index=False),
        "",
        "The denominator is CPM malignant patients where `is_malignant` is TRUE when available. Cross-source discordance here should be interpreted as a scoping queue, not an automatic correction list: some differences are expected because per-tumor path-event rollups, dominant-tumor patient-level staging, and legacy CPM fields operate at different grains.",
        "",
        "## 6. R1/R2/R3 fix-plan options",
        "",
        options.to_markdown(index=False),
        "",
        "## 7. Generated local artifacts",
        "",
        f"- `{out_dir.relative_to(REPO_ROOT)}/mig182_registry_inventory.csv`",
        f"- `{out_dir.relative_to(REPO_ROOT)}/mig182_registry_family_summary.csv`",
        f"- `{out_dir.relative_to(REPO_ROOT)}/mig182_path_internal_drift.csv`",
        f"- `{out_dir.relative_to(REPO_ROOT)}/mig182_path_t_stage_distribution.csv`",
        f"- `{out_dir.relative_to(REPO_ROOT)}/mig182_path_t_stage_mismatch_examples.csv`",
        f"- `{out_dir.relative_to(REPO_ROOT)}/mig182_patient_cross_source_drift.csv`",
        f"- `{out_dir.relative_to(REPO_ROOT)}/mig182_manuscript_impact.csv`",
        f"- `{out_dir.relative_to(REPO_ROOT)}/mig182_patient_discordance_examples.csv`",
        f"- `{out_dir.relative_to(REPO_ROOT)}/manifest.json`",
        "",
        "## Governance boundary",
        "",
        "This migration lane did not execute any `UPDATE`, `CREATE`, `ALTER`, `DROP`, or registry mutation in MotherDuck. The R1/R2/R3 plan requires Logan ratification before any apply lane is authored.",
    ]
    report_path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default=None, help="Output directory for local artifacts")
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else REPO_ROOT / "exports" / RUN_ID
    out_dir.mkdir(parents=True, exist_ok=True)

    con = connect_locked()
    registry = fetch_registry_inventory(con)
    family_summary = registry_family_summary(registry)
    path_df = fetch_path_events(con)
    cpm = fetch_cpm(con)
    path_internal, path_distribution, path_examples = path_internal_drift(path_df)
    path_rollup = patient_level_rollup(path_df)
    patient_summary, impact, patient_examples = patient_cross_source_drift(path_rollup, cpm)
    path_coverage, cpm_coverage = coverage_tables(path_df, cpm)
    options = option_matrix()

    csvs = {
        "mig182_registry_inventory.csv": registry,
        "mig182_registry_family_summary.csv": family_summary,
        "mig182_path_internal_drift.csv": path_internal,
        "mig182_path_t_stage_distribution.csv": path_distribution,
        "mig182_path_t_stage_mismatch_examples.csv": path_examples,
        "mig182_patient_cross_source_drift.csv": patient_summary,
        "mig182_manuscript_impact.csv": impact,
        "mig182_patient_discordance_examples.csv": patient_examples,
        "mig182_path_event_stage_coverage.csv": path_coverage,
        "mig182_cpm_stage_coverage.csv": cpm_coverage,
        "mig182_fix_option_matrix.csv": options,
    }
    for name, df in csvs.items():
        df.to_csv(out_dir / name, index=False)

    manifest = {
        "run_id": RUN_ID,
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "posture": "read_only_motherduck_local_artifacts_only",
        "target_db": "thyroid_canonical_publication_v1_0",
        "output_dir": str(out_dir.relative_to(REPO_ROOT)),
        "csv_files": sorted(csvs),
        "registry_mentions": int(len(registry)),
        "path_malignant_original_scope_mentions": int((registry["table_name"] == "canonical_path_malignant_events_v1").sum()),
        "governance": "No production data mutation; Logan ratification required before R1/R2/R3 apply lane.",
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    report_path = REPO_ROOT / "qc_framework_v1" / "reports" / REPORT_NAME
    write_report(report_path, registry, family_summary, path_internal, path_distribution, patient_summary, impact, path_coverage, cpm_coverage, options, out_dir)

    sql_path = REPO_ROOT / "qc_framework_v1" / "migrations" / "182_cf_87_ajcc_investigation_probes_20260429.sql"
    write_sql_artifact(sql_path)

    print(f"Wrote {report_path.relative_to(REPO_ROOT)}")
    print(f"Wrote {sql_path.relative_to(REPO_ROOT)}")
    print(f"Wrote {out_dir.relative_to(REPO_ROOT)}")
    print(family_summary.to_string(index=False))
    print(impact.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())