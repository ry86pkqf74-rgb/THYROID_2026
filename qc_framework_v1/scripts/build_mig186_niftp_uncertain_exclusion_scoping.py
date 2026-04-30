#!/usr/bin/env python3
"""Build mig_186 NIFTP/uncertain-malignancy read-only scoping artifacts.

This script performs only SELECTs against the locked MotherDuck publication
database and writes the local artifacts requested by
cursor_prompts/CURSOR_PROMPT_mig186_niftp_uncertain_exclusion_20260430.md.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

RUN_ID = "mig186_niftp_scoping_20260430"
OUT_DIR = REPO_ROOT / "exports" / RUN_ID
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_186_niftp_uncertain_exclusion_scoping_20260430.md"
SQL_PATH = REPO_ROOT / "qc_framework_v1" / "migrations" / "186_niftp_uncertain_exclusion_TBD_20260430.sql"
INVENTORY_CSV = OUT_DIR / "niftp_uncertain_inventory.csv"
PATIENT_SUMMARY_CSV = OUT_DIR / "niftp_uncertain_patient_disposition.csv"
MANIFEST_PATH = OUT_DIR / "manifest.json"

TARGET_DB = "thyroid_canonical_publication_v1_0"

NIFTP_PAT = "niftp|non[- ]?invasive follicular thyroid neoplasm"
UNCERTAIN_PAT = "uncertain|hurthle.*neoplasm|hürthle.*neoplasm|ft-ump|wdt-ump"


def fetch_df(con: Any, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def md_table(df: pd.DataFrame, max_rows: int = 80) -> str:
    if df.empty:
        return "_No rows._"
    return df.head(max_rows).to_markdown(index=False)


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null", "<na>"}:
        return ""
    return text


def is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = norm_text(value).lower()
    return text in {"true", "t", "1", "yes", "y", "x", "present", "positive"}


def classify_reason(primary_histology: Any, histology_variant: Any) -> str:
    text = f"{norm_text(primary_histology)} {norm_text(histology_variant)}".lower()
    has_niftp = pd.Series([text]).str.contains(NIFTP_PAT, regex=True, case=False).iloc[0]
    has_uncertain = pd.Series([text]).str.contains(UNCERTAIN_PAT, regex=True, case=False).iloc[0]
    if has_niftp and has_uncertain:
        return "niftp_and_uncertain_text"
    if has_niftp:
        return "niftp_who2017_non_malignant"
    if has_uncertain:
        return "uncertain_malignant_potential"
    return "other"


def get_columns(con: Any, table_name: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog = ?
          AND table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [TARGET_DB, table_name],
    ).fetchall()
    return [row[0] for row in rows]


def optional_select(columns: list[str], desired: list[str], table_alias: str = "") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    present = [f"{prefix}{col}" for col in desired if col in columns]
    return ",\n            ".join(present)


def build_inventory(con: Any) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path_cols = get_columns(con, "canonical_path_malignant_events_v1")
    select_cols = [
        "research_id",
        "surgery_episode_id",
        "tumor_ordinal",
        "surgery_date",
        "path_surgery_id",
        "specimen_id",
        "synoptic_row_ix",
        "primary_histology",
        "histology_variant",
        "size_greatest_dimension_cm",
        "t_stage_ajcc8",
        "n_stage_ajcc8",
        "m_stage_ajcc8",
        "stage_group_ajcc8",
        "t_stage_ajcc7",
        "n_stage_ajcc7",
        "m_stage_ajcc7",
        "stage_group_ajcc7",
        "staging_source_note",
        "build_script",
        "build_ts",
    ]
    selected = optional_select(path_cols, select_cols, "p")
    path_df = fetch_df(
        con,
        f"""
        SELECT
            {selected}
        FROM main.canonical_path_malignant_events_v1 p
        """,
    )
    path_df["research_id"] = path_df["research_id"].astype(str)
    path_df["affected_reason"] = path_df.apply(
        lambda row: classify_reason(row.get("primary_histology"), row.get("histology_variant")),
        axis=1,
    )
    affected = path_df[path_df["affected_reason"] != "other"].copy()
    affected["is_niftp"] = affected["affected_reason"].isin(
        ["niftp_who2017_non_malignant", "niftp_and_uncertain_text"]
    )
    affected["is_uncertain_malignant_potential"] = affected["affected_reason"].isin(
        ["uncertain_malignant_potential", "niftp_and_uncertain_text"]
    )

    key_cols = [col for col in ["research_id", "surgery_episode_id", "tumor_ordinal", "synoptic_row_ix"] if col in affected.columns]
    affected_keys = affected[key_cols].drop_duplicates()
    path_other = path_df.merge(affected_keys.assign(_affected_key=True), on=key_cols, how="left")
    path_other = path_other[path_other["_affected_key"].isna()].copy()

    other_by_patient = (
        path_other.groupby("research_id", dropna=False)
        .agg(
            other_path_malignant_events=("research_id", "size"),
            other_primary_histologies=("primary_histology", lambda s: " | ".join(sorted({norm_text(v) for v in s if norm_text(v)}))[:2000]),
            other_histology_variants=("histology_variant", lambda s: " | ".join(sorted({norm_text(v) for v in s if norm_text(v)}))[:2000]),
        )
        .reset_index()
    )

    cpm_cols = get_columns(con, "canonical_patient_master")
    cpm_desired = [
        "is_malignant",
        "histology_final",
        "histologic_types_all",
        "ajcc8_t_stage",
        "ajcc8_n_stage",
        "ajcc8_stage_group",
        "dominant_tumor_ajcc8_t_stage",
        "dominant_tumor_ajcc8_stage_group",
    ]
    cpm_selected = optional_select(cpm_cols, cpm_desired, "c")
    cpm_extra = f",\n            {cpm_selected}" if cpm_selected else ""
    cpm = fetch_df(
        con,
        f"""
        SELECT
            CAST(c.research_id AS VARCHAR) AS research_id{cpm_extra}
        FROM main.canonical_patient_master c
        """,
    )
    cpm = cpm.loc[:, ~cpm.columns.duplicated()].copy()
    cpm["research_id"] = cpm["research_id"].astype(str)

    patient_summary = (
        affected.groupby("research_id", dropna=False)
        .agg(
            affected_events=("research_id", "size"),
            niftp_events=("is_niftp", "sum"),
            uncertain_events=("is_uncertain_malignant_potential", "sum"),
            affected_primary_histologies=("primary_histology", lambda s: " | ".join(sorted({norm_text(v) for v in s if norm_text(v)}))[:2000]),
            affected_histology_variants=("histology_variant", lambda s: " | ".join(sorted({norm_text(v) for v in s if norm_text(v)}))[:2000]),
        )
        .reset_index()
        .merge(other_by_patient, on="research_id", how="left")
        .merge(cpm, on="research_id", how="left")
    )
    patient_summary["other_path_malignant_events"] = patient_summary["other_path_malignant_events"].fillna(0).astype(int)
    patient_summary["pm_is_malignant_bool"] = patient_summary.get("is_malignant", pd.Series([False] * len(patient_summary))).map(is_true)

    def disposition(row: pd.Series) -> str:
        if row["other_path_malignant_events"] > 0:
            return "mixed_keep_patient_exclude_affected_event_rows"
        if row["pm_is_malignant_bool"]:
            return "edge_no_other_path_event_but_pm_malignant"
        return "niftp_uncertain_only_candidate_patient_exclusion"

    patient_summary["proposed_patient_disposition"] = patient_summary.apply(disposition, axis=1)

    affected = affected.merge(
        patient_summary[["research_id", "other_path_malignant_events", "pm_is_malignant_bool", "proposed_patient_disposition"]],
        on="research_id",
        how="left",
    )
    affected = affected.sort_values(
        ["affected_reason", "research_id", "surgery_episode_id", "tumor_ordinal"],
        na_position="last",
    )
    affected["logan_spot_check_sample"] = False
    if len(affected):
        sample_idx = affected.groupby("affected_reason", sort=True).head(5).head(10).index
        affected.loc[sample_idx, "logan_spot_check_sample"] = True

    inventory = (
        affected.groupby(["primary_histology", "histology_variant", "affected_reason"], dropna=False)
        .agg(
            n_events=("research_id", "size"),
            n_pts=("research_id", pd.Series.nunique),
            n_with_t_stage=("t_stage_ajcc8", lambda s: s.notna().sum()),
            n_with_n_stage=("n_stage_ajcc8", lambda s: s.notna().sum()),
        )
        .reset_index()
        .sort_values(["n_events", "primary_histology", "histology_variant"], ascending=[False, True, True])
    )
    disposition = (
        patient_summary.groupby("proposed_patient_disposition", dropna=False)
        .agg(n_pts=("research_id", "nunique"), n_events=("affected_events", "sum"))
        .reset_index()
        .sort_values(["n_pts", "proposed_patient_disposition"], ascending=[False, True])
    )
    return affected, inventory, patient_summary, disposition


def build_cross_table_impact(con: Any, affected: pd.DataFrame, patient_summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    affected_pts = tuple(sorted(affected["research_id"].astype(str).unique()))
    values_sql = ", ".join([f"('{rid}')" for rid in affected_pts]) or "('')"

    def add(table: str, metric: str, value: Any, note: str = "") -> None:
        rows.append({"table_or_domain": table, "metric": metric, "value": value, "note": note})

    add("canonical_path_malignant_events_v1", "affected_events", len(affected), "Rows matching NIFTP/uncertain text filter")
    add("canonical_path_malignant_events_v1", "affected_patients", affected["research_id"].nunique(), "Distinct affected research_id values")

    for table in [
        "canonical_path_malignant_patient_rollup_v1",
        "canonical_invasion_events_v1",
        "canonical_us_lymph_node_patient_rollup_v2",
        "canonical_patient_master",
        "canonical_tumor_characteristics_v1",
        "tumor_episode_master_v2",
    ]:
        try:
            if not get_columns(con, table):
                add(table, "table_presence", "absent", "Not present in main schema of target DB at scoping time")
                continue
            count_df = fetch_df(
                con,
                f"""
                WITH affected(research_id) AS (VALUES {values_sql})
                SELECT
                    COUNT(*) AS rows_for_affected_patients,
                    COUNT(DISTINCT CAST(t.research_id AS VARCHAR)) AS affected_patients_present
                FROM main.{table} t
                JOIN affected a ON CAST(t.research_id AS VARCHAR) = a.research_id
                """,
            )
            add(table, "rows_for_affected_patients", int(count_df.iloc[0]["rows_for_affected_patients"]), "Patient-level join to affected rids")
            add(table, "affected_patients_present", int(count_df.iloc[0]["affected_patients_present"]), "Distinct affected rids present")
        except Exception as exc:  # pragma: no cover - live optional table guard
            add(table, "probe_error", str(exc), "Probe failed")

    if "histologic_types_all" in patient_summary.columns:
        hist_niftp = patient_summary["histologic_types_all"].fillna("").astype(str).str.contains("NIFTP", case=False, regex=False).sum()
        hist_uncertain = patient_summary["histologic_types_all"].fillna("").astype(str).str.contains("uncertain", case=False, regex=False).sum()
        add("canonical_patient_master", "affected_patients_with_niftp_in_histologic_types_all", int(hist_niftp), "CPM text field contains NIFTP")
        add("canonical_patient_master", "affected_patients_with_uncertain_in_histologic_types_all", int(hist_uncertain), "CPM text field contains uncertain")

    for _, row in patient_summary["proposed_patient_disposition"].value_counts().rename_axis("bucket").reset_index(name="n_pts").iterrows():
        add("cohort_impact", row["bucket"], int(row["n_pts"]), "Patient-disposition bucket")

    return pd.DataFrame(rows)


def write_sql() -> None:
    SQL_PATH.parent.mkdir(parents=True, exist_ok=True)
    SQL_PATH.write_text(
        """-- mig_186 NIFTP + uncertain-malignancy exclusion skeleton
-- Date: 2026-04-30
-- Status: PLACEHOLDER / NOT FOR EXECUTION until Logan ratifies disposition rule.
-- Target DB: thyroid_canonical_publication_v1_0
-- Recommended rule from scoping report: R-D hybrid — archive affected rows, then
-- delete NIFTP/uncertain-malignancy rows from canonical_path_malignant_events_v1,
-- preserving them in an indeterminate/provenance archive and opening
-- CF-mig186-WHO-2017-NIFTP-RECLASS for downstream rebuild review.

-- ---------------------------------------------------------------------------
-- A. Pre-snapshot affected rows before any mutation.
-- ---------------------------------------------------------------------------
-- CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig186_niftp_uncertain_YYYYMMDD AS
-- SELECT *
-- FROM thyroid_canonical_publication_v1_0.main.canonical_path_malignant_events_v1
-- WHERE primary_histology ILIKE '%NIFTP%'
--    OR histology_variant ILIKE '%NIFTP%'
--    OR primary_histology ILIKE '%uncertain%'
--    OR primary_histology ILIKE '%hurthle%neoplasm%'
--    OR primary_histology ILIKE '%hürthle%neoplasm%'
--    OR primary_histology ILIKE '%FT-UMP%'
--    OR primary_histology ILIKE '%WDT-UMP%';

-- Optional provenance-preserving indeterminate landing table if Logan chooses R-B/R-D.
-- CREATE TABLE IF NOT EXISTS thyroid_canonical_publication_v1_0.main.canonical_path_indeterminate_events_v1 AS
-- SELECT *,
--        'mig186_niftp_uncertain_exclusion'::VARCHAR AS indeterminate_reason,
--        CURRENT_TIMESTAMP AS reclassified_at
-- FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig186_niftp_uncertain_YYYYMMDD
-- WHERE FALSE;

-- INSERT INTO thyroid_canonical_publication_v1_0.main.canonical_path_indeterminate_events_v1
-- SELECT *,
--        CASE
--          WHEN primary_histology ILIKE '%NIFTP%' OR histology_variant ILIKE '%NIFTP%'
--            THEN 'NIFTP_WHO_2017_non_malignant'
--          ELSE 'uncertain_malignant_potential'
--        END AS indeterminate_reason,
--        CURRENT_TIMESTAMP AS reclassified_at
-- FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig186_niftp_uncertain_YYYYMMDD;

-- ---------------------------------------------------------------------------
-- B. Logan-ratified exclusion from malignant event table.
-- ---------------------------------------------------------------------------
-- DELETE FROM thyroid_canonical_publication_v1_0.main.canonical_path_malignant_events_v1
-- WHERE primary_histology ILIKE '%NIFTP%'
--    OR histology_variant ILIKE '%NIFTP%'
--    OR primary_histology ILIKE '%uncertain%'
--    OR primary_histology ILIKE '%hurthle%neoplasm%'
--    OR primary_histology ILIKE '%hürthle%neoplasm%'
--    OR primary_histology ILIKE '%FT-UMP%'
--    OR primary_histology ILIKE '%WDT-UMP%';

-- ---------------------------------------------------------------------------
-- C. Cascade rebuild placeholders after row-level exclusion.
-- ---------------------------------------------------------------------------
-- Rebuild/refresh, in dependency order, any downstream path malignant rollups,
-- patient-level dominant tumor fields, and manuscript registry metrics that read
-- canonical_path_malignant_events_v1. Do not run until dependency list is
-- enumerated and Logan approves exact scope.

-- ---------------------------------------------------------------------------
-- D. Registry/provenance note placeholders.
-- ---------------------------------------------------------------------------
-- INSERT INTO thyroid_canonical_publication_v1_0.manuscript_workspace.cpm_reconciliation_provenance_v1
--   (run_id, started_at, ended_at, phases_applied, critical_findings_cleared,
--    high_findings_cleared, med_findings_cleared, held_for_adjudication)
-- VALUES
--   ('mig186_niftp_uncertain_exclusion_apply_YYYYMMDD', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
--    'archive_indeterminate_delete_rebuild_registry', '0', '0', '0', 'CF-mig186-WHO-2017-NIFTP-RECLASS');
""",
        encoding="utf-8",
    )


def write_report(
    inventory: pd.DataFrame,
    cross_table_impact: pd.DataFrame,
    patient_summary: pd.DataFrame,
    disposition: pd.DataFrame,
    affected: pd.DataFrame,
) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    total_events = len(affected)
    total_patients = affected["research_id"].nunique()
    niftp_events = int(affected["is_niftp"].sum())
    uncertain_events = int(affected["is_uncertain_malignant_potential"].sum())
    niftp_patients = int(affected.loc[affected["is_niftp"], "research_id"].nunique())
    uncertain_patients = int(affected.loc[affected["is_uncertain_malignant_potential"], "research_id"].nunique())
    spot_cols = [
        col
        for col in [
            "research_id",
            "surgery_episode_id",
            "tumor_ordinal",
            "primary_histology",
            "histology_variant",
            "affected_reason",
            "proposed_patient_disposition",
        ]
        if col in affected.columns
    ]
    spot = affected[affected["logan_spot_check_sample"]][spot_cols].head(10)
    report = f"""# mig_186 NIFTP + uncertain-malignancy exclusion scoping

**Date:** 2026-04-30  
**Run ID:** `{RUN_ID}`  
**Posture:** READ-ONLY scoping; no MotherDuck DDL/DML executed.  
**Target DB:** `{TARGET_DB}`  
**Prompt:** `cursor_prompts/CURSOR_PROMPT_mig186_niftp_uncertain_exclusion_20260430.md`

## Executive summary

- Affected rows in `main.canonical_path_malignant_events_v1`: **{total_events:,} events / {total_patients:,} combined patients**.
- NIFTP-classified affected rows: **{niftp_events:,} events / {niftp_patients:,} patients**.
- Uncertain-malignant-potential affected rows: **{uncertain_events:,} events / {uncertain_patients:,} patients**.
- Recommended disposition remains **R-D hybrid**: snapshot affected rows, move/copy them to an indeterminate provenance table if desired, then exclude them from `canonical_path_malignant_events_v1` after Logan ratifies the rule.
- This run only authored a placeholder apply SQL; it did **not** execute any mutation against MotherDuck.

## 1. Full histology inventory

{md_table(inventory)}

## 2. Cross-table cascade analysis

{md_table(cross_table_impact, max_rows=120)}

## 3. Cohort impact: NIFTP-only vs mixed

{md_table(disposition)}

Interpretation:

- `mixed_keep_patient_exclude_affected_event_rows`: patient has at least one additional path-malignant event outside the NIFTP/uncertain rows; exclude the affected event row(s), but keep the patient in malignant-cohort analyses if the other malignant event remains valid.
- `edge_no_other_path_event_but_pm_malignant`: no other event in `canonical_path_malignant_events_v1`, but `canonical_patient_master.is_malignant` is true; requires rule review before patient-level cohort exclusion.
- `niftp_uncertain_only_candidate_patient_exclusion`: no other path-malignant event and CPM does not mark malignant; likely patient-level exclusion candidate if Logan ratifies WHO-2017 NIFTP/uncertain exclusion.

## 4. Disposition rule comparison

| Rule | Approach | Pros | Cons | Recommendation |
|---|---|---|---|---|
| R-A | Delete affected rows from `canonical_path_malignant_events_v1` | Clean manuscript malignant event table | Requires external audit trail | Accept only with pre-snapshot |
| R-B | Move rows to `canonical_path_indeterminate_events_v1` | Preserves queryable provenance | Adds new canonical surface | Good if downstream consumers need indeterminate events |
| R-C | Add `is_malignant_per_who_2017` flag | Non-destructive | Every consumer must remember filter | Too easy to misuse |
| R-D | Archive + optional indeterminate table + delete from malignant events | Clean malignant semantics and preserved provenance | Requires Logan ratification + downstream rebuild checklist | **Recommended** |

## 5. Manuscript implications

- NIFTP is non-malignant under WHO 2017 terminology; retaining these rows in a malignant event table can inflate malignant tumor/event counts.
- Mixed patients should remain analyzable through their non-NIFTP malignant event(s), but the NIFTP/uncertain event rows should be excluded from malignant-event denominators.
- NIFTP-only or uncertain-only patients should be excluded from malignant-cohort denominators unless a separate ratified malignant criterion exists outside the affected row.
- Any apply must open/track `CF-mig186-WHO-2017-NIFTP-RECLASS` and refresh dependent rollups after deletion.

## 6. Logan spot-check sample

Full inventory CSV: `exports/{RUN_ID}/niftp_uncertain_inventory.csv`

{md_table(spot, max_rows=10)}

## Deliverables written

- `qc_framework_v1/migrations/186_niftp_uncertain_exclusion_TBD_20260430.sql`
- `qc_framework_v1/reports/mig_186_niftp_uncertain_exclusion_scoping_20260430.md`
- `exports/{RUN_ID}/niftp_uncertain_inventory.csv`
- `exports/{RUN_ID}/niftp_uncertain_patient_disposition.csv`
- `exports/{RUN_ID}/manifest.json`
"""
    REPORT_PATH.write_text(report, encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = connect_locked()
    affected, inventory, patient_summary, disposition = build_inventory(con)
    cross_table_impact = build_cross_table_impact(con, affected, patient_summary)
    affected.to_csv(INVENTORY_CSV, index=False)
    patient_summary.to_csv(PATIENT_SUMMARY_CSV, index=False)
    write_sql()
    write_report(inventory, cross_table_impact, patient_summary, disposition, affected)

    manifest = {
        "run_id": RUN_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "posture": "read_only_scoping_no_motherduck_ddl_or_dml",
        "target_db": TARGET_DB,
        "affected_events": int(len(affected)),
        "affected_patients": int(affected["research_id"].nunique()),
        "niftp_events": int(affected["is_niftp"].sum()),
        "niftp_patients": int(affected.loc[affected["is_niftp"], "research_id"].nunique()),
        "uncertain_events": int(affected["is_uncertain_malignant_potential"].sum()),
        "uncertain_patients": int(affected.loc[affected["is_uncertain_malignant_potential"], "research_id"].nunique()),
        "patient_disposition": disposition.to_dict(orient="records"),
        "artifacts": [
            str(SQL_PATH.relative_to(REPO_ROOT)),
            str(REPORT_PATH.relative_to(REPO_ROOT)),
            str(INVENTORY_CSV.relative_to(REPO_ROOT)),
            str(PATIENT_SUMMARY_CSV.relative_to(REPO_ROOT)),
        ],
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()