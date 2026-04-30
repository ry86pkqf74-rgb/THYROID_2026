#!/usr/bin/env python3
"""Build mig_185 read-only duplicate scoping artifacts.

This script performs only SELECTs against the locked MotherDuck publication
DB and writes the local artifacts requested by
cursor_prompts/CURSOR_PROMPT_mig185_path_malignant_duplicate_probe_20260430.md.
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

RUN_ID = "mig185_dedupe_scoping_20260430"
OUT_DIR = REPO_ROOT / "exports" / RUN_ID
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_185_path_malignant_duplicate_scoping_20260430.md"
SQL_PATH = REPO_ROOT / "qc_framework_v1" / "migrations" / "185_path_malignant_dedupe_TBD_20260430.sql"

KEY_COLS = ["research_id", "surgery_episode_id", "tumor_ordinal"]
AUDIT_COLS = {"build_script", "build_ts", "consolidation_source", "extracted_at"}
SOURCE_COLS = {
    "path_surgery_id",
    "specimen_id",
    "synoptic_row_ix",
    "source_tables",
    "resolution_rule",
    "specimen_focus_id",
    "linkage_confidence_tier",
    "linkage_score",
    "staging_source_note",
}

LINEAGE_TABLES = [
    "main.canonical_path_malignant_events_v1",
    '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_20260422_002245',
    '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_date_retype_20260428',
    '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig178_20260429_205720',
    '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig178_20260429_205801',
    '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig178_20260429_205813',
]


def norm(value: Any) -> str:
    if pd.isna(value):
        return "<NULL>"
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def nunique_norm(series: pd.Series) -> int:
    return series.map(norm).nunique(dropna=False)


def fetch_df(con: Any, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def md_table(df: pd.DataFrame, max_rows: int = 80) -> str:
    if df.empty:
        return "_No rows._"
    return df.head(max_rows).to_markdown(index=False)


def table_counts(con: Any, table_fq: str) -> dict[str, Any]:
    try:
        total_rows, distinct_grains, duplicate_excess = con.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT (CAST(research_id AS VARCHAR), surgery_episode_id, tumor_ordinal)) AS distinct_grains,
                COUNT(*) - COUNT(DISTINCT (CAST(research_id AS VARCHAR), surgery_episode_id, tumor_ordinal)) AS duplicate_excess
            FROM {table_fq}
            WHERE primary_histology IS NOT NULL
              AND TRIM(CAST(primary_histology AS VARCHAR)) <> ''
            """
        ).fetchone()
        return {
            "table": table_fq,
            "total_rows": total_rows,
            "distinct_grains": distinct_grains,
            "duplicate_excess": duplicate_excess,
            "error": "",
        }
    except Exception as exc:  # pragma: no cover - live schema guard
        return {
            "table": table_fq,
            "total_rows": None,
            "distinct_grains": None,
            "duplicate_excess": None,
            "error": str(exc),
        }


def classify_duplicate_grains(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df["research_id"] = df["research_id"].astype(str)
    cols = list(df.columns)
    non_key_cols = [c for c in cols if c not in KEY_COLS]
    clinical_cols = [c for c in non_key_cols if c not in AUDIT_COLS and c not in SOURCE_COLS]

    rows_per_grain = df.groupby(KEY_COLS, dropna=False, sort=True).size().reset_index(name="n_rows")
    duplicate_keys = rows_per_grain[rows_per_grain["n_rows"] > 1].copy()
    duplicate_events = df.merge(duplicate_keys[KEY_COLS], on=KEY_COLS, how="inner")

    records: list[dict[str, Any]] = []
    for key, sub in duplicate_events.groupby(KEY_COLS, dropna=False, sort=True):
        diff_cols = [col for col in non_key_cols if nunique_norm(sub[col]) > 1]
        diff_set = set(diff_cols)
        clinical_diff = sorted([col for col in diff_cols if col in clinical_cols])
        source_diff = sorted([col for col in diff_cols if col in SOURCE_COLS])
        audit_diff = sorted([col for col in diff_cols if col in AUDIT_COLS])

        if not diff_cols:
            bucket = "A_fully_identical"
        elif diff_set <= AUDIT_COLS:
            bucket = "B_differs_in_audit_only"
        elif not clinical_diff and source_diff:
            bucket = "C_differs_in_synoptic_or_source_only"
        else:
            bucket = "D_differs_clinically"

        records.append(
            {
                "research_id": key[0],
                "surgery_episode_id": key[1],
                "tumor_ordinal": key[2],
                "n_rows": len(sub),
                "excess_rows": len(sub) - 1,
                "bucket": bucket,
                "diff_cols": "|".join(diff_cols),
                "clinical_diff_cols": "|".join(clinical_diff),
                "source_diff_cols": "|".join(source_diff),
                "audit_diff_cols": "|".join(audit_diff),
                "synoptic_row_ix_values": "|".join(sorted({norm(v) for v in sub.get("synoptic_row_ix", pd.Series([], dtype=object))})),
                "specimen_id_values": "|".join(sorted({norm(v) for v in sub.get("specimen_id", pd.Series([], dtype=object))})),
                "primary_histology_values": "|".join(sorted({norm(v) for v in sub.get("primary_histology", pd.Series([], dtype=object))})),
                "histology_variant_values": "|".join(sorted({norm(v) for v in sub.get("histology_variant", pd.Series([], dtype=object))})),
                "size_values": "|".join(sorted({norm(v) for v in sub.get("size_greatest_dimension_cm", pd.Series([], dtype=object))})),
                "ete_values": "|".join(sorted({norm(v) for v in sub.get("extrathyroidal_extension", pd.Series([], dtype=object))})),
                "t_stage_ajcc8_values": "|".join(sorted({norm(v) for v in sub.get("t_stage_ajcc8", pd.Series([], dtype=object))})),
            }
        )

    return pd.DataFrame(records), duplicate_events, rows_per_grain


def build_downstream_impact(con: Any, event_df: pd.DataFrame, rows_per_grain: pd.DataFrame) -> pd.DataFrame:
    impact: list[dict[str, Any]] = []

    def add_metric(domain: str, metric: str, value: Any, note: str = "") -> None:
        impact.append({"domain": domain, "metric": metric, "value": value, "note": note})

    duplicate_excess = len(event_df) - len(rows_per_grain)
    duplicate_grains = int((rows_per_grain["n_rows"] > 1).sum())
    add_metric("path_malignant_events", "total_rows", len(event_df))
    add_metric("path_malignant_events", "distinct_key_grains", len(rows_per_grain))
    add_metric("path_malignant_events", "duplicate_excess_rows", duplicate_excess)
    add_metric("path_malignant_events", "duplicate_grains", duplicate_grains)

    rollup = fetch_df(
        con,
        """
        WITH dedup AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 COUNT(DISTINCT (CAST(research_id AS VARCHAR), surgery_episode_id, tumor_ordinal)) AS n_tumors_dedup
          FROM main.canonical_path_malignant_events_v1
          GROUP BY 1
        ), live AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id, n_tumors_total
          FROM main.canonical_path_malignant_patient_rollup_v1
        )
        SELECT COUNT(*) AS patients_compared,
               SUM(CASE WHEN live.n_tumors_total > dedup.n_tumors_dedup THEN 1 ELSE 0 END) AS patients_inflated,
               SUM(live.n_tumors_total - dedup.n_tumors_dedup) AS tumor_count_excess
        FROM live JOIN dedup USING (research_id)
        """,
    )
    for col, val in rollup.iloc[0].items():
        add_metric(
            "canonical_path_malignant_patient_rollup_v1",
            col,
            int(val) if pd.notna(val) else "",
            "n_tumors_total uses COUNT(*) over event rows",
        )

    inv_cols = fetch_df(
        con,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog='thyroid_canonical_publication_v1_0'
          AND table_schema='main'
          AND table_name='canonical_invasion_events_v1'
        ORDER BY ordinal_position
        """,
    )["column_name"].tolist()
    add_metric("canonical_invasion_events_v1", "columns", ";".join(inv_cols), "schema inventory")
    if "linked_surgery_episode_id" in inv_cols:
        invasion = fetch_df(
            con,
            """
            WITH dup_surg AS (
              SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id, surgery_episode_id
              FROM main.canonical_path_malignant_events_v1
              GROUP BY 1,2,tumor_ordinal
              HAVING COUNT(*) > 1
            )
            SELECT COUNT(*) AS invasion_rows_total,
                   SUM(CASE WHEN d.research_id IS NOT NULL THEN 1 ELSE 0 END) AS invasion_rows_on_duplicate_surgery_grains,
                   COUNT(DISTINCT CASE WHEN d.research_id IS NOT NULL THEN CAST(i.research_id AS VARCHAR) END) AS patients_with_invasion_on_duplicate_surgery_grains
            FROM main.canonical_invasion_events_v1 i
            LEFT JOIN dup_surg d
              ON CAST(i.research_id AS VARCHAR)=d.research_id
             AND i.linked_surgery_episode_id=d.surgery_episode_id
            """,
        )
        for col, val in invasion.iloc[0].items():
            add_metric(
                "canonical_invasion_events_v1",
                col,
                int(val) if pd.notna(val) else "",
                "Joinable at patient+surgery only; no tumor_ordinal in invasion table",
            )

    try:
        us_rollup = fetch_df(
            con,
            """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT CAST(research_id AS VARCHAR)) AS distinct_patients,
                   COUNT(*)-COUNT(DISTINCT CAST(research_id AS VARCHAR)) AS duplicate_patient_rows
            FROM main.canonical_us_lymph_node_patient_rollup_v2
            """,
        )
        for col, val in us_rollup.iloc[0].items():
            add_metric(
                "canonical_us_lymph_node_patient_rollup_v2",
                col,
                int(val) if pd.notna(val) else "",
                "patient-grain rollup; not joined at path-event key",
            )
    except Exception as exc:  # pragma: no cover - optional table guard
        add_metric("canonical_us_lymph_node_patient_rollup_v2", "probe_error", str(exc))

    return pd.DataFrame(impact)


def write_sample_csv(duplicate_events: pd.DataFrame, classification: pd.DataFrame) -> None:
    sample_keys = classification.groupby("bucket", sort=True).head(5)[
        KEY_COLS + ["bucket", "n_rows", "excess_rows", "diff_cols"]
    ]
    sample_events = duplicate_events.merge(sample_keys, on=KEY_COLS, how="inner")
    sample_cols = [
        "bucket",
        *KEY_COLS,
        "n_rows",
        "excess_rows",
        "diff_cols",
        "surgery_date",
        "path_surgery_id",
        "specimen_id",
        "synoptic_row_ix",
        "primary_histology",
        "histology_variant",
        "size_greatest_dimension_cm",
        "tumor_size_cm_per_surgery",
        "extrathyroidal_extension",
        "gross_ete",
        "t_stage_ajcc8",
        "n_stage_ajcc8",
        "stage_group_ajcc8",
        "source_tables",
        "resolution_rule",
        "specimen_focus_id",
        "linkage_confidence_tier",
        "linkage_score",
        "build_script",
        "build_ts",
        "consolidation_source",
    ]
    sample_cols = [col for col in sample_cols if col in sample_events.columns]
    sample_events[sample_cols].sort_values(
        ["bucket", "research_id", "surgery_episode_id", "tumor_ordinal"]
    ).to_csv(OUT_DIR / "duplicate_examples.csv", index=False)


def write_report(
    event_df: pd.DataFrame,
    rows_per_grain: pd.DataFrame,
    classification: pd.DataFrame,
    rows_per_summary: pd.DataFrame,
    class_summary: pd.DataFrame,
    syn_summary: pd.DataFrame,
    lineage: pd.DataFrame,
    impact: pd.DataFrame,
) -> None:
    duplicate_excess = len(event_df) - len(rows_per_grain)
    duplicate_grains = int((rows_per_grain["n_rows"] > 1).sum())
    r_a_excess = int(
        classification.loc[classification["bucket"] == "A_fully_identical", "excess_rows"].sum()
    )
    r_b_excess = len(event_df) - len(
        event_df.drop_duplicates(KEY_COLS + ["synoptic_row_ix"])
    )
    null_surgery_summary = (
        classification.assign(surgery_episode_id_is_null=classification["surgery_episode_id"].isna())
        .groupby(["bucket", "surgery_episode_id_is_null"])
        .agg(n_grains=("research_id", "count"), duplicate_excess_rows=("excess_rows", "sum"))
        .reset_index()
    )

    report = f"""# mig_185 — canonical_path_malignant_events_v1 duplicate scoping

**Date:** 2026-04-30  
**Posture:** READ-ONLY scoping. No MotherDuck DDL/DML executed.  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Trigger:** Logan rid 2480 duplicate finding during R1 size CSV review.  

## Executive summary

Read-only probes confirm `main.canonical_path_malignant_events_v1` has **{len(event_df):,} rows** and **{len(rows_per_grain):,} distinct `(research_id, surgery_episode_id, tumor_ordinal)` grains**, for **{duplicate_excess:,} excess duplicate rows** across **{duplicate_grains:,} duplicate grains**.

The duplicate signal is already present in the Script 361 upstream archive `canonical_tumor_characteristics_v1_pre361_20260422_002245`, with the same **6,689 / 6,156 / 533** row/distinct/excess pattern. This supports the lineage finding that Script 361 faithfully copied pre-existing CTC duplicate grain rows rather than introducing them with later date-retype or mig178 passes.

## §1 Duplicate-pattern classification

### Rows per duplicate grain

{md_table(rows_per_summary)}

### Bucket definitions used

- `A_fully_identical`: all 56 columns are identical within the duplicate grain.
- `B_differs_in_audit_only`: only `build_script`, `build_ts`, `consolidation_source`, or `extracted_at` differ.
- `C_differs_in_synoptic_or_source_only`: clinical fields are identical, but source/provenance fields differ (`synoptic_row_ix`, `specimen_id`, `path_surgery_id`, `source_tables`, linkage fields, etc.).
- `D_differs_clinically`: at least one clinical/finding field differs (size, histology, ETE, invasion, margin, LN, stage, multifocality, completeness, etc.).

### Classification summary

{md_table(class_summary)}

### NULL surgery-episode contribution

{md_table(null_surgery_summary)}

**Important interpretation:** no duplicate grains are fully identical across all columns. The rid 2480 finding is identical on the selected review fields in the prompt, but the full-row comparison shows differences in source identifiers plus `site` / `data_completeness_pct`. In other words, the current 533 excess rows are mostly **source-distinct or clinically distinct rows sharing an under-specified logical key**, not simple byte-identical row repeats.

## §2 Script 361 lineage trace

Script 361 Step 1 (`scripts/361_op_path_consolidation.py`) builds `canonical_path_malignant_events_v1` with a faithful filtered copy:

```sql
CREATE OR REPLACE TABLE main.canonical_path_malignant_events_v1 AS
SELECT *
FROM main.canonical_tumor_characteristics_v1
WHERE primary_histology IS NOT NULL
  AND TRIM(CAST(primary_histology AS VARCHAR)) <> '';
```

The script then adds discordance/linkage/provenance columns. There is **no `ROW_NUMBER()` / `QUALIFY` dedupe gate** on `(research_id, surgery_episode_id, tumor_ordinal)` in Step 1.

### Lineage duplicate counts

{md_table(lineage)}

**Conclusion:** the 533 excess duplicate rows trace back to `canonical_tumor_characteristics_v1_pre361_20260422_002245`. Later archives preserve the same duplicate count, so later date-retype / mig178 operations did not create the duplicate grain problem.

## §3 `synoptic_row_ix` tiebreaker analysis

{md_table(syn_summary)}

`canonical_path_malignant_events_v1.synoptic_row_ix` is populated on current rows. For duplicate grains, multi-`synoptic_row_ix` patterns are concentrated in the source/provenance-only and clinical-difference buckets. This indicates most duplicate grains represent multiple source synoptic rows mapped to the same logical `(rid, surgery_episode_id, tumor_ordinal)` event. There is no meaningful fully-identical cleanup opportunity in the live table.

Sample rows for Logan spot-check are exported to `exports/mig185_dedupe_scoping_20260430/duplicate_examples.csv`. Full grain classification is exported to `exports/mig185_dedupe_scoping_20260430/duplicate_grain_classification.csv`.

## §4 Dedupe rule recommendation for Logan ratification

| Rule | Result from scoping | Recommendation |
|---|---:|---|
| R-A: drop fully-identical duplicates only | {r_a_excess:,} excess rows | Safe but **no-op** on current live data; useful as a guardrail, not as the mig_185 fix. |
| R-B: dedupe by `(rid, surg_ep, tumor_ord, synoptic_row_ix)` keeping max `build_ts` | {r_b_excess:,} excess rows | Also nearly no-op; confirms `synoptic_row_ix` is usually source-distinct and should be preserved rather than collapsed. |
| R-C: dedupe by `(rid, surg_ep, tumor_ord)` keeping maximum completeness | Aggressive | Not recommended without manual review of Bucket D because it can discard clinically distinct multi-row evidence. |

**Recommended Logan decision:** do **not** ratify a blind delete on the current key. Ratify a follow-up design lane that either (1) expands the path-malignant event grain to include a stable source-row/focus discriminator such as `synoptic_row_ix` / `specimen_id`, or (2) preserves all Bucket C/D rows in a review/staging table before any R-C-style one-row-per-key collapse. If an immediate patient-rollup correction is needed, recompute `n_tumors_total` from distinct `(research_id, surgery_episode_id, tumor_ordinal)` while leaving event rows untouched.

## §5 Logan spot-check samples

See:

- `exports/mig185_dedupe_scoping_20260430/duplicate_examples.csv`
- `exports/mig185_dedupe_scoping_20260430/duplicate_grain_classification.csv`
- `exports/mig185_dedupe_scoping_20260430/lineage_duplicate_counts.csv`
- `exports/mig185_dedupe_scoping_20260430/downstream_impact_metrics.csv`

## §6 Downstream impact

{md_table(impact, max_rows=120)}

Key impact interpretation:

1. `canonical_path_malignant_patient_rollup_v1.n_tumors_total` is inflated because Script 361 uses `COUNT(*)` over malignant events. The excess equals the duplicate-row excess where duplicate event rows pass through the rollup.
2. Boolean/max patient rollup fields are less sensitive, but mode/COUNT-derived fields can be affected.
3. `canonical_invasion_events_v1` is not keyed to `tumor_ordinal`; it can only be compared at patient+surgery grain, so path-event duplicate cleanup should be validated against invasion linkage before any aggressive collapse.
4. `canonical_us_lymph_node_patient_rollup_v2` is patient-grain and not directly duplicated by the path malignant event key; it should be treated as a downstream contextual rollup rather than an event-grain victim.

## Governance

This report and companion SQL are read-only scoping deliverables. No writes were made to MotherDuck. Any dedupe apply must be ratified by Logan before execution.
"""
    REPORT_PATH.write_text(report, encoding="utf-8")


def write_skeleton_sql(total_rows: int, distinct_grains: int, duplicate_excess: int) -> None:
    sql = f"""-- mig_185 — canonical_path_malignant_events_v1 duplicate dedupe skeleton
-- Date: 2026-04-30
-- Posture: PLACEHOLDER ONLY. DO NOT EXECUTE until Logan ratifies a rule.
-- Target DB: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Scoping summary from read-only report:
--   total rows: {total_rows:,}
--   distinct (research_id, surgery_episode_id, tumor_ordinal): {distinct_grains:,}
--   excess duplicate rows: {duplicate_excess:,}
--
-- LOGAN MUST RATIFY RULE BEFORE EXECUTION.

USE thyroid_canonical_publication_v1_0;
USE thyroid_canonical_publication_v1_0.main;

-- --------------------------------------------------------------------------
-- §A. Pre-snapshot (required before any future apply)
-- --------------------------------------------------------------------------
-- CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig185_<UTC> AS
-- SELECT * FROM main.canonical_path_malignant_events_v1;
--
-- CREATE TABLE manuscript_workspace.mig185_path_malignant_duplicate_pre_snapshot_<UTC> AS
-- WITH grain_counts AS (
--   SELECT research_id, surgery_episode_id, tumor_ordinal, COUNT(*) AS n_rows
--   FROM main.canonical_path_malignant_events_v1
--   GROUP BY 1,2,3
--   HAVING COUNT(*) > 1
-- )
-- SELECT * FROM grain_counts;

-- --------------------------------------------------------------------------
-- §B1. R-A candidate: drop fully-identical duplicate rows only
-- Safest/lossless. Requires implementation using a stable full-row partition
-- over all columns plus ROW_NUMBER within identical row partitions.
-- Scoping result: no-op on current live data (0 excess fully-identical rows).
-- --------------------------------------------------------------------------
-- LOGAN MUST RATIFY R-A BEFORE EXECUTION.
-- CREATE OR REPLACE TABLE main.canonical_path_malignant_events_v1_mig185_ra AS
-- SELECT * EXCLUDE (mig185_rn)
-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (
--            PARTITION BY
--              research_id, surgery_episode_id, tumor_ordinal,
--              surgery_date, path_surgery_id, specimen_id, synoptic_row_ix,
--              laterality, site, size_greatest_dimension_cm,
--              tumor_size_cm_per_surgery, primary_histology, histology_variant,
--              histology_source, extrathyroidal_extension, gross_ete,
--              lymphatic_invasion, vascular_invasion, angioinvasion_quantify,
--              perineural_invasion, capsular_invasion, margin_status,
--              ln_examined, ln_involved, nodal_disease_positive_count,
--              nodal_disease_total_count, extranodal_extension, number_of_tumors,
--              multifocality_flag, source_tables, resolution_rule,
--              data_completeness_pct, t_stage_ajcc7, n_stage_ajcc7,
--              m_stage_ajcc7, overall_stage_ajcc7, stage_group_ajcc7,
--              t_stage_ajcc8, n_stage_ajcc8, m_stage_ajcc8,
--              overall_stage_ajcc8, stage_group_ajcc8,
--              ajcc7_stage_calculable_flag, ajcc8_stage_calculable_flag,
--              staging_source_note, stage_migration_7_to_8,
--              discordance_histology_flag, discordance_t_stage_flag,
--              discordance_laterality_flag, discordance_notes,
--              specimen_focus_id, linkage_confidence_tier, linkage_score,
--              build_script, build_ts, consolidation_source
--            ORDER BY research_id
--          ) AS mig185_rn
--   FROM main.canonical_path_malignant_events_v1
-- )
-- WHERE mig185_rn = 1;

-- --------------------------------------------------------------------------
-- §B2. R-B candidate: dedupe build-copy duplicates within synoptic source row
-- Scoping result: near no-op on current live data (3 excess rows).
-- --------------------------------------------------------------------------
-- LOGAN MUST RATIFY R-B BEFORE EXECUTION.
-- CREATE OR REPLACE TABLE main.canonical_path_malignant_events_v1_mig185_rb AS
-- SELECT * EXCLUDE (mig185_rn)
-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (
--            PARTITION BY research_id, surgery_episode_id, tumor_ordinal, synoptic_row_ix
--            ORDER BY build_ts DESC NULLS LAST, data_completeness_pct DESC NULLS LAST
--          ) AS mig185_rn
--   FROM main.canonical_path_malignant_events_v1
-- )
-- WHERE mig185_rn = 1;

-- --------------------------------------------------------------------------
-- §B3. R-C candidate: one row per logical event grain by completeness score
-- Aggressive; can discard clinically/source-distinct evidence.
-- --------------------------------------------------------------------------
-- LOGAN MUST RATIFY R-C BEFORE EXECUTION.
-- CREATE OR REPLACE TABLE main.canonical_path_malignant_events_v1_mig185_rc AS
-- SELECT * EXCLUDE (mig185_rn)
-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (
--            PARTITION BY research_id, surgery_episode_id, tumor_ordinal
--            ORDER BY
--              data_completeness_pct DESC NULLS LAST,
--              (CASE WHEN size_greatest_dimension_cm IS NOT NULL THEN 1 ELSE 0 END
--               + CASE WHEN primary_histology IS NOT NULL THEN 1 ELSE 0 END
--               + CASE WHEN extrathyroidal_extension IS NOT NULL THEN 1 ELSE 0 END
--               + CASE WHEN t_stage_ajcc8 IS NOT NULL THEN 1 ELSE 0 END
--               + CASE WHEN n_stage_ajcc8 IS NOT NULL THEN 1 ELSE 0 END
--               + CASE WHEN stage_group_ajcc8 IS NOT NULL THEN 1 ELSE 0 END) DESC,
--              build_ts DESC NULLS LAST,
--              synoptic_row_ix ASC NULLS LAST
--          ) AS mig185_rn
--   FROM main.canonical_path_malignant_events_v1
-- )
-- WHERE mig185_rn = 1;

-- --------------------------------------------------------------------------
-- §C. Registry/provenance notes for future apply
-- --------------------------------------------------------------------------
-- Future apply must:
--   1. archive the pre-image to archive_pub_v1_0;
--   2. rebuild canonical_path_malignant_patient_rollup_v1 from deduped events;
--   3. refresh any md_ mirrors/readable views if applicable;
--   4. insert a per-phase row into manuscript_workspace.cpm_reconciliation_provenance_v1;
--   5. document affected tier-2 AJCC/invasion columns in canonical_column_verification_registry_v1.
"""
    SQL_PATH.write_text(sql, encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = connect_locked()
    event_df = fetch_df(con, "SELECT * FROM main.canonical_path_malignant_events_v1")
    classification, duplicate_events, rows_per_grain = classify_duplicate_grains(event_df)

    classification.to_csv(OUT_DIR / "duplicate_grain_classification.csv", index=False)
    write_sample_csv(duplicate_events, classification)

    duplicate_keys = rows_per_grain[rows_per_grain["n_rows"] > 1].copy()
    rows_per_summary = (
        duplicate_keys.groupby("n_rows")
        .agg(n_grains_with_this_count=("research_id", "count"), total_event_rows=("n_rows", "sum"))
        .reset_index()
        .sort_values("n_rows")
    )
    class_summary = (
        classification.groupby("bucket")
        .agg(n_grains=("research_id", "count"), duplicate_excess_rows=("excess_rows", "sum"), total_event_rows=("n_rows", "sum"))
        .reset_index()
        .sort_values("bucket")
    )
    syn_summary = (
        classification.assign(has_multi_syn=classification["synoptic_row_ix_values"].str.contains("|", regex=False))
        .groupby(["bucket", "has_multi_syn"])
        .agg(n_grains=("research_id", "count"), duplicate_excess_rows=("excess_rows", "sum"))
        .reset_index()
    )
    lineage = pd.DataFrame([table_counts(con, table_fq) for table_fq in LINEAGE_TABLES])
    impact = build_downstream_impact(con, event_df, rows_per_grain)

    lineage.to_csv(OUT_DIR / "lineage_duplicate_counts.csv", index=False)
    impact.to_csv(OUT_DIR / "downstream_impact_metrics.csv", index=False)
    write_report(event_df, rows_per_grain, classification, rows_per_summary, class_summary, syn_summary, lineage, impact)
    write_skeleton_sql(len(event_df), len(rows_per_grain), len(event_df) - len(rows_per_grain))

    manifest = {
        "run_id": RUN_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "posture": "read_only_scoping_no_motherduck_writes",
        "target_table": "main.canonical_path_malignant_events_v1",
        "row_counts": {
            "total_rows": len(event_df),
            "distinct_grains": len(rows_per_grain),
            "duplicate_excess_rows": len(event_df) - len(rows_per_grain),
            "duplicate_grains": int((rows_per_grain["n_rows"] > 1).sum()),
        },
        "outputs": [
            str(REPORT_PATH.relative_to(REPO_ROOT)),
            str(SQL_PATH.relative_to(REPO_ROOT)),
            str((OUT_DIR / "duplicate_examples.csv").relative_to(REPO_ROOT)),
            str((OUT_DIR / "duplicate_grain_classification.csv").relative_to(REPO_ROOT)),
            str((OUT_DIR / "lineage_duplicate_counts.csv").relative_to(REPO_ROOT)),
            str((OUT_DIR / "downstream_impact_metrics.csv").relative_to(REPO_ROOT)),
        ],
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
