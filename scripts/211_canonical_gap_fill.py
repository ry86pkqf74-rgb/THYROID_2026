#!/usr/bin/env python3
"""
THYROID_2026 — Script 211: Canonical Gap-Fill from Extracted + Episode Tables
Database: thyroid_ete_fix_20260413

Integrates validated data from 8 extracted/episode tables into
canonical_patient_master_v1 as new columns. No existing columns modified.

Sources:
  1. complication_phenotype_v1     → per-entity detail (~72 cols, 2,892 pts)
  2. extracted_rln_injury_refined_v2 → injury type/laterality/tier (9 cols, 92 pts)
  3. extracted_ete_subgraded_v1    → refined ETE subgrading (7 cols, 3,558 pts)
  4. extracted_postop_labs_expanded_v1 → calcium/PTH detail (10 cols, ~1,051 pts)
  5. rai_treatment_episode_v2      → cumulative dose/episode detail (11 cols, 862 pts)
  6. recurrence_event_clean_v1     → structural flag/event rank (4 cols, 1,946 pts)
  7. survival_cohort_enriched      → Tg log slope/risk band (5 cols, 10,507 pts)
  8. molecular_variant_long        → variant counts/rare genes (11 cols, 703 pts)

Skipped (fully absorbed): extracted_braf_recovery_v1, extracted_ras_patient_summary_v1

Run:
  .venv/bin/python scripts/211_canonical_gap_fill.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"

COMPLICATION_ENTITIES = [
    "hypocalcemia",
    "hypoparathyroidism",
    "rln_injury",
    "vocal_cord_paralysis",
    "vocal_cord_paresis",
    "hematoma",
    "seroma",
    "chyle_leak",
    "wound_infection",
]


def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


def build_complication_rollup_sql() -> str:
    """Per-entity complication pivot to patient level."""
    parts = []
    for ent in COMPLICATION_ENTITIES:
        short = ent.replace("vocal_cord_", "vc_")
        parts.extend([
            f"MAX(CASE WHEN complication_entity='{ent}' THEN confirmed_flag END) AS comp_{short}_confirmed",
            f"MAX(CASE WHEN complication_entity='{ent}' THEN suspected_flag END) AS comp_{short}_suspected",
            f"MAX(CASE WHEN complication_entity='{ent}' THEN treatment_requiring_flag END) AS comp_{short}_treatment_req",
            f"MIN(CASE WHEN complication_entity='{ent}' AND timing_days_post_surgery IS NOT NULL THEN timing_days_post_surgery END) AS comp_{short}_days_postop",
            f"MIN(CASE WHEN complication_entity='{ent}' THEN timing_window END) AS comp_{short}_timing_window",
            f"MIN(CASE WHEN complication_entity='{ent}' THEN evidence_tier END) AS comp_{short}_evidence_tier",
            f"MAX(CASE WHEN complication_entity='{ent}' THEN transient_flag END) AS comp_{short}_transient",
            f"MAX(CASE WHEN complication_entity='{ent}' THEN permanent_flag END) AS comp_{short}_permanent",
        ])
    parts.append("MAX(voice_permanence_noted) AS comp_voice_permanence_noted")
    parts.append("MAX(voice_resolution_noted) AS comp_voice_resolution_noted")
    cols = ",\n    ".join(parts)
    return f"""
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    {cols}
FROM complication_phenotype_v1
GROUP BY CAST(research_id AS BIGINT)
"""


RLN_SQL = """
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    injury_type AS rln_injury_type,
    laterality AS rln_laterality,
    rln_injury_tier,
    classification AS rln_classification,
    temporality AS rln_temporality,
    rln_injury_is_confirmed,
    rln_injury_evidence_strength AS rln_injury_evidence,
    CAST(detection_date AS DATE) AS rln_injury_detection_date,
    CAST(days_post_surgery AS INTEGER) AS rln_injury_days_postop
FROM extracted_rln_injury_refined_v2
"""

ETE_SQL = """
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    refined_ete_grade AS ete_refined_grade,
    subgrade_method AS ete_subgrade_method,
    original_grade AS ete_original_grade,
    original_source AS ete_original_source,
    op_note_grade AS ete_op_note_grade,
    CAST(op_note_confidence AS VARCHAR) AS ete_op_note_confidence,
    grading_source_note AS ete_subgrade_note
FROM extracted_ete_subgraded_v1
"""


def build_postop_labs_sql() -> str:
    return """
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    MIN(CASE WHEN lab_type='pth' THEN value END) AS postop_pth_min_value,
    MIN(CASE WHEN lab_type='pth' AND value = (
        SELECT MIN(value) FROM extracted_postop_labs_expanded_v1 e2
        WHERE e2.research_id = e1.research_id AND e2.lab_type='pth'
    ) THEN CAST(days_postop AS INTEGER) END) AS postop_pth_min_days_postop,
    COUNT(*) FILTER (WHERE lab_type='pth') AS postop_pth_n_measurements,
    MAX(CASE WHEN lab_type='pth' THEN CAST(source_reliability AS VARCHAR) END) AS postop_pth_source_reliability,
    MIN(CASE WHEN lab_type='total_calcium' THEN value END) AS postop_calcium_min_value,
    MIN(CASE WHEN lab_type='total_calcium' AND value = (
        SELECT MIN(value) FROM extracted_postop_labs_expanded_v1 e2
        WHERE e2.research_id = e1.research_id AND e2.lab_type='total_calcium'
    ) THEN CAST(days_postop AS INTEGER) END) AS postop_calcium_min_days_postop,
    COUNT(*) FILTER (WHERE lab_type='total_calcium') AS postop_calcium_n_measurements,
    MAX(CASE WHEN lab_type='total_calcium' THEN CAST(source_reliability AS VARCHAR) END) AS postop_calcium_source_reliability,
    MIN(CASE WHEN lab_type='ionized_calcium' THEN value END) AS postop_ionized_cal_min_value,
    TRUE AS postop_labs_has_data
FROM extracted_postop_labs_expanded_v1 e1
GROUP BY CAST(research_id AS BIGINT)
"""


RAI_SQL = """
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    SUM(dose_mci) AS rai_total_cumulative_dose_mci,
    MIN(dose_mci) AS rai_min_dose_mci,
    COUNT(*) FILTER (WHERE dose_mci IS NOT NULL) AS rai_n_episodes_with_dose,
    MIN(CAST(dose_confidence AS VARCHAR)) AS rai_dose_confidence_worst,
    BOOL_OR(completion_status IS NOT NULL) AS rai_has_completion_status,
    BOOL_OR(adjudication_status IS NOT NULL) AS rai_has_adjudication,
    STRING_AGG(DISTINCT rai_intent, ', ' ORDER BY rai_intent) AS rai_intent_list,
    COUNT(DISTINCT rai_intent) AS rai_n_distinct_intents,
    CAST(MIN(resolved_rai_date) AS DATE) AS rai_first_episode_date,
    CAST(MAX(resolved_rai_date) AS DATE) AS rai_last_episode_date,
    DATE_DIFF('day', MIN(resolved_rai_date), MAX(resolved_rai_date)) AS rai_episode_date_span_days
FROM rai_treatment_episode_v2
GROUP BY CAST(research_id AS BIGINT)
"""

RECURRENCE_SQL = """
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    MAX(structural_recurrence_flag) AS rec_structural_flag,
    MIN(CAST(event_rank AS INTEGER)) AS rec_event_rank,
    MIN(CAST(source_priority AS INTEGER)) AS rec_source_priority,
    MIN(source_table) AS rec_source_table
FROM recurrence_event_clean_v1
GROUP BY CAST(research_id AS BIGINT)
"""

SURVIVAL_SQL = """
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    MAX(tg_annual_log_slope) AS surv_tg_annual_log_slope,
    MIN(recurrence_risk_band) AS surv_recurrence_risk_band,
    COUNT(*) FILTER (WHERE event IS TRUE) AS surv_n_events,
    MAX(time_days) AS surv_max_time_days,
    MAX(time_days_capped) AS surv_max_time_days_capped
FROM survival_cohort_enriched
GROUP BY CAST(research_id AS BIGINT)
"""

MOLECULAR_SQL = """
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    COUNT(*) AS mol_n_variants_total,
    COUNT(DISTINCT gene_symbol) AS mol_n_distinct_genes,
    STRING_AGG(DISTINCT gene_symbol, ', ' ORDER BY gene_symbol) AS mol_genes_list,
    BOOL_OR(variant_class = 'FUSION') AS mol_has_fusion,
    COUNT(*) FILTER (WHERE variant_class = 'FUSION') AS mol_n_fusions,
    BOOL_OR(variant_class = 'SNV') AS mol_has_snv,
    COUNT(*) FILTER (WHERE variant_class = 'SNV') AS mol_n_snvs,
    BOOL_OR(gene_symbol = 'DICER1') AS mol_has_dicer1,
    BOOL_OR(gene_symbol = 'PIK3CA') AS mol_has_pik3ca,
    BOOL_OR(gene_symbol = 'TSHR') AS mol_has_tshr,
    STRING_AGG(DISTINCT variant_class, ', ' ORDER BY variant_class) AS mol_variant_classes
FROM molecular_variant_long
GROUP BY CAST(research_id AS BIGINT)
"""


def main():
    parser = argparse.ArgumentParser(description="Script 211: Canonical gap-fill")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL but don't execute")
    args = parser.parse_args()

    con = connect()
    print(f"[211] Connected to {DB}")

    # Step 1: Get current canonical column count
    cur_cols = con.execute(f"""
        SELECT COUNT(DISTINCT column_name)
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    print(f"[211] Current canonical columns: {cur_cols}")

    cur_rows = con.execute(f"SELECT COUNT(*) FROM {CANONICAL}").fetchone()[0]
    print(f"[211] Current canonical rows: {cur_rows}")

    # Step 2: Build all source CTEs
    comp_sql = build_complication_rollup_sql()
    postop_sql = build_postop_labs_sql()

    source_ctes = {
        "comp": comp_sql,
        "rln": RLN_SQL,
        "ete": ETE_SQL,
        "postop": postop_sql,
        "rai": RAI_SQL,
        "rec": RECURRENCE_SQL,
        "surv": SURVIVAL_SQL,
        "mol": MOLECULAR_SQL,
    }

    # Step 3: Build the mega-join SQL
    cte_defs = []
    for name, sql in source_ctes.items():
        cte_defs.append(f"{name}_cte AS ({sql})")

    # Collect all non-research_id columns from each CTE
    cte_col_refs = []
    for name in source_ctes:
        cte_col_refs.append(f"{name}_cte.*  EXCLUDE (research_id)")

    join_clauses = []
    for name in source_ctes:
        join_clauses.append(
            f"LEFT JOIN {name}_cte ON c.research_id = {name}_cte.research_id"
        )

    rebuild_sql = f"""
WITH
    {",".join(cte_defs)}

SELECT
    c.*,
    {",".join(cte_col_refs)}
FROM {CANONICAL} c
{chr(10).join(join_clauses)}
"""

    if args.dry_run:
        print("\n[211] DRY RUN — would execute:\n")
        print(rebuild_sql[:3000])
        print("...[truncated]...")
        return

    # Step 4: Execute rebuild
    t0 = time.time()
    print("[211] Rebuilding canonical with gap-fill columns...")

    # Use staging table approach to avoid lock conflicts
    staging = f"{CANONICAL}_staging_211"
    con.execute(f"DROP TABLE IF EXISTS {staging}")
    con.execute(f"CREATE TABLE {staging} AS {rebuild_sql}")

    # Validate staging
    stg_rows = con.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
    stg_rids = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {staging}").fetchone()[0]
    stg_null_rids = con.execute(
        f"SELECT COUNT(*) FROM {staging} WHERE research_id IS NULL"
    ).fetchone()[0]
    stg_dupes = stg_rows - stg_rids

    print(f"[211] Staging: {stg_rows} rows, {stg_rids} distinct RIDs, "
          f"{stg_null_rids} null RIDs, {stg_dupes} dupes")

    if stg_rows != cur_rows:
        print(f"[211] ERROR: Row count changed from {cur_rows} to {stg_rows}. Aborting.")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        sys.exit(1)

    if stg_dupes > 0:
        print(f"[211] ERROR: {stg_dupes} duplicate research_ids. Aborting.")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        sys.exit(1)

    if stg_null_rids > 0:
        print(f"[211] ERROR: {stg_null_rids} null research_ids. Aborting.")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        sys.exit(1)

    # Check fna_path_outcome preservation
    fna_check = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) FROM {staging}
    """).fetchone()[0]
    print(f"[211] fna_path_outcome nulls in staging: {fna_check}")

    # Swap
    con.execute(f"DROP TABLE IF EXISTS {CANONICAL}")
    con.execute(f"ALTER TABLE {staging} RENAME TO {CANONICAL}")

    elapsed = time.time() - t0
    print(f"[211] Rebuild complete in {elapsed:.1f}s")

    # Step 5: Final validation
    new_cols = con.execute(f"""
        SELECT COUNT(DISTINCT column_name)
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    added = new_cols - cur_cols
    print(f"[211] Columns: {cur_cols} → {new_cols} (+{added})")

    # Coverage report
    print("\n[211] === Coverage Report ===")
    coverage_sql = f"""
    SELECT
        COUNT(*) FILTER (WHERE comp_hypocalcemia_confirmed IS NOT NULL
                           OR comp_hypoparathyroidism_confirmed IS NOT NULL
                           OR comp_rln_injury_confirmed IS NOT NULL) AS comp_patients,
        COUNT(*) FILTER (WHERE rln_injury_type IS NOT NULL) AS rln_patients,
        COUNT(*) FILTER (WHERE ete_refined_grade IS NOT NULL) AS ete_patients,
        COUNT(*) FILTER (WHERE postop_labs_has_data IS TRUE) AS postop_patients,
        COUNT(*) FILTER (WHERE rai_total_cumulative_dose_mci IS NOT NULL
                           OR rai_n_episodes_with_dose IS NOT NULL) AS rai_patients,
        COUNT(*) FILTER (WHERE rec_structural_flag IS NOT NULL) AS rec_patients,
        COUNT(*) FILTER (WHERE surv_tg_annual_log_slope IS NOT NULL
                           OR surv_n_events IS NOT NULL) AS surv_patients,
        COUNT(*) FILTER (WHERE mol_n_variants_total IS NOT NULL) AS mol_patients
    FROM {CANONICAL}
    """
    cov = con.execute(coverage_sql).fetchone()
    labels = ["Complication", "RLN", "ETE", "PostopLabs", "RAI", "Recurrence", "Survival", "Molecular"]
    for lbl, val in zip(labels, cov):
        print(f"  {lbl:15s}: {val:,} patients with data")

    # Final invariant check
    final = con.execute(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT research_id) as distinct_rids,
            COUNT(*) FILTER (WHERE research_id IS NULL) as null_rids,
            COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) as null_fna
        FROM {CANONICAL}
    """).fetchone()
    print(f"\n[211] Final: {final[0]} rows, {final[1]} distinct RIDs, "
          f"{final[2]} null RIDs, {final[3]} null fna_path_outcome")

    if final[0] == 10871 and final[1] == 10871 and final[2] == 0:
        print("[211] ✓ All invariants PASS")
    else:
        print("[211] ✗ INVARIANT FAILURE — investigate immediately")
        sys.exit(1)

    print("[211] Done.")


if __name__ == "__main__":
    main()
