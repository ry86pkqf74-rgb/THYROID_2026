#!/usr/bin/env python3
"""
THYROID_2026 — Script 207: Full Canonical Master Expansion
Database: thyroid_ete_fix_20260413
Goal: Expand canonical_patient_master_v1 from 125 → ~260 columns
      by integrating ALL validated source data.

DESIGN RULES
  1. ONE row per patient. Always 10,871 rows. No exceptions.
  2. Every column traces to a specific source table.
  3. Highest version wins for versioned fields; _source suffix for provenance.
  4. patient_refined_master_clinical_v12 deduped with ROW_NUMBER() PARTITION BY
     research_id ORDER BY refined_at DESC NULLS LAST.
  5. All existing 125 column names are preserved unchanged.

Column sources:
  - gold_master_patient_facts_v1          (spine + group A/I/J/K/O/P)
  - canonical_diagnosis_unified_v1        (group: diagnosis)
  - canonical_recurrence_v1              (group: recurrence)
  - canonical_survival_followup_v1       (group: survival)
  - canonical_molecular_tested_v1        (group: molecular base)
  - patient_refined_master_clinical_v12  (groups B/E/F/G/H/L/M/N)
  - thyroid_scoring_py_v1               (group O2: scoring detail)
  - ct_imaging                           (group C: CT rollup)
  - nuclear_med                          (group D: nuclear med rollup)
  - complication_patient_summary_v1      (group K2: complication detail)
  - imaging_patient_summary_v1          (group B2: imaging summary)
  - tg_timeline_patient_summary_v1      (group I2: Tg timeline)
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DB = "thyroid_ete_fix_20260413"
TABLE = "canonical_patient_master_v1"


def connect():
    token = get_token()
    if not token:
        raise RuntimeError("MotherDuck token not found")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


# ---------------------------------------------------------------------------
# Phase 1: Audit queries
# ---------------------------------------------------------------------------
AUDIT_SQL = """
-- 1A: canonical vs gold_master cross-check
SELECT
    COUNT(*) AS total_joined,
    SUM(CASE WHEN CAST(c.ln_total_examined AS VARCHAR) != CAST(g.path_ln_examined_raw AS VARCHAR) THEN 1 ELSE 0 END) AS ln_mismatch,
    SUM(CASE WHEN CAST(c.ete_grade AS VARCHAR)         != CAST(g.ete_grade_final AS VARCHAR)      THEN 1 ELSE 0 END) AS ete_mismatch,
    SUM(CASE WHEN CAST(c.bethesda_final AS VARCHAR)    != CAST(g.fna_bethesda_final AS VARCHAR)   THEN 1 ELSE 0 END) AS bethesda_mismatch,
    SUM(CASE WHEN CAST(c.preop_tirads_best AS VARCHAR) != CAST(g.imaging_tirads_best AS VARCHAR)  THEN 1 ELSE 0 END) AS tirads_mismatch,
    SUM(CASE WHEN CAST(c.braf_positive AS VARCHAR)     != CAST(g.braf_positive_final AS VARCHAR)  THEN 1 ELSE 0 END) AS braf_mismatch
FROM {table} c
JOIN gold_master_patient_facts_v1 g
    ON c.research_id = CAST(g.research_id AS VARCHAR)
"""

TIRADS_AUDIT_SQL = """
-- 1B: TIRADS coverage cross-reference
SELECT
    COUNT(DISTINCT CAST(e.research_id AS VARCHAR))                                       AS etv_patients,
    COUNT(DISTINCT CASE WHEN i.research_id IS NULL
                        THEN CAST(e.research_id AS VARCHAR) END)                          AS etv_only_patients,
    COUNT(DISTINCT CASE WHEN i.tirads_acr_recalculated IS NULL
                        THEN CAST(e.research_id AS VARCHAR) END)                          AS etv_fills_gap
FROM extracted_tirads_validated_v1 e
LEFT JOIN (
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id, tirads_acr_recalculated
    FROM imaging_nodule_master_v1
    WHERE tirads_acr_recalculated IS NOT NULL
) i ON CAST(e.research_id AS VARCHAR) = i.research_id
"""

PRM_TIRADS_AUDIT_SQL = """
-- 1C: PRM v12 TIRADS coverage
SELECT
    COUNT(*)                                                                AS total_dedup,
    COUNT(CASE WHEN tirads_best_score_v12 IS NOT NULL THEN 1 END)         AS has_v12_tirads,
    COUNT(CASE WHEN tirads_reliability_v12 IS NOT NULL THEN 1 END)        AS has_reliability,
    COUNT(CASE WHEN tirads_source_v12 IS NOT NULL THEN 1 END)             AS has_source,
    COUNT(CASE WHEN tirads_worst_score_v12 IS NOT NULL THEN 1 END)        AS has_worst
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY refined_at DESC NULLS LAST
        ) AS rn
    FROM patient_refined_master_clinical_v12
) WHERE rn = 1
"""

# ---------------------------------------------------------------------------
# Main expansion SQL  — starts from old canonical (preserves all 125 cols)
#                       then joins new sources for ~135 additional columns
# ---------------------------------------------------------------------------
EXPANSION_SQL = """
WITH

-- ── Old canonical (all 125 existing columns, exact values preserved) ──────
old_canon AS (
    SELECT * FROM canonical_patient_master_v1
),

-- ── Gold master (for new columns not in old_canon) ────────────────────────
gm AS (
    SELECT * FROM gold_master_patient_facts_v1
),

-- ── PRM v12 deduplicated (10 patients have 27-64 duplicate rows) ─────────
prm AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY refined_at DESC NULLS LAST
            ) AS rn
        FROM patient_refined_master_clinical_v12
    ) WHERE rn = 1
),

-- ── Thyroid scoring (1:1, 10,871 rows) ───────────────────────────────────
scoring AS (
    SELECT * FROM thyroid_scoring_py_v1
),

-- ── CT imaging rollup (patient-level — worst findings) ───────────────────
ct AS (
    SELECT
        CAST(research_id AS VARCHAR)            AS research_id,
        COUNT(*)                                AS ct_n_exams,
        BOOL_OR(goiter_present)                 AS ct_goiter_present_any,
        BOOL_OR(substernal_extension)           AS ct_substernal_extension_any,
        BOOL_OR(CASE
            WHEN tracheal_deviation IS NOT NULL
             AND LOWER(TRIM(tracheal_deviation)) NOT IN ('none','no','false','')
            THEN TRUE ELSE FALSE END)            AS ct_tracheal_deviation_any,
        BOOL_OR(CASE
            WHEN tracheal_narrowing IS NOT NULL
             AND LOWER(TRIM(tracheal_narrowing)) NOT IN ('none','no','false','')
            THEN TRUE ELSE FALSE END)            AS ct_tracheal_narrowing_any,
        BOOL_OR(lymph_nodes_enlarged)           AS ct_ln_enlarged_any,
        BOOL_OR(lymph_nodes_suspicious)         AS ct_ln_suspicious_any,
        MAX(largest_lymph_node_short_axis_mm)   AS ct_largest_ln_short_axis_mm
    FROM ct_imaging
    GROUP BY research_id
),

-- ── Nuclear medicine rollup (patient-level) ──────────────────────────────
nm AS (
    SELECT
        CAST(research_id AS VARCHAR)            AS research_id,
        COUNT(*)                                AS nucmed_n_scans,
        BOOL_OR(
            LOWER(COALESCE(scantype,'')) LIKE '%rai%'
            OR LOWER(COALESCE(scantype,'')) LIKE '%iodine%'
            OR LOWER(COALESCE(scantype,'')) LIKE '%i-131%'
            OR LOWER(COALESCE(scantype,'')) LIKE '%i131%'
        )                                       AS nucmed_has_rai_scan,
        MAX(TRY_CAST(uptake_24hour AS DOUBLE))  AS nucmed_uptake_24hr_max,
        STRING_AGG(DISTINCT scantype, '; '
            ORDER BY scantype)                  AS nucmed_scan_types
    FROM nuclear_med
    GROUP BY research_id
),

-- ── Complication patient summary (dedup by latest) ────────────────────────
comp AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY summarized_at DESC NULLS LAST
            ) AS rn
        FROM complication_patient_summary_v1
    ) WHERE rn = 1
),

-- ── Imaging patient summary (1:1) ─────────────────────────────────────────
ips AS (
    SELECT * FROM imaging_patient_summary_v1
),

-- ── Tg timeline patient summary (1:1) ────────────────────────────────────
tg AS (
    SELECT * FROM tg_timeline_patient_summary_v1
)

SELECT
    -- ===================================================================
    -- BLOCK 0: ALL EXISTING 125 COLUMNS (verbatim from old canonical)
    -- ===================================================================
    oc.*,

    -- ===================================================================
    -- BLOCK A: DEMOGRAPHICS PROVENANCE (from gold_master)
    -- ===================================================================
    gm.demo_source,
    gm.demo_confidence,

    -- ===================================================================
    -- BLOCK B: PATHOLOGY RAW DETAIL (from gold_master)
    -- ===================================================================
    gm.path_histology_raw,
    gm.histology_final,
    gm.histology_source,
    gm.path_histology_variant_raw,
    gm.path_t_stage_raw,
    gm.path_n_stage_raw,
    gm.path_gross_ete_flag,
    gm.path_margin_raw,
    gm.ete_grade_source,

    -- ===================================================================
    -- BLOCK C: FNA DETAIL — BETHESDA PROVENANCE (from gold_master)
    -- ===================================================================
    gm.fna_bethesda_confidence,
    gm.fna_bethesda_source,

    -- ===================================================================
    -- BLOCK D: IMAGING TIRADS PROVENANCE (from gold_master)
    -- ===================================================================
    gm.imaging_tirads_source,
    gm.imaging_n_nodule_records,

    -- ===================================================================
    -- BLOCK E: TIRADS v12 — HIGHEST RESOLUTION (from PRM v12)
    -- ===================================================================
    prm.tirads_best_score_v12,
    prm.tirads_worst_score_v12,
    prm.tirads_best_category_v12,
    prm.tirads_worst_category_v12,
    prm.tirads_source_v12,
    prm.tirads_reliability_v12,
    prm.tirads_n_sources_v12,
    prm.tirads_n_nodule_records_v12,
    prm.tirads_concordant_count_v12,
    prm.tirads_mismatch_count_v12,
    prm.tirads_has_acr_recalc_v12,
    prm.tirads_nodule_size_max_mm_v12,
    prm.imaging_nodule_size_cm_v11,

    -- ===================================================================
    -- BLOCK F: IMAGING PATIENT SUMMARY (multi-exam context)
    -- ===================================================================
    CAST(ips.n_exams AS BIGINT)                 AS n_us_exams,
    CAST(ips.n_total_nodules AS BIGINT)         AS n_us_nodules_total,
    ips.bilateral_disease_flag,
    ips.first_exam_date                         AS us_first_exam_date,
    ips.last_exam_date                          AS us_last_exam_date,
    ips.dominant_nodule_size_cm,
    ips.has_suspicious_candidate,
    ips.longitudinal_assessment_available,
    ips.max_tirads_ever,
    ips.worst_tirads_category,

    -- ===================================================================
    -- BLOCK G: CT IMAGING ROLLUP
    -- ===================================================================
    ct.ct_n_exams,
    ct.ct_goiter_present_any,
    ct.ct_substernal_extension_any,
    ct.ct_tracheal_deviation_any,
    ct.ct_tracheal_narrowing_any,
    ct.ct_ln_enlarged_any,
    ct.ct_ln_suspicious_any,
    ct.ct_largest_ln_short_axis_mm,

    -- ===================================================================
    -- BLOCK H: NUCLEAR MEDICINE ROLLUP
    -- ===================================================================
    nm.nucmed_n_scans,
    nm.nucmed_has_rai_scan,
    nm.nucmed_uptake_24hr_max,
    nm.nucmed_scan_types,

    -- ===================================================================
    -- BLOCK I: FNA EXPANDED (from PRM v12)
    -- ===================================================================
    prm.n_fna_episodes,
    prm.cross_fna_concordance,
    prm.fna_confidence,
    prm.worst_bethesda_num,
    prm.bethesda_final_name,

    -- ===================================================================
    -- BLOCK J: ENE MULTI-SOURCE (from PRM v12)
    -- ===================================================================
    prm.ene_positive,
    prm.best_ene_grade,
    prm.ene_grade_v9,
    prm.ene_levels_v9,
    prm.ene_deposit_cm,
    prm.ene_path_synoptic,
    prm.ene_path_nlp,
    prm.ene_path_levels,
    prm.ene_op_intraop,
    prm.ene_ct,
    prm.ene_us,
    prm.ene_pet,
    prm.ene_rai_scan,
    prm.ene_n_sources,
    prm.ene_path_ct_concordance,
    prm.ene_record_count_v9,

    -- ===================================================================
    -- BLOCK K: MOLECULAR PROVENANCE (from gold_master)
    -- ===================================================================
    gm.braf_positive_final,
    gm.braf_detection_method,
    gm.braf_source,
    gm.mol_n_tests,
    gm.mol_test_date,

    -- ===================================================================
    -- BLOCK L: MOLECULAR EXPANDED v7/v11/v13 (from PRM v12)
    -- ===================================================================
    prm.molecular_tested_v7,
    prm.high_risk_molecular_v7,
    prm.n_molecular_tests_v7,
    prm.molecular_platforms_v7,
    prm.alk_positive_v7,
    prm.ret_positive_v7,
    prm.ntrk_positive_v7,
    prm.tp53_positive_v7,
    prm.eif1ax_positive,
    prm.pax8_pparg_positive,
    prm.any_fusion_positive,
    prm.braf_positive_v7,
    prm.braf_status_v7,
    prm.tert_positive_v7,
    prm.tert_status_v7,
    prm.ras_positive_v7,
    prm.ras_positive_v11,
    prm.nras_positive_v11,
    prm.hras_positive_v11,
    prm.kras_positive_v11,
    prm.ras_primary_subtype_v11,
    prm.ras_protein_change_v11,
    prm.ras_allele_freq_v11,
    prm.braf_recovered_status_v11,
    prm.braf_recovered_variant_v11,
    prm.braf_detection_method_v11,
    prm.ihc_braf_result_v13,
    prm.ihc_braf_note_type_v13,
    prm.ihc_braf_confidence_v13,
    prm.ras_resolved_gene_v13,
    prm.ras_resolved_variant_v13,
    prm.ras_resolved_af_v13,
    prm.ras_resolution_source_v13,
    prm.ras_resolution_confidence_v13,
    prm.tert_variant_v9,
    prm.tert_platforms_v9,
    prm.tert_test_count_v9,
    prm.tert_tested,
    prm.preop_sweep_genes_found_v11,

    -- ===================================================================
    -- BLOCK M: RAI EXPANDED (from PRM v12)
    -- ===================================================================
    prm.confirmed_rai_episodes,
    prm.n_rai_episodes,
    prm.rai_dose_v9,
    prm.rai_intent_v9,
    prm.rai_avidity,
    prm.rai_avid_flag,
    prm.rai_validation_tier,
    prm.rai_dose_source,
    prm.rai_dose_linkage,
    prm.max_stimulated_tg,
    prm.rai_stimulated_tg,
    prm.rai_stimulated_tsh,
    prm.post_rai_tg_nadir,
    prm.post_rai_tg_last,
    prm.post_rai_tg_count,
    prm.rai_scan_findings_v9,

    -- ===================================================================
    -- BLOCK N: LABS EXPANDED (from gold_master + PRM v12 + tg_timeline)
    -- ===================================================================
    gm.anti_tg_nadir,
    gm.anti_tg_rising_flag,
    gm.calcium_nadir,
    gm.calcium_supplement_required,
    prm.pth_nadir,
    prm.pth_nadir_30d,
    prm.pth_nadir_days_postop,
    prm.calcium_nadir_30d,
    prm.calcium_nadir_days_postop,
    tg.n_tg_measurements               AS n_tg_measurements_structured,
    tg.n_tgab_measurements,
    tg.tg_trajectory_class,
    tg.tgab_interference_flag,
    tg.tgab_nadir,
    tg.tgab_last_value,
    tg.tgab_peak,
    tg.tg_mean,
    tg.days_first_to_last_tg,
    tg.first_tg_date,
    tg.last_tg_date,
    tg.tg_last_censored,

    -- ===================================================================
    -- BLOCK O: RECURRENCE FLAGS (from gold_master)
    -- ===================================================================
    gm.any_recurrence_flag,
    gm.biochemical_recurrence_flag,

    -- ===================================================================
    -- BLOCK P: COMPLICATION DETAIL (from complication_patient_summary_v1)
    -- ===================================================================
    gm.any_confirmed_complication,
    comp.any_confirmed_complication_flag,
    comp.n_confirmed_complications,
    comp.has_low_pth_flag,
    comp.has_low_calcium_flag,
    comp.earliest_complication_days,
    comp.any_analysis_eligible_complication,

    -- ===================================================================
    -- BLOCK Q: PATHOLOGY INVASION/MARGIN DETAIL (from PRM v12)
    -- ===================================================================
    prm.capsular_invasion_refined,
    prm.capsular_invasion_v6,
    prm.vascular_who_2022_grade,
    prm.vasc_grade_final_v13,
    prm.vasc_vessel_count_v13,
    prm.vasc_source_final_v13,
    prm.vasc_confidence_final_v13,
    prm.lvi_grade_final_v13,
    prm.pni_positive,
    prm.pni_refined_v6,
    prm.margin_r_classification,
    prm.margin_r_class_v10,
    prm.n_tumors_v10,
    prm.max_tumor_size_cm_v10,
    prm.worst_ete_v10,
    prm.total_ln_positive_v10,

    -- ===================================================================
    -- BLOCK R: SURGERY EXPANDED (from gold_master)
    -- ===================================================================
    gm.op_strap_muscle_any,
    gm.op_reoperative_any,
    gm.op_parathyroid_autograft_any,
    gm.op_local_invasion_any,
    gm.op_tracheal_inv_any,
    gm.op_esophageal_inv_any,
    gm.op_intraop_gross_ete_any,
    gm.op_n_surgeries_with_findings,
    gm.op_findings_summary,

    -- ===================================================================
    -- BLOCK S: VOICE OUTCOMES (from PRM v12)
    -- ===================================================================
    prm.voice_outcome_category,
    prm.has_voice_data,
    prm.voice_followup_completeness,
    prm.voice_data_confidence,
    prm.days_to_first_laryngoscopy,
    prm.days_to_last_laryngoscopy,

    -- ===================================================================
    -- BLOCK T: LATERAL NECK DETAIL (from PRM v12 Phase 10)
    -- ===================================================================
    prm.lateral_neck_dissected_v10,
    prm.lateral_detection_method,
    prm.lateral_levels_v10,
    prm.lateral_side_v10,
    prm.lateral_source_v10,

    -- ===================================================================
    -- BLOCK U: COMPLETION THYROIDECTOMY (from PRM v12 Phase 8)
    -- ===================================================================
    prm.completion_reason,
    prm.completion_reason_confidence,
    prm.completion_histology_type,
    prm.completion_t_stage,
    prm.completion_prior_histology,
    prm.completion_braf_positive,
    prm.completion_tert_positive,

    -- ===================================================================
    -- BLOCK V: SCORING DETAIL (from thyroid_scoring_py_v1)
    -- ===================================================================
    scoring.gross_ete_flag,
    scoring.distant_mets_proxy,
    scoring.aggressive_variant_flag,
    scoring.ata_initial_risk,
    scoring.ata_response_is_provisional,
    scoring.ata_risk_calculable_flag,
    scoring.macis_calculable_flag,
    scoring.macis_missing_components,
    scoring.macis_risk_group,
    scoring.ajcc8_stage_calculable_flag,
    scoring.ajcc8_t_stage_calculable_flag,
    scoring.ages_calculable_flag,
    scoring.ames_calculable_flag,
    scoring.ames_risk,
    scoring.molecular_risk_calculable_flag,
    scoring.recurrence_flag                 AS recurrence_flag_scoring,
    scoring.first_recurrence_date,
    scoring.vasc_grade,
    scoring.margin_r_class,
    scoring.bethesda_category,
    scoring.bethesda_confidence,
    scoring.bethesda_num,
    scoring.bethesda_source,

    -- ===================================================================
    -- BLOCK W: ELIGIBILITY FLAGS (from gold_master)
    -- ===================================================================
    gm.analysis_eligible_flag,
    gm.molecular_eligible_flag,
    gm.rai_eligible_flag,
    gm.survival_eligible_flag,
    gm.ajcc8_calculable_flag,
    gm.ajcc8_missing_components,
    gm.ata_calculable_flag,
    gm.ata_response_calculable_flag,
    gm.macis_calculable_flag            AS gm_macis_calculable_flag,

    -- ===================================================================
    -- BLOCK X: PROVENANCE METADATA (from gold_master)
    -- ===================================================================
    gm.date_traceability_status

FROM old_canon oc
LEFT JOIN gm
    ON oc.research_id = CAST(gm.research_id AS VARCHAR)
LEFT JOIN prm
    ON oc.research_id = CAST(prm.research_id AS VARCHAR)
LEFT JOIN scoring
    ON oc.research_id = CAST(scoring.research_id AS VARCHAR)
LEFT JOIN ct
    ON oc.research_id = ct.research_id
LEFT JOIN nm
    ON oc.research_id = nm.research_id
LEFT JOIN comp
    ON oc.research_id = CAST(comp.research_id AS VARCHAR)
LEFT JOIN ips
    ON oc.research_id = CAST(ips.research_id AS VARCHAR)
LEFT JOIN tg
    ON oc.research_id = CAST(tg.research_id AS VARCHAR)
"""


def run_phase1_audit(con: duckdb.DuckDBPyConnection) -> None:
    print("\n" + "=" * 60)
    print("PHASE 1: PRE-BUILD AUDIT")
    print("=" * 60)

    print("\n1A — canonical vs gold_master cross-check:")
    r = con.execute(AUDIT_SQL.format(table=TABLE)).fetchone()
    fields = ["total_joined", "ln_mismatch", "ete_mismatch",
              "bethesda_mismatch", "tirads_mismatch", "braf_mismatch"]
    for k, v in zip(fields, r):
        status = "✓" if (k == "total_joined" or v == 0) else "⚠"
        print(f"  {status} {k}: {v}")

    print("\n1B — TIRADS cross-reference (extracted_tirads vs imaging_nodule_master):")
    r = con.execute(TIRADS_AUDIT_SQL).fetchone()
    print(f"  extracted_tirads_validated_v1 patients: {r[0]}")
    print(f"  ETv-only (not in imaging_nodule_master): {r[1]}")
    print(f"  ETv fills imaging_nodule_master gap:     {r[2]}")

    print("\n1C — PRM v12 TIRADS coverage:")
    r = con.execute(PRM_TIRADS_AUDIT_SQL).fetchone()
    print(f"  Dedup patients: {r[0]}, TIRADS v12: {r[1]}, "
          f"reliability: {r[2]}, source: {r[3]}, worst: {r[4]}")


def run_build(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("PHASE 2: BUILDING EXPANDED CANONICAL MASTER")
    print("=" * 60)

    con.execute(f"CREATE OR REPLACE TABLE {TABLE} AS {EXPANSION_SQL}")
    df = con.execute(f"SELECT * FROM {TABLE}").fetchdf()
    print(f"  Rows:    {len(df)}")
    print(f"  Patients:{df['research_id'].nunique()}")
    print(f"  Columns: {len(df.columns)}")
    return df


def run_validation(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("PHASE 3: VALIDATION")
    print("=" * 60)

    # 1. Row count
    assert len(df) == 10871, f"FAIL row count: expected 10,871, got {len(df)}"
    print(f"  ✓ Row count: {len(df)}")

    # 2. No duplicate research_ids
    dups = df["research_id"].duplicated().sum()
    assert dups == 0, f"FAIL: {dups} duplicate research_ids"
    print("  ✓ No duplicate research_ids")

    # 3. No NULL research_ids
    nulls = df["research_id"].isna().sum()
    assert nulls == 0, f"FAIL: {nulls} NULL research_ids"
    print("  ✓ No NULL research_ids")

    # 4. fna_path_outcome (must stay non-NULL)
    fpo_null = df["fna_path_outcome"].isna().sum()
    print(f"  {'✓' if fpo_null == 0 else '⚠'} fna_path_outcome NULL: {fpo_null}")

    # 5. Cross-check: TIRADS v12 vs preop_tirads_best (where both exist)
    both = df[df["tirads_best_score_v12"].notna() & df["preop_tirads_best"].notna()].copy()
    mismatch = (both["tirads_best_score_v12"] != both["preop_tirads_best"]).sum()
    print(f"  TIRADS v12 vs preop_tirads_best overlap: {len(both)} patients, "
          f"mismatch: {mismatch} (expected: systematic ACR delta is OK)")

    # 6. Cross-check: ln_total_examined vs tp_ln_examined (gold_master mirrors)
    print(f"  LN examined coverage: ln_total_examined={df['ln_total_examined'].notna().sum()}, "
          f"tp_ln_examined={df['tp_ln_examined'].notna().sum()}")

    # 7. Cross-check MotherDuck vs pandas
    md_count = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    assert md_count == len(df), f"FAIL: MD={md_count} vs pandas={len(df)}"
    print(f"  ✓ MotherDuck row count matches: {md_count}")

    # 8. Full coverage report
    print("\n  === COLUMN COVERAGE REPORT ===")
    print(f"  {'column':<50} {'non_null':>10} {'pct':>7}  source")
    print(f"  {'-'*50} {'-'*10} {'-'*7}  {'------'}")

    source_map = {
        # gold_master
        "age_at_surgery": "gold_master", "sex": "gold_master", "race": "gold_master",
        "demo_source": "gold_master", "demo_confidence": "gold_master",
        "path_histology_raw": "gold_master", "histology_final": "gold_master",
        "path_t_stage_raw": "gold_master", "path_n_stage_raw": "gold_master",
        "ete_grade": "gold_master", "ete_grade_source": "gold_master",
        "path_gross_ete_flag": "gold_master", "path_margin_raw": "gold_master",
        "ln_total_examined": "gold_master", "ln_total_positive": "gold_master",
        "braf_positive_final": "gold_master", "braf_detection_method": "gold_master",
        "anti_tg_nadir": "gold_master", "calcium_nadir": "gold_master",
        "analysis_eligible_flag": "gold_master", "molecular_eligible_flag": "gold_master",
        "rai_eligible_flag": "gold_master", "survival_eligible_flag": "gold_master",
        "date_traceability_status": "gold_master",
        "op_strap_muscle_any": "gold_master", "op_reoperative_any": "gold_master",
        "op_parathyroid_autograft_any": "gold_master",
        "any_confirmed_complication": "gold_master",
        "any_recurrence_flag": "gold_master", "biochemical_recurrence_flag": "gold_master",
        # prm v12
        "tirads_best_score_v12": "prm_v12", "tirads_worst_score_v12": "prm_v12",
        "tirads_reliability_v12": "prm_v12", "tirads_source_v12": "prm_v12",
        "ene_positive": "prm_v12", "best_ene_grade": "prm_v12",
        "ene_ct": "prm_v12", "ene_us": "prm_v12", "ene_pet": "prm_v12",
        "molecular_tested_v7": "prm_v12", "high_risk_molecular_v7": "prm_v12",
        "alk_positive_v7": "prm_v12", "ret_positive_v7": "prm_v12",
        "ntrk_positive_v7": "prm_v12", "tp53_positive_v7": "prm_v12",
        "braf_recovered_status_v11": "prm_v12", "ras_resolved_gene_v13": "prm_v12",
        "tert_variant_v9": "prm_v12", "tert_tested": "prm_v12",
        "confirmed_rai_episodes": "prm_v12", "rai_dose_v9": "prm_v12",
        "rai_avidity": "prm_v12", "rai_validation_tier": "prm_v12",
        "max_stimulated_tg": "prm_v12", "post_rai_tg_nadir": "prm_v12",
        "capsular_invasion_refined": "prm_v12", "vascular_who_2022_grade": "prm_v12",
        "vasc_grade_final_v13": "prm_v12", "lvi_grade_final_v13": "prm_v12",
        "pni_positive": "prm_v12", "margin_r_classification": "prm_v12",
        "completion_reason": "prm_v12", "voice_outcome_category": "prm_v12",
        "lateral_neck_dissected_v10": "prm_v12",
        "n_fna_episodes": "prm_v12", "bethesda_final_name": "prm_v12",
        # ct_imaging
        "ct_n_exams": "ct_imaging", "ct_goiter_present_any": "ct_imaging",
        "ct_substernal_extension_any": "ct_imaging", "ct_tracheal_deviation_any": "ct_imaging",
        "ct_ln_enlarged_any": "ct_imaging", "ct_ln_suspicious_any": "ct_imaging",
        # nuclear_med
        "nucmed_n_scans": "nuclear_med", "nucmed_has_rai_scan": "nuclear_med",
        # complication_summary
        "any_confirmed_complication_flag": "complication_summary",
        "n_confirmed_complications": "complication_summary",
        "has_low_pth_flag": "complication_summary",
        # thyroid_scoring
        "gross_ete_flag": "thyroid_scoring", "ata_initial_risk": "thyroid_scoring",
        "macis_risk_group": "thyroid_scoring", "aggressive_variant_flag": "thyroid_scoring",
        "recurrence_flag_scoring": "thyroid_scoring",
        # imaging_summary
        "n_us_exams": "imaging_patient_summary",
        "bilateral_disease_flag": "imaging_patient_summary",
        "dominant_nodule_size_cm": "imaging_patient_summary",
        # tg_timeline
        "n_tg_measurements_structured": "tg_timeline",
        "tg_trajectory_class": "tg_timeline", "tgab_interference_flag": "tg_timeline",
        "tgab_nadir": "tg_timeline",
    }

    for col in df.columns:
        non_null = int(df[col].notna().sum())
        pct = 100 * non_null / len(df)
        src = source_map.get(col, "gold_master/canonical")
        print(f"  {col:<50} {non_null:>10,} {pct:>6.1f}%  {src}")


def save_and_upload(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("PHASE 4: SAVE & UPLOAD")
    print("=" * 60)

    out_path = OUTPUT_DIR / "canonical_patient_master_v1.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  Saved parquet: {out_path}")
    print(f"  Size: {out_path.stat().st_size / 1_048_576:.1f} MB")

    md_rows = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    md_cols = con.execute(f"""
        SELECT COUNT(DISTINCT column_name) FROM information_schema.columns
        WHERE table_name='{TABLE}' AND table_schema='main'
    """).fetchone()[0]
    print(f"\n  MotherDuck: {TABLE}")
    print(f"  Rows:    {md_rows:,}")
    print(f"  Columns: {md_cols}")


def main() -> None:
    con = connect()
    print(f"Connected to MotherDuck — database: {DB}")

    run_phase1_audit(con)
    df = run_build(con)
    run_validation(con, df)
    save_and_upload(con, df)

    print("\n" + "=" * 60)
    print("✓ Script 207 COMPLETE")
    print(f"  Table:   {TABLE}")
    print(f"  Rows:    {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Database:{DB}")
    print("=" * 60)

    con.close()


if __name__ == "__main__":
    main()
