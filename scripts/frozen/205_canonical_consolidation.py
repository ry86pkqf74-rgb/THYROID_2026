#!/usr/bin/env python3
"""
THYROID_2026 — Script 205: Canonical Consolidation
Database: thyroid_ete_fix_20260413 (ONLY)

Integrates validated data into canonical_patient_master_v1:
  1. Propagate fna_path_outcome from "Thyroid 2026" → ete_fix
  2. Copy tirads_llm_extracted_v2 from thyroid_research_ro_v2
  3. Rebuild canonical_patient_master_v1 with new fields:
     - fna_path_outcome / concordance
     - TIRADS combined (imaging_nodule_master + LLM-extracted)
     - Multi-era Bethesda (2023/2015/2010) from fna_cytology
     - Detailed LN from tumor_pathology (per-cancer-type mets, deposit size, levels)
     - Imaging LN assessment from ultrasound_reports
  4. Validate and save
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DB = "thyroid_ete_fix_20260413"


def connect():
    token = get_token()
    if not token:
        raise RuntimeError("MotherDuck token not found")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


# ---------------------------------------------------------------------------
# STEP 1 — Propagate fna_path_outcome from "Thyroid 2026" to ete_fix
# ---------------------------------------------------------------------------

def step1_propagate_fna_path_outcome(con: duckdb.DuckDBPyConnection):
    print("\n" + "=" * 70)
    print("STEP 1 — Propagate fna_path_outcome from 'Thyroid 2026' → ete_fix")
    print("=" * 70)

    print("\n  BEFORE:")
    before = con.execute("""
        SELECT fna_path_outcome, COUNT(*) AS n
        FROM patient_refined_master_clinical_v12
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    print(before.to_string(index=False))

    con.execute("""
        UPDATE patient_refined_master_clinical_v12 AS dst
        SET fna_path_outcome = src.fna_path_outcome
        FROM "Thyroid 2026".main.patient_refined_master_clinical_v12 AS src
        WHERE CAST(dst.research_id AS VARCHAR) = CAST(src.research_id AS VARCHAR)
          AND src.fna_path_outcome IS NOT NULL
    """)

    con.execute("""
        UPDATE patient_refined_master_clinical_v12 AS dst
        SET fna_path_concordance_category = src.fna_path_concordance_category
        FROM "Thyroid 2026".main.patient_refined_master_clinical_v12 AS src
        WHERE CAST(dst.research_id AS VARCHAR) = CAST(src.research_id AS VARCHAR)
          AND src.fna_path_concordance_category IS NOT NULL
    """)

    con.execute("""
        UPDATE patient_refined_master_clinical_v12 AS dst
        SET fna_path_concordant = src.fna_path_concordant
        FROM "Thyroid 2026".main.patient_refined_master_clinical_v12 AS src
        WHERE CAST(dst.research_id AS VARCHAR) = CAST(src.research_id AS VARCHAR)
          AND src.fna_path_concordant IS NOT NULL
    """)

    print("\n  AFTER:")
    after = con.execute("""
        SELECT fna_path_outcome, COUNT(*) AS n
        FROM patient_refined_master_clinical_v12
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    print(after.to_string(index=False))

    null_count = con.execute("""
        SELECT COUNT(*) FROM patient_refined_master_clinical_v12
        WHERE fna_path_outcome IS NULL
    """).fetchone()[0]
    print(f"\n  fna_path_outcome NULL count: {null_count}")
    if null_count > 0:
        print("  ⚠ Some patients still have NULL fna_path_outcome")
    else:
        print("  ✓ All patients have fna_path_outcome")

    return null_count


# ---------------------------------------------------------------------------
# STEP 2 — Copy tirads_llm_extracted_v2 to ete_fix
# ---------------------------------------------------------------------------

def step2_copy_tirads_llm(con: duckdb.DuckDBPyConnection):
    print("\n" + "=" * 70)
    print("STEP 2 — Copy tirads_llm_extracted_v2 from thyroid_research_ro_v2")
    print("=" * 70)

    con.execute("""
        CREATE OR REPLACE TABLE tirads_llm_extracted_v2 AS
        SELECT * FROM thyroid_research_ro_v2.main.tirads_llm_extracted_v2
    """)

    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM tirads_llm_extracted_v2
    """).fetchone()
    print(f"  Rows: {r[0]}, Patients: {r[1]}")
    assert r[0] > 5000, f"Expected 5000+ rows, got {r[0]}"
    print("  ✓ tirads_llm_extracted_v2 copied successfully")


# ---------------------------------------------------------------------------
# STEP 3 — Rebuild canonical_patient_master_v1
# ---------------------------------------------------------------------------

MASTER_SQL = """
WITH patient_spine AS (
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
    FROM gold_master_patient_facts_v1
),

-- Canonical diagnosis (one row per patient, deduplicate)
diag AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
                CASE WHEN is_malignant THEN 0 ELSE 1 END,
                CASE
                    WHEN source_table = 'tumor_pathology' THEN 0
                    WHEN source_table = 'gold_master_patient_facts_v1' THEN 1
                    WHEN source_table = 'path_synoptics' THEN 2
                    ELSE 3
                END
            ) AS rn
        FROM canonical_diagnosis_unified_v1
    ) WHERE rn = 1
),

-- Canonical recurrence (one row per patient, deduplicate)
recur AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
                CASE WHEN recurrence_confirmed THEN 0 ELSE 1 END,
                CASE recurrence_type
                    WHEN 'structural_confirmed' THEN 1
                    WHEN 'fna_confirmed' THEN 2
                    WHEN 'structural_confirmed_legacy' THEN 3
                    WHEN 'biochemical_tg_rise' THEN 4
                    WHEN 'persistent_biochemical_disease' THEN 5
                    WHEN 'imaging_suspicious_unconfirmed' THEN 6
                    WHEN 'none' THEN 7
                    ELSE 8
                END
            ) AS rn
        FROM canonical_recurrence_v1
    ) WHERE rn = 1
),

-- Canonical survival (one row per patient)
surv AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY followup_days DESC NULLS LAST) AS rn
        FROM canonical_survival_followup_v1
    ) WHERE rn = 1
),

-- TIRADS combined: existing scored nodules + LLM-extracted
tirads_combined AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
        tirads_acr_recalculated AS tirads_score,
        exam_date
    FROM imaging_nodule_master_v1
    WHERE tirads_acr_recalculated IS NOT NULL

    UNION ALL

    SELECT CAST(research_id AS VARCHAR),
        CASE tirads_level_2017
            WHEN 'TR1' THEN 1 WHEN 'TR2' THEN 2 WHEN 'TR3' THEN 3
            WHEN 'TR4' THEN 4 WHEN 'TR5' THEN 5
        END,
        NULL
    FROM tirads_llm_extracted_v2
    WHERE tirads_level_2017 IS NOT NULL
),
tirads_patient AS (
    SELECT research_id,
        MAX(tirads_score) AS tirads_worst_combined,
        MIN(tirads_score) AS tirads_best_combined,
        COUNT(*) AS tirads_nodules_scored_combined
    FROM tirads_combined
    WHERE tirads_score IS NOT NULL
    GROUP BY research_id
),

-- Multi-era Bethesda from fna_cytology
bethesda_multi AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
        MAX(bethesda_2023_num) AS bethesda_2023,
        MAX(bethesda_2015_num) AS bethesda_2015,
        MAX(bethesda_2010_num) AS bethesda_2010,
        COUNT(*) AS n_fna_cytology_records
    FROM fna_cytology
    GROUP BY 1
),

-- Enhanced LN from tumor_pathology (one row per patient, prefer row with LN data)
tp_ln AS (
    SELECT * FROM (
        SELECT CAST(research_id AS VARCHAR) AS research_id,
            primary_ln_ln_total_examined AS tp_ln_examined,
            primary_ln_ln_total_positive AS tp_ln_positive,
            primary_ln_ln_extranodal_extension AS tp_ln_ene,
            primary_ln_ln_largest_deposit_cm AS tp_ln_largest_deposit_cm,
            ln_total_levels_involved AS tp_ln_levels_involved,
            primary_ln_ln_central_positive AS tp_ln_central_positive,
            primary_ln_ln_lateral_positive AS tp_ln_lateral_positive,
            ln_mets_ptc, ln_mets_ftc, ln_mets_mtc, ln_mets_atc,
            ln_mets_hurthle, ln_mets_pdtc,
            ln_mets_micrometastasis,
            ln_mets_extranodal_extension AS ln_mets_ene_count,
            ln_central_examined AS tp_central_examined,
            ln_central_positive AS tp_central_positive_total,
            ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
                CASE WHEN primary_ln_ln_total_positive IS NOT NULL THEN 0 ELSE 1 END,
                primary_ln_ln_total_examined DESC NULLS LAST
            ) AS rn
        FROM tumor_pathology
    ) WHERE rn = 1
),

-- Patient refined master (deduplicated — source has duplicate research_ids)
prm_dedup AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
                CASE WHEN fna_path_outcome IS NOT NULL THEN 0 ELSE 1 END
            ) AS rn
        FROM patient_refined_master_clinical_v12
    ) WHERE rn = 1
),

-- Imaging LN from ultrasound_reports
imaging_ln AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
        MAX(CASE
            WHEN lymph_node_assessment NOT IN (
                'No abnormal lymph nodes identified',
                'Normal cervical lymph nodes'
            ) THEN TRUE ELSE FALSE
        END) AS imaging_ln_abnormal,
        COUNT(DISTINCT ultrasound_date) AS n_us_with_ln_assessment
    FROM ultrasound_reports
    WHERE lymph_node_assessment IS NOT NULL
    GROUP BY 1
)

SELECT
    -- Identity
    ps.research_id,

    -- Demographics
    g.age_at_surgery,
    g.sex,
    g.race,

    -- Surgery
    g.surg_first_date AS first_surgery_date,
    g.surg_procedure_type,
    g.surg_n_procedures,
    g.surg_total_thyroidectomy,
    g.surg_hemithyroidectomy,

    -- Diagnosis (from canonical_diagnosis_unified_v1)
    d.is_malignant,
    d.diagnosis_primary,
    d.diagnosis_variant,
    d.diagnosis_full,
    d.n_tumors,

    -- Pathology tumor features (from gold_master)
    g.path_tumor_size_cm AS tumor_size_cm,
    g.path_multifocal_flag AS multifocal_flag,
    g.path_laterality AS laterality,
    g.ete_grade_final AS ete_grade,
    g.margin_status_final AS margin_status,
    g.closest_margin_mm,
    g.vascular_invasion_final AS vascular_invasion_grade,
    g.vascular_vessel_count AS vessel_count,
    g.path_lvi_raw AS lvi_grade,
    g.path_pni_raw AS perineural_invasion,

    -- Staging (from gold_master)
    g.ajcc8_t_stage,
    g.ajcc8_n_stage,
    g.ajcc8_m_stage,
    g.ajcc8_stage_group,
    g.ata_risk_category,
    g.ata_response_category,
    g.macis_score,
    g.ages_score,
    g.ames_risk_group,

    -- Lymph Nodes (from gold_master)
    g.path_ln_examined_raw AS ln_total_examined,
    g.path_ln_positive_raw AS ln_total_positive,
    g.ln_ratio,
    g.ln_positive_final AS ln_positive_flag,
    g.lateral_neck_dissected AS ln_lateral_dissected,
    g.path_ene_raw AS ln_ene_status,
    g.ln_burden_band,

    -- FNA/Bethesda (from gold_master + fna_cytology multi-era)
    g.fna_bethesda_final AS bethesda_final,
    bm.bethesda_2023,
    bm.bethesda_2015,
    bm.bethesda_2010,
    bm.n_fna_cytology_records,

    -- FNA-Path outcome (from patient_refined_master_clinical_v12)
    prm.fna_path_outcome,
    prm.fna_path_concordance_category,
    prm.fna_path_concordant,

    -- Imaging / TIRADS (from gold_master + combined TIRADS)
    g.imaging_tirads_best AS preop_tirads_best,
    g.imaging_tirads_worst AS preop_tirads_worst,
    g.imaging_tirads_category AS preop_tirads_category,
    g.imaging_nodule_size_cm AS preop_imaging_size_cm,
    tp_tirads.tirads_worst_combined,
    tp_tirads.tirads_best_combined,
    tp_tirads.tirads_nodules_scored_combined,

    -- Imaging LN assessment
    COALESCE(iln.imaging_ln_abnormal, FALSE) AS imaging_ln_abnormal,
    iln.n_us_with_ln_assessment,

    -- Molecular (from canonical_molecular_tested_v1)
    COALESCE(m.molecular_tested_confirmed, FALSE) AS molecular_tested_confirmed,
    m.platform_canonical AS mol_platform,
    m.test_count AS mol_test_count,
    m.has_thyroseq AS mol_has_thyroseq,
    m.has_afirma AS mol_has_afirma,
    COALESCE(m.braf_positive_canonical, g.braf_positive_final, FALSE) AS braf_positive,
    g.braf_variant_raw AS braf_variant,
    COALESCE(m.ras_positive_canonical, g.ras_positive_final, FALSE) AS ras_positive,
    g.ras_subtype_raw AS ras_subtype,
    COALESCE(m.tert_positive_canonical, g.tert_positive_final, FALSE) AS tert_positive,
    COALESCE(g.molecular_risk_tier, m.molecular_risk_tier) AS molecular_risk_tier,
    m.first_test_date AS mol_first_test_date,

    -- RAI (from gold_master)
    COALESCE(g.rai_received_flag, FALSE) AS rai_received_flag,
    g.rai_first_date,
    g.rai_max_dose_mci,

    -- Thyroglobulin (from gold_master)
    g.tg_nadir,
    g.tg_peak,
    g.tg_last_value,
    g.tg_rising_flag,
    g.tg_n_measurements,

    -- Recurrence (from canonical_recurrence_v1)
    COALESCE(r.recurrence_confirmed, FALSE) AS recurrence_confirmed,
    COALESCE(r.recurrence_type, 'none') AS recurrence_type,
    r.recurrence_date,
    r.recurrence_site,
    r.recurrence_histology,
    r.recurrence_evidence_source,
    r.recurrence_definition,
    r.time_to_recurrence_days,
    r.biochemical_tg_nadir AS biochemical_tg_nadir_after_surgery,
    r.biochemical_tg_at_recurrence,
    CASE WHEN r.recurrence_type = 'imaging_suspicious_unconfirmed' THEN TRUE ELSE FALSE END AS imaging_suspicious_unconfirmed,

    -- Complications (from gold_master)
    g.rln_status,
    g.rln_permanent_flag,
    g.rln_transient_flag,
    g.hypocalcemia_status,
    g.hypoparathyroidism_status,
    g.chyle_leak_status,
    g.hematoma_status,
    g.seroma_status,
    g.wound_infection_status,

    -- Operative details (from gold_master)
    g.op_rln_monitoring_any,
    g.op_drain_placed_any,

    -- Enhanced LN detail (from tumor_pathology)
    tln.tp_ln_examined,
    tln.tp_ln_positive,
    tln.tp_ln_ene,
    tln.tp_ln_largest_deposit_cm,
    tln.tp_ln_levels_involved,
    tln.tp_ln_central_positive,
    tln.tp_ln_lateral_positive,
    tln.tp_central_examined,
    tln.tp_central_positive_total,
    tln.ln_mets_ptc,
    tln.ln_mets_ftc,
    tln.ln_mets_mtc,
    tln.ln_mets_atc,
    tln.ln_mets_hurthle,
    tln.ln_mets_pdtc,
    tln.ln_mets_micrometastasis,
    tln.ln_mets_ene_count,

    -- Follow-up (from canonical_survival_followup_v1)
    sv.last_contact_date,
    sv.last_contact_source,
    sv.followup_days,
    sv.followup_years,
    sv.followup_category,

    -- Data quality flags
    CASE
        WHEN d.source_table = 'tumor_pathology' THEN 'HIGH'
        WHEN d.source_table = 'gold_master_patient_facts_v1' THEN 'MEDIUM'
        WHEN d.source_table = 'path_synoptics' THEN 'HIGH'
        ELSE 'LOW'
    END AS diagnosis_confidence,
    CASE
        WHEN r.recurrence_confirmed = TRUE THEN 'HIGH'
        WHEN r.recurrence_type IN ('biochemical_tg_rise','persistent_biochemical_disease') THEN 'MEDIUM'
        WHEN r.recurrence_type = 'imaging_suspicious_unconfirmed' THEN 'LOW'
        WHEN r.recurrence_type = 'none' THEN 'HIGH'
        ELSE 'LOW'
    END AS recurrence_data_confidence,
    CASE
        WHEN m.molecular_tested_confirmed = TRUE THEN 'HIGH'
        ELSE 'NOT_TESTED'
    END AS molecular_data_confidence,
    g.lab_completeness_score AS followup_completeness_score

FROM patient_spine ps
LEFT JOIN gold_master_patient_facts_v1 g
    ON ps.research_id = CAST(g.research_id AS VARCHAR)
LEFT JOIN diag d
    ON ps.research_id = d.research_id
LEFT JOIN recur r
    ON ps.research_id = CAST(r.research_id AS VARCHAR)
LEFT JOIN surv sv
    ON ps.research_id = sv.research_id
LEFT JOIN canonical_molecular_tested_v1 m
    ON ps.research_id = CAST(m.research_id AS VARCHAR)
LEFT JOIN prm_dedup prm
    ON ps.research_id = CAST(prm.research_id AS VARCHAR)
LEFT JOIN tirads_patient tp_tirads
    ON ps.research_id = tp_tirads.research_id
LEFT JOIN bethesda_multi bm
    ON ps.research_id = bm.research_id
LEFT JOIN tp_ln tln
    ON ps.research_id = tln.research_id
LEFT JOIN imaging_ln iln
    ON ps.research_id = iln.research_id
"""


def step3_rebuild_master(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    print("\n" + "=" * 70)
    print("STEP 3 — Rebuild canonical_patient_master_v1 with new fields")
    print("=" * 70)

    print("\n  Verifying prerequisites...")
    prereqs = {
        "canonical_diagnosis_unified_v1": 10871,
        "canonical_recurrence_v1": 10000,
        "canonical_survival_followup_v1": 10000,
        "canonical_molecular_tested_v1": 1000,
        "tirads_llm_extracted_v2": 5000,
        "fna_cytology": 5000,
    }
    for table, min_rows in prereqs.items():
        r = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {table}"
        ).fetchone()
        status = "✓" if r[1] >= min_rows else "⚠"
        print(f"    {status} {table}: {r[0]} rows, {r[1]} patients")

    print("\n  Building master table...")
    con.execute(
        f"CREATE OR REPLACE TABLE canonical_patient_master_v1 AS {MASTER_SQL}"
    )
    df = con.execute("SELECT * FROM canonical_patient_master_v1").fetchdf()
    print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    return df


# ---------------------------------------------------------------------------
# STEP 4 — Validate
# ---------------------------------------------------------------------------

def step4_validate(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    print("\n" + "=" * 70)
    print("STEP 4 — Validate rebuilt master")
    print("=" * 70)

    errors = []

    # 4a. Row count
    if len(df) != 10871:
        errors.append(f"Row count {len(df)} != 10871")
    print(f"  {'✓' if len(df) == 10871 else '✗'} Row count: {len(df)} (expected 10,871)")

    # 4b. No NULL research_id
    null_rids = df["research_id"].isna().sum()
    if null_rids > 0:
        errors.append(f"{null_rids} NULL research_ids")
    print(f"  {'✓' if null_rids == 0 else '✗'} NULL research_ids: {null_rids}")

    # 4c. fna_path_outcome coverage
    fna_null = df["fna_path_outcome"].isna().sum()
    print(f"  {'✓' if fna_null == 0 else '⚠'} fna_path_outcome NULL: {fna_null}")
    if fna_null == 0:
        print("    Distribution:")
        for val, cnt in df["fna_path_outcome"].value_counts().items():
            print(f"      {val}: {cnt}")

    # 4d. TIRADS coverage
    tirads_non_null = df["tirads_worst_combined"].notna().sum()
    print(f"  TIRADS coverage: {tirads_non_null}/{len(df)} ({100*tirads_non_null/len(df):.1f}%)")

    # 4e. Bethesda multi-era coverage
    for era in ["bethesda_2023", "bethesda_2015", "bethesda_2010"]:
        if era in df.columns:
            nn = df[era].notna().sum()
            print(f"  {era}: {nn}/{len(df)} ({100*nn/len(df):.1f}%)")

    # 4f. Enhanced LN coverage
    tp_ln_nn = df["tp_ln_examined"].notna().sum()
    print(f"  tumor_pathology LN examined: {tp_ln_nn}/{len(df)} ({100*tp_ln_nn/len(df):.1f}%)")

    # 4g. Imaging LN
    img_ln = (df["imaging_ln_abnormal"] == True).sum() if "imaging_ln_abnormal" in df.columns else 0
    print(f"  Imaging LN abnormal: {img_ln}")

    # Full completeness report
    print("\n  === FIELD COMPLETENESS (all columns) ===")
    for col in sorted(df.columns):
        non_null = df[col].notna().sum()
        pct = 100 * non_null / len(df)
        if pct < 100:
            print(f"    {col}: {non_null}/{len(df)} ({pct:.1f}%)")

    # Diagnosis distribution
    print("\n  === DIAGNOSIS DISTRIBUTION ===")
    malig = df[df["is_malignant"] == True]
    benig = df[df["is_malignant"] == False]
    print(f"  Malignant: {len(malig)}")
    print(f"  Benign:    {len(benig)}")

    # Recurrence summary
    print("\n  === RECURRENCE SUMMARY ===")
    confirmed = df[df["recurrence_confirmed"] == True]
    print(f"  Confirmed recurrences: {len(confirmed)}")
    rdist = df["recurrence_type"].value_counts()
    for k, v in rdist.items():
        if k != "none":
            print(f"    {k}: {v}")

    # Molecular summary
    print("\n  === MOLECULAR SUMMARY ===")
    tested = df[df["molecular_tested_confirmed"] == True]
    print(f"  Tested:  {len(tested)}")
    print(f"  BRAF+:   {(df['braf_positive'] == True).sum()}")
    print(f"  RAS+:    {(df['ras_positive'] == True).sum()}")
    print(f"  TERT+:   {(df['tert_positive'] == True).sum()}")

    # Follow-up summary
    print("\n  === FOLLOW-UP SUMMARY ===")
    fu = df["followup_years"].dropna()
    if len(fu) > 0:
        print(f"  Median: {fu.median():.2f} years")
        print(f"  Mean:   {fu.mean():.2f} years")
        print(f"  Patients with follow-up data: {len(fu)}")

    if errors:
        print(f"\n  ✗ VALIDATION ERRORS: {errors}")
        raise AssertionError(f"Validation failed: {errors}")
    else:
        print("\n  ✓ All validation checks passed")


# ---------------------------------------------------------------------------
# STEP 5 — Save parquet + verify upload
# ---------------------------------------------------------------------------

def step5_save(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    print("\n" + "=" * 70)
    print("STEP 5 — Save & verify")
    print("=" * 70)

    out_path = OUTPUT_DIR / "canonical_patient_master_v1.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  Saved: {out_path}")

    verify = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master_v1"
    ).fetchone()[0]
    print(f"  Uploaded to MotherDuck: {verify} rows")
    cols = len(df.columns)
    print(f"  Columns: {cols} (was 96)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    con = connect()
    print(f"Connected to MotherDuck {DB}")

    step1_propagate_fna_path_outcome(con)
    step2_copy_tirads_llm(con)
    df = step3_rebuild_master(con)
    step4_validate(con, df)
    step5_save(con, df)

    print("\n" + "=" * 70)
    print("✓ Script 205 COMPLETE — canonical_patient_master_v1 consolidated")
    print(f"  Database: {DB}")
    print("  Table: canonical_patient_master_v1")
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {len(df.columns)}")
    print("=" * 70)

    con.close()


if __name__ == "__main__":
    main()
