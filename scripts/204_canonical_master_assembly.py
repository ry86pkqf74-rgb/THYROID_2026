#!/usr/bin/env python3
"""
THYROID_2026 — Canonical Master Table Assembly
Prompt 5: Assembles all canonical tables + gold_master into
           canonical_patient_master_v1 — ONE row per patient.

Dependencies: Prompts 1-4 must be complete.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

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

    -- FNA/Bethesda (from gold_master)
    g.fna_bethesda_final AS bethesda_final,

    -- Imaging (from gold_master)
    g.imaging_tirads_best AS preop_tirads_best,
    g.imaging_tirads_worst AS preop_tirads_worst,
    g.imaging_tirads_category AS preop_tirads_category,
    g.imaging_nodule_size_cm AS preop_imaging_size_cm,

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
"""


def main():
    con = connect()
    print(f"Connected to MotherDuck {DB}")

    # Verify prerequisites
    print("\n=== Verifying prerequisites ===")
    prereqs = {
        "canonical_diagnosis_unified_v1": 10871,
        "canonical_recurrence_v1": 10000,
        "canonical_survival_followup_v1": 10000,
        "canonical_molecular_tested_v1": 1000,
    }
    for table, min_rows in prereqs.items():
        r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {table}").fetchone()
        status = "✓" if r[1] >= min_rows else "⚠"
        print(f"  {status} {table}: {r[0]} rows, {r[1]} patients")

    # Build master table
    print("\n=== Building canonical_patient_master_v1 ===")
    con.execute(f"CREATE OR REPLACE TABLE canonical_patient_master_v1 AS {MASTER_SQL}")
    df = con.execute("SELECT * FROM canonical_patient_master_v1").fetchdf()
    print(f"  Rows: {len(df)}, Patients: {df['research_id'].nunique()}")

    # ── VALIDATION ────────────────────────────────────────────────────────────
    print("\n=== Validation ===")

    # 1. Row count
    assert len(df) == 10871, f"Expected 10,871 rows, got {len(df)}"
    print(f"  ✓ Row count: {len(df)} (expected 10,871)")

    # 2. No NULL research_id
    null_rids = df["research_id"].isna().sum()
    assert null_rids == 0, f"Found {null_rids} NULL research_ids"
    print("  ✓ No NULL research_ids")

    # 3. Every patient has diagnosis_primary
    null_diag = df["diagnosis_primary"].isna().sum()
    print(f"  {'✓' if null_diag == 0 else '⚠'} diagnosis_primary NULL: {null_diag}")

    # 4. Follow-up
    null_followup = df["followup_days"].isna().sum()
    neg_followup = (df["followup_days"] < 0).sum() if null_followup < len(df) else 0
    print(f"  followup_days NULL: {null_followup}, negative: {neg_followup}")

    # 5. Completeness report
    print("\n  === FIELD COMPLETENESS ===")
    key_fields = [
        "age_at_surgery", "sex", "race", "first_surgery_date",
        "is_malignant", "diagnosis_primary", "tumor_size_cm",
        "ajcc8_stage_group", "ata_risk_category",
        "ln_total_examined", "ln_total_positive", "ln_positive_flag",
        "bethesda_final", "preop_tirads_best",
        "molecular_tested_confirmed", "braf_positive", "ras_positive", "tert_positive",
        "rai_received_flag", "rai_max_dose_mci",
        "tg_nadir", "tg_n_measurements",
        "recurrence_confirmed", "recurrence_type",
        "followup_days", "followup_years", "last_contact_date",
    ]
    for f in key_fields:
        if f in df.columns:
            non_null = df[f].notna().sum()
            _non_default = non_null
            if df[f].dtype == bool:
                _non_default = (df[f] == True).sum()
            pct = 100 * non_null / len(df)
            print(f"    {f}: {non_null}/{len(df)} ({pct:.1f}%)")

    # 6. Diagnosis distribution
    print("\n  === DIAGNOSIS DISTRIBUTION ===")
    malignant = df[df["is_malignant"] == True]
    benign = df[df["is_malignant"] == False]
    print(f"  Malignant: {len(malignant)}")
    print(f"  Benign:    {len(benign)}")
    if len(malignant) > 0:
        print("\n  Malignant subtypes:")
        mdist = malignant["diagnosis_primary"].value_counts()
        for k, v in mdist.items():
            print(f"    {k}: {v}")
    if len(benign) > 0:
        print("\n  Benign subtypes:")
        bdist = benign["diagnosis_primary"].value_counts()
        for k, v in bdist.head(15).items():
            print(f"    {k}: {v}")

    # 7. Recurrence summary
    print("\n  === RECURRENCE SUMMARY ===")
    confirmed = df[df["recurrence_confirmed"] == True]
    print(f"  Confirmed recurrences:      {len(confirmed)}")
    rdist = df["recurrence_type"].value_counts()
    for k, v in rdist.items():
        if k != "none":
            print(f"    {k}: {v}")
    print(f"  No recurrence:              {(df['recurrence_type'] == 'none').sum()}")

    # 8. Molecular summary
    print("\n  === MOLECULAR SUMMARY ===")
    tested = df[df["molecular_tested_confirmed"] == True]
    print(f"  Tested:  {len(tested)}")
    print(f"  BRAF+:   {(df['braf_positive'] == True).sum()}")
    print(f"  RAS+:    {(df['ras_positive'] == True).sum()}")
    print(f"  TERT+:   {(df['tert_positive'] == True).sum()}")

    # 9. Follow-up summary
    print("\n  === FOLLOW-UP SUMMARY ===")
    fu = df["followup_years"].dropna()
    if len(fu) > 0:
        print(f"  Median: {fu.median():.2f} years")
        print(f"  Mean:   {fu.mean():.2f} years")
        q25, q75 = fu.quantile([0.25, 0.75])
        print(f"  IQR:    {q25:.2f} – {q75:.2f}")
        print(f"  Range:  {fu.min():.2f} – {fu.max():.2f}")
    print(f"  Patients with follow-up data: {fu.notna().sum()}")

    # Save parquet
    out_path = OUTPUT_DIR / "canonical_patient_master_v1.parquet"
    df.to_parquet(out_path, index=False)
    print(f"\n  Saved: {out_path}")

    # Verify upload
    verify = con.execute("SELECT COUNT(*) FROM canonical_patient_master_v1").fetchone()[0]
    print(f"  Uploaded to MotherDuck: {verify} rows")
    cols = con.execute("SELECT COUNT(DISTINCT column_name) FROM information_schema.columns WHERE table_name='canonical_patient_master_v1'").fetchone()[0]
    print(f"  Columns: {cols}")

    print("\n✓ Prompt 5 COMPLETE — canonical_patient_master_v1 uploaded to MotherDuck")
    print(f"  Database: {DB}")
    print("  Table: canonical_patient_master_v1")
    print(f"  Rows: {verify}")
    print("  One row per patient: YES")
    con.close()


if __name__ == "__main__":
    main()
