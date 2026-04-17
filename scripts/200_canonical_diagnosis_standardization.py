#!/usr/bin/env python3
"""
THYROID_2026 — Canonical Diagnosis Standardization (Malignant + Benign)
Prompt 1: Pure SQL/Python — NO LLM needed.

Creates:
  - canonical_malignant_diagnosis_v1
  - canonical_benign_diagnosis_v1
  - canonical_diagnosis_unified_v1

All uploaded to MotherDuck thyroid_ete_fix_20260413.
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


# ── MALIGNANT HISTOLOGY STANDARDIZATION ──────────────────────────────────────

MALIGNANT_SQL = """
WITH raw_histology AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        TRIM(tumor_1_histology_base) AS hb_raw,
        TRIM(tumor_1_histology_variant) AS hv_raw,
        TRIM(tumor_1_histology_subtype_detail) AS hsd_raw,
        TRIM(tumor_2_histology_base) AS hb2_raw,
        COALESCE(
            NULLIF(TRIM(CAST(tumor_2_present AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(tumor_3_present AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(tumor_4_present AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(tumor_5_present AS VARCHAR)), '')
        ) IS NOT NULL AS has_multi_tumor,
        (CASE WHEN tumor_2_present IS NOT NULL AND TRIM(CAST(tumor_2_present AS VARCHAR)) != '' THEN 1 ELSE 0 END
         + CASE WHEN tumor_3_present IS NOT NULL AND TRIM(CAST(tumor_3_present AS VARCHAR)) != '' THEN 1 ELSE 0 END
         + CASE WHEN tumor_4_present IS NOT NULL AND TRIM(CAST(tumor_4_present AS VARCHAR)) != '' THEN 1 ELSE 0 END
         + CASE WHEN tumor_5_present IS NOT NULL AND TRIM(CAST(tumor_5_present AS VARCHAR)) != '' THEN 1 ELSE 0 END
         + 1) AS n_tumors
    FROM tumor_pathology
    WHERE tumor_1_histology_base IS NOT NULL
      AND TRIM(tumor_1_histology_base) != ''
),
standardized AS (
    SELECT
        research_id,
        hb_raw,
        hv_raw,
        hsd_raw,
        hb2_raw,
        has_multi_tumor,
        n_tumors,
        -- Base histology standardization
        CASE
            WHEN LOWER(TRIM(hb_raw)) IN ('ptc','papillary thyroid carcinoma') THEN 'PTC'
            WHEN LOWER(TRIM(hb_raw)) IN ('ftc','follicular carcinoma','follicular thyroid carcinoma') THEN 'FTC'
            WHEN LOWER(TRIM(hb_raw)) IN ('mtc','medullary thyroid carcinoma','medullary carcinoma') THEN 'MTC'
            WHEN LOWER(TRIM(hb_raw)) IN ('atc','anaplastic carcinoma','anaplastic thyroid carcinoma') THEN 'ATC'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%poorly differentiated%' OR LOWER(TRIM(hb_raw)) LIKE '%pooly differentiated%' THEN 'PDTC'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%differentiated high grade%' OR LOWER(TRIM(hb_raw)) LIKE '%dhgtc%' THEN 'DHGTC'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%niftp%' THEN 'NIFTP'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%ftump%' THEN 'FTUMP'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%follicular adenoma%' THEN 'follicular_adenoma'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%atypical%hurthle%' OR LOWER(TRIM(hb_raw)) LIKE '%atypical%oncocytic%' THEN 'HCC'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%hurthle%carcinoma%' OR LOWER(TRIM(hb_raw)) LIKE '%oncocytic%carcinoma%' THEN 'HCC'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%metastatic%' THEN
                CASE
                    WHEN LOWER(hb_raw) LIKE '%ptc%' OR LOWER(hb_raw) LIKE '%papillary%' OR LOWER(hb_raw) LIKE '%paillary%' THEN 'PTC'
                    WHEN LOWER(hb_raw) LIKE '%ftc%' OR LOWER(hb_raw) LIKE '%follicular%carcinoma%' THEN 'FTC'
                    WHEN LOWER(hb_raw) LIKE '%mtc%' OR LOWER(hb_raw) LIKE '%medullary%' THEN 'MTC'
                    WHEN LOWER(hb_raw) LIKE '%hurthle%' OR LOWER(hb_raw) LIKE '%oncocytic%' THEN 'HCC'
                    WHEN LOWER(hb_raw) LIKE '%anaplastic%' THEN 'ATC'
                    ELSE 'other_malignant'
                END
            WHEN LOWER(TRIM(hb_raw)) LIKE '%nut carcinoma%' THEN 'other_malignant'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%adenoid cystic%' THEN 'other_malignant'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%infiltrating%thymus%' OR LOWER(TRIM(hb_raw)) LIKE '%castle%' THEN 'other_malignant'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%squamous%' THEN 'other_malignant'
            WHEN LOWER(TRIM(hb_raw)) LIKE '%differentiated thyroid carcinoma%' THEN 'DTC_NOS'
            ELSE 'other_malignant'
        END AS histology_base_canonical,
        -- Variant standardization
        CASE
            WHEN hv_raw IS NULL OR TRIM(hv_raw) = '' THEN NULL
            WHEN LOWER(hv_raw) LIKE '%follicular variant%' THEN 'follicular_variant'
            WHEN LOWER(hv_raw) LIKE '%tall cell%' THEN 'tall_cell'
            WHEN LOWER(hv_raw) LIKE '%oncocytic%' OR LOWER(hv_raw) LIKE '%warthin%' OR LOWER(hv_raw) LIKE '%hurthle%type%' THEN 'oncocytic_warthin'
            WHEN LOWER(hv_raw) LIKE '%diffuse sclerosing%' THEN 'diffuse_sclerosing'
            WHEN LOWER(hv_raw) LIKE '%solid%' THEN 'solid'
            WHEN LOWER(hv_raw) LIKE '%cribriform%morular%' THEN 'cribriform_morular'
            WHEN LOWER(hv_raw) LIKE '%columnar%' THEN 'columnar_cell'
            WHEN LOWER(hv_raw) LIKE '%hobnail%' THEN 'hobnail'
            WHEN LOWER(hv_raw) LIKE '%minimally invasive%' THEN 'minimally_invasive'
            WHEN LOWER(hv_raw) LIKE '%widely invasive%' THEN 'widely_invasive'
            WHEN LOWER(hv_raw) LIKE '%classic%' OR LOWER(hv_raw) LIKE '%conventional%' THEN 'classical'
            ELSE hv_raw
        END AS histology_variant_canonical,
        -- Metastatic presentation flag
        LOWER(TRIM(hb_raw)) LIKE '%metastatic%' OR LOWER(TRIM(hb_raw)) LIKE '%recurrent%' AS is_metastatic_presentation,
        hsd_raw AS histology_subtype_detail,
        TRUE AS is_malignant
    FROM raw_histology
)
SELECT
    research_id,
    histology_base_canonical,
    histology_variant_canonical,
    histology_subtype_detail,
    CASE
        WHEN histology_variant_canonical IS NOT NULL
        THEN histology_base_canonical || ', ' || REPLACE(histology_variant_canonical, '_', ' ')
        ELSE histology_base_canonical
    END AS histology_full_descriptor,
    is_malignant,
    is_metastatic_presentation,
    n_tumors,
    CASE WHEN hb2_raw IS NOT NULL AND TRIM(hb2_raw) != '' THEN
        CASE
            WHEN LOWER(TRIM(hb2_raw)) IN ('ptc','papillary thyroid carcinoma') THEN 'PTC'
            WHEN LOWER(TRIM(hb2_raw)) IN ('ftc','follicular carcinoma') THEN 'FTC'
            WHEN LOWER(TRIM(hb2_raw)) IN ('mtc','medullary thyroid carcinoma') THEN 'MTC'
            ELSE TRIM(hb2_raw)
        END
    ELSE NULL END AS tumor_2_histology_canonical,
    hb_raw AS histology_base_raw,
    hv_raw AS histology_variant_raw,
    'tumor_pathology' AS source_table
FROM standardized
"""

# ── Also pull from gold_master histology_final for patients NOT in tumor_pathology
GOLD_MALIGNANT_SQL = """
WITH tp_patients AS (
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
    FROM tumor_pathology
    WHERE tumor_1_histology_base IS NOT NULL AND TRIM(tumor_1_histology_base) != ''
),
gold_only AS (
    SELECT
        CAST(g.research_id AS VARCHAR) AS research_id,
        TRIM(g.histology_final) AS hf
    FROM gold_master_patient_facts_v1 g
    WHERE g.histology_final IS NOT NULL
      AND TRIM(g.histology_final) != ''
      AND CAST(g.research_id AS VARCHAR) NOT IN (SELECT research_id FROM tp_patients)
)
SELECT
    research_id,
    CASE
        WHEN LOWER(hf) LIKE '%ptc%' OR LOWER(hf) LIKE '%papillary%' THEN 'PTC'
        WHEN LOWER(hf) LIKE 'follicular carcinoma%' OR LOWER(hf) = 'ftc' THEN 'FTC'
        WHEN LOWER(hf) LIKE '%mtc%' OR LOWER(hf) LIKE '%medullary%' THEN 'MTC'
        WHEN LOWER(hf) LIKE '%anaplastic%' OR LOWER(hf) = 'atc' THEN 'ATC'
        WHEN LOWER(hf) LIKE '%poorly differentiated%' OR LOWER(hf) LIKE '%pdtc%' THEN 'PDTC'
        WHEN LOWER(hf) LIKE '%differentiated high grade%' OR LOWER(hf) LIKE '%dhgtc%' THEN 'DHGTC'
        WHEN LOWER(hf) LIKE '%niftp%' THEN 'NIFTP'
        WHEN LOWER(hf) LIKE '%ftump%' THEN 'FTUMP'
        WHEN LOWER(hf) LIKE '%hurthle%' OR LOWER(hf) LIKE '%oncocytic%' THEN 'HCC'
        WHEN LOWER(hf) LIKE '%follicular adenoma%' THEN 'follicular_adenoma'
        ELSE 'other_malignant'
    END AS histology_base_canonical,
    CASE
        WHEN LOWER(hf) LIKE '%tall cell%' THEN 'tall_cell'
        WHEN LOWER(hf) LIKE '%follicular variant%' THEN 'follicular_variant'
        WHEN LOWER(hf) LIKE '%classical%' OR LOWER(hf) = 'ptc' THEN 'classical'
        WHEN LOWER(hf) LIKE '%diffuse sclerosing%' THEN 'diffuse_sclerosing'
        WHEN LOWER(hf) LIKE '%columnar%' THEN 'columnar_cell'
        WHEN LOWER(hf) LIKE '%hobnail%' THEN 'hobnail'
        WHEN LOWER(hf) LIKE '%warthin%' OR LOWER(hf) LIKE '%oncocytic%variant%' THEN 'oncocytic_warthin'
        ELSE NULL
    END AS histology_variant_canonical,
    NULL AS histology_subtype_detail,
    hf AS histology_full_descriptor,
    TRUE AS is_malignant,
    LOWER(hf) LIKE '%metastatic%' OR LOWER(hf) LIKE '%recurrent%' AS is_metastatic_presentation,
    1 AS n_tumors,
    NULL AS tumor_2_histology_canonical,
    hf AS histology_base_raw,
    NULL AS histology_variant_raw,
    'gold_master_patient_facts_v1' AS source_table
FROM gold_only
"""

# ── BENIGN DIAGNOSIS STANDARDIZATION ─────────────────────────────────────────

BENIGN_SQL = """
WITH malignant_patients AS (
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
    FROM tumor_pathology
    WHERE tumor_1_histology_base IS NOT NULL AND TRIM(tumor_1_histology_base) != ''
    UNION
    SELECT DISTINCT CAST(research_id AS VARCHAR)
    FROM gold_master_patient_facts_v1
    WHERE histology_final IS NOT NULL AND TRIM(histology_final) != ''
),
benign_raw AS (
    SELECT
        CAST(ps.research_id AS VARCHAR) AS research_id,
        -- Boolean flags from path_synoptics (populated = 'x' or non-empty)
        CASE WHEN ps.multinodular_goiter IS NOT NULL AND TRIM(CAST(ps.multinodular_goiter AS VARCHAR)) != '' THEN TRUE ELSE FALSE END AS has_multinodular_goiter,
        CASE WHEN ps.substernal_multinodular_goiter IS NOT NULL AND TRIM(CAST(ps.substernal_multinodular_goiter AS VARCHAR)) != '' THEN TRUE ELSE FALSE END AS has_substernal_goiter,
        CASE WHEN ps.follicular_adenoma IS NOT NULL AND TRIM(CAST(ps.follicular_adenoma AS VARCHAR)) != '' THEN TRUE ELSE FALSE END AS has_follicular_adenoma,
        CASE WHEN ps.hurthle_cell_oncocytic_adenoma IS NOT NULL AND TRIM(CAST(ps.hurthle_cell_oncocytic_adenoma AS VARCHAR)) != '' THEN TRUE ELSE FALSE END AS has_hurthle_cell_adenoma,
        CASE WHEN ps.adenomatoid_nodules IS NOT NULL AND TRIM(CAST(ps.adenomatoid_nodules AS VARCHAR)) != '' THEN TRUE ELSE FALSE END AS has_adenomatoid_nodules,
        CASE WHEN ps.adenomatous_hyperplasia IS NOT NULL AND TRIM(CAST(ps.adenomatous_hyperplasia AS VARCHAR)) != '' THEN TRUE ELSE FALSE END AS has_adenomatous_hyperplasia,
        CASE WHEN ps.atypical_adenomas IS NOT NULL AND TRIM(CAST(ps.atypical_adenomas AS VARCHAR)) != '' THEN TRUE ELSE FALSE END AS has_atypical_adenoma,
        CASE WHEN ps.hyalinizing_trabecular_tumor_adenoma IS NOT NULL AND TRIM(CAST(ps.hyalinizing_trabecular_tumor_adenoma AS VARCHAR)) != '' THEN TRUE ELSE FALSE END AS has_hyalinizing_trabecular_tumor,
        -- Parse from path_diagnosis_summary
        CASE WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%grave%' THEN TRUE ELSE FALSE END AS has_graves_disease,
        CASE WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%hashimoto%'
              OR LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%lymphocytic thyroiditis%' THEN TRUE ELSE FALSE END AS has_hashimotos,
        CASE WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%colloid%' THEN TRUE ELSE FALSE END AS has_colloid_nodule,
        CASE WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%nodular hyperplasia%'
              OR LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%hyperplastic%' THEN TRUE ELSE FALSE END AS has_nodular_hyperplasia,
        CASE WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%parathyroid%' THEN TRUE ELSE FALSE END AS has_intrathyroidal_parathyroid,
        CAST(ps.path_diagnosis_summary AS VARCHAR) AS path_diagnosis_summary_raw,
        FALSE AS is_malignant
    FROM path_synoptics ps
    WHERE CAST(ps.research_id AS VARCHAR) NOT IN (SELECT rid FROM malignant_patients)
)
SELECT
    research_id,
    -- Primary diagnosis with hierarchy
    CASE
        WHEN has_atypical_adenoma THEN 'atypical_follicular_adenoma'
        WHEN has_follicular_adenoma THEN 'follicular_adenoma'
        WHEN has_hurthle_cell_adenoma THEN 'hurthle_cell_adenoma'
        WHEN has_hyalinizing_trabecular_tumor THEN 'hyalinizing_trabecular_tumor'
        WHEN has_substernal_goiter THEN 'substernal_multinodular_goiter'
        WHEN has_multinodular_goiter THEN 'multinodular_goiter'
        WHEN has_graves_disease THEN 'graves_disease'
        WHEN has_hashimotos THEN 'hashimotos_thyroiditis'
        WHEN has_nodular_hyperplasia THEN 'nodular_hyperplasia'
        WHEN has_adenomatous_hyperplasia THEN 'adenomatous_hyperplasia'
        WHEN has_adenomatoid_nodules THEN 'adenomatoid_nodules'
        WHEN has_colloid_nodule THEN 'colloid_nodule'
        WHEN has_intrathyroidal_parathyroid THEN 'intrathyroidal_parathyroid'
        ELSE 'other_benign'
    END AS benign_diagnosis_primary,
    has_multinodular_goiter,
    has_substernal_goiter,
    has_follicular_adenoma,
    has_hurthle_cell_adenoma,
    has_adenomatoid_nodules,
    has_adenomatous_hyperplasia,
    has_atypical_adenoma,
    has_hyalinizing_trabecular_tumor,
    has_graves_disease,
    has_hashimotos,
    has_colloid_nodule,
    has_nodular_hyperplasia,
    has_intrathyroidal_parathyroid,
    is_malignant,
    path_diagnosis_summary_raw,
    'path_synoptics' AS source_table
FROM benign_raw
"""

# ── UNIFIED TABLE ────────────────────────────────────────────────────────────

UNIFIED_SQL = """
SELECT
    research_id,
    histology_base_canonical AS diagnosis_primary,
    histology_variant_canonical AS diagnosis_variant,
    histology_full_descriptor AS diagnosis_full,
    is_malignant,
    n_tumors,
    source_table
FROM canonical_malignant_diagnosis_v1
UNION ALL
SELECT
    research_id,
    benign_diagnosis_primary AS diagnosis_primary,
    NULL AS diagnosis_variant,
    benign_diagnosis_primary AS diagnosis_full,
    FALSE AS is_malignant,
    NULL AS n_tumors,
    source_table
FROM canonical_benign_diagnosis_v1
"""


def main():
    con = connect()
    print(f"Connected to MotherDuck {DB}")

    # ── Step 1: Malignant from tumor_pathology ────────────────────────────────
    print("\n=== Step 1: Malignant histology from tumor_pathology ===")
    df_malig_tp = con.execute(MALIGNANT_SQL).fetchdf()
    print(f"  tumor_pathology malignant: {len(df_malig_tp)} rows, {df_malig_tp['research_id'].nunique()} patients")

    # ── Step 2: Malignant from gold_master only (not in tumor_pathology) ──────
    print("\n=== Step 2: Malignant from gold_master (supplemental) ===")
    df_malig_gold = con.execute(GOLD_MALIGNANT_SQL).fetchdf()
    print(f"  gold-only malignant: {len(df_malig_gold)} rows")

    # Combine — deduplicate by research_id (prefer tumor_pathology)
    df_malignant = pd.concat([df_malig_tp, df_malig_gold], ignore_index=True)
    df_malignant = df_malignant.drop_duplicates(subset=["research_id"], keep="first")
    print(f"  Combined malignant: {len(df_malignant)} rows, {df_malignant['research_id'].nunique()} patients")

    print("\n  Base histology distribution:")
    dist = df_malignant["histology_base_canonical"].value_counts()
    for k, v in dist.items():
        print(f"    {k}: {v}")

    print("\n  Variant distribution (malignant):")
    vdist = df_malignant["histology_variant_canonical"].value_counts(dropna=False)
    for k, v in vdist.head(15).items():
        print(f"    {k}: {v}")

    # Save
    out_malig = OUTPUT_DIR / "canonical_malignant_diagnosis_v1.parquet"
    df_malignant.to_parquet(out_malig, index=False)
    print(f"\n  Saved: {out_malig}")

    # ── Step 3: Benign ────────────────────────────────────────────────────────
    print("\n=== Step 3: Benign diagnoses from path_synoptics ===")
    df_benign = con.execute(BENIGN_SQL).fetchdf()
    print(f"  Benign: {len(df_benign)} rows, {df_benign['research_id'].nunique()} patients")

    print("\n  Benign diagnosis distribution:")
    bdist = df_benign["benign_diagnosis_primary"].value_counts()
    for k, v in bdist.items():
        print(f"    {k}: {v}")

    # Save
    out_benign = OUTPUT_DIR / "canonical_benign_diagnosis_v1.parquet"
    df_benign.to_parquet(out_benign, index=False)
    print(f"\n  Saved: {out_benign}")

    # ── Step 4: Unified ──────────────────────────────────────────────────────
    print("\n=== Step 4: Building unified diagnosis table ===")

    # Upload malignant and benign first, then build unified
    con.execute("CREATE OR REPLACE TABLE canonical_malignant_diagnosis_v1 AS SELECT * FROM read_parquet(?)", [str(out_malig)])
    con.execute("CREATE OR REPLACE TABLE canonical_benign_diagnosis_v1 AS SELECT * FROM read_parquet(?)", [str(out_benign)])

    con.execute(f"CREATE OR REPLACE TABLE canonical_diagnosis_unified_v1 AS {UNIFIED_SQL}")
    df_unified = con.execute("SELECT * FROM canonical_diagnosis_unified_v1").fetchdf()
    print(f"  Unified: {len(df_unified)} rows, {df_unified['research_id'].nunique()} patients")

    # Save
    out_unified = OUTPUT_DIR / "canonical_diagnosis_unified_v1.parquet"
    df_unified.to_parquet(out_unified, index=False)
    print(f"  Saved: {out_unified}")

    # ── Step 5: Validation ───────────────────────────────────────────────────
    print("\n=== Step 5: Validation ===")

    gold_count = con.execute("SELECT COUNT(DISTINCT research_id) FROM gold_master_patient_facts_v1").fetchone()[0]
    unified_count = df_unified["research_id"].nunique()
    print(f"  Gold master patients: {gold_count}")
    print(f"  Unified patients:     {unified_count}")

    # Check for missing patients
    missing = con.execute("""
        SELECT CAST(g.research_id AS VARCHAR) AS rid
        FROM gold_master_patient_facts_v1 g
        WHERE CAST(g.research_id AS VARCHAR) NOT IN (
            SELECT research_id FROM canonical_diagnosis_unified_v1
        )
    """).fetchdf()
    print(f"  Patients in gold but NOT in unified: {len(missing)}")

    if len(missing) > 0:
        print("  ⚠ Missing patients — attempting to fill from path_synoptics catch-all...")
        # These are patients with NEITHER tumor_pathology NOR clear benign flags
        # but they exist in gold_master (likely via path_synoptics with only path_diagnosis_summary)
        catchall_sql = f"""
        INSERT INTO canonical_diagnosis_unified_v1
        SELECT
            CAST(ps.research_id AS VARCHAR) AS research_id,
            CASE
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%carcinoma%'
                     OR LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%cancer%'
                     OR LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%malignant%'
                THEN 'other_malignant'
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%grave%' THEN 'graves_disease'
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%hashimoto%' THEN 'hashimotos_thyroiditis'
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%goiter%' THEN 'multinodular_goiter'
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%hyperplasia%' THEN 'nodular_hyperplasia'
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%adenoma%' THEN 'follicular_adenoma'
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%colloid%' THEN 'colloid_nodule'
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%thyroiditis%' THEN 'hashimotos_thyroiditis'
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%nodule%' THEN 'adenomatoid_nodules'
                ELSE 'unclassified_benign'
            END AS diagnosis_primary,
            NULL AS diagnosis_variant,
            COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR), 'unclassified') AS diagnosis_full,
            CASE
                WHEN LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%carcinoma%'
                     OR LOWER(COALESCE(CAST(ps.path_diagnosis_summary AS VARCHAR),'')) LIKE '%cancer%'
                THEN TRUE
                ELSE FALSE
            END AS is_malignant,
            NULL AS n_tumors,
            'path_synoptics_catchall' AS source_table
        FROM path_synoptics ps
        WHERE CAST(ps.research_id AS VARCHAR) IN ({','.join("'" + str(r) + "'" for r in missing['rid'].tolist())})
        """
        if len(missing) <= 2000:
            con.execute(catchall_sql)
            new_total = con.execute("SELECT COUNT(DISTINCT research_id) FROM canonical_diagnosis_unified_v1").fetchone()[0]
            print(f"  After catchall: {new_total} patients")
        else:
            print("  Too many missing — skipping inline catchall, investigate manually")

    # Final distribution
    print("\n  === FINAL DISTRIBUTION ===")
    final = con.execute("""
        SELECT diagnosis_primary, is_malignant, COUNT(*) AS cnt
        FROM canonical_diagnosis_unified_v1
        GROUP BY 1, 2
        ORDER BY cnt DESC
    """).fetchdf()
    print(final.to_string(index=False))

    malignant_n = con.execute("SELECT COUNT(*) FROM canonical_diagnosis_unified_v1 WHERE is_malignant = TRUE").fetchone()[0]
    benign_n = con.execute("SELECT COUNT(*) FROM canonical_diagnosis_unified_v1 WHERE is_malignant = FALSE").fetchone()[0]
    total = con.execute("SELECT COUNT(DISTINCT research_id) FROM canonical_diagnosis_unified_v1").fetchone()[0]
    print(f"\n  Summary: {malignant_n} malignant + {benign_n} benign = {malignant_n + benign_n} total ({total} unique patients)")
    print(f"  Target: {gold_count} (gold master)")

    # Re-export final unified from MD
    df_final = con.execute("SELECT * FROM canonical_diagnosis_unified_v1").fetchdf()
    df_final.to_parquet(out_unified, index=False)

    print("\n✓ Prompt 1 COMPLETE — canonical_diagnosis_unified_v1 uploaded to MotherDuck")
    con.close()


if __name__ == "__main__":
    main()
