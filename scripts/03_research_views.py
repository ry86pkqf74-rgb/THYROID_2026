#!/usr/bin/env python3
"""
03_research_views.py

Phase 2 research-facing analytic views for thyroid cancer studies.
"""

import argparse
import os
from pathlib import Path
import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
MD_DATABASE = "thyroid_research_2026"


VIEWS_SQL: dict[str, str] = {
    "ptc_cohort": """
CREATE OR REPLACE VIEW ptc_cohort AS
SELECT
    tp.research_id,
    mc.surgery_date,
    mc.age_at_surgery,
    mc.sex,
    tp.histology_1_type,
    tp.histology_1_t_stage_ajcc8 AS t_stage_ajcc8,
    tp.histology_1_n_stage_ajcc8 AS n_stage_ajcc8,
    tp.histology_1_m_stage_ajcc8 AS m_stage_ajcc8,
    tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
    TRY_CAST(tp.histology_1_ln_examined AS DOUBLE) AS ln_examined,
    TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS ln_positive,
    ps.tumor_1_extrathyroidal_extension AS tumor_1_extrathyroidal_ext,
    ps.path_extended_gross_path AS tumor_1_gross_ete,
    ps.tumor_1_capsular_invasion AS tumor_1_ete_microscopic_only,
    tp.tumor_1_histology_variant,
    tp.variant_standardized,
    tp.surgery_type_normalized
FROM tumor_pathology tp
LEFT JOIN master_cohort mc ON mc.research_id = tp.research_id
LEFT JOIN path_synoptics ps ON ps.research_id = tp.research_id
WHERE UPPER(COALESCE(tp.histology_1_type, '')) = 'PTC'
  AND (
        LOWER(COALESCE(tp.tumor_1_histology_variant, '')) LIKE '%classic%'
        OR LOWER(COALESCE(tp.variant_standardized, '')) LIKE '%classic%'
        OR (
            COALESCE(tp.tumor_1_histology_variant, '') = ''
            AND COALESCE(tp.variant_standardized, '') = ''
        )
      )
""",
    "recurrence_risk_cohort": """
CREATE OR REPLACE VIEW recurrence_risk_cohort AS
WITH tg AS (
    SELECT
        llv.research_id,
        llv.cleaned_numeric_result AS tg_value,
        llv.original_result AS tg_original_result,
        llv.specimen_ts AS tg_ts
    FROM longitudinal_lab_view llv
    WHERE llv.lab_type = 'thyroglobulin'
),
tg_clean AS (
    SELECT *
    FROM tg
    WHERE tg_value IS NOT NULL
),
tg_agg AS (
    SELECT
        research_id,
        COUNT(*) AS n_tg_measurements,
        MIN(tg_value) AS tg_min,
        MAX(tg_value) AS tg_max,
        AVG(tg_value) AS tg_mean,
        MIN(tg_ts) AS tg_first_date,
        MAX(tg_ts) AS tg_last_date,
        arg_min(tg_value, tg_ts) AS tg_first_value,
        arg_max(tg_value, tg_ts) AS tg_last_value,
        arg_min(tg_original_result, tg_ts) AS tg_first_original_result,
        arg_max(tg_original_result, tg_ts) AS tg_last_original_result
    FROM tg_clean
    GROUP BY research_id
)
SELECT
    mc.research_id,
    mc.age_at_surgery,
    mc.sex,
    mc.surgery_date,
    tp.histology_1_type,
    tp.histology_1_t_stage_ajcc8 AS t_stage_ajcc8,
    tp.histology_1_n_stage_ajcc8 AS n_stage_ajcc8,
    tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
    tp.tumor_1_extrathyroidal_ext,
    tp.tumor_1_gross_ete,
    tp.variant_standardized,
    tp.surgery_type_normalized,
    tg_agg.n_tg_measurements,
    tg_agg.tg_first_date,
    tg_agg.tg_last_date,
    tg_agg.tg_first_value,
    tg_agg.tg_last_value,
    tg_agg.tg_first_original_result,
    tg_agg.tg_last_original_result,
    tg_agg.tg_min,
    tg_agg.tg_max,
    tg_agg.tg_mean,
    CASE
        WHEN tg_agg.n_tg_measurements >= 2
        THEN (tg_agg.tg_last_value - tg_agg.tg_first_value) / NULLIF(tg_agg.n_tg_measurements - 1, 0)
        ELSE NULL
    END AS tg_delta_per_measurement,
    CASE
        WHEN UPPER(COALESCE(tp.histology_1_overall_stage_ajcc8, '')) IN ('III', 'IVA', 'IVB')
             OR LOWER(COALESCE(CAST(tp.tumor_1_gross_ete AS VARCHAR), '')) IN ('true', 'yes', '1')
             OR COALESCE(tg_agg.tg_max, 0) >= 10
        THEN 'high'
        WHEN UPPER(COALESCE(tp.histology_1_overall_stage_ajcc8, '')) IN ('II')
             OR COALESCE(tg_agg.tg_max, 0) >= 2
        THEN 'intermediate'
        ELSE 'low'
    END AS recurrence_risk_band
FROM master_cohort mc
LEFT JOIN tumor_pathology tp ON mc.research_id = tp.research_id
LEFT JOIN tg_agg ON mc.research_id = tg_agg.research_id
WHERE mc.has_tumor_pathology = TRUE
""",
    "imaging_pathology_correlation": """
CREATE OR REPLACE VIEW imaging_pathology_correlation AS
WITH us AS (
    SELECT
        research_id,
        COUNT(*) AS us_count,
        MAX(TRY_CAST(ultrasound_date AS TIMESTAMP)) AS us_last_date,
        MAX(
            GREATEST(
                COALESCE(TRY_CAST(nodule_1_ti_rads AS DOUBLE), 0),
                COALESCE(TRY_CAST(nodule_2_ti_rads AS DOUBLE), 0),
                COALESCE(TRY_CAST(nodule_3_ti_rads AS DOUBLE), 0),
                COALESCE(TRY_CAST(nodule_4_ti_rads AS DOUBLE), 0),
                COALESCE(TRY_CAST(nodule_5_ti_rads AS DOUBLE), 0)
            )
        ) AS max_tirads
    FROM ultrasound_reports
    GROUP BY research_id
),
ct AS (
    SELECT
        research_id,
        COUNT(*) AS ct_count,
        MAX(CASE WHEN LOWER(CAST(thyroid_nodule AS VARCHAR)) IN ('true', 'yes', '1') THEN 1 ELSE 0 END) AS ct_nodule_flag,
        MAX(CASE WHEN LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) IN ('true', 'yes', '1') THEN 1 ELSE 0 END) AS ct_pathologic_ln_flag
    FROM ct_imaging
    GROUP BY research_id
),
mri AS (
    SELECT
        research_id,
        COUNT(*) AS mri_count,
        MAX(CASE WHEN LOWER(CAST(thyroid_nodule AS VARCHAR)) IN ('true', 'yes', '1') THEN 1 ELSE 0 END) AS mri_nodule_flag,
        MAX(CASE WHEN LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) IN ('true', 'yes', '1') THEN 1 ELSE 0 END) AS mri_pathologic_ln_flag
    FROM mri_imaging
    GROUP BY research_id
)
SELECT
    mc.research_id,
    tp.histology_1_type,
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
    tp.histology_1_n_stage_ajcc8 AS n_stage_ajcc8,
    COALESCE(us.us_count, 0) AS us_count,
    COALESCE(ct.ct_count, 0) AS ct_count,
    COALESCE(mri.mri_count, 0) AS mri_count,
    us.max_tirads,
    COALESCE(ct.ct_nodule_flag, 0) AS ct_nodule_flag,
    COALESCE(ct.ct_pathologic_ln_flag, 0) AS ct_pathologic_ln_flag,
    COALESCE(mri.mri_nodule_flag, 0) AS mri_nodule_flag,
    COALESCE(mri.mri_pathologic_ln_flag, 0) AS mri_pathologic_ln_flag
FROM master_cohort mc
LEFT JOIN tumor_pathology tp ON mc.research_id = tp.research_id
LEFT JOIN us ON mc.research_id = us.research_id
LEFT JOIN ct ON mc.research_id = ct.research_id
LEFT JOIN mri ON mc.research_id = mri.research_id
WHERE mc.has_tumor_pathology = TRUE
""",
    "fna_accuracy_view": """
CREATE OR REPLACE VIEW fna_accuracy_view AS
WITH final_dx AS (
    SELECT
        mc.research_id,
        CASE
            WHEN mc.has_tumor_pathology = TRUE THEN 1
            WHEN mc.has_tumor_pathology = FALSE AND mc.has_benign_pathology = TRUE THEN 0
            ELSE NULL
        END AS final_malignant
    FROM master_cohort mc
),
fna_scored AS (
    SELECT
        f.research_id,
        f.fna_index,
        f.fna_date,
        f.bethesda_2023_num,
        f.bethesda_2023_name,
        TRY_CAST(f.bethesda_2023_num AS DOUBLE) AS bethesda_num,
        CASE
            WHEN TRY_CAST(f.bethesda_2023_num AS DOUBLE) >= 5 THEN 1
            WHEN TRY_CAST(f.bethesda_2023_num AS DOUBLE) < 5 THEN 0
            ELSE NULL
        END AS fna_test_positive
    FROM fna_cytology f
)
SELECT
    fs.research_id,
    fs.fna_index,
    fs.fna_date,
    fs.bethesda_2023_num,
    fs.bethesda_2023_name,
    fs.fna_test_positive,
    dx.final_malignant,
    CASE
        WHEN fs.fna_test_positive = 1 AND dx.final_malignant = 1 THEN 'TP'
        WHEN fs.fna_test_positive = 1 AND dx.final_malignant = 0 THEN 'FP'
        WHEN fs.fna_test_positive = 0 AND dx.final_malignant = 1 THEN 'FN'
        WHEN fs.fna_test_positive = 0 AND dx.final_malignant = 0 THEN 'TN'
        ELSE NULL
    END AS confusion_class
FROM fna_scored fs
LEFT JOIN final_dx dx ON fs.research_id = dx.research_id
WHERE dx.final_malignant IS NOT NULL
""",
    "lymph_node_metastasis_view": """
CREATE OR REPLACE VIEW lymph_node_metastasis_view AS
SELECT
    research_id,
    histology_1_type,
    TRY_CAST(histology_1_ln_examined AS DOUBLE) AS ln_examined_total,
    TRY_CAST(histology_1_ln_positive AS DOUBLE) AS ln_positive_total,
    TRY_CAST(ln_level_i_examined AS DOUBLE) AS ln_level_i_examined,
    TRY_CAST(ln_level_i_positive AS DOUBLE) AS ln_level_i_positive,
    TRY_CAST(ln_level_ii_examined AS DOUBLE) AS ln_level_ii_examined,
    TRY_CAST(ln_level_ii_positive AS DOUBLE) AS ln_level_ii_positive,
    TRY_CAST(ln_level_iii_examined AS DOUBLE) AS ln_level_iii_examined,
    TRY_CAST(ln_level_iii_positive AS DOUBLE) AS ln_level_iii_positive,
    TRY_CAST(ln_level_iv_examined AS DOUBLE) AS ln_level_iv_examined,
    TRY_CAST(ln_level_iv_positive AS DOUBLE) AS ln_level_iv_positive,
    TRY_CAST(ln_level_v_examined AS DOUBLE) AS ln_level_v_examined,
    TRY_CAST(ln_level_v_positive AS DOUBLE) AS ln_level_v_positive,
    TRY_CAST(ln_level_vi_examined AS DOUBLE) AS ln_level_vi_examined,
    TRY_CAST(ln_level_vi_positive AS DOUBLE) AS ln_level_vi_positive,
    TRY_CAST(ln_level_vii_examined AS DOUBLE) AS ln_level_vii_examined,
    TRY_CAST(ln_level_vii_positive AS DOUBLE) AS ln_level_vii_positive,
    TRY_CAST(histology_1_ln_ratio AS DOUBLE) AS ln_ratio,
    histology_1_ln_extranodal_extension
FROM tumor_pathology
WHERE TRY_CAST(histology_1_ln_examined AS DOUBLE) IS NOT NULL
   OR TRY_CAST(histology_1_ln_positive AS DOUBLE) IS NOT NULL
""",
    "benign_vs_malignant_comparison": """
CREATE OR REPLACE VIEW benign_vs_malignant_comparison AS
SELECT
    mc.research_id,
    mc.age_at_surgery,
    mc.sex,
    mc.surgery_date,
    CASE
        WHEN mc.has_tumor_pathology = TRUE THEN 'malignant'
        WHEN mc.has_tumor_pathology = FALSE AND mc.has_benign_pathology = TRUE THEN 'benign'
        ELSE 'unknown'
    END AS disease_group,
    tp.histology_1_type,
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
    tp.histology_1_overall_stage_ajcc8,
    bp.hashimoto_thyroiditis,
    bp.graves_disease,
    bp.multinodular_goiter,
    bp.follicular_adenoma,
    bp.is_mng,
    bp.is_graves,
    bp.is_follicular_adenoma,
    bp.is_hurthle_adenoma,
    bp.is_hashimoto,
    bp.is_hyalinizing_trabecular,
    bp.is_tgdc,
    bp.surgery_type_normalized AS benign_surgery_type,
    tp.variant_standardized,
    tp.surgery_type_normalized AS malignant_surgery_type,
    mc.has_fna_cytology,
    mc.has_ultrasound_reports,
    mc.has_ct_imaging,
    mc.has_mri_imaging,
    mc.has_nuclear_med,
    mc.has_thyroglobulin_labs,
    mc.has_anti_thyroglobulin_labs
FROM master_cohort mc
LEFT JOIN tumor_pathology tp ON mc.research_id = tp.research_id
LEFT JOIN benign_pathology bp ON mc.research_id = bp.research_id
WHERE mc.has_tumor_pathology = TRUE
   OR mc.has_benign_pathology = TRUE
""",
    "longitudinal_lab_view": """
CREATE OR REPLACE VIEW longitudinal_lab_view AS
WITH labs AS (
    SELECT
        research_id,
        lab_type,
        lab_index,
        test_name,
        TRY_CAST(specimen_collect_dt AS TIMESTAMP) AS specimen_ts,
        CAST(result AS VARCHAR) AS original_result,
        units,
        CASE
            WHEN regexp_matches(trim(CAST(result AS VARCHAR)), '^[<>]=?')
            THEN regexp_extract(trim(CAST(result AS VARCHAR)), '^([<>]=?)', 1)
            ELSE NULL
        END AS inequality_symbol,
        TRY_CAST(
            regexp_extract(CAST(result AS VARCHAR), '(-?[0-9]+(?:\\.[0-9]+)?)', 1)
            AS DOUBLE
        ) AS cleaned_numeric_result
    FROM lab_timeline
    WHERE lab_type IN ('thyroglobulin', 'anti_thyroglobulin')
),
base AS (
    SELECT
        *,
        MIN(specimen_ts) OVER (PARTITION BY research_id, lab_type) AS first_specimen_ts
    FROM labs
)
SELECT
    research_id,
    lab_type,
    lab_index,
    test_name,
    specimen_ts,
    original_result,
    units,
    inequality_symbol,
    cleaned_numeric_result,
    cleaned_numeric_result AS numeric_result,
    datediff('day', first_specimen_ts, specimen_ts) AS days_from_first_lab
FROM base
WHERE specimen_ts IS NOT NULL
ORDER BY research_id, lab_type, specimen_ts
""",
    "data_completeness_by_year": """
CREATE OR REPLACE VIEW data_completeness_by_year AS
WITH base AS (
    SELECT
        TRY_CAST(surgery_date AS DATE) AS surgery_dt,
        has_tumor_pathology,
        has_benign_pathology,
        has_fna_cytology,
        has_ultrasound_reports,
        has_ct_imaging,
        has_mri_imaging,
        has_nuclear_med,
        has_thyroglobulin_labs,
        has_anti_thyroglobulin_labs
    FROM master_cohort
),
yeared AS (
    SELECT
        EXTRACT(YEAR FROM surgery_dt) AS surgery_year,
        *
    FROM base
    WHERE surgery_dt IS NOT NULL
)
SELECT
    surgery_year,
    COUNT(*) AS n_patients,
    SUM(CASE WHEN has_tumor_pathology THEN 1 ELSE 0 END) AS n_tumor_pathology,
    SUM(CASE WHEN has_benign_pathology THEN 1 ELSE 0 END) AS n_benign_pathology,
    SUM(CASE WHEN has_fna_cytology THEN 1 ELSE 0 END) AS n_fna_cytology,
    SUM(CASE WHEN has_ultrasound_reports THEN 1 ELSE 0 END) AS n_ultrasound_reports,
    SUM(CASE WHEN has_ct_imaging THEN 1 ELSE 0 END) AS n_ct_imaging,
    SUM(CASE WHEN has_mri_imaging THEN 1 ELSE 0 END) AS n_mri_imaging,
    SUM(CASE WHEN has_nuclear_med THEN 1 ELSE 0 END) AS n_nuclear_med,
    SUM(CASE WHEN has_thyroglobulin_labs THEN 1 ELSE 0 END) AS n_thyroglobulin_labs,
    SUM(CASE WHEN has_anti_thyroglobulin_labs THEN 1 ELSE 0 END) AS n_anti_thyroglobulin_labs,
    ROUND(100.0 * SUM(CASE WHEN has_tumor_pathology THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_tumor_pathology,
    ROUND(100.0 * SUM(CASE WHEN has_fna_cytology THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_fna_cytology,
    ROUND(100.0 * SUM(CASE WHEN has_ultrasound_reports THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_ultrasound_reports,
    ROUND(100.0 * SUM(CASE WHEN has_thyroglobulin_labs THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_thyroglobulin_labs
FROM yeared
GROUP BY surgery_year
ORDER BY surgery_year
""",
    "advanced_features_v3": """
CREATE OR REPLACE VIEW advanced_features_v3 AS
SELECT
    mc.research_id,
    mc.age_at_surgery,
    mc.sex,
    mc.surgery_date,
    -- Phase 4 standardized columns (from tumor_pathology)
    tp.histology_1_type,
    tp.variant_standardized,
    tp.surgery_type_normalized AS malignant_surgery_type,
    tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
    tp.tumor_focality_overall,
    TRY_CAST(ps.tumor_focality AS VARCHAR) AS num_tumors,
    -- Invasion (from path_synoptics — tumor_pathology lacks these columns locally)
    ps.tumor_1_angioinvasion        AS tumor_1_vascular_invasion,
    ps.tumor_1_lymphatic_invasion,
    ps.tumor_1_perineural_invasion,
    ps.tumor_1_capsular_invasion,
    -- ETE (from path_synoptics)
    ps.tumor_1_extrathyroidal_extension AS tumor_1_extrathyroidal_ext,
    ps.path_extended_gross_path         AS tumor_1_gross_ete,
    NULL::VARCHAR                        AS tumor_1_ete_microscopic_only,
    -- LN summary (from path_synoptics)
    TRY_CAST(ps.tumor_1_ln_examined AS DOUBLE) AS ln_examined,
    TRY_CAST(ps.tumor_1_ln_involved AS DOUBLE) AS ln_positive,
    CASE
        WHEN TRY_CAST(ps.tumor_1_ln_examined AS DOUBLE) > 0
        THEN ROUND(TRY_CAST(ps.tumor_1_ln_involved AS DOUBLE) /
                   TRY_CAST(ps.tumor_1_ln_examined AS DOUBLE), 4)
        ELSE NULL
    END AS ln_ratio,
    -- Phase 5 mutation flags (from tumor_pathology where present)
    tp.braf_mutation_mentioned,
    tp.ras_mutation_mentioned,
    tp.ret_mutation_mentioned,
    tp.tert_mutation_mentioned,
    NULL::BOOLEAN AS ntrk_mutation_mentioned,
    NULL::BOOLEAN AS alk_mutation_mentioned,
    -- Benign flags
    bp.is_mng,
    bp.is_graves,
    bp.is_follicular_adenoma,
    bp.is_hurthle_adenoma,
    bp.is_hashimoto,
    bp.is_hyalinizing_trabecular,
    bp.is_tgdc,
    bp.surgery_type_normalized AS benign_surgery_type,
    -- Data availability
    mc.has_tumor_pathology,
    mc.has_benign_pathology,
    mc.has_fna_cytology,
    mc.has_ultrasound_reports,
    mc.has_ct_imaging,
    mc.has_mri_imaging,
    mc.has_nuclear_med,
    mc.has_thyroglobulin_labs,
    mc.has_anti_thyroglobulin_labs,
    mc.has_parathyroid
FROM master_cohort mc
LEFT JOIN tumor_pathology tp ON mc.research_id = tp.research_id
LEFT JOIN path_synoptics  ps ON ps.research_id = mc.research_id
LEFT JOIN benign_pathology bp ON mc.research_id = bp.research_id
""",
}


def _connect(use_md: bool) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection — MotherDuck RW when --md, otherwise local."""
    if use_md:
        token = os.environ.get("MOTHERDUCK_TOKEN") or ""
        if not token:
            # Try loading from .streamlit/secrets.toml
            try:
                import tomllib  # Python 3.11+
            except ImportError:
                import tomli as tomllib  # type: ignore
            secrets_path = ROOT / ".streamlit" / "secrets.toml"
            with open(secrets_path, "rb") as f:
                token = tomllib.load(f).get("MOTHERDUCK_TOKEN", "")
        if not token:
            raise RuntimeError("MOTHERDUCK_TOKEN not set and not found in secrets.toml")
        con = duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")
        con.execute(f"USE {MD_DATABASE}")
        print(f"Connected to MotherDuck: {MD_DATABASE}")
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"Connected to local: {DB_PATH}")
    return con


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Phase 2 research views")
    parser.add_argument("--md", action="store_true", help="Target MotherDuck instead of local DuckDB")
    args = parser.parse_args()

    con = _connect(args.md)

    print("=" * 72)
    print("PHASE 2: Creating thyroid cancer research views")
    print("=" * 72)

    creation_order = [
        "ptc_cohort",
        "longitudinal_lab_view",
        "recurrence_risk_cohort",
        "imaging_pathology_correlation",
        "fna_accuracy_view",
        "lymph_node_metastasis_view",
        "benign_vs_malignant_comparison",
        "data_completeness_by_year",
        "advanced_features_v3",
    ]

    for name in creation_order:
        sql = VIEWS_SQL[name]
        con.execute(sql)
        n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"✅ {name:35s} rows={n:,}")

    # Backward-compat alias so legacy references to advanced_features_view
    # continue to work alongside the canonical advanced_features_v3 name.
    con.execute(
        "CREATE OR REPLACE VIEW advanced_features_view AS "
        "SELECT * FROM advanced_features_v3;"
    )
    print(f"{'advanced_features_view (alias)':35s} ✓")

    # Clean genetics view: adds normalized test_platform + result_category columns
    # derived from the raw Excel-flattened columns ingested by script 07.
    # genetic_testing may not exist yet on a fresh local DB, so guard gracefully.
    try:
        con.execute("""
            CREATE OR REPLACE VIEW genetic_testing_clean AS
            SELECT
                *,
                COALESCE(
                    "Genetic Test Performed_1",
                    "Thyroseq/Afirma_1",
                    "Thyroseq/Afirma_2",
                    "Thyroseq/Afirma_3",
                    'Unknown'
                ) AS test_platform,
                COALESCE(
                    "Detailed findings_1",
                    "Detailed findings_3",
                    "Genetic_test_2",
                    'Unknown'
                ) AS result_category
            FROM genetic_testing;
        """)
        print(f"{'genetic_testing_clean (view)':35s} ✓")
    except Exception as exc:
        print(f"{'genetic_testing_clean':35s} skipped ({exc})")

    # =========================================================================
    # MOTHERDUCK OPTIMIZATION LAYER (added 2026-03-10)
    # Materialized tables for sub-second dashboard performance.
    # Safe to re-run: all use CREATE OR REPLACE.
    # =========================================================================
    print("-" * 72)
    print("Creating MotherDuck optimization tables…")

    # 1. Core wide table — physically sorted for common filters + stats.
    #    advanced_features_v3 was just created above, so no circular ref.
    try:
        con.execute("""
            CREATE OR REPLACE TABLE advanced_features_sorted AS
            SELECT * FROM advanced_features_v3
            ORDER BY surgery_date DESC NULLS LAST,
                     histology_1_type,
                     overall_stage_ajcc8,
                     braf_mutation_mentioned DESC NULLS LAST,
                     research_id;
        """)
        n = con.execute("SELECT COUNT(*) FROM advanced_features_sorted").fetchone()[0]
        print(f"{'advanced_features_sorted':35s} rows={n:,}")
    except Exception as exc:
        print(f"{'advanced_features_sorted':35s} skipped ({exc})")

    # 2. Overview KPIs — tiny pre-computed table; one row only.
    try:
        con.execute("""
            CREATE OR REPLACE TABLE overview_kpis AS
            SELECT
                COUNT(*)                                                    AS total_patients,
                COUNT(CASE WHEN histology_1_type = 'PTC' THEN 1 END)       AS ptc_count,
                AVG(age_at_surgery) FILTER (WHERE age_at_surgery IS NOT NULL) AS avg_age,
                COUNT(CASE WHEN braf_mutation_mentioned THEN 1 END)         AS braf_positive,
                MAX(EXTRACT(YEAR FROM TRY_CAST(surgery_date AS DATE)))      AS max_year,
                MIN(EXTRACT(YEAR FROM TRY_CAST(surgery_date AS DATE)))      AS min_year,
                COUNT(DISTINCT research_id)                                 AS unique_patients
            FROM advanced_features_sorted;
        """)
        print(f"{'overview_kpis':35s} ✓")
    except Exception as exc:
        print(f"{'overview_kpis':35s} skipped ({exc})")

    # 3. Genetics summary — pre-grouped for platform + result pies.
    try:
        con.execute("""
            CREATE OR REPLACE TABLE genetics_summary AS
            SELECT
                COALESCE(
                    "Genetic Test Performed_1",
                    "Thyroseq/Afirma_1",
                    "Thyroseq/Afirma_2",
                    "Thyroseq/Afirma_3",
                    'Unknown'
                ) AS test_platform,
                COALESCE(
                    "Detailed findings_1",
                    "Detailed findings_3",
                    "Genetic_test_2",
                    'Unknown'
                ) AS result_category,
                COUNT(*) AS n
            FROM genetic_testing
            GROUP BY 1, 2;
        """)
        n = con.execute("SELECT COUNT(*) FROM genetics_summary").fetchone()[0]
        print(f"{'genetics_summary':35s} rows={n:,}")
    except Exception as exc:
        print(f"{'genetics_summary':35s} skipped ({exc})")

    # 4. Pathology summary — pre-aggregated capsular/invasion + margin counts.
    #    Uses a LEFT JOIN on research_id (no CROSS JOIN).
    try:
        con.execute("""
            CREATE OR REPLACE TABLE pathology_summary AS
            SELECT
                tw.research_id,
                COALESCE(TRY_CAST(tw."right_lobe_g" AS DOUBLE), 0)  AS right_lobe_weight,
                COALESCE(TRY_CAST(tw."left_lobe_g"  AS DOUBLE), 0)  AS left_lobe_weight,
                COALESCE(
                    CAST(tp.tumor_1_capsular_invasion AS VARCHAR),
                    CAST(tp.tumor_2_capsular_invasion AS VARCHAR),
                    CAST(tp.tumor_3_capsular_invasion AS VARCHAR),
                    CAST(tp.tumor_4_capsular_invasion AS VARCHAR),
                    CAST(tp.tumor_5_capsular_invasion AS VARCHAR),
                    'Unknown'
                ) AS capsular_invasion,
                COALESCE(
                    CAST(tp.tumor_1_margin_status AS VARCHAR),
                    'Unknown'
                ) AS surgical_margins
            FROM thyroid_weights tw
            LEFT JOIN tumor_pathology tp ON tw.research_id = tp.research_id;
        """)
        n = con.execute("SELECT COUNT(*) FROM pathology_summary").fetchone()[0]
        print(f"{'pathology_summary':35s} rows={n:,}")
    except Exception as exc:
        print(f"{'pathology_summary':35s} skipped ({exc})")

    # 5. Refresh backward-compat aliases so nothing breaks.
    #    advanced_features_sorted is now the physical table; both views
    #    point to it so reads skip the live view scan entirely.
    #    DROP TABLE first in case the name was previously a table.
    try:
        con.execute(
            "CREATE OR REPLACE VIEW advanced_features_view AS "
            "SELECT * FROM advanced_features_sorted;"
        )
        print(f"{'advanced_features_view (alias)':35s} ✓")
    except Exception as exc:
        print(f"{'advanced_features_view alias':35s} skipped ({exc})")
    try:
        con.execute("DROP VIEW IF EXISTS advanced_features_v3;")
        con.execute(
            "CREATE OR REPLACE VIEW advanced_features_v3 AS "
            "SELECT * FROM advanced_features_sorted;"
        )
        print(f"{'advanced_features_v3 (alias)':35s} ✓")
    except Exception as exc:
        print(f"{'advanced_features_v3 alias':35s} skipped ({exc})")

    # ── STREAMLIT-SPECIFIC TABLES (safe fallback for deployment errors) ──
    #
    # These materialized tables resolve the "Required view not available"
    # error on Streamlit Cloud.  Script 18 creates the full adjudication
    # versions; these safe versions use only columns present in the base
    # episode tables so they succeed even without the full 15→19 chain.
    print("-" * 72)
    print("STREAMLIT SUPPORT TABLES")

    try:
        con.execute("""
            CREATE OR REPLACE TABLE streamlit_patient_header_v AS
            SELECT
                s.research_id,
                s.histology_normalized AS primary_histology,
                s.variant_normalized AS primary_variant,
                s.t_stage_reconciled AS primary_t_stage,
                s.histology_status,
                s.highest_severity AS overall_severity,
                CASE
                    WHEN s.total_issues >= 5 THEN 'critical'
                    WHEN s.total_issues >= 3 THEN 'high'
                    WHEN s.total_issues >= 1 THEN 'medium'
                    ELSE 'low'
                END AS review_priority_tier,
                s.total_issues AS total_review_items,
                CASE WHEN s.histology_status IS NOT NULL
                     THEN TRUE ELSE FALSE END AS histology_analysis_eligible,
                s.has_high_risk_molecular AS has_eligible_molecular,
                s.has_definite_rai AS has_eligible_rai,
                s.molecular_test_count,
                s.rai_episode_count,
                s.total_issues AS total_validation_issues
            FROM patient_reconciliation_summary_v s;
        """)
        n = con.execute(
            "SELECT COUNT(*) FROM streamlit_patient_header_v"
        ).fetchone()[0]
        print(f"{'streamlit_patient_header_v':35s} rows={n:,}")
    except Exception as exc:
        print(f"{'streamlit_patient_header_v':35s} skipped ({exc})")

    try:
        con.execute("""
            CREATE OR REPLACE TABLE streamlit_cohort_qc_summary_v AS
            WITH total_patients AS (
                SELECT COUNT(DISTINCT research_id) AS n
                FROM patient_reconciliation_summary_v
            )
            SELECT
                (SELECT COUNT(*) FROM histology_analysis_cohort_v
                 WHERE discordance_type IS NOT NULL) AS histology_discordant,
                (SELECT COUNT(*) FROM histology_analysis_cohort_v
                 WHERE analysis_eligible_flag = TRUE) AS histology_analysis_eligible,
                (SELECT COUNT(*) FROM histology_analysis_cohort_v
                 WHERE adjudication_needed_flag = TRUE) AS histology_review_needed,
                (SELECT COUNT(*) FROM molecular_episode_v3) AS molecular_total_rows,
                (SELECT COUNT(*) FROM molecular_episode_v3
                 WHERE molecular_analysis_eligible_flag = TRUE) AS molecular_analysis_eligible,
                (SELECT COUNT(*) FROM molecular_episode_v3
                 WHERE molecular_analysis_eligible_flag = FALSE) AS molecular_unresolved,
                (SELECT COUNT(*) FROM molecular_episode_v3
                 WHERE TRY_CAST(overall_linkage_confidence AS INTEGER) < 50
                   AND molecular_analysis_eligible_flag = TRUE) AS molecular_low_confidence,
                (SELECT COUNT(*) FROM rai_episode_v3) AS rai_total_captured,
                (SELECT COUNT(*) FROM rai_episode_v3
                 WHERE rai_assertion_status IN (
                     'definite_received', 'likely_received'
                 )) AS rai_definite_likely,
                (SELECT COUNT(*) FROM rai_episode_v3
                 WHERE rai_assertion_status IN (
                     'definite_received', 'likely_received'
                 )) AS rai_analyzable,
                (SELECT COUNT(*) FROM rai_episode_v3
                 WHERE rai_assertion_status NOT IN (
                     'negated', 'definite_received', 'likely_received'
                 )) AS rai_unresolved,
                (SELECT COUNT(*) FROM validation_failures_v3
                 WHERE severity = 'error') AS validation_errors,
                (SELECT COUNT(*) FROM validation_failures_v3
                 WHERE severity = 'warning') AS validation_warnings,
                (SELECT COUNT(*) FROM validation_failures_v3
                 WHERE severity = 'info') AS validation_info,
                (SELECT COUNT(DISTINCT research_id)
                 FROM validation_failures_v3) AS validation_patients_affected,
                (SELECT COUNT(*) FROM patient_reconciliation_summary_v
                 WHERE total_issues > 0) AS review_queue_patients,
                (SELECT COUNT(*) FROM patient_reconciliation_summary_v
                 WHERE total_issues >= 5) AS review_critical_patients,
                (SELECT COUNT(*) FROM patient_reconciliation_summary_v
                 WHERE total_issues >= 3
                   AND total_issues < 5) AS review_high_patients,
                (SELECT n FROM total_patients) AS total_patients,
                CURRENT_DATE AS last_refresh_date,
                '2026-03-10' AS build_version;
        """)
        n = con.execute(
            "SELECT COUNT(*) FROM streamlit_cohort_qc_summary_v"
        ).fetchone()[0]
        print(f"{'streamlit_cohort_qc_summary_v':35s} rows={n:,}")
    except Exception as exc:
        print(f"{'streamlit_cohort_qc_summary_v':35s} skipped ({exc})")

    # ── FINAL OPTIMIZATION LAYER ─────────────────────────────────────────
    try:
        con.execute("""
            CREATE OR REPLACE TABLE survival_cohort AS
            SELECT * FROM advanced_features_sorted
            WHERE has_thyroglobulin_labs = TRUE
              OR has_nuclear_med = TRUE;
        """)
        n = con.execute("SELECT COUNT(*) FROM survival_cohort").fetchone()[0]
        print(f"{'survival_cohort':35s} rows={n:,}")
    except Exception as exc:
        print(f"{'survival_cohort':35s} skipped ({exc})")

    try:
        con.execute("""
            CREATE OR REPLACE TABLE publication_kpis AS
            SELECT * FROM overview_kpis;
        """)
        print(f"{'publication_kpis':35s} ✓")
    except Exception as exc:
        print(f"{'publication_kpis':35s} skipped ({exc})")

    print("-" * 72)
    print("Validation snapshot (9 views)")
    for name in creation_order:
        n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"{name:35s} {n:>12,}")

    con.close()
    print("=" * 72)
    print("Done.")


if __name__ == "__main__":
    main()
