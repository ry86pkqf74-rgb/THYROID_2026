#!/usr/bin/env python3
"""
03_research_views.py

Phase 2 research-facing analytic views for thyroid cancer studies.
"""

from pathlib import Path
import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"


VIEWS_SQL: dict[str, str] = {
    "ptc_cohort": """
CREATE OR REPLACE VIEW ptc_cohort AS
SELECT
    tp.research_id,
    tp.surgery_date,
    tp.age_at_surgery,
    tp.sex,
    tp.histology_1_type,
    tp.histology_1_t_stage_ajcc8 AS t_stage_ajcc8,
    tp.histology_1_n_stage_ajcc8 AS n_stage_ajcc8,
    tp.histology_1_m_stage_ajcc8 AS m_stage_ajcc8,
    tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
    TRY_CAST(tp.histology_1_ln_examined AS DOUBLE) AS ln_examined,
    TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS ln_positive,
    tp.tumor_1_extrathyroidal_ext,
    tp.tumor_1_gross_ete,
    tp.tumor_1_ete_microscopic_only,
    tp.tumor_1_histology_variant,
    tp.tumor_1_histology_subtype_detail,
    tp.variant_standardized,
    tp.surgery_type_normalized
FROM tumor_pathology tp
WHERE UPPER(COALESCE(tp.histology_1_type, '')) = 'PTC'
  AND (
        LOWER(COALESCE(tp.tumor_1_histology_variant, '')) LIKE '%classic%'
        OR LOWER(COALESCE(tp.tumor_1_histology_subtype_detail, '')) LIKE '%classic%'
        OR (
            COALESCE(tp.tumor_1_histology_variant, '') = ''
            AND COALESCE(tp.tumor_1_histology_subtype_detail, '') = ''
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
    -- Phase 4 standardized columns
    tp.histology_1_type,
    tp.variant_standardized,
    tp.surgery_type_normalized AS malignant_surgery_type,
    tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
    tp.tumor_focality_overall,
    TRY_CAST(tp.num_tumors_identified AS INTEGER) AS num_tumors,
    -- Invasion (already structured)
    tp.tumor_1_vascular_invasion,
    tp.tumor_1_lymphatic_invasion,
    tp.tumor_1_perineural_invasion,
    tp.tumor_1_capsular_invasion,
    -- ETE (already structured)
    tp.tumor_1_extrathyroidal_ext,
    tp.tumor_1_gross_ete,
    tp.tumor_1_ete_microscopic_only,
    -- LN summary
    TRY_CAST(tp.histology_1_ln_examined AS DOUBLE) AS ln_examined,
    TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS ln_positive,
    TRY_CAST(tp.histology_1_ln_ratio AS DOUBLE) AS ln_ratio,
    -- Phase 5 mutation flags
    tp.braf_mutation_mentioned,
    tp.ras_mutation_mentioned,
    tp.ret_mutation_mentioned,
    tp.tert_mutation_mentioned,
    tp.ntrk_mutation_mentioned,
    tp.alk_mutation_mentioned,
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
LEFT JOIN benign_pathology bp ON mc.research_id = bp.research_id
""",
}


def main() -> None:
    con = duckdb.connect(str(DB_PATH))

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
