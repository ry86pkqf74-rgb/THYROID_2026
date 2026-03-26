-- MotherDuck / thyroid_research_2026 — verification for molecular utilization V2
-- Run 01_views_and_cohort_v2.sql first (or full study refresh via run_analysis.py --v2).

-- 0) Schema sanity: v2 linkage table (do not use v2 empty table)
SELECT 'fna_molecular_linkage_v2' AS tbl, COUNT(*)::BIGINT AS n FROM fna_molecular_linkage_v2
UNION ALL
SELECT 'fna_molecular_linkage_v3', COUNT(*)::BIGINT FROM fna_molecular_linkage_v3;

-- 1) Core cohort counts
SELECT 'all_eligible_indeterminate_v2' AS view_name, COUNT(*)::BIGINT AS n_rows,
       COUNT(DISTINCT research_id)::BIGINT AS n_patients
FROM all_eligible_indeterminate_v2
UNION ALL
SELECT 'tested_indeterminate_v2', COUNT(*)::BIGINT, COUNT(DISTINCT research_id)::BIGINT FROM tested_indeterminate_v2
UNION ALL
SELECT 'operated_indeterminate_v2', COUNT(*)::BIGINT, COUNT(DISTINCT research_id)::BIGINT FROM operated_indeterminate_v2
UNION ALL
SELECT 'bethesdaV_secondary_v2', COUNT(*)::BIGINT, COUNT(DISTINCT research_id)::BIGINT FROM bethesdaV_secondary_v2;

-- 2) Episode sensitivity snapshot
SELECT * FROM episode_sensitivity_v2;

-- 3) Temporal integrity: latest test should be on/after index FNA
SELECT COUNT(*) AS n_violations_latest_before_index
FROM all_eligible_indeterminate_v2
WHERE ever_tested_ts_af_after_index
  AND latest_test_date < index_indeterminate_fna_date;

-- 4) Preop tests must be on/before surgery when surgery exists
SELECT COUNT(*) AS n_violations_preop_after_surgery
FROM all_eligible_indeterminate_v2
WHERE preop_tested_among_surgeries
  AND preop_test_date IS NOT NULL
  AND surg_first_date IS NOT NULL
  AND preop_test_date > CAST(surg_first_date AS DATE);

-- 5) Cross-check manuscript Bethesda III/IV spine
SELECT
    (SELECT COUNT(*) FROM manuscript_cohort_v1 WHERE CAST(fna_bethesda_final AS INTEGER) IN (3, 4)) AS ms_b34_all,
    (SELECT COUNT(DISTINCT CAST(research_id AS INTEGER)) FROM index_indeterminate_fna_34_v2) AS index_fna_34_patients_2015,
    (SELECT COUNT(*) FROM all_eligible_indeterminate_v2) AS v2_eligible_intersection;

-- 6) Manual review queue size
SELECT review_reason, COUNT(*) AS n
FROM manual_review_molecular_path_mismatch_v2
GROUP BY 1
ORDER BY 2 DESC;

-- 7) Export drivers (paste results / CSV from run_analysis.py --v2)
SELECT * FROM ana_v2_testing_uptake_by_year_bethesda;
SELECT * FROM ana_v2_platform_mix_by_year;
SELECT * FROM ana_v2_result_mix_by_platform;
SELECT * FROM ana_v2_surgery_rates_tested_vs_never;
SELECT * FROM ana_v2_pathology_among_operated;
SELECT * FROM ana_v2_surgery_extent_operated;
SELECT * FROM ana_v2_mutation_families_tested;
SELECT * FROM ana_v2_testing_uptake_by_year_bethesda3;
SELECT * FROM ana_v2_testing_uptake_by_year_bethesda4;
SELECT * FROM ana_v2_pathology_among_operated_bethesda3;
SELECT * FROM ana_v2_pathology_among_operated_bethesda4;
SELECT * FROM ana_v2_testing_uptake_by_year_bethesda_sens2018;
SELECT * FROM ana_v2_bethesda5_uptake_by_year;
SELECT * FROM ana_v2_bethesda5_platform_by_year;
SELECT * FROM qa_small_cell_strata_v2;
