-- Paste into MotherDuck Console / Notebook (thyroid_research_2026)
-- Companion to studies/molecular_utilization_2026 — verification & Pro charting

-- 0) Deploy study views (run 01_views_and_cohort.sql first from repo or paste here)

-- 1) Linkage completeness (corrected LIKE predicates)
SELECT *
FROM val_episode_linkage_completeness_v1
WHERE table_name LIKE '%molecular%'
   OR table_name LIKE '%fna%'
   OR table_name LIKE '%path%'
ORDER BY linkage_type;

-- 2) v2 vs v3 molecular linkage row counts
SELECT 'fna_molecular_linkage_v2' AS tbl, COUNT(*) AS n FROM fna_molecular_linkage_v2
UNION ALL
SELECT 'fna_molecular_linkage_v3', COUNT(*) FROM fna_molecular_linkage_v3;

-- 3) Episode-level operated Bethesda III–V + surgery/pathology v3 bridge (audit)
SELECT * FROM audit_fna_episode_operated_b35_v1;

-- 4) Patient-level cohort (primary analysis)
SELECT
    COUNT(*) AS n_patients,
    SUM(tested_preop_flag::INT) AS n_preop_mol_tested,
    ROUND(100.0 * SUM(tested_preop_flag::INT) / COUNT(*), 2) AS pct_tested
FROM indeterminate_molecular_cohort_v1;

-- 5) Molecular episode ID linkage fraction (expect high when molecular_episode_id populated)
SELECT
    COUNT(*) AS n,
    SUM(CASE WHEN molecular_episode_id IS NOT NULL THEN 1 ELSE 0 END) AS n_linked,
    ROUND(100.0 * SUM(CASE WHEN molecular_episode_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_linked
FROM indeterminate_molecular_cohort_v1;

-- 6) Temporal integrity — preop tests only (by construction); show post-op violations if any slipped in
SELECT
    SUM(
        CASE
            WHEN tested_preop_flag AND test_date IS NOT NULL AND surg_first_date IS NOT NULL
                AND test_date > surg_first_date
            THEN 1 ELSE 0
        END
    ) AS n_postop_test_violations
FROM indeterminate_molecular_cohort_v1;

-- 7) manuscript_cohort_v1 vs study cohort count cross-check
SELECT
    (SELECT COUNT(*) FROM manuscript_cohort_v1 WHERE fna_bethesda_final IN (3, 4, 5)) AS ms_b35_all,
    (SELECT COUNT(*) FROM manuscript_cohort_v1
     WHERE fna_bethesda_final IN (3, 4, 5) AND surg_first_date IS NOT NULL
           AND histology_final IS NOT NULL AND TRIM(histology_final) <> '') AS ms_b35_surg_hist,
    (SELECT COUNT(*) FROM indeterminate_molecular_cohort_v1) AS study_cohort;

-- 8) Table exports for dashboards
SELECT * FROM table1_utilization_by_surgery_year_v1;
SELECT * FROM table1_utilization_by_size_v1;
SELECT * FROM table1_utilization_by_surgery_era_v1;
SELECT * FROM table1_molecular_tests_by_year_v1;
SELECT * FROM table2_platform_bethesda_result_v1;
SELECT * FROM table3_rom_by_platform_result_v1;
SELECT * FROM bonus_surgery_extent_by_result_v1;
SELECT * FROM sankey_flow_edges_v1;
