-- THYROID_2026 molecular utilization study
-- Database: local DuckDB / local DuckDB  database thyroid_master.duckdb
-- Tag: v2026.03.13 | Descriptive counts only (no inference)
--
-- Cohort: Bethesda III–V (fna_bethesda_final IN (3,4,5)), surgery date present,
--         final histology text present (patient-level manuscript spine).
-- Molecular: ThyroSeq/Afirma with test date on/before surg_first_date (preoperative).
-- Note: fna_molecular_linkage_v2 is empty in production; v3 is sparse on this chain —
--       patient-level temporal join recovers interpretable N for Tables 1–3.

-- ── Reference: map overall_result_class to manuscript buckets ─────────────
CREATE OR REPLACE VIEW mol_result_class_map_v1 AS
SELECT * FROM (VALUES
    ('negative', 'Benign'),
    ('positive', 'Malignant'),
    ('suspicious', 'Suspicious'),
    ('indeterminate', 'Inconclusive'),
    ('non_diagnostic', 'Inconclusive'),
    ('cancelled', 'Inconclusive'),
    ('other', 'Inconclusive')
) AS t(overall_result_class, result_bucket);


-- ── Primary analytic cohort (patient-level) ─────────────────────────────────
CREATE OR REPLACE VIEW indeterminate_molecular_cohort_v1 AS
WITH base AS (
    SELECT
        mc.research_id,
        mc.fna_bethesda_final AS bethesda_category,
        mc.surg_first_date,
        mc.histology_final,
        mc.path_tumor_size_cm,
        mc.imaging_nodule_size_cm,
        COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) AS size_cm_any,
        CASE
            WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) IS NULL THEN 'Unknown'
            WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) < 2 THEN '<2 cm'
            WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) <= 4 THEN '2–4 cm'
            ELSE '>4 cm'
        END AS size_stratum,
        CASE WHEN YEAR(mc.surg_first_date) < 2021 THEN 'pre_2021' ELSE '2021_plus' END AS surgery_era,
        CASE
            WHEN mc.histology_final IS NULL THEN NULL
            WHEN LOWER(mc.histology_final) LIKE '%niftp%' THEN FALSE
            WHEN LOWER(TRIM(mc.histology_final)) = 'ptc'
                OR LOWER(mc.histology_final) LIKE 'ptc %'
                OR LOWER(mc.histology_final) LIKE 'ptc,%' THEN TRUE
            WHEN regexp_matches(
                LOWER(mc.histology_final),
                'carcinoma|metastatic|lymphoma|sarcoma|malignant'
            ) THEN TRUE
            ELSE FALSE
        END AS histology_malignant_flag
    FROM manuscript_cohort_v1 mc
    WHERE mc.fna_bethesda_final IN (3, 4, 5)
      AND mc.surg_first_date IS NOT NULL
      AND mc.histology_final IS NOT NULL
      AND TRIM(mc.histology_final) <> ''
),
preop_mol_ranked AS (
    SELECT
        b.research_id,
        m.molecular_episode_id,
        m.platform,
        m.overall_result_class,
        COALESCE(TRY_CAST(m.resolved_test_date AS DATE), m.test_date_native) AS test_date,
        m.inadequate_flag,
        m.cancelled_flag,
        ROW_NUMBER() OVER (
            PARTITION BY b.research_id
            ORDER BY COALESCE(TRY_CAST(m.resolved_test_date AS DATE), m.test_date_native) DESC NULLS LAST,
                     m.molecular_episode_id DESC
        ) AS rn_last_preop
    FROM base b
    INNER JOIN molecular_test_episode_v2 m ON m.research_id = b.research_id
    WHERE m.platform IN ('ThyroSeq', 'Afirma')
      AND COALESCE(TRY_CAST(m.resolved_test_date AS DATE), m.test_date_native) IS NOT NULL
      AND COALESCE(TRY_CAST(m.resolved_test_date AS DATE), m.test_date_native) <= b.surg_first_date
),
preop_mol_one AS (
    SELECT * FROM preop_mol_ranked WHERE rn_last_preop = 1
),
mapped AS (
    SELECT
        b.*,
        p.molecular_episode_id,
        p.platform,
        p.overall_result_class,
        p.test_date,
        COALESCE(map.result_bucket, 'Inconclusive') AS result_bucket,
        (p.molecular_episode_id IS NOT NULL) AS tested_preop_flag,
        CASE WHEN YEAR(p.test_date) < 2021 THEN 'pre_2021' ELSE '2021_plus' END AS test_era
    FROM base b
    LEFT JOIN preop_mol_one p ON b.research_id = p.research_id
    LEFT JOIN mol_result_class_map_v1 map
        ON LOWER(TRIM(COALESCE(p.overall_result_class, ''))) = map.overall_result_class
)
SELECT * FROM mapped;


-- ── Table 1: Utilization by surgery year × Bethesda ────────────────────────
-- Denominator: patients in cohort; tested = preop ThyroSeq/Afirma
CREATE OR REPLACE VIEW table1_utilization_by_surgery_year_v1 AS
SELECT
    YEAR(surg_first_date) AS surgery_year,
    bethesda_category,
    COUNT(*) AS n_patients,
    SUM(CASE WHEN tested_preop_flag THEN 1 ELSE 0 END) AS n_tested_preop,
    ROUND(100.0 * SUM(CASE WHEN tested_preop_flag THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_tested
FROM indeterminate_molecular_cohort_v1
GROUP BY 1, 2
ORDER BY 1, 2;


CREATE OR REPLACE VIEW table1_utilization_by_size_v1 AS
SELECT
    size_stratum,
    bethesda_category,
    COUNT(*) AS n_patients,
    SUM(CASE WHEN tested_preop_flag THEN 1 ELSE 0 END) AS n_tested_preop,
    ROUND(100.0 * SUM(CASE WHEN tested_preop_flag THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_tested
FROM indeterminate_molecular_cohort_v1
GROUP BY 1, 2
ORDER BY 1, 2;


CREATE OR REPLACE VIEW table1_utilization_by_surgery_era_v1 AS
SELECT
    surgery_era,
    bethesda_category,
    COUNT(*) AS n_patients,
    SUM(CASE WHEN tested_preop_flag THEN 1 ELSE 0 END) AS n_tested_preop,
    ROUND(100.0 * SUM(CASE WHEN tested_preop_flag THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_tested
FROM indeterminate_molecular_cohort_v1
GROUP BY 1, 2
ORDER BY 1, 2;


-- Test-year distribution among *tested* patients only (secondary to surgery-year denominator above)
CREATE OR REPLACE VIEW table1_molecular_tests_by_year_v1 AS
SELECT
    YEAR(test_date) AS test_year,
    bethesda_category,
    platform,
    COUNT(*) AS n_tests
FROM indeterminate_molecular_cohort_v1
WHERE tested_preop_flag AND test_date IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


-- ── Table 2: Platform × Bethesda × result (tested patients only) ──────────
CREATE OR REPLACE VIEW table2_platform_bethesda_result_v1 AS
WITH tested AS (
    SELECT * FROM indeterminate_molecular_cohort_v1 WHERE tested_preop_flag
),
tot AS (
    SELECT bethesda_category, COUNT(*) AS n_bet FROM tested GROUP BY 1
)
SELECT
    t.platform,
    t.bethesda_category,
    COALESCE(t.result_bucket, 'Inconclusive') AS result_bucket,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / tot.n_bet, 2) AS col_pct_within_bethesda
FROM tested t
JOIN tot ON tot.bethesda_category = t.bethesda_category
GROUP BY 1, 2, 3, tot.n_bet
ORDER BY 2, 1, 3;


-- ── Table 3: ROM by platform × result bucket (+ never-tested comparator) ──
CREATE OR REPLACE VIEW table3_rom_by_platform_result_v1 AS
WITH spine AS (
    SELECT
        research_id,
        bethesda_category,
        tested_preop_flag,
        CASE WHEN NOT tested_preop_flag THEN 'Never-tested' ELSE COALESCE(platform, 'Unknown_platform') END AS platform,
        CASE
            WHEN NOT tested_preop_flag THEN 'Never-tested'
            ELSE COALESCE(result_bucket, 'Inconclusive')
        END AS result_bucket,
        histology_malignant_flag
    FROM indeterminate_molecular_cohort_v1
)
SELECT
    platform,
    result_bucket,
    COUNT(*) AS n,
    SUM(CASE WHEN histology_malignant_flag IS TRUE THEN 1 ELSE 0 END) AS n_malignant,
    ROUND(
        100.0 * SUM(CASE WHEN histology_malignant_flag IS TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    ) AS pct_rom
FROM spine
GROUP BY 1, 2
ORDER BY 1, 2;


-- ── Bonus: Lobe vs total among tested (first operative episode) ───────────
CREATE OR REPLACE VIEW bonus_surgery_extent_by_result_v1 AS
WITH first_op AS (
    SELECT
        research_id,
        procedure_normalized,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) NULLS LAST
        ) AS rn
    FROM operative_episode_detail_v2
),
first_ep AS (SELECT * FROM first_op WHERE rn = 1),
cohort AS (
    SELECT research_id, result_bucket, tested_preop_flag
    FROM indeterminate_molecular_cohort_v1
    WHERE tested_preop_flag
)
SELECT
    COALESCE(c.result_bucket, 'Inconclusive') AS result_bucket,
    COUNT(*) AS n,
    SUM(CASE WHEN fe.procedure_normalized = 'hemithyroidectomy' THEN 1 ELSE 0 END) AS n_lobe,
    SUM(CASE WHEN fe.procedure_normalized = 'total_thyroidectomy' THEN 1 ELSE 0 END) AS n_total,
    ROUND(100.0 * SUM(CASE WHEN fe.procedure_normalized = 'hemithyroidectomy' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_lobe,
    ROUND(100.0 * SUM(CASE WHEN fe.procedure_normalized = 'total_thyroidectomy' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_total
FROM cohort c
LEFT JOIN first_ep fe ON fe.research_id = c.research_id
GROUP BY 1
ORDER BY 1;


-- ── Sankey edge prep (aggregate flows) ─────────────────────────────────────
CREATE OR REPLACE VIEW sankey_flow_edges_v1 AS
WITH first_op AS (
    SELECT
        research_id,
        procedure_normalized,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) NULLS LAST
        ) AS rn
    FROM operative_episode_detail_v2
),
first_ep AS (SELECT research_id, procedure_normalized FROM first_op WHERE rn = 1),
tumor_ranked AS (
    SELECT
        research_id,
        LOWER(COALESCE(procedure_raw, '')) AS pr,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY surgery_date NULLS LAST, surgery_episode_id
        ) AS rn
    FROM tumor_episode_master_v2
    WHERE surgery_date IS NOT NULL
),
completion AS (
    SELECT
        t1.research_id,
        TRUE AS completion_after_initial_lobe
    FROM tumor_ranked t1
    INNER JOIN tumor_ranked t2
        ON t1.research_id = t2.research_id AND t1.rn = 1 AND t2.rn = 2
    WHERE (
        t1.pr LIKE '%hemithyroid%' OR t1.pr LIKE '%thyroid lobectomy%' OR t1.pr LIKE '%lobectomy%'
    )
      AND t1.pr NOT LIKE '%total%'
      AND (
            t2.pr LIKE '%completion%'
            OR t2.pr LIKE '%total thyroid%'
            OR t2.pr LIKE '%total thyroidectomy%'
        )
),
enriched AS (
    SELECT
        c.research_id,
        'Bethesda_' || CAST(c.bethesda_category AS VARCHAR) AS bethesda_lbl,
        COALESCE(c.result_bucket, 'Never-tested') AS result_lbl,
        COALESCE(fe.procedure_normalized, 'unknown_procedure') AS procedure_lbl,
        CASE WHEN COALESCE(ci.completion_after_initial_lobe, FALSE) THEN 'completion_TT' ELSE 'no_completion' END AS completion_lbl
    FROM indeterminate_molecular_cohort_v1 c
    LEFT JOIN first_ep fe ON fe.research_id = c.research_id
    LEFT JOIN completion ci ON ci.research_id = c.research_id
)
SELECT bethesda_lbl AS source, result_lbl AS target, COUNT(*)::BIGINT AS value FROM enriched GROUP BY 1, 2
UNION ALL
SELECT result_lbl AS source, procedure_lbl AS target, COUNT(*)::BIGINT AS value FROM enriched GROUP BY 1, 2
UNION ALL
SELECT procedure_lbl AS source, completion_lbl AS target, COUNT(*)::BIGINT AS value FROM enriched GROUP BY 1, 2;


-- ── Cross-check: episode-level FNA + surgery v3 spine row count ────────────
CREATE OR REPLACE VIEW audit_fna_episode_operated_b35_v1 AS
WITH fna_base AS (
    SELECT DISTINCT research_id, fna_episode_id, bethesda_category
    FROM fna_episode_master_v2
    WHERE bethesda_category IN (3, 4, 5)
),
ps AS (
    SELECT *
    FROM preop_surgery_linkage_v3
    WHERE preop_type = 'fna' AND score_rank = 1 AND analysis_eligible_link_flag
),
joined AS (
    SELECT b.*, ps.surgery_episode_id
    FROM fna_base b
    INNER JOIN ps ON b.research_id = ps.research_id AND b.fna_episode_id = ps.preop_episode_id
),
sp AS (
    SELECT *
    FROM surgery_pathology_linkage_v3
    WHERE score_rank = 1 AND analysis_eligible_link_flag
)
SELECT
    COUNT(*) AS n_fna_episodes,
    COUNT(DISTINCT j.research_id) AS n_patients
FROM joined j
INNER JOIN sp ON j.research_id = sp.research_id AND j.surgery_episode_id = sp.surgery_episode_id;
