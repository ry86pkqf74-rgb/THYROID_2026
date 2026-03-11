-- scripts/22_manuscript_package_v3.sql
-- Manuscript Package V3 — publication-ready aggregated views
-- Produces Table 1 (demographics), Table 2 (survival metrics), Table 3 (genotype outcomes)
-- All values formatted for direct LaTeX export via 22_manuscript_package.py
--
-- Depends on: enriched_patient_timeline_v3_mv, genotype_stratified_outcomes_v3_mv,
--             time_to_rai_v3_mv, recurrence_free_survival_v3_mv
-- Run after: scripts/21_survival_analysis_v3.sql

USE thyroid_research_2026;

-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 1: Patient Demographics
-- One row per characteristic; col2 = n (%), col3 = value (range/SD)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW manuscript_table1_demographics_v AS
WITH patient_spine AS (
    SELECT
        research_id,
        MAX(sex)            AS sex,
        MAX(age_at_surgery) AS age_at_surgery,
        MAX(histology_1_type) AS histology,
        MAX(overall_stage_ajcc8) AS ajcc_stage
    FROM enriched_patient_timeline_v3_mv
    GROUP BY research_id
),
geno AS (
    SELECT
        research_id,
        MAX(braf_ras_status)     AS braf_ras_status,
        MAX(date_rescue_confidence) AS date_rescue_confidence,
        BOOL_OR(COALESCE(has_genetic_testing, FALSE)) AS has_genetic_testing
    FROM genotype_stratified_outcomes_v3_mv
    GROUP BY research_id
),
combined AS (
    SELECT
        p.research_id,
        p.sex,
        p.age_at_surgery,
        p.histology,
        p.ajcc_stage,
        g.braf_ras_status,
        g.date_rescue_confidence,
        g.has_genetic_testing
    FROM patient_spine p
    LEFT JOIN geno g ON CAST(p.research_id AS VARCHAR) = CAST(g.research_id AS VARCHAR)
),
totals AS (SELECT COUNT(DISTINCT research_id) AS n_total FROM combined),
age_stats AS (
    SELECT
        ROUND(AVG(TRY_CAST(age_at_surgery AS DOUBLE)), 1) AS mean_age,
        ROUND(STDDEV(TRY_CAST(age_at_surgery AS DOUBLE)), 1) AS sd_age,
        MIN(TRY_CAST(age_at_surgery AS DOUBLE)) AS min_age,
        MAX(TRY_CAST(age_at_surgery AS DOUBLE)) AS max_age
    FROM combined
    WHERE age_at_surgery IS NOT NULL
)
SELECT
    row_order,
    characteristic,
    n_count,
    pct_or_stat,
    display_value
FROM (
    -- Total N
    SELECT 1 AS row_order, 'Total patients' AS characteristic,
           (SELECT n_total FROM totals) AS n_count,
           NULL AS pct_or_stat,
           CAST((SELECT n_total FROM totals) AS VARCHAR) AS display_value
    UNION ALL
    -- Female sex
    SELECT 2, 'Female sex',
           COUNT(*) FILTER (WHERE LOWER(COALESCE(sex,'')) LIKE 'f%'),
           ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(sex,'')) LIKE 'f%')
                 / NULLIF((SELECT n_total FROM totals), 0), 1),
           CAST(COUNT(*) FILTER (WHERE LOWER(COALESCE(sex,'')) LIKE 'f%') AS VARCHAR)
           || ' (' ||
           CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(sex,'')) LIKE 'f%')
                      / NULLIF((SELECT n_total FROM totals), 0), 1) AS VARCHAR)
           || '%)'
    FROM combined
    UNION ALL
    -- Age
    SELECT 3, 'Age at surgery, years — mean (SD)',
           NULL,
           (SELECT mean_age FROM age_stats),
           (SELECT CAST(mean_age AS VARCHAR) || ' (' || CAST(sd_age AS VARCHAR) || ')' FROM age_stats)
    UNION ALL
    -- Histology: PTC classic
    SELECT 4, 'Histology: PTC classic',
           COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%classic%'),
           ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%classic%')
                 / NULLIF((SELECT n_total FROM totals), 0), 1),
           CAST(COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%classic%') AS VARCHAR)
           || ' (' ||
           CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%classic%')
                      / NULLIF((SELECT n_total FROM totals), 0), 1) AS VARCHAR) || '%)'
    FROM combined
    UNION ALL
    -- Histology: PTC follicular variant
    SELECT 5, 'Histology: PTC follicular variant',
           COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%follicular%'),
           ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%follicular%')
                 / NULLIF((SELECT n_total FROM totals), 0), 1),
           CAST(COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%follicular%') AS VARCHAR)
           || ' (' ||
           CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%follicular%')
                      / NULLIF((SELECT n_total FROM totals), 0), 1) AS VARCHAR) || '%)'
    FROM combined
    UNION ALL
    -- Histology: PTC tall cell
    SELECT 6, 'Histology: PTC tall cell',
           COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%tall%'),
           ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%tall%')
                 / NULLIF((SELECT n_total FROM totals), 0), 1),
           CAST(COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%tall%') AS VARCHAR)
           || ' (' ||
           CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(histology,'')) LIKE '%tall%')
                      / NULLIF((SELECT n_total FROM totals), 0), 1) AS VARCHAR) || '%)'
    FROM combined
    UNION ALL
    -- AJCC Stage I/II
    SELECT 7, 'AJCC 8th Ed Stage I/II',
           COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc_stage,'')) IN ('I','IA','IB','II','IIA','IIB')),
           ROUND(100.0 * COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc_stage,'')) IN ('I','IA','IB','II','IIA','IIB'))
                 / NULLIF((SELECT n_total FROM totals), 0), 1),
           CAST(COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc_stage,'')) IN ('I','IA','IB','II','IIA','IIB')) AS VARCHAR)
           || ' (' ||
           CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc_stage,'')) IN ('I','IA','IB','II','IIA','IIB'))
                      / NULLIF((SELECT n_total FROM totals), 0), 1) AS VARCHAR) || '%)'
    FROM combined
    UNION ALL
    -- AJCC Stage III/IV
    SELECT 8, 'AJCC 8th Ed Stage III/IV',
           COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc_stage,'')) IN ('III','IVA','IVB','IVC','IV')),
           ROUND(100.0 * COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc_stage,'')) IN ('III','IVA','IVB','IVC','IV'))
                 / NULLIF((SELECT n_total FROM totals), 0), 1),
           CAST(COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc_stage,'')) IN ('III','IVA','IVB','IVC','IV')) AS VARCHAR)
           || ' (' ||
           CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc_stage,'')) IN ('III','IVA','IVB','IVC','IV'))
                      / NULLIF((SELECT n_total FROM totals), 0), 1) AS VARCHAR) || '%)'
    FROM combined
    UNION ALL
    -- BRAF/RAS positive
    SELECT 9, 'BRAF/RAS mutation positive',
           COUNT(*) FILTER (WHERE braf_ras_status = 'BRAF/RAS+'),
           ROUND(100.0 * COUNT(*) FILTER (WHERE braf_ras_status = 'BRAF/RAS+')
                 / NULLIF(COUNT(*) FILTER (WHERE has_genetic_testing), 0), 1),
           CAST(COUNT(*) FILTER (WHERE braf_ras_status = 'BRAF/RAS+') AS VARCHAR)
           || '/' ||
           CAST(COUNT(*) FILTER (WHERE has_genetic_testing) AS VARCHAR)
           || ' tested ('
           || CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE braf_ras_status = 'BRAF/RAS+')
                         / NULLIF(COUNT(*) FILTER (WHERE has_genetic_testing), 0), 1) AS VARCHAR)
           || '%)'
    FROM combined
    UNION ALL
    -- Date rescue confidence: high
    SELECT 10, 'Date rescue confidence: high (≥80%)',
           COUNT(*) FILTER (WHERE date_rescue_confidence = 'high'),
           ROUND(100.0 * COUNT(*) FILTER (WHERE date_rescue_confidence = 'high')
                 / NULLIF((SELECT n_total FROM totals), 0), 1),
           CAST(COUNT(*) FILTER (WHERE date_rescue_confidence = 'high') AS VARCHAR)
           || ' (' ||
           CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE date_rescue_confidence = 'high')
                      / NULLIF((SELECT n_total FROM totals), 0), 1) AS VARCHAR) || '%)'
    FROM combined
) t
ORDER BY row_order;


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 2: Survival Metrics by Stage
-- Median time-to-RAI, recurrence rate, and censoring by AJCC stage group
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW manuscript_table2_survival_v AS
WITH base AS (
    SELECT
        research_id,
        time_to_rai_days,
        time_to_recurrence_days,
        censoring_flag,
        ajcc_stage_grouped,
        date_rescue_confidence
    FROM genotype_stratified_outcomes_v3_mv
    WHERE ajcc_stage_grouped IS NOT NULL
),
stage_agg AS (
    SELECT
        ajcc_stage_grouped                                             AS "AJCC Stage Group",
        COUNT(*)                                                        AS "N",
        COUNT(*) FILTER (WHERE time_to_rai_days IS NOT NULL)           AS "Received RAI, n",
        ROUND(100.0 * COUNT(*) FILTER (WHERE time_to_rai_days IS NOT NULL)
              / NULLIF(COUNT(*), 0), 1)                                AS "RAI Rate (%)",
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_rai_days) / 30.44, 1)
                                                                        AS "Median Time-to-RAI (months)",
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY time_to_rai_days) / 30.44, 1)
                                                                        AS "Q1 Time-to-RAI (months)",
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_to_rai_days) / 30.44, 1)
                                                                        AS "Q3 Time-to-RAI (months)",
        COUNT(*) FILTER (WHERE COALESCE(censoring_flag, 1) = 0)        AS "Recurrence Events, n",
        ROUND(100.0 * COUNT(*) FILTER (WHERE COALESCE(censoring_flag, 1) = 0)
              / NULLIF(COUNT(*), 0), 1)                                AS "Recurrence Rate (%)",
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_recurrence_days) / 365.25, 2)
                                                                        AS "Median RFS (years)",
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY time_to_recurrence_days) / 365.25, 2)
                                                                        AS "Q1 RFS (years)",
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_to_recurrence_days) / 365.25, 2)
                                                                        AS "Q3 RFS (years)"
    FROM base
    GROUP BY ajcc_stage_grouped
)
SELECT
    "AJCC Stage Group",
    "N",
    "Received RAI, n",
    "RAI Rate (%)",
    COALESCE(CAST("Median Time-to-RAI (months)" AS VARCHAR), 'NR')
        || ' [' || COALESCE(CAST("Q1 Time-to-RAI (months)" AS VARCHAR), '–')
        || '–' || COALESCE(CAST("Q3 Time-to-RAI (months)" AS VARCHAR), '–') || ']'
        AS "Median Time-to-RAI, months [IQR]",
    "Recurrence Events, n",
    "Recurrence Rate (%)",
    COALESCE(CAST("Median RFS (years)" AS VARCHAR), 'NR')
        || ' [' || COALESCE(CAST("Q1 RFS (years)" AS VARCHAR), '–')
        || '–' || COALESCE(CAST("Q3 RFS (years)" AS VARCHAR), '–') || ']'
        AS "Median RFS, years [IQR]"
FROM stage_agg
ORDER BY "AJCC Stage Group";


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 3: Genotype-Stratified Outcomes
-- BRAF/RAS+ vs wild-type: RAI, recurrence, and follow-up metrics
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW manuscript_table3_genotype_v AS
WITH base AS (
    SELECT
        research_id,
        braf_ras_status                                                    AS genotype_group,
        time_to_rai_days,
        time_to_recurrence_days,
        censoring_flag,
        ajcc_stage_grouped,
        histology_1_type
    FROM genotype_stratified_outcomes_v3_mv
    WHERE braf_ras_status IS NOT NULL
),
geno_agg AS (
    SELECT
        genotype_group                                                      AS "Genotype Group",
        COUNT(*)                                                             AS "N",
        COUNT(*) FILTER (WHERE time_to_rai_days IS NOT NULL)                AS "Received RAI, n",
        ROUND(100.0 * COUNT(*) FILTER (WHERE time_to_rai_days IS NOT NULL)
              / NULLIF(COUNT(*), 0), 1)                                     AS "RAI Rate (%)",
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_rai_days) / 30.44, 1)
                                                                             AS "Median Time-to-RAI (months)",
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY time_to_rai_days) / 30.44, 1)
                                                                             AS "Q1 Time-to-RAI (months)",
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_to_rai_days) / 30.44, 1)
                                                                             AS "Q3 Time-to-RAI (months)",
        COUNT(*) FILTER (WHERE COALESCE(censoring_flag, 1) = 0)             AS "Recurrence Events, n",
        ROUND(100.0 * COUNT(*) FILTER (WHERE COALESCE(censoring_flag, 1) = 0)
              / NULLIF(COUNT(*), 0), 1)                                     AS "Recurrence Rate (%)",
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_recurrence_days) / 365.25, 2)
                                                                             AS "Median RFS (years)",
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY time_to_recurrence_days) / 365.25, 2)
                                                                             AS "Q1 RFS (years)",
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_to_recurrence_days) / 365.25, 2)
                                                                             AS "Q3 RFS (years)",
        ROUND(100.0 * COUNT(*) FILTER (WHERE ajcc_stage_grouped = 'III/IV')
              / NULLIF(COUNT(*), 0), 1)                                     AS "Stage III/IV (%)"
    FROM base
    GROUP BY genotype_group
)
SELECT
    "Genotype Group",
    "N",
    "Received RAI, n",
    "RAI Rate (%)",
    COALESCE(CAST("Median Time-to-RAI (months)" AS VARCHAR), 'NR')
        || ' [' || COALESCE(CAST("Q1 Time-to-RAI (months)" AS VARCHAR), '–')
        || '–' || COALESCE(CAST("Q3 Time-to-RAI (months)" AS VARCHAR), '–') || ']'
        AS "Median Time-to-RAI, months [IQR]",
    "Recurrence Events, n",
    "Recurrence Rate (%)",
    COALESCE(CAST("Median RFS (years)" AS VARCHAR), 'NR')
        || ' [' || COALESCE(CAST("Q1 RFS (years)" AS VARCHAR), '–')
        || '–' || COALESCE(CAST("Q3 RFS (years)" AS VARCHAR), '–') || ']'
        AS "Median RFS, years [IQR]",
    "Stage III/IV (%)"
FROM geno_agg
ORDER BY "Genotype Group";
