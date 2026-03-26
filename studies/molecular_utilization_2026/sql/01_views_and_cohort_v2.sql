-- THYROID_2026 molecular utilization study — V2 manuscript refresh
-- Database: MotherDuck / DuckDB  thyroid_research_2026
--
-- Design: Primary paper = Bethesda III/IV, patient-level temporal spine + episode sensitivity.
--         Denominator = index indeterminate FNA in molecular era (2015+); sensitivity era column 2018+.
--         Secondary = Bethesda V cohort; operated subset for pathology / extent (no tumor_pathology-only denom).
-- Join rules: FNA always on (research_id, fna_episode_id); do not use fna_molecular_linkage_v2.
-- Pathology: prefer tumor_pathology.histology_1_type; histology_final keywords = fallback only.

-- ═══════════════════════════════════════════════════════════════════════════
-- Result class map (shared; idempotent with v1)
-- ═══════════════════════════════════════════════════════════════════════════
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


-- ═══════════════════════════════════════════════════════════════════════════
-- One row per patient tumor_pathology (largest lesion first; descriptive spine)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW tumor_pathology_patient_v2 AS
SELECT *
FROM tumor_pathology
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY research_id
    ORDER BY histology_1_largest_tumor_cm DESC NULLS LAST
) = 1;


-- ═══════════════════════════════════════════════════════════════════════════
-- Keyword fallback on histology_final (same logic as v1 ROM flag; not primary path)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW histology_final_keyword_flags_v2 AS
SELECT
    CAST(research_id AS INTEGER) AS research_id,
    histology_final,
    CASE
        WHEN histology_final IS NULL OR TRIM(CAST(histology_final AS VARCHAR)) = '' THEN NULL
        WHEN LOWER(CAST(histology_final AS VARCHAR)) LIKE '%niftp%' THEN FALSE
        WHEN LOWER(TRIM(CAST(histology_final AS VARCHAR))) = 'ptc'
            OR LOWER(CAST(histology_final AS VARCHAR)) LIKE 'ptc %'
            OR LOWER(CAST(histology_final AS VARCHAR)) LIKE 'ptc,%' THEN TRUE
        WHEN regexp_matches(
            LOWER(CAST(histology_final AS VARCHAR)),
            'carcinoma|metastatic|lymphoma|sarcoma|malignant'
        ) THEN TRUE
        ELSE FALSE
    END AS keyword_malignant_flag,
    CASE
        WHEN histology_final IS NULL OR TRIM(CAST(histology_final AS VARCHAR)) = '' THEN FALSE
        WHEN LOWER(CAST(histology_final AS VARCHAR)) LIKE '%niftp%' THEN TRUE
        ELSE FALSE
    END AS keyword_niftp_flag
FROM manuscript_cohort_v1;


-- ═══════════════════════════════════════════════════════════════════════════
-- Pathology bucket: structured first, then keyword fallback (never tumor-only denom)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW pathology_bucket_logic_v2 AS
SELECT
    CAST(mc.research_id AS INTEGER) AS research_id,
    tp.histology_1_type AS histology_1_type_structured,
    mc.histology_final,
    CASE
        WHEN tp.histology_1_type IS NOT NULL THEN 'structured_histology_1_type'
        WHEN mc.histology_final IS NOT NULL AND TRIM(mc.histology_final) <> '' THEN 'histology_final_text'
        ELSE 'missing'
    END AS pathology_primary_source,
    CASE
        WHEN tp.histology_1_type IS NOT NULL AND LOWER(tp.histology_1_type) LIKE '%niftp%' THEN 'NIFTP'
        WHEN tp.histology_1_type IS NOT NULL AND (
            tp.histology_1_type IN ('PTC', 'FTC', 'MTC', 'PDTC', 'ATC', 'HCC')
            OR LOWER(CAST(tp.histology_1_type AS VARCHAR)) LIKE '%carcinoma%'
        ) THEN 'Malignant_non_NIFTP'
        WHEN tp.histology_1_type IS NOT NULL AND (
            LOWER(CAST(tp.histology_1_type AS VARCHAR)) IN (
                'benign', 'hyperplasia', 'adenoma', 'nodular', 'colloid', 'lymphocytic', 'hashimoto'
            )
            OR LOWER(CAST(tp.histology_1_type AS VARCHAR)) LIKE '%adenoma%'
        ) THEN 'Benign_structured'
        WHEN tp.histology_1_type IS NOT NULL THEN 'Other_structured'
        WHEN hf.keyword_niftp_flag THEN 'NIFTP'
        WHEN hf.keyword_malignant_flag IS TRUE THEN 'Malignant_non_NIFTP'
        WHEN hf.keyword_malignant_flag IS FALSE THEN 'Benign_negative_keyword'
        ELSE NULL
    END AS pathology_final_bucket
FROM manuscript_cohort_v1 mc
LEFT JOIN tumor_pathology_patient_v2 tp ON CAST(tp.research_id AS INTEGER) = CAST(mc.research_id AS INTEGER)
LEFT JOIN histology_final_keyword_flags_v2 hf ON hf.research_id = CAST(mc.research_id AS INTEGER);


-- ═══════════════════════════════════════════════════════════════════════════
-- Index indeterminate FNA (first Bethesda III/IV FNA on/after era start)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW index_indeterminate_fna_34_v2 AS
WITH mc34 AS (
    SELECT CAST(research_id AS INTEGER) AS research_id, fna_bethesda_final
    FROM manuscript_cohort_v1
    WHERE fna_bethesda_final IN (3, 4)
),
ranked_fna AS (
    SELECT
        f.research_id,
        f.fna_episode_id,
        f.resolved_fna_date,
        f.date_status AS fna_date_status,
        f.bethesda_category AS index_bethesda_at_dx,
        ROW_NUMBER() OVER (
            PARTITION BY f.research_id
            ORDER BY f.resolved_fna_date ASC, f.fna_episode_id ASC
        ) AS rn
    FROM fna_episode_master_v2 f
    INNER JOIN mc34 mc ON mc.research_id = f.research_id
    WHERE f.bethesda_category IN (3, 4)
      AND f.resolved_fna_date >= DATE '2015-01-01'
)
SELECT
    research_id,
    fna_episode_id AS index_fna_episode_id,
    resolved_fna_date AS index_indeterminate_fna_date,
    fna_date_status,
    index_bethesda_at_dx
FROM ranked_fna
WHERE rn = 1;


CREATE OR REPLACE VIEW index_indeterminate_fna_5_v2 AS
WITH mc5 AS (
    SELECT CAST(research_id AS INTEGER) AS research_id, fna_bethesda_final
    FROM manuscript_cohort_v1
    WHERE fna_bethesda_final = 5
),
ranked_fna AS (
    SELECT
        f.research_id,
        f.fna_episode_id,
        f.resolved_fna_date,
        f.date_status AS fna_date_status,
        f.bethesda_category AS index_bethesda_at_dx,
        ROW_NUMBER() OVER (
            PARTITION BY f.research_id
            ORDER BY f.resolved_fna_date ASC, f.fna_episode_id ASC
        ) AS rn
    FROM fna_episode_master_v2 f
    INNER JOIN mc5 mc ON mc.research_id = f.research_id
    WHERE f.bethesda_category = 5
      AND f.resolved_fna_date >= DATE '2015-01-01'
)
SELECT
    research_id,
    fna_episode_id AS index_fna_episode_id,
    resolved_fna_date AS index_indeterminate_fna_date,
    fna_date_status,
    index_bethesda_at_dx
FROM ranked_fna
WHERE rn = 1;


-- ═══════════════════════════════════════════════════════════════════════════
-- Molecular test date helper (preserves date_status from episode table)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW molecular_ts_af_date_v2 AS
SELECT
    research_id,
    molecular_episode_id,
    platform,
    overall_result_class,
    inadequate_flag,
    cancelled_flag,
    date_status AS molecular_date_status,
    date_confidence AS molecular_date_confidence,
    COALESCE(TRY_CAST(resolved_test_date AS DATE), test_date_native) AS test_date_effective,
    braf_flag,
    ras_flag,
    ras_subtype,
    ret_flag,
    ret_fusion_flag,
    tert_flag,
    ntrk_flag,
    fusion_flag,
    tp53_flag,
    high_risk_marker_flag
FROM molecular_test_episode_v2
WHERE platform IN ('ThyroSeq', 'Afirma');


-- ═══════════════════════════════════════════════════════════════════════════
-- A. Primary denominator + test linkage (Bethesda III/IV, 2015+ index FNA)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW all_eligible_indeterminate_v2 AS
WITH idx AS (
    SELECT * FROM index_indeterminate_fna_34_v2
),
mc AS (
    SELECT *
    FROM manuscript_cohort_v1
    WHERE fna_bethesda_final IN (3, 4)
),
mol_after_index AS (
    SELECT
        i.research_id,
        m.molecular_episode_id,
        m.platform,
        m.overall_result_class,
        m.molecular_date_status,
        m.molecular_date_confidence,
        m.test_date_effective,
        m.inadequate_flag,
        m.cancelled_flag,
        m.braf_flag,
        m.ras_flag,
        m.ras_subtype,
        m.ret_flag,
        m.ret_fusion_flag,
        m.tert_flag,
        m.ntrk_flag,
        m.fusion_flag,
        m.tp53_flag,
        m.high_risk_marker_flag,
        ROW_NUMBER() OVER (
            PARTITION BY i.research_id
            ORDER BY m.test_date_effective DESC NULLS LAST, m.molecular_episode_id DESC
        ) AS rn_last
    FROM idx i
    INNER JOIN molecular_ts_af_date_v2 m ON m.research_id = i.research_id
    WHERE m.test_date_effective IS NOT NULL
      AND m.test_date_effective >= i.index_indeterminate_fna_date
),
mol_latest AS (
    SELECT * FROM mol_after_index WHERE rn_last = 1
),
mol_preop AS (
    SELECT
        i.research_id,
        m.molecular_episode_id,
        m.platform,
        m.overall_result_class,
        m.test_date_effective,
        m.molecular_date_status,
        ROW_NUMBER() OVER (
            PARTITION BY i.research_id
            ORDER BY m.test_date_effective DESC NULLS LAST, m.molecular_episode_id DESC
        ) AS rn_preop
    FROM idx i
    INNER JOIN mc ON CAST(mc.research_id AS INTEGER) = i.research_id
    INNER JOIN molecular_ts_af_date_v2 m ON m.research_id = i.research_id
    WHERE mc.surg_first_date IS NOT NULL
      AND m.test_date_effective IS NOT NULL
      AND m.test_date_effective <= CAST(mc.surg_first_date AS DATE)
      AND m.test_date_effective >= i.index_indeterminate_fna_date
),
mol_preop_one AS (
    SELECT * FROM mol_preop WHERE rn_preop = 1
)
SELECT
    CAST(idx.research_id AS INTEGER) AS research_id,
    idx.index_fna_episode_id,
    idx.index_indeterminate_fna_date,
    idx.fna_date_status,
    idx.index_bethesda_at_dx,
    CAST(mc.fna_bethesda_final AS INTEGER) AS manuscript_bethesda_final,
    YEAR(idx.index_indeterminate_fna_date) AS index_year,
    idx.index_indeterminate_fna_date >= DATE '2018-01-01' AS sensitivity_era_2018,
    COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) AS size_cm_any,
    CASE
        WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) IS NULL THEN 'Unknown'
        WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) < 2 THEN '<2 cm'
        WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) <= 4 THEN '2–4 cm'
        ELSE '>4 cm'
    END AS size_stratum,
    mc.surg_first_date,
    mc.histology_final,
    pb.pathology_primary_source,
    pb.pathology_final_bucket,
    pb.histology_1_type_structured,
    (mol_latest.molecular_episode_id IS NOT NULL) AS ever_tested_ts_af_after_index,
    mol_latest.molecular_episode_id AS latest_molecular_episode_id,
    mol_latest.platform AS latest_test_platform,
    mol_latest.overall_result_class AS latest_overall_result_class,
    mol_latest.molecular_date_status AS latest_molecular_date_status,
    mol_latest.molecular_date_confidence AS latest_molecular_date_confidence,
    mol_latest.test_date_effective AS latest_test_date,
    COALESCE(map.result_bucket, 'Inconclusive') AS latest_result_bucket,
    (mol_preop_one.molecular_episode_id IS NOT NULL) AS preop_tested_among_surgeries,
    mol_preop_one.platform AS preop_test_platform,
    mol_preop_one.test_date_effective AS preop_test_date,
    mol_preop_one.molecular_date_status AS preop_molecular_date_status,
    COALESCE(map_preop.result_bucket, 'Inconclusive') AS preop_result_bucket,
    mol_latest.braf_flag AS latest_braf_flag,
    mol_latest.ras_flag AS latest_ras_flag,
    mol_latest.ras_subtype AS latest_ras_subtype,
    mol_latest.ret_fusion_flag AS latest_ret_fusion_flag,
    mol_latest.tert_flag AS latest_tert_flag,
    mol_latest.ntrk_flag AS latest_ntrk_flag,
    mol_latest.fusion_flag AS latest_fusion_flag,
    mol_latest.tp53_flag AS latest_tp53_flag,
    mol_latest.high_risk_marker_flag AS latest_high_risk_marker_flag
FROM idx
INNER JOIN mc ON CAST(mc.research_id AS INTEGER) = idx.research_id
LEFT JOIN pathology_bucket_logic_v2 pb ON CAST(pb.research_id AS INTEGER) = idx.research_id
LEFT JOIN mol_latest ON mol_latest.research_id = idx.research_id
LEFT JOIN mol_preop_one ON mol_preop_one.research_id = idx.research_id
LEFT JOIN mol_result_class_map_v1 map
    ON LOWER(TRIM(COALESCE(mol_latest.overall_result_class, ''))) = map.overall_result_class
LEFT JOIN mol_result_class_map_v1 map_preop
    ON LOWER(TRIM(COALESCE(mol_preop_one.overall_result_class, ''))) = map_preop.overall_result_class;


-- ═══════════════════════════════════════════════════════════════════════════
-- B. Tested subset (ThyroSeq/Afirma any time on/after index FNA)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW tested_indeterminate_v2 AS
SELECT * FROM all_eligible_indeterminate_v2
WHERE ever_tested_ts_af_after_index;


-- ═══════════════════════════════════════════════════════════════════════════
-- C. Operated nested cohort (primary Bethesda III/IV; surgery date present)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW operated_indeterminate_v2 AS
SELECT *
FROM all_eligible_indeterminate_v2
WHERE surg_first_date IS NOT NULL;


-- ═══════════════════════════════════════════════════════════════════════════
-- D. Bethesda V secondary (parallel patient spine, 2015+ index FNA)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW bethesdaV_secondary_v2 AS
WITH idx AS (
    SELECT * FROM index_indeterminate_fna_5_v2
),
mc AS (
    SELECT * FROM manuscript_cohort_v1 WHERE CAST(fna_bethesda_final AS INTEGER) = 5
),
mol_after_index AS (
    SELECT
        i.research_id,
        m.molecular_episode_id,
        m.platform,
        m.overall_result_class,
        m.molecular_date_status,
        m.test_date_effective,
        m.braf_flag,
        m.ras_flag,
        m.tert_flag,
        ROW_NUMBER() OVER (
            PARTITION BY i.research_id
            ORDER BY m.test_date_effective DESC NULLS LAST, m.molecular_episode_id DESC
        ) AS rn_last
    FROM idx i
    INNER JOIN molecular_ts_af_date_v2 m ON m.research_id = i.research_id
    WHERE m.test_date_effective IS NOT NULL
      AND m.test_date_effective >= i.index_indeterminate_fna_date
),
mol_latest AS (SELECT * FROM mol_after_index WHERE rn_last = 1),
mol_preop AS (
    SELECT
        i.research_id,
        m.molecular_episode_id,
        m.platform,
        m.overall_result_class,
        m.test_date_effective,
        ROW_NUMBER() OVER (
            PARTITION BY i.research_id
            ORDER BY m.test_date_effective DESC NULLS LAST, m.molecular_episode_id DESC
        ) AS rn_preop
    FROM idx i
    INNER JOIN mc ON CAST(mc.research_id AS INTEGER) = i.research_id
    INNER JOIN molecular_ts_af_date_v2 m ON m.research_id = i.research_id
    WHERE mc.surg_first_date IS NOT NULL
      AND m.test_date_effective IS NOT NULL
      AND m.test_date_effective <= CAST(mc.surg_first_date AS DATE)
      AND m.test_date_effective >= i.index_indeterminate_fna_date
),
mol_preop_one AS (SELECT * FROM mol_preop WHERE rn_preop = 1)
SELECT
    CAST(idx.research_id AS INTEGER) AS research_id,
    idx.index_fna_episode_id,
    idx.index_indeterminate_fna_date,
    idx.fna_date_status,
    idx.index_bethesda_at_dx,
    5 AS manuscript_bethesda_final,
    YEAR(idx.index_indeterminate_fna_date) AS index_year,
    idx.index_indeterminate_fna_date >= DATE '2018-01-01' AS sensitivity_era_2018,
    COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) AS size_cm_any,
    CASE
        WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) IS NULL THEN 'Unknown'
        WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) < 2 THEN '<2 cm'
        WHEN COALESCE(mc.path_tumor_size_cm, mc.imaging_nodule_size_cm) <= 4 THEN '2–4 cm'
        ELSE '>4 cm'
    END AS size_stratum,
    mc.surg_first_date,
    mc.histology_final,
    pb.pathology_primary_source,
    pb.pathology_final_bucket,
    pb.histology_1_type_structured,
    (mol_latest.molecular_episode_id IS NOT NULL) AS ever_tested_ts_af_after_index,
    mol_latest.platform AS latest_test_platform,
    mol_latest.test_date_effective AS latest_test_date,
    COALESCE(map.result_bucket, 'Inconclusive') AS latest_result_bucket,
    (mol_preop_one.molecular_episode_id IS NOT NULL) AS preop_tested_among_surgeries,
    COALESCE(map_preop.result_bucket, 'Inconclusive') AS preop_result_bucket
FROM idx
INNER JOIN mc ON CAST(mc.research_id AS INTEGER) = idx.research_id
LEFT JOIN pathology_bucket_logic_v2 pb ON pb.research_id = idx.research_id
LEFT JOIN mol_latest ON mol_latest.research_id = idx.research_id
LEFT JOIN mol_preop_one ON mol_preop_one.research_id = idx.research_id
LEFT JOIN mol_result_class_map_v1 map
    ON LOWER(TRIM(COALESCE(mol_latest.overall_result_class, ''))) = map.overall_result_class
LEFT JOIN mol_result_class_map_v1 map_preop
    ON LOWER(TRIM(COALESCE(mol_preop_one.overall_result_class, ''))) = map_preop.overall_result_class;


-- ═══════════════════════════════════════════════════════════════════════════
-- E. Patient vs episode grain + v3 molecular link coverage (Bethesda III/IV era)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW episode_sensitivity_v2 AS
WITH mc34 AS (
    SELECT CAST(research_id AS INTEGER) AS research_id
    FROM manuscript_cohort_v1
    WHERE fna_bethesda_final IN (3, 4)
),
episodes AS (
    SELECT f.research_id, f.fna_episode_id
    FROM fna_episode_master_v2 f
    INNER JOIN mc34 mc ON mc.research_id = f.research_id
    WHERE f.bethesda_category IN (3, 4)
      AND f.resolved_fna_date >= DATE '2015-01-01'
),
ep_mol_v3 AS (
    SELECT e.research_id, e.fna_episode_id
    FROM episodes e
    INNER JOIN fna_molecular_linkage_v3 l
        ON l.research_id = e.research_id AND l.fna_episode_id = e.fna_episode_id
    WHERE l.score_rank = 1
      AND l.analysis_eligible_link_flag
      AND l.platform IN ('ThyroSeq', 'Afirma')
),
patient_test AS (
    SELECT research_id FROM all_eligible_indeterminate_v2 WHERE ever_tested_ts_af_after_index
)
SELECT 'patient_grain_eligible_2015' AS cohort_label,
       COUNT(*)::BIGINT AS n_rows,
       COUNT(DISTINCT research_id)::BIGINT AS n_patients,
       NULL::BIGINT AS n_episodes
FROM all_eligible_indeterminate_v2
UNION ALL
SELECT 'patient_grain_tested_ts_af', COUNT(*)::BIGINT, COUNT(DISTINCT research_id)::BIGINT, NULL::BIGINT
FROM tested_indeterminate_v2
UNION ALL
SELECT 'episode_grain_el eligible_fna_34_2015', COUNT(*)::BIGINT,
       COUNT(DISTINCT research_id)::BIGINT, COUNT(*)::BIGINT
FROM episodes
UNION ALL
SELECT 'episode_grain_v3_molecular_linked', COUNT(*)::BIGINT,
       COUNT(DISTINCT research_id)::BIGINT, COUNT(*)::BIGINT
FROM ep_mol_v3
UNION ALL
SELECT 'patient_grain_temporal_tested_distinct',
       COUNT(DISTINCT research_id)::BIGINT,
       COUNT(DISTINCT research_id)::BIGINT,
       NULL::BIGINT
FROM patient_test
UNION ALL
SELECT 'patient_distinct_episode_v3_molecular_linked',
       COUNT(DISTINCT research_id)::BIGINT,
       NULL::BIGINT,
       NULL::BIGINT
FROM ep_mol_v3;


-- ═══════════════════════════════════════════════════════════════════════════
-- Manual review: benign/negative molecular → malignant/NIFTP surgical pathology
-- + suspicious/inconclusive molecular with pathology for audit
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW manual_review_molecular_path_mismatch_v2 AS
SELECT
    o.research_id,
    o.index_fna_episode_id,
    o.index_indeterminate_fna_date,
    o.manuscript_bethesda_final,
    o.latest_result_bucket,
    o.latest_overall_result_class,
    o.latest_test_platform,
    o.pathology_final_bucket,
    o.pathology_primary_source,
    o.histology_1_type_structured,
    o.histology_final,
    'benign_neg_mol_malignant_or_niftp_path' AS review_reason
FROM operated_indeterminate_v2 o
WHERE o.latest_result_bucket = 'Benign'
  AND o.pathology_final_bucket IN ('Malignant_non_NIFTP', 'NIFTP')
UNION ALL
SELECT
    o.research_id,
    o.index_fna_episode_id,
    o.index_indeterminate_fna_date,
    o.manuscript_bethesda_final,
    o.latest_result_bucket,
    o.latest_overall_result_class,
    o.latest_test_platform,
    o.pathology_final_bucket,
    o.pathology_primary_source,
    o.histology_1_type_structured,
    o.histology_final,
    'suspicious_inconclusive_mol_small_cell_audit'
FROM operated_indeterminate_v2 o
WHERE o.latest_result_bucket IN ('Suspicious', 'Inconclusive')
  AND o.pathology_final_bucket IS NOT NULL;


-- QA: strata with small counts (suppress in public tables <10 rule-of-thumb)
CREATE OR REPLACE VIEW qa_small_cell_strata_v2 AS
SELECT 'uptake_by_year_bethesda' AS analysis,
       CAST(index_year AS VARCHAR) AS stratum_key,
       CAST(index_bethesda_at_dx AS VARCHAR) AS stratum_key2,
       COUNT(*) AS n
FROM all_eligible_indeterminate_v2
GROUP BY 1, 2, 3
HAVING COUNT(*) < 10
UNION ALL
SELECT 'platform_mix_by_test_year',
       CAST(YEAR(latest_test_date) AS VARCHAR),
       COALESCE(latest_test_platform, ''),
       COUNT(*)
FROM tested_indeterminate_v2
WHERE latest_test_date IS NOT NULL
GROUP BY 1, 2, 3
HAVING COUNT(*) < 10;


-- ═══════════════════════════════════════════════════════════════════════════
-- Analyses 1–9 (primary Bethesda III/IV all_eligible; operated for pathology/extent)
-- ═══════════════════════════════════════════════════════════════════════════

-- 1) Testing uptake by index year × Bethesda (index episode category)
CREATE OR REPLACE VIEW ana_v2_testing_uptake_by_year_bethesda AS
SELECT
    index_year,
    index_bethesda_at_dx AS bethesda_category,
    COUNT(*) AS n_eligible,
    SUM(CASE WHEN ever_tested_ts_af_after_index THEN 1 ELSE 0 END) AS n_tested,
    ROUND(100.0 * SUM(CASE WHEN ever_tested_ts_af_after_index THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_tested
FROM all_eligible_indeterminate_v2
GROUP BY 1, 2
ORDER BY 1, 2;


-- 2) Platform mix by year (among tested)
CREATE OR REPLACE VIEW ana_v2_platform_mix_by_year AS
SELECT
    YEAR(latest_test_date) AS test_year,
    latest_test_platform AS platform,
    COUNT(*) AS n_patients
FROM tested_indeterminate_v2
WHERE latest_test_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;


-- 3) Result-class mix by platform (among tested)
CREATE OR REPLACE VIEW ana_v2_result_mix_by_platform AS
WITH t AS (SELECT * FROM tested_indeterminate_v2), tot AS (
    SELECT latest_test_platform AS platform, COUNT(*) AS denom FROM t GROUP BY 1
)
SELECT
    t.latest_test_platform AS platform,
    COALESCE(t.latest_result_bucket, 'Inconclusive') AS result_bucket,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / NULLIF(tot.denom, 0), 2) AS col_pct_within_platform
FROM t
JOIN tot ON tot.platform = t.latest_test_platform
GROUP BY 1, 2, tot.denom
ORDER BY 1, 2;


-- 4) Surgery rates: tested vs never-tested (descriptive; all eligible)
CREATE OR REPLACE VIEW ana_v2_surgery_rates_tested_vs_never AS
SELECT
    CASE WHEN ever_tested_ts_af_after_index THEN 'Tested' ELSE 'Never-tested' END AS testing_group,
    COUNT(*) AS n_patients,
    SUM(CASE WHEN surg_first_date IS NOT NULL THEN 1 ELSE 0 END) AS n_surgery,
    ROUND(100.0 * SUM(CASE WHEN surg_first_date IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_surgery
FROM all_eligible_indeterminate_v2
GROUP BY 1
ORDER BY 1;


-- 5) Pathology distribution among operated (structured/keyword spine)
CREATE OR REPLACE VIEW ana_v2_pathology_among_operated AS
SELECT
    COALESCE(pathology_final_bucket, 'Missing/Unknown') AS pathology_bucket,
    COUNT(*) AS n
FROM operated_indeterminate_v2
WHERE surg_first_date IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;


-- 6) Surgery extent + completion thyroidectomy (operated III/IV; first op + completion pattern)
CREATE OR REPLACE VIEW ana_v2_surgery_extent_operated AS
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
        t1.pr LIKE '%hemithyroid%'       OR t1.pr LIKE '%thyroid lobectomy%' OR t1.pr LIKE '%lobectomy%'
    )
      AND t1.pr NOT LIKE '%total%'
      AND (
            t2.pr LIKE '%completion%'
            OR t2.pr LIKE '%total thyroid%'
            OR t2.pr LIKE '%total thyroidectomy%'
        )
)
SELECT
    COALESCE(fe.procedure_normalized, 'unknown') AS first_procedure_normalized,
    COUNT(*) AS n_patients,
    SUM(CASE WHEN fe.procedure_normalized = 'hemithyroidectomy' THEN 1 ELSE 0 END) AS n_hemithyroid,
    SUM(CASE WHEN fe.procedure_normalized = 'total_thyroidectomy' THEN 1 ELSE 0 END) AS n_total_primary,
    SUM(CASE WHEN COALESCE(c.completion_after_initial_lobe, FALSE) THEN 1 ELSE 0 END) AS n_completion_after_lobe
FROM operated_indeterminate_v2 o
LEFT JOIN first_ep fe ON fe.research_id = o.research_id
LEFT JOIN completion c ON c.research_id = o.research_id
GROUP BY 1
ORDER BY 2 DESC;


-- 7) Mutation-family summary (tested cohort; latest test flags)
CREATE OR REPLACE VIEW ana_v2_mutation_families_tested AS
SELECT
    COUNT(*) AS n_tested_patients,
    SUM(CASE WHEN latest_braf_flag THEN 1 ELSE 0 END) AS n_braf,
    SUM(CASE WHEN latest_ras_flag THEN 1 ELSE 0 END) AS n_ras,
    SUM(CASE WHEN latest_ret_fusion_flag THEN 1 ELSE 0 END) AS n_ret_fusion,
    SUM(CASE WHEN latest_tert_flag THEN 1 ELSE 0 END) AS n_tert,
    SUM(CASE WHEN latest_ntrk_flag THEN 1 ELSE 0 END) AS n_ntrk,
    SUM(CASE WHEN latest_fusion_flag THEN 1 ELSE 0 END) AS n_any_fusion_flag,
    SUM(CASE WHEN latest_tp53_flag THEN 1 ELSE 0 END) AS n_tp53,
    SUM(CASE WHEN latest_high_risk_marker_flag THEN 1 ELSE 0 END) AS n_high_risk_marker
FROM tested_indeterminate_v2;


-- 8) Bethesda III vs IV — duplicate primary tables split
CREATE OR REPLACE VIEW ana_v2_testing_uptake_by_year_bethesda3 AS
SELECT * FROM ana_v2_testing_uptake_by_year_bethesda WHERE bethesda_category = 3;

CREATE OR REPLACE VIEW ana_v2_testing_uptake_by_year_bethesda4 AS
SELECT * FROM ana_v2_testing_uptake_by_year_bethesda WHERE bethesda_category = 4;

CREATE OR REPLACE VIEW ana_v2_pathology_among_operated_bethesda3 AS
SELECT COALESCE(pathology_final_bucket, 'Missing/Unknown') AS pathology_bucket, COUNT(*) AS n
FROM operated_indeterminate_v2
WHERE index_bethesda_at_dx = 3
GROUP BY 1 ORDER BY 2 DESC;

CREATE OR REPLACE VIEW ana_v2_pathology_among_operated_bethesda4 AS
SELECT COALESCE(pathology_final_bucket, 'Missing/Unknown') AS pathology_bucket, COUNT(*) AS n
FROM operated_indeterminate_v2
WHERE index_bethesda_at_dx = 4
GROUP BY 1 ORDER BY 2 DESC;


-- 9) Bethesda V secondary — uptake + platform (exploratory)
CREATE OR REPLACE VIEW ana_v2_bethesda5_uptake_by_year AS
SELECT
    index_year,
    COUNT(*) AS n_eligible,
    SUM(CASE WHEN ever_tested_ts_af_after_index THEN 1 ELSE 0 END) AS n_tested,
    ROUND(100.0 * SUM(CASE WHEN ever_tested_ts_af_after_index THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_tested
FROM bethesdaV_secondary_v2
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW ana_v2_bethesda5_platform_by_year AS
SELECT
    YEAR(latest_test_date) AS test_year,
    latest_test_platform AS platform,
    COUNT(*) AS n
FROM bethesdaV_secondary_v2
WHERE ever_tested_ts_af_after_index AND latest_test_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;


-- Sensitivity era 2018+ slice (primary analytic duplication)
CREATE OR REPLACE VIEW ana_v2_testing_uptake_by_year_bethesda_sens2018 AS
SELECT
    index_year,
    index_bethesda_at_dx AS bethesda_category,
    COUNT(*) AS n_eligible,
    SUM(CASE WHEN ever_tested_ts_af_after_index THEN 1 ELSE 0 END) AS n_tested,
    ROUND(100.0 * SUM(CASE WHEN ever_tested_ts_af_after_index THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_tested
FROM all_eligible_indeterminate_v2
WHERE sensitivity_era_2018
GROUP BY 1, 2
ORDER BY 1, 2;