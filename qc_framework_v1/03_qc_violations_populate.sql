-- =====================================================================
-- qc_framework_v1 / 03_qc_violations_populate.sql
--
-- Target DB:     thyroid_canonical_publication_v1_0
-- Target schema: manuscript_workspace
-- Source data:   main.manuscript_cohort_v1 (patient),
--                main.canonical_us_nodule_v2 (nodule),
--                main.canonical_path_malignant_events_v1 (tumor/surgery),
--                main.canonical_fna_events_v1 (FNA episode),
--                main.recurrence_event_clean_v1 (recurrence event)
--
-- Idempotent: TRUNCATE both tables, then repopulate.
-- =====================================================================

TRUNCATE TABLE manuscript_workspace.qc_violations_v1;
TRUNCATE TABLE manuscript_workspace.qc_event_issues_v1;


-- =====================================================================
-- Patient-grain rules on manuscript_cohort_v1
-- =====================================================================

-- LN01: ln_positive_final > path_ln_examined_raw
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'LN01_POSITIVE_GT_EXAMINED', 'critical', 'ln',
       CONCAT('pos=', ln_positive_final, ' exam=', path_ln_examined_raw)
FROM main.manuscript_cohort_v1
WHERE ln_positive_final IS NOT NULL
  AND path_ln_examined_raw IS NOT NULL
  AND ln_positive_final > path_ln_examined_raw;

-- LN02: pos > 0 with examined 0 or null
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'LN02_POSITIVE_WITHOUT_EXAMINED', 'critical', 'ln',
       CONCAT('pos=', ln_positive_final,
              ' exam=', COALESCE(CAST(path_ln_examined_raw AS VARCHAR),'NULL'))
FROM main.manuscript_cohort_v1
WHERE ln_positive_final IS NOT NULL
  AND ln_positive_final > 0
  AND COALESCE(path_ln_examined_raw, 0) = 0;

-- LN03: raw vs final disagree
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'LN03_RAW_VS_FINAL_DISAGREE', 'warning', 'ln',
       CONCAT('raw=', path_ln_positive_raw, ' final=', ln_positive_final)
FROM main.manuscript_cohort_v1
WHERE path_ln_positive_raw IS NOT NULL
  AND ln_positive_final IS NOT NULL
  AND path_ln_positive_raw <> ln_positive_final;

-- LN04: both null (info)
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'LN04_LN_DATA_MISSING', 'info', 'ln',
       'ln_positive_final & path_ln_examined_raw both NULL'
FROM main.manuscript_cohort_v1
WHERE ln_positive_final IS NULL
  AND path_ln_examined_raw IS NULL;

-- REC02: flag without date
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'REC02_FLAG_WITHOUT_DATE', 'warning', 'recurrence',
       'any_recurrence_flag=TRUE, recurrence_date NULL'
FROM main.manuscript_cohort_v1
WHERE COALESCE(any_recurrence_flag, FALSE) = TRUE
  AND recurrence_date IS NULL;

-- REC03: date without flag
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'REC03_DATE_WITHOUT_FLAG', 'warning', 'recurrence',
       CONCAT('date=', CAST(recurrence_date AS VARCHAR),
              ' flag=', COALESCE(CAST(any_recurrence_flag AS VARCHAR),'NULL'))
FROM main.manuscript_cohort_v1
WHERE recurrence_date IS NOT NULL
  AND COALESCE(any_recurrence_flag, FALSE) = FALSE;

-- SURG01: any disagreement among the three surgery date columns when non-null
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'SURG01_DATE_DIVERGENCE', 'critical', 'surgery',
       CONCAT('first_surgery_date=', CAST(first_surgery_date AS VARCHAR),
              ' surg_first_date=',  CAST(surg_first_date  AS VARCHAR),
              ' surgery_date=',     CAST(surgery_date     AS VARCHAR))
FROM main.manuscript_cohort_v1
WHERE (first_surgery_date IS NOT NULL AND surg_first_date IS NOT NULL
       AND first_surgery_date <> surg_first_date)
   OR (first_surgery_date IS NOT NULL AND surgery_date IS NOT NULL
       AND first_surgery_date <> surgery_date)
   OR (surg_first_date IS NOT NULL AND surgery_date IS NOT NULL
       AND surg_first_date <> surgery_date);

-- SURG02: all three identical (info / schema smell)
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'SURG02_TRIPLE_DATE_DUPLICATE', 'info', 'surgery',
       'first_surgery_date = surg_first_date = surgery_date'
FROM main.manuscript_cohort_v1
WHERE first_surgery_date IS NOT NULL
  AND surg_first_date IS NOT NULL
  AND surgery_date IS NOT NULL
  AND first_surgery_date = surg_first_date
  AND surg_first_date = surgery_date;

-- HIST01: whitespace
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'HIST01_WHITESPACE', 'warning', 'histology',
       CONCAT('raw=[', histology_final, ']')
FROM main.manuscript_cohort_v1
WHERE histology_final IS NOT NULL
  AND histology_final <> TRIM(histology_final);

-- HIST02: PTC-like but not in canonical list
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'HIST02_UNNORMALIZED_VARIANT', 'warning', 'histology',
       CONCAT('histology_final=[', histology_final, ']')
FROM main.manuscript_cohort_v1
WHERE histology_final ILIKE '%PTC%'
  AND TRIM(histology_final) NOT IN (
      'PTC','PTC classical','PTC follicular variant','PTC tall cell variant',
      'PTC columnar cell variant','PTC diffuse sclerosing variant',
      'PTC hobnail variant','PTC oncocytic variant','PTC solid variant',
      'PTC cribriform-morular variant','PTMC'
  );

-- HIST03: metastatic prefix
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'HIST03_METASTATIC_PREFIX', 'warning', 'histology',
       CONCAT('histology_final=[', histology_final, ']')
FROM main.manuscript_cohort_v1
WHERE histology_final ILIKE 'metastatic %';

-- AJCC02: cohort-level calculable but N stage null
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, details)
SELECT research_id, 'AJCC02_COHORT_CALCULABLE_BUT_N_NULL', 'warning', 'staging',
       CONCAT('ajcc8_calculable_flag=TRUE, ajcc8_n_stage=NULL ',
              '(missing=', COALESCE(ajcc8_missing_components,'unspecified'), ')')
FROM main.manuscript_cohort_v1
WHERE ajcc8_calculable_flag = TRUE
  AND ajcc8_n_stage IS NULL;


-- =====================================================================
-- Cross-table rules (joined, aggregated to patient-grain)
-- research_id types differ:
--   manuscript_cohort_v1.research_id = BIGINT
--   recurrence_event_clean_v1.research_id = VARCHAR
--   canonical_fna_events_v1.research_id   = VARCHAR
-- Cast to BIGINT via TRY_CAST to avoid a hard failure on malformed IDs.
-- =====================================================================

-- REC01: recurrence before surgery
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, n_events, details)
SELECT
    c.research_id,
    'REC01_RECURRENCE_BEFORE_SURGERY',
    'critical',
    'recurrence',
    COUNT(*),
    CONCAT('n_events=', COUNT(*),
           ' earliest_rec=', CAST(MIN(r.recurrence_date) AS VARCHAR),
           ' first_surg=',  CAST(c.first_surgery_date AS VARCHAR))
FROM main.manuscript_cohort_v1 c
JOIN main.recurrence_event_clean_v1 r
  ON TRY_CAST(r.research_id AS BIGINT) = c.research_id
WHERE c.first_surgery_date IS NOT NULL
  AND r.recurrence_date IS NOT NULL
  AND r.recurrence_date < c.first_surgery_date
GROUP BY c.research_id, c.first_surgery_date;

-- Drill-down rows for REC01
INSERT INTO manuscript_workspace.qc_event_issues_v1
    (research_id, rule_id, source_table, source_pk, details)
SELECT
    c.research_id,
    'REC01_RECURRENCE_BEFORE_SURGERY',
    'recurrence_event_clean_v1',
    CONCAT('event_rank=', r.event_rank, ' source=', r.source_table),
    CONCAT('rec=', CAST(r.recurrence_date AS VARCHAR),
           ' surg=', CAST(c.first_surgery_date AS VARCHAR),
           ' type=', COALESCE(r.recurrence_type,''))
FROM main.manuscript_cohort_v1 c
JOIN main.recurrence_event_clean_v1 r
  ON TRY_CAST(r.research_id AS BIGINT) = c.research_id
WHERE c.first_surgery_date IS NOT NULL
  AND r.recurrence_date IS NOT NULL
  AND r.recurrence_date < c.first_surgery_date;

-- FNA01: FNA dated AFTER first surgery (impossible for pre-op FNA)
INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, n_events, details)
SELECT
    c.research_id,
    'FNA01_FNA_AFTER_SURGERY',
    'critical',
    'temporal',
    COUNT(*),
    CONCAT('n_events=', COUNT(*),
           ' latest_fna=', CAST(MAX(f.fna_date_resolved) AS VARCHAR),
           ' first_surg=', CAST(c.first_surgery_date AS VARCHAR))
FROM main.manuscript_cohort_v1 c
JOIN main.canonical_fna_events_v1 f
  ON TRY_CAST(f.research_id AS BIGINT) = c.research_id
WHERE c.first_surgery_date IS NOT NULL
  AND f.fna_date_resolved IS NOT NULL
  AND CAST(f.fna_date_resolved AS TIMESTAMP) > c.first_surgery_date
GROUP BY c.research_id, c.first_surgery_date;

INSERT INTO manuscript_workspace.qc_event_issues_v1
    (research_id, rule_id, source_table, source_pk, details)
SELECT
    c.research_id,
    'FNA01_FNA_AFTER_SURGERY',
    'canonical_fna_events_v1',
    CONCAT('fna_event_id=', f.fna_event_id),
    CONCAT('fna=', CAST(f.fna_date_resolved AS VARCHAR),
           ' surg=', CAST(c.first_surgery_date AS VARCHAR),
           ' days_to_surg=', COALESCE(CAST(f.days_to_surgery AS VARCHAR),'NULL'))
FROM main.manuscript_cohort_v1 c
JOIN main.canonical_fna_events_v1 f
  ON TRY_CAST(f.research_id AS BIGINT) = c.research_id
WHERE c.first_surgery_date IS NOT NULL
  AND f.fna_date_resolved IS NOT NULL
  AND CAST(f.fna_date_resolved AS TIMESTAMP) > c.first_surgery_date;


-- =====================================================================
-- TIRADS rules on canonical_us_nodule_v2 (event-grain, aggregated)
-- Skips is_aggregate_row rows since those are intentionally summarized.
-- =====================================================================

-- TIR01: acr2017_tirads_points inconsistent with acr2017_tirads_category
--   Canonical ACR 2017 bands: 0=TR1, 2=TR2, 3=TR3, 4-6=TR4, 7+=TR5.
--   If your institutional mapping differs (e.g. 1-2=TR2), edit the CASE.
INSERT INTO manuscript_workspace.qc_event_issues_v1
    (research_id, rule_id, source_table, source_pk, details)
SELECT
    n.research_id,
    'TIR01_POINTS_CATEGORY_MISMATCH',
    'canonical_us_nodule_v2',
    CONCAT('nodule_id=', n.nodule_id, ' us_exam_id=', n.us_exam_id,
           ' idx=', n.nodule_index_within_exam),
    CONCAT('points=', n.acr2017_tirads_points,
           ' category=', n.acr2017_tirads_category,
           ' expected=',
           CASE
             WHEN n.acr2017_tirads_points = 0 THEN 'TR1'
             WHEN n.acr2017_tirads_points = 2 THEN 'TR2'
             WHEN n.acr2017_tirads_points = 3 THEN 'TR3'
             WHEN n.acr2017_tirads_points BETWEEN 4 AND 6 THEN 'TR4'
             WHEN n.acr2017_tirads_points >= 7 THEN 'TR5'
             ELSE 'UNDEFINED'
           END,
           ' resolution_rule=', COALESCE(n.resolution_rule,''))
FROM main.canonical_us_nodule_v2 n
WHERE COALESCE(n.is_aggregate_row, FALSE) = FALSE
  AND n.acr2017_tirads_points IS NOT NULL
  AND n.acr2017_tirads_category IS NOT NULL
  AND n.acr2017_tirads_category <>
      CASE
        WHEN n.acr2017_tirads_points = 0 THEN 'TR1'
        WHEN n.acr2017_tirads_points = 2 THEN 'TR2'
        WHEN n.acr2017_tirads_points = 3 THEN 'TR3'
        WHEN n.acr2017_tirads_points BETWEEN 4 AND 6 THEN 'TR4'
        WHEN n.acr2017_tirads_points >= 7 THEN 'TR5'
        ELSE NULL
      END;

INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, n_events, details)
SELECT
    research_id,
    'TIR01_POINTS_CATEGORY_MISMATCH',
    'warning',
    'tirads',
    COUNT(*),
    CONCAT('n_nodules=', COUNT(*))
FROM manuscript_workspace.qc_event_issues_v1
WHERE rule_id = 'TIR01_POINTS_CATEGORY_MISMATCH'
GROUP BY research_id;

-- TIR02: concordance flag is literally wrong
INSERT INTO manuscript_workspace.qc_event_issues_v1
    (research_id, rule_id, source_table, source_pk, details)
SELECT
    n.research_id,
    'TIR02_CONCORDANCE_FLAG_WRONG',
    'canonical_us_nodule_v2',
    CONCAT('nodule_id=', n.nodule_id, ' us_exam_id=', n.us_exam_id),
    CONCAT('acr2017=', n.acr2017_tirads_category,
           ' updated=', n.updated_tirads_category,
           ' flag=', CAST(n.acr2017_vs_updated_concordant AS VARCHAR))
FROM main.canonical_us_nodule_v2 n
WHERE COALESCE(n.is_aggregate_row, FALSE) = FALSE
  AND n.acr2017_vs_updated_concordant = FALSE
  AND n.acr2017_tirads_category IS NOT NULL
  AND n.updated_tirads_category IS NOT NULL
  AND n.acr2017_tirads_category = n.updated_tirads_category;

INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, n_events, details)
SELECT research_id, 'TIR02_CONCORDANCE_FLAG_WRONG', 'warning', 'tirads',
       COUNT(*), CONCAT('n_nodules=', COUNT(*))
FROM manuscript_workspace.qc_event_issues_v1
WHERE rule_id = 'TIR02_CONCORDANCE_FLAG_WRONG'
GROUP BY research_id;

-- TIR03: under-exploded multi-nodule exams
-- Signature: same (research_id, us_exam_id), many non-aggregate nodule rows,
-- many distinct reported TIRADS numbers, but the computed category collapses
-- to ≤2 distinct values. Concentrates on resolution_rule='inm_v1_only'.
INSERT INTO manuscript_workspace.qc_event_issues_v1
    (research_id, rule_id, source_table, source_pk, details)
WITH exam_stats AS (
    SELECT
        research_id,
        us_exam_id,
        COUNT(*) AS n_nodule_rows,
        COUNT(DISTINCT tirads_reported_in_text) AS n_distinct_reported,
        COUNT(DISTINCT acr2017_tirads_category) AS n_distinct_computed,
        COUNT(*) FILTER (WHERE resolution_rule = 'inm_v1_only')
            AS n_inm_v1_only,
        MAX(resolution_rule)                    AS any_resolution_rule,
        MAX(source_tables_cunc_legacy)          AS any_source_tables
    FROM main.canonical_us_nodule_v2
    WHERE COALESCE(is_aggregate_row, FALSE) = FALSE
      AND us_exam_id IS NOT NULL
    GROUP BY research_id, us_exam_id
)
SELECT
    research_id,
    'TIR03_MULTI_NODULE_UNDEREXPLODED',
    'canonical_us_nodule_v2',
    CONCAT('us_exam_id=', us_exam_id),
    CONCAT('n_rows=', n_nodule_rows,
           ' n_reported_tirads=', n_distinct_reported,
           ' n_computed_categories=', n_distinct_computed,
           ' n_inm_v1_only=', n_inm_v1_only,
           ' rule=', COALESCE(any_resolution_rule,''))
FROM exam_stats
WHERE n_nodule_rows >= 6
  AND n_distinct_reported >= 4
  AND n_distinct_computed <= 2;

INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, n_events, details)
SELECT research_id, 'TIR03_MULTI_NODULE_UNDEREXPLODED', 'critical', 'tirads',
       COUNT(*), CONCAT('n_exams=', COUNT(*))
FROM manuscript_workspace.qc_event_issues_v1
WHERE rule_id = 'TIR03_MULTI_NODULE_UNDEREXPLODED'
GROUP BY research_id;


-- =====================================================================
-- Pathology ETE + AJCC rules on canonical_path_malignant_events_v1
-- =====================================================================

-- ETE01: extrathyroidal_extension not in controlled vocabulary
INSERT INTO manuscript_workspace.qc_event_issues_v1
    (research_id, rule_id, source_table, source_pk, details)
SELECT
    p.research_id,
    'ETE01_NONNORMALIZED_STRING',
    'canonical_path_malignant_events_v1',
    CONCAT('surgery_episode_id=', p.surgery_episode_id,
           ' tumor_ordinal=', p.tumor_ordinal),
    CONCAT('ete=[', p.extrathyroidal_extension, ']',
           ' gross_ete=', COALESCE(CAST(p.gross_ete AS VARCHAR),'NULL'))
FROM main.canonical_path_malignant_events_v1 p
WHERE p.extrathyroidal_extension IS NOT NULL
  AND LOWER(TRIM(p.extrathyroidal_extension)) NOT IN (
      'none','absent','no','negative','not identified',
      'minimal','microscopic','focal',
      'gross','extensive','present','yes',
      'unknown','not applicable','not specified','n/a'
  );

INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, n_events, details)
SELECT research_id, 'ETE01_NONNORMALIZED_STRING', 'warning', 'pathology',
       COUNT(*), CONCAT('n_events=', COUNT(*))
FROM manuscript_workspace.qc_event_issues_v1
WHERE rule_id = 'ETE01_NONNORMALIZED_STRING'
GROUP BY research_id;

-- ETE02: gross_ete=1 but string says minimal/microscopic/focal
INSERT INTO manuscript_workspace.qc_event_issues_v1
    (research_id, rule_id, source_table, source_pk, details)
SELECT
    p.research_id,
    'ETE02_GROSS_FLAG_VS_STRING_INCONSISTENT',
    'canonical_path_malignant_events_v1',
    CONCAT('surgery_episode_id=', p.surgery_episode_id,
           ' tumor_ordinal=', p.tumor_ordinal),
    CONCAT('gross_ete=1 but ete=[', p.extrathyroidal_extension, ']')
FROM main.canonical_path_malignant_events_v1 p
WHERE p.gross_ete = 1
  AND LOWER(TRIM(COALESCE(p.extrathyroidal_extension,'')))
      IN ('minimal','microscopic','focal');

INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, n_events, details)
SELECT research_id, 'ETE02_GROSS_FLAG_VS_STRING_INCONSISTENT', 'critical', 'pathology',
       COUNT(*), CONCAT('n_events=', COUNT(*))
FROM manuscript_workspace.qc_event_issues_v1
WHERE rule_id = 'ETE02_GROSS_FLAG_VS_STRING_INCONSISTENT'
GROUP BY research_id;

-- AJCC01: event-grain calculable but n_stage null
INSERT INTO manuscript_workspace.qc_event_issues_v1
    (research_id, rule_id, source_table, source_pk, details)
SELECT
    p.research_id,
    'AJCC01_CALCULABLE_BUT_N_NULL',
    'canonical_path_malignant_events_v1',
    CONCAT('surgery_episode_id=', p.surgery_episode_id,
           ' tumor_ordinal=', p.tumor_ordinal),
    CONCAT('t=', COALESCE(p.t_stage_ajcc8,'NULL'),
           ' n=', COALESCE(p.n_stage_ajcc8,'NULL'),
           ' m=', COALESCE(p.m_stage_ajcc8,'NULL'))
FROM main.canonical_path_malignant_events_v1 p
WHERE p.ajcc8_stage_calculable_flag = TRUE
  AND p.n_stage_ajcc8 IS NULL;

INSERT INTO manuscript_workspace.qc_violations_v1
    (research_id, rule_id, severity, category, n_events, details)
SELECT research_id, 'AJCC01_CALCULABLE_BUT_N_NULL', 'warning', 'staging',
       COUNT(*), CONCAT('n_events=', COUNT(*))
FROM manuscript_workspace.qc_event_issues_v1
WHERE rule_id = 'AJCC01_CALCULABLE_BUT_N_NULL'
GROUP BY research_id;


-- ---- Final summary --------------------------------------------------
SELECT * FROM manuscript_workspace.qc_violations_summary_v1;
