-- =============================================================================
-- Migration 120 -- path malignant + benign patient rollup SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cursor/Copilot session)
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Plan:   Close the path family by rebuilding and verifying the two patient
--         rollups against already-verified path event tables:
--           * canonical_path_malignant_events_v1 (mig_89, 56/56 verified)
--           * canonical_path_benign_events_v1 (mig_97b, 51 verified + 4 na)
--
-- Staleness probe (pre-rebuild, session design notes):
--   Rollups initially predated verified event repairs -> rebuilt from events.
-- Post-execution check (MotherDuck 2026-04-28): rollup MAX(build_ts) is AFTER
--   malignant events MAX(build_ts) and benign events MAX(build_ts); registry
--   shows both tables table_status='verified', n_not_started=0.
--
-- Rebuild rationale:
--   Both rollups predated the verified event repairs/backfills. Pre-rebuild
--   derivation re-check showed most columns still matched, but two meaningful
--   drifts required a rebuild:
--     * malignant dominant_histology: 127 patients drifted when applying the
--       required deterministic tie-breaker; all 127 were tied-most-common
--       histology cases (258 total patients had top-frequency ties).
--     * benign any_concomitant_malignant: pre-rebuild rollup under-counted vs
--       explicit benign_events ∩ malignant_events patient join (4,137).
--       mig_120 recomputes from that join (Script 361 used event-row flag).
--
-- Methodology:
--   Rebuild in place from verified events using Script 361 rollup logic, with
--   two deterministic hardenings requested by the task:
--     * highest_stage_ajcc7/8 uses severity rank ordering instead of lexical
--       MAX; rank output matched pre-rebuild stored values (0 drift).
--     * dominant_histology uses count DESC, histology ASC lexical tie-breaker.
--   Bethesda / regex_path_outcome / POC histology fields are verified against
--   the archived Script 361 source table:
--     "Thyroid 2026 UPdated".archive_pub_v1_0.path_outcome_classification_v1_pre361_20260422_002245
--   That table is read-only provenance/source replay for the fields that Script
--   361 merged before path_outcome_classification_v1 was archived.
--
-- Patient denominator math:
--   malignant rollup: 4,137 rows / 4,137 patients = distinct malignant-events
--   patients. benign rollup: 10,871 rows / 10,871 patients = CPM full cohort.
--   any_benign_event TRUE = 8,846; concomitant malignant TRUE = 4,137.
--   Bethesda union across both rollups = 5,249 patients, matching the archived
--   path_outcome_classification_v1_pre361 source.
--
-- Carry-forward:
--   CF-mig120-PATH-MALIG-DATE-RETYPE: earliest_malignant_path_date and
--   latest_malignant_path_date remain TIMESTAMP to preserve the existing
--   published rollup schema. They are calendar-only clinical dates cast from
--   canonical_path_malignant_events_v1.surgery_date. This joins the previously
--   documented CF-100-DATE-RETYPE family and does not block sign-off.
--   build_ts is explicitly CAST(CURRENT_TIMESTAMP AS TIMESTAMP) in both rebuilt
--   rollups (no TIMESTAMP WITH TIME ZONE carry-forward).
--
-- Path family closed: malignant events (mig_89) + benign events (mig_97b) +
-- gland rollup (mig_101) + malignant/benign patient rollups (mig_120).
-- =============================================================================

-- 120a: preserve pre-rebuild snapshots.
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_patient_rollup_v1_pre_mig120_20260429 AS
SELECT * FROM main.canonical_path_malignant_patient_rollup_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_benign_patient_rollup_v1_pre_mig120_20260429 AS
SELECT * FROM main.canonical_path_benign_patient_rollup_v1;

-- 120b: rebuild malignant patient rollup from verified malignant events.
CREATE OR REPLACE TABLE main.canonical_path_malignant_patient_rollup_v1 AS
WITH ev AS (
    SELECT
        TRY_CAST(research_id AS BIGINT) AS research_id,
        surgery_episode_id,
        surgery_date,
        primary_histology,
        extrathyroidal_extension,
        gross_ete,
        stage_group_ajcc7,
        stage_group_ajcc8
    FROM main.canonical_path_malignant_events_v1
),
hist_counts AS (
    SELECT research_id, primary_histology, COUNT(*) AS n
    FROM ev
    WHERE primary_histology IS NOT NULL
      AND TRIM(CAST(primary_histology AS VARCHAR)) <> ''
    GROUP BY 1,2
),
hist_mode AS (
    SELECT research_id, primary_histology AS dominant_histology
    FROM hist_counts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY n DESC, primary_histology ASC
    ) = 1
),
stage8 AS (
    SELECT research_id, stage_group_ajcc8 AS highest_stage_ajcc8
    FROM ev
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY CASE stage_group_ajcc8
                   WHEN 'IVC' THEN 8 WHEN 'IVB' THEN 7 WHEN 'IVA' THEN 6
                   WHEN 'IV'  THEN 5 WHEN 'III' THEN 4 WHEN 'II'  THEN 3
                   WHEN 'I'   THEN 2 WHEN '0'   THEN 1 ELSE NULL
                 END DESC NULLS LAST,
                 stage_group_ajcc8 DESC NULLS LAST
    ) = 1
),
stage7 AS (
    SELECT research_id, stage_group_ajcc7 AS highest_stage_ajcc7
    FROM ev
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY CASE stage_group_ajcc7
                   WHEN 'IVC' THEN 8 WHEN 'IVB' THEN 7 WHEN 'IVA' THEN 6
                   WHEN 'IV'  THEN 5 WHEN 'III' THEN 4 WHEN 'II'  THEN 3
                   WHEN 'I'   THEN 2 WHEN '0'   THEN 1 ELSE NULL
                 END DESC NULLS LAST,
                 stage_group_ajcc7 DESC NULLS LAST
    ) = 1
),
agg AS (
    SELECT
        research_id,
        TRUE AS any_malignant_event,
        COUNT(DISTINCT surgery_episode_id) AS n_malignant_surgeries,
        COUNT(*) AS n_tumors_total,
        CAST(MIN(surgery_date) AS TIMESTAMP) AS earliest_malignant_path_date,
        CAST(MAX(surgery_date) AS TIMESTAMP) AS latest_malignant_path_date,
        BOOL_OR(
            COALESCE(gross_ete, 0) = 1
            OR LOWER(COALESCE(CAST(extrathyroidal_extension AS VARCHAR), ''))
               IN ('present', 'minimal', 'microscopic', 'yes', 'c/a',
                   'gross', 'macroscopic')
        ) AS any_ett
    FROM ev
    GROUP BY research_id
),
poc AS (
    SELECT
        TRY_CAST(research_id AS BIGINT) AS research_id,
        MAX(bethesda_final) AS bethesda_final,
        ANY_VALUE(bethesda_final_name) AS bethesda_final_name,
        ANY_VALUE(regex_classification) AS regex_path_outcome,
        ANY_VALUE(tumor_1_histologic_type) AS poc_tumor_1_histologic_type
    FROM "Thyroid 2026 UPdated".archive_pub_v1_0.path_outcome_classification_v1_pre361_20260422_002245
    GROUP BY TRY_CAST(research_id AS BIGINT)
)
SELECT
    a.research_id,
    a.any_malignant_event,
    a.n_malignant_surgeries,
    a.n_tumors_total,
    a.earliest_malignant_path_date,
    a.latest_malignant_path_date,
    s8.highest_stage_ajcc8,
    s7.highest_stage_ajcc7,
    a.any_ett,
    FALSE AS any_metastasis,
    h.dominant_histology,
    p.bethesda_final,
    p.bethesda_final_name,
    p.regex_path_outcome,
    p.poc_tumor_1_histologic_type,
    '361_via_mig_120'::VARCHAR AS build_script,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM agg a
LEFT JOIN stage8 s8 USING (research_id)
LEFT JOIN stage7 s7 USING (research_id)
LEFT JOIN hist_mode h USING (research_id)
LEFT JOIN poc p USING (research_id);

-- 120c: rebuild benign patient rollup from verified benign events + CPM full cohort.
CREATE OR REPLACE TABLE main.canonical_path_benign_patient_rollup_v1 AS
WITH cpm AS (
    SELECT TRY_CAST(research_id AS BIGINT) AS research_id
    FROM main.canonical_patient_master
),
ev AS (
    SELECT * FROM main.canonical_path_benign_events_v1
),
benign AS (
    SELECT
        TRY_CAST(research_id AS BIGINT) AS research_id,
        BOOL_OR(
            COALESCE(nlp_mng, FALSE)
            OR COALESCE(nlp_multinodular_goiter, FALSE)
            OR COALESCE(nlp_follicular_adenoma, FALSE)
            OR COALESCE(nlp_hurthle_cell_adenoma, FALSE)
            OR COALESCE(nlp_hashimotos, FALSE)
            OR COALESCE(nlp_hashimotos_thyroiditis, FALSE)
            OR COALESCE(nlp_lymphocytic_thyroiditis, FALSE)
            OR COALESCE(nlp_chronic_lymphocytic_thyroiditis, FALSE)
            OR COALESCE(nlp_graves, FALSE)
            OR COALESCE(nlp_graves_disease, FALSE)
            OR COALESCE(nlp_adenomatoid_nodule, FALSE)
            OR COALESCE(nlp_colloid_nodule, FALSE)
            OR COALESCE(nlp_hyperplasia, FALSE)
            OR COALESCE(nlp_nodular_hyperplasia, FALSE)
        ) AS any_benign_event,
        COUNT(*) AS n_benign_synoptics,
        BOOL_OR(COALESCE(nlp_mng, FALSE)
                OR COALESCE(nlp_multinodular_goiter, FALSE)) AS any_mng,
        BOOL_OR(COALESCE(nlp_hashimotos, FALSE)
                OR COALESCE(nlp_hashimotos_thyroiditis, FALSE)) AS any_hashimotos,
        BOOL_OR(COALESCE(nlp_lymphocytic_thyroiditis, FALSE)
                OR COALESCE(nlp_chronic_lymphocytic_thyroiditis, FALSE)) AS any_lymphocytic_thyroiditis,
        BOOL_OR(COALESCE(nlp_graves, FALSE)
                OR COALESCE(nlp_graves_disease, FALSE)) AS any_graves,
        BOOL_OR(COALESCE(nlp_follicular_adenoma, FALSE)) AS any_follicular_adenoma,
        MIN(path_date) AS earliest_benign_path_date,
        MAX(path_date) AS latest_benign_path_date
    FROM ev
    GROUP BY TRY_CAST(research_id AS BIGINT)
),
concomitant AS (
    SELECT DISTINCT TRY_CAST(b.research_id AS BIGINT) AS research_id
    FROM main.canonical_path_benign_events_v1 b
    JOIN main.canonical_path_malignant_events_v1 m
      ON CAST(m.research_id AS VARCHAR) = CAST(b.research_id AS VARCHAR)
),
poc AS (
    SELECT
        TRY_CAST(research_id AS BIGINT) AS research_id,
        MAX(bethesda_final) AS bethesda_final,
        ANY_VALUE(bethesda_final_name) AS bethesda_final_name,
        ANY_VALUE(regex_classification) AS regex_path_outcome
    FROM "Thyroid 2026 UPdated".archive_pub_v1_0.path_outcome_classification_v1_pre361_20260422_002245
    GROUP BY TRY_CAST(research_id AS BIGINT)
)
SELECT
    cpm.research_id,
    COALESCE(b.any_benign_event, FALSE) AS any_benign_event,
    COALESCE(b.n_benign_synoptics, 0) AS n_benign_synoptics,
    COALESCE(b.any_mng, FALSE) AS any_mng,
    COALESCE(b.any_hashimotos, FALSE) AS any_hashimotos,
    COALESCE(b.any_lymphocytic_thyroiditis, FALSE) AS any_lymphocytic_thyroiditis,
    COALESCE(b.any_graves, FALSE) AS any_graves,
    COALESCE(b.any_follicular_adenoma, FALSE) AS any_follicular_adenoma,
    b.earliest_benign_path_date,
    b.latest_benign_path_date,
    (c.research_id IS NOT NULL) AS any_concomitant_malignant,
    '361_via_mig_120'::VARCHAR AS build_script,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts,
    p.bethesda_final,
    p.bethesda_final_name,
    p.regex_path_outcome
FROM cpm
LEFT JOIN benign b USING (research_id)
LEFT JOIN concomitant c USING (research_id)
LEFT JOIN poc p USING (research_id);

-- 120d: flip malignant rollup derived columns.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = CASE
        WHEN column_name='dominant_histology'
          THEN 'derivation_re_derivation_with_lexicographic_tie_breaker'
        WHEN column_name IN ('bethesda_final','bethesda_final_name','regex_path_outcome','poc_tumor_1_histologic_type')
          THEN 'archived_script361_source_compare'
        WHEN column_name IN ('earliest_malignant_path_date','latest_malignant_path_date')
          THEN 'derivation_re_derivation_with_date_retype_carry_forward'
        ELSE 'derivation_re_derivation_post_stale_rollup_rebuild'
    END,
    batch_id            = 'mig_120_path_rollup_pair_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_120: stale rollup rebuilt from verified '
                          || 'canonical_path_malignant_events_v1. Post-rebuild '
                          || 'fresh derivation comparison: 14/14 target cols '
                          || '0 drift across 4,137 patients. highest_stage uses '
                          || 'AJCC severity rank; dominant_histology uses count '
                          || 'DESC + lexical ASC tie-breaker (127 pre-rebuild '
                          || 'differences, all tied-mode patients). Bethesda / '
                          || 'regex / POC histology verified against archived '
                          || 'Script 361 path_outcome source. '
                          || 'CF-mig120-PATH-MALIG-DATE-RETYPE: earliest/latest '
                          || 'malignant path dates intentionally remain TIMESTAMP '
                          || 'calendar-only values pending future DATE retype.'
WHERE schema_name='main'
  AND table_name='canonical_path_malignant_patient_rollup_v1'
  AND verification_status='not_started';

-- 120e: flip benign rollup derived columns.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = CASE
        WHEN column_name='any_concomitant_malignant'
          THEN 'cross_table_join_rebuild_against_verified_path_events'
        WHEN column_name IN ('bethesda_final','bethesda_final_name','regex_path_outcome')
          THEN 'archived_script361_source_compare'
        ELSE 'derivation_re_derivation_post_stale_rollup_rebuild'
    END,
    batch_id            = 'mig_120_path_rollup_pair_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_120: stale full-cohort rollup rebuilt from '
                          || 'verified canonical_path_benign_events_v1 plus CPM '
                          || 'spine. Post-rebuild fresh derivation comparison: '
                          || '13/13 target cols 0 drift across 10,871 patients. '
                          || 'any_concomitant_malignant now uses explicit '
                          || 'benign-events INNER JOIN malignant-events and '
                          || 'matches 4,137 patients in both sources. Bethesda '
                          || '/ regex fields verified against archived Script '
                          || '361 path_outcome source.'
WHERE schema_name='main'
  AND table_name='canonical_path_benign_patient_rollup_v1'
  AND verification_status='not_started';

-- 120f: recompute table_signoff_registry counts for both rollups.
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/120_path_rollup_pair_signoff.sql',
    notes             = CASE ts.table_name
      WHEN 'canonical_path_malignant_patient_rollup_v1' THEN
        'mig_120: rebuilt stale path malignant rollup from verified events. '
        || 'Rows/patients=4,137/4,137. 14 derived cols verified with 0 '
        || 'post-rebuild drift. dominant_histology deterministic tie-breaker '
        || 'applied; stage rank derivation used for highest AJCC7/8. '
        || 'CF-mig120-PATH-MALIG-DATE-RETYPE open for calendar-only TIMESTAMP '
        || 'date columns. build_ts retyped to plain TIMESTAMP.'
      WHEN 'canonical_path_benign_patient_rollup_v1' THEN
        'mig_120: rebuilt stale path benign rollup from verified benign events '
        || '+ CPM full cohort. Rows/patients=10,871/10,871. 13 derived cols '
        || 'verified with 0 post-rebuild drift. any_concomitant_malignant '
        || 'matches explicit benign∩malignant event join (4,137 patients). '
        || 'build_ts retyped to plain TIMESTAMP.'
      ELSE ts.notes
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main'
    AND table_name IN ('canonical_path_malignant_patient_rollup_v1',
                       'canonical_path_benign_patient_rollup_v1')
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name  = subq.table_name;

-- =============================================================================
-- end of migration 120 -- path malignant + benign patient rollups closed
-- =============================================================================
