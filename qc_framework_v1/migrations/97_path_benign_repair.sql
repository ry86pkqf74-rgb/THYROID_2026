-- =============================================================================
-- Migration 97 -- canonical_path_benign_events_v1 structural repair + sign-off
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with GitHub Copilot)
-- Batch:  mig_97_path_benign_repair_20260428
--
-- Live build_script: 396 (scripts/396_specimen_master_repair.py Phase C),
-- originally inheriting Script 361 Step 2 semantics.
--
-- Repairs
-- -------
-- 1) surgery_episode_id: backfill deterministic operative date matches for
--    benign path rows where specimen_master linkage did not provide an episode.
--    Uses verified canonical_operative_events_v1 and excludes
--    date_status='opnote_clustered' historical-date artifacts. Preflight found
--    2,643/2,656 NULL rows match within <=7 days (2,619 exact-day + 24 within
--    7 days; no same-distance ambiguities).
-- 2) has_concomitant_malignant_event: recompute from same-episode OR path-date
--    within +/-30 days of canonical_path_malignant_events_v1.surgery_date.
--    This only reads the malignant canonical; it does not modify it.
-- 3) synoptic_row_ix: path_synoptics has no synoptic_row_ix column in the live
--    canonical DB, and Scripts 361/396 intentionally left this NULL rather than
--    synthesizing a fake Script-108 pandas-load-order index. Register as failed
--    carry-forward CF-PATH-BENIGN-SYNOPTIC-ROW-IX.
-- 4) zero-prevalence placeholder NLP flags: nlp_normal_thyroid, nlp_nifcp,
--    nlp_nifp, nlp_nifpt marked NA (not present in cohort vocabulary).
-- 5) all remaining cols verified by mechanical derivation/source comparison or
--    auto provenance/identifier status.
-- =============================================================================

BEGIN TRANSACTION;

CREATE OR REPLACE TEMP TABLE mig_97_op_dates AS
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  surgery_episode_id,
  COALESCE(
    TRY_STRPTIME(CAST(resolved_surgery_date AS VARCHAR), '%m/%d/%Y')::DATE,
    TRY_CAST(surgery_date_native AS DATE)
  ) AS op_date
FROM main.canonical_operative_events_v1
WHERE surgery_episode_id IS NOT NULL
  AND COALESCE(CAST(date_status AS VARCHAR), '') <> 'opnote_clustered';

CREATE OR REPLACE TEMP TABLE mig_97_episode_backfill AS
SELECT *
FROM (
  SELECT
    b.research_id,
    b.synoptic_row_ord,
    op.surgery_episode_id AS new_surgery_episode_id,
    ABS(DATE_DIFF('day', b.path_date, op.op_date)) AS abs_day_diff,
    COUNT(*) OVER (
      PARTITION BY b.research_id, b.synoptic_row_ord,
                   ABS(DATE_DIFF('day', b.path_date, op.op_date))
    ) AS n_at_same_distance,
    ROW_NUMBER() OVER (
      PARTITION BY b.research_id, b.synoptic_row_ord
      ORDER BY ABS(DATE_DIFF('day', b.path_date, op.op_date)) ASC,
               op.surgery_episode_id ASC
    ) AS rn
  FROM main.canonical_path_benign_events_v1 b
  JOIN mig_97_op_dates op
    ON op.research_id = CAST(b.research_id AS VARCHAR)
   AND b.path_date IS NOT NULL
   AND op.op_date IS NOT NULL
   AND ABS(DATE_DIFF('day', b.path_date, op.op_date)) <= 7
  WHERE b.surgery_episode_id IS NULL
) ranked
WHERE rn = 1
  AND n_at_same_distance = 1;

UPDATE main.canonical_path_benign_events_v1 AS b
SET surgery_episode_id = bf.new_surgery_episode_id,
    linkage_quality = CASE
      WHEN b.specimen_id IS NOT NULL THEN 'full'
      ELSE 'specimen_only'
    END
FROM mig_97_episode_backfill bf
WHERE b.research_id = bf.research_id
  AND b.synoptic_row_ord = bf.synoptic_row_ord;

UPDATE main.canonical_path_benign_events_v1 AS b
SET has_concomitant_malignant_event = EXISTS (
  SELECT 1
  FROM main.canonical_path_malignant_events_v1 m
  WHERE CAST(m.research_id AS VARCHAR) = CAST(b.research_id AS VARCHAR)
    AND (
      m.surgery_episode_id IS NOT DISTINCT FROM b.surgery_episode_id
      OR (
        b.path_date IS NOT NULL
        AND ABS(
          DATE_DIFF(
            'day',
            b.path_date,
            COALESCE(
              TRY_STRPTIME(CAST(m.surgery_date AS VARCHAR), '%m/%d/%Y')::DATE,
              TRY_CAST(m.surgery_date AS DATE)
            )
          )
        ) <= 30
      )
    )
);

-- Failed carry-forward: no live source column exists to populate Script-108
-- global pandas-load-order synoptic_row_ix for benign rows.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'failed',
    verified_by         = 'logan',
    verification_method = 'carry_forward_source_absent',
    upstream_source     = 'main.path_synoptics (no synoptic_row_ix column); Scripts 361/396 intentionally left canonical_path_benign_events_v1.synoptic_row_ix NULL rather than synthesizing ROW_NUMBER',
    batch_id            = 'mig_97_path_benign_repair_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes, '')
                          || ' | mig_97: CF-PATH-BENIGN-SYNOPTIC-ROW-IX remains open. path_synoptics has no synoptic_row_ix; do not synthesize Script-108 pandas-load-order index via SQL ROW_NUMBER.'
WHERE schema_name='main'
  AND table_name='canonical_path_benign_events_v1'
  AND column_name='synoptic_row_ix';

-- Zero-prevalence / no-vocabulary placeholder NLP flags.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verified_by         = 'logan',
    verification_method = 'not_applicable_zero_prevalence',
    upstream_source     = 'Scripts 361/396 BENIGN_FLAG_MAP placeholder; no path_synoptics source column and 0 TRUE rows in cohort',
    batch_id            = 'mig_97_path_benign_repair_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes, '')
                          || ' | mig_97: not present in cohort vocabulary; placeholder-FALSE flag with 0 TRUE rows.'
WHERE schema_name='main'
  AND table_name='canonical_path_benign_events_v1'
  AND column_name IN ('nlp_normal_thyroid','nlp_nifcp','nlp_nifp','nlp_nifpt');

-- NLP flags that are real path_synoptics-derived structured markers.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    upstream_source     = 'main.path_synoptics structured benign/thyroiditis source columns; Scripts 361/396 _benign_flag_select rule',
    batch_id            = 'mig_97_path_benign_repair_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes, '')
                          || ' | mig_97: 100% mechanical match to Scripts 361/396 structured path_synoptics flag rule; sample audit summary written to verification_csvs/canonical_path_benign_events_v1/nlp_flag_verification_summary_mig_97.csv.'
WHERE schema_name='main'
  AND table_name='canonical_path_benign_events_v1'
  AND column_name LIKE 'nlp_%'
  AND column_name NOT IN ('nlp_normal_thyroid','nlp_nifcp','nlp_nifp','nlp_nifpt');

-- Linkage/source/derived/provenance columns (everything except the failed
-- synoptic_row_ix and the zero-prevalence placeholder flags above).
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = CASE
      WHEN column_name IN ('build_script','build_ts','source_table')
        THEN 'auto_no_source_counterpart'
      WHEN column_name IN ('research_id','specimen_id','accession_or_source_id','source_report_id')
        THEN 'mechanical_source_compare'
      ELSE 'mechanical_derivation_compare'
    END,
    upstream_source     = CASE
      WHEN column_name IN ('build_script','build_ts','source_table')
        THEN 'pipeline provenance constants from scripts/396_specimen_master_repair.py Phase C'
      WHEN column_name IN ('research_id','path_date','synoptic_row_ord','source_report_id','source_text_type')
        THEN 'main.path_synoptics with Scripts 361/396 deterministic within-patient ordering'
      WHEN column_name IN ('specimen_id','accession_or_source_id','sm_n_specimens_for_date','linkage_ambiguous_multi_specimen')
        THEN 'main.specimen_master_v1 picked one row per (research_id, procedure_date_day) using lowest specimen_id'
      WHEN column_name IN ('surgery_episode_id','linkage_quality')
        THEN 'main.specimen_master_v1 plus mig_97 verified-operative date backfill from main.canonical_operative_events_v1'
      WHEN column_name='has_concomitant_malignant_event'
        THEN 'EXISTS same surgery_episode_id OR path_date within +/-30 days of main.canonical_path_malignant_events_v1.surgery_date'
      ELSE 'Scripts 361/396 derivation rule re-run / no source counterpart'
    END,
    batch_id            = 'mig_97_path_benign_repair_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes, '')
                          || ' | mig_97: verified by build-script rule re-run and targeted structural repair; surgery_episode_id NULLs reduced by deterministic operative-date backfill, concomitant malignancy flag recomputed.'
WHERE schema_name='main'
  AND table_name='canonical_path_benign_events_v1'
  AND column_name NOT LIKE 'nlp_%'
  AND column_name <> 'synoptic_row_ix';

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/97_path_benign_repair.sql'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_path_benign_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name  = subq.table_name;

COMMIT;

-- Expected post-state:
--   rows=11,688; patients=10,871;
--   surgery_episode_id NULLs 2,656 -> 13;
--   has_concomitant_malignant_event TRUE 1 -> ~4,375 rows;
--   registry: 50 verified / 4 na / 1 failed (synoptic_row_ix carry-forward).
-- =============================================================================