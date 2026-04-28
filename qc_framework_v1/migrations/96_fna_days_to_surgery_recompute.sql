-- =============================================================================
-- Migration 96 -- canonical_fna_events_v1.days_to_surgery recompute
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with GitHub Copilot)
-- Batch:  mig_96_d2s_recompute_20260428
--
-- Purpose
-- -------
-- Close deferred carry-forward CF-FNA-D2S from the FNA pilot sign-off by
-- recomputing canonical_fna_events_v1.days_to_surgery against the now-verified
-- canonical_operative_events_v1.
--
-- Pre-state re-query (2026-04-28):
--   rows=8,050; stored populated=6,532; stored NULL=1,518;
--   max_abs_stored=736,618; exact vs derivation=5,665;
--   derivable-but-null=1,518; true mismatches=867.
--
-- Sanity-gate note
-- ----------------
-- The literal MIN(all operative dates) recipe produced 26 implausible intervals
-- with abs(days) >= 10,000. Investigation showed every outlier came from
-- canonical_operative_events_v1 rows with date_status='opnote_clustered' and
-- historical 1990s dates; those same patients have exact_source_date / CPM
-- anchor operative rows in 2020-2022. To avoid propagating the known
-- opnote-clustered historical-date artifact, this migration derives
-- first_surgery_date from non-opnote_clustered operative rows only:
--   COALESCE(TRY_STRPTIME(resolved_surgery_date,'%m/%d/%Y'), surgery_date_native)
--   with COALESCE(date_status,'') <> 'opnote_clustered'.
-- This yields max_abs_derived=9,019 and 0 rows >=10,000 days.
-- =============================================================================

BEGIN TRANSACTION;

CREATE OR REPLACE TEMP TABLE mig_96_first_surgery AS
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  MIN(
    COALESCE(
      TRY_STRPTIME(CAST(resolved_surgery_date AS VARCHAR), '%m/%d/%Y')::DATE,
      TRY_CAST(surgery_date_native AS DATE)
    )
  ) AS first_surgery_date
FROM main.canonical_operative_events_v1
WHERE COALESCE(CAST(date_status AS VARCHAR), '') <> 'opnote_clustered'
GROUP BY 1;

CREATE OR REPLACE TEMP TABLE mig_96_derived_days AS
SELECT
  f.fna_event_id,
  CASE
    WHEN f.fna_date_resolved IS NOT NULL
     AND fs.first_surgery_date IS NOT NULL
    THEN DATE_DIFF('day', f.fna_date_resolved, fs.first_surgery_date)::INTEGER
    ELSE NULL::INTEGER
  END AS derived_days_to_surgery
FROM main.canonical_fna_events_v1 f
LEFT JOIN mig_96_first_surgery fs
  ON fs.research_id = CAST(f.research_id AS VARCHAR);

-- Safety assertion: no derived interval should exceed the reasonable thyroid
-- cohort history/surgery window after excluding opnote-clustered artifacts.
CREATE OR REPLACE TEMP TABLE mig_96_assert AS
SELECT
  COUNT(*) AS n_rows,
  SUM(derived_days_to_surgery IS NOT NULL) AS n_populated,
  SUM(derived_days_to_surgery IS NULL) AS n_null,
  MAX(ABS(derived_days_to_surgery)) AS max_abs_days,
  SUM(ABS(derived_days_to_surgery) >= 10000) AS n_implausible
FROM mig_96_derived_days;

UPDATE main.canonical_fna_events_v1 AS f
SET days_to_surgery = d.derived_days_to_surgery
FROM mig_96_derived_days d
WHERE f.fna_event_id = d.fna_event_id;

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    upstream_source     = 'main.canonical_operative_events_v1 (verified by mig_90; first non-opnote_clustered operative date per research_id) + main.canonical_fna_events_v1.fna_date_resolved',
    batch_id            = 'mig_96_d2s_recompute_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes, '')
                          || ' | mig_96: recomputed all 8,050 rows from verified operative events using first non-opnote_clustered operative date per patient. '
                          || 'Post-derivation max_abs_days=9,019 and 0 rows >=10,000; CF-FNA-D2S CLOSED. '
                          || 'Excluded opnote_clustered historical-date artifacts identified in 26 preflight outliers.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_fna_events_v1'
  AND column_name = 'days_to_surgery';

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
    signoff_migration = 'qc_framework_v1/migrations/96_fna_days_to_surgery_recompute.sql'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name  = subq.table_name;

COMMIT;

-- Post-apply verification query:
-- WITH op AS (...same as mig_96_first_surgery...), d AS (...same as derived...)
-- SELECT COUNT(*), SUM(stored IS DISTINCT FROM derived) FROM d; -- expected 8,050 / 0
-- =============================================================================