-- =============================================================================
-- Migration 85 -- canonical_path_malignant_events_v1.surgery_date VERIFIED
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   First per-column verification of canonical_path_malignant_events_v1
--         under Protocol v2. Column 1 = surgery_date (the natural-key date
--         anchor for all subsequent mechanical_source_compare columns).
--
-- Method: mechanical_source_compare
-- Source: raw/All Diagnoses & synoptic 12_1_2025.xlsx > 'synoptics + Dx merged'
--         (mirrored at main.path_synoptics.surg_date)
-- Join:   per-canonical-row EXISTS check on (research_id, surgery_date)
--         against path_synoptics. Both sides are TIMESTAMP; exact equality.
--
-- Result: 6,689 / 6,689 MATCH (100 %). Zero MISMATCH / NO_SOURCE_MATCH /
--         DB_NULL_SOURCE_HAS / SOURCE_NULL_DB_HAS. Logan-approved one-click
--         sign-off per Protocol v2 §6 Step C.
--
-- Audit:  verification_csvs/canonical_path_malignant_events_v1/
--           surgery_date__mig_85.csv (6,689 rows + preamble + header)
-- Build:  qc_framework_v1/scripts/
--           build_path_malignant_surgery_date_review.py
--
-- No row-level data writes; no canonical_logan_review_log_v1 entries
-- (no Logan corrections were needed).
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 85a: flip surgery_date to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_source_compare',
    upstream_source     = 'main.path_synoptics.surg_date',
    batch_id            = 'mig_85_path_malignant_surgery_date',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_85: 6,689/6,689 MATCH against path_synoptics.surg_date '
                          || 'on (research_id, surgery_date). TIMESTAMP exact-equality compare. '
                          || 'No mismatches, no NULLs, no missing-source rows. CSV: '
                          || 'verification_csvs/canonical_path_malignant_events_v1/surgery_date__mig_85.csv'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_v1'
  AND column_name = 'surgery_date';

-- 85b: recompute table_signoff_registry counts
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_path_malignant_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 85 -- surgery_date verified; column 1 of 47 closed
-- Table progress: 1 verified / 47 not_started (-1) / 9 na = 56 total
-- =============================================================================
