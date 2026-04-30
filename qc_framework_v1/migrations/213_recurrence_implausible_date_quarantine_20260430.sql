-- mig_213 — canonical_recurrence_resolved_v1: ADD is_implausible_date_quarantine + UPDATE 132 rows
-- Batch_id: mig_213_recurrence_implausible_date_quarantine_20260430
-- Lane: B (Cline Sonnet 4.6) from CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md
-- Logan-ratified decision: imaging/path dates <1990 are implausible; negative days_to_path_proven
--   on path-proven rows is also implausible.
-- Pre-probe confirmed: 132 rows match criteria; spot-check rids 12057/10622/9182/8203 all present.
-- Base table: main.canonical_recurrence_resolved_v1 (10,871 rows, 19 cols pre-mig)
-- Database: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT APPLY (Logan-authorized via locked prompt 2026-04-30); no BEGIN TRANSACTION.
-- CAST(CURRENT_TIMESTAMP AS TIMESTAMP) for build_ts convention.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A Pre-snapshot: full table + col_registry + signoff_registry (audit trail)
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_mig213_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig213_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_recurrence_resolved_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig213_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig213_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name  = 'canonical_recurrence_resolved_v1';

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig213_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig213_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE table_name = 'canonical_recurrence_resolved_v1';

-- =============================================================================
-- §B ALTER TABLE — add quarantine flag (DEFAULT FALSE, idempotent-safe)
-- =============================================================================

ALTER TABLE main.canonical_recurrence_resolved_v1
  ADD COLUMN is_implausible_date_quarantine BOOLEAN DEFAULT FALSE;

-- =============================================================================
-- §C UPDATE — flag 132 implausible-date rows
-- Criteria (Logan-ratified):
--   1. recurrence_imaging_suspicious_date year < 1990
--   2. recurrence_path_proven_date year < 1990
--   3. recurrence_path_proven=TRUE AND days_to_path_proven < 0
-- =============================================================================

UPDATE main.canonical_recurrence_resolved_v1
SET is_implausible_date_quarantine = TRUE
WHERE EXTRACT(YEAR FROM recurrence_imaging_suspicious_date) < 1990
   OR EXTRACT(YEAR FROM recurrence_path_proven_date) < 1990
   OR (recurrence_path_proven = TRUE AND days_to_path_proven < 0);

-- =============================================================================
-- §D Verify post-update (read-only checks embedded as comments after apply)
-- Expected: quarantine_count=132; spot-check rids 12057/10622/9182/8203 all TRUE
-- =============================================================================
-- SELECT COUNT(*) FROM main.canonical_recurrence_resolved_v1
--   WHERE is_implausible_date_quarantine = TRUE;
-- -- Expect: 132
--
-- SELECT research_id, is_implausible_date_quarantine,
--        recurrence_imaging_suspicious_date, recurrence_path_proven_date,
--        recurrence_path_proven, days_to_path_proven
-- FROM main.canonical_recurrence_resolved_v1
-- WHERE research_id IN ('12057','10622','9182','8203')
-- ORDER BY research_id;
-- -- Expect: all 4 rows have is_implausible_date_quarantine = TRUE

-- =============================================================================
-- §E INSERT 1 col_registry row for is_implausible_date_quarantine
-- =============================================================================

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
VALUES (
  'main',
  'canonical_recurrence_resolved_v1',
  'is_implausible_date_quarantine',
  'BOOLEAN',
  20,
  'analytic',
  'canonical_recurrence_resolved_v1.recurrence_imaging_suspicious_date, recurrence_path_proven_date, recurrence_path_proven, days_to_path_proven',
  'verified',
  'mig_213',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'derivation_logan_ratified_pre_1990_dates_plus_path_proven_negative_days',
  'mig_213_recurrence_implausible_date_quarantine_20260430',
  'mig_213 Lane B: quarantine flag for 132 rows with implausible recurrence dates. Criteria: imaging_date<1990 OR path_proven_date<1990 OR (path_proven=TRUE AND days_to_path_proven<0). Logan-ratified 2026-04-30. Time-dependent recurrence analyses MUST add WHERE is_implausible_date_quarantine=FALSE. Spot-check: rids 12057 (year 202 path date), 10622 (1950 path date), 9182 (-87 days path_proven), 8203 (-18 days path_proven) all flagged.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
);

-- =============================================================================
-- §F UPDATE signoff_registry: n_columns_total 19→20, n_verified 16→17
-- =============================================================================

UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = 20,
    n_verified      = 17,
    signoff_migration = 'qc_framework_v1/migrations/213_recurrence_implausible_date_quarantine_20260430.sql',
    notes = COALESCE(notes, '')
            || ' | mig_213 2026-04-30: ADD is_implausible_date_quarantine BOOLEAN DEFAULT FALSE; UPDATE 132 implausible-date rows (pre_1990 imaging/path dates + path_proven=TRUE AND days_to_path_proven<0). Logan-ratified. n_cols 19->20, n_verified 16->17. Time-dependent analyses must filter WHERE is_implausible_date_quarantine=FALSE.'
WHERE table_name = 'canonical_recurrence_resolved_v1';

-- =============================================================================
-- §G INSERT provenance row
-- =============================================================================

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
VALUES (
  'canonical_cleanup_mig213_recurrence_quarantine_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'lane_b_alter_add_quarantine_flag_update_132_rows_register_col',
  '0', '0', '1',
  '0 | mig_213 Lane B: ALTER TABLE canonical_recurrence_resolved_v1 ADD COLUMN is_implausible_date_quarantine BOOLEAN DEFAULT FALSE; UPDATE 132 rows (pre-1990 imaging/path dates + path_proven=TRUE AND days_to_path_proven<0). Col registry +1 (verified). Signoff registry n_cols 19->20, n_verified 16->17. Logan-ratified 2026-04-30. Spot-check rids 12057/10622/9182/8203 all flagged. Time-dependent recurrence analyses must add WHERE is_implausible_date_quarantine=FALSE.'
);
