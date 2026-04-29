-- =============================================================================
-- Migration 117 -- Audit drift reconciliation (post-Cursors 8 + 9)
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Reconcile gate-5 audit drift introduced by recently-verified tables
--         (Cursor 8 labs family mig_115 + Cursor 9 molecular_v2 mig_116).
--         At mig_109 close (37 verified tables), gate 5 was 0. After Cursors
--         8 + 9 + Cowork mig_110/111/112/113/114 = 51 verified, gate 5 jumped
--         to 10 violations.
--
-- Investigation (run 2026-04-29 via Cowork query):
--   10 violations split cleanly into 2 buckets:
--
--   Bucket A (6 cols): Provenance / build / ingestion timestamps that should
--     have been on the audit allowlist from the start.
--     - canonical_molecular_genetics_v2.built_at         (TIMESTAMP, single 2026-04-21)
--     - canonical_labs_calcium_v1.ingestion_date         (TIMESTAMP, single 2026-04-21)
--     - canonical_labs_pth_v1.ingestion_date             (TIMESTAMP, single 2026-04-21)
--     - canonical_labs_thyroglobulin_v1.ingestion_date   (TIMESTAMP, single 2026-04-21)
--     - canonical_labs_tsh_v1.ingestion_date             (TIMESTAMP, single 2026-04-21)
--     - canonical_labs_vitamin_d_v1.ingestion_date       (TIMESTAMP, single 2026-04-21)
--     Each table has only one distinct day (the build date), confirming these
--     are pipeline timestamps not clinical events. Disposition: extend the
--     audit allowlist with `built_at` and `ingestion_date`.
--
--   Bucket B (4 cols): Clinical date columns stored as VARCHAR or TIMESTAMP
--     instead of DATE. Per `feedback_clinical_dates_calendar_only.md`
--     (Logan-ratified 2026-04-28), clinical dates MUST be DATE. These are
--     real CFs joining the existing CF-100-DATE-RETYPE bucket.
--     - canonical_pathology_clinical_events_v1.note_date     (VARCHAR)
--     - canonical_cervical_ln_clinical_events_v1.note_date   (VARCHAR)
--     - canonical_molecular_genetics_v2.resolved_test_date   (VARCHAR, MM/DD/YYYY)
--     - canonical_molecular_genetics_v2.test_date_native     (TIMESTAMP at midnight)
--     Disposition: append CF-117-DATE-RETYPE notes to each col registry row.
--     A future repair migration will batch-retype all CF-100/117 cols at once
--     (likely scope: 5+ canonical tables, per the existing CF-100 pattern).
--
-- Revised canonical audit allowlist (extends mig_109's allowlist by 2):
--   ADD: 'built_at', 'ingestion_date'
--   Full list: build_ts, built_at, extracted_at, llm_build_ts, llm_extracted_at,
--              verified_ts, signed_off_ts, registered_ts, updated_at,
--              created_at, promoted_at, completed_at, started_at, ended_at,
--              ingested_at_utc, ingestion_date, lab_datetime
--
-- Post-mig_117 expected state:
--   Gate 1 (verified_tables_total): 51 (unchanged)
--   Gate 2 (tables_missing_signoff): 0 (unchanged)
--   Gate 3 (tables_count_mismatch): 0 (unchanged)
--   Gate 4 (verified_cols_missing_metadata): 0 (unchanged)
--   Gate 5 (date_violations_on_verified — extended allowlist): 4
--           (the 4 known CF-117-DATE-RETYPE rows; reconciled, not blocking)
--
-- Carry-forwards opened:
--   CF-117-DATE-RETYPE (4 cols):
--     Rolls into the eventual single repair-migration with CF-100-DATE-RETYPE.
--     For each col: ADD COLUMN <col>_dt DATE; UPDATE … SET <col>_dt =
--     TRY_STRPTIME(<col>,'<fmt>')::DATE; DROP VARCHAR/TIMESTAMP variant; RENAME.
--   No retype performed in mig_117 — verify-with-note pattern (matches mig_100).
-- =============================================================================

-- 117a: append audit-allowlist note to 6 provenance cols
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_117: provenance timestamp; added to canonical audit '
            || 'allowlist (joins build_ts/extracted_at/ingested_at_utc/etc). '
            || 'Single-day distinct value confirms pipeline-time origin '
            || '(2026-04-21 build/ingestion). Not a clinical date.'
WHERE schema_name = 'main'
  AND (
       (table_name = 'canonical_molecular_genetics_v2' AND column_name = 'built_at')
    OR (table_name = 'canonical_labs_calcium_v1'        AND column_name = 'ingestion_date')
    OR (table_name = 'canonical_labs_pth_v1'            AND column_name = 'ingestion_date')
    OR (table_name = 'canonical_labs_thyroglobulin_v1'  AND column_name = 'ingestion_date')
    OR (table_name = 'canonical_labs_tsh_v1'            AND column_name = 'ingestion_date')
    OR (table_name = 'canonical_labs_vitamin_d_v1'      AND column_name = 'ingestion_date')
  );

-- 117b: append CF-117-DATE-RETYPE note to 4 clinical-date violations
-- pathology_clinical.note_date (VARCHAR)
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-117-DATE-RETYPE: stored as VARCHAR; clinical-event date '
            || 'requires DATE per Logan rule (feedback_clinical_dates_'
            || 'calendar_only.md, 2026-04-28). Future repair-mig will retype '
            || 'in batch with CF-100-DATE-RETYPE cols.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_pathology_clinical_events_v1'
  AND column_name = 'note_date';

-- cervical_ln_clinical.note_date (VARCHAR)
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-117-DATE-RETYPE: stored as VARCHAR; clinical-event date '
            || 'requires DATE per Logan rule (feedback_clinical_dates_'
            || 'calendar_only.md, 2026-04-28). Future repair-mig will retype '
            || 'in batch with CF-100-DATE-RETYPE cols.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_cervical_ln_clinical_events_v1'
  AND column_name = 'note_date';

-- molecular_v2.resolved_test_date (VARCHAR MM/DD/YYYY)
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-117-DATE-RETYPE: stored as VARCHAR ''MM/DD/YYYY''; '
            || 'clinical-resolved test date requires DATE per Logan rule '
            || '(feedback_clinical_dates_calendar_only.md, 2026-04-28). '
            || 'Future repair-mig: TRY_STRPTIME('
            || 'resolved_test_date,''%m/%d/%Y'')::DATE; joins CF-100 batch.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_molecular_genetics_v2'
  AND column_name = 'resolved_test_date';

-- molecular_v2.test_date_native (TIMESTAMP at midnight)
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-117-DATE-RETYPE: stored as TIMESTAMP (all values at '
            || '00:00:00); clinical native test date requires DATE per Logan '
            || 'rule (feedback_clinical_dates_calendar_only.md, 2026-04-28). '
            || 'Future repair-mig: CAST AS DATE; joins CF-100 batch.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_molecular_genetics_v2'
  AND column_name = 'test_date_native';

-- =============================================================================
-- end of migration 117
--
-- Re-run gate 5 with EXTENDED allowlist (verification probe):
-- WITH verified_tables AS (
--   SELECT table_name FROM main.canonical_table_signoff_registry_v1
--   WHERE table_status='verified' AND table_name LIKE 'canonical_%'
-- ),
-- audit_allowlist AS (
--   SELECT col_name FROM (VALUES
--     ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),
--     ('llm_extracted_at'),('verified_ts'),('signed_off_ts'),
--     ('registered_ts'),('updated_at'),('created_at'),('promoted_at'),
--     ('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
--     ('ingestion_date'),('lab_datetime')
--   ) v(col_name)
-- )
-- SELECT COUNT(*) AS gate5_extended -- expect 4 (the 4 CF-117 rows)
-- FROM information_schema.columns c
-- JOIN verified_tables v ON c.table_name = v.table_name
-- WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
--   AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
--   AND c.column_name NOT LIKE '%_status'
--   AND c.column_name NOT LIKE '%_source'
--   AND c.column_name NOT LIKE '%_keyword'
--   AND c.column_name NOT LIKE '%_raw'
--   AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
--        OR (c.data_type='VARCHAR' AND (c.column_name ILIKE '%date%' OR c.column_name ILIKE '%dt')));
--
-- This becomes the new canonical audit query template (replaces mig_109's).
-- =============================================================================
