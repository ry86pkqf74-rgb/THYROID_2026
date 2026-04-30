-- mig_208 — note_date VARCHAR → DATE retype on 3 canonicals (closes CF-117-DATE-RETYPE / CF-100-DATE-RETYPE)
-- Batch_id: mig_208_note_date_retype_3_canonicals_20260430
-- Predecessor: mig_160b PM date cols retype (commit 83fa6f1) established the TRY_CAST(NULLIF()) +
--   per-col format probe pattern. mig_127 marked these 3 note_date cols as na in col_registry but
--   the underlying VARCHAR remained → §14 verification suite caught them.
-- Per `feedback_clinical_dates_calendar_only.md` (Logan-ratified 2026-04-28): clinical event date cols MUST be DATE.
--
-- Probes (verified pre-apply via Cowork verification suite session 2026-04-30):
--   canonical_cervical_ln_clinical_events_v1.note_date  → 4,493/4,493 = '' (100% empty placeholder)
--   canonical_pathology_clinical_events_v1.note_date    → 13,358/13,358 = '' (100% empty placeholder)
--   canonical_molecular_genetics_from_notes_v2.note_date → 1,738 total (659 NULL + 1,079 MM/DD/YYYY); 100% TRY_STRPTIME parseable
--
-- 0 view-body dependencies confirmed (duckdb_views() probe).
-- Database: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT APPLY (Logan-authorized 2026-04-30 'address issues now'); pre-snapshots for safety.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A Pre-snapshot the 3 canonicals + col registry baseline
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_cervical_ln_clinical_events_v1_pre_mig208_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig208_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_cervical_ln_clinical_events_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_pathology_clinical_events_v1_pre_mig208_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig208_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_pathology_clinical_events_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_molecular_genetics_from_notes_v2_pre_mig208_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig208_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_molecular_genetics_from_notes_v2;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig208_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig208_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1;

-- =============================================================================
-- §B ALTER COLUMN — VARCHAR → DATE
-- =============================================================================

-- Empty-placeholder col 1: 100% '' → all NULL DATE (TRY_CAST(NULLIF()) per mig_160b §B fix)
ALTER TABLE main.canonical_cervical_ln_clinical_events_v1
  ALTER COLUMN note_date TYPE DATE
  USING TRY_CAST(NULLIF(note_date,'') AS DATE);

-- Empty-placeholder col 2: 100% '' → all NULL DATE
ALTER TABLE main.canonical_pathology_clinical_events_v1
  ALTER COLUMN note_date TYPE DATE
  USING TRY_CAST(NULLIF(note_date,'') AS DATE);

-- Real-data col: 659 NULL + 1,079 MM/DD/YYYY → DATE via TRY_STRPTIME (verified 100% parse rate)
ALTER TABLE main.canonical_molecular_genetics_from_notes_v2
  ALTER COLUMN note_date TYPE DATE
  USING CAST(TRY_STRPTIME(NULLIF(note_date,''), '%m/%d/%Y') AS DATE);

-- =============================================================================
-- §C UPDATE col registry — flip the 3 'na' rows to 'verified' with retype trace
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET data_type = 'DATE',
    verification_status = 'verified',
    verified_by = 'mig_208',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'varchar_to_date_retype_with_format_note',
    batch_id = 'mig_208_note_date_retype_3_canonicals_20260430',
    notes = 'mig_208 retype VARCHAR→DATE: source was 100% empty-string placeholder (4,493 rows); TRY_CAST(NULLIF()) → all NULL DATE. Closes CF-117-DATE-RETYPE per feedback_clinical_dates_calendar_only.md. Pre-snapshot at archive_pub_v1_0.canonical_cervical_ln_clinical_events_v1_pre_mig208_20260430.'
WHERE schema_name='main'
  AND table_name='canonical_cervical_ln_clinical_events_v1'
  AND column_name='note_date';

UPDATE main.canonical_column_verification_registry_v1
SET data_type = 'DATE',
    verification_status = 'verified',
    verified_by = 'mig_208',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'varchar_to_date_retype_with_format_note',
    batch_id = 'mig_208_note_date_retype_3_canonicals_20260430',
    notes = 'mig_208 retype VARCHAR→DATE: source was 100% empty-string placeholder (13,358 rows); TRY_CAST(NULLIF()) → all NULL DATE. Closes CF-117-DATE-RETYPE per feedback_clinical_dates_calendar_only.md. Pre-snapshot at archive_pub_v1_0.canonical_pathology_clinical_events_v1_pre_mig208_20260430.'
WHERE schema_name='main'
  AND table_name='canonical_pathology_clinical_events_v1'
  AND column_name='note_date';

UPDATE main.canonical_column_verification_registry_v1
SET data_type = 'DATE',
    verification_status = 'verified',
    verified_by = 'mig_208',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'varchar_to_date_retype_with_format_note',
    batch_id = 'mig_208_note_date_retype_3_canonicals_20260430',
    notes = 'mig_208 retype VARCHAR→DATE: source had 1,079 MM/DD/YYYY strings + 659 NULLs; TRY_STRPTIME(%m/%d/%Y)::DATE → 1,079 valid DATEs + 659 NULL (100% parse rate verified pre-apply). Closes mig_127 deferred CF. Pre-snapshot at archive_pub_v1_0.canonical_molecular_genetics_from_notes_v2_pre_mig208_20260430.'
WHERE schema_name='main'
  AND table_name='canonical_molecular_genetics_from_notes_v2'
  AND column_name='note_date';

-- =============================================================================
-- §D Update signoff_registry n_verified/n_na counts on the 3 affected tables
--     (3 cols flipped from na → verified means n_verified +=1 and n_na -=1 each)
-- =============================================================================

UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = n_verified + 1,
    n_na = n_na - 1
WHERE table_name IN (
  'canonical_cervical_ln_clinical_events_v1',
  'canonical_pathology_clinical_events_v1',
  'canonical_molecular_genetics_from_notes_v2'
);

-- =============================================================================
-- §E Provenance row insert
-- =============================================================================

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_208_note_date_retype_3_canonicals_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'alter_column_varchar_to_date_3_canonicals_plus_registry_flip_na_to_verified',
   'CF-117-DATE-RETYPE+CF-100-DATE-RETYPE+mig127-DEFERRED-DATE-RETYPE',
   'cowork_verify_suite_s14_post_audit_allowlist_extension',
   '3_alter_column_3_registry_updates_3_signoff_count_updates',
   'none');

-- =============================================================================
-- §F Verification (post-apply state)
-- =============================================================================

-- §F1 The 3 cols are now DATE
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND column_name='note_date'
  AND table_name IN ('canonical_cervical_ln_clinical_events_v1','canonical_pathology_clinical_events_v1','canonical_molecular_genetics_from_notes_v2');

-- §F2 Molecular: parse fidelity preserved (1,079 valid + 659 NULL)
SELECT
  COUNT(*) AS n_total,
  COUNT(note_date) AS n_non_null,
  COUNT(*) FILTER (WHERE note_date IS NULL) AS n_null
FROM main.canonical_molecular_genetics_from_notes_v2;

-- §F3 Re-run §14 (canonical_*-scoped + extended allowlist; should return 0 rows)
SELECT c.table_name, c.column_name, c.data_type
FROM information_schema.columns c
JOIN main.canonical_table_signoff_registry_v1 t USING (table_name)
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND t.table_status='verified'
  AND c.table_name LIKE 'canonical_%'
  AND c.column_name SIMILAR TO '(.+_date|date_.+|surgery_date|fna_date|path_date|exam_date|finding_date|first_.+_date|last_.+_date|recurrence_date|first_followup_date|last_followup_date)'
  AND c.data_type NOT IN ('DATE')
  AND c.column_name NOT IN ('build_ts','build_migration','extracted_at','llm_extracted_at','registered_ts','signed_off_ts','verified_ts','ingestion_date','validated_at',
                            'date_confidence','date_source_keyword','date_status','date_traceability_status','sm_n_specimens_for_date')
  AND NOT regexp_matches(c.column_name, '_at$')
  AND NOT regexp_matches(c.column_name, '_ts$')
  AND NOT regexp_matches(c.column_name, '_confidence$')
  AND NOT regexp_matches(c.column_name, '_source_keyword$')
  AND NOT regexp_matches(c.column_name, '_status$')
ORDER BY c.table_name, c.column_name;

-- §F4 Gate3 still clean (n_verified + n_na = n_columns_total on all 3 affected tables)
SELECT table_name, n_verified, n_na, n_columns_total, (n_verified + n_na = n_columns_total) AS gate3_ok
FROM main.canonical_table_signoff_registry_v1
WHERE table_name IN ('canonical_cervical_ln_clinical_events_v1','canonical_pathology_clinical_events_v1','canonical_molecular_genetics_from_notes_v2');
