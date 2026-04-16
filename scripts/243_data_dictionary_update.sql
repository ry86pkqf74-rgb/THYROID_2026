-- ============================================================================
-- Script 243 — Update data_dictionary_v240 (status + replacement_column_name)
-- Date:    2026-04-16
-- Author:  THYROID_2026 canonical-finalization run (v1_0 lock)
--
-- Purpose
-- -------
-- Extend the canonical data dictionary with a `status` domain column and a
-- `replacement_column_name` pointer, then fold in every change that landed
-- in Scripts 237-242:
--   - deprecated__tumor_size_cm        (deprecated, -> path_tumor_size_cm)
--   - deprecated__imaging_nodule_size_cm (deprecated, -> dominant_nodule_size_cm)
--   - fna_size_cm / size_score          (provisional, from Script 237)
--   - serial_imaging_us.*               (authoritative, from Script 238)
--   - path_size_adjudication_v241.*     (provisional, from Script 241)
--   - rai_benign_histology_recovery_v234.*  (remove entirely, per Script 239)
--
-- Tables READ
--   thyroid_canonical_publication_v1_0.main.data_dictionary_v240
--   thyroid_canonical_publication_v1_0.main.canonical_patient_master
--
-- Tables WRITTEN
--   ALTER TABLE data_dictionary_v240 ADD COLUMN status VARCHAR
--   ALTER TABLE data_dictionary_v240 ADD COLUMN replacement_column_name VARCHAR
--   UPDATE / DELETE / INSERT rows (idempotent: delete-then-insert by column_name)
--   Archive: "Thyroid 2026 UPdated".archive_pub_v1_0.data_dictionary_v240_pre243_backup_<run_date>
--
-- Rollback plan
--   CREATE OR REPLACE TABLE data_dictionary_v240 AS
--     SELECT * FROM "Thyroid 2026 UPdated".archive_pub_v1_0.data_dictionary_v240_pre243_backup_20260416;
--
-- Status domain
--   authoritative    — current canonical column
--   deprecated       — renamed to deprecated__<old>; pointer to replacement
--   provisional      — new/proposed column pending sign-off
--   recovery_pending — (reserved; no active rows in v1_0)
-- ============================================================================

-- LOG: PHASE 1 — archive pre-script dictionary
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.data_dictionary_v240_pre243_backup_20260416
  AS SELECT * FROM data_dictionary_v240;

COMMENT ON TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.data_dictionary_v240_pre243_backup_20260416 IS
  'Script 243 (2026-04-16): pre-update snapshot of main.data_dictionary_v240 (before status + replacement_column_name columns were added and before deprecated/provisional rows were updated).';

-- ASSERT: archive exists and matches source row count
SELECT
  (SELECT COUNT(*) FROM "Thyroid 2026 UPdated".archive_pub_v1_0.data_dictionary_v240_pre243_backup_20260416)
  =
  (SELECT COUNT(*) FROM data_dictionary_v240) AS ok;

-- LOG: PHASE 2 — extend schema (idempotent ADD COLUMN)
ALTER TABLE data_dictionary_v240 ADD COLUMN IF NOT EXISTS status VARCHAR;
ALTER TABLE data_dictionary_v240 ADD COLUMN IF NOT EXISTS replacement_column_name VARCHAR;

-- Backfill status for pre-existing rows that don't already have a value
UPDATE data_dictionary_v240
SET status = 'authoritative'
WHERE status IS NULL
  AND (description IS NULL OR description NOT LIKE 'PROVISIONAL%');

-- Script 237 provisional rows pre-existed — mark them explicitly
UPDATE data_dictionary_v240
SET status = 'provisional'
WHERE column_name IN ('fna_size_cm', 'size_score')
  AND description LIKE 'PROVISIONAL (Script 237%';

-- LOG: PHASE 3 — remove rows whose CPM column was renamed in Script 240
-- (tumor_size_cm and imaging_nodule_size_cm no longer exist; they became deprecated__*)
DELETE FROM data_dictionary_v240
WHERE column_name IN ('tumor_size_cm', 'imaging_nodule_size_cm')
  AND (description IS NULL OR description NOT LIKE '%imaging_fna_linkage_v3%');

-- LOG: PHASE 4 — remove any rows for rai_benign_histology_recovery_v234 (table dropped in Script 239)
DELETE FROM data_dictionary_v240
WHERE description LIKE '%rai_benign_histology_recovery_v234%'
   OR column_name IN ('current_is_malignant','current_histology','synoptic_histology','synoptic_variant','provenance');

-- LOG: PHASE 5 — insert deprecated rows for the two renamed CPM columns
-- (idempotent: delete first, then insert)
DELETE FROM data_dictionary_v240
WHERE column_name IN ('deprecated__tumor_size_cm', 'deprecated__imaging_nodule_size_cm');

INSERT INTO data_dictionary_v240
  (column_name, data_type, ordinal_position, n_non_null, pct_non_null, n_distinct, description, status, replacement_column_name)
SELECT
  'deprecated__tumor_size_cm' AS column_name,
  'DOUBLE' AS data_type,
  (SELECT ordinal_position FROM information_schema.columns
   WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
     AND table_name='canonical_patient_master' AND column_name='deprecated__tumor_size_cm') AS ordinal_position,
  COUNT(deprecated__tumor_size_cm)::BIGINT AS n_non_null,
  100.0 * COUNT(deprecated__tumor_size_cm) / NULLIF(COUNT(*), 0) AS pct_non_null,
  COUNT(DISTINCT deprecated__tumor_size_cm)::BIGINT AS n_distinct,
  'DEPRECATED 2026-04-16 (Script 240): byte-identical duplicate of path_tumor_size_cm across 4130/4130 populated rows. Will be removed in v1_1.' AS description,
  'deprecated' AS status,
  'path_tumor_size_cm' AS replacement_column_name
FROM canonical_patient_master;

INSERT INTO data_dictionary_v240
  (column_name, data_type, ordinal_position, n_non_null, pct_non_null, n_distinct, description, status, replacement_column_name)
SELECT
  'deprecated__imaging_nodule_size_cm' AS column_name,
  'DOUBLE' AS data_type,
  (SELECT ordinal_position FROM information_schema.columns
   WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
     AND table_name='canonical_patient_master' AND column_name='deprecated__imaging_nodule_size_cm') AS ordinal_position,
  COUNT(deprecated__imaging_nodule_size_cm)::BIGINT AS n_non_null,
  100.0 * COUNT(deprecated__imaging_nodule_size_cm) / NULLIF(COUNT(*), 0) AS pct_non_null,
  COUNT(DISTINCT deprecated__imaging_nodule_size_cm)::BIGINT AS n_distinct,
  'DEPRECATED 2026-04-16 (Script 240): inconsistent per-patient aggregation (44.8%% MAX / 31.5%% MIN / 15.1%% MEAN across 3,439 patients). Will be removed in v1_1.' AS description,
  'deprecated' AS status,
  'dominant_nodule_size_cm' AS replacement_column_name
FROM canonical_patient_master;

-- LOG: PHASE 6 — insert serial_imaging_us.* rows (authoritative; Script 238 provenance)
-- We write one row per non-identity column. research_id is an identity column
-- and already has a CPM-scoped authoritative row in the dict.
DELETE FROM data_dictionary_v240
WHERE description LIKE '%table=serial_imaging_us%';

INSERT INTO data_dictionary_v240
  (column_name, data_type, ordinal_position, n_non_null, pct_non_null, n_distinct, description, status, replacement_column_name)
VALUES
  ('us_date',                       'VARCHAR', NULL, NULL, NULL, NULL, 'Script 238 (2026-04-16): table=serial_imaging_us. ISO date string (preserved as VARCHAR to match ultrasound_reports.ultrasound_date source column).', 'authoritative', NULL),
  ('dominant_nodule_size_on_us',    'DOUBLE',  NULL, NULL, NULL, NULL, 'Script 238 (2026-04-16): table=serial_imaging_us. max_dimension_cm of the largest nodule that exam from imaging_nodule_master_v1 (no dominant_nodule_flag exists; largest is the operational proxy). ~96.9%% populated.', 'authoritative', NULL),
  ('us_findings_impression',        'VARCHAR', NULL, NULL, NULL, NULL, 'Script 238 (2026-04-16): table=serial_imaging_us. ultrasound_reports.source_us_impression.', 'authoritative', NULL),
  ('us_impression',                 'VARCHAR', NULL, NULL, NULL, NULL, 'Script 238 (2026-04-16): table=serial_imaging_us. ultrasound_reports.clinical_impression.', 'authoritative', NULL),
  ('dominant_nodule_location',      'VARCHAR', NULL, NULL, NULL, NULL, 'Script 238 (2026-04-16): table=serial_imaging_us. location_raw of the largest-nodule row on the same exam. ~96.9%% populated.', 'authoritative', NULL);

-- LOG: PHASE 7 — insert path_size_adjudication_v241.* rows (provisional; Script 241 provenance)
DELETE FROM data_dictionary_v240
WHERE description LIKE '%table=path_size_adjudication_v241%';

INSERT INTO data_dictionary_v240
  (column_name, data_type, ordinal_position, n_non_null, pct_non_null, n_distinct, description, status, replacement_column_name)
VALUES
  ('path_tumor_size_cm',                       'DOUBLE',  NULL, NULL, NULL, NULL, 'Script 241 (2026-04-16): table=path_size_adjudication_v241. Copy of canonical_patient_master.path_tumor_size_cm for outliers.',                                                              'provisional', NULL),
  ('tumor_size_cm_max',                        'DOUBLE',  NULL, NULL, NULL, NULL, 'Script 241 (2026-04-16): table=path_size_adjudication_v241. Copy of canonical_patient_master.tumor_size_cm_max for outliers.',                                                               'provisional', NULL),
  ('n_foci_path',                              'BIGINT',  NULL, NULL, NULL, NULL, 'Script 241 (2026-04-16): table=path_size_adjudication_v241. Distinct tumor_index count from specimen_tumor_focus_v1 per patient.',                                                           'provisional', NULL),
  ('n_tumors_path',                            'BIGINT',  NULL, NULL, NULL, NULL, 'Script 241 (2026-04-16): table=path_size_adjudication_v241. From patient_tumor_rollup_v1.',                                                                                                 'provisional', NULL),
  ('proposed_path_tumor_size_cm_adjudicated',  'DOUBLE',  NULL, NULL, NULL, NULL, 'Script 241 (2026-04-16): table=path_size_adjudication_v241. Proposed adjudicated value (NULL when path >10cm; tumor_size_cm_max when n_foci_path>1; path_tumor_size_cm otherwise). Clinician sign-off required before applying to CPM.', 'provisional', NULL),
  ('adjudication_rule',                        'VARCHAR', NULL, NULL, NULL, NULL, 'Script 241 (2026-04-16): table=path_size_adjudication_v241. One of: outlier_manual_review_required | multifocal_use_rollup_max | unifocal_retain_path_size.',                                'provisional', NULL),
  ('review_priority',                          'VARCHAR', NULL, NULL, NULL, NULL, 'Script 241 (2026-04-16): table=path_size_adjudication_v241. HIGH (path>10cm) | MEDIUM (multifocal or unifocal discrepancy).',                                                              'provisional', NULL);

-- LOG: PHASE 8 — sanity update: make sure every deprecated__ column in CPM has a dict row
-- (defensive — if a future run adds a third deprecated__ column, this keeps it consistent)

-- LOG: PHASE 9 — assertions
-- ASSERT: both new columns exist on data_dictionary_v240
SELECT
  (SELECT COUNT(*) FROM information_schema.columns
   WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
     AND table_name='data_dictionary_v240'
     AND column_name IN ('status','replacement_column_name')) = 2 AS ok;

-- ASSERT: every deprecated__ column in CPM has a dict row with status='deprecated' AND non-null replacement
SELECT
  (SELECT COUNT(*) FROM information_schema.columns c
   WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
     AND c.table_name='canonical_patient_master' AND c.column_name LIKE 'deprecated__%')
  =
  (SELECT COUNT(*) FROM data_dictionary_v240
   WHERE column_name LIKE 'deprecated__%'
     AND status = 'deprecated' AND replacement_column_name IS NOT NULL)
  AS ok;

-- ASSERT: no rows with status IS NULL
SELECT COUNT(*) = 0 AS ok FROM data_dictionary_v240 WHERE status IS NULL;

-- ASSERT: status domain is the allowed set
SELECT
  COUNT(*) = 0 AS ok FROM data_dictionary_v240
  WHERE status NOT IN ('authoritative','deprecated','provisional','recovery_pending');

-- ASSERT: no stale rows for tumor_size_cm / imaging_nodule_size_cm (renamed, must not appear under the old name)
SELECT COUNT(*) = 0 AS ok
FROM data_dictionary_v240
WHERE column_name IN ('tumor_size_cm','imaging_nodule_size_cm')
  AND (description IS NULL OR description NOT LIKE '%imaging_fna_linkage_v3%');

-- ASSERT: no rai_benign_histology_recovery_v234 rows (table dropped in Script 239)
SELECT COUNT(*) = 0 AS ok FROM data_dictionary_v240
WHERE description LIKE '%rai_benign_histology_recovery_v234%';

-- ASSERT: path_size_adjudication_v241.* rows all have status='provisional' (exactly 7 columns)
SELECT COUNT(*) = 7 AS ok
FROM data_dictionary_v240
WHERE description LIKE '%table=path_size_adjudication_v241%'
  AND status = 'provisional';

-- ASSERT: serial_imaging_us.* rows all have status='authoritative' (5 non-identity columns)
SELECT COUNT(*) = 5 AS ok
FROM data_dictionary_v240
WHERE description LIKE '%table=serial_imaging_us%'
  AND status = 'authoritative';

-- ASSERT: archive backup present
SELECT COUNT(*) = 1 AS ok
FROM information_schema.tables
WHERE table_catalog='Thyroid 2026 UPdated' AND table_schema='archive_pub_v1_0'
  AND table_name='data_dictionary_v240_pre243_backup_20260416';

-- ASSERT: canonical_patient_master untouched
SELECT COUNT(*) = 10871 AS ok FROM canonical_patient_master;

-- LOG: PHASE 10 — status breakdown (diagnostic)
SELECT status, COUNT(*) AS n FROM data_dictionary_v240 GROUP BY status ORDER BY n DESC;

-- LOG: Script 243 complete. Dictionary extended; all status invariants hold.
