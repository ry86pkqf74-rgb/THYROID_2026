-- ============================================================================
-- TRACKED OPERATOR SQL — EMR cohort demo fields apply (CPM Phase 1 audit)
-- ============================================================================
-- This file is a tracked audit twin of `sql/mig_079_emr_demographics_import.sql`.
-- `.gitignore` patterns `*_demographics_*` and `*demographics_import*` would hide
-- common operator filenames from `git status`; this path avoids those substrings.
-- Primary reference / migration_id: mig_079_emr_demographics_import (THY-1).
-- ============================================================================
-- mig_079_emr_demographics_import
-- ============================================================================
-- Date:    2026-05-06
-- Author:  Cursor agent at Logan's request
-- DFL:     DFL-20260506-TF1 (Data Feedback Log, Airtable base appJYOnUb7KrHKwpV)
-- Linear:  THY-1
-- Status:  TEMPLATE ONLY - DO NOT RUN until Logan provides the EMR export.
-- ============================================================================
--
-- PURPOSE
--   Import an EMR demographics export keyed by research_id to close the residual
--   TGDC race-coverage gap. This template is intentionally conservative:
--     * It does NOT overwrite non-null canonical values by default.
--     * It creates aggregate audit counts only.
--     * It does NOT persist DOB in the canonical table.
--     * It raises errors if source/target schemas do not match expectations.
--
-- LIVE SCHEMA CONFIRMATION (2026-05-06, INFORMATION_SCHEMA)
--   No standalone `pub_canonical.canonical_demographics_v1` table exists.
--   Current canonical demographics fields live in:
--     `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
--   Relevant columns present:
--     research_id STRING, race STRING, sex STRING, demo_source STRING,
--     demo_confidence INT64, age_at_surgery INT64, cpm_built_at TIMESTAMP.
--   No canonical DOB or ethnicity column is present in this target table.
--
-- SOURCE REQUIREMENT
--   Logan must provide a restricted GCS object, preferably CSV:
--     gs://REPLACE_WITH_RESTRICTED_BUCKET/emr_demographics_YYYYMMDD.csv
--   Required header columns:
--     research_id,race,ethnicity,sex,dob
--
-- PHI NOTE
--   DOB is accepted only for linkage/age validation during this import. It is
--   not written to canonical_patient_master and this script logs only counts.
--
-- ROLLBACK
--   A full pre-update snapshot is created at:
--     `thyroid-canonical-pub-2026.pub_signoff.canonical_patient_master_pre_mig079`
--   To rollback:
--     CREATE OR REPLACE TABLE
--       `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
--     AS SELECT * FROM
--       `thyroid-canonical-pub-2026.pub_signoff.canonical_patient_master_pre_mig079`;
-- ============================================================================

-- ============================================================================
-- OPERATOR CONFIGURATION - EDIT THESE BEFORE RUNNING
-- ============================================================================
DECLARE source_uri STRING DEFAULT 'gs://REPLACE_WITH_RESTRICTED_BUCKET/emr_demographics_YYYYMMDD.csv';
DECLARE source_format STRING DEFAULT 'CSV';  -- Supported values in this template: CSV or PARQUET.
DECLARE allow_overwrite_existing BOOL DEFAULT FALSE;

DECLARE migration_id STRING DEFAULT 'mig_079_emr_demographics_import';
DECLARE applied_by STRING DEFAULT 'manual_runner_after_logan_emr_export';
DECLARE rows_before INT64 DEFAULT NULL;
DECLARE rows_after INT64 DEFAULT NULL;
DECLARE race_non_null_before INT64 DEFAULT NULL;
DECLARE race_non_null_after INT64 DEFAULT NULL;
DECLARE sex_non_null_before INT64 DEFAULT NULL;
DECLARE sex_non_null_after INT64 DEFAULT NULL;

-- ============================================================================
-- PART 0 - Confirm target shape and create rollback snapshot
-- ============================================================================
ASSERT (
  SELECT COUNT(*) = 0
  FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = 'canonical_demographics_v1'
) AS 'Unexpected standalone pub_canonical.canonical_demographics_v1 exists. Stop and adapt this migration to the new canonical demographics table.';

ASSERT (
  SELECT COUNT(*) = 6
  FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'canonical_patient_master'
    AND (
      (column_name = 'research_id' AND data_type = 'STRING') OR
      (column_name = 'race' AND data_type = 'STRING') OR
      (column_name = 'sex' AND data_type = 'STRING') OR
      (column_name = 'demo_source' AND data_type = 'STRING') OR
      (column_name = 'demo_confidence' AND data_type = 'INT64') OR
      (column_name = 'cpm_built_at' AND data_type = 'TIMESTAMP')
    )
) AS 'canonical_patient_master demographics schema does not match mig_079 expectations.';

CREATE OR REPLACE TABLE
  `thyroid-canonical-pub-2026.pub_signoff.canonical_patient_master_pre_mig079`
AS
SELECT *
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`;

-- ============================================================================
-- PART 1 - Load EMR export into a transient raw staging table
-- ============================================================================
CREATE OR REPLACE TABLE
  `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_raw` (
    research_id STRING,
    race STRING,
    ethnicity STRING,
    sex STRING,
    dob STRING
  );

-- Recommended CSV path. Keep source_uri in a restricted bucket; do not store
-- the file in git. If using Parquet, switch source_format to PARQUET and use
-- the PARQUET LOAD DATA block below.
IF source_format = 'CSV' THEN
  EXECUTE IMMEDIATE FORMAT("""
    LOAD DATA OVERWRITE
      `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_raw`
    FROM FILES (
      format = 'CSV',
      uris = ['%s'],
      skip_leading_rows = 1,
      field_delimiter = ',',
      quote = '"',
      allow_quoted_newlines = TRUE
    )
  """, source_uri);
ELSEIF source_format = 'PARQUET' THEN
  EXECUTE IMMEDIATE FORMAT("""
    LOAD DATA OVERWRITE
      `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_raw`
    FROM FILES (
      format = 'PARQUET',
      uris = ['%s']
    )
  """, source_uri);
ELSE
  RAISE USING MESSAGE = 'source_format must be CSV or PARQUET';
END IF;

-- ============================================================================
-- PART 2 - Schema and source-quality audit
-- ============================================================================
CREATE OR REPLACE TABLE
  `thyroid-canonical-pub-2026.pub_signoff.mig079_emr_demographics_schema_audit_v1`
AS
WITH required_import_cols AS (
  SELECT 'research_id' AS column_name, 'STRING' AS expected_type UNION ALL
  SELECT 'race', 'STRING' UNION ALL
  SELECT 'ethnicity', 'STRING' UNION ALL
  SELECT 'sex', 'STRING' UNION ALL
  SELECT 'dob', 'STRING'
),
import_cols AS (
  SELECT column_name, data_type
  FROM `thyroid-canonical-pub-2026.pub_workspace.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'stg_emr_demographics_import_mig079_raw'
),
target_cols AS (
  SELECT column_name, data_type
  FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'canonical_patient_master'
)
SELECT
  'required_import_column' AS audit_scope,
  r.column_name,
  r.expected_type,
  i.data_type AS observed_type,
  i.column_name IS NOT NULL AS present
FROM required_import_cols AS r
LEFT JOIN import_cols AS i USING (column_name)
UNION ALL
SELECT
  'canonical_target_column' AS audit_scope,
  t.column_name,
  t.data_type AS expected_type,
  t.data_type AS observed_type,
  TRUE AS present
FROM target_cols AS t
WHERE t.column_name IN ('research_id', 'race', 'sex', 'demo_source', 'demo_confidence', 'cpm_built_at');

ASSERT (
  SELECT COUNT(*) = 0
  FROM `thyroid-canonical-pub-2026.pub_signoff.mig079_emr_demographics_schema_audit_v1`
  WHERE audit_scope = 'required_import_column'
    AND present = FALSE
) AS 'EMR import is missing one or more required columns: research_id, race, ethnicity, sex, dob.';

CREATE OR REPLACE TABLE
  `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_norm`
AS
WITH normalized AS (
  SELECT
    TRIM(CAST(research_id AS STRING)) AS research_id,
    NULLIF(TRIM(CAST(race AS STRING)), '') AS race_import,
    NULLIF(TRIM(CAST(ethnicity AS STRING)), '') AS ethnicity_import,
    NULLIF(TRIM(CAST(sex AS STRING)), '') AS sex_import,
    SAFE_CAST(NULLIF(TRIM(CAST(dob AS STRING)), '') AS DATE) AS dob_import,
    CAST(NULL AS STRING) AS emr_source,
    CAST(NULL AS STRING) AS exported_at
  FROM `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_raw`
)
SELECT *
FROM normalized
WHERE research_id IS NOT NULL
  AND research_id <> '';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT research_id
    FROM `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_norm`
    GROUP BY research_id
    HAVING COUNT(*) > 1
  )
) AS 'EMR import has duplicate research_id rows. Resolve duplicates before applying.';

CREATE OR REPLACE TABLE
  `thyroid-canonical-pub-2026.pub_signoff.mig079_emr_demographics_apply_audit_v1`
AS
WITH joined AS (
  SELECT
    c.research_id,
    c.race AS race_before,
    s.race_import,
    c.sex AS sex_before,
    s.sex_import,
    s.ethnicity_import,
    s.dob_import,
    c.age_at_surgery,
    c.demo_source AS demo_source_before,
    c.demo_confidence AS demo_confidence_before
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` AS c
  LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_norm` AS s
    ON c.research_id = s.research_id
)
SELECT
  COUNT(*) AS canonical_rows,
  COUNTIF(race_before IS NOT NULL AND TRIM(race_before) <> '') AS race_non_null_before,
  COUNTIF(sex_before IS NOT NULL AND TRIM(sex_before) <> '') AS sex_non_null_before,
  COUNTIF(race_import IS NOT NULL) AS imported_race_rows_matched_to_canonical,
  COUNTIF(sex_import IS NOT NULL) AS imported_sex_rows_matched_to_canonical,
  COUNTIF((race_before IS NULL OR TRIM(race_before) = '') AND race_import IS NOT NULL) AS race_fill_candidates,
  COUNTIF((sex_before IS NULL OR TRIM(sex_before) = '') AND sex_import IS NOT NULL) AS sex_fill_candidates,
  COUNTIF(race_before IS NOT NULL AND TRIM(race_before) <> '' AND race_import IS NOT NULL AND race_before != race_import) AS race_conflict_count,
  COUNTIF(sex_before IS NOT NULL AND TRIM(sex_before) <> '' AND sex_import IS NOT NULL AND sex_before != sex_import) AS sex_conflict_count,
  COUNTIF(ethnicity_import IS NOT NULL) AS ethnicity_present_in_import_not_loaded_to_canonical,
  COUNTIF(dob_import IS NOT NULL) AS dob_present_in_import_not_loaded_to_canonical
FROM joined;

ASSERT (
  allow_overwrite_existing
  OR (
    SELECT race_conflict_count = 0 AND sex_conflict_count = 0
    FROM `thyroid-canonical-pub-2026.pub_signoff.mig079_emr_demographics_apply_audit_v1`
  )
) AS 'Conflicting non-null canonical race/sex values detected. Review counts and set allow_overwrite_existing only after Logan approval.';

-- ============================================================================
-- PART 3 - Apply safe fills to canonical_patient_master
-- ============================================================================
SET rows_before = (
  SELECT COUNT(*)
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
);

SET race_non_null_before = (
  SELECT COUNTIF(race IS NOT NULL AND TRIM(race) <> '')
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
);

SET sex_non_null_before = (
  SELECT COUNTIF(sex IS NOT NULL AND TRIM(sex) <> '')
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
);

UPDATE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` AS c
SET
  race = CASE
    WHEN allow_overwrite_existing AND s.race_import IS NOT NULL THEN s.race_import
    WHEN (c.race IS NULL OR TRIM(c.race) = '') AND s.race_import IS NOT NULL THEN s.race_import
    ELSE c.race
  END,
  sex = CASE
    WHEN allow_overwrite_existing AND s.sex_import IS NOT NULL THEN s.sex_import
    WHEN (c.sex IS NULL OR TRIM(c.sex) = '') AND s.sex_import IS NOT NULL THEN s.sex_import
    ELSE c.sex
  END,
  demo_source = CASE
    WHEN ((c.race IS NULL OR TRIM(c.race) = '') AND s.race_import IS NOT NULL)
      OR ((c.sex IS NULL OR TRIM(c.sex) = '') AND s.sex_import IS NOT NULL)
      OR allow_overwrite_existing
    THEN 'emr_demographics_import_mig079'
    ELSE c.demo_source
  END,
  demo_confidence = CASE
    WHEN ((c.race IS NULL OR TRIM(c.race) = '') AND s.race_import IS NOT NULL)
      OR ((c.sex IS NULL OR TRIM(c.sex) = '') AND s.sex_import IS NOT NULL)
      OR allow_overwrite_existing
    THEN 100
    ELSE c.demo_confidence
  END,
  cpm_built_at = CASE
    WHEN ((c.race IS NULL OR TRIM(c.race) = '') AND s.race_import IS NOT NULL)
      OR ((c.sex IS NULL OR TRIM(c.sex) = '') AND s.sex_import IS NOT NULL)
      OR allow_overwrite_existing
    THEN CURRENT_TIMESTAMP()
    ELSE c.cpm_built_at
  END
FROM `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_norm` AS s
WHERE c.research_id = s.research_id
  AND (
    allow_overwrite_existing
    OR (c.race IS NULL OR TRIM(c.race) = '' OR c.sex IS NULL OR TRIM(c.sex) = '')
  );

SET rows_after = (
  SELECT COUNT(*)
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
);

SET race_non_null_after = (
  SELECT COUNTIF(race IS NOT NULL AND TRIM(race) <> '')
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
);

SET sex_non_null_after = (
  SELECT COUNTIF(sex IS NOT NULL AND TRIM(sex) <> '')
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
);

ASSERT rows_before = rows_after AS 'Row count changed in canonical_patient_master; rollback and investigate.';

CREATE OR REPLACE TABLE
  `thyroid-canonical-pub-2026.pub_signoff.mig079_emr_demographics_result_counts_v1`
AS
SELECT
  rows_before,
  rows_after,
  race_non_null_before,
  race_non_null_after,
  race_non_null_after - race_non_null_before AS race_non_null_delta,
  sex_non_null_before,
  sex_non_null_after,
  sex_non_null_after - sex_non_null_before AS sex_non_null_delta,
  CURRENT_TIMESTAMP() AS counted_at;

-- ============================================================================
-- PART 4 - Governance log
-- ============================================================================
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
  (migration_id, applied_at, applied_by, description, affected_dataset,
   affected_table, pre_snapshot_table, rows_before, rows_after, rollback_sql,
   notes)
VALUES (
  migration_id,
  CURRENT_TIMESTAMP(),
  applied_by,
  'EMR demographics import for THY-1 TGDC residual race-coverage gap; safe-fill race/sex only, no DOB persisted.',
  'pub_canonical',
  'canonical_patient_master',
  'thyroid-canonical-pub-2026.pub_signoff.canonical_patient_master_pre_mig079',
  rows_before,
  rows_after,
  'CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` AS SELECT * FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_patient_master_pre_mig079`;',
  FORMAT(
    'DFL=DFL-20260506-TF1; THY-1; source_uri=%s; race_non_null_before=%d; race_non_null_after=%d; sex_non_null_before=%d; sex_non_null_after=%d; no row-level PHI logged.',
    source_uri,
    race_non_null_before,
    race_non_null_after,
    sex_non_null_before,
    sex_non_null_after
  )
);

-- Optional PHI-minimization cleanup after successful verification. Leave the
-- normalized staging table only if an approved reviewer needs aggregate reruns.
-- DROP TABLE IF EXISTS `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_raw`;
-- DROP TABLE IF EXISTS `thyroid-canonical-pub-2026.pub_workspace.stg_emr_demographics_import_mig079_norm`;

-- ============================================================================
-- POST-RUN VERIFICATION (counts only; no row contents)
-- ============================================================================
-- SELECT * FROM `thyroid-canonical-pub-2026.pub_signoff.mig079_emr_demographics_apply_audit_v1`;
-- SELECT * FROM `thyroid-canonical-pub-2026.pub_signoff.mig079_emr_demographics_result_counts_v1`;
-- SELECT migration_id, applied_at, rows_before, rows_after, notes
-- FROM `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
-- WHERE migration_id = 'mig_079_emr_demographics_import'
-- ORDER BY applied_at DESC
-- LIMIT 1;
-- ============================================================================
