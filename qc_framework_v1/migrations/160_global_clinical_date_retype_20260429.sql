-- =============================================================================
-- Migration 160 — GLOBAL CALENDAR-DATE RETYPE (gate-5 closure, mig_160)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Lane:   48 — batch_id mig_160_global_clinical_date_retype_20260429
--
-- Spec:   cursor_prompts/CURSOR_PROMPT_global_clinical_date_retype_20260429.md
--
-- Goal:   Close gate 5 (clinical DATE stored as TIMESTAMP/VARCHAR on verified
--          canonicals) from **21 columns → 0** using the refined audit query
--          (mig_127: exclude verification_status='na', regex word-boundary).
--
-- Scope (5 physical tables, 21 cols):
--   * canonical_ete_event_resolved_v1.last_known_alive_date — TIMESTAMP→DATE
--   * canonical_frozen_section_patient_rollup_v1 — frozen_1_date..frozen_12_date,
--       frozen_section_first_date, frozen_section_last_date — VARCHAR→DATE (MM/DD/YYYY
--       lineage per mig_119 rebuild STRFTIME projection)
--   * canonical_molecular_genetics_v2.resolved_test_date — VARCHAR→DATE;
--       test_date_native — TIMESTAMP→DATE
--   * canonical_path_malignant_patient_rollup_v1.earliest_malignant_path_date,
--       latest_malignant_path_date — TIMESTAMP→DATE
--   * canonical_recurrence_v1.first_surgery_date, recurrence_date — TIMESTAMP→DATE
--
-- Preconditions (Cowork / Logan — run BEFORE Section C):
--   1. Pre-flight probe (prompt §1a) returns COUNT = **21** rows.
--   2. For each VARCHAR col: probe §2c — COUNT where non-null AND TRY parse IS NULL = **0**.
--   3. For each TIMESTAMP col: confirm time-of-day is midnight-only OR document
--      CF-mig160-TIMESTAMP-TIME-PORTION-NONZERO-<col>.
--   4. connect_locked() / USE thyroid_canonical_publication_v1_0; CPM 10871 invariant.
--
-- VARCHAR USING aligns with scripts/413_clinical_date_retype.py (COALESCE TRY_STRPTIME
-- '%m/%d/%Y', '%Y-%m-%d', '%-m/%-d/%Y') plus '%m/%d/%y' (2-digit year; DuckDB maps %y→20YY)
-- and literal guards for '', nan, none, null (case-insensitive trim).
--
-- Rollback:
--   CREATE OR REPLACE TABLE main.<table> AS
--   SELECT * EXCLUDE (pre_mig160_snapshot_ts)
--   FROM "Thyroid 2026 UPdated".archive_pub_v1_0.<table>_pre_mig160_20260429;
--   (adjust EXCLUDE if snapshot helper cols differ)
--
-- Dependent views: After ALTER, any view whose duckdb_views().sql still assumes old
--   types may need CREATE OR REPLACE — mirror scripts/413_clinical_date_retype.py
--   dependent-view pass if BinderException on SELECT.
--
-- CF carry-forwards (informational on success):
--   * CF-mig160-GATE-5-CLOSURE — gate 5 count 21→0 post-apply + Section E verify
--   * CF-mig160-2DIGIT-YEAR-NORMALIZATION-APPLIED — cols using '%m/%d/%y' arm (frozen
--     rollup slots rare edge ISO; molecular resolved_test_date per live data)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section A — Pre-snapshots (archive_pub_v1_0; full row copy + stamp)
-- Re-run safety: DROP TABLE archive ..._pre_mig160_20260429 manually if repeating.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_ete_event_resolved_v1_pre_mig160_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig160_snapshot_ts
FROM main.canonical_ete_event_resolved_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig160_snapshot_ts
FROM main.canonical_frozen_section_patient_rollup_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_molecular_genetics_v2_pre_mig160_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig160_snapshot_ts
FROM main.canonical_molecular_genetics_v2;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_patient_rollup_v1_pre_mig160_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig160_snapshot_ts
FROM main.canonical_path_malignant_patient_rollup_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig160_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig160_snapshot_ts
FROM main.canonical_recurrence_v1;

-- -----------------------------------------------------------------------------
-- Section B — Format-inventory probes (REFERENCE ONLY; uncomment & run per col)
-- -----------------------------------------------------------------------------
-- SELECT 'canonical_frozen_section_patient_rollup_v1.frozen_1_date' AS src, value, COUNT(*) AS n
-- FROM main.canonical_frozen_section_patient_rollup_v1
-- WHERE frozen_1_date IS NOT NULL
-- GROUP BY value ORDER BY n DESC LIMIT 20;
--
-- SELECT COUNT(*) AS would_fail FROM main.canonical_frozen_section_patient_rollup_v1
-- WHERE frozen_1_date IS NOT NULL AND COALESCE(
--   TRY_STRPTIME(TRIM(frozen_1_date), '%m/%d/%Y')::DATE,
--   TRY_STRPTIME(TRIM(frozen_1_date), '%m/%d/%y')::DATE,
--   TRY_STRPTIME(TRIM(frozen_1_date), '%Y-%m-%d')::DATE,
--   TRY_STRPTIME(TRIM(frozen_1_date), '%-m/%-d/%Y')::DATE
-- ) IS NULL;
--
-- TIMESTAMP midnight probe (example — last_known_alive_date):
-- SELECT MIN(EXTRACT(HOUR FROM last_known_alive_date)), MAX(EXTRACT(HOUR FROM last_known_alive_date)),
--        MIN(EXTRACT(MINUTE FROM last_known_alive_date)), COUNT(*)
-- FROM main.canonical_ete_event_resolved_v1 WHERE last_known_alive_date IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Section C + D — ALTER COLUMN retypes + registry append (single transaction)
-- -----------------------------------------------------------------------------
BEGIN TRANSACTION;

-- ---- canonical_ete_event_resolved_v1 (1 TIMESTAMP→DATE)
ALTER TABLE main.canonical_ete_event_resolved_v1
  ALTER COLUMN last_known_alive_date SET DATA TYPE DATE
  USING CAST(last_known_alive_date AS DATE);

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_160: Calendar-DATE retype applied. Was TIMESTAMP, now DATE. '
            || 'Calendar truncation CAST(last_known_alive_date AS DATE). '
            || 'Pre-snapshot archive_pub_v1_0.canonical_ete_event_resolved_v1_pre_mig160_20260429. '
            || 'CF-mig121-ETE-EVENT-LAST-ALIVE-RETYPE superseded by mig_160.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_event_resolved_v1'
  AND column_name = 'last_known_alive_date';

-- ---- canonical_frozen_section_patient_rollup_v1 (14 VARCHAR→DATE; MM/DD/YYYY primary)
-- frozen_1_date .. frozen_12_date
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_1_date SET DATA TYPE DATE USING CASE WHEN frozen_1_date IS NULL OR LOWER(TRIM(frozen_1_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_1_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_1_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_1_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_1_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_2_date SET DATA TYPE DATE USING CASE WHEN frozen_2_date IS NULL OR LOWER(TRIM(frozen_2_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_2_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_2_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_2_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_2_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_3_date SET DATA TYPE DATE USING CASE WHEN frozen_3_date IS NULL OR LOWER(TRIM(frozen_3_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_3_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_3_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_3_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_3_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_4_date SET DATA TYPE DATE USING CASE WHEN frozen_4_date IS NULL OR LOWER(TRIM(frozen_4_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_4_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_4_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_4_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_4_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_5_date SET DATA TYPE DATE USING CASE WHEN frozen_5_date IS NULL OR LOWER(TRIM(frozen_5_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_5_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_5_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_5_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_5_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_6_date SET DATA TYPE DATE USING CASE WHEN frozen_6_date IS NULL OR LOWER(TRIM(frozen_6_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_6_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_6_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_6_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_6_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_7_date SET DATA TYPE DATE USING CASE WHEN frozen_7_date IS NULL OR LOWER(TRIM(frozen_7_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_7_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_7_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_7_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_7_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_8_date SET DATA TYPE DATE USING CASE WHEN frozen_8_date IS NULL OR LOWER(TRIM(frozen_8_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_8_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_8_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_8_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_8_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_9_date SET DATA TYPE DATE USING CASE WHEN frozen_9_date IS NULL OR LOWER(TRIM(frozen_9_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_9_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_9_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_9_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_9_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_10_date SET DATA TYPE DATE USING CASE WHEN frozen_10_date IS NULL OR LOWER(TRIM(frozen_10_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_10_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_10_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_10_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_10_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_11_date SET DATA TYPE DATE USING CASE WHEN frozen_11_date IS NULL OR LOWER(TRIM(frozen_11_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_11_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_11_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_11_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_11_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_12_date SET DATA TYPE DATE USING CASE WHEN frozen_12_date IS NULL OR LOWER(TRIM(frozen_12_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_12_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_12_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_12_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_12_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_section_first_date SET DATA TYPE DATE USING CASE WHEN frozen_section_first_date IS NULL OR LOWER(TRIM(frozen_section_first_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_section_first_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_section_first_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_section_first_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_section_first_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_frozen_section_patient_rollup_v1 ALTER COLUMN frozen_section_last_date SET DATA TYPE DATE USING CASE WHEN frozen_section_last_date IS NULL OR LOWER(TRIM(frozen_section_last_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(frozen_section_last_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(frozen_section_last_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(frozen_section_last_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(frozen_section_last_date), '%-m/%-d/%Y')::DATE) END;

UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_1_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_2_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_3_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_4_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_5_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_6_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_7_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_8_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_9_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_10_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_11_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen slot / first / last). Formats coerced via TRY_STRPTIME mm/dd/YYYY+y+ISO; CF-119-FROZEN-ROLLUP-DATE-RETYPE CLOSED. Pre-snapshot canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_12_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen rollup aggregate first). Same parser as slot dates; CF-119 CLOSED.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_section_first_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR→DATE (frozen rollup aggregate last). Same parser as slot dates; CF-119 CLOSED.' WHERE schema_name = 'main' AND table_name = 'canonical_frozen_section_patient_rollup_v1' AND column_name = 'frozen_section_last_date';

-- ---- canonical_molecular_genetics_v2 (1 VARCHAR + 1 TIMESTAMP)
ALTER TABLE main.canonical_molecular_genetics_v2 ALTER COLUMN resolved_test_date SET DATA TYPE DATE USING CASE WHEN resolved_test_date IS NULL OR LOWER(TRIM(resolved_test_date)) IN ('','nan','none','null') THEN CAST(NULL AS DATE) ELSE COALESCE(TRY_STRPTIME(TRIM(resolved_test_date), '%m/%d/%Y')::DATE, TRY_STRPTIME(TRIM(resolved_test_date), '%m/%d/%y')::DATE, TRY_STRPTIME(TRIM(resolved_test_date), '%Y-%m-%d')::DATE, TRY_STRPTIME(TRIM(resolved_test_date), '%-m/%-d/%Y')::DATE) END;
ALTER TABLE main.canonical_molecular_genetics_v2 ALTER COLUMN test_date_native SET DATA TYPE DATE USING CAST(test_date_native AS DATE);

UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: VARCHAR resolved_test_date→DATE (TRY_STRPTIME ladder); pre-snapshot canonical_molecular_genetics_v2_pre_mig160_20260429. CF-mig160-2DIGIT-YEAR if %y arm used.' WHERE schema_name = 'main' AND table_name = 'canonical_molecular_genetics_v2' AND column_name = 'resolved_test_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: TIMESTAMP test_date_native→DATE (CAST calendar day); pre-snapshot canonical_molecular_genetics_v2_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_molecular_genetics_v2' AND column_name = 'test_date_native';

-- ---- canonical_path_malignant_patient_rollup_v1 (2 TIMESTAMP→DATE)
ALTER TABLE main.canonical_path_malignant_patient_rollup_v1 ALTER COLUMN earliest_malignant_path_date SET DATA TYPE DATE USING CAST(earliest_malignant_path_date AS DATE);
ALTER TABLE main.canonical_path_malignant_patient_rollup_v1 ALTER COLUMN latest_malignant_path_date SET DATA TYPE DATE USING CAST(latest_malignant_path_date AS DATE);

UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: TIMESTAMP earliest_malignant_path_date→DATE; pre-snapshot canonical_path_malignant_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_path_malignant_patient_rollup_v1' AND column_name = 'earliest_malignant_path_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: TIMESTAMP latest_malignant_path_date→DATE; pre-snapshot canonical_path_malignant_patient_rollup_v1_pre_mig160_20260429.' WHERE schema_name = 'main' AND table_name = 'canonical_path_malignant_patient_rollup_v1' AND column_name = 'latest_malignant_path_date';

-- ---- canonical_recurrence_v1 (2 TIMESTAMP→DATE)
ALTER TABLE main.canonical_recurrence_v1 ALTER COLUMN first_surgery_date SET DATA TYPE DATE USING CAST(first_surgery_date AS DATE);
ALTER TABLE main.canonical_recurrence_v1 ALTER COLUMN recurrence_date SET DATA TYPE DATE USING CAST(recurrence_date AS DATE);

UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: TIMESTAMP first_surgery_date→DATE (CAST); pre-snapshot canonical_recurrence_v1_pre_mig160_20260429; mig_131 spine.' WHERE schema_name = 'main' AND table_name = 'canonical_recurrence_v1' AND column_name = 'first_surgery_date';
UPDATE main.canonical_column_verification_registry_v1 SET notes = COALESCE(notes,'') || ' | mig_160: TIMESTAMP recurrence_date→DATE (CAST); pre-snapshot canonical_recurrence_v1_pre_mig160_20260429; mig_131 spine.' WHERE schema_name = 'main' AND table_name = 'canonical_recurrence_v1' AND column_name = 'recurrence_date';

COMMIT;

-- -----------------------------------------------------------------------------
-- Section E — Post-apply gate-5 verification (expect **0** rows OR COUNT = 0)
-- -----------------------------------------------------------------------------
-- WITH verified_tables AS (
--   SELECT table_name FROM main.canonical_table_signoff_registry_v1
--   WHERE table_status='verified' AND table_name LIKE 'canonical_%'
-- ),
-- audit_allowlist AS (
--   SELECT col_name FROM (VALUES
--     ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),('llm_extracted_at'),
--     ('verified_ts'),('signed_off_ts'),('registered_ts'),('updated_at'),('created_at'),
--     ('promoted_at'),('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
--     ('ingestion_date'),('lab_datetime')
--   ) v(col_name)
-- )
-- SELECT c.table_name, c.column_name, c.data_type
-- FROM information_schema.columns c
-- JOIN verified_tables v ON c.table_name = v.table_name
-- LEFT JOIN main.canonical_column_verification_registry_v1 r
--   ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
-- WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
--   AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
--   AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source'
--   AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw'
--   AND COALESCE(r.verification_status,'unknown') != 'na'
--   AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
--        OR (c.data_type='VARCHAR' AND (regexp_matches(c.column_name, '(^|_)dates?(_|$)')
--                                        OR regexp_matches(c.column_name, '(^|_)dt(_|$)'))))
-- ORDER BY c.table_name, c.column_name;
--
-- Aggregate gate:
-- WITH (...) SELECT COUNT(*) AS gate5_date_violations FROM ... ;  -- expect 0

-- =============================================================================
-- end migration 160 — global clinical DATE retype (21 cols × 5 tables)
-- =============================================================================
