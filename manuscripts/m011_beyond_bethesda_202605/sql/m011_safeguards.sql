-- =====================================================================
-- M011 — Iterative-build safeguards
-- Run at the START of every iteration, BEFORE rebuilding the cohort.
-- Order: snapshot -> QC -> provenance -> column audit -> (rebuild) -> iteration diff
-- =====================================================================

-- ---------------------------------------------------------------------
-- STEP A. Snapshot-before-overwrite — freeze the current build into pub_archive
-- Use the CURRENT date suffix each iteration (here: 20260514 = v1 baseline).
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_archive.m011_frame_b_v1_baseline_20260514` AS
  SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b`;
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_archive.m011_frame_a_primary_v1_baseline_20260514` AS
  SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary`;
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_archive.m011_patient_base_v1_baseline_20260514` AS
  SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_patient_base`;
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_archive.m011_model_data_v1_baseline_20260514` AS
  SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data`;
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_archive.m011_model_metrics_v1_baseline_20260514` AS
  SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_metrics`;
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_archive.m011_cohort_audit_v1_baseline_20260514` AS
  SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_cohort_audit`;
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_archive.m011_sensitivity_metrics_v1_baseline_20260514` AS
  SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_sensitivity_metrics`;

-- ---------------------------------------------------------------------
-- STEP B. Cohort-scoped QC assertions — M011-relevant subset of
-- pub_signoff.qc_assertions_v1 + temporal/linkage checks, FILTERED to the cohort.
-- (Full table-creation SQL: see the m011_cohort_qc CREATE TABLE block in the
--  session deliverables; reproduced compactly here.)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_cohort_qc` AS
WITH pc AS (SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b` WHERE in_primary_cohort),
fa AS (SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary` WHERE in_frame_a_cohort)
SELECT * FROM UNNEST([
  STRUCT('QC_BETHESDA_ENUM' AS check_id,'error' AS severity,
    (SELECT COUNTIF(bethesda_highest NOT IN (1,2,3,4,5,6)) FROM pc) AS violations,
    'Bethesda category outside {1..6} in primary cohort' AS description),
  ('QC_SURGERY_DATE_RANGE','error',(SELECT COUNTIF(surgery_date < DATE '1985-01-01' OR surgery_date > CURRENT_DATE()) FROM pc),'surgery_date outside 1985..today'),
  ('QC_RESEARCH_ID_UNIQUE','error',(SELECT COUNT(*)-COUNT(DISTINCT research_id) FROM pc),'duplicate research_id in patient-level frame'),
  ('QC_RESEARCH_ID_IN_MASTER','error',(SELECT COUNTIF(p.research_id IS NULL) FROM pc LEFT JOIN (SELECT DISTINCT CAST(research_id AS STRING) research_id FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`) p USING(research_id)),'research_id not in patient master'),
  ('QC_FNA_PREOP_TEMPORAL','error',(SELECT COUNTIF(first_fna_date > surgery_date) FROM pc WHERE first_fna_date IS NOT NULL AND surgery_date IS NOT NULL),'preop FNA date after surgery'),
  ('QC_US_PREOP_TEMPORAL','error',(SELECT COUNTIF(first_us_date > surgery_date) FROM pc WHERE first_us_date IS NOT NULL AND surgery_date IS NOT NULL),'preop US date after surgery'),
  ('QC_MOLECULAR_PREOP_TEMPORAL','warn',(SELECT COUNTIF(first_molecular_date > surgery_date) FROM pc WHERE first_molecular_date IS NOT NULL AND surgery_date IS NOT NULL),'preop molecular date after surgery'),
  ('QC_LN01_NONNEGATIVE','error',(SELECT COUNTIF(ln_positive_final < 0) FROM pc),'ln_positive_final negative (THY-89 family)'),
  ('QC_LN02_PLAUSIBLE_MAX','warn',(SELECT COUNTIF(ln_positive_final > 100) FROM pc),'ln_positive_final >100 (THY-89 family)'),
  ('QC_OUTCOME_NONNULL_MODELED','error',(SELECT COUNTIF(label IS NULL) FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data`),'modeled rows with NULL label'),
  ('QC_FRAMEA_LINK_TIER_VALID','error',(SELECT COUNTIF(linkage_confidence_tier NOT IN ('exact_match','high_confidence','plausible','weak','unlinked')) FROM fa),'Frame A tier outside enum'),
  ('QC_TIRADS_ENUM','warn',(SELECT COUNTIF(acr_imputed_max IS NOT NULL AND acr_imputed_max NOT BETWEEN 1 AND 5) FROM pc),'ACR TI-RADS outside {1..5}')
]);
-- Gate: every error-severity row must have violations = 0 before locking numbers.
-- SELECT * FROM m011_cohort_qc WHERE severity='error' AND violations > 0;

-- ---------------------------------------------------------------------
-- STEP C. Provenance manifest — pub_workspace.m011_provenance_manifest
-- (Static UNNEST of source table + last_modified + consumed_for; see session
--  deliverables for the full CREATE TABLE. Refresh source_last_modified each
--  iteration from `pub_canonical.__TABLES__` / each source dataset's __TABLES__.)
-- Refresh helper:
--   SELECT table_id, TIMESTAMP_MILLIS(last_modified_time) lm, row_count
--   FROM `thyroid-canonical-pub-2026.pub_canonical.__TABLES__`
--   WHERE table_id IN ('manuscript_cohort_v1','canonical_fna_events_v1', ...);

-- ---------------------------------------------------------------------
-- STEP D. Competing-source column audit — pub_workspace.m011_column_source_audit
-- Cross-checks every manuscript column against:
--   pub_signoff.canonical_column_verification_registry_v1  (verification_status)
--   pub_signoff.deprecation_registry_v1                    (deprecated columns)
-- and flags the known source-of-truth conflicts (surgery date / LN / histology).
-- Refresh helper:
--   SELECT table_name, column_name, verification_status FROM
--   `thyroid-canonical-pub-2026.pub_signoff.canonical_column_verification_registry_v1`
--   WHERE table_name='manuscript_cohort_v1' AND column_name IN (...M011 columns...);
--   SELECT * FROM `thyroid-canonical-pub-2026.pub_signoff.deprecation_registry_v1`;

-- ---------------------------------------------------------------------
-- STEP E (optional). Google Cloud AI column verification
-- Independent semantic check that M011 sourced the right columns, using the
-- BigQuery data dictionary + AI.GENERATE (or the project's console custom Agent):
--
-- SELECT a.m011_column, a.source_table, a.sot_status,
--   AI.GENERATE(
--     CONCAT('Given this data-dictionary entry: ',
--            (SELECT STRING_AGG(CONCAT(column_name,': ',COALESCE(description,'')))
--             FROM `thyroid-canonical-pub-2026.pub_canonical.data_dictionary_v279` d
--             WHERE d.table_name = a.source_table),
--            ' -- is "', a.m011_column, '" the correct column for: ', a.note,
--            '? Answer CONFIRM or REVIEW with one-line reason.'),
--     connection_id => 'us.vertex_ai',
--     endpoint => 'gemini-2.0-flash'
--   ).result AS ai_verdict
-- FROM `thyroid-canonical-pub-2026.pub_workspace.m011_column_source_audit` a;
--
-- Requires a BigQuery -> Vertex AI connection; if not provisioned, run the
-- column audit against the human-ratified verification registry only (Step D).
