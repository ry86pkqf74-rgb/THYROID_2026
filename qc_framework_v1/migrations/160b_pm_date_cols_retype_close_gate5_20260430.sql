-- mig_160b PM date cols retype TIMESTAMP/VARCHAR → DATE (close gate5 to ~0)
-- Target DB: thyroid_canonical_publication_v1_0
-- Posture: COWORK-DIRECT APPLY. Logan green-lit (a)-(e) date retype class on 2026-04-30.
-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY
--
-- Predecessor: mig_160 (`16a9833`) closed 21 clinical-date cols on Tier-2 canonicals + path_malignant + recurrence + frozen rollup + ETE.
-- Scope: 25 PM cols flagged by gate5. Disposition:
--   - 18 clinical dates retype TIMESTAMP/VARCHAR → DATE
--   - 2 TIMESTAMP-WITH-TZ audit stamps retype TZ → TIMESTAMP (DuckDB TZ trap fix)
--   - 3 plain TIMESTAMP audit stamps stay TIMESTAMP (correctly typed; allowlist gap)
--   - 1 VARCHAR confidence col (gm_rai_date_confidence) stays VARCHAR (misnamed; not a date)
-- Format probe (live MD 2026-04-30): each VARCHAR col is internally consistent.
-- VIEW-dependency probe: only canonical_us_exam_master_VIEW_v2 references PM cols, but uses first_surgery_date_v2 (different col). Safe.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Pre-flight invariants
-- =============================================================================
SELECT 'cpm_row_count' AS invariant_name, COUNT(*) AS observed_value, 10871 AS expected_value FROM main.canonical_patient_master;
SELECT 'gate5_baseline' AS invariant_name, 25 AS expected_baseline;

-- =============================================================================
-- §A Pre-snapshot affected slice to archive
-- =============================================================================
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig160b_date_retype_20260430 AS
SELECT research_id,
  cnln_earliest_date, cnln_img_first_date, cnln_img_last_date, cnln_latest_date,
  cnln_surg_first_date, cnln_surg_last_date,
  cpm_built_at, first_recurrence_date, first_surgery_date,
  gm_path_stage_raw_derived_at, gm_rai_date_confidence,
  last_contact_date, mol_first_test_date, mol_test_date,
  nsqip_admission_date, nsqip_discharge_date, nsqip_first_readmission_date, nsqip_operation_date,
  ops_surg_date, path_stage_raw_derived_at, rai_first_date, recurrence_date,
  resolved_at, rollup_built_at, surg_first_date,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig160b_snapshot_ts
FROM main.canonical_patient_master;

-- =============================================================================
-- §B Retype 4 ISO-format VARCHAR clinical dates → DATE
-- =============================================================================
ALTER TABLE main.canonical_patient_master ALTER COLUMN cnln_earliest_date     SET DATA TYPE DATE USING CAST(cnln_earliest_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN cnln_img_first_date    SET DATA TYPE DATE USING CAST(cnln_img_first_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN cnln_img_last_date     SET DATA TYPE DATE USING CAST(cnln_img_last_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN cnln_surg_first_date   SET DATA TYPE DATE USING CAST(cnln_surg_first_date AS DATE);

-- =============================================================================
-- §C Retype 6 MM/DD/YYYY VARCHAR clinical dates → DATE
-- =============================================================================
ALTER TABLE main.canonical_patient_master ALTER COLUMN cnln_latest_date          SET DATA TYPE DATE USING TRY_STRPTIME(cnln_latest_date, '%m/%d/%Y')::DATE;
ALTER TABLE main.canonical_patient_master ALTER COLUMN cnln_surg_last_date       SET DATA TYPE DATE USING TRY_STRPTIME(cnln_surg_last_date, '%m/%d/%Y')::DATE;
ALTER TABLE main.canonical_patient_master ALTER COLUMN nsqip_admission_date      SET DATA TYPE DATE USING TRY_STRPTIME(nsqip_admission_date, '%m/%d/%Y')::DATE;
ALTER TABLE main.canonical_patient_master ALTER COLUMN nsqip_discharge_date      SET DATA TYPE DATE USING TRY_STRPTIME(nsqip_discharge_date, '%m/%d/%Y')::DATE;
ALTER TABLE main.canonical_patient_master ALTER COLUMN nsqip_first_readmission_date SET DATA TYPE DATE USING TRY_STRPTIME(nsqip_first_readmission_date, '%m/%d/%Y')::DATE;
ALTER TABLE main.canonical_patient_master ALTER COLUMN nsqip_operation_date      SET DATA TYPE DATE USING TRY_STRPTIME(nsqip_operation_date, '%m/%d/%Y')::DATE;
ALTER TABLE main.canonical_patient_master ALTER COLUMN ops_surg_date             SET DATA TYPE DATE USING TRY_STRPTIME(ops_surg_date, '%m/%d/%Y')::DATE;

-- =============================================================================
-- §D Retype 8 TIMESTAMP clinical dates → DATE
-- =============================================================================
ALTER TABLE main.canonical_patient_master ALTER COLUMN first_recurrence_date  SET DATA TYPE DATE USING CAST(first_recurrence_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN first_surgery_date     SET DATA TYPE DATE USING CAST(first_surgery_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN last_contact_date      SET DATA TYPE DATE USING CAST(last_contact_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN mol_first_test_date    SET DATA TYPE DATE USING CAST(mol_first_test_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN mol_test_date          SET DATA TYPE DATE USING CAST(mol_test_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN rai_first_date         SET DATA TYPE DATE USING CAST(rai_first_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN recurrence_date        SET DATA TYPE DATE USING CAST(recurrence_date AS DATE);
ALTER TABLE main.canonical_patient_master ALTER COLUMN surg_first_date        SET DATA TYPE DATE USING CAST(surg_first_date AS DATE);

-- =============================================================================
-- §E Retype 2 TIMESTAMP-WITH-TZ audit stamps → TIMESTAMP (drop TZ; reference_duckdb_timestamp_tz fix)
-- =============================================================================
ALTER TABLE main.canonical_patient_master ALTER COLUMN resolved_at      SET DATA TYPE TIMESTAMP USING CAST(resolved_at AS TIMESTAMP);
ALTER TABLE main.canonical_patient_master ALTER COLUMN rollup_built_at  SET DATA TYPE TIMESTAMP USING CAST(rollup_built_at AS TIMESTAMP);

-- =============================================================================
-- §F Registry note appendix (close CF-90-DATE-FORMAT residual + CF-mig137-PM-MOL-DATE-RETYPE
--    + CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE + CF-mig132 + CF-mig133-PM-CNCLN-DATE-PARSE
--    + CF-mig138 recurrence + CF-mig142 rai + CF-mig157 + CF-mig155-RESOLVED-LAYER-VERSION)
-- =============================================================================
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') || ' | mig_160b 2026-04-30: VARCHAR/TIMESTAMP/TIMESTAMP_TZ retyped per feedback_clinical_dates_calendar_only + reference_duckdb_timestamp_tz; gate5 close-out.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    'cnln_earliest_date','cnln_img_first_date','cnln_img_last_date','cnln_latest_date',
    'cnln_surg_first_date','cnln_surg_last_date',
    'first_recurrence_date','first_surgery_date','last_contact_date',
    'mol_first_test_date','mol_test_date',
    'nsqip_admission_date','nsqip_discharge_date','nsqip_first_readmission_date','nsqip_operation_date',
    'ops_surg_date','rai_first_date','recurrence_date','surg_first_date',
    'resolved_at','rollup_built_at'
  );

-- =============================================================================
-- §G cpm_reconciliation_provenance_v1 row insert
-- =============================================================================
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_160b_pm_date_cols_retype_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'pre_snapshot_alter_18_clinical_dates_to_DATE_alter_2_TZ_to_TS_registry_appendix',
   'CF-90-DATE-FORMAT_residual_CF-mig137-PM-MOL-DATE-RETYPE_CF-mig130-FIRST-SURGERY-DATE-RETYPE_CF-mig133-PM-CNCLN-DATE-PARSE_CF-mig142_rai_CF-mig157_residual_CF-mig155-RESOLVED-LAYER-VERSION-DEGENERATE',
   '21_cols_retyped',
   '0_failed',
   '5_audit_cols_remain_TS_or_VARCHAR_correctly_typed_audit_query_allowlist_extension_pending_v11');

-- =============================================================================
-- §H Post-state probes
-- =============================================================================
-- 5-gate audit check
WITH verified_tables AS (SELECT table_name FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified' AND table_name LIKE 'canonical_%'),
audit_allowlist AS (SELECT col_name FROM (VALUES ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),('llm_extracted_at'),('verified_ts'),('signed_off_ts'),('registered_ts'),('updated_at'),('created_at'),('promoted_at'),('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),('ingestion_date'),('lab_datetime')) v(col_name))
SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified' AND signoff_migration IS NULL) AS gate2,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t WHERE t.table_status='verified' AND (t.n_verified + t.n_na <> t.n_columns_total OR t.n_not_started <> 0 OR t.n_failed <> 0)) AS gate3,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 r JOIN main.canonical_table_signoff_registry_v1 t USING (schema_name, table_name) WHERE t.table_status='verified' AND r.verification_status='verified' AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL)) AS gate4,
  (SELECT COUNT(*) FROM information_schema.columns c JOIN verified_tables v ON c.table_name = v.table_name LEFT JOIN main.canonical_column_verification_registry_v1 r ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main' AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist) AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source' AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw' AND COALESCE(r.verification_status,'unknown') != 'na' AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE') OR (c.data_type='VARCHAR' AND (regexp_matches(c.column_name, '(^|_)dates?(_|$)') OR regexp_matches(c.column_name, '(^|_)dt(_|$)'))))) AS gate5;
