-- mig_209 — ChatGPT P1 schema/registry reconciliation
-- Batch_id: mig_209_chatgpt_p1_registry_reconciliation_20260430
-- Trigger: ChatGPT review of v1.0 publication identified 3 schema/registry mismatches:
--   (a) canonical_path_malignant_events_v1: 9 live cols missing from registry
--       (ajcc_resolution_source/confidence + is_source_distinct_duplicate_grain + 6× *_stage_ajcc{7,8}_resolved)
--       — products of mig_184_v2 R1 RATIFIED + mig_185b + mig_188 r1c upstage
--   (b) canonical_us_exam_master_VIEW_v2: 1 live col missing (exam_id_source) — product of mig_187 R-A + mig_202
--   (c) canonical_invasion_patient_rollup_v1: 8 stale registry rows for cols dropped from live
--       (any_*_anywhere — likely dropped in mig_177b/mig_179 invasion rebuild)
-- All cols were already verified at the data layer by Cowork verification suite v2 §4-§5 (AJCC), §9 (dup), §10-§11 (exam_id_source).
-- Database: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT APPLY (Logan-authorized 2026-04-30 'address them'); registry-only mutations; no data writes.

USE thyroid_canonical_publication_v1_0;

-- §A pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig209_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig209_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig209_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig209_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1;

-- §B INSERT 9 PM cols + 1 exam_master col into col_registry as 'verified'
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
SELECT
  'main', c.table_name, c.column_name, c.data_type, c.ordinal_position,
  CASE
    WHEN c.column_name IN ('ajcc_resolution_source','ajcc_resolution_confidence','is_source_distinct_duplicate_grain') THEN 'provenance'
    WHEN c.column_name LIKE '%_stage_%_resolved' THEN 'analytic'
    WHEN c.column_name='exam_id_source' THEN 'provenance'
    ELSE 'analytic'
  END,
  CASE
    WHEN c.column_name LIKE '%_stage_ajcc%_resolved' THEN 'mig_184_v2 R1 AJCC RATIFIED resolved-stage build'
    WHEN c.column_name='ajcc_resolution_source' THEN 'mig_184_v2 R1 AJCC RATIFIED + mig188 r1c upstage rule'
    WHEN c.column_name='ajcc_resolution_confidence' THEN 'mig_184_v2 R1 AJCC RATIFIED'
    WHEN c.column_name='is_source_distinct_duplicate_grain' THEN 'mig_185b path_malignant duplicate scope'
    WHEN c.column_name='exam_id_source' THEN 'mig_187 R-A exam-master rebuild + mig_202 Script 366 fix'
  END,
  'verified', 'mig_209', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CASE
    WHEN c.column_name LIKE '%_stage_ajcc%_resolved' THEN 'mig_184_v2_post_apply_r1c_upstage_resolution_verified'
    WHEN c.column_name='ajcc_resolution_source' THEN 'mig_184_v2_resolution_source_label_distribution_verified_via_cowork_verify_suite_s5'
    WHEN c.column_name='ajcc_resolution_confidence' THEN 'mig_184_v2_self_describing_provenance'
    WHEN c.column_name='is_source_distinct_duplicate_grain' THEN 'mig_185b_self_describing_dup_flag_verified_525_TRUE_5944_FALSE'
    WHEN c.column_name='exam_id_source' THEN 'mig_187_R_A_exam_id_source_label_verified_via_cowork_verify_suite_s10_s11'
  END,
  'mig_209_chatgpt_p1_registry_reconciliation_20260430',
  'mig_209 P1 retro-registration after ChatGPT review identified 9 PM AJCC/dup cols + 1 exam_master exam_id_source col missing from registry. All cols are products of already-applied lanes verified at data layer.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND ((c.table_name='canonical_path_malignant_events_v1' AND c.column_name IN (
       'ajcc_resolution_source','ajcc_resolution_confidence','is_source_distinct_duplicate_grain',
       't_stage_ajcc7_resolved','n_stage_ajcc7_resolved','m_stage_ajcc7_resolved',
       't_stage_ajcc8_resolved','n_stage_ajcc8_resolved','m_stage_ajcc8_resolved'))
    OR (c.table_name='canonical_us_exam_master_VIEW_v2' AND c.column_name='exam_id_source'));

-- §C UPDATE 8 stale invasion_rollup col_registry rows → 'deprecated_dropped_from_live'
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='deprecated_dropped_from_live',
    verified_by='mig_209',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method='registry_only_archive_col_no_longer_in_live_table',
    batch_id='mig_209_chatgpt_p1_registry_reconciliation_20260430',
    notes=COALESCE(notes,'') || ' | mig_209 P1: col dropped from live in earlier rebuild lane (mig_177b/mig_179); registry row preserved with deprecated marker.'
WHERE schema_name='main' AND table_name='canonical_invasion_patient_rollup_v1'
  AND column_name IN (
    'any_carotid_encasement_anywhere','any_mediastinal_vessel_anywhere',
    'any_pT4a_final_anywhere','any_pT4b_final_anywhere',
    'any_prevertebral_fascia_anywhere','any_rln_invasion_anywhere',
    'any_rln_invasion_in_imaging','any_rln_invasion_in_op_or_path');

-- §D bump signoff_registry counts
UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = n_verified + 9, n_columns_total = n_columns_total + 9
WHERE table_name='canonical_path_malignant_events_v1';

UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = n_verified + 1, n_columns_total = n_columns_total + 1
WHERE table_name='canonical_us_exam_master_VIEW_v2';

UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = n_verified - 8, n_columns_total = n_columns_total - 8
WHERE table_name='canonical_invasion_patient_rollup_v1';

-- §E provenance
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_209_chatgpt_p1_registry_reconciliation_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'register_9_pm_ajcc_resolved_cols+1_exam_master_exam_id_source+deprecate_8_invasion_rollup_stale_cols',
   'CHATGPT-P1-SCHEMA-REGISTRY-MISMATCH-PM-EXAM-INVASION',
   'governance_completeness_post_mig_184_v2_185b_187_RA',
   '10_inserts_8_deprecates_3_signoff_count_updates',
   'none');
