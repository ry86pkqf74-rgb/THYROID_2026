-- Migration: 157b_clinical_residual_cleanup_20260429.sql
-- Purpose: Cleanup on canonical_patient_master mig_157 clinical-residual cluster (60 cols) post-Cowork
--          independent verification (Path C):
--
--          (A) high_risk_molecular_v7: reclassify verified -> na (Type-B placeholder).
--              Cohort-uniformity sweep: 0 TRUE / 10,024 FALSE / 847 NULL — degenerate FALSE-only across
--              the entire non-null subset (10,024 patients evaluated, 0 met high-risk-molecular-v7
--              criteria). Same Type-B pattern as mig_148b iodine_avidity_flag, mig_151b radtx degenerates,
--              mig_156b prm_high_risk_marker_any. The v7 high-risk molecular ladder yielded zero
--              positives on current build — likely awaits v8 ladder rebuild with real positive signal.
--              Will return to verified when rebuilt.
--
--          NOTE on the 2 TIMESTAMP date cols (first_recurrence_date, last_contact_date):
--          These are gate-5 violators that the agent's CF-mig157-CLINICAL-DATE-RETYPE flagged.
--          They are already in scope for the planned mig_160 global clinical-date retype migration
--          (cursor_prompts/CURSOR_PROMPT_global_clinical_date_retype_20260429.md). No action here.
--
--          Independent verification pre-cleanup:
--            - 60 not_started cols confirmed pre-mig_157 apply
--            - All 12 SSOTs (note_entities_llm_presenting_symptoms, canonical_path_malignant_*,
--              canonical_recurrence_v1, canonical_labs_*, canonical_complications_events_v1,
--              canonical_molecular_genetics_v2) confirmed live in main
--            - high_risk_molecular_v7: 0 T / 10024 F / 847 N confirmed
--            - first_recurrence_date / last_contact_date data_type=TIMESTAMP confirmed
--            - first_tg_date / last_tg_date / tsh_suppressed_first_date data_type=DATE confirmed
--
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 46b (mig_157 cleanup; registry-only)
-- Effect : PM n_verified -1 / n_na +1; CF appendix on high_risk_molecular_v7

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verification_method = 'helper_placeholder_pending_real_extraction_per_mig148b_pattern',
    batch_id = 'mig_157b_clinical_residual_cleanup_20260429',
    verified_by = 'cowork_mig_157b',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'') ||
            ' | mig_157b: Reclassified verified->na. Cowork independent sweep: 0 TRUE / 10024 FALSE / ' ||
            '847 NULL — degenerate FALSE-only across the entire non-null subset (10024 patients ' ||
            'evaluated, 0 met high-risk-molecular-v7 criteria). Same Type-B pattern as mig_148b ' ||
            'iodine_avidity_flag, mig_151b radtx degenerates, mig_156b prm_high_risk_marker_any. ' ||
            'The v7 high-risk molecular ladder yielded zero positives on current build — likely awaits ' ||
            'v8 ladder rebuild with real positive signal. Will return to verified when rebuilt.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'high_risk_molecular_v7';

-- Resync signoff registry
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes           = COALESCE(ts.notes,'') ||
                      ' | mig_157b: high_risk_molecular_v7 verified->na (Type-B 0 TRUE / 10024 FALSE).'
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- End of mig_157b. Already applied via query_rw 2026-04-29.
-- Pre-snapshot:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig157_20260429
