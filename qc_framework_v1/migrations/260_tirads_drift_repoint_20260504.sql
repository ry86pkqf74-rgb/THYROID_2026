-- =============================================================================
-- mig_260 — TIRADS drift repoint (CF-mig260f)
-- Date:    2026-05-04
-- Lane:    Downstream-only — live CPM is correct post-mig_265. TI-RADS bands for
--          manuscript/Snowflake consumers come from main.canonical_us_patient_master_VIEW_v2
--          (SSOT column: max_tirads_category_ever), not removed CPM v12/imaging/preop cols.
--
-- Changes:
--   * manuscript_workspace.cohort_m025_tirads_performance_v1 — add explicit
--     cupm.max_tirads_category_ever AS max_tirads_category_ever (same value as
--     tirads_worst_category_v12 — exposes SSOT name for Prompt 7 / Table 1 parity).
--
-- Prerequisites: thyroid_canonical_publication_v1_0 + canonical_us_patient_master_VIEW_v2.
-- Nodule-level spine (mig_306): verified live — TIRADS from canonical_us_nodule_v2 only.
--   CPM join is research_id + is_malignant only (no removed TIRADS columns).
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m025_tirads_performance_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.race,
  cupm.tirads_category_at_last_preop_exam AS preop_tirads_category,
  cupm.tirads_category_at_first_exam AS tirads_best_category_v12,
  cupm.max_tirads_category_ever AS tirads_worst_category_v12,
  cupm.max_tirads_category_ever AS max_tirads_category_ever,
  CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT) AS tirads_best_score_v12,
  CAST(substr(cupm.max_tirads_category_ever, 3) AS BIGINT) AS tirads_worst_score_v12,
  CAST(NULL AS VARCHAR) AS tirads_worst_rank_source,
  cupm.n_us_exams AS n_us_exams,
  p.dominant_nodule_size_cm AS imaging_nodule_size_cm,
  p.dominant_nodule_size_cm,
  p.bethesda_final,
  p.bethesda_final_name,
  p.histology_final,
  p.is_malignant,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.path_tumor_size_cm,
  p.fna_path_concordance_category,
  p.fna_path_concordant,
  p.surg_procedure_type,
  p.surg_first_date
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE (cupm.tirads_category_at_last_preop_exam IS NOT NULL)
   OR (cupm.tirads_category_at_first_exam IS NOT NULL);

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_260',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig260',
  'mig_260: TIRADS drift repoint — cohort_m025_tirads_performance_v1 exposes max_tirads_category_ever (canonical_us_patient_master_VIEW_v2 SSOT). No CPM TIRADS writers. Snowflake: 01_export + Prompt 7 already join CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT. cohort_m025_nodule_level_v1 (mig_306): per-nodule TIRADS from canonical_us_nodule_v2 only — CPM used for is_malignant only. Closes CF-mig260f.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_260');
