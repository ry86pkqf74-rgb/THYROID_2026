-- mig_277: NIFTP full reclassify (95 residual malignant-flagged patients)
-- Date: 2026-05-03 (apply on MotherDuck thyroid_canonical_publication_v1_0)
--
-- Logan-ratified: NIFTP is never malignant under WHO 2017 / AJCC 8, regardless of
-- preoperative Bethesda category. Generalizes mig_264b (22 NIFTP @ Bethesda II).
--
-- Live CPM columns: ajcc8_t_stage / ajcc8_n_stage / ajcc8_m_stage (not ajcc8_t sans _stage).
-- Resolved overlay: *_stage_resolved quartet.
--
-- Archive: "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig277_20260503 (117 rows = all NIFTP)
--
-- Preconditions (verify before apply): 117 NIFTP rows; 95 is_malignant=TRUE; pre n_malignant cohort 4113.
-- Postconditions: NIFTP + is_malignant = 0; NIFTP + ajcc8_stage_group = 0; n_malignant = 4018 (36.96%).
--
-- signoff_migration: mig_277 / cursor_composer_mig277
-- Closes: CF-mig264b-NIFTP-RESIDUAL-95
-- Opens: CF-mig277-MANUSCRIPT-RATE-FOOTNOTE (37.83% → 36.96% malignant rate prose)

/*
-- Probe 2.1
SELECT bethesda_final, COUNT(*) AS n,
       COUNT(*) FILTER (WHERE is_malignant) AS n_malig,
       COUNT(*) FILTER (WHERE ajcc8_stage_group IS NOT NULL) AS n_w_stage
FROM main.canonical_patient_master
WHERE histology_final ILIKE '%niftp%'
GROUP BY bethesda_final ORDER BY bethesda_final NULLS LAST;
*/

/*
-- Archive (117 rows)

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig277_20260503 AS
SELECT research_id, histology_final, bethesda_final, is_malignant,
       ajcc8_stage_group, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage,
       ajcc8_stage_group_resolved, ajcc8_t_stage_resolved,
       ajcc8_n_stage_resolved, ajcc8_m_stage_resolved
FROM main.canonical_patient_master
WHERE histology_final ILIKE '%niftp%';
*/

/*
-- Apply (expect 95 rows updated)

UPDATE main.canonical_patient_master
SET
  is_malignant = FALSE,
  ajcc8_stage_group = NULL,
  ajcc8_t_stage = NULL,
  ajcc8_n_stage = NULL,
  ajcc8_m_stage = NULL,
  ajcc8_stage_group_resolved = NULL,
  ajcc8_t_stage_resolved = NULL,
  ajcc8_n_stage_resolved = NULL,
  ajcc8_m_stage_resolved = NULL,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE histology_final ILIKE '%niftp%' AND is_malignant = TRUE;
*/

/*
-- Verify
SELECT COUNT(*) AS n_total,
       COUNT(*) FILTER (WHERE is_malignant) AS n_malig,
       ROUND(100.0 * COUNT(*) FILTER (WHERE is_malignant) / COUNT(*), 2) AS pct_malig,
       COUNT(*) FILTER (WHERE histology_final ILIKE '%niftp%' AND is_malignant) AS n_niftp_mal,
       COUNT(*) FILTER (WHERE histology_final ILIKE '%niftp%' AND ajcc8_stage_group IS NOT NULL) AS n_niftp_stg
FROM main.canonical_patient_master;
*/

/*
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES (
  'mig_277', CURRENT_TIMESTAMP, 'cursor_composer_mig277',
  'mig_277: NIFTP full reclassify — ... (see mig_277_apply_log)'
);
*/
