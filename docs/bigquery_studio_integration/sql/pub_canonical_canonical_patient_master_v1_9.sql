-- pub_canonical.canonical_patient_master_v1_9
-- canonical_patient_master enriched with the workup-census columns, as a VIEW.
-- The base canonical_patient_master table is NOT altered - downstream consumers of the
-- base table are unaffected. Use this v1_9 view when the workup-census columns are wanted
-- alongside the master. 10,871 rows x 2,375 columns.
-- ct_n_exams / mri_n_exams / n_surgeries already existed on the master and are kept from
-- the master; the census duplicates are excluded here.
-- Migration: mig_cw_workup_census_canonical_20260514. Built 2026-05-14.

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master_v1_9`
OPTIONS (
  description = 'canonical_patient_master enriched with the workup-census columns. One row per patient (10,871). This is a VIEW: it left-joins pub_canonical.canonical_patient_workup_census_v1 onto canonical_patient_master on research_id. The base canonical_patient_master table is unchanged - downstream consumers of the base table are unaffected; use this v1_9 view when the workup-census columns are wanted alongside the master. The census columns ct_n_exams / mri_n_exams / n_surgeries already existed on canonical_patient_master and are kept from the master (the census versions are excluded here; the standalone census table retains them). Built by Cowork / BigQuery Studio Integration Plan 2026-05-14.'
) AS
SELECT
  m.*,
  c.* EXCEPT(research_id, ct_n_exams, mri_n_exams, n_surgeries)
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` m
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_workup_census_v1` c
  ON c.research_id = m.research_id;
