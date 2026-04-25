-- ============================================================================
-- Migration 60 — canonical_invasion_patient_rollup_v1 v2 feeder refresh
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Scope:         Rollup only. main.canonical_invasion_events_v1 remains frozen
--                at Script 363 v3-iter-2 (51,773 rows / 10,871 patients).
-- Sources:       main.canonical_invasion_events_v1
--                main.canonical_vascular_invasion_{events,patient_rollup}_v1
--                main.canonical_airway_invasion_{events,patient_rollup}_v1
--                main.canonical_ete_subgrade_patient_rollup_v1
--                main.canonical_t4b_invasion_patient_rollup_v1
-- Side-car:      Clamp one parathyroid glands_identified_count=5 contract
--                violation to the schema max of 4.
-- Date:          2026-04-24
-- build_script:  mig_60_invasion_rollup_v2_refresh_20260424
-- ============================================================================

CREATE OR REPLACE TABLE main.canonical_invasion_patient_rollup_v1 AS
WITH
events_agg AS (
  SELECT
    research_id,
    BOOL_OR(invasion_type = 'gross_ete' AND finding_status = 'present') AS any_gross_ete_anywhere_evt,
    BOOL_OR(invasion_type = 'gross_ete' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_gross_ete_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'gross_ete' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_gross_ete_in_imaging_evt,
    BOOL_OR(invasion_type = 'microscopic_ete' AND finding_status = 'present') AS any_microscopic_ete_anywhere_evt,
    BOOL_OR(invasion_type = 'microscopic_ete' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_microscopic_ete_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'microscopic_ete' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_microscopic_ete_in_imaging_evt,

    -- Preserve Script 363 row-level TRUEs, then OR in v2 feeders below.
    -- This satisfies the hard no-retraction QA gate while the events table remains frozen.
    BOOL_OR(invasion_type = 'vascular_microscopic' AND finding_status = 'present') AS any_vascular_microscopic_anywhere_evt,
    BOOL_OR(invasion_type = 'vascular_microscopic' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_vascular_microscopic_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'vascular_microscopic' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_vascular_microscopic_in_imaging_evt,
    BOOL_OR(invasion_type = 'lymphatic_microscopic' AND finding_status = 'present') AS any_lymphatic_microscopic_anywhere_evt,
    BOOL_OR(invasion_type = 'lymphatic_microscopic' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_lymphatic_microscopic_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'lymphatic_microscopic' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_lymphatic_microscopic_in_imaging_evt,
    BOOL_OR(invasion_type = 'perineural' AND finding_status = 'present') AS any_perineural_anywhere_evt,
    BOOL_OR(invasion_type = 'perineural' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_perineural_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'perineural' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_perineural_in_imaging_evt,

    -- Capsular / soft_tissue have no v2 feeder in this migration.
    BOOL_OR(invasion_type = 'capsular' AND finding_status = 'present') AS any_capsular_anywhere_evt,
    BOOL_OR(invasion_type = 'capsular' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_capsular_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'capsular' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_capsular_in_imaging_evt,
    BOOL_OR(invasion_type = 'soft_tissue' AND finding_status = 'present') AS any_soft_tissue_anywhere_evt,
    BOOL_OR(invasion_type = 'soft_tissue' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_soft_tissue_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'soft_tissue' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_soft_tissue_in_imaging_evt,

    -- Preserve Script 363 row-level TRUEs, then OR in airway v2 below.
    BOOL_OR(invasion_type = 'airway' AND finding_status = 'present') AS any_airway_anywhere_evt,
    BOOL_OR(invasion_type = 'airway' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_airway_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'airway' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_airway_in_imaging_evt,
    BOOL_OR(invasion_type = 'tracheal' AND finding_status = 'present') AS any_tracheal_anywhere_evt,
    BOOL_OR(invasion_type = 'tracheal' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_tracheal_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'tracheal' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_tracheal_in_imaging_evt,
    BOOL_OR(invasion_type = 'esophageal' AND finding_status = 'present') AS any_esophageal_anywhere_evt,
    BOOL_OR(invasion_type = 'esophageal' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_esophageal_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'esophageal' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_esophageal_in_imaging_evt
  FROM main.canonical_invasion_events_v1
  GROUP BY research_id
),
vascular_v2_split AS (
  SELECT
    research_id,
    BOOL_OR(vascular_invasion = 'present') AS any_vasc_anywhere,
    BOOL_OR(lymphatic_invasion = 'present') AS any_lymph_anywhere,
    BOOL_OR(perineural_invasion = 'present') AS any_pni_anywhere,
    BOOL_OR(vascular_invasion = 'present' AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_vasc_op_or_path,
    BOOL_OR(lymphatic_invasion = 'present' AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_lymph_op_or_path,
    BOOL_OR(perineural_invasion = 'present' AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_pni_op_or_path,
    BOOL_OR(vascular_invasion = 'present' AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_vasc_imaging,
    BOOL_OR(lymphatic_invasion = 'present' AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_lymph_imaging,
    BOOL_OR(perineural_invasion = 'present' AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_pni_imaging
  FROM main.canonical_vascular_invasion_events_v1
  GROUP BY research_id
),
airway_v2_split AS (
  SELECT
    research_id,
    BOOL_OR(tracheal_invasion IN ('present', 'shaved')) AS any_trach_anywhere,
    BOOL_OR(
      tracheal_invasion IN ('present', 'shaved')
      OR laryngeal_invasion = 'present'
      OR cricoid_invasion = 'present'
      OR esophageal_invasion = 'present'
    ) AS any_airway_anywhere,
    BOOL_OR(esophageal_invasion = 'present') AS any_esoph_anywhere,
    BOOL_OR(rln_invasion = 'present') AS any_rln_anywhere,
    BOOL_OR(t4a_implication = 'pT4a') AS any_pT4a_direct_anywhere,
    BOOL_OR(tracheal_invasion IN ('present', 'shaved') AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_trach_op_or_path,
    BOOL_OR(tracheal_invasion IN ('present', 'shaved') AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_trach_imaging,
    BOOL_OR((
      tracheal_invasion IN ('present', 'shaved')
      OR laryngeal_invasion = 'present'
      OR cricoid_invasion = 'present'
      OR esophageal_invasion = 'present'
    ) AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_airway_op_or_path,
    BOOL_OR((
      tracheal_invasion IN ('present', 'shaved')
      OR laryngeal_invasion = 'present'
      OR cricoid_invasion = 'present'
      OR esophageal_invasion = 'present'
    ) AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_airway_imaging,
    BOOL_OR(esophageal_invasion = 'present' AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_esoph_op_or_path,
    BOOL_OR(esophageal_invasion = 'present' AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_esoph_imaging,
    BOOL_OR(rln_invasion = 'present' AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_rln_op_or_path,
    BOOL_OR(rln_invasion = 'present' AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_rln_imaging
  FROM main.canonical_airway_invasion_events_v1
  GROUP BY research_id
),
ete_sub_v2 AS (
  SELECT
    research_id,
    any_gross_ete AS v2_gross_ete_anywhere,
    any_microscopic_ete AS v2_micro_ete_anywhere,
    any_pT4a AS v2_pT4a_ete,
    any_pT4b AS v2_pT4b_ete
  FROM main.canonical_ete_subgrade_patient_rollup_v1
),
t4b_v2 AS (
  SELECT
    research_id,
    any_pT4b_final AS v2_pT4b_final,
    any_pT4b_direct AS v2_pT4b_direct,
    any_carotid_encasement AS v2_carotid,
    any_mediastinal_vessel AS v2_mediastinal,
    any_prevertebral_fascia AS v2_prevertebral
  FROM main.canonical_t4b_invasion_patient_rollup_v1
)
SELECT
  e.research_id,

  COALESCE(e.any_gross_ete_anywhere_evt, FALSE) OR COALESCE(es.v2_gross_ete_anywhere, FALSE) AS any_gross_ete_anywhere,
  COALESCE(e.any_gross_ete_in_op_or_path_evt, FALSE) OR COALESCE(es.v2_gross_ete_anywhere, FALSE) AS any_gross_ete_in_op_or_path,
  COALESCE(e.any_gross_ete_in_imaging_evt, FALSE) AS any_gross_ete_in_imaging,
  COALESCE(e.any_microscopic_ete_anywhere_evt, FALSE) OR COALESCE(es.v2_micro_ete_anywhere, FALSE) AS any_microscopic_ete_anywhere,
  COALESCE(e.any_microscopic_ete_in_op_or_path_evt, FALSE) OR COALESCE(es.v2_micro_ete_anywhere, FALSE) AS any_microscopic_ete_in_op_or_path,
  COALESCE(e.any_microscopic_ete_in_imaging_evt, FALSE) AS any_microscopic_ete_in_imaging,

  COALESCE(e.any_vascular_microscopic_anywhere_evt, FALSE) OR COALESCE(vv.any_vasc_anywhere, FALSE) AS any_vascular_microscopic_anywhere,
  COALESCE(e.any_vascular_microscopic_in_op_or_path_evt, FALSE) OR COALESCE(vv.any_vasc_op_or_path, FALSE) AS any_vascular_microscopic_in_op_or_path,
  COALESCE(e.any_vascular_microscopic_in_imaging_evt, FALSE) OR COALESCE(vv.any_vasc_imaging, FALSE) AS any_vascular_microscopic_in_imaging,
  COALESCE(e.any_lymphatic_microscopic_anywhere_evt, FALSE) OR COALESCE(vv.any_lymph_anywhere, FALSE) AS any_lymphatic_microscopic_anywhere,
  COALESCE(e.any_lymphatic_microscopic_in_op_or_path_evt, FALSE) OR COALESCE(vv.any_lymph_op_or_path, FALSE) AS any_lymphatic_microscopic_in_op_or_path,
  COALESCE(e.any_lymphatic_microscopic_in_imaging_evt, FALSE) OR COALESCE(vv.any_lymph_imaging, FALSE) AS any_lymphatic_microscopic_in_imaging,
  COALESCE(e.any_perineural_anywhere_evt, FALSE) OR COALESCE(vv.any_pni_anywhere, FALSE) AS any_perineural_anywhere,
  COALESCE(e.any_perineural_in_op_or_path_evt, FALSE) OR COALESCE(vv.any_pni_op_or_path, FALSE) AS any_perineural_in_op_or_path,
  COALESCE(e.any_perineural_in_imaging_evt, FALSE) OR COALESCE(vv.any_pni_imaging, FALSE) AS any_perineural_in_imaging,

  COALESCE(e.any_capsular_anywhere_evt, FALSE) AS any_capsular_anywhere,
  COALESCE(e.any_capsular_in_op_or_path_evt, FALSE) AS any_capsular_in_op_or_path,
  COALESCE(e.any_capsular_in_imaging_evt, FALSE) AS any_capsular_in_imaging,
  COALESCE(e.any_soft_tissue_anywhere_evt, FALSE) AS any_soft_tissue_anywhere,
  COALESCE(e.any_soft_tissue_in_op_or_path_evt, FALSE) AS any_soft_tissue_in_op_or_path,
  COALESCE(e.any_soft_tissue_in_imaging_evt, FALSE) AS any_soft_tissue_in_imaging,

  COALESCE(e.any_airway_anywhere_evt, FALSE) OR COALESCE(av.any_airway_anywhere, FALSE) AS any_airway_anywhere,
  COALESCE(e.any_airway_in_op_or_path_evt, FALSE) OR COALESCE(av.any_airway_op_or_path, FALSE) AS any_airway_in_op_or_path,
  COALESCE(e.any_airway_in_imaging_evt, FALSE) OR COALESCE(av.any_airway_imaging, FALSE) AS any_airway_in_imaging,
  COALESCE(e.any_tracheal_anywhere_evt, FALSE) OR COALESCE(av.any_trach_anywhere, FALSE) AS any_tracheal_anywhere,
  COALESCE(e.any_tracheal_in_op_or_path_evt, FALSE) OR COALESCE(av.any_trach_op_or_path, FALSE) AS any_tracheal_in_op_or_path,
  COALESCE(e.any_tracheal_in_imaging_evt, FALSE) OR COALESCE(av.any_trach_imaging, FALSE) AS any_tracheal_in_imaging,
  COALESCE(e.any_esophageal_anywhere_evt, FALSE) OR COALESCE(av.any_esoph_anywhere, FALSE) AS any_esophageal_anywhere,
  COALESCE(e.any_esophageal_in_op_or_path_evt, FALSE) OR COALESCE(av.any_esoph_op_or_path, FALSE) AS any_esophageal_in_op_or_path,
  COALESCE(e.any_esophageal_in_imaging_evt, FALSE) OR COALESCE(av.any_esoph_imaging, FALSE) AS any_esophageal_in_imaging,

  COALESCE(av.any_rln_anywhere, FALSE) AS any_rln_invasion_anywhere,
  COALESCE(av.any_rln_op_or_path, FALSE) AS any_rln_invasion_in_op_or_path,
  COALESCE(av.any_rln_imaging, FALSE) AS any_rln_invasion_in_imaging,

  COALESCE(av.any_pT4a_direct_anywhere, FALSE) OR COALESCE(es.v2_pT4a_ete, FALSE) AS any_pT4a_final_anywhere,
  COALESCE(es.v2_pT4b_ete, FALSE) OR COALESCE(t.v2_pT4b_final, FALSE) AS any_pT4b_final_anywhere,

  COALESCE(t.v2_carotid, FALSE) AS any_carotid_encasement_anywhere,
  COALESCE(t.v2_mediastinal, FALSE) AS any_mediastinal_vessel_anywhere,
  COALESCE(t.v2_prevertebral, FALSE) AS any_prevertebral_fascia_anywhere,

  'mig_60_invasion_rollup_v2_refresh_20260424' AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM events_agg e
LEFT JOIN vascular_v2_split vv USING (research_id)
LEFT JOIN airway_v2_split av USING (research_id)
LEFT JOIN ete_sub_v2 es USING (research_id)
LEFT JOIN t4b_v2 t USING (research_id);

COMMENT ON TABLE main.canonical_invasion_patient_rollup_v1 IS
'[domain=invasion_findings; grain=per_patient] — mig_60_invasion_rollup_v2_refresh_20260424. Events layer frozen at Script 363; patient rollup refresh ORs structured event aggregate with v2 vascular, airway, ETE-subgrade, and T4b feeder rollups.';

CREATE SCHEMA IF NOT EXISTS archive_pub_v1_0;

CREATE TABLE IF NOT EXISTS archive_pub_v1_0.detail_table_registry_v1_pre_mig60_20260424 AS
SELECT *
FROM manuscript_workspace.detail_table_registry_v1;

DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name = 'canonical_invasion_patient_rollup_v1';

INSERT INTO manuscript_workspace.detail_table_registry_v1
 (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
  domain, feeds_master_columns, description, canonical_version,
  feeds_master_columns_secondary, feeds_master_columns_array,
  needs_manual_review, superseded_by, renamed_by_script)
VALUES
 ('canonical_invasion_patient_rollup_v1', 'main', 'research_id', 'per_patient',
  10871, 10871, 'invasion_findings', NULL,
  '[domain=invasion_findings; grain=per_patient] - source: mig_60_invasion_rollup_v2_refresh_20260424. '
  || 'Cross-modal invasion finding canonical. Events layer frozen at Script 363 (51,773 rows); '
  || 'patient rollup preserves Script 363 finding_status=present positives and additively ORs '
  || 'vascular/lymphatic/perineural/airway/tracheal/esophageal/RLN/pT4a/pT4b signals '
  || 'from canonical_{vascular,airway,ete_subgrade,t4b}_invasion_patient_rollup_v1; '
  || 'structured path-synoptic feeders retained for gross_ete/microscopic_ete/capsular/soft_tissue. '
  || 'Rows=10871, patients=10871.',
  'v1_0_mig_60_invasion_rollup_v2_refresh_20260424',
  NULL, NULL, FALSE, NULL, NULL);

UPDATE main.canonical_parathyroid_events_v1
SET glands_identified_count = 4,
    build_script = 'mig_60_parathyroid_glands5_patch_20260424'
WHERE glands_identified_count = 5;

UPDATE main.canonical_parathyroid_patient_rollup_v1 r
SET max_glands_identified = (
      SELECT MAX(e.glands_identified_count)
      FROM main.canonical_parathyroid_events_v1 e
      WHERE e.research_id = r.research_id
    ),
    build_script = CASE
      WHEN build_script LIKE '%;mig_60_parathyroid_glands5_patch_20260424;mig_60_parathyroid_glands5_patch_20260424%'
        THEN replace(
          build_script,
          ';mig_60_parathyroid_glands5_patch_20260424;mig_60_parathyroid_glands5_patch_20260424',
          ';mig_60_parathyroid_glands5_patch_20260424'
        )
      WHEN build_script LIKE '%mig_60_parathyroid_glands5_patch_20260424%'
        THEN build_script
      ELSE build_script || ';mig_60_parathyroid_glands5_patch_20260424'
    END
WHERE r.research_id IN (
  SELECT DISTINCT research_id
  FROM main.canonical_parathyroid_events_v1
  WHERE build_script = 'mig_60_parathyroid_glands5_patch_20260424'
);
