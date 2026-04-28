-- =============================================================================
-- Migration 95 -- ETE three-bucket taxonomy + invasion-family rollup closeout
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser <logan.glosser@gmail.com> (Cowork session)
-- Scope:
--   1. Fix CF-91-GROSS-VS-MICRO-ETE-NAMING by splitting generic ETE present
--      away from true gross/extensive ETE.
--   2. Rename canonical_invasion_events_v1.linkage_ambiguous_multi_episode
--      to linkage_ambiguous_multi_finding.
--   3. Rebuild canonical_invasion_patient_rollup_v1 with NFS and any-ETE
--      union columns.
--   4. Repoint downstream CPM ETE feeder columns to the corrected rollup and
--      log CPM provenance.
--   5. Verify/sign off the five invasion-family patient rollups.
-- =============================================================================
-- Target taxonomy:
--   gross_ete                         = explicit gross / extensive / macroscopic
--   microscopic_ete                   = explicit microscopic / minimal / focal
--   ete_present_not_further_specified = ETE present but not further graded
--
-- Important: source_row_id='gross_ete|...' with evidence_qualifier='1' remains
-- gross_ete. The reclass target is generic structured extrathyroidal_extension
-- source text such as present / yes / true.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_invasion_events_v1_pre_mig95_20260428_ete_taxonomy AS
SELECT * FROM main.canonical_invasion_events_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_invasion_patient_rollup_v1_pre_mig95_20260428_ete_taxonomy AS
SELECT * FROM main.canonical_invasion_patient_rollup_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig95_20260428_ete_taxonomy AS
SELECT * FROM main.canonical_patient_master;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig95_20260428_ete_taxonomy AS
SELECT * FROM main.canonical_column_verification_registry_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig95_20260428_ete_taxonomy AS
SELECT * FROM main.canonical_table_signoff_registry_v1;

-- CF-91-LINKAGE-COL-NAME: the value counts multiple findings for the same
-- finding-date partition, not multiple candidate surgery episodes.
ALTER TABLE main.canonical_invasion_events_v1
RENAME COLUMN linkage_ambiguous_multi_episode TO linkage_ambiguous_multi_finding;

UPDATE main.canonical_column_verification_registry_v1
SET column_name = 'linkage_ambiguous_multi_finding',
    notes = COALESCE(notes, '') || ' | mig_95 rename from linkage_ambiguous_multi_episode; counts multi-finding ambiguity, not candidate surgery episodes.'
WHERE schema_name='main'
  AND table_name='canonical_invasion_events_v1'
  AND column_name='linkage_ambiguous_multi_episode';

-- CF-91-GROSS-VS-MICRO-ETE-NAMING: generic structured path ETE is present NFS,
-- not gross. Preserve explicit gross_ete BIGINT source rows and extensive rows.
UPDATE main.canonical_invasion_events_v1
SET invasion_type = 'ete_present_not_further_specified',
    build_script = 'mig_95_ete_three_bucket_taxonomy_20260428'
WHERE finding_status='present'
  AND invasion_type='gross_ete'
  AND source_modality='synoptic_path'
  AND source_kind='structured'
  AND LOWER(TRIM(REGEXP_REPLACE(COALESCE(evidence_qualifier, ''), ';+$', '')))
      IN ('present', 'yes', 'true', 'present (perithyroidal fibroadipose tissue involved)');

-- Rebuild the family rollup from corrected events plus already-verified sibling
-- rollups. This is Migration 60's feeder-refresh pattern with the NFS bucket and
-- explicit any-ETE union columns added.
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
    BOOL_OR(invasion_type = 'ete_present_not_further_specified' AND finding_status = 'present') AS any_ete_present_not_further_specified_anywhere_evt,
    BOOL_OR(invasion_type = 'ete_present_not_further_specified' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_ete_present_not_further_specified_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'ete_present_not_further_specified' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_ete_present_not_further_specified_in_imaging_evt,
    BOOL_OR(invasion_type IN ('gross_ete', 'microscopic_ete', 'ete_present_not_further_specified', 'soft_tissue') AND finding_status = 'present') AS any_ete_anywhere_evt,
    BOOL_OR(invasion_type IN ('gross_ete', 'microscopic_ete', 'ete_present_not_further_specified', 'soft_tissue') AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_ete_in_op_or_path_evt,
    BOOL_OR(invasion_type IN ('gross_ete', 'microscopic_ete', 'ete_present_not_further_specified', 'soft_tissue') AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_ete_in_imaging_evt,

    BOOL_OR(invasion_type = 'vascular_microscopic' AND finding_status = 'present') AS any_vascular_microscopic_anywhere_evt,
    BOOL_OR(invasion_type = 'vascular_microscopic' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_vascular_microscopic_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'vascular_microscopic' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_vascular_microscopic_in_imaging_evt,
    BOOL_OR(invasion_type = 'lymphatic_microscopic' AND finding_status = 'present') AS any_lymphatic_microscopic_anywhere_evt,
    BOOL_OR(invasion_type = 'lymphatic_microscopic' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_lymphatic_microscopic_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'lymphatic_microscopic' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_lymphatic_microscopic_in_imaging_evt,
    BOOL_OR(invasion_type = 'perineural' AND finding_status = 'present') AS any_perineural_anywhere_evt,
    BOOL_OR(invasion_type = 'perineural' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_perineural_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'perineural' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_perineural_in_imaging_evt,
    BOOL_OR(invasion_type = 'capsular' AND finding_status = 'present') AS any_capsular_anywhere_evt,
    BOOL_OR(invasion_type = 'capsular' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_capsular_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'capsular' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_capsular_in_imaging_evt,
    BOOL_OR(invasion_type = 'soft_tissue' AND finding_status = 'present') AS any_soft_tissue_anywhere_evt,
    BOOL_OR(invasion_type = 'soft_tissue' AND finding_status = 'present' AND source_modality IN ('op_note', 'synoptic_path')) AS any_soft_tissue_in_op_or_path_evt,
    BOOL_OR(invasion_type = 'soft_tissue' AND finding_status = 'present' AND source_modality IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed')) AS any_soft_tissue_in_imaging_evt,
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
  SELECT research_id,
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
  SELECT research_id,
    BOOL_OR(tracheal_invasion IN ('present', 'shaved')) AS any_trach_anywhere,
    BOOL_OR(tracheal_invasion IN ('present', 'shaved') OR laryngeal_invasion = 'present' OR cricoid_invasion = 'present' OR esophageal_invasion = 'present') AS any_airway_anywhere,
    BOOL_OR(esophageal_invasion = 'present') AS any_esoph_anywhere,
    BOOL_OR(rln_invasion = 'present') AS any_rln_anywhere,
    BOOL_OR(t4a_implication = 'pT4a') AS any_pT4a_direct_anywhere,
    BOOL_OR(tracheal_invasion IN ('present', 'shaved') AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_trach_op_or_path,
    BOOL_OR(tracheal_invasion IN ('present', 'shaved') AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_trach_imaging,
    BOOL_OR((tracheal_invasion IN ('present', 'shaved') OR laryngeal_invasion = 'present' OR cricoid_invasion = 'present' OR esophageal_invasion = 'present') AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_airway_op_or_path,
    BOOL_OR((tracheal_invasion IN ('present', 'shaved') OR laryngeal_invasion = 'present' OR cricoid_invasion = 'present' OR esophageal_invasion = 'present') AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_airway_imaging,
    BOOL_OR(esophageal_invasion = 'present' AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_esoph_op_or_path,
    BOOL_OR(esophageal_invasion = 'present' AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_esoph_imaging,
    BOOL_OR(rln_invasion = 'present' AND COALESCE(note_type, '') NOT IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_rln_op_or_path,
    BOOL_OR(rln_invasion = 'present' AND note_type IN ('ct', 'mri', 'ultrasound', 'pet_ct', 'nucmed', 'imaging')) AS any_rln_imaging
  FROM main.canonical_airway_invasion_events_v1
  GROUP BY research_id
),
ete_sub_v2 AS (
  SELECT research_id,
    any_gross_ete AS v2_gross_ete_anywhere,
    any_microscopic_ete AS v2_micro_ete_anywhere,
    any_pT4a AS v2_pT4a_ete,
    any_pT4b AS v2_pT4b_ete
  FROM main.canonical_ete_subgrade_patient_rollup_v1
),
t4b_v2 AS (
  SELECT research_id,
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
  COALESCE(e.any_ete_present_not_further_specified_anywhere_evt, FALSE) AS any_ete_present_not_further_specified_anywhere,
  COALESCE(e.any_ete_present_not_further_specified_in_op_or_path_evt, FALSE) AS any_ete_present_not_further_specified_in_op_or_path,
  COALESCE(e.any_ete_present_not_further_specified_in_imaging_evt, FALSE) AS any_ete_present_not_further_specified_in_imaging,
  COALESCE(e.any_ete_anywhere_evt, FALSE) OR COALESCE(es.v2_gross_ete_anywhere, FALSE) OR COALESCE(es.v2_micro_ete_anywhere, FALSE) AS any_ete_anywhere,
  COALESCE(e.any_ete_in_op_or_path_evt, FALSE) OR COALESCE(es.v2_gross_ete_anywhere, FALSE) OR COALESCE(es.v2_micro_ete_anywhere, FALSE) AS any_ete_in_op_or_path,
  COALESCE(e.any_ete_in_imaging_evt, FALSE) AS any_ete_in_imaging,
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
  'mig_95_ete_three_bucket_taxonomy_20260428' AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM events_agg e
LEFT JOIN vascular_v2_split vv USING (research_id)
LEFT JOIN airway_v2_split av USING (research_id)
LEFT JOIN ete_sub_v2 es USING (research_id)
LEFT JOIN t4b_v2 t USING (research_id);

COMMENT ON TABLE main.canonical_invasion_patient_rollup_v1 IS
'[domain=invasion_findings; grain=per_patient] — mig_95_ete_three_bucket_taxonomy_20260428. ETE taxonomy is gross/extensive, microscopic, and present-not-further-specified; any_ete_* is the analysis union across gross, microscopic, NFS, and soft_tissue.';

CREATE OR REPLACE VIEW views_readable.invasion_events_VIEW_v1 AS
SELECT * FROM main.canonical_invasion_events_v1;

CREATE OR REPLACE VIEW views_readable.invasion_patient_rollup_VIEW_v1 AS
SELECT * FROM main.canonical_invasion_patient_rollup_v1;

-- CPM downstream feeder alignment. Gross-specific columns now remain gross;
-- path ETE mention / any ETE flags use the new union. No AJCC restaging is
-- done here; staging requires the CPM AJCC derivation pipeline.
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS any_ete_present_not_further_specified_anywhere BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS any_ete_present_not_further_specified_in_op_or_path BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS any_ete_present_not_further_specified_in_imaging BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS any_ete_anywhere BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS any_ete_in_op_or_path BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS any_ete_in_imaging BOOLEAN;

UPDATE main.canonical_patient_master AS cpm
SET gross_ete_flag = r.any_gross_ete_anywhere,
    op_intraop_gross_ete_any = r.any_gross_ete_anywhere,
    op_nlp_gross_invasion = r.any_gross_ete_anywhere,
    nlp_path_ete_mentioned = r.any_ete_in_op_or_path,
    any_microscopic_ete_anywhere = r.any_microscopic_ete_anywhere,
    any_ete_present_not_further_specified_anywhere = r.any_ete_present_not_further_specified_anywhere,
    any_ete_present_not_further_specified_in_op_or_path = r.any_ete_present_not_further_specified_in_op_or_path,
    any_ete_present_not_further_specified_in_imaging = r.any_ete_present_not_further_specified_in_imaging,
    any_ete_anywhere = r.any_ete_anywhere,
    any_ete_in_op_or_path = r.any_ete_in_op_or_path,
    any_ete_in_imaging = r.any_ete_in_imaging,
    ete_any_present_path = r.any_ete_in_op_or_path,
    cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.canonical_invasion_patient_rollup_v1 AS r
WHERE CAST(cpm.research_id AS BIGINT) = r.research_id;

-- Registry additions for new rollup columns.
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category,
   upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes, registered_ts)
SELECT 'main', 'canonical_invasion_patient_rollup_v1', col, 'BOOLEAN', 0,
       'derived', 'main.canonical_invasion_events_v1', 'verified',
       'Logan Glosser (Cowork 2026-04-28)', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
       'mechanical_derivation_compare', 'mig_95_2026-04-28',
       'New mig_95 ETE taxonomy rollup column; rederived from corrected canonical_invasion_events_v1.',
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM (VALUES
  ('any_ete_present_not_further_specified_anywhere'),
  ('any_ete_present_not_further_specified_in_op_or_path'),
  ('any_ete_present_not_further_specified_in_imaging'),
  ('any_ete_anywhere'),
  ('any_ete_in_op_or_path'),
  ('any_ete_in_imaging')
) AS v(col)
WHERE NOT EXISTS (
  SELECT 1 FROM main.canonical_column_verification_registry_v1 r
  WHERE r.schema_name='main'
    AND r.table_name='canonical_invasion_patient_rollup_v1'
    AND r.column_name=v.col
);

-- Registry additions for new CPM columns. CPM itself remains a later large-table
-- verification target, so these are registered but not table-signoff-verified.
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category,
   upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes, registered_ts)
SELECT 'main', 'canonical_patient_master', col, 'BOOLEAN', 0,
       'derived', 'main.canonical_invasion_patient_rollup_v1', 'not_started',
       NULL, NULL, NULL, NULL,
       'Added by mig_95 as downstream ETE taxonomy feeder; CPM table-level verification deferred.',
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM (VALUES
  ('any_ete_present_not_further_specified_anywhere'),
  ('any_ete_present_not_further_specified_in_op_or_path'),
  ('any_ete_present_not_further_specified_in_imaging'),
  ('any_ete_anywhere'),
  ('any_ete_in_op_or_path'),
  ('any_ete_in_imaging')
) AS v(col)
WHERE NOT EXISTS (
  SELECT 1 FROM main.canonical_column_verification_registry_v1 r
  WHERE r.schema_name='main'
    AND r.table_name='canonical_patient_master'
    AND r.column_name=v.col
);

-- Mark corrected events taxonomy and the five invasion-family rollups verified.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='verified',
    verified_by='Logan Glosser (Cowork 2026-04-28)',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method=CASE
      WHEN table_name LIKE '%patient_rollup%' THEN 'mechanical_derivation_compare'
      WHEN table_name='canonical_invasion_events_v1' AND column_name='invasion_type' THEN 'taxonomy_rederivation_compare'
      ELSE COALESCE(verification_method, 'mechanical_derivation_compare')
    END,
    batch_id='mig_95_2026-04-28',
    notes=COALESCE(notes, '') || ' | mig_95: ETE taxonomy standardized as gross/extensive, microscopic, present_not_further_specified; rollup rederived.'
WHERE schema_name='main'
  AND table_name IN (
    'canonical_airway_invasion_patient_rollup_v1',
    'canonical_esophageal_invasion_patient_rollup_v1',
    'canonical_t4b_invasion_patient_rollup_v1',
    'canonical_vascular_invasion_patient_rollup_v1',
    'canonical_invasion_patient_rollup_v1'
  )
  AND verification_status='not_started';

UPDATE main.canonical_column_verification_registry_v1
SET verification_status='verified',
    verified_by='Logan Glosser (Cowork 2026-04-28)',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method='taxonomy_rederivation_compare',
    batch_id='mig_95_2026-04-28',
    notes=COALESCE(notes, '') || ' | mig_95: generic structured path ETE reclassified from gross_ete to ete_present_not_further_specified; explicit gross_ete=1/extensive retained as gross.'
WHERE schema_name='main'
  AND table_name='canonical_invasion_events_v1'
  AND column_name='invasion_type';

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0
      THEN CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
      ELSE ts.signed_off_ts
    END,
    signoff_migration = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0
      THEN 'qc_framework_v1/migrations/95_ete_taxonomy_and_invasion_rollups.sql'
      ELSE ts.signoff_migration
    END,
    notes = COALESCE(ts.notes, '') || ' | mig_95: invasion-family rollup verified after ETE taxonomy correction.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main'
    AND table_name IN (
      'canonical_airway_invasion_patient_rollup_v1',
      'canonical_esophageal_invasion_patient_rollup_v1',
      'canonical_t4b_invasion_patient_rollup_v1',
      'canonical_vascular_invasion_patient_rollup_v1',
      'canonical_invasion_patient_rollup_v1',
      'canonical_invasion_events_v1'
    )
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
VALUES
  ('canonical_cleanup_ete_taxonomy_20260428_phase1',
   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
   'ete_three_bucket_taxonomy_invasion_rollup_cpm_feeder_sync',
   'CF-91-GROSS-VS-MICRO-ETE-NAMING;CF-91-LINKAGE-COL-NAME',
   'invasion_family_rollups_verified',
   'CF-91-VOCAL-CORD;CF-91-NON-PRIMARY-THYROID;CF-91-LN-ENE-DOMAIN routed_to_future_domains',
   'ajcc8_t_stage_rederivation_deferred_to_CPM_AJCC_pipeline');

-- end migration 95
