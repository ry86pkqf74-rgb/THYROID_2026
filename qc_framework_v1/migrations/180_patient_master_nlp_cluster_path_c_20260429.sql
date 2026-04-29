-- =============================================================================
-- Migration 180 — PM nlp_* cluster Path-C verify + apply SQL
-- =============================================================================
-- Date: 2026-04-29
-- Batch: mig_180_patient_master_nlp_cluster_path_c_20260429
-- Posture: authored SQL artifact only in Cursor/Logan lane; execute via governed coworker Path-C apply.
-- Target DB: thyroid_canonical_publication_v1_0
-- Primary table touched: main.canonical_column_verification_registry_v1
-- Data tables touched: NONE (registry/signoff only).
-- =============================================================================

-- §0 — pre-flight invariants (read-only)
SELECT COUNT(*) AS cpm_rows, COUNT(DISTINCT research_id) AS cpm_distinct_research_id
FROM main.canonical_patient_master;

SELECT verification_status, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND LEFT(column_name, 4) = 'nlp_'
GROUP BY 1
ORDER BY 1;

-- §A — pre-snapshot of affected registry rows
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig180_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig180_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND LEFT(column_name, 4) = 'nlp_'
  AND verification_status = 'not_started'
  AND batch_id IS NULL;

-- §B — global Path-C stamp on scoped nlp_* rows
UPDATE main.canonical_column_verification_registry_v1
SET verified_by = 'Logan Glosser <logan.glosser@gmail.com>',
    batch_id = 'mig_180_patient_master_nlp_cluster_path_c_20260429',
    verification_method = 'Path C: PM nlp cluster lineage + source-discovery + cohort-uniformity sweep',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 methodology: inventory 116 PM nlp_* columns; map families to note_entities_llm/canonical NLP sources; replay generic count/presence metrics where schema-safe; classify Boolean uniformity Type-A/Type-B; registry-only apply' WHEN POSITION('mig_180 methodology: inventory 116 PM nlp_* columns; map families to note_entities_llm/canonical NLP sources; replay generic count/presence metrics where schema-safe; classify Boolean uniformity Type-A/Type-B; registry-only apply' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 methodology: inventory 116 PM nlp_* columns; map families to note_entities_llm/canonical NLP sources; replay generic count/presence metrics where schema-safe; classify Boolean uniformity Type-A/Type-B; registry-only apply' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND LEFT(column_name, 4) = 'nlp_'
  AND verification_status = 'not_started'
  AND batch_id IS NULL;

-- §C — per-column status flips
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_airway_has_data', 'nlp_airway_key_finding', 'nlp_airway_n_entities', 'nlp_airway_n_notes', 'nlp_cervln_confidence_tier', 'nlp_cervln_has_data', 'nlp_cervln_n_entities', 'nlp_cervln_positive_mentioned', 'nlp_dynrisk_has_data', 'nlp_dynrisk_key_finding', 'nlp_dynrisk_n_entities', 'nlp_dynrisk_n_notes', 'nlp_esoph_confidence_tier', 'nlp_esoph_has_data', 'nlp_esoph_n_entities', 'nlp_esoph_positive_mentioned', 'nlp_frozensec_has_data', 'nlp_frozensec_key_finding', 'nlp_frozensec_n_entities', 'nlp_frozensec_n_notes', 'nlp_funcoutcome_has_data', 'nlp_funcoutcome_key_finding', 'nlp_funcoutcome_n_entities', 'nlp_funcoutcome_n_notes', 'nlp_imaging_has_data', 'nlp_imaging_key_finding', 'nlp_imaging_n_entities', 'nlp_imaging_n_notes', 'nlp_labs_has_data', 'nlp_labs_key_finding', 'nlp_labs_n_entities', 'nlp_labs_n_notes', 'nlp_ln_has_data', 'nlp_ln_levels_mentioned', 'nlp_ln_n_entities', 'nlp_ln_n_notes', 'nlp_ln_positive_mentioned', 'nlp_ne_complications_has_data', 'nlp_ne_complications_n_rows', 'nlp_ne_genetics_has_data', 'nlp_ne_genetics_n_rows', 'nlp_ne_medications_has_data', 'nlp_ne_medications_n_rows', 'nlp_ne_operative_has_data', 'nlp_ne_operative_n_rows', 'nlp_ne_problemlist_has_data', 'nlp_ne_problemlist_n_rows', 'nlp_ne_staging_has_data', 'nlp_ne_staging_n_rows', 'nlp_parathyroid_has_data', 'nlp_parathyroid_key_finding', 'nlp_parathyroid_n_entities', 'nlp_parathyroid_n_notes', 'nlp_path_confidence_tier', 'nlp_path_has_data', 'nlp_path_ln_positive_mentioned', 'nlp_path_margin_mentioned', 'nlp_path_multifocal_concordance_v2', 'nlp_path_multifocal_mentioned', 'nlp_path_n_entities', 'nlp_path_n_notes', 'nlp_path_positive_mentioned', 'nlp_path_vasc_inv_mentioned', 'nlp_physexam_has_data', 'nlp_physexam_key_finding', 'nlp_physexam_n_entities', 'nlp_physexam_n_notes', 'nlp_pmhx_has_data', 'nlp_pmhx_key_finding', 'nlp_pmhx_n_entities', 'nlp_pmhx_n_notes', 'nlp_pshx_has_data', 'nlp_pshx_key_finding', 'nlp_pshx_n_entities', 'nlp_pshx_n_notes', 'nlp_ptdecision_has_data', 'nlp_ptdecision_key_finding', 'nlp_ptdecision_n_entities', 'nlp_ptdecision_n_notes', 'nlp_radtx_has_data', 'nlp_radtx_key_finding', 'nlp_radtx_n_entities', 'nlp_radtx_n_notes', 'nlp_rec_any_mentioned', 'nlp_rec_confidence_tier', 'nlp_rec_disease_free_mentioned', 'nlp_rec_earliest_date', 'nlp_rec_earliest_days_from_surg', 'nlp_rec_has_data', 'nlp_rec_n_entities', 'nlp_rec_type_worst', 'nlp_survfu_has_data', 'nlp_survfu_key_finding', 'nlp_survfu_n_entities', 'nlp_survfu_n_notes', 'nlp_symptoms_has_data', 'nlp_symptoms_key_finding', 'nlp_symptoms_n_entities', 'nlp_symptoms_n_notes', 'nlp_tg_has_data', 'nlp_tg_n_entities', 'nlp_tg_undetectable_mentioned', 'nlp_tirads_has_component_detail', 'nlp_tirads_has_data', 'nlp_tirads_max_category', 'nlp_tirads_n_entities', 'nlp_tirads_n_notes', 'nlp_usnodule_has_data', 'nlp_usnodule_key_finding', 'nlp_usnodule_n_entities', 'nlp_usnodule_n_notes', 'nlp_vasc_confidence_tier', 'nlp_vasc_has_data', 'nlp_vasc_n_entities', 'nlp_vasc_positive_mentioned');

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_tg_rising_mentioned');

-- §D — per-family / per-column carry-forward notes
UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_dynrisk_has_data; true=25 false=0 null=10846' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_dynrisk_has_data; true=25 false=0 null=10846' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_dynrisk_has_data; true=25 false=0 null=10846' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_dynrisk_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_funcoutcome_has_data; true=1623 false=0 null=9248' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_funcoutcome_has_data; true=1623 false=0 null=9248' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_funcoutcome_has_data; true=1623 false=0 null=9248' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_funcoutcome_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_imaging_has_data; true=1728 false=0 null=9143' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_imaging_has_data; true=1728 false=0 null=9143' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_imaging_has_data; true=1728 false=0 null=9143' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_imaging_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_labs_has_data; true=791 false=0 null=10080' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_labs_has_data; true=791 false=0 null=10080' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_labs_has_data; true=791 false=0 null=10080' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_labs_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ln_has_data; true=868 false=0 null=10003' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ln_has_data; true=868 false=0 null=10003' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ln_has_data; true=868 false=0 null=10003' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_ln_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_complications_has_data; true=2840 false=0 null=8031' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_complications_has_data; true=2840 false=0 null=8031' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_complications_has_data; true=2840 false=0 null=8031' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_ne_complications_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_genetics_has_data; true=605 false=0 null=10266' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_genetics_has_data; true=605 false=0 null=10266' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_genetics_has_data; true=605 false=0 null=10266' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_ne_genetics_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_medications_has_data; true=2070 false=0 null=8801' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_medications_has_data; true=2070 false=0 null=8801' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_medications_has_data; true=2070 false=0 null=8801' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_ne_medications_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_operative_has_data; true=4031 false=0 null=6840' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_operative_has_data; true=4031 false=0 null=6840' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_operative_has_data; true=4031 false=0 null=6840' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_ne_operative_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_problemlist_has_data; true=4036 false=0 null=6835' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_problemlist_has_data; true=4036 false=0 null=6835' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_problemlist_has_data; true=4036 false=0 null=6835' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_ne_problemlist_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_staging_has_data; true=1639 false=0 null=9232' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_staging_has_data; true=1639 false=0 null=9232' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_staging_has_data; true=1639 false=0 null=9232' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_ne_staging_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_physexam_has_data; true=512 false=0 null=10359' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_physexam_has_data; true=512 false=0 null=10359' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_physexam_has_data; true=512 false=0 null=10359' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_physexam_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pmhx_has_data; true=290 false=0 null=10581' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pmhx_has_data; true=290 false=0 null=10581' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pmhx_has_data; true=290 false=0 null=10581' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_pmhx_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pshx_has_data; true=1864 false=0 null=9007' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pshx_has_data; true=1864 false=0 null=9007' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pshx_has_data; true=1864 false=0 null=9007' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_pshx_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ptdecision_has_data; true=367 false=0 null=10504' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ptdecision_has_data; true=367 false=0 null=10504' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ptdecision_has_data; true=367 false=0 null=10504' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_ptdecision_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_radtx_has_data; true=210 false=0 null=10661' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_radtx_has_data; true=210 false=0 null=10661' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_radtx_has_data; true=210 false=0 null=10661' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_radtx_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_any_mentioned; true=133 false=0 null=10738' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_any_mentioned; true=133 false=0 null=10738' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_any_mentioned; true=133 false=0 null=10738' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_rec_any_mentioned';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_has_data; true=133 false=0 null=10738' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_has_data; true=133 false=0 null=10738' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_has_data; true=133 false=0 null=10738' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_rec_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_survfu_has_data; true=2911 false=0 null=7960' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_survfu_has_data; true=2911 false=0 null=7960' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_survfu_has_data; true=2911 false=0 null=7960' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_survfu_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_symptoms_has_data; true=116 false=0 null=10755' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_symptoms_has_data; true=116 false=0 null=10755' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_symptoms_has_data; true=116 false=0 null=10755' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_symptoms_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tg_has_data; true=49 false=0 null=10822' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tg_has_data; true=49 false=0 null=10822' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tg_has_data; true=49 false=0 null=10822' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_tg_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_b_placeholder_zero_true; CF-mig180-NLP-PLACEHOLDER-nlp_tg_rising_mentioned; true=0 false=49 null=10822' WHEN POSITION('mig_180 type_b_placeholder_zero_true; CF-mig180-NLP-PLACEHOLDER-nlp_tg_rising_mentioned; true=0 false=49 null=10822' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_b_placeholder_zero_true; CF-mig180-NLP-PLACEHOLDER-nlp_tg_rising_mentioned; true=0 false=49 null=10822' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_tg_rising_mentioned';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tirads_has_data; true=1715 false=0 null=9156' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tirads_has_data; true=1715 false=0 null=9156' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tirads_has_data; true=1715 false=0 null=9156' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_tirads_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_usnodule_has_data; true=18 false=0 null=10853' WHEN POSITION('mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_usnodule_has_data; true=18 false=0 null=10853' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 type_a_presence_flag_true_only; CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_usnodule_has_data; true=18 false=0 null=10853' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'nlp_usnodule_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_funcoutcome; CF-mig180-NLP-UPSTREAM-MISSING-funcoutcome; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_funcoutcome; CF-mig180-NLP-UPSTREAM-MISSING-funcoutcome; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_funcoutcome; CF-mig180-NLP-UPSTREAM-MISSING-funcoutcome; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_funcoutcome_has_data', 'nlp_funcoutcome_key_finding', 'nlp_funcoutcome_n_entities', 'nlp_funcoutcome_n_notes');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_imaging; CF-mig180-NLP-UPSTREAM-MISSING-imaging; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_imaging; CF-mig180-NLP-UPSTREAM-MISSING-imaging; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_imaging; CF-mig180-NLP-UPSTREAM-MISSING-imaging; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_imaging_has_data', 'nlp_imaging_key_finding', 'nlp_imaging_n_entities', 'nlp_imaging_n_notes');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_labs; CF-mig180-NLP-UPSTREAM-MISSING-labs; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_labs; CF-mig180-NLP-UPSTREAM-MISSING-labs; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_labs; CF-mig180-NLP-UPSTREAM-MISSING-labs; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_labs_has_data', 'nlp_labs_key_finding', 'nlp_labs_n_entities', 'nlp_labs_n_notes');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_ne_complications; CF-mig180-NLP-UPSTREAM-MISSING-ne_complications; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_ne_complications; CF-mig180-NLP-UPSTREAM-MISSING-ne_complications; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_ne_complications; CF-mig180-NLP-UPSTREAM-MISSING-ne_complications; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_ne_complications_has_data', 'nlp_ne_complications_n_rows');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_ne_genetics; CF-mig180-NLP-UPSTREAM-MISSING-ne_genetics; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_ne_genetics; CF-mig180-NLP-UPSTREAM-MISSING-ne_genetics; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_ne_genetics; CF-mig180-NLP-UPSTREAM-MISSING-ne_genetics; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_ne_genetics_has_data', 'nlp_ne_genetics_n_rows');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_ne_medications; CF-mig180-NLP-UPSTREAM-MISSING-ne_medications; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_ne_medications; CF-mig180-NLP-UPSTREAM-MISSING-ne_medications; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_ne_medications; CF-mig180-NLP-UPSTREAM-MISSING-ne_medications; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_ne_medications_has_data', 'nlp_ne_medications_n_rows');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_ne_problemlist; CF-mig180-NLP-UPSTREAM-MISSING-ne_problemlist; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_ne_problemlist; CF-mig180-NLP-UPSTREAM-MISSING-ne_problemlist; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_ne_problemlist; CF-mig180-NLP-UPSTREAM-MISSING-ne_problemlist; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_ne_problemlist_has_data', 'nlp_ne_problemlist_n_rows');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_ne_staging; CF-mig180-NLP-UPSTREAM-MISSING-ne_staging; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_ne_staging; CF-mig180-NLP-UPSTREAM-MISSING-ne_staging; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_ne_staging; CF-mig180-NLP-UPSTREAM-MISSING-ne_staging; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_ne_staging_has_data', 'nlp_ne_staging_n_rows');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_physexam; CF-mig180-NLP-UPSTREAM-MISSING-physexam; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_physexam; CF-mig180-NLP-UPSTREAM-MISSING-physexam; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_physexam; CF-mig180-NLP-UPSTREAM-MISSING-physexam; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_physexam_has_data', 'nlp_physexam_key_finding', 'nlp_physexam_n_entities', 'nlp_physexam_n_notes');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_ptdecision; CF-mig180-NLP-UPSTREAM-MISSING-ptdecision; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_ptdecision; CF-mig180-NLP-UPSTREAM-MISSING-ptdecision; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_ptdecision; CF-mig180-NLP-UPSTREAM-MISSING-ptdecision; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_ptdecision_has_data', 'nlp_ptdecision_key_finding', 'nlp_ptdecision_n_entities', 'nlp_ptdecision_n_notes');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_radtx; CF-mig180-NLP-UPSTREAM-MISSING-radtx; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_radtx; CF-mig180-NLP-UPSTREAM-MISSING-radtx; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_radtx; CF-mig180-NLP-UPSTREAM-MISSING-radtx; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_radtx_has_data', 'nlp_radtx_key_finding', 'nlp_radtx_n_entities', 'nlp_radtx_n_notes');

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN 'mig_180 upstream source missing for family nlp_usnodule; CF-mig180-NLP-UPSTREAM-MISSING-usnodule; no PM data mutation performed' WHEN POSITION('mig_180 upstream source missing for family nlp_usnodule; CF-mig180-NLP-UPSTREAM-MISSING-usnodule; no PM data mutation performed' IN notes) > 0 THEN notes ELSE notes || '; ' || 'mig_180 upstream source missing for family nlp_usnodule; CF-mig180-NLP-UPSTREAM-MISSING-usnodule; no PM data mutation performed' END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_usnodule_has_data', 'nlp_usnodule_key_finding', 'nlp_usnodule_n_entities', 'nlp_usnodule_n_notes');

-- §E — resync table signoff registry for canonical_patient_master
UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='verified'),
    n_na = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='na'),
    n_not_started = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='not_started'),
    signoff_migration = 'qc_framework_v1/migrations/180_patient_master_nlp_cluster_path_c_20260429.sql',
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'canonical_patient_master';

-- §F — post-state verification probes (read-only)
SELECT verification_status, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND LEFT(column_name, 4) = 'nlp_'
  AND batch_id = 'mig_180_patient_master_nlp_cluster_path_c_20260429'
GROUP BY 1
ORDER BY 1;

SELECT n_verified, n_na, n_not_started, signoff_migration, signed_off_ts
FROM main.canonical_table_signoff_registry_v1
WHERE table_name = 'canonical_patient_master';

SELECT COUNT(*) AS cpm_rows, COUNT(DISTINCT research_id) AS cpm_distinct_research_id
FROM main.canonical_patient_master;
