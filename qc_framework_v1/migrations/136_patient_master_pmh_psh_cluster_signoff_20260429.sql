-- =============================================================================
-- Migration 136 — canonical_patient_master PMH + PSH CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane 26 — Protocol v2 patient_master verification (Cowork survey 80 cols).
-- batch_id: mig_136_patient_master_pmh_psh_cluster_20260429
--
-- Predicate (live MotherDuck registry pre-apply):
--   pmhx_*/pshx_* + prior_surg%/history columns still not_started excluding other lanes.
--
-- SSOT lineage (verification contract):
--   * PMH NLP + LLM — scripts/215_deep_nlp_entity_integration.py aggregates from
--     archived note_entities_problem_list (regex/problem-list tiers, mig_107 / mig_114) +
--     LLM tiers from main.note_entities_llm_past_medical_hx.
--   * PSH NLP + LLM — same Script 215 from main.note_entities_llm_past_surgical_hx
--     (confidence ≥ 0.7, present polarity); deterministic replay probes 0 drift on 2026-04-29.
--     Cross-domain: canonical_psh_events_v1 + Script 365 (mig_104) differs by design from
--     LLM ontology — do NOT conflate operative-evidence drift (CF-104) with pshx_nlp_*.
--
-- Caveats pinned in-batch:
--   * STRING_AGG parity — probe note_types vs list_sort (CF-mig58-STRING-AGG-ORDER).
--   * first_*_days_from_surgery — not naive DATE_DIFF vs first_surgery_date; Script 215 anchor.
--     (CF-mig136-DAYS-SEMANTIC)
--   * NULL vs 0 on *_n_mentions absent text — COALESCE in replay probes
--     (feedback_recurrence_imaging_n_events_null.md pattern).
--
-- Pre-apply integrity (MotherDuck RW): 80 cols `not_started` for this lane; post-apply
-- n_verified advances by exactly 80 on canonical_table_signoff_registry_v1 row (CPM).
--
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 136a — 5 cols — LLM extraction provenance (past medical HX)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'llm_extraction_provenance_metadata_passthrough',
    batch_id            = 'mig_136_patient_master_pmh_psh_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_136 PMH+PSH cluster (Lane 26). SSOT LLM rollup from '
                          || 'note_entities_llm_past_medical_hx (215); provenance metadata passthrough.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'pmhx_llm_extraction_method', 'pmhx_llm_mean_confidence', 'pmhx_llm_min_confidence',
    'pmhx_llm_n_source_notes', 'pmhx_llm_note_types'
  );


-- -----------------------------------------------------------------------------
-- 136b — 5 cols — LLM extraction provenance (past surgical HX)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'llm_extraction_provenance_metadata_passthrough',
    batch_id            = 'mig_136_patient_master_pmh_psh_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_136 PMH+PSH cluster (Lane 26). SSOT LLM rollup from '
                          || 'note_entities_llm_past_surgical_hx (215); provenance passthrough.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'pshx_llm_extraction_method', 'pshx_llm_mean_confidence', 'pshx_llm_min_confidence',
    'pshx_llm_n_source_notes', 'pshx_llm_note_types'
  );


-- -----------------------------------------------------------------------------
-- 136c — 12 cols — Tier-3 NLP aggregates (PSHx from LLM surgical notes / Script 215)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215',
    batch_id            = 'mig_136_patient_master_pmh_psh_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_136 PMH+PSH cluster (Lane 26). Replay from '
                          || 'note_entities_llm_past_surgical_hx per 215 thresholds; STRING_AGG '
                          || 'parity list_sort(note_types); not 1:1 with canonical_psh_events_v1 '
                          || '(mig_104) ontology vs LLM entities — see CF-mig136-104-ontology.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'pshx_nlp_prior_fna', 'pshx_nlp_prior_fna_n_mentions',
    'pshx_nlp_prior_neck_dissection', 'pshx_nlp_prior_parathyroidectomy',
    'pshx_nlp_prior_rai', 'pshx_nlp_prior_rai_date',
    'pshx_nlp_prior_rai_days_from_surg', 'pshx_nlp_prior_rai_n_mentions',
    'pshx_nlp_prior_thyroidectomy', 'pshx_nlp_prior_thyroidectomy_date',
    'pshx_nlp_prior_thyroidectomy_days_from_surg', 'pshx_nlp_prior_thyroidectomy_n_mentions'
  );


-- -----------------------------------------------------------------------------
-- 136d — 58 cols — PMHx NLP aggregates (problem list regex + LLM tier3 medical hx)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_nlp_aggregate_per_condition',
    batch_id            = 'mig_136_patient_master_pmh_psh_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_136 PMH+PSH cluster (Lane 26). 215 rollup: regex/problem-list '
                          || '+ LLM tiers (note_entities_llm_past_medical_hx); align mig_107/114 PMH '
                          || 'family; *_days_from_surgery = Script anchor not naive DATE_DIFF '
                          || '(CF-mig136-DAYS-SEMANTIC).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'pmhx_nlp_afib', 'pmhx_nlp_afib_n_mentions',
    'pmhx_nlp_asthma', 'pmhx_nlp_asthma_n_mentions',
    'pmhx_nlp_autoimmune_thyroid_hx', 'pmhx_nlp_autoimmune_thyroid_hx_n_mentions',
    'pmhx_nlp_breast_cancer', 'pmhx_nlp_breast_cancer_n_mentions',
    'pmhx_nlp_cad', 'pmhx_nlp_cad_n_mentions',
    'pmhx_nlp_ckd', 'pmhx_nlp_ckd_n_mentions',
    'pmhx_nlp_coagulopathy',
    'pmhx_nlp_comorbidity_list',
    'pmhx_nlp_copd', 'pmhx_nlp_copd_n_mentions',
    'pmhx_nlp_depression', 'pmhx_nlp_depression_n_mentions',
    'pmhx_nlp_diabetes', 'pmhx_nlp_diabetes_first_date', 'pmhx_nlp_diabetes_first_days_from_surg',
    'pmhx_nlp_diabetes_n_mentions',
    'pmhx_nlp_extraction_method',
    'pmhx_nlp_family_hx_cancer', 'pmhx_nlp_family_hx_thyroid_n_mentions',
    'pmhx_nlp_gerd', 'pmhx_nlp_gerd_n_mentions',
    'pmhx_nlp_hypertension', 'pmhx_nlp_hypertension_first_date',
    'pmhx_nlp_hypertension_first_days_from_surg', 'pmhx_nlp_hypertension_n_mentions',
    'pmhx_nlp_hyperthyroidism', 'pmhx_nlp_hyperthyroidism_first_date',
    'pmhx_nlp_hyperthyroidism_first_days_from_surg', 'pmhx_nlp_hyperthyroidism_n_mentions',
    'pmhx_nlp_hypothyroidism', 'pmhx_nlp_hypothyroidism_first_date',
    'pmhx_nlp_hypothyroidism_first_days_from_surg', 'pmhx_nlp_hypothyroidism_n_mentions',
    'pmhx_nlp_lung_cancer', 'pmhx_nlp_lung_cancer_n_mentions',
    'pmhx_nlp_men_syndrome',
    'pmhx_nlp_n_comorbidities',
    'pmhx_nlp_n_source_notes',
    'pmhx_nlp_note_types',
    'pmhx_nlp_obesity', 'pmhx_nlp_obesity_first_date', 'pmhx_nlp_obesity_first_days_from_surg',
    'pmhx_nlp_obesity_n_mentions',
    'pmhx_nlp_osteoporosis',
    'pmhx_nlp_prior_cancer_hx', 'pmhx_nlp_prior_cancer_hx_n_mentions',
    'pmhx_nlp_radiation_exposure', 'pmhx_nlp_radiation_exposure_confidence',
    'pmhx_nlp_radiation_exposure_date', 'pmhx_nlp_radiation_exposure_days_from_surg',
    'pmhx_nlp_radiation_exposure_n_mentions',
    'pmhx_nlp_smoking_status'
  );


-- -----------------------------------------------------------------------------
-- 136 — refresh patient_master table rollup (same pattern as mig_133/mig_135)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_136: PMH + PSH thematic cluster CLOSED (80 cols).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'       THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'           THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


-- -----------------------------------------------------------------------------
-- 136e — informational CF tagging (registry notes only — cross-domain ontology)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig136-104-ONTOLOGY: pshx_nlp_* LLM entity aggregates are not the same '
            || 'vocabulary object as canonical_psh_events_v1 (Script 365 mig_104) or operative '
            || 'replay drift counts; comparisons are informational.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_136_patient_master_pmh_psh_cluster_20260429'
  AND column_name LIKE 'pshx_nlp_%';


COMMIT;


-- =============================================================================
-- end migration 136 — CPM PMH + PSH cluster verified (80 cols flipped this lane)
-- =============================================================================
