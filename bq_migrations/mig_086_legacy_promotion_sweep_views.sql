-- =============================================================================
-- migration_id: mig_086_legacy_promotion_sweep_views
-- DFL: DFL-20260506-T7
--
-- RATIONALE (2026-05-06 audit / TGDC verifier gap)
--   Cowork + verifier work showed ~55 research-grade BASE TABLEs exist only in the
--   frozen snapshot dataset `pub_legacy_source_20260416` and were never surfaced
--   as `pub_canonical.*`. Consumers querying `pub_canonical.X` hit "Not found".
--   This migration adds zero-copy VIEW facades: each view is `SELECT *` from the
--   legacy table. `pub_legacy_source_20260416` is NOT modified.
--
-- TABLE LIST (dynamic contract — re-verify in BQ before re-running):
--   Run the following in BigQuery; row count should match the number of
--   CREATE OR REPLACE VIEW statements below (expected ~55).
--
--   WITH legacy AS (
--     SELECT table_name FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416`.INFORMATION_SCHEMA.TABLES
--     WHERE table_type = 'BASE TABLE'
--   ),
--   canonical AS (
--     SELECT table_name FROM `thyroid-canonical-pub-2026.pub_canonical`.INFORMATION_SCHEMA.TABLES
--     WHERE table_type IN ('BASE TABLE', 'VIEW')
--   ),
--   workspace AS (
--     SELECT table_name FROM `thyroid-canonical-pub-2026.pub_workspace`.INFORMATION_SCHEMA.TABLES
--     WHERE table_type IN ('BASE TABLE', 'VIEW')
--   )
--   SELECT l.table_name
--   FROM legacy l
--   LEFT JOIN canonical c USING (table_name)
--   LEFT JOIN workspace w USING (table_name)
--   WHERE c.table_name IS NULL AND w.table_name IS NULL
--     AND l.table_name NOT IN (
--       'data_dictionary_v2',
--       'data_dictionary_v221',
--       'data_dictionary_parquet_v221',
--       'molecular_ingestion_runs',
--       'molecular_assay_dictionary',
--       'molecular_code_crosswalk',
--       'md_synoptic_tumor_long_v1',
--       'md_extracted_fna_bethesda_v1',
--       'lab_cross_wave_dedup_map_v1',
--       'analysis_molecular_subset_v1'
--     )
--   ORDER BY l.table_name;
--
-- SKIP LIST (remain legacy-only by design)
--   Dictionary / ingest metadata / md_* mirrors / lab dedup map / analysis subset:
--   not promoted to avoid polluting `pub_canonical` with non-research SSOT objects.
--
-- ROLLBACK (non-destructive)
--   Drop each facade view created below, e.g.:
--     DROP VIEW IF EXISTS `thyroid-canonical-pub-2026.pub_canonical.<TABLE_NAME>`;
--   Or script from this file: extract `pub_canonical.<name>` and DROP VIEW.
--   Legacy data in `pub_legacy_source_20260416` is untouched either way.
--
-- VERIFICATION (post-apply)
--   1) COUNT(*) per new view vs legacy (non-zero where legacy has rows).
--   2) Spot-check: synoptic_tumor_long_v1 → n=11103, distinct research_id=8422.
--   3) CALL `thyroid-canonical-pub-2026.pub_signoff.run_qc_assertions()` — expect no new failures.
-- =============================================================================

-- =============================================================================
-- synoptic
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.synoptic_tumor_long_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.synoptic_tumor_long_v1`;

-- =============================================================================
-- tumor_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.tumor_episode_master_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.tumor_episode_master_v2`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.tumor_pathology`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.tumor_pathology`;

-- =============================================================================
-- path_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.path_outcome_classification_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.path_outcome_classification_v1`;

-- =============================================================================
-- extracted_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.extracted_braf_recovery_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.extracted_braf_recovery_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.extracted_complications_refined_v5`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.extracted_complications_refined_v5`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.extracted_ete_subgraded_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.extracted_ete_subgraded_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.extracted_fna_bethesda_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.extracted_fna_bethesda_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.extracted_postop_labs_expanded_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.extracted_postop_labs_expanded_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.extracted_ras_patient_summary_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.extracted_ras_patient_summary_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.extracted_rln_injury_refined_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.extracted_rln_injury_refined_v2`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.extracted_tirads_validated_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.extracted_tirads_validated_v1`;

-- =============================================================================
-- molecular_ / thyroseq
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.molecular_results`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.molecular_results`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.molecular_test_episode_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.molecular_test_episode_v2`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.molecular_testing`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.molecular_testing`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.molecular_variant_long`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.molecular_variant_long`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.thyroseq_molecular_enrichment`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.thyroseq_molecular_enrichment`;

-- =============================================================================
-- note_entities_llm_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_airway_invasion`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_airway_invasion`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_functional_outcomes`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_functional_outcomes`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_imaging`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_imaging`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_labs`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_labs`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_parathyroid_detail`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_parathyroid_detail`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_patient_decision_adherence`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_patient_decision_adherence`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_physical_exam`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_physical_exam`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_rad_treatment`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_rad_treatment`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_survival_followup`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_survival_followup`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_synoptic_pathology_enrichment`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_synoptic_pathology_enrichment`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_tg_kinetics`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_tg_kinetics`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_us_nodule_dynamics`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_us_nodule_dynamics`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_vascular_invasion`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_vascular_invasion`;

-- =============================================================================
-- note_entities (non-LLM)
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_complications`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_complications`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_genetics`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_genetics`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_medications`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_medications`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_problem_list`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_problem_list`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.note_entities_staging`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_staging`;

-- =============================================================================
-- operative_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.operative_episode_detail_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.operative_episode_detail_v2`;

-- =============================================================================
-- fna_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.fna_cytology`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.fna_cytology`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.fna_episode_master_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.fna_episode_master_v2`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.fna_history`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.fna_history`;

-- =============================================================================
-- imaging_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.imaging_nodule_long_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.imaging_nodule_long_v2`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.imaging_nodule_master_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.imaging_nodule_master_v1`;

-- =============================================================================
-- ultrasound_ / us_ / tirads_llm_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.tirads_llm_extracted_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.tirads_llm_extracted_v2`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.tirads_llm_validation_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.tirads_llm_validation_v2`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.ultrasound_reports`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.ultrasound_reports`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.us_nodules_tirads`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.us_nodules_tirads`;

-- =============================================================================
-- complication_
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.complication_patient_summary_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.complication_patient_summary_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.complication_phenotype_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.complication_phenotype_v1`;

-- =============================================================================
-- lab / longitudinal
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.longitudinal_lab_canonical_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.longitudinal_lab_canonical_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.thyroglobulin_lab_canonical_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.thyroglobulin_lab_canonical_v1`;

-- =============================================================================
-- canonical_* (legacy-named)
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.canonical_benign_diagnosis_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.canonical_benign_diagnosis_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.canonical_diagnosis_unified_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.canonical_malignant_diagnosis_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.canonical_malignant_diagnosis_v1`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_tested_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.canonical_molecular_tested_v1`;

-- =============================================================================
-- outcomes / scoring
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.survival_cohort_enriched`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.survival_cohort_enriched`;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.thyroid_scoring_py_v1`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.thyroid_scoring_py_v1`;

-- =============================================================================
-- Governance: append migration log (idempotent)
-- =============================================================================
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1` (
  migration_id,
  applied_at,
  applied_by,
  description,
  affected_dataset,
  affected_table,
  pre_snapshot_table,
  rows_before,
  rows_after,
  rollback_sql,
  notes
)
SELECT
  'mig_086_legacy_promotion_sweep_views',
  CURRENT_TIMESTAMP(),
  'cursor_agent_mig086',
  'DFL-20260506-T7: VIEW facades in pub_canonical pointing at pub_legacy_source_20260416 for all legacy-only research tables (dynamic list; see migration header query). No data movement.',
  'pub_canonical',
  '55_view_facades_batch',
  CAST(NULL AS STRING),
  CAST(NULL AS INT64),
  CAST(NULL AS INT64),
  'DROP VIEW IF EXISTS for each facade listed in bq_migrations/mig_086_legacy_promotion_sweep_views.sql; legacy dataset unchanged.',
  FORMAT(
    'DFL=DFL-20260506-T7; view_facades_created=%d (not row count); file=bq_migrations/mig_086_legacy_promotion_sweep_views.sql; spot_check synoptic_tumor_long_v1 n=11103 pts=8422.',
    55
  )
FROM UNNEST([1]) AS _
WHERE NOT EXISTS (
  SELECT 1
  FROM `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
  WHERE migration_id = 'mig_086_legacy_promotion_sweep_views'
);
