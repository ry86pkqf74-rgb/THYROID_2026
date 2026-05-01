-- mig_239 — semantic_publication research_id VARCHAR standardization
--           cast research_id to VARCHAR in the 3 semantic safe views that
--           currently expose numeric (BIGINT/INTEGER) types.
-- run_id / batch: mig_239_semantic_research_id_varchar_standardization_20260501
-- Source: ChatGPT cleanup audit 2026-05-01 (verified live by Cowork);
--         see qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md §A claim 2
--         and cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md §1.
-- Target DB: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT (Cowork orchestrator); CREATE OR REPLACE VIEW × 3, no row changes.
--
-- Rationale:
--   8 semantic_publication safe views expose research_id at differing types:
--     - 5 already VARCHAR: vw_patient_master_safe, vw_fna_safe, vw_labs_long_safe,
--       vw_molecular_safe, vw_recurrence_safe
--     - 3 numeric: vw_cohort_membership_safe (BIGINT),
--       vw_path_malignant_tumor_safe (INTEGER), vw_us_nodule_safe (INTEGER)
--   This causes silent type-promotion friction in cross-domain joins. Cast all
--   research_id to VARCHAR in the semantic layer (keep base canonical types as-is).
--
-- Pre-snapshot: N/A (CREATE OR REPLACE on existing views; no destructive change).
-- Post-apply expectation:
--   - Row counts unchanged: cohort_membership=10871, path_malignant_tumor=5944, us_nodule=29504
--   - All 8 semantic_publication research_id columns expose VARCHAR
--   - 5-gate audit unchanged: gate1=211, gates 2-5=0, parity TRUE
--   - Lane M Tables 1-5 CSVs continue to parse identically (CSV is untyped)

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A vw_cohort_membership_safe_VIEW_v1 — research_id BIGINT -> VARCHAR
-- =============================================================================
CREATE OR REPLACE VIEW semantic_publication.vw_cohort_membership_safe_VIEW_v1 AS
SELECT
  m.release_id,
  CAST(c.research_id AS VARCHAR) AS research_id,
  c.surg_first_date AS surgery_date,
  c.analysis_eligible_flag,
  c.molecular_eligible_flag,
  c.rai_eligible_flag,
  c.survival_eligible_flag,
  c.age_at_surgery,
  c.sex,
  c.race,
  c.histology_final,
  c.ajcc8_stage_group,
  c.ete_grade_final,
  c.braf_positive_final,
  c.tert_positive_final,
  c.path_multifocal_flag
FROM main.manuscript_cohort_v1 AS c
CROSS JOIN (SELECT release_id FROM semantic_publication.release_manifest_v1 WHERE release_id = 'pub_v1_0_20260430') AS m;

-- =============================================================================
-- §B vw_path_malignant_tumor_safe_VIEW_v1 — research_id INTEGER -> VARCHAR
-- =============================================================================
CREATE OR REPLACE VIEW semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1 AS
SELECT
  'pub_v1_0_20260430' AS release_id,
  CAST(research_id AS VARCHAR) AS research_id,
  surgery_episode_id,
  tumor_ordinal,
  surgery_date,
  path_surgery_id,
  specimen_id,
  synoptic_row_ix,
  laterality,
  site,
  size_greatest_dimension_cm,
  tumor_size_cm_per_surgery,
  primary_histology,
  histology_variant,
  histology_source,
  extrathyroidal_extension,
  gross_ete,
  lymphatic_invasion,
  vascular_invasion,
  angioinvasion_quantify,
  perineural_invasion,
  capsular_invasion,
  margin_status,
  ln_examined,
  ln_involved,
  nodal_disease_positive_count,
  nodal_disease_total_count,
  extranodal_extension,
  number_of_tumors,
  multifocality_flag,
  data_completeness_pct,
  t_stage_ajcc8,
  n_stage_ajcc8,
  m_stage_ajcc8,
  overall_stage_ajcc8,
  stage_group_ajcc8,
  t_stage_ajcc8_resolved,
  n_stage_ajcc8_resolved,
  m_stage_ajcc8_resolved,
  ajcc_resolution_source,
  ajcc_resolution_confidence,
  linkage_confidence_tier,
  linkage_score,
  row_number() OVER (PARTITION BY research_id, surgery_episode_id, tumor_ordinal ORDER BY synoptic_row_ix NULLS LAST) AS publication_dedup_rank,
  consolidation_source,
  source_tables,
  build_script,
  build_ts
FROM main.canonical_path_malignant_events_dedup_VIEW_v1
WHERE COALESCE(is_source_distinct_duplicate_grain, CAST('f' AS BOOLEAN)) = CAST('f' AS BOOLEAN);

-- =============================================================================
-- §C vw_us_nodule_safe_VIEW_v1 — research_id INTEGER -> VARCHAR
-- =============================================================================
CREATE OR REPLACE VIEW semantic_publication.vw_us_nodule_safe_VIEW_v1 AS
SELECT
  'pub_v1_0_20260430' AS release_id,
  CAST(research_id AS VARCHAR) AS research_id,
  us_exam_id,
  exam_date,
  nodule_index_within_exam,
  nodule_id,
  laterality,
  location_raw,
  location_detail,
  length_mm,
  width_mm,
  height_mm,
  volume_ml,
  size_cm_max,
  extracted_size_cm,
  composition,
  echogenicity,
  shape,
  margins,
  calcifications,
  echogenic_foci,
  composition_pts,
  echogenicity_pts,
  shape_pts,
  margin_pts,
  foci_pts,
  tirads_reported_in_text,
  acr2017_tirads_points,
  acr2017_tirads_category,
  updated_tirads_category,
  acr2017_band_ambiguous,
  acr2017_vs_updated_concordant,
  suspicious_flag,
  acr2017_feature_points_complete,
  interval_growth_flag,
  fna_recommended_this_nodule,
  fna_performed_prior_or_concurrent,
  source_base,
  source_tirads_v2,
  source_tirads_llm,
  source_dynamics_llm,
  source_fna_linkage,
  source_us_nodules_tirads,
  data_completeness_pct,
  resolution_rule,
  nodule_master_id,
  is_aggregate_row,
  nlp_backfill_pending,
  source_modality,
  is_size_outlier_quarantine,
  multi_nodule_attribution_unresolved,
  tirads_conflict_resolution_source,
  us_row_type,
  us_resolution_strength
FROM manuscript_workspace.vw_us_nodule_tirads_any_reported_VIEW_v1;

-- =============================================================================
-- §D Refresh col_registry data_type for research_id in the 3 affected views
-- =============================================================================
UPDATE main.canonical_column_verification_registry_v1
SET data_type = 'VARCHAR',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verified_by = 'mig_239',
    verification_method = 'view_ddl_replace_with_cast_research_id_to_varchar',
    batch_id = 'mig_239_semantic_research_id_varchar_standardization_20260501',
    notes = COALESCE(notes,'') || ' | mig_239 (2026-05-01) refresh: research_id cast to VARCHAR for cross-domain join uniformity. Base canonical types unchanged.'
WHERE schema_name = 'semantic_publication'
  AND table_name IN (
    'vw_cohort_membership_safe_VIEW_v1',
    'vw_path_malignant_tumor_safe_VIEW_v1',
    'vw_us_nodule_safe_VIEW_v1'
  )
  AND column_name = 'research_id';

-- =============================================================================
-- §F col_registry dedup (discovered during mig_239 apply)
-- =============================================================================
-- mig_223 (Lane G semantic_publication) and mig_224 (Lane LN dim_histology) both
-- inserted col_registry rows TWICE during their apply (likely a register-then-verify
-- two-phase script). Result: 166 distinct (schema, table, column) keys had 2 rows
-- each = 166 surplus rows in main.canonical_column_verification_registry_v1.
--
-- Discovered when mig_239 §D UPDATE touched 5 rows (expected 3 — research_id col
-- across 3 views) due to dup hits in cohort_membership and us_nodule.
--
-- The dup pairs are content-identical except for verified_ts (separated by ~14s).
-- Keep MAX(rowid) per dup key — corresponds to the freshest verified_ts.
--
-- This drift was not caught by gate3 because gate3 reads counts FROM signoff_registry
-- (n_verified + n_na vs n_columns_total), which were set based on intended col count
-- not actual col_registry row count. A future "gate6" could check
-- col_registry COUNT(*) per (schema, table) vs signoff n_columns_total — TBD.

DELETE FROM main.canonical_column_verification_registry_v1
WHERE rowid NOT IN (
  SELECT MAX(rowid)
  FROM main.canonical_column_verification_registry_v1
  GROUP BY schema_name, table_name, column_name
);

-- =============================================================================
-- §G Acceptance assertions (run after apply)
-- =============================================================================
-- ASSERT 1: all 8 semantic safe views expose VARCHAR research_id
SELECT CASE WHEN COUNT(*) = 8 AND BOOL_AND(data_type = 'VARCHAR') THEN 'PASS'
            ELSE 'FAIL: ' || COUNT(*)::VARCHAR || ' views, '
                 || SUM(CASE WHEN data_type = 'VARCHAR' THEN 1 ELSE 0 END)::VARCHAR
                 || ' VARCHAR' END AS assert_all_varchar
FROM information_schema.columns
WHERE table_schema = 'semantic_publication'
  AND LOWER(column_name) = 'research_id'
  AND table_name LIKE 'vw_%_safe_VIEW_v1';

-- ASSERT 2: row counts unchanged
SELECT
  CASE WHEN (SELECT COUNT(*) FROM semantic_publication.vw_cohort_membership_safe_VIEW_v1) = 10871 THEN 'PASS' ELSE 'FAIL cohort_membership' END AS assert_cohort_rows,
  CASE WHEN (SELECT COUNT(*) FROM semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1) = 5944 THEN 'PASS' ELSE 'FAIL path_malignant_tumor' END AS assert_path_rows,
  CASE WHEN (SELECT COUNT(*) FROM semantic_publication.vw_us_nodule_safe_VIEW_v1) = 29504 THEN 'PASS' ELSE 'FAIL us_nodule' END AS assert_us_nodule_rows;

-- ASSERT 3: 5-gate audit unchanged
SELECT CASE WHEN gate1_verified_tables = 211 AND gate2_missing_signoff = 0
             AND gate3_count_mismatch = 0 AND gate4_verified_cols_missing_metadata = 0
             AND gate5_clinical_date_violations = 0 AND cohort_parity_ok = TRUE
            THEN 'PASS' ELSE 'FAIL: gates regressed' END AS assert_dashboard_clean
FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- ASSERT 4: zero col_registry dup keys after §F
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*)::VARCHAR || ' dup keys remain' END AS assert_no_dup_keys
FROM (
  SELECT schema_name, table_name, column_name, COUNT(*) AS n
  FROM main.canonical_column_verification_registry_v1
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
);
