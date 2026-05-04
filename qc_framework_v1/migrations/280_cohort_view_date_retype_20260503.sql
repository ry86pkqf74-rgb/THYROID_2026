-- mig_280 — manuscript_workspace cohort_m037 + cohort_m025 repair (2026-05-03)
-- Context: mig_160b retyped CPM DATE columns → catalog drift (m037 BinderException TIMESTAMP vs DATE on resolve).
-- m025 additionally referenced canonical_us_patient_master_VIEW_v2.tirads_worst_rank_source after column retirement.
--
-- Target DB: thyroid_canonical_publication_v1_0 (USE before running unqualified DDL).
--
-- Preconditions: connect_locked publication session; canonical_patient_master = 10871 rows.
--
-- Closes: CF-mig277-COHORT-VIEW-BINDER, CF-mig160b-COHORT-VIEW-CASCADE.

-- -----------------------------------------------------------------------------
-- §1 Archive pre-state (duckdb_views; single row per view in canonical catalog)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_cohort_m037_ln_metastasis_v1_pre_mig280_20260503 AS
SELECT
  database_name AS view_catalog,
  schema_name   AS view_schema,
  view_name,
  sql           AS view_definition,
  CURRENT_TIMESTAMP AS snapshot_at
FROM duckdb_views()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = 'manuscript_workspace'
  AND view_name = 'cohort_m037_ln_metastasis_v1';

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_cohort_m025_tirads_performance_v1_pre_mig280_20260503 AS
SELECT
  database_name AS view_catalog,
  schema_name   AS view_schema,
  view_name,
  sql           AS view_definition,
  CURRENT_TIMESTAMP AS snapshot_at
FROM duckdb_views()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = 'manuscript_workspace'
  AND view_name = 'cohort_m025_tirads_performance_v1';

-- -----------------------------------------------------------------------------
-- §2 REPLACE views (canonical catalog)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m037_ln_metastasis_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.histology_final,
  p.is_malignant,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.multifocal_flag_path,
  p.ete_grade_final,
  p.gross_ete_flag,
  p.ln_positive_flag,
  p.ln_positive_final,
  p.ln_total_examined,
  p.ln_total_positive,
  p.ln_ratio,
  p.ln_ene_status,
  p.ln_burden_band,
  p.ln_lateral_dissected,
  p.lateral_neck_dissected,
  p.ln_rollup_central_examined,
  p.ln_rollup_central_positive,
  p.ln_rollup_lateral_right_examined,
  p.ln_rollup_lateral_right_positive,
  p.ln_rollup_lateral_left_examined,
  p.ln_rollup_lateral_left_positive,
  p.ln_rollup_total_levels_involved,
  p.ln_level_i_positive,
  p.ln_level_ii_positive,
  p.ln_level_iii_positive,
  p.ln_level_iv_positive,
  p.ln_level_v_positive,
  p.ln_level_vi_positive,
  p.ajcc8_n_stage,
  p.ajcc8_stage_group,
  p.braf_positive_final,
  p.tert_positive_final,
  p.any_recurrence_flag,
  p.structural_recurrence_flag,
  p.followup_years,
  p.surg_procedure_type,
  p.surg_first_date
FROM main.canonical_patient_master AS p
WHERE ((p.is_malignant = CAST('t' AS BOOLEAN)) AND ((p.ln_total_examined > 0) OR (p.ln_positive_flag = CAST('t' AS BOOLEAN))));

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m025_tirads_performance_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.race,
  cupm.tirads_category_at_last_preop_exam AS preop_tirads_category,
  cupm.tirads_category_at_first_exam AS tirads_best_category_v12,
  cupm.max_tirads_category_ever AS tirads_worst_category_v12,
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

-- -----------------------------------------------------------------------------
-- §3 Verify (counts only; expects ~2.2k M037 and ~3.3–3.5k M025)
-- -----------------------------------------------------------------------------

SELECT 'cohort_m037_ln_metastasis_v1' AS v,
       COUNT(*) AS n FROM manuscript_workspace.cohort_m037_ln_metastasis_v1
UNION ALL
SELECT 'cohort_m025_tirads_performance_v1' AS v,
       COUNT(*) AS n FROM manuscript_workspace.cohort_m025_tirads_performance_v1;

-- Post-apply observation (MotherDuck, 2026-05-03): M037 n=2233, M025 n=3375.
-- Apply path: `.venv/bin/python scripts/mig_280_cohort_view_date_retype.py` (writes signoff_migration).
