-- =============================================================================
-- Migration 157 — canonical_patient_master CLINICAL-RESIDUAL CLUSTER sign-off (Lane 46)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   46 — clinical residual thematic slice (**60** cols).
-- batch_id: mig_157_patient_master_clinical_residual_cluster_20260429
--
-- Pre-flight (MotherDuck live 2026-04-29): `information_schema.columns` on CPM for the
-- §1a list returns **exactly 60** rows — sub-clusters 157a–157l per cowork prompt
-- `cursor_prompts/CURSOR_PROMPT_patient_master_clinical_residual_cluster_20260429.md`.
--
-- SSOT (all verified `main` unless noted):
--   * **157a sx_*:** `note_entities_llm_presenting_symptoms` — extraction faithfulness.
--   * **157b diagnosis_*, 157k histology/multifocal:** `canonical_path_malignant_patient_rollup_v1`.
--   * **157c demo_*:** no `canonical_patient_demographics_v1` — `scripts/frozen/207_canonical_master_expansion.py`
--     gold_master / coalesce lineage into CPM (**verification_method** names builder path).
--   * **157d first_recurrence_*:** `canonical_recurrence_v1` MIN(recurrence_date_resolved) replay.
--     **first_tg_*/last_tg_*/157j tgab_*:** `canonical_labs_thyroglobulin_v1`.
--     **last_contact_*:** multi-feed MAX resolution (labs/notes/imaging) — tiebreaker documented CF.
--   * **157e:** `canonical_complications_events_v1` MIN(timing_days).
--   * **157f gland_weight_*:** structured pathology / path malignant + thyroid_weights-style rollups.
--   * **157g lateral_*_v10:** versioned lateral-neck fields (cleaning rule from consolidation builder).
--   * **157h voice_*/wound_*/concern_*:** complications events + note-derived wound tier strings.
--   * **157i calcium_*/tsh_suppressed_*:** `canonical_labs_calcium_v1`, `canonical_labs_tsh_v1` +
--     post-op window logic; orthogonal vs **mig_150** `postop_low_calcium_flag` (Ca < 8 / 30d rule —
--     **2** patients FALSE flag but nadir < 8.0 mg/dL on 2026-04-29 spot-check; **0** TRUE flag with nadir ≥ 8).
--   * **157l preop_sweep_*/high_risk_molecular_*:** `canonical_molecular_genetics_v2`; **preop_imaging_size_cm**
--     cross-source (US / NLP / path).
--
-- Data-type audit (live):
--   * **DATE policy breach:** `first_recurrence_date`, `last_contact_date` stored **TIMESTAMP**
--     (calendar comparison should CAST AS DATE — **CF-mig157-CLINICAL-DATE-RETYPE**).
--   * **OK DATE:** `first_tg_date`, `last_tg_date`, `tsh_suppressed_first_date`.
--   * **Numeric OK:** gland weights, dominant nodule cm, calcium_nadir*, tgab_* numerics, preop_imaging DOUBLE.
--
-- BOOLEAN cohort uniformity (10,871 rows, 2026-04-29):
--   * **sx_nlp_any** TRUE **116** / FALSE **0** / NULL **10,755** — NLP spine sparse; not near-uniform FALSE on non-null.
--   * **sx_nlp_dysphagia** TRUE **16**; **dyspnea** **6**; **hoarseness** **15**; **neck_mass** TRUE **73** FALSE **43** NULL **10,755**.
--   * **calcium_supplement_required** TRUE **30**.
--   * **tsh_suppressed_ever** TRUE **43** FALSE **158** NULL **10,670**; **threshold_0_5** TRUE **22** FALSE **34** NULL **10,815**.
--   * **tgab_interference_flag** TRUE **494** — within expected non-zero band.
--   * **aggressive_variant_flag** TRUE **43** (~0.4% — below 5–15% rule-of-thumb; ladder strict — **CF-mig157-AGGRESSIVE-VARIANT-LADDER**).
--   * **high_risk_molecular_v7** TRUE **0** FALSE **10,024** NULL **847** — **CF-mig157-HIGH-RISK-MOL-ZERO-TRUE** (degenerate TRUE cohort).
--   * **multifocal_flag_path** TRUE **1440** — mixed.
--
-- **dominant_nodule_size_cm vs _v2:** **9,640** exact match (incl. both NULL); **1,065** both non-null differ;
-- **166** v2-only non-null — **CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT**.
--
-- VARCHAR degeneracy (informational): **gland_weight_source** **1** distinct; **sx_llm_extraction_method** **1**;
-- **tsh_suppressed_ever_source** all **NULL** (**CF-mig157-TSH-SUPPRESSED-SOURCE-ALL-NULL**).
--
-- Apply on MotherDuck RW (`thyroid_canonical_publication_v1_0`). Cowork agent prepares SQL only;
-- **Logan executes** per Protocol v2 §5.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Pre-snapshot (run before TRANSACTION).
-- Re-run: DROP TABLE IF EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig157_20260429;
-- -----------------------------------------------------------------------------
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig157_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig157_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'sx_llm_extraction_method', 'sx_llm_mean_confidence', 'sx_llm_n_source_notes',
    'sx_nlp_any_symptom_data', 'sx_nlp_dysphagia', 'sx_nlp_dyspnea', 'sx_nlp_hoarseness', 'sx_nlp_neck_mass',
    'diagnosis_confidence', 'diagnosis_full', 'diagnosis_primary', 'diagnosis_variant',
    'demo_confidence', 'demo_source',
    'first_recurrence_date', 'first_recurrence_days_from_surg', 'first_tg_date', 'first_tg_days_from_surg',
    'last_contact_date', 'last_contact_days_from_surg', 'last_contact_source', 'last_tg_date', 'last_tg_days_from_surg',
    'earliest_complication_days',
    'gland_weight_combined_g', 'gland_weight_final_g', 'gland_weight_isthmus_g',
    'gland_weight_left_lobe_g', 'gland_weight_right_lobe_g', 'gland_weight_source', 'gland_weight_total_reported_g',
    'lateral_detection_method', 'lateral_levels_v10', 'lateral_side_v10', 'lateral_source_v10',
    'voice_data_confidence', 'voice_outcome_category', 'wound_infection_status', 'concern_highest_tier',
    'calcium_nadir', 'calcium_nadir_30d', 'calcium_nadir_days_postop', 'calcium_supplement_required',
    'tsh_suppressed_ever', 'tsh_suppressed_ever_source', 'tsh_suppressed_ever_threshold_0_5', 'tsh_suppressed_first_date',
    'tgab_interference_flag', 'tgab_last_value', 'tgab_nadir', 'tgab_peak',
    'aggressive_variant_flag', 'dominant_nodule_size_cm', 'dominant_nodule_size_cm_v2',
    'histologic_types_all', 'histologic_variants_all',
    'preop_imaging_size_cm', 'preop_sweep_genes_found_v11', 'high_risk_molecular_v7', 'multifocal_flag_path'
  );

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 157a — 8 cols — symptom NLP (LLM aggregate + rule-based sx_nlp flags)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_llm_presenting_symptoms',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157a). sx_llm_* + sx_nlp_* vs '
                          || 'note_entities_llm_presenting_symptoms; sparse NULL cohort on sx_nlp (CF).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'sx_llm_extraction_method',
        'sx_llm_mean_confidence',
        'sx_llm_n_source_notes',
        'sx_nlp_any_symptom_data',
        'sx_nlp_dysphagia',
        'sx_nlp_dyspnea',
        'sx_nlp_hoarseness',
        'sx_nlp_neck_mass'
      );

-- -----------------------------------------------------------------------------
-- 157b — 4 cols — diagnosis rollup
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_path_malignant_patient_rollup_v1',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157b). diagnosis_* vs path_malignant patient rollup primary_dx / full string.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'diagnosis_confidence',
        'diagnosis_full',
        'diagnosis_primary',
        'diagnosis_variant'
      );

-- -----------------------------------------------------------------------------
-- 157c — 2 cols — demographics provenance (207 gold_master coalesce — no standalone demographics v1 table)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_gold_master_demographics_coalesce_207',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157c). demo_confidence/demo_source from 207 expansion + CPM builder SSOT.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'demo_confidence',
        'demo_source'
      );

-- -----------------------------------------------------------------------------
-- 157d — 9 cols — first/last clinical dates + offsets + last_contact_source
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_recurrence_v1_min_date_and_lab_tg_extrema_and_multi_feed_contact',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157d). first_recurrence_* vs canonical_recurrence_v1; '
                          || 'first_tg_*/last_tg_* vs canonical_labs_thyroglobulin_v1 MIN/MAX; '
                          || 'last_contact_* multi-feed MAX (CF LAST-CONTACT tiebreaker).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'first_recurrence_date',
        'first_recurrence_days_from_surg',
        'first_tg_date',
        'first_tg_days_from_surg',
        'last_contact_date',
        'last_contact_days_from_surg',
        'last_contact_source',
        'last_tg_date',
        'last_tg_days_from_surg'
      );

-- -----------------------------------------------------------------------------
-- 157e — 1 col — earliest complication timing
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_v1',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157e). earliest_complication_days = MIN(timing_days) complications events.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'earliest_complication_days'
      );

-- -----------------------------------------------------------------------------
-- 157f — 7 cols — gland morphometry
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_source_pathology_gland_weight_resolution',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157f). gland_weight_* DOUBLE vs structured pathology feeds + rollup tiebreaker.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'gland_weight_combined_g',
        'gland_weight_final_g',
        'gland_weight_isthmus_g',
        'gland_weight_left_lobe_g',
        'gland_weight_right_lobe_g',
        'gland_weight_source',
        'gland_weight_total_reported_g'
      );

-- -----------------------------------------------------------------------------
-- 157g — 4 cols — lateral neck dissection v10
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_consolidation_lateral_neck_v10_cleaning',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157g). lateral_*_v10 version-pinned lateral neck dissection fields.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'lateral_detection_method',
        'lateral_levels_v10',
        'lateral_side_v10',
        'lateral_source_v10'
      );

-- -----------------------------------------------------------------------------
-- 157h — 4 cols — voice / wound / concern tier
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_v1',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157h). voice_*/concern_* complications phenotype; wound_infection_status notes.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'voice_data_confidence',
        'voice_outcome_category',
        'wound_infection_status',
        'concern_highest_tier'
      );

-- -----------------------------------------------------------------------------
-- 157i — 8 cols — post-op calcium nadir + TSH suppressed flags
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_calcium_v1_and_labs_tsh_v1_postop_window',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157i). calcium_* + tsh_suppressed_* vs canonical_labs_calcium_v1 / '
                          || 'canonical_labs_tsh_v1; cross mig_150 postop_low_calcium_flag (CF).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'calcium_nadir',
        'calcium_nadir_30d',
        'calcium_nadir_days_postop',
        'calcium_supplement_required',
        'tsh_suppressed_ever',
        'tsh_suppressed_ever_source',
        'tsh_suppressed_ever_threshold_0_5',
        'tsh_suppressed_first_date'
      );

-- -----------------------------------------------------------------------------
-- 157j — 4 cols — TgAb residual
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_thyroglobulin_v1',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157j). tgab_* analyte slice on thyroglobulin canonical lab table.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'tgab_interference_flag',
        'tgab_last_value',
        'tgab_nadir',
        'tgab_peak'
      );

-- -----------------------------------------------------------------------------
-- 157k — 5 cols — histology aggregates + aggressive variant + dominant nodule sizes
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_path_malignant_patient_rollup_v1',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157k). histologic_* STRING_AGG + aggressive_variant ladder; '
                          || 'dominant_nodule v1 vs v2 drift CF.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'aggressive_variant_flag',
        'dominant_nodule_size_cm',
        'dominant_nodule_size_cm_v2',
        'histologic_types_all',
        'histologic_variants_all'
      );

-- -----------------------------------------------------------------------------
-- 157l — 4 cols — preop imaging + molecular residual + multifocal path flag
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_molecular_genetics_v2_and_cross_source_preop_imaging',
    batch_id            = 'mig_157_patient_master_clinical_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_157 Lane 46 (157l). preop_sweep_genes + high_risk_molecular_v7 vs molecular_genetics_v2; '
                          || 'preop_imaging_size_cm cross-source; multifocal_flag_path path rollup.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'preop_imaging_size_cm',
        'preop_sweep_genes_found_v11',
        'high_risk_molecular_v7',
        'multifocal_flag_path'
      );

-- -----------------------------------------------------------------------------
-- 157m — Resync canonical_table_signoff_registry_v1 for CPM (+60 verified this batch)
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
    signoff_migration = 'qc_framework_v1/migrations/157_patient_master_clinical_residual_cluster_signoff_20260429.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_157: CPM clinical-residual cluster CLOSED (60 cols verified).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- -----------------------------------------------------------------------------
-- Carry-forwards (CF) — append to verified mig_157 notes
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-CLINICAL-DATE-RETYPE: column TIMESTAMP in build; calendar semantics use CAST(... AS DATE).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name IN ('first_recurrence_date', 'last_contact_date');

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-HIGH-RISK-MOL-ZERO-TRUE: TRUE cohort absent on non-null rows; builder ATA-high-risk gate.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name = 'high_risk_molecular_v7';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-SX-NLP-SPARSE-NULL: majority NULL sx_nlp_*; TRUE counts small — expected NLP coverage gap.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name IN (
        'sx_nlp_any_symptom_data',
        'sx_nlp_dysphagia',
        'sx_nlp_dyspnea',
        'sx_nlp_hoarseness',
        'sx_nlp_neck_mass'
      );

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-AGGRESSIVE-VARIANT-LADDER: low TRUE rate (43/10871) — strict tall/columnar/hobnail ladder.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name = 'aggressive_variant_flag';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT: 1065 both-non-null differ; 166 v2-only — cross-feed reconcile.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name IN ('dominant_nodule_size_cm', 'dominant_nodule_size_cm_v2');

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-CALCIUM-VS-MIG150-CROSS: 2pt FALSE postop_low_calcium_flag but calcium_nadir < 8 — tier defs.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name IN (
        'calcium_nadir',
        'calcium_nadir_30d',
        'calcium_supplement_required'
      );

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-TSH-SUPPRESSED-SOURCE-ALL-NULL: tsh_suppressed_ever_source unpopulated — provenance gap.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name = 'tsh_suppressed_ever_source';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-TSH-SUPPRESSED-VS-MIG150-CROSS: no mig_150 postop-low-TSH twin column — verify vs canonical_labs_tsh_v1 windows.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name IN (
        'tsh_suppressed_ever',
        'tsh_suppressed_ever_threshold_0_5',
        'tsh_suppressed_first_date'
      );

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-VALUE-DEGENERATE-UPSTREAM-gland_weight_source: single distinct string in cohort.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name = 'gland_weight_source';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-VALUE-DEGENERATE-UPSTREAM-sx_llm_extraction_method: single distinct string.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name = 'sx_llm_extraction_method';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig157-LAST-CONTACT-MULTI-FEED-RESOLUTION: MAX over feeds; last_contact_source 41-tier provenance mix.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
  AND column_name IN ('last_contact_date', 'last_contact_days_from_surg', 'last_contact_source');

COMMIT;

-- =============================================================================
-- end migration 157 — CPM clinical-residual cluster (60 cols)
-- =============================================================================
