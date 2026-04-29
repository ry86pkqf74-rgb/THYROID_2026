-- =============================================================================
-- Migration 149 -- canonical_patient_master SYNOPTIC-PATHOLOGY CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   39 — synoptic pathology thematic slice (~32 cols; mig_149).
-- batch_id: mig_149_patient_master_synoptic_pathology_cluster_20260429
--
-- Probe (information_schema; MotherDuck RW thyroid_canonical_publication_v1_0):
--   WHERE table_name='canonical_patient_master' AND column_name LIKE 'syn\_%' ESCAPE '\'
--     AND column_name NOT IN (
--       SELECT column_name FROM main.canonical_column_verification_registry_v1
--       WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
--     )
--   Cardinality: **32** columns (exactly); executed 2026-04-29 pre-apply.
--
-- Cohort parity: canonical_patient_master = 10,871 rows / distinct research_id (connect_locked).
--
-- VARCHAR vs DOUBLE measurement audit:
--   * syn_*_size_cm (left/right/isthmus): **VARCHAR** — free-text gland dimensions from
--     path_synoptics.{ll_size_cm, rl_size_cm, isthmus_size_cm}; not bare DOUBLE literals.
--   * syn_*_weight_g + syn_total_weight_g: **DOUBLE** (Script 214 clean_gland_weight semantics).
--   * syn_margin_distance_mm_num: **DOUBLE** (Script 248 TRY_CAST from raw VARCHAR).
--   * syn_margin_distance_mm_raw_str: **VARCHAR** — retains literal **'nan'** on ~9,517 cells
--     per Script 248 **PRESERVE_RAW** (CF-mig149-SYN-MARGIN-RAW-NAN; not mig_144 retype lane).
--
-- Derivation re-derivation (MotherDuck SQL/Python probes, 2026-04-29):
--   * **BOOL_OR** benign/inflammatory / procedural markers vs path_synoptics with
--     LOWER(RTRIM(TRIM(...), ';')) normalization for semicolon-terminated Excel cells:
--     0 drift vs CPM for representative flags (e.g. syn_hashimoto).
--   * **syn_*_size_cm**: patient-level value matches path_synoptics row selected by
--     ORDER BY (CASE WHEN field IS NOT NULL THEN 0 ELSE 1 END), surg_date DESC NULLS LAST
--     (prefer latest **non-null** morphometry over latest surgery with empty slot) — 0 drift.
--   * **syn_capsular_invasion_clean / syn_lymphatic_invasion_clean / syn_necrosis_clean**:
--     Script 214 cleaning CASE ladder + semicolon strip + same non-null/surg_date ordering — 0 drift.
--   * **syn_architecture**: same ordering; tie-only residual vs pure surg_date-first is below
--     2% without path synoptic_row_ix on publication path_synoptics (omitted column).
--   * **syn_carcinoma_on_frozen**: **Script 360** frozen-stage block (NOT raw Script 214
--     clean_x_marker_to_bool on 'x'): BOOL_OR(LOWER(TRIM(carci...)) IN
--     ('yes','y','true','1','positive','pos')) then **COALESCE(..., FALSE)** — 0 drift.
--   * **syn_margin_distance_mm_num**: TRY_CAST(NULLIF(TRIM(CAST(raw AS VARCHAR)),'nan') AS DOUBLE)
--     from tumor_1_distance_to_closest_margin_mm on the same **recent-row** pick as 214 —
--     0 drift vs CPM.
--   * **syn_histologic_grade**: MAX(TRY_CAST(tumor_1_histologic_grade AS DOUBLE)) rolled to BIGINT — 0 drift.
--
-- Findings-vs-staging (Logan-ratified): capsular / lymphatic / necrosis columns are **synoptic
-- findings** — not reverse-inferred from AJCC staging columns (feedback_findings_vs_staging.md).
--
-- BOOLEAN cohort uniformity (non-degenerate; MotherDuck live):
--   * syn_hashimoto: n_TRUE=248 (~2.3%) — below textbook co-occurrence; reflects CAP synoptic
--     x-marker capture, not cohort FALSE-degeneracy (CF-mig149-SYN-HASHIMOTO-LOW-TEMPLATE-YIELD).
--   * syn_chronic_thyroiditis: n_TRUE=1096 — acceptable signal.
--   * syn_multinodular_goiter: n_TRUE=6075 (~56%) — elevated vs 30–50% heuristic but not near-uniform.
--   * syn_capsular_invasion_clean: ~1,046 rows with non-absent cleaned values (~9.6%).
--   * syn_io_rln_monitoring: n_TRUE=120 — independent of operative_cluster op_rln_* columns
--     (synoptic io_rln_monitoring grain; CF-mig149-SYN-IO-RLN-VS-OPERATIVE-DISTINCT).
--   * No BOOLEAN flipped here has n_TRUE=0 with expected pathology signal; none are >99% TRUE.
--
-- Gate 4 (verified rows require verified_by + verification_method + batch_id + verified_ts):
--   0 violations pre-apply on existing **verified** CPM registry rows (queried 2026-04-29).
--
-- Active parallel lanes (do not touch in this file): mig_142 RAI PM, mig_145 CT, mig_146 MRI+PET,
-- mig_147 nucmed, mig_148 RAI upstream, sibling lanes parathyroid / postop+TP / meds+radtx.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 149a — 9 cols — synoptic_gland_morphometry_aggregate (sizes VARCHAR + weights DOUBLE + parathyroid)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_synoptic_per_specimen',
    batch_id            = 'mig_149_patient_master_synoptic_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_149 synoptic-pathology cluster (Lane 39). '
                          || 'Morphometry from path_synoptics ll/rl/isthmus size+weight + parathyroid '
                          || 'counts/specimen flag; non-null-first then latest surg_date ordering.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'syn_isthmus_size_cm',
    'syn_isthmus_weight_g',
    'syn_left_lobe_size_cm',
    'syn_left_lobe_weight_g',
    'syn_right_lobe_size_cm',
    'syn_right_lobe_weight_g',
    'syn_total_weight_g',
    'syn_n_parathyroid_identified',
    'syn_parathyroid_in_specimen'
  );


-- -----------------------------------------------------------------------------
-- 149b — 8 cols — synoptic_benign_inflammatory_bool_or_vs_path_synoptics
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_path_synoptics_source',
    batch_id            = 'mig_149_patient_master_synoptic_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_149 synoptic-pathology cluster (Lane 39). '
                          || 'BOOL_OR on cleaned x-marker fields (+ MNG substernal union where applicable).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'syn_adenomatoid_nodules',
    'syn_chronic_thyroiditis',
    'syn_colloid_nodule',
    'syn_follicular_adenoma',
    'syn_graves',
    'syn_hashimoto',
    'syn_hyperplastic_nodules',
    'syn_multinodular_goiter'
  );


-- -----------------------------------------------------------------------------
-- 149c — 5 cols — synoptic_invasion_margin_grade_cleaned_findings
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_synoptic_cleaned_field',
    batch_id            = 'mig_149_patient_master_synoptic_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_149 synoptic-pathology cluster (Lane 39). '
                          || 'Script-214-style cleaners on tumor_1_* synoptic fields (semicolon strip). '
                          || 'Findings-primary vs staging per feedback_findings_vs_staging.md.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'syn_architecture',
    'syn_capsular_invasion_clean',
    'syn_lymphatic_invasion_clean',
    'syn_necrosis_clean',
    'syn_carcinoma_on_frozen'
  );


-- -----------------------------------------------------------------------------
-- 149d — 2 cols — synoptic_margin_distance_num_and_raw_str (248 split lineage)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_passthrough_raw_str',
    batch_id            = 'mig_149_patient_master_synoptic_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_149 synoptic-pathology cluster (Lane 39). '
                          || 'margin: syn_margin_distance_mm_num from TRY_CAST(NULLIF raw,''nan''); '
                          || 'raw_str preserves literal nan per Script 248 PRESERVE_RAW '
                          || '(CF-mig149-SYN-MARGIN-RAW-NAN).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'syn_margin_distance_mm_num',
    'syn_margin_distance_mm_raw_str'
  );


-- -----------------------------------------------------------------------------
-- 149e — 6 cols — mitotic_ki67_grade_hurthle_ccell_synoptic
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_path_synoptics_source',
    batch_id            = 'mig_149_patient_master_synoptic_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_149 synoptic-pathology cluster (Lane 39). '
                          || 'Mitotic/numeric grade + Ki-67 VARCHAR + Hurthle/C-cell boolean composites '
                          || 'from tumor_1 + benign synoptic slots (Script 214 lineage).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'syn_mitotic_rate_numeric',
    'syn_mitotic_rate_qualifier',
    'syn_ki67_index',
    'syn_histologic_grade',
    'syn_hurthle_cell_change',
    'syn_c_cell_hyperplasia'
  );


-- -----------------------------------------------------------------------------
-- 149f — 2 cols — synoptic_procedural_metadata_dissection_rln
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_path_synoptics_source',
    batch_id            = 'mig_149_patient_master_synoptic_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_149 synoptic-pathology cluster (Lane 39). '
                          || 'central_compartment_dissection + io_rln_monitoring from path_synoptics; '
                          || 'distinct grain from operative_cluster IO/RLN columns '
                          || '(CF-mig149-SYN-IO-RLN-VS-OPERATIVE-DISTINCT).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'syn_central_dissection',
    'syn_io_rln_monitoring'
  );


-- -----------------------------------------------------------------------------
-- 149g — refresh canonical_table_signoff_registry_v1 for CPM (n_verified +32)
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
    signoff_migration = 'qc_framework_v1/migrations/149_patient_master_synoptic_pathology_cluster_signoff_20260429.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_149: synoptic-pathology thematic cluster CLOSED (32 cols verified).'
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


COMMIT;


-- =============================================================================
-- end migration 149 — CPM synoptic-pathology cluster (32 cols not_started → verified)
-- =============================================================================
