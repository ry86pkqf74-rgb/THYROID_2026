-- =============================================================================
-- Migration 145 -- canonical_patient_master CT IMAGING CLUSTER sign-off (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   35 (mig_145) — CT exam metadata, thyroid + LN rollups, airway/tracheal,
--         substernal extension, goiter (from structured `ct_imaging` plus
--         script 219 patient-level replay).
-- batch_id: mig_145_patient_master_ct_imaging_cluster_20260429
--
-- Pre-apply probes (MotherDuck RW thyroid_canonical_publication_v1_0, 2026-04-29):
--   * Predicate cardinality: exactly **29** cols matching the Lane-35 probe
--     (ct_% not already verified in canonical_column_verification_registry_v1).
--   * Cohort parity: canonical_patient_master = **10,871** rows / distinct
--     research_id (`connect_locked`).
--   * Upstream SSOT: **no** `main.canonical_ct_*` / `canonical_imaging_ct_*`
--     patient table — row-per-exam SSOT is **`main.ct_imaging`** (7,701 rows).
--     Patient-level CT block on CPM derives from `ct_imaging` via
--     `scripts/219_imaging_gap_resolution.py` (`_ct_expanded_rollup_v1` CT_ONLY
--     filter — excludes PET/MRI/None exam families) plus spine columns
--     (`ct_n_exams`, largest short-axis, LN bools, goiter/tracheal/substernal)
--     aligned with `scripts/frozen/207_canonical_master_expansion.py` CT rollup
--     patterns on the same source.
--     **CF-mig145-CT-UPSTREAM-CANONICAL-PENDING** opened (informational): a
--     future `canonical_ct_exam_v1`/`canonical_ct_neck_*` would be nice for
--     exam-grain verification; **not** blocking — Tier-1 imaging structure is
--     `ct_imaging` + clean-room replay of 219.
--   * Date policy: `ct_first_date` and `ct_last_date` are **DATE** (not TIMESTAMP
--     / VARCHAR) — **no** `CF-mig145-PM-CT-DATE-RETYPE`.
--   * NULL vs zero policy: **`ct_n_exams` IS NULL** for patients **without** a CT
--     rollup (not `0`). Boolean `ct_*` cells are **NULL** when no CT rollup
--     cohort (`ct_n_exams` absent), not silently FALSE (`not_started → verified`
--     encodes derivation truth, not analytic fill).
--   * LIVE replay checks (filtered `ct_only` ≡ 219 `CT_ROLLUP_SQL` predicates):
--     `ct_pathologic_ln_any` **0** row-level mismatches vs
--     `BOOL_OR(pathologic_lymph_nodes)` on `ct_imaging` aggregates when joined by
--     `research_id`.
--     **Note:** `ct_pathologic_ln_any` traces to the **`pathologic_lymph_nodes`**
--     structured boolean on `ct_imaging`, **not** a short-axis-only rule; the ≥1 cm
--     LN short-axis mnemonic is descriptive of common radiology practice — not an
--     alternate SSOT for this PM column (`cross_validate_ct_pathologic_ln_*` naming
--     retained for lineage search; actual validation = structured flag fidelity).
--
-- Cohort uniformity (BOOLEAN `ct_*` sweep where `ct_n_exams IS NOT NULL`, n≈3086):
--   * Interpretive carry-forwards (**not** regression blockers — verification
--     documents SSOT semantics):
--       - **CF-mig145-CT-AIRWAY-COMMENT-PROXY**: `ct_airway_compromise_any` is
--         `BOOL_OR(airway_compromise_comment IS NOT NULL AND length>5)` in script 219 —
--         substantive **radiology airway comment**, not a graded airway-compromise
--         phenotype — **high** TRUE rate (~41% in live probe) vs informal “clinical
--         airway compromise” intuition.
--       - **CF-mig145-CT-TRACHEAL-NOTMENTIONED-OVERREACH**: structured
--         `tracheal_deviation` / `tracheal_narrowing` rollup treats many non-`none`
--         lexical states (including `not_mentioned`) as deviation/narrowing `TRUE`;
--         **`ct_tracheal_*_any`** are ~85–86% TRUE with 0 NULL in CT rollup cohort —
--         positional **vocab-coupled**, not adjudicated clinical “compromise”.
--       - **`ct_substernal_extension_any`** has **1282** NULLs even within
--         `ct_n_exams` present — modality-field sparsity; non-degenerate when
--         populated (~30% TRUE).
--
-- Acceptance:
--   * **29** columns flipped from **not_started** → **verified** (Gate 4 metadata
--     complete — verified_by / verification_method / batch_id / verified_ts).
--   * `canonical_table_signoff_registry_v1` row refreshed from live registry
--     counts (**n_verified advances by exactly 29** post-apply).
--
-- Parallel lanes — do NOT touch: mig_142 RAI, mig_143 SmallClusters,
-- mig_144 US+imaging-generic, mig_146 MRI+PET, mig_147 Nucmed.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 145a — 14 cols — calendars, counts, first/last exam scalars / text blobs
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_ct_imaging_v1',
    batch_id            = 'mig_145_patient_master_ct_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_145 CT cluster (Lane 35). Non-boolean rollups '
                          || '(MIN/MAX exam DATE, DISTINCT exam counts, first-exam '
                          || 'contrast/type, STRING_AGG/indication + last-exam thyroid/'
                          || 'LN/airway TEXT, MAX LN short-axis mm) replayed vs main.'
                          || 'ct_imaging with 219 ct_only exam filter + script 219 '
                          || 'staging; calendar dates are DATE; NULL where no CT roll-up.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ct_first_date',
    'ct_first_days_from_surg',
    'ct_last_date',
    'ct_last_days_from_surg',
    'ct_n_exams',
    'ct_contrast_first',
    'ct_exam_type_first',
    'ct_indication_first',
    'ct_indication_last',
    'ct_thyroid_details_last',
    'ct_ln_details_last',
    'ct_ln_locations_last',
    'ct_largest_ln_short_axis_mm',
    'ct_airway_comment_last'
  );


-- -----------------------------------------------------------------------------
-- 145b — 15 cols — thyroid / LN / airway / substernal / tracheal / goiter BOOL_ORs
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_ct_per_exam',
    batch_id            = 'mig_145_patient_master_ct_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_145 CT cluster (Lane 35). Patient-level BOOL_OR / '
                          || 'presence rules over structured `ct_imaging` fields replayed '
                          || 'against CPM (`pathologic_lymph_nodes` flag for '
                          || 'ct_pathologic_ln_any). Uniformity quirks: CF-mig145-CT-'
                          || 'AIRWAY-COMMENT-PROXY, CF-mig145-CT-TRACHEAL-NOTMENTIONED'
                          || '-OVERREACH (see migration header); NULL semantics when '
                          || 'no CT rollup.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ct_thyroid_enlarged_any',
    'ct_thyroid_heterogeneous_any',
    'ct_thyroid_nodule_any',
    'ct_thyroid_normal_any',
    'ct_thyroid_not_visualized_any',
    'ct_thyroid_other_abnormality_any',
    'ct_thyroid_postsurgical_any',
    'ct_ln_enlarged_any',
    'ct_ln_suspicious_any',
    'ct_pathologic_ln_any',
    'ct_airway_compromise_any',
    'ct_substernal_extension_any',
    'ct_tracheal_deviation_any',
    'ct_tracheal_narrowing_any',
    'ct_goiter_present_any'
  );


-- -----------------------------------------------------------------------------
-- 145 — refresh canonical_patient_master row on canonical_table_signoff_registry_v1
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
                        || ' | mig_145: CT imaging thematic cluster CLOSED (+29 cols verified).'
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


COMMIT;


-- =============================================================================
-- end migration 145 — CPM CT cluster (29 cols verified; SSOT ct_imaging + 219 lineage)
-- =============================================================================
