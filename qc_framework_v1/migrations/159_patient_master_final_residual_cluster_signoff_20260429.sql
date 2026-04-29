-- =============================================================================
-- Migration 159 — canonical_patient_master FINAL RESIDUAL CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   47 — Final residual thematic slice (**27** cols).
-- Prompt: cursor_prompts/CURSOR_PROMPT_patient_master_final_residual_cluster_20260429.md
-- batch_id: mig_159_patient_master_final_residual_cluster_20260429
--
-- Pre-apply probes (MotherDuck RW `thyroid_canonical_publication_v1_0`, Cowork 2026-04-29):
--   * **§1a allow-list:** exactly **27** physical columns on `main.canonical_patient_master`; dtypes
--     spot-checked — `max_stimulated_tg_date` is **DATE** (not TIMESTAMP/VARCHAR) —
--     **CF-mig159-MAX-STIM-TG-DATE-RETYPE-CLEAR**.
--   * **Registry:** all **27** rows **`verification_status='not_started'`** pre-apply.
--   * **Cohort parity:** `canonical_patient_master` = **10,871** rows / distinct `research_id**.
--   * **SSOT existence (`information_schema.tables`):** `canonical_molecular_genetics_v2`,
--     `canonical_path_malignant_patient_rollup_v1`, `canonical_labs_thyroglobulin_v1`,
--     `canonical_complications_events_v1`, `canonical_operative_events_v1` — **all present**.
--
-- **Molecular single-gene BOOLEAN cohort-uniformity (TRUE counts, non-zero where expected):**
--   `alk_positive_v7` **11**; `completion_braf_positive` **14**; `completion_tert_positive` **3**;
--   `eif1ax_positive` **24**; `hras_positive_v11` **114**; `kras_positive_v11` **59**; `nras_positive_v11`
--   **196**; `ntrk_positive_v7` **16**; `pax8_pparg_positive` **36**; `tp53_positive_v7` **20**.
--   **→** no **CF-mig159-COHORT-UNIFORM-FALSE-* / placeholder-zero** on this slice.
--
-- **Molecular versioning (v7 vs v11 cols):** version suffixes denote distinct builder passes
-- (`project_round2_llm_integration_script_386_closeout.md` lineage). Sign-off = aggregate replay vs
-- `canonical_molecular_genetics_v2` gene/fusion predicates — informational
-- **CF-mig159-MOLECULAR-V7-V11-DRIFT** (tracked if future cross-col strict equality tests differ).
--
-- **Completion thyroidectomy (2 cols):** `completion_reason` categorical + `completion_reason_confidence`
-- DOUBLE — **685** pts with confidence non-null; derivation ties `canonical_operative_events_v1`
-- + completion-Thyr LLM lineage (completion_reason enums per extraction contract).
--
-- **Bilateral flags:** `bilateral_disease_flag` **2,142** TRUE vs `bilateral_path_flag` **912** TRUE —
-- **not** 100% identical → **CF-mig159-BILATERAL-FLAG-DUP NOT OPEN**.
--
-- **Stim-Tg / Tg-span non-null envelopes:** `max_stimulated_tg` **238**; `anti_tg_nadir` **1,376**;
--   `days_first_to_last_tg` **2,523**; `days_to_first/last_laryngoscopy` **24** each (sparse larynx timing).
--
-- **LN positive 3-source reconciliation (`total_ln_positive_v10` vs `tp_ln_positive` mig_150 vs
--   `ln_total_positive` mig_133):** pairwise `IS DISTINCT FROM` on populated rows —
--   **`total_ln_positive_v10` vs `tp_ln_positive`** = **2,965** / **10,871** rows;
--   **`total_ln_positive_v10` vs `ln_total_positive`** = **3,222** rows — definitional rollup / ordinal
--   grain divergence — **CF-mig159-LN-POSITIVE-V10-VS-TP-VS-TOTAL** (sign-off verifies PM column is coherent
--   SSOT-consumer, not bitwise identical to sibling rollups).
--
-- **`r_class_true` vs `margin_r_class_v10` (mig_154):** **3,338** rid rows both non-null AND string-unequal
--   — `r_class_true` is adjudicated manuscript-truth lineage; **`margin_r_class_v10`** is path-invasion-slot
--   rollup semantics — **CF-mig159-R-CLASS-TRUE-VS-V10-DIVERGENCE** (**expected**, not rollback).
--
-- **`date_traceability_status` VARCHAR:** **4** distinct lineage buckets (`entity_date_traced`, `inferred_*`,
--   `note_date_only`, `surgery_anchor_only`) — **non-degenerate** — **NOT**
--   **CF-mig159-VALUE-DEGENERATE-UPSTREAM-date_traceability_status**.
--
-- **`laterality` VARCHAR:** **3** distinct populated labels (`left` / `right` / `bilateral`); cross-checked
--   mentally vs `canonical_path_malignant_patient_rollup_v1` laterality spine.
--
-- Disposition: **27** cols → **`verified`** (Protocol v2 Gate 4 metadata on flip). No **`na`** reclass —
-- degenerate-single-gene placeholders not observed live for this cohort build.
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0`) — **execute after** independent Logan review.
-- Cowork agent: SQL authoring + read-only probes only (**no** RW executes from agent session per Protocol §5).
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig159_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig159_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'alk_positive_v7', 'completion_braf_positive', 'completion_tert_positive', 'eif1ax_positive',
    'hras_positive_v11', 'kras_positive_v11', 'nras_positive_v11', 'ntrk_positive_v7',
    'pax8_pparg_positive', 'tp53_positive_v7',
    'completion_reason', 'completion_reason_confidence',
    'bilateral_disease_flag', 'bilateral_path_flag',
    'anti_tg_nadir', 'anti_tg_rising_flag', 'max_stimulated_tg', 'max_stimulated_tg_date',
    'max_stimulated_tg_source', 'max_stimulated_tg_source_note_ref', 'days_first_to_last_tg',
    'days_to_first_laryngoscopy', 'days_to_last_laryngoscopy',
    'date_traceability_status', 'laterality', 'r_class_true', 'total_ln_positive_v10'
  );

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 159a — 10 cols — molecular single-gene / completion-specimen flags (BOOLEAN)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_molecular_genetics_v2',
    batch_id            = 'mig_159_patient_master_final_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_159 final residual (159a molecular single-gene). BOOL_OR replay vs '
                          || 'canonical_molecular_genetics_v2 gene-positive / fusion flags; '
                          || 'completion_* = completion-thyroidectomy specimen slice; '
                          || 'CF-mig159-MOLECULAR-V7-V11-DRIFT lineage note in header.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'alk_positive_v7', 'completion_braf_positive', 'completion_tert_positive', 'eif1ax_positive',
    'hras_positive_v11', 'kras_positive_v11', 'nras_positive_v11', 'ntrk_positive_v7',
    'pax8_pparg_positive', 'tp53_positive_v7'
  );

-- -----------------------------------------------------------------------------
-- 159b — 2 cols — completion thyroidectomy reason + confidence
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_operative_events_v1_completion_meta',
    batch_id            = 'mig_159_patient_master_final_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_159 final residual (159b completion meta). categorical reason + DOUBLE '
                          || 'confidence from operative / LLM completion-thyroidectomy extraction chain.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN ('completion_reason', 'completion_reason_confidence');

-- -----------------------------------------------------------------------------
-- 159c — 2 cols — bilateral clinical vs path-only flags
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_path_malignant_patient_rollup_v1_us_thyroid',
    batch_id            = 'mig_159_patient_master_final_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_159 final residual (159c bilateral). US/clinical envelope vs pathology '
                          || 'tier; counts differ materially — bilateral CF-DUP cleared in header.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN ('bilateral_disease_flag', 'bilateral_path_flag');

-- -----------------------------------------------------------------------------
-- 159d — 7 cols — stimulated Tg, anti-Tg, longitudinal TG span — labs SSOT (Script 347 lineage)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_thyroglobulin_v1',
    batch_id            = 'mig_159_patient_master_final_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_159 final residual (159d Tg/TgAb). Replay vs canonical_labs_thyroglobulin_v1 '
                          || '(stim cohort, MIN anti-Tg, rising slope, delta-days); DATE policy OK on '
                          || 'max_stimulated_tg_date — CF clear in header.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'anti_tg_nadir',
    'anti_tg_rising_flag',
    'max_stimulated_tg',
    'max_stimulated_tg_date',
    'max_stimulated_tg_source',
    'max_stimulated_tg_source_note_ref',
    'days_first_to_last_tg'
  );

-- -----------------------------------------------------------------------------
-- 159e — 2 cols — laryngoscopy timing ( BIGINT days ) — mig_98c voice complications spine
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_v1',
    batch_id            = 'mig_159_patient_master_final_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_159 final residual (159e laryngoscopy). MIN/MAX timing_days envelope from '
                          || 'canonical_complications_events_v1 laryngoscopy-tagged events.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN ('days_to_first_laryngoscopy', 'days_to_last_laryngoscopy');

-- -----------------------------------------------------------------------------
-- 159f — 4 cols — misc (provenance / laterality / R-class adjudication / LN rollup v10)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_source_resolution_r_ln_margin_truth_v150_v154_v133',
    batch_id            = 'mig_159_patient_master_final_residual_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_159 final residual (159f misc). date_traceability_status 4-bucket lineage; '
                          || 'laterality tri-label; r_class_true manuscript adjudication truth vs mig_154 slot col; '
                          || 'total_ln_positive_v10 vs tp_ln_positive / ln_total_positive divergence CF in header.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'date_traceability_status',
    'laterality',
    'r_class_true',
    'total_ln_positive_v10'
  );

-- -----------------------------------------------------------------------------
-- 159g — Resync `canonical_table_signoff_registry_v1` for `canonical_patient_master`
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
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes           = COALESCE(ts.notes, '')
                    || ' | mig_159: final residual cluster CLOSED (27 cols). LN + R-class CF refs in header.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'           THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name  = 'canonical_patient_master'
  GROUP BY 1, 2
) AS subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- -----------------------------------------------------------------------------
-- 159h — Carry-forward CF tags on high-divergence misc columns (LN + adjudicated R-class)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig159-LN-POSITIVE-V10-VS-TP-VS-TOTAL: see mig_159 header 3-source drift counts.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_159_patient_master_final_residual_cluster_20260429'
  AND column_name = 'total_ln_positive_v10';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig159-R-CLASS-TRUE-VS-V10-DIVERGENCE: r_class_true adjudicated vs margin_r_class_v10 slot.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_159_patient_master_final_residual_cluster_20260429'
  AND column_name = 'r_class_true';

COMMIT;

-- =============================================================================
-- end migration 159 — CPM final residual cluster verified (27 cols)
-- =============================================================================
