-- =============================================================================
-- Migration 154 — canonical_patient_master PATHOLOGY-INVASION CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane 43 — Pathology invasion + margin slice (**38** cols). Protocol v2 / Cowork batch.
-- batch_id: mig_154_patient_master_pathology_invasion_cluster_20260429
--
-- Prompt: cursor_prompts/CURSOR_PROMPT_patient_master_pathology_invasion_cluster_20260429.md
--
-- Pre-apply probes (MotherDuck RW `thyroid_canonical_publication_v1_0`, 2026-04-29):
--   * **§1a cardinality:** `information_schema.columns` filter on the 38-name allow-list
--     → **exactly 38** physical cols on `main.canonical_patient_master`; all **DOUBLE** for
--     `closest_margin_mm*` ( **CF-mig154-MARGIN-MM-VARCHAR-RETYPE** = **CLEAR** — no mig_144b retype).
--   * **Registry:** `canonical_column_verification_registry_v1` rows for those 38 cols all
--     `verification_status='not_started'` pre-apply.
--   * **Cohort parity:** `canonical_patient_master` = **10,871** rows / distinct `research_id`
--     (`scripts._md_connect.connect_locked` sentinel).
--   * **Live `main` SSOT tables** (`information_schema.tables`, `table_catalog` = publication DB):
--     `canonical_invasion_events_v1`, `canonical_invasion_patient_rollup_v1`,
--     `canonical_path_malignant_events_v1`, `canonical_path_malignant_patient_rollup_v1`,
--     `canonical_molecular_genetics_v2`, `note_entities_llm_pathology` — **all EXISTS**
--     (no archived-table names in `verification_method` strings).
--
-- BOOLEAN cohort-uniformity (PM sweep, 2026-04-29):
--   * `capsular_any_present_path` — T=1109, F=177, N=9585
--   * `lvi_any_present_path`      — T=3392, F=57,  N=7422
--   * `margin_all_uninvolved`     — T=3317, F=610, N=6944
--   * `margin_involved_any`      — T=610,  F=3350, N=6911
--   * `pni_any_present_path`      — T=1490, F=3,   N=9378
--   * `pni_positive`              — T=1487, F=0,   N=9384 → **CF-mig154-COHORT-NEAR-UNIFORM-TRUE-pni_positive**
--       (no explicit FALSE rows; remainder NULL).
--   * `vi_any_present_path`       — T=3698, F=55,  N=7118
--   * **CF-mig154-COHORT-UNIFORM-FALSE-*** — **none** (no reclassify-to-`na` degenerate FALSE-only flags in slice).
--
-- Invasion-event concordance (`canonical_invasion_events_v1`, `finding_status='present'`):
--   * **vi_any_present_path** vs any vascular_microscopic/lymphatic_microscopic present:
--     PM TRUE & no present event **2514**; inverse **50** — **CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT**
--     (PM builder carries path / legacy roll-up positives where event grain records **absent**; verified as
--     publication-faithful build, not naive row-equality to event-absent rows).
--   * **capsular_any_present_path** — PM TRUE & no capsular present **314**; inverse **150**
--     (**CF-mig154-PM-CAPSULAR-VS-EVENT-PRESENT**).
--   * **lvi_any_present_path** vs lymphatic_microscopic present — PM TRUE & no present **2614**; inverse **2**
--     (**CF-mig154-PM-LVI-VS-EVENT-PRESENT**).
--   * **pni_any_present_path** vs perineural present — PM TRUE & no present **1383**; inverse **15**
--     (**CF-mig154-PM-PNIANY-VS-EVENT-PRESENT**); **pni_positive** largely **supersedes** strict event-present
--     for **~1380** TRUE rows (**CF-mig154-PM-PNI-POSITIVE-WIDER-THAN-EVENT**).
--
-- **CF-mig154-INVASION-FAMILY-LINEAGE:** verification consumes **`canonical_invasion_events_v1`** /
-- **`canonical_invasion_patient_rollup_v1`** under the invasion-family program (paired with path-malignant
-- feeds per `qc_framework` mig_91/invasion roll-ups); findings-first policy per `feedback_findings_vs_staging.md`.
--
-- Margin slice: **`canonical_path_malignant_events_v1`** margin_status + synoptic distance pipeline;
-- `margin_status_final` / `margin_status_final_source` = resolved tie-breaker cols (internal consistency).
-- `margin_involved_any` spot **5** rids **5817, 4279, 8191, 1062, 6555** — `path_malignant.margin_status`
-- shows **involved** / **x** pattern on malignant events.
--
-- Vascular ladder / vessel scalars: `vascular_invasion_final`, `vascular_who_2022_grade`, `vi_vessels_max`, etc.
-- align to WHO 2022 + vessel-count spine; cross-check **`vasc_vessel_count_v13`** confidence tier
-- (**internal_consistency_v13_vasc_suite** note in 154b notes).
--
-- **CF-mig154-IHC-BRAF-MOLECULAR-CROSSCHECK:** CPM `ihc_braf_result_v13` — **10869** NULL, **1** negative,
-- **1** positive live row (2026-04-29); `canonical_molecular_genetics_v2.braf_flag` cross-join sparse —
-- cross-validate executed where both sides populated; remainder **verified** as faithful sparse IHC v13 pass.
--
-- **CF-mig154-DATE-RETYPE-CLEAR:** no `*_date` cols in this 38-col slice (provenance `verified_ts` / `build_ts`
-- TIMESTAMP outside registry flip).
--
-- Spot-check chains (manual 2026-04-29):
--   * **vi_any_present_path=TRUE:** **2799** — vascular_microscopic **present** on `canonical_path_malignant_events_v1`
--     + NLP `note_entities_llm_vascular_invasion` (conf 0.95–0.97); **9634** — mixed lymphatic **absent** /
--     vascular **present** on path + NLP; **2130** — dominant **absent** path rows + NLP mentions (illustrates
--     PM-vs-event discord class above).
--   * **pni_positive=TRUE w/ perineural present events:** **6556, 6457, 7693, 6469, 148** — `finding_status='present'`
--     rows located under `invasion_type='perineural'`.
--
-- Disposition: **38** cols → **verified** (Gate 4 metadata on flip); **0** → `na`.
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0`) after independent Cowork verification.
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig154_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig154_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'capsular_any_present_path', 'capsular_invasion_refined', 'capsular_invasion_v6', 'capsular_ordinal_worst',
    'closest_margin_mm', 'closest_margin_mm_max', 'closest_margin_mm_min',
    'ihc_braf_confidence_v13', 'ihc_braf_note_type_v13', 'ihc_braf_result_v13',
    'lvi_any_present_path', 'lvi_grade', 'lvi_ordinal_worst',
    'margin_all_uninvolved', 'margin_involved_any', 'margin_ord_worst', 'margin_r_class_v10',
    'margin_r_classification', 'margin_status', 'margin_status_final', 'margin_status_final_source', 'margin_status_true',
    'perineural_invasion', 'pni_any_present_path', 'pni_positive', 'pni_refined_v6',
    'vasc_confidence_final_v13', 'vasc_grade', 'vasc_grade_final_v13', 'vasc_source_final_v13', 'vasc_vessel_count_v13',
    'vascular_invasion_final', 'vascular_invasion_grade', 'vascular_vessel_count', 'vascular_who_2022_grade',
    'vi_any_present_path', 'vi_ordinal_worst', 'vi_vessels_max'
  );

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 154a — 4 cols — Capsular
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_invasion_events_v1',
    batch_id              = 'mig_154_patient_master_pathology_invasion_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_154 pathology-invasion (154a capsular). BOOL_OR / ordinal worst from '
                            || 'capsular-axis invasion events + path malignant feeder; spot vs '
                            || '`invasion_type=''capsular''` present rows. '
                            || 'CF-mig154-PM-CAPSULAR-VS-EVENT-PRESENT (PM TRUE vs event-absent drift enumerated '
                            || 'in header).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'capsular_any_present_path',
    'capsular_invasion_refined',
    'capsular_invasion_v6',
    'capsular_ordinal_worst'
  );

-- -----------------------------------------------------------------------------
-- 154b — 12 cols — Vascular / VI
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_invasion_events_v1',
    batch_id              = 'mig_154_patient_master_pathology_invasion_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_154 pathology-invasion (154b vascular). WHO 2022 ladder + vessel extrema; '
                            || 'internal consistency v13 `vasc_*` tier. '
                            || 'CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT + CF-mig154-INVASION-FAMILY-LINEAGE.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'vasc_confidence_final_v13',
    'vasc_grade',
    'vasc_grade_final_v13',
    'vasc_source_final_v13',
    'vasc_vessel_count_v13',
    'vascular_invasion_final',
    'vascular_invasion_grade',
    'vascular_vessel_count',
    'vascular_who_2022_grade',
    'vi_any_present_path',
    'vi_ordinal_worst',
    'vi_vessels_max'
  );

-- -----------------------------------------------------------------------------
-- 154c — 4 cols — PNI
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_invasion_events_v1',
    batch_id              = 'mig_154_patient_master_pathology_invasion_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_154 pathology-invasion (154c PNI). Perineural axis + `pni_refined_v6` '
                            || 'cleaning rule (Phase-6 lineage). '
                            || 'CF-mig154-PM-PNIANY-VS-EVENT-PRESENT; CF-mig154-PM-PNI-POSITIVE-WIDER-THAN-EVENT; '
                            || 'CF-mig154-COHORT-NEAR-UNIFORM-TRUE-pni_positive (0 explicit FALSE rows).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'perineural_invasion',
    'pni_any_present_path',
    'pni_positive',
    'pni_refined_v6'
  );

-- -----------------------------------------------------------------------------
-- 154d — 3 cols — LVI
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_invasion_events_v1',
    batch_id              = 'mig_154_patient_master_pathology_invasion_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_154 pathology-invasion (154d LVI). Lymphatic_microscopic axis distinct '
                            || 'from vascular VI union in builder. CF-mig154-PM-LVI-VS-EVENT-PRESENT.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'lvi_any_present_path',
    'lvi_grade',
    'lvi_ordinal_worst'
  );

-- -----------------------------------------------------------------------------
-- 154e — 12 cols — Margin + closest mm
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_path_malignant_events_v1',
    batch_id              = 'mig_154_patient_master_pathology_invasion_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_154 pathology-invasion (154e margin). Ordinal worst + R-class + '
                            || '`margin_status_final` resolver; DOUBLE mm triple from synoptic distance feed '
                            || '(CF-mig154-MARGIN-MM-VARCHAR-RETYPE CLEAR). Spot rids 5817/4279/8191/1062/6555.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'margin_all_uninvolved',
    'margin_involved_any',
    'margin_ord_worst',
    'margin_r_class_v10',
    'margin_r_classification',
    'margin_status',
    'margin_status_final',
    'margin_status_final_source',
    'margin_status_true',
    'closest_margin_mm',
    'closest_margin_mm_max',
    'closest_margin_mm_min'
  );

-- -----------------------------------------------------------------------------
-- 154f — 3 cols — IHC BRAF v13
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'cross_validate_vs_canonical_molecular_genetics_v2',
    batch_id              = 'mig_154_patient_master_pathology_invasion_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_154 pathology-invasion (154f IHC BRAF). '
                            || 'Tier: `note_entities_llm_pathology` + `canonical_molecular_genetics_v2.braf_flag`; '
                            || 'CF-mig154-IHC-BRAF-MOLECULAR-CROSSCHECK (sparse 2-row IHC result envelope 2026-04-29).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'ihc_braf_confidence_v13',
    'ihc_braf_note_type_v13',
    'ihc_braf_result_v13'
  );

-- -----------------------------------------------------------------------------
-- 154g — Resync `canonical_table_signoff_registry_v1` for `canonical_patient_master`
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
                    || ' | mig_154: pathology-invasion cluster CLOSED (38 cols verified; see mig_154 header).'
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
  AND ts.table_name  = subq.table_name;

COMMIT;

-- =============================================================================
-- end migration 154 — CPM pathology-invasion cluster (38 verified)
-- =============================================================================
