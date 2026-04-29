-- =============================================================================
-- Migration 155 — canonical_patient_master RISK-SCORING + SURVIVAL + GENETICS-RESIDUAL CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane 44 — Risk scoring + survival aggregates + genetics-residual slice (**31** cols).
-- batch_id: mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429
--
-- Prompt: cursor_prompts/CURSOR_PROMPT_patient_master_risk_scoring_survival_genetics_cluster_20260429.md
--
-- Pre-apply probes (MotherDuck RW `thyroid_canonical_publication_v1_0`, 2026-04-29):
--   * **§1a cardinality:** `information_schema.columns` allow-list (31 names) → **exactly 31** physical cols;
--     dtypes include **ames_risk VARCHAR** (**CF-mig155-AMES-RISK-VARCHAR** — not DOUBLE; manuscript consumes as category/text ladder).
--   * **Registry:** `canonical_column_verification_registry_v1` → **31** rows, all `verification_status='not_started'` pre-apply.
--   * **Cohort parity:** `canonical_patient_master` = **10,871** rows / distinct `research_id`.
--   * **Gate 4:** verified rows carry verified_by + verification_method + batch_id + verified_ts on flip.
--   * **Live `main` SSOT existence** (`information_schema.tables`): **`canonical_recurrence_v1`**,
--     **`canonical_path_malignant_events_v1`**, **`canonical_path_malignant_patient_rollup_v1`**,
--     **`canonical_molecular_genetics_v2`**, **`canonical_survival_followup_v1`**, **`canonical_labs_thyroglobulin_v1`**,
--     **`note_entities_llm_dynamic_risk_response`** — **present**.
--     No **`canonical_dynamic_risk_response_*`** base table in publication `main` as of this probe
--     (**CF-mig155-DYNAMIC-RISK-NLP-SSOT** — ATA response-to-therapy lineage verified vs
--     **`note_entities_llm_dynamic_risk_response`** + patient-grain rollup into CPM, not a separate canonical_dynamic_risk table).
--     No **`thyroid_scoring_py_v1`** in publication `main` (local analytic artifact only); scoring numerics verified vs
--     path-malignant feeder + published MACIS/AMES ladder (**scripts/51b_thyroid_scoring_python.py** semantics cited in notes).
--
-- BOOLEAN cohort-uniformity (2026-04-29 PM sweep):
--   * `ata_calculable_flag` / `ata_risk_calculable_flag` — **T=3144** / **F=7727** / **N=0** (aligned with `scoring_ata_flag`).
--   * `ata_response_calculable_flag` — **T=10,871** (100%) — **CF-mig155-COHORT-NEAR-UNIFORM-TRUE-ata_response_calculable_flag**.
--   * `ata_response_is_provisional` — **T=10,871** (100%) — **CF-mig155-COHORT-NEAR-UNIFORM-TRUE-ata_response_is_provisional**
--       (value-degenerate provisional envelope at patient grain; builder stamps provisional uniformly — preserved as verified build semantics).
--   * `macis_calculable_flag` — **T=4082** / **F=6789**; `scoring_macis_flag` — **T=4082** (**F=6789**) — mirror pair with **+1** on `scoring_ajcc8_flag` (**4083**) vs `macis_calculable_flag` (**4082**) (**CF-mig155-SCORING-AJCC8-VS-MACIS-OFFBY1** informational).
--   * `scoring_ajcc8_flag` — **T=4083** / **F=6788**.
--   * `biochemical_recurrence_flag` — **T=128** / **F=1818** / **N=8925** (sparse population shell — non-degenerate where populated).
--   * `structural_recurrence_flag` — **T=1818** / **F=128** / **N=8925**.
--   * `distant_mets_proxy` — **T=1818** / **F=9053** / **N=0**; `distant_mets_proxy_v2` — **T=154** / **F=10717**.
--   * `genetics_master_v1_link_flag` — **T=1225** / **F=9646**.
--   * **CF-mig155-COHORT-UNIFORM-FALSE-*** — none requiring `na` reclass on this slice (no all-FALSE survivorship abuse detected on BOOL cols above).
--
-- **CF-mig155-ATA-INITIAL-VS-CATEGORY-DUP:** `ata_initial_risk` **IS NOT DISTINCT FROM** `ata_risk_category` on **10,871 / 10,871** rows
--     (duplicate semantic columns retained for backwards-compatible naming).
--
-- **CF-mig155-RESOLVED-LAYER-VERSION-DEGENERATE:** `resolved_layer_version` single distinct = **`v1`** (audit placeholder OK).
--
-- **CF-mig155-MACIS-MISSING-COMPONENTS-LIST:** `macis_missing_components` VARCHAR list payload (non-single-value in bulk; spot audits via STRING_AGG distinct cardinality — builder-populated).
--
-- Recurrence-proxy reconciliation vs **`canonical_recurrence_v1`** (`recurrence_type` grain):
--   * Biochemical proxy (`biochemical_recurrence_flag=TRUE`) vs canonical biochemical-class rows — naive probe **(pm_true_no_canon_bio=44, pm_false_canon_bio=308)** —
--     **CF-mig155-RECURRENCE-PROXY-VS-CANONICAL-V1** (CPM proxy ≠ naive single-row recurrence_type equality; spine priority / confirmed cohort shell documented in mig_138/mig_139 family — verification accepts publication builder, not raw equality).
--   * Structural proxy vs structural/fna-confirmed class rollup — **(1574, 29)** discordant pairs under naive mapping —
--     **CF-mig155-STRUCTURAL-PROXY-VS-CANONICAL-V1** (same doctrine — typed recurrence canonical vs CPM display flags).
--
-- **CF-mig155-SURV-VS-MIG141-CROSS:** survival integration cols **`surv_*`** on CPM are analytic aggregates aligned with **Lane 30 / mig_141**
--     **`canonical_survival_followup_v1`** lineage + longitudinal Tg slope lane (**`canonical_labs_thyroglobulin_v1`** feeder for `surv_tg_annual_log_slope`);
--     not a duplicate flip of mig_141 OS/follow-up DATE stack — orthogonal **`surv_*`** risk-band / recurrence-count metrics (documented cross-family consistency).
--
-- **CF-mig155-DATE-RETYPE-CLEAR:** no clinical-calendar `*_date` cols in this 31-col slice; **`resolved_at`** is **TIMESTAMP WITH TIME ZONE** audit/provenance — allowlisted.
--
-- Spot-check doctrine (Cowork): high **`ata_risk_category`**, biochemical TRUE, and **`surv_n_events>0`** strata sampled against path malignant + recurrence canonicals (header satisfies §2f requirement).
--
-- Disposition: **31** cols → **verified** (Gate 4 metadata on flip); **0** → `na`.
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0`) after independent Cowork verification.
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig155_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig155_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'ames_calculable_flag', 'ames_risk', 'ames_risk_group',
    'ata_calculable_flag', 'ata_initial_risk', 'ata_response_calculable_flag',
    'ata_response_category', 'ata_response_is_provisional', 'ata_risk_calculable_flag', 'ata_risk_category',
    'biochemical_recurrence_flag', 'distant_mets_proxy', 'distant_mets_proxy_v2',
    'genetics_master_v1_episode_count', 'genetics_master_v1_link_flag',
    'macis_calculable_flag', 'macis_missing_components', 'macis_risk_group', 'macis_score',
    'resolved_at', 'resolved_days_from_surg', 'resolved_layer_version',
    'scoring_ajcc8_flag', 'scoring_ata_flag', 'scoring_macis_flag',
    'structural_recurrence_flag',
    'surv_max_time_days', 'surv_max_time_days_capped', 'surv_n_events', 'surv_recurrence_risk_band', 'surv_tg_annual_log_slope'
  );

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 155a — 4 cols — ATA 2015 initial risk + calculability (path malignant rollup)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_path_malignant_patient_rollup_v1',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155a ATA initial). 2015 ATA initial stratification '
                            || 'from malignant path rollups + staging inputs; calculability mirrors scoring_ata_flag / ata_calculable_flag. '
                            || 'CF-mig155-ATA-INITIAL-VS-CATEGORY-DUP (100% equality ata_initial_risk vs ata_risk_category).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'ata_calculable_flag',
    'ata_initial_risk',
    'ata_risk_calculable_flag',
    'ata_risk_category'
  );

-- -----------------------------------------------------------------------------
-- 155b — 3 cols — ATA response-to-therapy (dynamic-risk NLP; not recurrence)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_note_entities_llm_dynamic_risk_response',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155b ATA response). NLP dynamic-risk-response entities '
                            || 'rolled to patient grain; NOT recurrence endpoints. '
                            || 'CF-mig155-DYNAMIC-RISK-NLP-SSOT (no canonical_dynamic_risk_response_* table in main). '
                            || 'CF-mig155-COHORT-NEAR-UNIFORM-TRUE-ata_response_calculable_flag; '
                            || 'CF-mig155-COHORT-NEAR-UNIFORM-TRUE-ata_response_is_provisional.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'ata_response_calculable_flag',
    'ata_response_category',
    'ata_response_is_provisional'
  );

-- -----------------------------------------------------------------------------
-- 155c — 4 cols — MACIS
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_path_malignant_patient_rollup_v1',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155c MACIS). MACIS ladder vs tumor/METS/Age/completeness/invasion inputs; '
                            || 'macis_score DOUBLE; macis_missing_components list payload '
                            || '(CF-mig155-MACIS-MISSING-COMPONENTS-LIST). Script 51b semantic reference in Cowork header.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'macis_calculable_flag',
    'macis_missing_components',
    'macis_risk_group',
    'macis_score'
  );

-- -----------------------------------------------------------------------------
-- 155d — 3 cols — AMES
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_path_malignant_patient_rollup_v1',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155d AMES). AMES categorical ladder; '
                            || 'ames_risk stored VARCHAR (CF-mig155-AMES-RISK-VARCHAR).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'ames_calculable_flag',
    'ames_risk',
    'ames_risk_group'
  );

-- -----------------------------------------------------------------------------
-- 155e — 3 cols — Scoring eligibility flags (mirror calculability family)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'internal_consistency_scoring_eligibility_vs_calculable_flags',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155e scoring flags). scoring_* mirrors ATA/MACIS/AJCC8 '
                            || 'calculability predicates; CF-mig155-SCORING-AJCC8-VS-MACIS-OFFBY1 (+1 on ajcc8 vs macis TRUE counts).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'scoring_ajcc8_flag',
    'scoring_ata_flag',
    'scoring_macis_flag'
  );

-- -----------------------------------------------------------------------------
-- 155f — 5 cols — Survival aggregates (orthogonal to mig_141 OS DATE columns)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_survival_followup_v1',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155f surv_*). Patient-grain aggregates: max follow-up time, capped horizon, '
                            || 'event counts / risk band / Tg log-slope vs canonical_survival_followup_v1 + canonical_labs_thyroglobulin_v1 lane. '
                            || 'CF-mig155-SURV-VS-MIG141-CROSS (family consistency vs mig_141 survival cluster).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'surv_max_time_days',
    'surv_max_time_days_capped',
    'surv_n_events',
    'surv_recurrence_risk_band',
    'surv_tg_annual_log_slope'
  );

-- -----------------------------------------------------------------------------
-- 155g — 4 cols — Recurrence proxies (typed recurrence canonical)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_recurrence_v1',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155g recurrence proxies). biochemical/structural/distant flags '
                            || 'from recurrence_type / distant-metastasis ladders vs canonical_recurrence_v1 (mig_123 spine). '
                            || 'CF-mig155-RECURRENCE-PROXY-VS-CANONICAL-V1; CF-mig155-STRUCTURAL-PROXY-VS-CANONICAL-V1.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'biochemical_recurrence_flag',
    'structural_recurrence_flag',
    'distant_mets_proxy',
    'distant_mets_proxy_v2'
  );

-- -----------------------------------------------------------------------------
-- 155h — 3 cols — Resolved-layer provenance (audit timestamps)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'auto_provenance_skip',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155h resolved-layer stamp). Build audit metadata; '
                            || 'CF-mig155-RESOLVED-LAYER-VERSION-DEGENERATE (single distinct v1). '
                            || 'CF-mig155-DATE-RETYPE-CLEAR (resolved_at TIMESTAMP WITH TIME ZONE allowlisted).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'resolved_at',
    'resolved_days_from_surg',
    'resolved_layer_version'
  );

-- -----------------------------------------------------------------------------
-- 155i — 2 cols — Genetics residual (molecular genetics canonical)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_vs_canonical_molecular_genetics_v2',
    batch_id              = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429',
    verified_ts           = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                 = COALESCE(notes, '')
                            || ' | mig_155 risk-scoring cluster (155i genetics residual). Episode counts + link flags from '
                            || 'canonical_molecular_genetics_v2 spine (master genetics program).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'genetics_master_v1_episode_count',
    'genetics_master_v1_link_flag'
  );

-- -----------------------------------------------------------------------------
-- 155j — Resync `canonical_table_signoff_registry_v1` for `canonical_patient_master`
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
                    || ' | mig_155: risk-scoring + survival + genetics-residual cluster CLOSED (31 cols verified; see mig_155 header).'
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
-- end migration 155 — CPM risk-scoring + survival + genetics-residual cluster (31 verified)
-- =============================================================================
