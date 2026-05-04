-- mig_298 — M004 Autoimmune × Cancer cohort view (Option 2 — NLP-augmented)
-- Generated: 2026-05-04
-- Target DB: thyroid_canonical_publication_v1_0
--
-- Context:
--   M004 ready-for-writing brief (manuscript_outputs/v1_0_20260501/M004_READY_FOR_WRITING_BRIEF.md)
--   selected the NLP-augmented exposure path. SF AI_CLASSIFY pilots N4 (Hashimoto) and
--   N5 (Graves) cleared the 70% gate at full scale (round 19 commit a80ae25), so
--   the combined NLP+syn exposure is the locked spec for the M004 cohort view.
--
-- Combined exposure rules:
--   has_hashi  := COALESCE(syn_hashimoto, FALSE) OR COALESCE(nlp_hashimoto, FALSE)
--   has_graves := COALESCE(syn_graves,    FALSE) OR COALESCE(nlp_graves,    FALSE)
--
-- Source NLP rollup tables (Snowflake, materialized to MD by mig_298 apply script):
--   THYROID_VALIDATION.PUBLIC.NLP_HASHIMOTO_FULL_RESULTS_v1 — note-level (hashimoto_status enum)
--   THYROID_VALIDATION.PUBLIC.NLP_GRAVES_FULL_RESULTS_v1    — note-level (graves_status enum)
--
-- Apply path:
--   .venv/bin/python scripts/mig_298_m004_cohort_view.py
--
-- Closes:
--   - cursor_prompts/CURSOR_PROMPT_MIG_298_M004_COHORT_VIEW_BUILD_20260504.md (Option 2 elected)
--   - Unblocks M004 v1.0 submission package (next mig_301 candidate per ready-for-writing brief)

-- -----------------------------------------------------------------------------
-- §1 NLP per-patient rollup (materialized in MD; built by apply script via SF pull)
-- -----------------------------------------------------------------------------
-- The apply script issues:
--   CREATE OR REPLACE TABLE manuscript_workspace.m004_nlp_autoimmune_rollup_v1 (
--     research_id    BIGINT PRIMARY KEY,
--     nlp_hashimoto  BOOLEAN,
--     nlp_graves     BOOLEAN,
--     hashimoto_n_notes_present INTEGER,
--     graves_n_notes_present    INTEGER,
--     llm_model      VARCHAR,
--     materialized_at TIMESTAMP
--   );
-- Population logic per SF source:
--   nlp_hashimoto = MAX(hashimoto_status = 'hashimoto_present') OVER research_id
--   nlp_graves    = MAX(graves_status    = 'graves_present')    OVER research_id

-- -----------------------------------------------------------------------------
-- §2 Cohort view (final spec — Option 2 NLP+syn combined)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m004_autoimmune_cancer_v1 AS
SELECT
  pm.research_id,
  pm.age_at_surgery,
  pm.sex,
  pm.race,
  pm.is_malignant,
  pm.histology_final,
  pm.surg_first_date,
  pm.first_surgery_date,
  pm.followup_years,
  pm.death_occurred,
  pm.overall_survival_days,
  pm.any_recurrence_flag,
  -- Synoptic-only exposures (Option 1 baseline — preserved for sensitivity panels)
  pm.syn_hashimoto,
  pm.syn_graves,
  -- NLP-augmented exposures (Option 2 — from SF AI_CLASSIFY rollup)
  COALESCE(r.nlp_hashimoto, FALSE) AS nlp_hashimoto,
  COALESCE(r.nlp_graves,    FALSE) AS nlp_graves,
  -- Combined exposures (primary M004 covariates)
  (COALESCE(pm.syn_hashimoto, FALSE) OR COALESCE(r.nlp_hashimoto, FALSE)) AS has_hashi,
  (COALESCE(pm.syn_graves,    FALSE) OR COALESCE(r.nlp_graves,    FALSE)) AS has_graves,
  CASE
    WHEN (COALESCE(pm.syn_hashimoto, FALSE) OR COALESCE(r.nlp_hashimoto, FALSE))
     AND (COALESCE(pm.syn_graves,    FALSE) OR COALESCE(r.nlp_graves,    FALSE)) THEN 'both'
    WHEN (COALESCE(pm.syn_hashimoto, FALSE) OR COALESCE(r.nlp_hashimoto, FALSE)) THEN 'hashimoto_only'
    WHEN (COALESCE(pm.syn_graves,    FALSE) OR COALESCE(r.nlp_graves,    FALSE)) THEN 'graves_only'
    ELSE 'neither'
  END AS autoimmune_category,
  -- Smoking + family-hx covariates (mig_281 promotion)
  pm.pmhx_nlp_smoking_status,
  pm.pmhx_nlp_family_hx_thyroid,
  -- Provenance
  r.llm_model         AS nlp_llm_model,
  r.materialized_at   AS nlp_materialized_at
FROM main.canonical_patient_master pm
LEFT JOIN manuscript_workspace.m004_nlp_autoimmune_rollup_v1 r
  USING (research_id);

-- -----------------------------------------------------------------------------
-- §3 Verification (handled by apply script; expected counts from M004 brief):
--    autoimmune_category   n_expected   n_malig   pct_malig
--    both                  ~52
--    hashimoto_only        ~296   (348 total hashi - 52 both)
--    graves_only          ~1552   (1604 total graves - 52 both)
--    neither              ~8971
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- §4 Signoff (executed by apply script):
--   INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES (...)
-- -----------------------------------------------------------------------------
