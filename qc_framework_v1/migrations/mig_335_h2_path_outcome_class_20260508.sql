-- =============================================================================
-- mig_335 — pub_workspace.cohort_h2_pathology_outcome_v1 (VIEW)
--         + pub_workspace.h2_path_reconciliation_candidates_v1 (TABLE shell)
--
-- Date:       2026-05-08
-- Lane:       H2 manuscript — goiter/SDOH pathology-outcome classification
-- Depends:    pub_canonical.canonical_patient_master
--             pub_canonical.canonical_path_benign_events_v1
--             pub_canonical.canonical_path_benign_patient_rollup_v1
--             pub_canonical.canonical_path_malignant_events_v1
--             pub_canonical.canonical_path_malignant_patient_rollup_v1
--             pub_canonical.canonical_path_indeterminate_events_v1
--             pub_canonical.canonical_pmh_patient_rollup_v1
-- Author:     Cowork (Claude, Cursor Agent)
--
-- AUDIT ANCHORS:
--   DFL-20260507-H2-PATHCLASS-DERIVE       (Data Feedback Log, THYROID_MANUSCRIPT
--                                            base appJYOnUb7KrHKwpV,
--                                            table tblsiYKJtKcktkzze)
--   DFL-20260507-H2-RECONCILE-CANDIDATES   (Data Feedback Log, same base/table)
--   THY-33 (Linear, team Thyroid Database THY)
--
-- CONTEXT:
--   H2 manuscript (Hypothesis 2: goiter/SDOH) requires a per-patient cohort
--   view with:
--     (a) a 4-level pathology-outcome classifier (frank_malignancy /
--         benign_plus_incidental_microcarcinoma / indeterminate / pure_benign),
--     (b) 29 patient-level NLP flag columns rolled up from
--         canonical_path_benign_events_v1 via LOGICAL_OR, and
--     (c) PMH, preop-workup, and surgical-gland covariates.
--   This view is purely additive — no canonical_* tables are mutated.
--
--   Race bucket (5-level, per H2 v3 manuscript Table 1):
--     Exact recode from studies/hypothesis2_goiter_sdoh/
--       H2_AOSO_submission_package_v1_0/run_h2_v3_analysis.py
--       MAIN_RACES = ["Black or African American","White","Asian",
--                     "Unknown or Not Reported"]
--     Black/AA  → race = 'Black or African American'
--     White     → race = 'White'
--     Asian     → race = 'Asian'
--     Unknown   → race = 'Unknown or Not Reported' OR race IS NULL
--     Other     → all remaining (AI/AN, NHPI, Hispanic, Other, etc.)
--
--   NLP flags (29 output columns; MNG excluded — it is the cohort filter):
--     Per spec: 30 source NLP entities; nlp_mng / nlp_multinodular_goiter are
--     excluded as output columns because syn_multinodular_goiter = TRUE is the
--     WHERE clause. The 29 output columns preserve compound rollups:
--       hashimotos         = nlp_hashimotos OR nlp_hashimotos_thyroiditis
--       dequervain_granulomatous = nlp_de_quervains_thyroiditis OR
--                                  nlp_granulomatous_thyroiditis
--       graves             = nlp_graves OR nlp_graves_disease
--       adenomatous_hyperplasia = nlp_nodular_hyperplasia (structural alias)
--
--   Pathology outcome class (priority: frank > microcarcinoma > indeterminate
--                                      > pure_benign):
--     frank_malignancy                    is_malignant AND max tumor >= 1.0 cm
--     benign_plus_incidental_microcarcinoma  is_malignant AND max tumor <  1.0 cm
--     indeterminate                       NOT is_malignant AND has indeterminate
--                                         event in canonical_path_indeterminate_
--                                         events_v1
--     pure_benign                         NOT is_malignant AND no indeterminate
--                                         event
--
--   Dominant malignant group (8-level, NULL for benign/indeterminate patients):
--     PTC_classical, PTC_variants, FTC, Hurthle_oncocytic, MTC, PDTC, ATC,
--     Indeterminate_NIFTP_FTUMP_WDTUMP
--     Mapped from canonical_path_malignant_patient_rollup_v1.dominant_histology
--     + worst aggressive variant from canonical_path_malignant_events_v1.
--
-- SCOPE:
--   cohort_h2_pathology_outcome_v1    — VIEW, 6,075 rows (MNG cohort)
--   h2_path_reconciliation_candidates_v1 — TABLE shell (populated in Phase 2
--     after pub_workspace.h2_manual_path_flags_v1 CSV is loaded by Logan)
--
-- PHASE 2 NOTE:
--   graves_manual_only_flag is NULL placeholder in Phase 1.  Phase 2 will
--   merge canonical_path_benign_overrides_v1 (not created yet) to populate
--   it for patients where Graves was confirmed by manual review only.
--
-- VERIFY (post-apply):
--   SELECT COUNT(*), COUNTIF(is_malignant)
--   FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`;
--   -- expect: 6075 total, ~1528 malignant
--
--   SELECT race_bucket, COUNT(*) cnt
--   FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`
--   GROUP BY 1 ORDER BY cnt DESC;
--   -- expect: Black/AA ~2918, White ~2500, Unknown ~359, Asian ~193, Other ~105
--
--   SELECT pathology_outcome_class, COUNT(*) cnt
--   FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`
--   GROUP BY 1;
--   -- expect: pure_benign ~4500, benign_plus_incidental_microcarcinoma ~1000,
--   --         frank_malignancy ~400+, indeterminate ~140
-- =============================================================================

-- ── §1 View: cohort_h2_pathology_outcome_v1 ──────────────────────────────────
-- Audit anchor: DFL-20260507-H2-PATHCLASS-DERIVE / THY-33

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1` AS
WITH

  -- ── benign NLP rollup: LOGICAL_OR per patient across all benign events ──────
  -- Source: canonical_path_benign_events_v1 (event-grain, one row per synoptic)
  -- Compound rollups documented in header block above.
  benign_nlp_rollup AS (
    SELECT
      research_id,
      -- 29 output columns (MNG excluded — cohort filter):
      LOGICAL_OR(nlp_atypical_adenoma)                                         AS nlp_atypical_adenoma,
      LOGICAL_OR(nlp_hyperplasia)                                              AS nlp_hyperplasia,
      LOGICAL_OR(nlp_substernal_mng)                                           AS nlp_substernal_mng,
      LOGICAL_OR(nlp_nodular_hyperplasia)                                      AS nlp_adenomatous_hyperplasia,
      LOGICAL_OR(nlp_papillary_hyperplasia)                                    AS nlp_papillary_hyperplasia,
      LOGICAL_OR(nlp_hurthle_cell_adenoma)                                     AS nlp_hurthle_adenoma,
      LOGICAL_OR(nlp_follicular_adenoma)                                       AS nlp_follicular_adenoma,
      LOGICAL_OR(nlp_hyalinizing_trabecular_tumor)                             AS nlp_hyalinizing_trabecular,
      LOGICAL_OR(nlp_lymphocytic_thyroiditis)                                  AS nlp_lymphocytic_thyroiditis,
      LOGICAL_OR(nlp_chronic_lymphocytic_thyroiditis)                          AS nlp_chronic_lymphocytic_thyroiditis,
      LOGICAL_OR(nlp_hashimotos OR nlp_hashimotos_thyroiditis)                 AS nlp_hashimotos,
      LOGICAL_OR(nlp_palpation_thyroiditis)                                    AS nlp_palpation_thyroiditis,
      LOGICAL_OR(nlp_chronic_thyroiditis)                                      AS nlp_chronic_thyroiditis,
      LOGICAL_OR(nlp_de_quervains_thyroiditis OR nlp_granulomatous_thyroiditis) AS nlp_dequervain_granulomatous,
      LOGICAL_OR(nlp_autoimmune_thyroiditis)                                   AS nlp_autoimmune_thyroiditis,
      LOGICAL_OR(nlp_riedels_thyroiditis)                                      AS nlp_riedels,
      LOGICAL_OR(nlp_chronic_inflammation)                                     AS nlp_chronic_inflammation,
      LOGICAL_OR(nlp_cystic_change)                                            AS nlp_cystic_change,
      LOGICAL_OR(nlp_c_cell_hyperplasia)                                       AS nlp_c_cell_hyperplasia,
      LOGICAL_OR(nlp_hurthle_cell_change)                                      AS nlp_hurthle_change,
      LOGICAL_OR(nlp_hurthle_cell_metaplasia)                                  AS nlp_hurthle_metaplasia,
      LOGICAL_OR(nlp_hurthle_cell_nodule)                                      AS nlp_hurthle_nodule,
      LOGICAL_OR(nlp_follicular_nodule)                                        AS nlp_follicular_nodule,
      LOGICAL_OR(nlp_hyperplastic_nodules)                                     AS nlp_hyperplastic_nodules,
      LOGICAL_OR(nlp_adenomatoid_nodule)                                       AS nlp_adenomatoid_nodule,
      LOGICAL_OR(nlp_colloid_nodule)                                           AS nlp_colloid_nodule,
      LOGICAL_OR(nlp_colloid_cyst)                                             AS nlp_colloid_cyst,
      LOGICAL_OR(nlp_graves OR nlp_graves_disease)                             AS nlp_graves,
      LOGICAL_OR(nlp_thyroglossal_duct_cyst)                                   AS nlp_thyroglossal_duct_cyst
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_events_v1`
    GROUP BY research_id
  ),

  -- ── max malignant tumor size per patient (for microcarcinoma threshold) ─────
  -- Threshold: <1.0 cm = incidental microcarcinoma; >=1.0 cm = frank malignancy
  max_tumor AS (
    SELECT
      research_id,
      MAX(tumor_size_cm_per_surgery) AS max_malignant_tumor_size_cm
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_events_v1`
    GROUP BY research_id
  ),

  -- ── indeterminate-event sentinel (EXISTS pattern via DISTINCT) ───────────────
  has_indeterminate AS (
    SELECT DISTINCT research_id
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_indeterminate_events_v1`
  ),

  -- ── worst aggressive variant per patient across all malignant events ─────────
  -- Priority: aggressive PTC variants (tall cell > diffuse sclerosing > …) then
  -- oncocytic/hurthle, then microcarcinoma last (lowest priority).
  malignant_variant_rollup AS (
    SELECT
      research_id,
      ARRAY_AGG(
        histology_variant IGNORE NULLS
        ORDER BY CASE histology_variant
          WHEN 'tall cell'          THEN 1
          WHEN 'diffuse sclerosing' THEN 2
          WHEN 'hobnail'            THEN 3
          WHEN 'insular'            THEN 4
          WHEN 'solid'              THEN 5
          WHEN 'columnar'           THEN 6
          WHEN 'cribriform morular' THEN 7
          WHEN 'warthin-like'       THEN 8
          WHEN 'occult sclerosing'  THEN 9
          WHEN 'sclerosing variant' THEN 10
          WHEN 'oncocytic/hurthle'  THEN 20
          WHEN 'oxyphilic'          THEN 21
          WHEN 'microcarcinoma'     THEN 90
          ELSE                           50
        END
        LIMIT 1
      )[SAFE_OFFSET(0)]                                                  AS worst_variant
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_events_v1`
    GROUP BY research_id
  )

-- ── main projection ──────────────────────────────────────────────────────────
SELECT
  cpm.research_id,

  -- ── §A Demographics ─────────────────────────────────────────────────────────
  cpm.age_at_surgery,
  cpm.sex,
  cpm.bmi_combined,

  -- ── §B Race bucket (5-level, per H2 v3 Table 1) ─────────────────────────────
  -- Matches MAIN_RACES + figure ORDER in run_h2_v3_analysis.py.
  -- "Unknown or Not Reported" also catches NULL (COALESCE to sentinel string).
  CASE
    WHEN cpm.race = 'Black or African American'      THEN 'Black/AA'
    WHEN cpm.race = 'White'                           THEN 'White'
    WHEN cpm.race = 'Asian'                           THEN 'Asian'
    WHEN cpm.race IS NULL
      OR cpm.race = 'Unknown or Not Reported'         THEN 'Unknown'
    ELSE                                                   'Other'
  END                                                                    AS race_bucket,

  -- ── §C NLP benign flags (29 patient-level rollups) ──────────────────────────
  -- NULL rows from LEFT JOIN coalesced to FALSE (no benign events = no flag).
  COALESCE(bn.nlp_atypical_adenoma,                FALSE) AS nlp_atypical_adenoma,
  COALESCE(bn.nlp_hyperplasia,                     FALSE) AS nlp_hyperplasia,
  COALESCE(bn.nlp_substernal_mng,                  FALSE) AS nlp_substernal_mng,
  COALESCE(bn.nlp_adenomatous_hyperplasia,          FALSE) AS nlp_adenomatous_hyperplasia,
  COALESCE(bn.nlp_papillary_hyperplasia,            FALSE) AS nlp_papillary_hyperplasia,
  COALESCE(bn.nlp_hurthle_adenoma,                 FALSE) AS nlp_hurthle_adenoma,
  COALESCE(bn.nlp_follicular_adenoma,              FALSE) AS nlp_follicular_adenoma,
  COALESCE(bn.nlp_hyalinizing_trabecular,          FALSE) AS nlp_hyalinizing_trabecular,
  COALESCE(bn.nlp_lymphocytic_thyroiditis,         FALSE) AS nlp_lymphocytic_thyroiditis,
  COALESCE(bn.nlp_chronic_lymphocytic_thyroiditis, FALSE) AS nlp_chronic_lymphocytic_thyroiditis,
  COALESCE(bn.nlp_hashimotos,                      FALSE) AS nlp_hashimotos,
  COALESCE(bn.nlp_palpation_thyroiditis,           FALSE) AS nlp_palpation_thyroiditis,
  COALESCE(bn.nlp_chronic_thyroiditis,             FALSE) AS nlp_chronic_thyroiditis,
  COALESCE(bn.nlp_dequervain_granulomatous,        FALSE) AS nlp_dequervain_granulomatous,
  COALESCE(bn.nlp_autoimmune_thyroiditis,          FALSE) AS nlp_autoimmune_thyroiditis,
  COALESCE(bn.nlp_riedels,                         FALSE) AS nlp_riedels,
  COALESCE(bn.nlp_chronic_inflammation,            FALSE) AS nlp_chronic_inflammation,
  COALESCE(bn.nlp_cystic_change,                   FALSE) AS nlp_cystic_change,
  COALESCE(bn.nlp_c_cell_hyperplasia,              FALSE) AS nlp_c_cell_hyperplasia,
  COALESCE(bn.nlp_hurthle_change,                  FALSE) AS nlp_hurthle_change,
  COALESCE(bn.nlp_hurthle_metaplasia,              FALSE) AS nlp_hurthle_metaplasia,
  COALESCE(bn.nlp_hurthle_nodule,                  FALSE) AS nlp_hurthle_nodule,
  COALESCE(bn.nlp_follicular_nodule,               FALSE) AS nlp_follicular_nodule,
  COALESCE(bn.nlp_hyperplastic_nodules,            FALSE) AS nlp_hyperplastic_nodules,
  COALESCE(bn.nlp_adenomatoid_nodule,              FALSE) AS nlp_adenomatoid_nodule,
  COALESCE(bn.nlp_colloid_nodule,                  FALSE) AS nlp_colloid_nodule,
  COALESCE(bn.nlp_colloid_cyst,                    FALSE) AS nlp_colloid_cyst,
  COALESCE(bn.nlp_graves,                          FALSE) AS nlp_graves,
  COALESCE(bn.nlp_thyroglossal_duct_cyst,          FALSE) AS nlp_thyroglossal_duct_cyst,

  -- ── §D Composite flags ───────────────────────────────────────────────────────
  -- chronic_thyroiditis_composite: any of the four Hashimoto/lymphocytic/chronic
  -- entities (NLP only; no PMH input — PMH covered by pmh_autoimmune_thyroid_hx).
  (   COALESCE(bn.nlp_hashimotos,                      FALSE)
   OR COALESCE(bn.nlp_lymphocytic_thyroiditis,         FALSE)
   OR COALESCE(bn.nlp_chronic_lymphocytic_thyroiditis, FALSE)
   OR COALESCE(bn.nlp_chronic_thyroiditis,             FALSE)
  )                                                                      AS chronic_thyroiditis_composite,

  -- graves_composite: NLP graves flag OR PMH hyperthyroidism_definitive.
  -- Phase 2 will additionally incorporate canonical_path_benign_overrides_v1
  -- manual-x flags; graves_manual_only_flag is NULL placeholder until then.
  (   COALESCE(bn.nlp_graves,                          FALSE)
   OR COALESCE(pmh.pmh_hyperthyroidism_definitive,     FALSE)
  )                                                                      AS graves_composite,

  -- Phase 2 placeholder (populated from canonical_path_benign_overrides_v1).
  CAST(NULL AS BOOL)                                                     AS graves_manual_only_flag,

  -- ── §E Pathology outcome class (4-level, first-match priority) ───────────────
  -- frank_malignancy              : is_malignant AND max tumor >= 1.0 cm
  -- benign_plus_incidental_microcarcinoma : is_malignant AND max tumor <  1.0 cm
  --   (includes NULL size — conservative: size unknown treated as microcarcinoma)
  -- indeterminate                 : NOT is_malignant AND has indeterminate event
  -- pure_benign                   : NOT is_malignant, no indeterminate event
  CASE
    WHEN cpm.is_malignant = TRUE
         AND COALESCE(mt.max_malignant_tumor_size_cm, 0.0) >= 1.0
      THEN 'frank_malignancy'
    WHEN cpm.is_malignant = TRUE
         AND (mt.max_malignant_tumor_size_cm IS NULL
              OR mt.max_malignant_tumor_size_cm < 1.0)
      THEN 'benign_plus_incidental_microcarcinoma'
    WHEN cpm.is_malignant = FALSE
         AND hi.research_id IS NOT NULL
      THEN 'indeterminate'
    ELSE 'pure_benign'
  END                                                                    AS pathology_outcome_class,

  -- ── §F Dominant malignant group (8-level enum; NULL for non-malignant) ───────
  -- Source: canonical_path_malignant_patient_rollup_v1.dominant_histology +
  --         worst aggressive variant from canonical_path_malignant_events_v1.
  -- Hurthle_oncocytic captures FTC with oncocytic/hurthle variant (standard
  -- Hurthle cell carcinoma classification per WHO 2022).
  CASE
    WHEN LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%ptc%'
      OR LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%papillary%'
      THEN CASE
             WHEN mv.worst_variant IN (
               'tall cell', 'diffuse sclerosing', 'hobnail', 'columnar',
               'insular', 'solid', 'cribriform morular', 'warthin-like',
               'occult sclerosing', 'sclerosing variant'
             ) THEN 'PTC_variants'
             ELSE 'PTC_classical'
           END
    WHEN LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%follicular carc%'
      THEN CASE
             WHEN mv.worst_variant IN ('oncocytic/hurthle', 'oxyphilic')
               THEN 'Hurthle_oncocytic'
             ELSE 'FTC'
           END
    WHEN LOWER(COALESCE(mr.dominant_histology, '')) = 'mtc'
      OR LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%medullary%'
      THEN 'MTC'
    WHEN LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%poorly diff%'
      OR LOWER(COALESCE(mr.dominant_histology, '')) = 'pdtc'
      OR LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%differentiated high grade%'
      THEN 'PDTC'
    WHEN LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%anaplastic%'
      THEN 'ATC'
    WHEN LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%ftump%'
      OR LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%niftp%'
      OR LOWER(COALESCE(mr.dominant_histology, '')) LIKE '%wdt-ump%'
      THEN 'Indeterminate_NIFTP_FTUMP_WDTUMP'
    WHEN mr.dominant_histology IS NOT NULL
      THEN 'PTC_classical'  -- fallback for DTC_NOS / unmapped malignant rows
    ELSE NULL               -- pure_benign / indeterminate patients
  END                                                                    AS dominant_malignant_group,

  -- ── §G is_malignant passthrough ──────────────────────────────────────────────
  cpm.is_malignant,

  -- ── §H PMH covariates (_definitive tier; exceptions noted) ──────────────────
  -- pmh_radiation_exposure and pmh_family_hx_thyroid use _any_evidence per spec
  -- (NLP under-captures these at _definitive tier per mig_265 / CF-mig261c/d/e).
  COALESCE(pmh.pmh_diabetes_definitive,              FALSE) AS pmh_diabetes,
  COALESCE(pmh.pmh_hypertension_definitive,          FALSE) AS pmh_hypertension,
  COALESCE(pmh.pmh_obesity_definitive,               FALSE) AS pmh_obesity,
  COALESCE(pmh.pmh_ckd_definitive,                   FALSE) AS pmh_ckd,
  COALESCE(pmh.pmh_cad_definitive,                   FALSE) AS pmh_cad,
  COALESCE(pmh.pmh_radiation_exposure_any_evidence,  FALSE) AS pmh_radiation_exposure,
  COALESCE(pmh.pmh_family_hx_thyroid_any_evidence,   FALSE) AS pmh_family_hx_thyroid,
  COALESCE(pmh.pmh_prior_cancer_hx_definitive,       FALSE) AS pmh_prior_cancer_hx,
  COALESCE(pmh.pmh_autoimmune_thyroid_hx_definitive, FALSE) AS pmh_autoimmune_thyroid_hx,
  pmh.pmh_smoking_status_current,
  pmh.pmh_smoking_status_former,
  pmh.pmh_smoking_status_never,

  -- ── §I Preop workup (CPM columns) ───────────────────────────────────────────
  cpm.n_us_exams,
  cpm.n_fna_episodes,
  cpm.prm_first_fna_days_from_surg,
  cpm.bethesda_num                                                       AS bethesda_final,
  cpm.worst_bethesda_num,
  cpm.molecular_tested_confirmed,
  cpm.mol_has_thyroseq,
  cpm.mol_has_afirma,

  -- ── §J Surgical / gland ──────────────────────────────────────────────────────
  cpm.gland_weight_final_g,
  -- any_substernal_extension mirrors H2 v3 analysis "any_substernal" column:
  --   CT substernal extension OR MRI substernal (either modality).
  (COALESCE(cpm.ct_substernal_extension_any, FALSE)
   OR COALESCE(cpm.mri_substernal_any, FALSE))                          AS any_substernal_extension,
  cpm.surg_procedure_type

FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` cpm

-- benign NLP rollup (LEFT JOIN: patients with no benign events yield all FALSE)
LEFT JOIN benign_nlp_rollup bn
  USING (research_id)

-- max malignant tumor size (LEFT JOIN: benign-only patients have no rows)
LEFT JOIN max_tumor mt
  USING (research_id)

-- indeterminate sentinel (LEFT JOIN: NULL research_id = no indeterminate event)
LEFT JOIN has_indeterminate hi
  USING (research_id)

-- malignant patient rollup (LEFT JOIN: benign-only patients have no rows)
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_patient_rollup_v1` mr
  USING (research_id)

-- worst variant per patient (LEFT JOIN)
LEFT JOIN malignant_variant_rollup mv
  USING (research_id)

-- PMH rollup (LEFT JOIN: patients with no PMH events yield all NULLs / FALSE)
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_pmh_patient_rollup_v1` pmh
  USING (research_id)

-- Cohort filter: H2 = multinodular goiter surgical patients only
WHERE cpm.syn_multinodular_goiter = TRUE;


-- =============================================================================
-- §2 TABLE shell: h2_path_reconciliation_candidates_v1
-- Audit anchor: DFL-20260507-H2-RECONCILE-CANDIDATES / THY-33
--
-- Schema is pre-created empty.  Phase 2 INSERT (commented out below) runs
-- after Logan uploads pub_workspace.h2_manual_path_flags_v1 as a CSV
-- (research_id + 30 binary cols, no PHI).
--
-- Expected: ~25 unique research_ids across 13 discrepant categories.
-- =============================================================================

CREATE TABLE IF NOT EXISTS
  `thyroid-canonical-pub-2026.pub_workspace.h2_path_reconciliation_candidates_v1`
(
  research_id      STRING  NOT NULL
    OPTIONS(description = 'De-identified patient key (no MRN / name / DOB)'),
  category         STRING  NOT NULL
    OPTIONS(description = 'One of the 29 NLP flag column names from cohort_h2_pathology_outcome_v1'),
  manual_flag      BOOL    NOT NULL
    OPTIONS(description = 'Manual classification assigned by Logan during pathology review'),
  nlp_flag         BOOL    NOT NULL
    OPTIONS(description = 'NLP-derived flag rolled up from canonical_path_benign_events_v1'),
  discrepancy_type STRING  NOT NULL
    OPTIONS(description = 'NLP_ONLY (NLP=TRUE, manual=FALSE) or MANUAL_ONLY (manual=TRUE, NLP=FALSE)')
)
OPTIONS(
  description = 'H2 pathology NLP-vs-manual reconciliation discrepancy rows (mig_335, 2026-05-08). '
                'Populated via Phase 2 INSERT after h2_manual_path_flags_v1 CSV is loaded. '
                'Audit anchors: DFL-20260507-H2-RECONCILE-CANDIDATES / THY-33.'
);


-- =============================================================================
-- §3 Phase 2 INSERT — run after h2_manual_path_flags_v1 is loaded
-- (commented out; un-comment and execute once CSV is loaded by Logan)
-- =============================================================================
--
-- INSERT INTO `thyroid-canonical-pub-2026.pub_workspace.h2_path_reconciliation_candidates_v1`
-- WITH
--   manual_long AS (
--     SELECT research_id, category, manual_flag
--     FROM `thyroid-canonical-pub-2026.pub_workspace.h2_manual_path_flags_v1`
--     UNPIVOT (manual_flag FOR category IN (
--       nlp_atypical_adenoma, nlp_hyperplasia, nlp_substernal_mng,
--       nlp_adenomatous_hyperplasia, nlp_papillary_hyperplasia,
--       nlp_hurthle_adenoma, nlp_follicular_adenoma, nlp_hyalinizing_trabecular,
--       nlp_lymphocytic_thyroiditis, nlp_chronic_lymphocytic_thyroiditis,
--       nlp_hashimotos, nlp_palpation_thyroiditis, nlp_chronic_thyroiditis,
--       nlp_dequervain_granulomatous, nlp_autoimmune_thyroiditis, nlp_riedels,
--       nlp_chronic_inflammation, nlp_cystic_change, nlp_c_cell_hyperplasia,
--       nlp_hurthle_change, nlp_hurthle_metaplasia, nlp_hurthle_nodule,
--       nlp_follicular_nodule, nlp_hyperplastic_nodules, nlp_adenomatoid_nodule,
--       nlp_colloid_nodule, nlp_colloid_cyst, nlp_graves, nlp_thyroglossal_duct_cyst
--     ))
--   ),
--   nlp_long AS (
--     SELECT research_id, category, nlp_flag
--     FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`
--     UNPIVOT (nlp_flag FOR category IN (
--       nlp_atypical_adenoma, nlp_hyperplasia, nlp_substernal_mng,
--       nlp_adenomatous_hyperplasia, nlp_papillary_hyperplasia,
--       nlp_hurthle_adenoma, nlp_follicular_adenoma, nlp_hyalinizing_trabecular,
--       nlp_lymphocytic_thyroiditis, nlp_chronic_lymphocytic_thyroiditis,
--       nlp_hashimotos, nlp_palpation_thyroiditis, nlp_chronic_thyroiditis,
--       nlp_dequervain_granulomatous, nlp_autoimmune_thyroiditis, nlp_riedels,
--       nlp_chronic_inflammation, nlp_cystic_change, nlp_c_cell_hyperplasia,
--       nlp_hurthle_change, nlp_hurthle_metaplasia, nlp_hurthle_nodule,
--       nlp_follicular_nodule, nlp_hyperplastic_nodules, nlp_adenomatoid_nodule,
--       nlp_colloid_nodule, nlp_colloid_cyst, nlp_graves, nlp_thyroglossal_duct_cyst
--     ))
--   )
-- SELECT
--   m.research_id,
--   m.category,
--   m.manual_flag,
--   COALESCE(n.nlp_flag, FALSE) AS nlp_flag,
--   CASE
--     WHEN m.manual_flag = TRUE  AND NOT COALESCE(n.nlp_flag, FALSE) THEN 'MANUAL_ONLY'
--     WHEN m.manual_flag = FALSE AND COALESCE(n.nlp_flag, FALSE)     THEN 'NLP_ONLY'
--   END AS discrepancy_type
-- FROM manual_long m
-- LEFT JOIN nlp_long n USING (research_id, category)
-- WHERE m.manual_flag != COALESCE(n.nlp_flag, FALSE);


-- =============================================================================
-- §4 Post-apply verification
-- =============================================================================
--
-- Primary row counts:
--   SELECT COUNT(*) AS n_total, COUNTIF(is_malignant) AS n_malignant
--   FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`;
--   -- expect: n_total=6075, n_malignant~1528
--
-- Race breakdown (matches H2 v3 Table 1):
--   SELECT race_bucket, COUNT(*) AS cnt
--   FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`
--   GROUP BY 1 ORDER BY cnt DESC;
--   -- expect: Black/AA~2918, White~2500, Unknown~359, Asian~193, Other~105
--
-- Outcome class distribution:
--   SELECT pathology_outcome_class, COUNT(*) AS cnt
--   FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`
--   GROUP BY 1;
--   -- expect: pure_benign~4500, benign_plus_incidental_microcarcinoma~1000,
--   --         frank_malignancy~400+, indeterminate~140
--
-- Dominant malignant group (malignant patients only):
--   SELECT dominant_malignant_group, COUNT(*) AS cnt
--   FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`
--   WHERE is_malignant = TRUE GROUP BY 1 ORDER BY cnt DESC;
--
-- h2_path_reconciliation_candidates_v1 shell (should be empty until Phase 2):
--   SELECT COUNT(*) AS n_rows
--   FROM `thyroid-canonical-pub-2026.pub_workspace.h2_path_reconciliation_candidates_v1`;
--   -- expect: 0 rows in Phase 1


-- =============================================================================
-- §5 signoff_migration row (apply after verification PASS)
-- =============================================================================
-- INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.signoff_migration`
--   (migration_id, target_table, migration_date, applied_by,
--    verification_status, verification_notes)
-- VALUES (
--   'mig_335',
--   'pub_workspace.cohort_h2_pathology_outcome_v1 + pub_workspace.h2_path_reconciliation_candidates_v1',
--   DATE '2026-05-08',
--   'Cowork',
--   'PASS',
--   'n_total=6075 MATCH; n_malignant=<fill>; race breakdown matches H2 v3 Table 1; '
--   'outcome classes: pure_benign=<fill>, microcarcinoma=<fill>, frank=<fill>, indeterminate=<fill>; '
--   'reconciliation_candidates table shell empty (Phase 2 pending CSV upload). '
--   'Anchors: DFL-20260507-H2-PATHCLASS-DERIVE, DFL-20260507-H2-RECONCILE-CANDIDATES, THY-33.'
-- );


-- =============================================================================
-- End mig_335
-- =============================================================================
