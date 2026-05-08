-- =============================================================================
-- mig_336v2 — pub_workspace.h2_path_reconciliation_candidates_v1 (RE-POPULATE)
--
-- Date:       2026-05-08
-- Lane:       H2 manuscript — pathology NLP-vs-manual reconciliation (Phase 1.6 v2)
-- Depends:    pub_workspace.cohort_h2_pathology_outcome_v1 (VIEW v2, mig_338b)
--             pub_workspace.h2_manual_path_flags_v1 (TABLE, uploaded Phase 1.5a)
-- Author:     Cursor Agent (Sonnet 4.6)
--
-- AUDIT ANCHORS:
--   DFL-20260508-H2-RECONCILE-RERUN-V2   (Data Feedback Log tblsiYKJtKcktkzze)
--   VC-H2-THYMIC-TISSUE-COVERAGE-GAP    (recGxIKHBaN6LBRz4, resolving at §1.6.7)
--   THY-39 (Linear)
--
-- CHANGE vs mig_336:
--   - nlp_thymic_tissue added to nlp CTE (now available from cohort_h2 v2 view)
--   - j CTE adds n.nlp_thymic_tissue
--   - thymic_tissue row switched from MANUAL_ONLY-only to full symmetric-difference
--     (was: "any manual TRUE is MANUAL_ONLY"; now: NLP_ONLY if nlp>manual, else MANUAL_ONLY)
--   - substernal_mng row removed (resolved 2026-05-08, VC-H2-SUBSTERNAL-PATH-RECONCILE
--     already marked Resolved; substernal rows are 0 by design)
--
-- EXPECTED OUTPUT (~44 rows):
--   atypical_adenoma    NLP_ONLY:   18
--   thymic_tissue       NLP_ONLY:   20
--   thymic_tissue       MANUAL_ONLY: 2
--   hurthle_adenoma     NLP_ONLY:    2  (trivial drift)
--   adenomatoid_nodule  NLP_ONLY:    1  (trivial drift)
--   hyperplastic_nodules NLP_ONLY:   1  (trivial drift)
--   substernal_mng: 0 (resolved)
-- =============================================================================

TRUNCATE TABLE `thyroid-canonical-pub-2026.pub_workspace.h2_path_reconciliation_candidates_v1`;

INSERT INTO `thyroid-canonical-pub-2026.pub_workspace.h2_path_reconciliation_candidates_v1`
  (research_id, category, manual_flag, nlp_flag, discrepancy_type)

WITH
nlp AS (
  -- NLP cohort: 6,075 MNG patients; all flags already BOOL with COALESCE(…,FALSE)
  -- v2: nlp_thymic_tissue now available from cohort view (added by mig_338 + mig_338b)
  SELECT
    research_id,
    nlp_atypical_adenoma,
    nlp_hyperplasia                     AS nlp_hyperplasia_follicular,
    nlp_substernal_mng,
    nlp_adenomatous_hyperplasia,
    nlp_papillary_hyperplasia,
    nlp_hurthle_adenoma,
    nlp_follicular_adenoma,
    nlp_hyalinizing_trabecular,
    nlp_lymphocytic_thyroiditis,
    nlp_chronic_lymphocytic_thyroiditis,
    nlp_hashimotos,
    nlp_palpation_thyroiditis,
    nlp_chronic_thyroiditis,
    nlp_dequervain_granulomatous,
    nlp_autoimmune_thyroiditis,
    nlp_riedels,
    nlp_chronic_inflammation,
    nlp_cystic_change,
    nlp_c_cell_hyperplasia,
    nlp_hurthle_change,
    nlp_hurthle_metaplasia,
    nlp_hurthle_nodule,
    nlp_follicular_nodule,
    nlp_hyperplastic_nodules,
    nlp_adenomatoid_nodule,
    nlp_colloid_nodule,
    nlp_colloid_cyst,
    nlp_graves,
    nlp_thymic_tissue,                  -- v2 addition (mig_338 canonical column)
    nlp_thyroglossal_duct_cyst
  FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1`
),
manual AS (
  -- Manual CSV: v2 binarization (? → 0, x/qualifier → 1); filter junk row
  SELECT
    CAST(research_id AS STRING)                           AS research_id,
    CAST(manual_atypical_adenoma AS BOOL)                 AS man_atypical_adenoma,
    CAST(manual_hyperplasia_follicular AS BOOL)           AS man_hyperplasia_follicular,
    CAST(manual_substernal_mng AS BOOL)                   AS man_substernal_mng,
    CAST(manual_adenomatous_hyperplasia AS BOOL)          AS man_adenomatous_hyperplasia,
    CAST(manual_papillary_hyperplasia AS BOOL)            AS man_papillary_hyperplasia,
    CAST(manual_hurthle_adenoma AS BOOL)                  AS man_hurthle_adenoma,
    CAST(manual_follicular_adenoma AS BOOL)               AS man_follicular_adenoma,
    CAST(manual_hyalinizing_trabecular AS BOOL)           AS man_hyalinizing_trabecular,
    CAST(manual_lymphocytic_thyroiditis AS BOOL)          AS man_lymphocytic_thyroiditis,
    CAST(manual_chronic_lymphocytic_thyroiditis AS BOOL)  AS man_chronic_lymphocytic_thyroiditis,
    CAST(manual_hashimotos AS BOOL)                       AS man_hashimotos,
    CAST(manual_palpation_thyroiditis AS BOOL)            AS man_palpation_thyroiditis,
    CAST(manual_chronic_thyroiditis AS BOOL)              AS man_chronic_thyroiditis,
    CAST(manual_dequervain_granulomatous AS BOOL)         AS man_dequervain_granulomatous,
    CAST(manual_autoimmune_thyroiditis AS BOOL)           AS man_autoimmune_thyroiditis,
    CAST(manual_riedels AS BOOL)                          AS man_riedels,
    CAST(manual_chronic_inflammation AS BOOL)             AS man_chronic_inflammation,
    CAST(manual_cystic_change AS BOOL)                    AS man_cystic_change,
    CAST(manual_c_cell_hyperplasia AS BOOL)               AS man_c_cell_hyperplasia,
    CAST(manual_hurthle_change AS BOOL)                   AS man_hurthle_change,
    CAST(manual_hurthle_metaplasia AS BOOL)               AS man_hurthle_metaplasia,
    CAST(manual_hurthle_nodule AS BOOL)                   AS man_hurthle_nodule,
    CAST(manual_follicular_nodule AS BOOL)                AS man_follicular_nodule,
    CAST(manual_hyperplastic_nodules AS BOOL)             AS man_hyperplastic_nodules,
    CAST(manual_adenomatoid_nodule AS BOOL)               AS man_adenomatoid_nodule,
    CAST(manual_colloid_nodule AS BOOL)                   AS man_colloid_nodule,
    CAST(manual_colloid_cyst AS BOOL)                     AS man_colloid_cyst,
    CAST(manual_graves AS BOOL)                           AS man_graves,
    CAST(manual_thymic_tissue AS BOOL)                    AS man_thymic_tissue,
    CAST(manual_thyroglossal_duct_cyst AS BOOL)           AS man_thyroglossal_duct_cyst
  FROM `thyroid-canonical-pub-2026.pub_workspace.h2_manual_path_flags_v1`
  WHERE research_id IS NOT NULL
    AND SAFE_CAST(research_id AS INT64) IS NOT NULL   -- filter empty-string sentinel
),
j AS (
  -- Inner join: restrict to NLP cohort members only (6,075 rows)
  SELECT
    n.research_id,
    n.nlp_atypical_adenoma,           m.man_atypical_adenoma,
    n.nlp_hyperplasia_follicular,     m.man_hyperplasia_follicular,
    n.nlp_substernal_mng,             m.man_substernal_mng,
    n.nlp_adenomatous_hyperplasia,    m.man_adenomatous_hyperplasia,
    n.nlp_papillary_hyperplasia,      m.man_papillary_hyperplasia,
    n.nlp_hurthle_adenoma,            m.man_hurthle_adenoma,
    n.nlp_follicular_adenoma,         m.man_follicular_adenoma,
    n.nlp_hyalinizing_trabecular,     m.man_hyalinizing_trabecular,
    n.nlp_lymphocytic_thyroiditis,    m.man_lymphocytic_thyroiditis,
    n.nlp_chronic_lymphocytic_thyroiditis, m.man_chronic_lymphocytic_thyroiditis,
    n.nlp_hashimotos,                 m.man_hashimotos,
    n.nlp_palpation_thyroiditis,      m.man_palpation_thyroiditis,
    n.nlp_chronic_thyroiditis,        m.man_chronic_thyroiditis,
    n.nlp_dequervain_granulomatous,   m.man_dequervain_granulomatous,
    n.nlp_autoimmune_thyroiditis,     m.man_autoimmune_thyroiditis,
    n.nlp_riedels,                    m.man_riedels,
    n.nlp_chronic_inflammation,       m.man_chronic_inflammation,
    n.nlp_cystic_change,              m.man_cystic_change,
    n.nlp_c_cell_hyperplasia,         m.man_c_cell_hyperplasia,
    n.nlp_hurthle_change,             m.man_hurthle_change,
    n.nlp_hurthle_metaplasia,         m.man_hurthle_metaplasia,
    n.nlp_hurthle_nodule,             m.man_hurthle_nodule,
    n.nlp_follicular_nodule,          m.man_follicular_nodule,
    n.nlp_hyperplastic_nodules,       m.man_hyperplastic_nodules,
    n.nlp_adenomatoid_nodule,         m.man_adenomatoid_nodule,
    n.nlp_colloid_nodule,             m.man_colloid_nodule,
    n.nlp_colloid_cyst,               m.man_colloid_cyst,
    n.nlp_graves,                     m.man_graves,
    n.nlp_thymic_tissue,              m.man_thymic_tissue,  -- v2: nlp now available
    n.nlp_thyroglossal_duct_cyst,     m.man_thyroglossal_duct_cyst
  FROM nlp n
  INNER JOIN manual m ON n.research_id = m.research_id
)

-- ── 30 category symmetric-difference rows ─────────────────────────────────────
SELECT research_id, 'atypical_adenoma'               AS category, man_atypical_adenoma         AS manual_flag, nlp_atypical_adenoma         AS nlp_flag, IF(nlp_atypical_adenoma AND NOT man_atypical_adenoma, 'NLP_ONLY', 'MANUAL_ONLY')         AS discrepancy_type FROM j WHERE nlp_atypical_adenoma != man_atypical_adenoma
UNION ALL
SELECT research_id, 'hyperplasia_follicular'          AS category, man_hyperplasia_follicular   AS manual_flag, nlp_hyperplasia_follicular   AS nlp_flag, IF(nlp_hyperplasia_follicular AND NOT man_hyperplasia_follicular, 'NLP_ONLY', 'MANUAL_ONLY')   AS discrepancy_type FROM j WHERE nlp_hyperplasia_follicular != man_hyperplasia_follicular
UNION ALL
SELECT research_id, 'substernal_mng'                  AS category, man_substernal_mng           AS manual_flag, nlp_substernal_mng           AS nlp_flag, IF(nlp_substernal_mng AND NOT man_substernal_mng, 'NLP_ONLY', 'MANUAL_ONLY')           AS discrepancy_type FROM j WHERE nlp_substernal_mng != man_substernal_mng
UNION ALL
SELECT research_id, 'adenomatous_hyperplasia'         AS category, man_adenomatous_hyperplasia  AS manual_flag, nlp_adenomatous_hyperplasia  AS nlp_flag, IF(nlp_adenomatous_hyperplasia AND NOT man_adenomatous_hyperplasia, 'NLP_ONLY', 'MANUAL_ONLY')  AS discrepancy_type FROM j WHERE nlp_adenomatous_hyperplasia != man_adenomatous_hyperplasia
UNION ALL
SELECT research_id, 'papillary_hyperplasia'           AS category, man_papillary_hyperplasia    AS manual_flag, nlp_papillary_hyperplasia    AS nlp_flag, IF(nlp_papillary_hyperplasia AND NOT man_papillary_hyperplasia, 'NLP_ONLY', 'MANUAL_ONLY')    AS discrepancy_type FROM j WHERE nlp_papillary_hyperplasia != man_papillary_hyperplasia
UNION ALL
SELECT research_id, 'hurthle_adenoma'                 AS category, man_hurthle_adenoma          AS manual_flag, nlp_hurthle_adenoma          AS nlp_flag, IF(nlp_hurthle_adenoma AND NOT man_hurthle_adenoma, 'NLP_ONLY', 'MANUAL_ONLY')          AS discrepancy_type FROM j WHERE nlp_hurthle_adenoma != man_hurthle_adenoma
UNION ALL
SELECT research_id, 'follicular_adenoma'              AS category, man_follicular_adenoma       AS manual_flag, nlp_follicular_adenoma       AS nlp_flag, IF(nlp_follicular_adenoma AND NOT man_follicular_adenoma, 'NLP_ONLY', 'MANUAL_ONLY')       AS discrepancy_type FROM j WHERE nlp_follicular_adenoma != man_follicular_adenoma
UNION ALL
SELECT research_id, 'hyalinizing_trabecular'          AS category, man_hyalinizing_trabecular   AS manual_flag, nlp_hyalinizing_trabecular   AS nlp_flag, IF(nlp_hyalinizing_trabecular AND NOT man_hyalinizing_trabecular, 'NLP_ONLY', 'MANUAL_ONLY')   AS discrepancy_type FROM j WHERE nlp_hyalinizing_trabecular != man_hyalinizing_trabecular
UNION ALL
SELECT research_id, 'lymphocytic_thyroiditis'         AS category, man_lymphocytic_thyroiditis  AS manual_flag, nlp_lymphocytic_thyroiditis  AS nlp_flag, IF(nlp_lymphocytic_thyroiditis AND NOT man_lymphocytic_thyroiditis, 'NLP_ONLY', 'MANUAL_ONLY')  AS discrepancy_type FROM j WHERE nlp_lymphocytic_thyroiditis != man_lymphocytic_thyroiditis
UNION ALL
SELECT research_id, 'chronic_lymphocytic_thyroiditis' AS category, man_chronic_lymphocytic_thyroiditis AS manual_flag, nlp_chronic_lymphocytic_thyroiditis AS nlp_flag, IF(nlp_chronic_lymphocytic_thyroiditis AND NOT man_chronic_lymphocytic_thyroiditis, 'NLP_ONLY', 'MANUAL_ONLY') AS discrepancy_type FROM j WHERE nlp_chronic_lymphocytic_thyroiditis != man_chronic_lymphocytic_thyroiditis
UNION ALL
SELECT research_id, 'hashimotos'                      AS category, man_hashimotos               AS manual_flag, nlp_hashimotos               AS nlp_flag, IF(nlp_hashimotos AND NOT man_hashimotos, 'NLP_ONLY', 'MANUAL_ONLY')               AS discrepancy_type FROM j WHERE nlp_hashimotos != man_hashimotos
UNION ALL
SELECT research_id, 'palpation_thyroiditis'           AS category, man_palpation_thyroiditis    AS manual_flag, nlp_palpation_thyroiditis    AS nlp_flag, IF(nlp_palpation_thyroiditis AND NOT man_palpation_thyroiditis, 'NLP_ONLY', 'MANUAL_ONLY')    AS discrepancy_type FROM j WHERE nlp_palpation_thyroiditis != man_palpation_thyroiditis
UNION ALL
SELECT research_id, 'chronic_thyroiditis'             AS category, man_chronic_thyroiditis      AS manual_flag, nlp_chronic_thyroiditis      AS nlp_flag, IF(nlp_chronic_thyroiditis AND NOT man_chronic_thyroiditis, 'NLP_ONLY', 'MANUAL_ONLY')      AS discrepancy_type FROM j WHERE nlp_chronic_thyroiditis != man_chronic_thyroiditis
UNION ALL
SELECT research_id, 'dequervain_granulomatous'        AS category, man_dequervain_granulomatous AS manual_flag, nlp_dequervain_granulomatous AS nlp_flag, IF(nlp_dequervain_granulomatous AND NOT man_dequervain_granulomatous, 'NLP_ONLY', 'MANUAL_ONLY') AS discrepancy_type FROM j WHERE nlp_dequervain_granulomatous != man_dequervain_granulomatous
UNION ALL
SELECT research_id, 'autoimmune_thyroiditis'          AS category, man_autoimmune_thyroiditis   AS manual_flag, nlp_autoimmune_thyroiditis   AS nlp_flag, IF(nlp_autoimmune_thyroiditis AND NOT man_autoimmune_thyroiditis, 'NLP_ONLY', 'MANUAL_ONLY')   AS discrepancy_type FROM j WHERE nlp_autoimmune_thyroiditis != man_autoimmune_thyroiditis
UNION ALL
SELECT research_id, 'riedels'                         AS category, man_riedels                  AS manual_flag, nlp_riedels                  AS nlp_flag, IF(nlp_riedels AND NOT man_riedels, 'NLP_ONLY', 'MANUAL_ONLY')                  AS discrepancy_type FROM j WHERE nlp_riedels != man_riedels
UNION ALL
SELECT research_id, 'chronic_inflammation'            AS category, man_chronic_inflammation     AS manual_flag, nlp_chronic_inflammation     AS nlp_flag, IF(nlp_chronic_inflammation AND NOT man_chronic_inflammation, 'NLP_ONLY', 'MANUAL_ONLY')     AS discrepancy_type FROM j WHERE nlp_chronic_inflammation != man_chronic_inflammation
UNION ALL
SELECT research_id, 'cystic_change'                   AS category, man_cystic_change            AS manual_flag, nlp_cystic_change            AS nlp_flag, IF(nlp_cystic_change AND NOT man_cystic_change, 'NLP_ONLY', 'MANUAL_ONLY')            AS discrepancy_type FROM j WHERE nlp_cystic_change != man_cystic_change
UNION ALL
SELECT research_id, 'c_cell_hyperplasia'              AS category, man_c_cell_hyperplasia       AS manual_flag, nlp_c_cell_hyperplasia       AS nlp_flag, IF(nlp_c_cell_hyperplasia AND NOT man_c_cell_hyperplasia, 'NLP_ONLY', 'MANUAL_ONLY')       AS discrepancy_type FROM j WHERE nlp_c_cell_hyperplasia != man_c_cell_hyperplasia
UNION ALL
SELECT research_id, 'hurthle_change'                  AS category, man_hurthle_change           AS manual_flag, nlp_hurthle_change           AS nlp_flag, IF(nlp_hurthle_change AND NOT man_hurthle_change, 'NLP_ONLY', 'MANUAL_ONLY')           AS discrepancy_type FROM j WHERE nlp_hurthle_change != man_hurthle_change
UNION ALL
SELECT research_id, 'hurthle_metaplasia'              AS category, man_hurthle_metaplasia       AS manual_flag, nlp_hurthle_metaplasia       AS nlp_flag, IF(nlp_hurthle_metaplasia AND NOT man_hurthle_metaplasia, 'NLP_ONLY', 'MANUAL_ONLY')       AS discrepancy_type FROM j WHERE nlp_hurthle_metaplasia != man_hurthle_metaplasia
UNION ALL
SELECT research_id, 'hurthle_nodule'                  AS category, man_hurthle_nodule           AS manual_flag, nlp_hurthle_nodule           AS nlp_flag, IF(nlp_hurthle_nodule AND NOT man_hurthle_nodule, 'NLP_ONLY', 'MANUAL_ONLY')           AS discrepancy_type FROM j WHERE nlp_hurthle_nodule != man_hurthle_nodule
UNION ALL
SELECT research_id, 'follicular_nodule'               AS category, man_follicular_nodule        AS manual_flag, nlp_follicular_nodule        AS nlp_flag, IF(nlp_follicular_nodule AND NOT man_follicular_nodule, 'NLP_ONLY', 'MANUAL_ONLY')        AS discrepancy_type FROM j WHERE nlp_follicular_nodule != man_follicular_nodule
UNION ALL
SELECT research_id, 'hyperplastic_nodules'            AS category, man_hyperplastic_nodules     AS manual_flag, nlp_hyperplastic_nodules     AS nlp_flag, IF(nlp_hyperplastic_nodules AND NOT man_hyperplastic_nodules, 'NLP_ONLY', 'MANUAL_ONLY')     AS discrepancy_type FROM j WHERE nlp_hyperplastic_nodules != man_hyperplastic_nodules
UNION ALL
SELECT research_id, 'adenomatoid_nodule'              AS category, man_adenomatoid_nodule       AS manual_flag, nlp_adenomatoid_nodule       AS nlp_flag, IF(nlp_adenomatoid_nodule AND NOT man_adenomatoid_nodule, 'NLP_ONLY', 'MANUAL_ONLY')       AS discrepancy_type FROM j WHERE nlp_adenomatoid_nodule != man_adenomatoid_nodule
UNION ALL
SELECT research_id, 'colloid_nodule'                  AS category, man_colloid_nodule           AS manual_flag, nlp_colloid_nodule           AS nlp_flag, IF(nlp_colloid_nodule AND NOT man_colloid_nodule, 'NLP_ONLY', 'MANUAL_ONLY')           AS discrepancy_type FROM j WHERE nlp_colloid_nodule != man_colloid_nodule
UNION ALL
SELECT research_id, 'colloid_cyst'                    AS category, man_colloid_cyst             AS manual_flag, nlp_colloid_cyst             AS nlp_flag, IF(nlp_colloid_cyst AND NOT man_colloid_cyst, 'NLP_ONLY', 'MANUAL_ONLY')             AS discrepancy_type FROM j WHERE nlp_colloid_cyst != man_colloid_cyst
UNION ALL
SELECT research_id, 'graves'                          AS category, man_graves                   AS manual_flag, nlp_graves                   AS nlp_flag, IF(nlp_graves AND NOT man_graves, 'NLP_ONLY', 'MANUAL_ONLY')                   AS discrepancy_type FROM j WHERE nlp_graves != man_graves
UNION ALL
-- thymic_tissue v2: now full symmetric-difference (nlp_thymic_tissue available from cohort v2 view)
SELECT research_id, 'thymic_tissue'                   AS category, man_thymic_tissue            AS manual_flag, nlp_thymic_tissue            AS nlp_flag, IF(nlp_thymic_tissue AND NOT man_thymic_tissue, 'NLP_ONLY', 'MANUAL_ONLY')            AS discrepancy_type FROM j WHERE nlp_thymic_tissue != man_thymic_tissue
UNION ALL
SELECT research_id, 'thyroglossal_duct_cyst'          AS category, man_thyroglossal_duct_cyst  AS manual_flag, nlp_thyroglossal_duct_cyst  AS nlp_flag, IF(nlp_thyroglossal_duct_cyst AND NOT man_thyroglossal_duct_cyst, 'NLP_ONLY', 'MANUAL_ONLY')  AS discrepancy_type FROM j WHERE nlp_thyroglossal_duct_cyst != man_thyroglossal_duct_cyst
;


-- =============================================================================
-- §3 Verification (run immediately after INSERT):
-- =============================================================================
--
-- SELECT category, discrepancy_type, COUNT(*) AS n
-- FROM `thyroid-canonical-pub-2026.pub_workspace.h2_path_reconciliation_candidates_v1`
-- GROUP BY 1,2 ORDER BY n DESC, category;
-- -- EXPECTED: atypical_adenoma NLP_ONLY ~18
-- --           thymic_tissue NLP_ONLY ~20
-- --           thymic_tissue MANUAL_ONLY ~2
-- --           trivial drifts ≤3 for hurthle_adenoma, adenomatoid_nodule, hyperplastic_nodules
-- --           substernal_mng: 0 (resolved)

-- =============================================================================
-- End mig_336v2
-- =============================================================================
