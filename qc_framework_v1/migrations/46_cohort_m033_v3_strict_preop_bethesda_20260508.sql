-- ============================================================================
-- Migration 46 — M033 v3 cohort rebuilds with strict-preop Bethesda
-- ============================================================================
-- Target (3 tables):
--   thyroid-canonical-pub-2026.pub_workspace.cohort_m033_b3b4_v3          (PRIMARY)
--   thyroid-canonical-pub-2026.pub_workspace.cohort_m033_all_molecular_v3 (SENSITIVITY-1)
--   thyroid-canonical-pub-2026.pub_workspace.cohort_m033_b5b6_v3          (SENSITIVITY-2)
-- Author: Cursor session 2026-05-08 / Logan Glosser
-- Audit:  DFL-20260508-M033-V3-COHORT-B3B4-BUILD         (rec0EFYYHKQjcRSvF)
--         DFL-20260508-M033-V3-COHORT-ALL-MOLECULAR-BUILD (reclZdL7WMSoLjtWQ)
--         DFL-20260508-M033-V3-COHORT-B5B6-BUILD          (recWSTCMtlRTgYJ1M)
-- Depends on: 45b (canonical_fna_patient_rollup_v1_1) + 45c (master_v1_1)
-- Linear:  THY-48 sub-task B (M033 v3 cohort builds)
-- ----------------------------------------------------------------------------
-- Design
--   Mirror cohort_m033_afirma_thyroseq_v1 schema exactly, replacing
--   bethesda_final (legacy loose-window) with bethesda_final_strict_preop
--   as the primary Bethesda variable. Legacy value retained as
--   bethesda_final_legacy for cross-tab reproducibility.
--
--   Cohort entry criterion (Logan confirmed 2026-05-08):
--     PRIMARY: mol_has_afirma OR mol_has_thyroseq, AND strict-preop B3 or B4
--     SENSITIVITY-1: mol_has_afirma OR mol_has_thyroseq (all-molecular, N=969)
--     SENSITIVITY-2: mol_has_afirma OR mol_has_thyroseq, AND strict-preop B5 or B6
--
--   Pre-flight (verified 2026-05-08):
--     PRIMARY (B3/B4): N=520  (vs N=510 in v1 legacy — net +10 from strict-preop)
--     SENSITIVITY-1:  N=969  (same as v1; bethesda swap only)
--     SENSITIVITY-2:  N=214  (new cohort, no v1 equivalent)
--
-- Hard rules respected
--   * v1 preserved bit-for-bit (nothing deleted)
--   * DFLs pre-logged before any DDL ran
--   * No PHI: research_id only
-- ============================================================================

-- ============================================================================
-- Shared CTE (source columns from canonical_patient_master_v1_1)
-- ============================================================================
-- Note: canonical_patient_master_v1_1 is a VIEW joining v1 + fna_rollup_v1_1,
-- so bethesda_final_strict_preop / bethesda_final_strict_preop_max_2023 / etc.
-- are available. bethesda_num (the v1 legacy float field) maps to bethesda_final
-- (INT64) in the rollup.

-- ============================================================================
-- Table 1: PRIMARY — B3/B4 restricted (N=520)
-- ============================================================================
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.cohort_m033_b3b4_v3`
CLUSTER BY research_id
OPTIONS(
  description="M033 PRIMARY v3 cohort: Afirma/ThyroSeq-tested patients with strict-preop Bethesda III or IV. N=520 (vs N=510 in v1 legacy). Bethesda source: canonical_patient_master_v1_1.bethesda_final_strict_preop. Audit: DFL-20260508-M033-V3-COHORT-B3B4-BUILD. v1 preserved at cohort_m033_afirma_thyroseq_v1."
)
AS
SELECT
  CAST(m.research_id AS STRING)                        AS research_id,
  m.age_at_surgery,
  m.sex,
  -- Primary Bethesda (strict-preop, Logan-confirmed 2026-05-08)
  m.bethesda_final_strict_preop                        AS bethesda_final,
  m.bethesda_final_name                                AS bethesda_final_name,
  -- Legacy for cross-tab
  CAST(m.bethesda_num AS INT64)                        AS bethesda_final_legacy,
  m.bethesda_disagree_with_legacy_flag,
  -- Molecular platform fields (carried from v1)
  m.mol_platform                                       AS mol_platform,
  m.mol_has_afirma,
  m.mol_has_thyroseq,
  m.mol_first_test_date,
  m.mol_genes_list,
  m.mol_n_variants_total,
  m.mol_n_distinct_genes,
  m.mol_has_fusion,
  -- Molecular risk / variant flags
  CASE
    WHEN m.braf_positive_final THEN 'high'
    WHEN m.ras_positive_final  THEN 'intermediate'
    ELSE NULL
  END                                                  AS molecular_risk_tier,
  m.braf_positive_final,
  m.ras_positive_final,
  m.tert_positive_final,
  -- Pathology + outcomes
  m.histology_final,
  m.is_malignant,
  m.tumor_size_cm_dominant                             AS tumor_size_cm,
  m.ete_grade_final                                    AS ete_grade_final,
  m.surg_procedure_type,
  CASE WHEN LOWER(m.surg_procedure_type) LIKE '%total%' THEN TRUE ELSE FALSE END AS surg_total_thyroidectomy,
  CASE WHEN LOWER(m.surg_procedure_type) LIKE '%hemi%'
            OR LOWER(m.surg_procedure_type) LIKE '%lobect%' THEN TRUE ELSE FALSE END AS surg_hemithyroidectomy,
  m.ln_positive_flag,
  m.ajcc8_stage_group,
  m.ata_risk_category,
  m.any_recurrence_flag,
  m.followup_years,
  m.surg_first_date,
  -- Audit metadata
  'DFL-20260508-M033-V3-COHORT-B3B4-BUILD'            AS dfl_audit_id,
  CURRENT_TIMESTAMP()                                  AS v3_built_at
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master_v1_1` m
WHERE (m.mol_has_afirma IS TRUE OR m.mol_has_thyroseq IS TRUE)
  AND m.bethesda_final_strict_preop IN (3, 4);

-- ============================================================================
-- Contract validation: PRIMARY cohort
-- ============================================================================
SELECT
  'CONTRACT_m033_b3b4_v3' AS check_name,
  COUNT(*) AS n,
  COUNTIF(bethesda_final IN (3,4)) AS n_b3b4_check,
  COUNTIF(bethesda_final IS NULL) AS n_null_bethesda_check,
  COUNTIF(bethesda_final_legacy IS NOT NULL AND bethesda_final != bethesda_final_legacy) AS n_disagree,
  COUNT(DISTINCT CASE WHEN mol_has_afirma THEN 1 END)    AS has_afirma_col,
  COUNT(DISTINCT CASE WHEN mol_has_thyroseq THEN 1 END)  AS has_thyroseq_col
FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_m033_b3b4_v3`;
-- Expected: n=520; n_b3b4_check=520; n_null_bethesda_check=0; n_disagree=some (documented)

-- ============================================================================
-- Table 2: SENSITIVITY-1 — All-molecular (N=969, mirrors v1 universe)
-- ============================================================================
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.cohort_m033_all_molecular_v3`
CLUSTER BY research_id
OPTIONS(
  description="M033 SENSITIVITY-1 v3 cohort: all Afirma/ThyroSeq-tested patients (N=969). Mirrors v1 universe. Bethesda source: canonical_patient_master_v1_1.bethesda_final_strict_preop. Audit: DFL-20260508-M033-V3-COHORT-ALL-MOLECULAR-BUILD."
)
AS
SELECT
  CAST(m.research_id AS STRING)                        AS research_id,
  m.age_at_surgery,
  m.sex,
  -- Primary Bethesda (strict-preop)
  m.bethesda_final_strict_preop                        AS bethesda_final,
  m.bethesda_final_name                                AS bethesda_final_name,
  -- Legacy for cross-tab
  CAST(m.bethesda_num AS INT64)                        AS bethesda_final_legacy,
  m.bethesda_disagree_with_legacy_flag,
  m.mol_platform                                       AS mol_platform,
  m.mol_has_afirma,
  m.mol_has_thyroseq,
  m.mol_first_test_date,
  m.mol_genes_list,
  m.mol_n_variants_total,
  m.mol_n_distinct_genes,
  m.mol_has_fusion,
  CASE
    WHEN m.braf_positive_final THEN 'high'
    WHEN m.ras_positive_final  THEN 'intermediate'
    ELSE NULL
  END                                                  AS molecular_risk_tier,
  m.braf_positive_final,
  m.ras_positive_final,
  m.tert_positive_final,
  m.histology_final,
  m.is_malignant,
  m.tumor_size_cm_dominant                             AS tumor_size_cm,
  m.ete_grade_final                                    AS ete_grade_final,
  m.surg_procedure_type,
  CASE WHEN LOWER(m.surg_procedure_type) LIKE '%total%' THEN TRUE ELSE FALSE END AS surg_total_thyroidectomy,
  CASE WHEN LOWER(m.surg_procedure_type) LIKE '%hemi%'
            OR LOWER(m.surg_procedure_type) LIKE '%lobect%' THEN TRUE ELSE FALSE END AS surg_hemithyroidectomy,
  m.ln_positive_flag,
  m.ajcc8_stage_group,
  m.ata_risk_category,
  m.any_recurrence_flag,
  m.followup_years,
  m.surg_first_date,
  'DFL-20260508-M033-V3-COHORT-ALL-MOLECULAR-BUILD'   AS dfl_audit_id,
  CURRENT_TIMESTAMP()                                  AS v3_built_at
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master_v1_1` m
WHERE (m.mol_has_afirma IS TRUE OR m.mol_has_thyroseq IS TRUE);

SELECT
  'CONTRACT_m033_all_molecular_v3' AS check_name,
  COUNT(*) AS n,
  COUNTIF(bethesda_final IS NOT NULL) AS n_bethesda_pop,
  COUNTIF(bethesda_final IS NULL)     AS n_bethesda_null,
  COUNTIF(bethesda_final IN (3,4))    AS n_b3b4
FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_m033_all_molecular_v3`;
-- Expected: n=969; n_bethesda_pop=~804; n_bethesda_null=~165; n_b3b4=520

-- ============================================================================
-- Table 3: SENSITIVITY-2 — B5/B6 only (N=214)
-- ============================================================================
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.cohort_m033_b5b6_v3`
CLUSTER BY research_id
OPTIONS(
  description="M033 SENSITIVITY-2 v3 cohort: Afirma/ThyroSeq-tested with strict-preop Bethesda V or VI. N=214. Audit: DFL-20260508-M033-V3-COHORT-B5B6-BUILD."
)
AS
SELECT
  CAST(m.research_id AS STRING)                        AS research_id,
  m.age_at_surgery,
  m.sex,
  m.bethesda_final_strict_preop                        AS bethesda_final,
  m.bethesda_final_name                                AS bethesda_final_name,
  CAST(m.bethesda_num AS INT64)                        AS bethesda_final_legacy,
  m.bethesda_disagree_with_legacy_flag,
  m.mol_platform                                       AS mol_platform,
  m.mol_has_afirma,
  m.mol_has_thyroseq,
  m.mol_first_test_date,
  m.mol_genes_list,
  m.mol_n_variants_total,
  m.mol_n_distinct_genes,
  m.mol_has_fusion,
  CASE
    WHEN m.braf_positive_final THEN 'high'
    WHEN m.ras_positive_final  THEN 'intermediate'
    ELSE NULL
  END                                                  AS molecular_risk_tier,
  m.braf_positive_final,
  m.ras_positive_final,
  m.tert_positive_final,
  m.histology_final,
  m.is_malignant,
  m.tumor_size_cm_dominant                             AS tumor_size_cm,
  m.ete_grade_final                                    AS ete_grade_final,
  m.surg_procedure_type,
  CASE WHEN LOWER(m.surg_procedure_type) LIKE '%total%' THEN TRUE ELSE FALSE END AS surg_total_thyroidectomy,
  CASE WHEN LOWER(m.surg_procedure_type) LIKE '%hemi%'
            OR LOWER(m.surg_procedure_type) LIKE '%lobect%' THEN TRUE ELSE FALSE END AS surg_hemithyroidectomy,
  m.ln_positive_flag,
  m.ajcc8_stage_group,
  m.ata_risk_category,
  m.any_recurrence_flag,
  m.followup_years,
  m.surg_first_date,
  'DFL-20260508-M033-V3-COHORT-B5B6-BUILD'            AS dfl_audit_id,
  CURRENT_TIMESTAMP()                                  AS v3_built_at
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master_v1_1` m
WHERE (m.mol_has_afirma IS TRUE OR m.mol_has_thyroseq IS TRUE)
  AND m.bethesda_final_strict_preop IN (5, 6);

SELECT
  'CONTRACT_m033_b5b6_v3' AS check_name,
  COUNT(*) AS n,
  COUNTIF(bethesda_final IN (5,6)) AS n_b5b6_check
FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_m033_b5b6_v3`;
-- Expected: n=214
