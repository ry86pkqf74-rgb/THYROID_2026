-- =============================================================================
-- mig_319 — manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1
-- =============================================================================
-- Date:   2026-05-05 (UTC)
-- Target: thyroid_canonical_publication_v1_0
--
-- Replaces the 1-column stub with a full analytic spine (N = m083 dual-platform
-- analytic cohort, 167 patients): per-platform BRAF from canonical_molecular_genetics_v2,
-- discordance flag, and pathology-line reference from CMG rows where
-- report_text_source = 'enrichment.pathology_raw' (NOT pure surgical IHC — see
-- view COMMENT).
--
-- Cohort spine (NO circular reference to this view): patients in
-- molecular_platform_resolved_v1 with BOTH ThyroSeq + Afirma in
-- mol_platform_resolved (N=167, matches prior m083 analytic cohort).
--
-- §0 — Repair m083_dual_platform_analytic_v1 — it previously filtered through
--       cohort_m083_* stub and caused infinite recursion once this view was built.
--       Predicate below is the SSOT dual-platform definition.
--
-- Validation (run after apply; see scripts/verify_mig319_m083_cohort_view.py):
--   * 130 <= COUNT(*) <= 200
--   * path_braf_status non-null on >= 40% of rows
--   * Discordance among evaluable pairs is expected HIGH (~60%+) vs literature
--     for same-marker cross-lab agreement — Afirma classifier vs ThyroSeq NGS.
--
-- Closes: CF-M083-STUB
-- =============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.m083_dual_platform_analytic_v1 AS
SELECT
  c.research_id,
  c.age_at_surgery,
  c.sex,
  c.race,
  c.histology_final,
  c.tumor_size_cm_dominant,
  c.is_malignant,
  c.ajcc8_stage_group,
  c.ajcc8_t_stage,
  c.ajcc8_n_stage,
  c.braf_positive_final,
  c.braf_variant,
  c.braf_detection_method_v11,
  c.ras_positive_final,
  c.ras_subtype,
  c.mol_has_fusion,
  c.molecular_risk_tier,
  c.mol_n_distinct_genes,
  c.mol_genes_list,
  r.mol_platform_original,
  r.mol_platform_resolved,
  r.mol_platform_evidence,
  r.mol_platform_confidence,
  r.n_episodes_used,
  c.any_recurrence_flag,
  c.any_confirmed_complication_flag,
  c.rai_received_reconciled,
  CASE
    WHEN c.ete_grade_final IS NULL THEN 'none'
    WHEN c.ete_grade_final IN ('None', 'false', 'absent') THEN 'none'
    WHEN c.ete_grade_final = 'microscopic' THEN 'microscopic'
    WHEN c.ete_grade_final = 'gross' THEN 'gross'
    ELSE 'present_ungraded'
  END AS ete_grade_clean,
  c.vascular_invasion_final,
  c.ln_positive_final,
  c.ln_rollup_total_examined,
  c.ln_rollup_total_positive,
  c.surg_procedure_type
FROM main.canonical_patient_master AS c
INNER JOIN manuscript_workspace.molecular_platform_resolved_v1 AS r
  ON c.research_id = r.research_id
WHERE r.mol_platform_resolved LIKE '%ThyroSeq%'
  AND r.mol_platform_resolved LIKE '%Afirma%';

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1 AS
WITH cohort_spine AS (
  SELECT c.research_id
  FROM main.canonical_patient_master AS c
  INNER JOIN manuscript_workspace.molecular_platform_resolved_v1 AS r
    ON c.research_id = r.research_id
  WHERE r.mol_platform_resolved LIKE '%ThyroSeq%'
    AND r.mol_platform_resolved LIKE '%Afirma%'
),
ts_per_pt AS (
  SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    BOOL_OR(braf_flag) AS thyroseq_braf_pos,
    MAX(braf_variant) FILTER (WHERE platform = 'ThyroSeq') AS thyroseq_braf_variant,
    MAX(TRY_CAST(resolved_test_date AS DATE)) FILTER (WHERE platform = 'ThyroSeq')
      AS thyroseq_latest_test_date,
    COUNT(*) FILTER (WHERE platform = 'ThyroSeq') AS thyroseq_n_episodes
  FROM main.canonical_molecular_genetics_v2
  WHERE platform = 'ThyroSeq'
  GROUP BY 1
),
ts_braf_vaf AS (
  SELECT
    CAST(c.research_id AS VARCHAR) AS research_id,
    MAX(t.af_pct) AS thyroseq_braf_vaf_pct
  FROM main.canonical_molecular_genetics_v2 AS c
  CROSS JOIN UNNEST(c.gene_mutations_variants) AS _(t)
  WHERE c.platform = 'ThyroSeq'
    AND t.gene = 'BRAF'
    AND t.af_pct IS NOT NULL
  GROUP BY 1
),
af_per_pt AS (
  SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    BOOL_OR(braf_flag) AS afirma_braf_pos,
    MAX(
      CASE
        WHEN LOWER(TRIM(COALESCE(afirma_braf_result, ''))) IN ('positive', 'rositive') THEN 'Positive'
        WHEN LOWER(TRIM(COALESCE(afirma_braf_result, ''))) IN ('negative', 'not_detected', 'not detected') THEN 'Negative'
        WHEN afirma_braf_result IS NOT NULL AND TRIM(CAST(afirma_braf_result AS VARCHAR)) <> ''
          THEN TRIM(CAST(afirma_braf_result AS VARCHAR))
        ELSE NULL
      END
    ) AS afirma_braf_result_raw,
    MAX(rom_percent_point) FILTER (WHERE platform = 'Afirma') AS afirma_rom_percent_point,
    MAX(TRY_CAST(resolved_test_date AS DATE)) FILTER (WHERE platform = 'Afirma')
      AS afirma_latest_test_date,
    COUNT(*) FILTER (WHERE platform = 'Afirma') AS afirma_n_episodes
  FROM main.canonical_molecular_genetics_v2
  WHERE platform = 'Afirma'
  GROUP BY 1
),
path_line_cmg AS (
  SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    BOOL_OR(braf_flag) AS path_line_braf_pos,
    COUNT(*) AS path_line_n_cmg_episodes
  FROM main.canonical_molecular_genetics_v2
  WHERE report_text_source = 'enrichment.pathology_raw'
  GROUP BY 1
),
dual AS (
  SELECT
    s.research_id,
    ts.thyroseq_braf_pos,
    ts.thyroseq_braf_variant,
    ts.thyroseq_latest_test_date,
    ts.thyroseq_n_episodes,
    tv.thyroseq_braf_vaf_pct,
    af.afirma_braf_pos,
    af.afirma_braf_result_raw,
    af.afirma_rom_percent_point,
    af.afirma_latest_test_date,
    af.afirma_n_episodes,
    pl.path_line_braf_pos,
    pl.path_line_n_cmg_episodes,
    CASE
      WHEN ts.thyroseq_braf_pos IS TRUE THEN 'positive'
      WHEN ts.thyroseq_braf_pos IS FALSE THEN 'negative'
      ELSE NULL
    END AS thyroseq_braf,
    CASE
      WHEN af.afirma_braf_pos IS TRUE THEN 'positive'
      WHEN af.afirma_braf_pos IS FALSE THEN 'negative'
      ELSE NULL
    END AS afirma_braf,
    CASE
      WHEN pl.path_line_n_cmg_episodes IS NULL OR pl.path_line_n_cmg_episodes = 0 THEN NULL
      WHEN pl.path_line_braf_pos IS TRUE THEN 'positive'
      ELSE 'negative'
    END AS path_braf_status,
    CASE
      WHEN ts.thyroseq_latest_test_date IS NOT NULL
           AND af.afirma_latest_test_date IS NOT NULL
        THEN GREATEST(ts.thyroseq_latest_test_date, af.afirma_latest_test_date)
      WHEN ts.thyroseq_latest_test_date IS NOT NULL
        THEN ts.thyroseq_latest_test_date
      ELSE af.afirma_latest_test_date
    END AS latest_test_date_either_platform
  FROM cohort_spine AS s
  LEFT JOIN ts_per_pt AS ts USING (research_id)
  LEFT JOIN ts_braf_vaf AS tv USING (research_id)
  LEFT JOIN af_per_pt AS af USING (research_id)
  LEFT JOIN path_line_cmg AS pl USING (research_id)
)
SELECT
  cpm.research_id,
  cpm.age_at_surgery,
  cpm.sex,
  cpm.histology_final,
  cpm.is_malignant,
  cpm.ajcc8_t_stage,
  cpm.ajcc8_n_stage,
  cpm.ajcc8_m_stage,
  cpm.ajcc8_stage_group,
  cpm.path_tumor_size_cm AS tumor_size_cm,
  cpm.tumor_size_cm_dominant,
  cpm.lvi_grade,
  cpm.lvi_ordinal_worst,
  cpm.ete_grade_final_v2 AS ete_grade_final,
  cpm.any_recurrence_flag,
  cpm.structural_recurrence_flag,
  cpm.followup_years,
  -- Per-platform molecular rollups (canonical_molecular_genetics_v2)
  d.afirma_braf,
  d.afirma_braf_result_raw,
  d.afirma_rom_percent_point AS afirma_braf_score,
  d.thyroseq_braf,
  d.thyroseq_braf_variant,
  d.thyroseq_braf_vaf_pct AS thyroseq_braf_vaf,
  d.thyroseq_n_episodes,
  d.afirma_n_episodes,
  d.latest_test_date_either_platform AS latest_test_date,
  CASE
    WHEN d.thyroseq_braf_pos IS NULL OR d.afirma_braf_pos IS NULL THEN NULL
    WHEN d.thyroseq_braf_pos IS NOT DISTINCT FROM d.afirma_braf_pos THEN FALSE
    ELSE TRUE
  END AS dual_platform_discordant_flag,
  -- Pathology-line reference from pathology-enriched CMG episodes (see COMMENT)
  d.path_braf_status,
  d.path_line_n_cmg_episodes,
  CASE
    WHEN d.path_braf_status IS NULL THEN NULL
    WHEN d.afirma_braf IS NULL THEN NULL
    WHEN d.path_braf_status = d.afirma_braf THEN 'concordant'
    WHEN d.path_braf_status = 'positive' AND d.afirma_braf = 'negative' THEN 'afirma_false_negative'
    WHEN d.path_braf_status = 'negative' AND d.afirma_braf = 'positive' THEN 'afirma_false_positive'
    ELSE 'discordant_other'
  END AS afirma_vs_path_concordance,
  CASE
    WHEN d.path_braf_status IS NULL THEN NULL
    WHEN d.thyroseq_braf IS NULL THEN NULL
    WHEN d.path_braf_status = d.thyroseq_braf THEN 'concordant'
    WHEN d.path_braf_status = 'positive' AND d.thyroseq_braf = 'negative' THEN 'thyroseq_false_negative'
    WHEN d.path_braf_status = 'negative' AND d.thyroseq_braf = 'positive' THEN 'thyroseq_false_positive'
    ELSE 'discordant_other'
  END AS thyroseq_vs_path_concordance
FROM dual AS d
INNER JOIN main.canonical_patient_master AS cpm
  ON cpm.research_id = d.research_id;

COMMENT ON VIEW manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1 IS
'mig_319 (2026-05-05): M083 dual-platform (Afirma + ThyroSeq) cohort with per-platform BRAF BOOL_OR flags from canonical_molecular_genetics_v2, discordance vs paired platform, and path_braf_status derived from CMG rows with report_text_source=enrichment.pathology_raw (path report text / enrichment — not a standalone IHC registry). For McNemar / kappa use evaluable subset where both platform aggregates are non-NULL (typically 160/167).';
