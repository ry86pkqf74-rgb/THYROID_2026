-- LOGAN-ADAPTABLE TEMPLATE; READY FOR MANUSCRIPT USE POST mig_188b/186b/185b/187 APPLY
-- mig_196 Template 3 — AJCC8 stage group × histology (long-format)
-- Target DB: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- OUTPUT: histology_bucket, stage_group_resolved, n_patients, pct_within_histology
-- `T0_or_unstaged` bucket captures NULL / blank resolved stage group for manuscript transparency.
--
-- CAVEATS
-- * Uses same analytic cohort definition as Templates 1–2 (mig_195 flow step 4 ∩ malignant path events).
-- * Denominator `pct_within_histology` = row group count / histology_bucket total (distinct patients).

USE thyroid_canonical_publication_v1_0;

WITH
malignant_pts AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_path_malignant_events_v1
),
indeterminate_only_rid AS (
  SELECT DISTINCT CAST(i.research_id AS VARCHAR) AS research_id
  FROM main.canonical_path_indeterminate_events_v1 AS i
  WHERE NOT EXISTS (
    SELECT 1 FROM main.canonical_path_malignant_events_v1 AS m
    WHERE CAST(m.research_id AS VARCHAR) = CAST(i.research_id AS VARCHAR)
  )
),
step1_pool AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_patient_master
  WHERE is_malignant IS TRUE
),
step2_pool AS (
  SELECT p.research_id
  FROM step1_pool p
  WHERE NOT EXISTS (
    SELECT 1 FROM indeterminate_only_rid i WHERE i.research_id = p.research_id
  )
),
step3_pool AS (
  SELECT p.research_id
  FROM step2_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE NOT (c.histology_final IS NULL AND c.ajcc8_t_stage_resolved IS NULL)
),
step4_pool AS (
  SELECT p.research_id
  FROM step3_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE c.last_contact_date IS NOT NULL
),
analytic_rid AS (
  SELECT DISTINCT p.research_id
  FROM step4_pool p
  INNER JOIN malignant_pts m ON m.research_id = p.research_id
),
patient_row AS (
  SELECT
    CAST(c.research_id AS VARCHAR) AS research_id,
    CASE
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%papillary%' OR LOWER(c.histology_final) LIKE '%ptc%'
        OR LOWER(COALESCE(c.histologic_types_all, '')) LIKE '%ptc%'
      ) THEN 'PTC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%follicular%'
        AND LOWER(c.histology_final) NOT LIKE '%papillary%'
      ) THEN 'FTC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%medullary%' OR LOWER(c.histology_final) LIKE '%mtc%'
      ) THEN 'MTC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%anaplastic%' OR LOWER(c.histology_final) LIKE '%atc%'
      ) THEN 'ATC'
      WHEN c.histology_final IS NOT NULL AND TRIM(CAST(c.histology_final AS VARCHAR)) <> ''
        THEN 'other_or_mixed'
      ELSE 'unknown_histology'
    END AS histology_bucket,
    COALESCE(NULLIF(TRIM(CAST(c.ajcc8_stage_group_resolved AS VARCHAR)), ''), 'T0_or_unstaged') AS stage_group_resolved
  FROM main.canonical_patient_master c
  INNER JOIN analytic_rid ar ON ar.research_id = CAST(c.research_id AS VARCHAR)
),
hist_denom AS (
  SELECT histology_bucket, COUNT(*)::BIGINT AS hist_n
  FROM patient_row
  GROUP BY 1
),
cell_ct AS (
  SELECT histology_bucket, stage_group_resolved, COUNT(*)::BIGINT AS n_patients
  FROM patient_row
  GROUP BY 1, 2
)

SELECT
  c.histology_bucket,
  c.stage_group_resolved,
  c.n_patients,
  ROUND(100.0 * c.n_patients / NULLIF(h.hist_n, 0), 2) AS pct_within_histology,
  h.hist_n AS histology_row_total
FROM cell_ct c
INNER JOIN hist_denom h ON h.histology_bucket = c.histology_bucket
ORDER BY c.histology_bucket, c.stage_group_resolved;
