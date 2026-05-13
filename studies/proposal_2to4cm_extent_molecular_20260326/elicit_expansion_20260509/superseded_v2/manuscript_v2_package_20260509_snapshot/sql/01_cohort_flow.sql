-- 01_cohort_flow.sql
-- Reproduces the cohort_flow_bq.csv counts for the 2026-05-09 EXT2-4 Elicit expansion.
-- Reads only; no writes.

WITH base AS (
  SELECT
    research_id,
    surg_first_date,
    surg_procedure_type,
    surg_total_thyroidectomy,
    surg_hemithyroidectomy,
    imaging_nodule_size_cm,
    path_tumor_size_cm,
    fna_bethesda_final,
    mol_platform,
    histology_final
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
)
SELECT
  COUNT(*) AS n_total_master,
  COUNTIF(surg_first_date IS NOT NULL) AS n_with_surgery_date,
  COUNTIF(surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')) AS n_lobe_or_total,
  COUNTIF(surg_total_thyroidectomy = TRUE) AS n_total_thyroidectomy,
  COUNTIF(surg_hemithyroidectomy = TRUE) AS n_hemithyroidectomy,
  COUNTIF(fna_bethesda_final IS NOT NULL) AS n_with_bethesda,
  COUNTIF(fna_bethesda_final IN (3,4)) AS n_bethesda_3_or_4,
  COUNTIF(mol_platform IN ('Afirma','ThyroSeq')) AS n_named_platform,
  COUNTIF(fna_bethesda_final IN (3,4)
          AND mol_platform IN ('Afirma','ThyroSeq')
          AND histology_final IS NOT NULL) AS n_b34_named_with_histology,
  COUNTIF(fna_bethesda_final IN (3,4)
          AND mol_platform = 'Afirma'
          AND histology_final IS NOT NULL) AS n_afirma_b34_with_histology,
  COUNTIF(fna_bethesda_final IN (3,4)
          AND mol_platform = 'ThyroSeq'
          AND histology_final IS NOT NULL) AS n_thyroseq_b34_with_histology,
  COUNTIF(imaging_nodule_size_cm BETWEEN 2.0 AND 4.0) AS n_preop_2to4cm_full_master,
  COUNTIF(path_tumor_size_cm BETWEEN 2.0 AND 4.0) AS n_path_2to4cm_full_master
FROM base;
