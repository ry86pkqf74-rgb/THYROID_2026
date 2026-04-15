-- Task 1: Path outcome classification audit
-- Pure regex classification against path_synoptics text fields
-- Deduped to ONE row per patient (worst-case text aggregation)

WITH combined_text AS (
  SELECT 
    pm.research_id,
    pm.fna_path_outcome as current_outcome,
    pm.bethesda_final,
    pm.bethesda_final_name,
    LOWER(COALESCE(ps.synoptic_diagnosis,'') || ' ' || COALESCE(ps.path_diagnosis_summary,'') || ' ' || COALESCE(ps.tumor_1_histologic_type,'')) as all_text,
    ps.tumor_1_histologic_type
  FROM patient_refined_master_clinical_v12 pm
  JOIN path_synoptics ps ON pm.research_id = CAST(ps.research_id AS BIGINT)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pm.research_id
    ORDER BY LENGTH(COALESCE(ps.synoptic_diagnosis,'') || COALESCE(ps.path_diagnosis_summary,'') || COALESCE(ps.tumor_1_histologic_type,'')) DESC
  ) = 1
),
classified AS (
  SELECT *,
    CASE 
      WHEN regexp_matches(all_text, 
        'papillary.*(carcinoma|thyroid cancer)|follicular carcinoma|follicular.*(cell|variant).*carcinoma|medullary.*carcinoma|medullary thyroid|anaplastic|poorly differentiated.*carcinoma|hurthle.*cell.*carcinoma|oncocytic.*carcinoma|metastatic.*(carcinoma|thyroid|ptc|mtc)|insular.*carcinoma|tall cell.*variant|columnar.*cell|diffuse sclerosing|hobnail|cribriform|solid.*variant.*ptc|warthin|^ptc$|^ptc |^mtc$|^mtc |squamous cell carcinoma.*thyroid|lymphoma.*thyroid|thyroid.*lymphoma')
        THEN 'malignant'
      WHEN regexp_matches(all_text,
        'niftp|ftump|wdt-ump|uncertain malignant potential|noninvasive follicular thyroid neoplasm|well.differentiated tumor of uncertain|follicular tumor of uncertain')
        THEN 'borderline_indeterminate'
      WHEN regexp_matches(all_text,
        'benign|nodular hyperplasia|nodular thyroid hyperplasia|multinodular goiter|nodular goiter|colloid nodule|colloid goiter|follicular adenoma|adenomatoid nodule|hashimoto|graves|lymphocytic thyroiditis|follicular nodular disease|thyroid hyperplasia|adenomatous goiter|adenomatous nodule|mng nos|multinodular colloid|nodular colloid|hurthle cell adenoma|oncocytic adenoma|follicular hyperplasia|diffuse hyperplasia|toxic goiter|substernal goiter')
        AND NOT regexp_matches(all_text,
        'carcinoma|malign|metastatic|anaplastic|poorly differentiated|lymphoma')
        THEN 'benign'
      WHEN regexp_matches(all_text, 'thyroiditis|goiter|hyperplasia|adenoma')
        AND NOT regexp_matches(all_text, 'carcinoma|malign|metastatic|anaplastic|poorly differentiated|lymphoma|niftp|ftump|uncertain')
        THEN 'benign'
      WHEN LENGTH(TRIM(all_text)) > 5 THEN 'unclassified_has_text'
      ELSE 'no_text'
    END as regex_classification
  FROM combined_text
)
SELECT 
  current_outcome, regex_classification, COUNT(*) as patients
FROM classified
GROUP BY 1, 2
ORDER BY 1 NULLS FIRST, 3 DESC;
