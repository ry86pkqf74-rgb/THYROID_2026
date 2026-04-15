-- V2 expanded classification: resolves remaining 685 unclassified patients
-- Key improvements:
--   1. histologic_type-based classification (MTC, PTC, rare cancers)
--   2. Lymphoma variants (DLBCL, MALT, Hodgkin, Burkitt)
--   3. Rare thyroid/non-thyroid malignancies (angiosarcoma, rhabdomyosarcoma, melanoma)
--   4. Negated malignancy recognition ("negative for carcinoma" = benign)
--   5. Non-neoplastic procedures (thyroglossal, parathyroid, abscess)
--   6. Catch-all: no malignancy words at all → benign

WITH combined_text AS (
  SELECT
    pm.research_id,
    pm.fna_path_outcome AS current_outcome,
    pm.bethesda_final,
    pm.bethesda_final_name,
    LOWER(
      COALESCE(ps.synoptic_diagnosis, '') || ' ' ||
      COALESCE(ps.path_diagnosis_summary, '') || ' ' ||
      COALESCE(ps.tumor_1_histologic_type, '')
    ) AS all_text,
    ps.tumor_1_histologic_type
  FROM patient_refined_master_clinical_v12 pm
  JOIN path_synoptics ps
    ON pm.research_id = CAST(ps.research_id AS BIGINT)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pm.research_id
    ORDER BY LENGTH(
      COALESCE(ps.synoptic_diagnosis, '') ||
      COALESCE(ps.path_diagnosis_summary, '') ||
      COALESCE(ps.tumor_1_histologic_type, '')
    ) DESC
  ) = 1
),
classified AS (
  SELECT *,
    CASE
      -- ============================================================
      -- TIER 0: tumor_1_histologic_type explicitly names cancer
      -- ============================================================
      WHEN tumor_1_histologic_type IS NOT NULL
        AND regexp_matches(LOWER(tumor_1_histologic_type),
          'carcinoma|ptc|mtc|lymphoma|sarcoma|angiosarcoma|melanoma|high.grade')
        THEN 'malignant'

      -- ============================================================
      -- TIER 1: MALIGNANT — original + expanded patterns
      -- ============================================================
      WHEN regexp_matches(all_text,
        'papillary.*(carcinoma|thyroid cancer)'
        '|follicular carcinoma'
        '|follicular.*(cell|variant).*carcinoma'
        '|medullary.*carcinoma|medullary thyroid'
        '|anaplastic'
        '|poorly differentiated.*carcinoma'
        '|hurthle.*cell.*carcinoma|h.rthle.*cell.*carcinoma'
        '|oncocytic.*carcinoma'
        '|metastatic.*(carcinoma|thyroid|melanoma)'
        '|insular.*carcinoma'
        '|tall cell.*variant|columnar.*cell'
        '|diffuse sclerosing|hobnail|cribriform'
        '|solid.*variant.*ptc|warthin'
        '|squamous cell carcinoma.*thyroid'
        '|lymphoma.*thyroid|thyroid.*lymphoma'
        -- V2 EXPANDED:
        '|diffuse large b.cell|large b.cell lymphoma'
        '|malt lymphoma|maltoma'
        '|marginal zone lymphoma|marginal zone b.cell'
        '|hodgkin lymphoma|classical hodgkin'
        '|burkitt'
        '|b.cell lymphoma'
        '|angiosarcoma'
        '|rhabdomyosarcoma'
        '|adenoid cystic carcinoma'
        '|parathyroid carcinoma'
        '|metastatic melanoma'
        '|thymus.*like.*differentiation|castle'
        '|high.grade carcinoma|high.grade b cell'
        '|differentiated.*high.*grade.*thyroid'
        '|well differentiated thyroid carcinoma'
        '|infiltrating carcinoma'
        '|carcinoma.*type cannot'
      ) THEN 'malignant'

      -- ============================================================
      -- TIER 2: BORDERLINE — NIFTP, FTUMP, WDT-UMP
      -- ============================================================
      WHEN regexp_matches(all_text,
        'niftp|ftump|wdt-ump'
        '|uncertain malignant potential'
        '|undetermined malignant potential'
        '|noninvasive follicular thyroid neoplasm'
        '|well.differentiated tumor of uncertain'
        '|well.differentiated tumor of undetermined'
        '|follicular tumor of uncertain'
        '|atypical follicular adenoma'
        '|atypical oncocytic adenoma'
      ) THEN 'borderline_indeterminate'

      -- ============================================================
      -- TIER 3: BENIGN — original patterns (with malignancy exclusion)
      -- ============================================================
      WHEN regexp_matches(all_text,
        'benign|nodular hyperplasia|nodular thyroid hyperplasia'
        '|multinodular goiter|nodular goiter'
        '|colloid nodule|colloid goiter'
        '|follicular adenoma|adenomatoid nodule'
        '|hashimoto|graves|grave''s'
        '|lymphocytic thyroiditis'
        '|follicular nodular disease|thyroid hyperplasia'
        '|adenomatous goiter|adenomatous nodule'
        '|mng nos|multinodular colloid|nodular colloid'
        '|hurthle cell adenoma|h.rthle cell adenoma'
        '|oncocytic adenoma'
        '|follicular hyperplasia|diffuse hyperplasia'
        '|toxic goiter|substernal goiter'
        '|goitre'
        '|hyperplastic thyroid|hyperplastic nodule|hyperplastic appearing'
        '|multinodular thyroid'
        '|follicular nodule|adenomatoid'
        '|thyroglossal'
        '|branchial cleft'
      ) AND NOT regexp_matches(all_text,
        'carcinoma|malign|metastatic|anaplastic|poorly differentiated|lymphoma|sarcoma|melanoma'
      ) THEN 'benign'

      -- ============================================================
      -- TIER 4: BENIGN — benign patterns + NEGATED malignancy
      -- (text says "negative for carcinoma" but has benign thyroid finding)
      -- ============================================================
      WHEN regexp_matches(all_text,
        'benign|hyperplasia|hyperplastic|adenoma|adenomatoid|goiter|goitre'
        '|thyroiditis|hashimoto|graves|nodular|colloid|follicular nodular'
        '|multinodular|hurthle cell adenoma|oncocytic adenoma'
        '|thyroglossal|branchial|parathyroid'
      ) AND regexp_matches(all_text,
        'negative for (carcinoma|malignan|metasta|neoplasm|neoplasia|tumor)'
        '|no (carcinoma|malignan|evidence of malignan|evidence of carcinoma)'
        '|no diagnostic (mal|evidence)'
        '|no histologic evidence'
        '|no morphologic evidence'
        '|without evidence of mal'
        '|findings diagnostic of malignancy not identified'
        '|no atypia or malignancy'
      ) AND NOT regexp_matches(all_text,
        'papillary.*(carcinoma|thyroid cancer)'
        '|follicular carcinoma'
        '|medullary.*carcinoma|medullary thyroid'
        '|diffuse large b.cell|malt lymphoma|marginal zone|hodgkin'
        '|b.cell lymphoma|burkitt'
        '|angiosarcoma|rhabdomyosarcoma|melanoma'
        '|parathyroid carcinoma'
        '|adenoid cystic carcinoma'
        '|thymus.*like|castle'
        '|high.grade.*carcinoma|differentiated.*high.*grade'
        '|squamous cell carcinoma'
        '|infiltrating carcinoma'
        '|anaplastic|poorly differentiated.*carcinoma'
      ) THEN 'benign'

      -- ============================================================
      -- TIER 5: BENIGN — non-neoplastic procedures
      -- (thyroglossal, parathyroid-only, abscess, granulomatous, cyst)
      -- ============================================================
      WHEN regexp_matches(all_text,
        'thyroglossal|branchial cleft|abscess|necrotizing granulomatous'
        '|granulomatous inflammation|fibroadipose'
      ) AND NOT regexp_matches(all_text,
        'carcinoma|malignan|metasta|lymphoma|sarcoma|melanoma'
      ) THEN 'benign'

      -- ============================================================
      -- TIER 6: BENIGN catch-all — NO malignancy words at all
      -- If the text has zero cancer-related terms, it's benign
      -- ============================================================
      WHEN NOT regexp_matches(all_text,
        'carcinoma|malignan|metasta|cancer|lymphoma|sarcoma|melanoma'
        '|poorly differentiated|anaplastic|high.grade'
        '|\bptc\b|\bmtc\b'
      ) THEN 'benign'

      -- ============================================================
      -- TIER 7: BENIGN — negated-only malignancy (no true positive)
      -- All malignancy words appear only in negation context
      -- ============================================================
      WHEN regexp_matches(all_text,
        'negative for|no evidence|no malignancy|no carcinoma|not identified|negative for metast'
      ) AND NOT regexp_matches(all_text,
        'papillary.*(carcinoma|thyroid cancer)'
        '|follicular carcinoma'
        '|medullary.*carcinoma|medullary thyroid'
        '|diffuse large b.cell|large b.cell lymphoma|malt|marginal zone|hodgkin|burkitt'
        '|b.cell lymphoma'
        '|angiosarcoma|rhabdomyosarcoma|melanoma'
        '|parathyroid carcinoma|adenoid cystic'
        '|thymus.*like|castle'
        '|differentiated.*high.*grade|high.grade carcinoma'
        '|squamous cell carcinoma|infiltrating carcinoma'
        '|anaplastic|poorly differentiated.*carcinoma'
      ) THEN 'benign'

      -- ============================================================
      -- TIER 8: BENIGN — features of malignancy not found / ruled out
      -- ============================================================
      WHEN regexp_matches(all_text,
        'features of malignancy|interpretation of carcinoma is'
        '|no significant atypia'
        '|no metastatic malignancy'
        '|microfollicular adenoma'
        '|benign follicular tissue'
        '|benign thyroid parenchyma'
        '|benign adenomatoid'
        '|benign histologic'
        '|benign lymph node'
        '|foamy\s+histiocytes'
      ) AND NOT regexp_matches(all_text,
        'papillary.*(carcinoma|thyroid cancer)'
        '|follicular carcinoma'
        '|medullary.*carcinoma'
        '|diffuse large b.cell|malt lymphoma|marginal zone|hodgkin|b.cell lymphoma'
        '|angiosarcoma|rhabdomyosarcoma|melanoma'
        '|parathyroid carcinoma|adenoid cystic'
        '|differentiated.*high.*grade|high.grade carcinoma'
        '|anaplastic|poorly differentiated.*carcinoma'
        '|classic ptc|classic.* ptc|pathological stage.*pt'
      ) THEN 'benign'

      -- ============================================================
      -- TIER 8b: BENIGN — non-thyroid cancer coexisting with benign thyroid
      -- (tongue/scalp/vocal fold SCC + benign thyroid pathology)
      -- ============================================================
      WHEN (
        regexp_matches(all_text, 'mng|foamy histiocytes|chronic thyroiditis|follicular adenoma|nodular hyperplasia')
        AND regexp_matches(all_text, 'squamous cell carcinoma|invasive squamous cell')
        AND NOT regexp_matches(all_text, 'squamous cell carcinoma of the thyroid|squamous cell carcinoma of thyroid|squamous cell carcinoma.*thyroid gland')
        AND regexp_matches(all_text, 'tongue|scalp|vocal|tonsil|skin,|neck mass|fold|biopsy.*squamous')
      ) THEN 'benign'

      -- ============================================================
      -- TIER 8c: BENIGN — follicular adenoma ruled NOT carcinoma by consultation
      -- ============================================================
      WHEN regexp_matches(all_text, 'follicular adenoma')
        AND regexp_matches(all_text, 'interpretation of carcinoma|consultation|not.*characteristic.*carcinoma|did not show')
        AND NOT regexp_matches(all_text, 'papillary.*(carcinoma|thyroid cancer)|medullary.*carcinoma|diffuse large b.cell')
        THEN 'benign'

      -- ============================================================
      -- TIER 9: MALIGNANT — clinical narrative with explicit staging
      -- ============================================================
      WHEN regexp_matches(all_text,
        'pathological stage.*pt[1-4]'
        '|classic.* ptc|classic ptc'
        '|pathology revealed.*ptc'
        '|pathology revealed.*carcinoma'
      ) THEN 'malignant'

      -- Has text but still unclassifiable
      WHEN LENGTH(TRIM(all_text)) > 5 THEN 'unclassified_has_text'
      ELSE 'no_text'
    END AS regex_classification
  FROM combined_text
)
