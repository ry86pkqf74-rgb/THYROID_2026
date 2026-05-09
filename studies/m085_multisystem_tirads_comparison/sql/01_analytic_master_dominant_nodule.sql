-- M085 Multisystem TIRADS Comparison
-- Patient-level analytic master, dominant nodule = largest scored
-- Built 2026-05-09
WITH dom_nodule AS (
  SELECT
    research_id, nodule_id, us_exam_id, exam_date,
    size_cm_max,
    tirads_reported_in_text, tirads_reported_system_validated,
    -- ACR 2017
    acr2017_total_pts_imputed, acr2017_total_pts_strict,
    acr2017_category_imputed, acr2017_category_strict,
    acr2017_features_complete_imputed, acr2017_features_complete_strict,
    acr2017_fna_recommended_imputed, acr2017_fna_recommended_strict,
    -- Kwak
    kwak_n_suspicious_features, kwak_category, kwak_fna_recommended,
    -- K-TIRADS
    ktirads_composition_class, ktirads_n_suspicious, ktirads_category, ktirads_fna_recommended,
    -- C-TIRADS
    ctirads_score, ctirads_category, ctirads_fna_recommended,
    -- EU-TIRADS
    eutirads_pattern, eutirads_category, eutirads_fna_recommended,
    -- ATA
    ata_pattern, ata_fna_recommended,
    -- BTA
    bta_category,
    -- AACE
    aace_class, aace_fna_recommended,
    -- Horvath
    horvath_pattern, horvath_category, horvath_confidence,
    -- Park 2009 (3 coefficient sets)
    park2009_logit, park2009_probability, park2009_category,
    park_cosmos_logit, park_cosmos_probability, park_cosmos_category,
    park_cohort_logit, park_cohort_probability, park_cohort_category,
    -- SRU
    sru_recommendation,
    ROW_NUMBER() OVER (
      PARTITION BY research_id
      ORDER BY size_cm_max DESC NULLS LAST, exam_date DESC, nodule_id
    ) AS rn
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
),
pt AS (
  SELECT
    research_id,
    age_at_surgery,
    sex,
    race,
    bmi_combined,
    first_surgery_date_v2 AS first_surgery_date,
    EXTRACT(YEAR FROM first_surgery_date_v2) AS surgery_year,
    followup_years,
    ajcc8_t_stage_resolved,
    ajcc8_n_stage_resolved,
    ajcc8_m_stage_resolved,
    ajcc8_stage_group_resolved
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master_v1_1`
),
dx AS (
  SELECT research_id, is_malignant, diagnosis_primary, diagnosis_full
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1`
),
op AS (
  SELECT
    research_id,
    n_surgeries,
    n_total_thyroidectomies,
    n_hemithyroidectomies,
    n_completion_thyroidectomies,
    n_central_neck_dissections,
    n_lateral_neck_dissections,
    any_frozen_section,
    any_rln_monitoring,
    earliest_surgery_date AS op_earliest_surg_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_patient_rollup_v1_1`
),
mol AS (
  -- Most recent molecular test per patient
  SELECT
    research_id,
    ANY_VALUE(platform) AS molecular_platform,
    ANY_VALUE(overall_result_class) AS molecular_result_class,
    ANY_VALUE(braf_flag) AS molecular_braf_flag,
    ANY_VALUE(ras_flag) AS molecular_ras_flag,
    ANY_VALUE(ret_flag) AS molecular_ret_flag,
    ANY_VALUE(tert_flag) AS molecular_tert_flag,
    ANY_VALUE(high_risk_marker_flag) AS molecular_high_risk_flag,
    COUNT(*) AS n_molecular_tests
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2`
  GROUP BY research_id
),
fna AS (
  -- Worst (highest) Bethesda category per patient
  SELECT
    research_id,
    MAX(SAFE_CAST(REGEXP_EXTRACT(bethesda_category, r'(\d)') AS INT64)) AS max_bethesda,
    COUNT(*) AS n_fnas
  FROM `thyroid-canonical-pub-2026.pub_canonical.fna_episode_master_v2`
  GROUP BY research_id
),
wt AS (
  SELECT research_id, MAX(specimen_weight_combined) AS thyroid_weight_g
  FROM `thyroid-canonical-pub-2026.pub_canonical.thyroid_weights`
  GROUP BY research_id
),
sz AS (
  SELECT research_id, MAX(total_volume_cm3) AS thyroid_volume_cm3
  FROM `thyroid-canonical-pub-2026.pub_canonical.thyroid_sizes`
  GROUP BY research_id
),
malig_tumor AS (
  -- Largest malignant tumor per patient
  SELECT
    research_id,
    ANY_VALUE(primary_histology) AS dom_tumor_histology,
    ANY_VALUE(histology_variant) AS dom_tumor_histology_variant,
    MAX(size_greatest_dimension_cm) AS max_tumor_size_cm,
    ANY_VALUE(extrathyroidal_extension) AS dom_tumor_ete,
    ANY_VALUE(multifocality_flag) AS multifocality,
    ANY_VALUE(ln_involved) AS ln_involved
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_events_v1`
  GROUP BY research_id
)
SELECT
  d.* EXCEPT(rn),
  pt.age_at_surgery, pt.sex, pt.race, pt.bmi_combined,
  pt.first_surgery_date, pt.surgery_year, pt.followup_years,
  pt.ajcc8_t_stage_resolved, pt.ajcc8_n_stage_resolved,
  pt.ajcc8_m_stage_resolved, pt.ajcc8_stage_group_resolved,
  dx.is_malignant, dx.diagnosis_primary, dx.diagnosis_full,
  op.n_surgeries, op.n_total_thyroidectomies, op.n_hemithyroidectomies,
  op.n_completion_thyroidectomies, op.n_central_neck_dissections,
  op.n_lateral_neck_dissections, op.any_frozen_section, op.any_rln_monitoring,
  op.op_earliest_surg_date,
  mol.molecular_platform, mol.molecular_result_class, mol.molecular_braf_flag,
  mol.molecular_ras_flag, mol.molecular_ret_flag, mol.molecular_tert_flag,
  mol.molecular_high_risk_flag, mol.n_molecular_tests,
  fna.max_bethesda, fna.n_fnas,
  wt.thyroid_weight_g, sz.thyroid_volume_cm3,
  mt.dom_tumor_histology, mt.dom_tumor_histology_variant, mt.max_tumor_size_cm,
  mt.dom_tumor_ete, mt.multifocality, mt.ln_involved,
  -- Surgery type derived
  CASE
    WHEN op.n_total_thyroidectomies > 0 THEN 'Total'
    WHEN op.n_completion_thyroidectomies > 0 THEN 'Completion'
    WHEN op.n_hemithyroidectomies > 0 THEN 'Hemi'
    ELSE 'Unknown'
  END AS surgery_type,
  -- ATA-pattern → 5-band coding for ROC
  CASE ata_pattern
    WHEN 'high_suspicion' THEN 5
    WHEN 'intermediate_suspicion' THEN 4
    WHEN 'low_suspicion' THEN 3
    WHEN 'very_low_suspicion' THEN 2
    WHEN 'benign' THEN 1
    ELSE NULL
  END AS ata_pattern_ord,
  -- ACR-imputed numeric category
  SAFE_CAST(REGEXP_EXTRACT(acr2017_category_imputed, r'(\d)') AS INT64) AS acr2017_imputed_int,
  SAFE_CAST(REGEXP_EXTRACT(eutirads_category, r'(\d)') AS INT64) AS eutirads_int,
  SAFE_CAST(REGEXP_EXTRACT(ktirads_category, r'(\d)') AS INT64) AS ktirads_int,
  SAFE_CAST(REGEXP_EXTRACT(kwak_category, r'(\d)') AS INT64) AS kwak_int,
  SAFE_CAST(REGEXP_EXTRACT(ctirads_category, r'(\d)') AS INT64) AS ctirads_int,
  SAFE_CAST(REGEXP_EXTRACT(bta_category, r'U(\d)') AS INT64) AS bta_int,
  SAFE_CAST(REGEXP_EXTRACT(horvath_category, r'(\d)') AS INT64) AS horvath_int,
  SAFE_CAST(REGEXP_EXTRACT(park2009_category, r'(\d)') AS INT64) AS park2009_int
FROM dom_nodule d
LEFT JOIN pt USING(research_id)
LEFT JOIN dx USING(research_id)
LEFT JOIN op USING(research_id)
LEFT JOIN mol USING(research_id)
LEFT JOIN fna USING(research_id)
LEFT JOIN wt USING(research_id)
LEFT JOIN sz USING(research_id)
LEFT JOIN malig_tumor mt USING(research_id)
WHERE d.rn = 1
  AND dx.is_malignant IS NOT NULL  -- require pathology outcome
