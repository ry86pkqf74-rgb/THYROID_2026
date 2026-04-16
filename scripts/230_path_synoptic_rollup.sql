-- ============================================================================
-- Script 230: Patient-Level Path Synoptic Rollup
-- Repository: https://github.com/ry86pkqf74-rgb/THYROID_2026
-- Database: thyroid_canonical_publication_v1_0 on MotherDuck
-- 
-- Purpose: Build patient_tumor_rollup_v1 from synoptic_tumor_long_v1 so that
--          per-tumor pathology data (size, margin, LVI, angioinvasion, PNI,
--          capsular invasion, ETE, focality) is correctly rolled up to the
--          patient level.
--
-- Fixes these bugs in canonical_patient_master_v221:
--   1. margin_status_final corrupted — 99% of mETE labeled R1, but raw data 
--      shows 84% uninvolved. Conflated ETE with margin status.
--   2. lvi_grade_final_v13 collapsed 92-95% to "present_ungraded" — lost signal
--   3. path_n_tumors, multifocal_flag, path_multifocal_flag all 100% NULL
--   4. Tumor 2-5 data (up to 5 tumors/patient) never rolled up — only tumor 1
--   5. bilateral_disease_flag only 37% populated — underdiscovered
--   6. angioinvasion not surfaced to canonical master at all
--
-- Key decoding (verified against distance-to-closest-margin):
--   - For ETE / LVI / angioinvasion / capsular invasion / PNI:
--       'x' = checkbox on PRESENT  = feature IS present
--   - For margin_status:
--       'x' = checkbox on UNINVOLVED = negative margin (R0)
--       'involved' = positive margin (R1)
--
-- Usage:
--   Run this SQL via motherduck_client in read-write mode.
--   Produces: thyroid_canonical_publication_v1_0.main.patient_tumor_rollup_v1
-- ============================================================================

DROP TABLE IF EXISTS thyroid_canonical_publication_v1_0.main.patient_tumor_rollup_v1;

CREATE TABLE thyroid_canonical_publication_v1_0.main.patient_tumor_rollup_v1 AS
WITH 
-- ------------------------------------------------------------------------
-- Step 1: Normalize per-tumor field values into clean categorical flags
-- ------------------------------------------------------------------------
tumor_normalized AS (
  SELECT 
    CAST(research_id AS VARCHAR) AS research_id,
    tumor_index,
    size_greatest_dimension_cm AS tumor_size_cm,
    site AS tumor_site,
    histologic_type,
    histologic_variant,
    
    -- ETE: present if 'x' or any explicit present-level term
    CASE 
      WHEN LOWER(TRIM(extrathyroidal_extension)) IN ('x','present','yes','yes;','focal','minimal','microscopic','extensive','yes, extensive','yes (minimal)','yes (focal)','microscopic extension','microscopiic','minimal into fat','minimal microscopic','microscopic extension','present (microscopic perithyroidal soft tissue only with no clinical or macroscopic evidence of invasion)')
        THEN 'present'
      WHEN LOWER(TRIM(extrathyroidal_extension)) IN ('false','no','none','negative','not_identified')
        THEN 'absent'
      WHEN LOWER(TRIM(extrathyroidal_extension)) IN ('c/a','indeterminate','n/s','n/a')
        THEN 'indeterminate'
      WHEN extrathyroidal_extension IS NULL THEN NULL
      ELSE 'other'
    END AS ete_clean,
    
    -- ETE grade ordinal for worst-case rollup (0=none, 1=microscopic, 2=gross, 9=indeterminate)
    CASE 
      WHEN LOWER(TRIM(extrathyroidal_extension)) IN ('extensive','yes, extensive') THEN 2  -- gross
      WHEN LOWER(TRIM(extrathyroidal_extension)) IN ('x','present','yes','yes;','focal','minimal','microscopic','yes (minimal)','yes (focal)','microscopic extension','microscopiic','minimal into fat','minimal microscopic','present (microscopic perithyroidal soft tissue only with no clinical or macroscopic evidence of invasion)') THEN 1  -- microscopic
      WHEN LOWER(TRIM(extrathyroidal_extension)) IN ('false','no','none','negative') THEN 0
      WHEN LOWER(TRIM(extrathyroidal_extension)) IN ('c/a','indeterminate','n/s') THEN 9
      ELSE NULL
    END AS ete_ordinal,
    
    -- Margin status: CRITICAL — 'x' means UNINVOLVED (opposite of other fields)
    CASE 
      WHEN LOWER(TRIM(margin_status)) IN ('x','negative','uninvolved','no','not involved') THEN 'uninvolved'
      WHEN LOWER(TRIM(margin_status)) IN ('involved','present','yes') THEN 'involved'
      WHEN LOWER(TRIM(margin_status)) IN ('c/a','indeterminate') THEN 'indeterminate'
      WHEN margin_status IS NULL THEN NULL
      ELSE 'other'
    END AS margin_clean,
    
    -- Lymphatic (lymphovascular) invasion: 'x' = PRESENT
    CASE 
      WHEN LOWER(TRIM(lymphatic_invasion)) IN ('x','present','yes','focal','extensive','preesent') THEN 'present'
      WHEN LOWER(TRIM(lymphatic_invasion)) IN ('no','none','negative','absent') THEN 'absent'
      WHEN LOWER(TRIM(lymphatic_invasion)) IN ('c/a','indeterminate','indetermiante','indeeterminate') THEN 'indeterminate'
      WHEN lymphatic_invasion IS NULL THEN NULL
      ELSE 'other'
    END AS lvi_clean,
    
    -- LVI ordinal for worst-case rollup (0=absent, 1=focal, 2=present_ungraded, 3=extensive, 9=indeterminate)
    CASE 
      WHEN LOWER(TRIM(lymphatic_invasion)) = 'extensive' THEN 3
      WHEN LOWER(TRIM(lymphatic_invasion)) IN ('x','present','yes','preesent') THEN 2
      WHEN LOWER(TRIM(lymphatic_invasion)) = 'focal' THEN 1
      WHEN LOWER(TRIM(lymphatic_invasion)) IN ('no','none','negative','absent') THEN 0
      WHEN LOWER(TRIM(lymphatic_invasion)) IN ('c/a','indeterminate') THEN 9
      ELSE NULL
    END AS lvi_ordinal,
    
    -- Angioinvasion (vascular invasion): 'x' = PRESENT
    CASE 
      WHEN LOWER(TRIM(angioinvasion)) IN ('x','present','yes','focal','extensive') THEN 'present'
      WHEN LOWER(TRIM(angioinvasion)) IN ('no','none','negative','absent') THEN 'absent'
      WHEN LOWER(TRIM(angioinvasion)) IN ('c/a','indeterminate') THEN 'indeterminate'
      WHEN angioinvasion IS NULL THEN NULL
      ELSE 'other'
    END AS vi_clean,
    
    CASE 
      WHEN LOWER(TRIM(angioinvasion)) = 'extensive' THEN 3
      WHEN LOWER(TRIM(angioinvasion)) IN ('x','present','yes') THEN 2
      WHEN LOWER(TRIM(angioinvasion)) = 'focal' THEN 1
      WHEN LOWER(TRIM(angioinvasion)) IN ('no','none','negative','absent') THEN 0
      WHEN LOWER(TRIM(angioinvasion)) IN ('c/a','indeterminate') THEN 9
      ELSE NULL
    END AS vi_ordinal,
    
    angioinvasion_quantify AS vi_count_vessels,
    
    -- Perineural invasion: 'x' = PRESENT
    CASE 
      WHEN LOWER(TRIM(perineural_invasion)) IN ('x','present','yes','focal','extensive') THEN 'present'
      WHEN LOWER(TRIM(perineural_invasion)) IN ('no','none','negative','absent') THEN 'absent'
      WHEN LOWER(TRIM(perineural_invasion)) IN ('c/a','indeterminate') THEN 'indeterminate'
      WHEN perineural_invasion IS NULL THEN NULL
      ELSE 'other'
    END AS pni_clean,
    
    -- Capsular invasion: 'x' = PRESENT
    CASE 
      WHEN LOWER(TRIM(capsular_invasion)) IN ('x','present','yes','yes;','focal','minimal','widely invasive','minimally invasive','infiltrative') THEN 'present'
      WHEN LOWER(TRIM(capsular_invasion)) IN ('no','none','negative','absent','no;') THEN 'absent'
      WHEN LOWER(TRIM(capsular_invasion)) IN ('c/a','indeterminate','n/s','n/s;','n/a') THEN 'indeterminate'
      WHEN capsular_invasion IS NULL THEN NULL
      ELSE 'other'
    END AS capsular_clean,
    
    -- Capsular subgrade: widely invasive > minimally invasive > focal > present_ungraded
    CASE 
      WHEN LOWER(TRIM(capsular_invasion)) = 'widely invasive' THEN 3
      WHEN LOWER(TRIM(capsular_invasion)) IN ('minimally invasive','minimal') THEN 2
      WHEN LOWER(TRIM(capsular_invasion)) = 'focal' THEN 1
      WHEN LOWER(TRIM(capsular_invasion)) IN ('x','present','yes','yes;','infiltrative') THEN 2
      WHEN LOWER(TRIM(capsular_invasion)) IN ('no','none','negative','absent','no;') THEN 0
      WHEN LOWER(TRIM(capsular_invasion)) IN ('c/a','indeterminate','n/s') THEN 9
      ELSE NULL
    END AS capsular_ordinal,
    
    -- Site parsing for laterality
    CASE 
      WHEN LOWER(TRIM(site)) LIKE '%right lobe%' AND LOWER(TRIM(site)) LIKE '%left lobe%' THEN 'bilateral'
      WHEN LOWER(TRIM(site)) LIKE '%right%' OR LOWER(TRIM(site)) = 'rl' THEN 'right'
      WHEN LOWER(TRIM(site)) LIKE '%left%' OR LOWER(TRIM(site)) = 'll' THEN 'left'
      WHEN LOWER(TRIM(site)) = 'isthmus' THEN 'isthmus'
      ELSE NULL
    END AS lobe_side
    
  FROM thyroid_canonical_publication_v1_0.main.synoptic_tumor_long_v1
),
-- ------------------------------------------------------------------------
-- Step 2: Bring in distance-to-closest-margin from path_synoptics
-- (not in synoptic_tumor_long_v1, only in wide path_synoptics)
-- ------------------------------------------------------------------------
distance_per_patient AS (
  SELECT 
    research_id,
    MIN(TRY_CAST(dist AS DOUBLE)) FILTER (WHERE TRY_CAST(dist AS DOUBLE) IS NOT NULL) AS closest_margin_mm_min,
    MAX(TRY_CAST(dist AS DOUBLE)) FILTER (WHERE TRY_CAST(dist AS DOUBLE) IS NOT NULL) AS closest_margin_mm_max
  FROM (
    SELECT research_id, tumor_1_distance_to_closest_margin_mm AS dist 
      FROM thyroid_canonical_publication_v1_0.main.path_synoptics
    UNION ALL
    SELECT research_id, tumor_2_distance_to_closest_margin_mm FROM thyroid_canonical_publication_v1_0.main.path_synoptics
    UNION ALL
    SELECT research_id, tumor_3_distance_to_closest_margin_mm FROM thyroid_canonical_publication_v1_0.main.path_synoptics
    UNION ALL
    SELECT research_id, tumor_4_distance_to_closest_margin_mm FROM thyroid_canonical_publication_v1_0.main.path_synoptics
    UNION ALL
    SELECT research_id, tumor_5_distance_to_closest_margin_mm FROM thyroid_canonical_publication_v1_0.main.path_synoptics
  ) u
  GROUP BY research_id
),
-- ------------------------------------------------------------------------
-- Step 3: Collapse to one row per patient
-- ------------------------------------------------------------------------
patient_rollup AS (
  SELECT
    tn.research_id,
    
    -- Tumor count & focality ---------------------------------------------
    COUNT(*) AS n_tumors_path,
    COUNT(*) FILTER (WHERE tn.tumor_size_cm IS NOT NULL) AS n_tumors_with_size,
    CAST(COUNT(*) > 1 AS BOOLEAN) AS multifocal_flag_path,
    
    -- Tumor size ---------------------------------------------------------
    -- Dominant = tumor at tumor_index=1 (usually the dominant per synoptic convention)
    MAX(tn.tumor_size_cm) FILTER (WHERE tn.tumor_index = 1) AS tumor_size_cm_dominant,
    MAX(tn.tumor_size_cm) AS tumor_size_cm_max,
    MIN(tn.tumor_size_cm) AS tumor_size_cm_min,
    SUM(tn.tumor_size_cm) AS tumor_size_cm_sum,
    AVG(tn.tumor_size_cm) AS tumor_size_cm_mean,
    
    -- Laterality from tumor sites ----------------------------------------
    CAST(BOOL_OR(tn.lobe_side IN ('right','bilateral')) AS BOOLEAN) AS has_right_tumor,
    CAST(BOOL_OR(tn.lobe_side IN ('left','bilateral')) AS BOOLEAN) AS has_left_tumor,
    CAST(BOOL_OR(tn.lobe_side = 'isthmus') AS BOOLEAN) AS has_isthmus_tumor,
    CAST(BOOL_OR(tn.lobe_side IN ('right','bilateral')) 
         AND BOOL_OR(tn.lobe_side IN ('left','bilateral')) AS BOOLEAN) AS bilateral_path_flag,
    
    -- ETE ----------------------------------------------------------------
    CAST(BOOL_OR(tn.ete_clean = 'present') AS BOOLEAN) AS ete_any_present_path,
    MAX(tn.ete_ordinal) FILTER (WHERE tn.ete_ordinal < 9) AS ete_ordinal_worst,
    COUNT(*) FILTER (WHERE tn.ete_clean = 'present') AS n_tumors_ete_present,
    
    -- Margin status — 'x' = UNINVOLVED -----------------------------------
    CAST(BOOL_OR(tn.margin_clean = 'involved') AS BOOLEAN) AS margin_involved_any,
    CAST(BOOL_AND(tn.margin_clean = 'uninvolved') 
         FILTER (WHERE tn.margin_clean IN ('involved','uninvolved')) AS BOOLEAN) AS margin_all_uninvolved,
    -- Worst margin status: involved > indeterminate > uninvolved
    MAX(CASE tn.margin_clean WHEN 'involved' THEN 2 WHEN 'indeterminate' THEN 1 
                             WHEN 'uninvolved' THEN 0 ELSE NULL END) AS margin_ord_worst,
    COUNT(*) FILTER (WHERE tn.margin_clean = 'involved') AS n_tumors_margin_involved,
    COUNT(*) FILTER (WHERE tn.margin_clean = 'uninvolved') AS n_tumors_margin_uninvolved,
    
    -- LVI ----------------------------------------------------------------
    CAST(BOOL_OR(tn.lvi_clean = 'present') AS BOOLEAN) AS lvi_any_present_path,
    MAX(tn.lvi_ordinal) FILTER (WHERE tn.lvi_ordinal < 9) AS lvi_ordinal_worst,
    COUNT(*) FILTER (WHERE tn.lvi_clean = 'present') AS n_tumors_lvi_present,
    
    -- Vascular invasion / angioinvasion ----------------------------------
    CAST(BOOL_OR(tn.vi_clean = 'present') AS BOOLEAN) AS vi_any_present_path,
    MAX(tn.vi_ordinal) FILTER (WHERE tn.vi_ordinal < 9) AS vi_ordinal_worst,
    COUNT(*) FILTER (WHERE tn.vi_clean = 'present') AS n_tumors_vi_present,
    MAX(tn.vi_count_vessels) AS vi_vessels_max,
    
    -- Perineural invasion ------------------------------------------------
    CAST(BOOL_OR(tn.pni_clean = 'present') AS BOOLEAN) AS pni_any_present_path,
    COUNT(*) FILTER (WHERE tn.pni_clean = 'present') AS n_tumors_pni_present,
    
    -- Capsular invasion --------------------------------------------------
    CAST(BOOL_OR(tn.capsular_clean = 'present') AS BOOLEAN) AS capsular_any_present_path,
    MAX(tn.capsular_ordinal) FILTER (WHERE tn.capsular_ordinal < 9) AS capsular_ordinal_worst,
    
    -- Histologic variants present ----------------------------------------
    STRING_AGG(DISTINCT tn.histologic_variant, ' | ') FILTER (WHERE tn.histologic_variant IS NOT NULL) AS histologic_variants_all,
    STRING_AGG(DISTINCT tn.histologic_type, ' | ') FILTER (WHERE tn.histologic_type IS NOT NULL) AS histologic_types_all,
    
    -- Provenance ---------------------------------------------------------
    'synoptic_tumor_long_v1' AS rollup_source_table,
    '230_path_synoptic_rollup' AS rollup_script_version,
    CURRENT_TIMESTAMP AS rollup_built_at
    
  FROM tumor_normalized tn
  GROUP BY tn.research_id
)

-- ------------------------------------------------------------------------
-- Final: join rollup with distance info
-- ------------------------------------------------------------------------
SELECT 
  pr.*,
  d.closest_margin_mm_min,
  d.closest_margin_mm_max,
  -- Derive R classification from TRUE margin involvement
  CASE 
    WHEN pr.margin_involved_any = TRUE THEN 'R1'
    WHEN pr.margin_all_uninvolved = TRUE THEN 'R0'
    WHEN pr.margin_ord_worst IS NULL THEN NULL
    ELSE 'Rx'  -- indeterminate only
  END AS r_class_true,
  -- Derive 3-level ordinal margin
  CASE pr.margin_ord_worst
    WHEN 2 THEN 'involved'
    WHEN 1 THEN 'indeterminate'
    WHEN 0 THEN 'uninvolved'
    ELSE NULL
  END AS margin_status_true
FROM patient_rollup pr
LEFT JOIN distance_per_patient d ON CAST(d.research_id AS VARCHAR) = pr.research_id;

-- ============================================================================
-- VALIDATION
-- ============================================================================
-- Expected: ~8,422 patients with rollup data (matches synoptic_tumor_long_v1 coverage)

COMMENT ON TABLE thyroid_canonical_publication_v1_0.main.patient_tumor_rollup_v1 IS 
  'Patient-level rollup of synoptic_tumor_long_v1 (5,455 tumor rows for PTC cohort, 11,103 total). Built by script 230. Fixes margin/LVI/multifocal bugs in canonical_patient_master_v221. 8,422 patients with pathology synoptic data. Key decoding: x=PRESENT for ETE/LVI/VI/PNI/capsular; x=UNINVOLVED for margin_status.';
