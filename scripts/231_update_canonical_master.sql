-- ============================================================================
-- Script 231: Update canonical_patient_master with Fixed Pathology Rollup
-- Repository: https://github.com/ry86pkqf74-rgb/THYROID_2026
-- Database: thyroid_canonical_publication_v1_0 on MotherDuck
--
-- Purpose: Create canonical_patient_master_v222 by joining the existing
--          v221 with patient_tumor_rollup_v1 and replacing/adding the 
--          corrected columns.
--
-- Strategy: Non-destructive. We:
--   1. Snapshot current canonical_patient_master to _v221_backup (if not exists)
--   2. Build canonical_patient_master_v222 as v221 + new columns
--   3. Swap the `canonical_patient_master` alias via CREATE OR REPLACE TABLE
--   4. Previous version v221 stays for audit trail
--
-- NEW COLUMNS ADDED (pathology-rollup-derived):
--   tumor_size_cm_dominant, tumor_size_cm_max, tumor_size_cm_min,
--   tumor_size_cm_sum, tumor_size_cm_mean
--   n_tumors_path (true count, not 0/NULL)
--   multifocal_flag_path (bool, was 0% populated)
--   bilateral_path_flag (bool, improved coverage)
--   has_right_tumor, has_left_tumor, has_isthmus_tumor
--   ete_any_present_path, ete_ordinal_worst, n_tumors_ete_present
--   margin_involved_any, margin_all_uninvolved, r_class_true, margin_status_true
--   n_tumors_margin_involved, n_tumors_margin_uninvolved
--   closest_margin_mm_min, closest_margin_mm_max
--   lvi_any_present_path, lvi_ordinal_worst, n_tumors_lvi_present
--   vi_any_present_path, vi_ordinal_worst, n_tumors_vi_present, vi_vessels_max
--   pni_any_present_path, n_tumors_pni_present
--   capsular_any_present_path, capsular_ordinal_worst
--   histologic_variants_all, histologic_types_all
--   rollup_source_table, rollup_script_version, rollup_built_at
--
-- DEPRECATED COLUMNS (kept for audit, but DO NOT USE):
--   margin_status_final, margin_r_class  (buggy: conflated with ETE)
--   lvi_grade_final_v13                  (buggy: collapsed)
--   path_multifocal_flag, multifocal_flag (0% populated)
--   path_n_tumors                        (0% populated)
--   max_tumor_size_cm_v10                (35% coverage vs 99.8% in tumor_size_cm)
--
-- Use the _TRUE versions going forward:
--   margin: r_class_true, margin_status_true, margin_involved_any
--   lvi:    lvi_any_present_path, lvi_ordinal_worst
--   size:   tumor_size_cm (existing, unchanged) OR tumor_size_cm_max (per-tumor max)
--   multi:  multifocal_flag_path, n_tumors_path
-- ============================================================================

-- Step 1: Backup v221 if not already done (idempotent)
CREATE TABLE IF NOT EXISTS thyroid_canonical_publication_v1_0.main.canonical_patient_master_v221_backup AS
SELECT * FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- Step 2: Build v222
DROP TABLE IF EXISTS thyroid_canonical_publication_v1_0.main.canonical_patient_master_v222;
CREATE TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master_v222 AS
SELECT 
  cpm.*,
  -- ---- New columns from patient_tumor_rollup_v1 --------------------------
  -- Tumor count & focality
  r.n_tumors_path,
  r.n_tumors_with_size,
  r.multifocal_flag_path,
  
  -- Tumor size (multi-lesion aware)
  r.tumor_size_cm_dominant,
  r.tumor_size_cm_max,
  r.tumor_size_cm_min,
  r.tumor_size_cm_sum,
  r.tumor_size_cm_mean,
  
  -- Laterality
  r.has_right_tumor,
  r.has_left_tumor,
  r.has_isthmus_tumor,
  r.bilateral_path_flag,
  
  -- ETE (any-tumor, worst-tumor)
  r.ete_any_present_path,
  r.ete_ordinal_worst,
  r.n_tumors_ete_present,
  
  -- Margin — TRUE values (replaces buggy margin_status_final/margin_r_class)
  r.margin_involved_any,
  r.margin_all_uninvolved,
  r.margin_ord_worst,
  r.r_class_true,
  r.margin_status_true,
  r.n_tumors_margin_involved,
  r.n_tumors_margin_uninvolved,
  r.closest_margin_mm_min,
  r.closest_margin_mm_max,
  
  -- LVI — TRUE values (replaces buggy lvi_grade_final_v13)
  r.lvi_any_present_path,
  r.lvi_ordinal_worst,
  r.n_tumors_lvi_present,
  
  -- Vascular (angio) invasion
  r.vi_any_present_path,
  r.vi_ordinal_worst,
  r.n_tumors_vi_present,
  r.vi_vessels_max,
  
  -- Perineural invasion (patient-level any-positive)
  r.pni_any_present_path,
  r.n_tumors_pni_present,
  
  -- Capsular invasion (patient-level any-positive)
  r.capsular_any_present_path,
  r.capsular_ordinal_worst,
  
  -- Variants
  r.histologic_variants_all,
  r.histologic_types_all,
  
  -- Provenance
  r.rollup_source_table,
  r.rollup_script_version,
  r.rollup_built_at

FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master cpm
LEFT JOIN thyroid_canonical_publication_v1_0.main.patient_tumor_rollup_v1 r 
  USING (research_id);

-- Step 3: Swap alias
-- Must keep canonical_patient_master as the alias that analyses use
DROP TABLE IF EXISTS thyroid_canonical_publication_v1_0.main.canonical_patient_master_v221;
CREATE TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master_v221 AS
SELECT * FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master_v221_backup;

DROP TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master;
CREATE TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master AS
SELECT * FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master_v222;

COMMENT ON TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master IS
  'Master analytical table: 10,871 patients × ~1,335+ columns. v222 built 2026-04 from v221 + patient_tumor_rollup_v1. Fixes margin/LVI/multifocal bugs. Use r_class_true, margin_status_true (not margin_r_class/margin_status_final), lvi_ordinal_worst (not lvi_grade_final_v13), multifocal_flag_path (not multifocal_flag). See script 230/231 for details.';

COMMENT ON TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master_v221 IS
  'DEPRECATED. Retained for audit. Known bugs: margin_status_final/margin_r_class incorrectly labeled R1 for anyone with mETE (actual R0 rate is ~84%). lvi_grade_final_v13 collapsed 92-95% to "present_ungraded". path_multifocal_flag/multifocal_flag 100% NULL. Use canonical_patient_master (v222) going forward.';
