-- ============================================================================
-- SQL Queries Used in THYROID_2026 ETE Staging Manuscript
-- Forensic extraction: 2026-03-18
-- Database: thyroid_research_2026 (MotherDuck)
-- ============================================================================

-- ============================================================================
-- 1. PTC COHORT VIEW (scripts/03_research_views.py)
-- Purpose: Base PTC population for all analyses
-- ============================================================================
CREATE OR REPLACE VIEW ptc_cohort AS
SELECT
    ps.research_id,
    ps.surg_date,
    ps.age,
    ps.gender,
    ps.race,
    ps.tumor_1_histologic_type,
    ps.tumor_1_size_greatest_dimension_cm,
    ps.tumor_1_extrathyroidal_extension,
    ps.tumor_1_gross_ete,
    ps.tumor_1_ete_microscopic_only,
    ps.tumor_1_ln_involved,
    ps.tumor_1_level_examined,
    ps.overall_stage_ajcc8,
    ps.thyroid_procedure,
    tp.histology_1_type,
    tp.histology_1_largest_tumor_cm,
    tp.histology_1_ln_positive,
    tp.histology_1_ln_examined
FROM path_synoptics ps
LEFT JOIN tumor_pathology tp ON ps.research_id = tp.research_id
WHERE tp.histology_1_type = 'PTC';

-- ============================================================================
-- 2. RECURRENCE RISK COHORT VIEW (scripts/03_research_views.py)
-- Purpose: PTC patients with risk stratification
-- ============================================================================
CREATE OR REPLACE VIEW recurrence_risk_cohort AS
SELECT
    pc.*,
    CASE
        WHEN pc.overall_stage_ajcc8 ILIKE 'III%'
             OR pc.overall_stage_ajcc8 ILIKE 'IV%'
             OR pc.tumor_1_gross_ete IS NOT NULL
        THEN 'high'
        WHEN pc.overall_stage_ajcc8 ILIKE 'II%'
        THEN 'intermediate'
        ELSE 'low'
    END AS recurrence_risk_band
FROM ptc_cohort pc;

-- ============================================================================
-- 3. RECURRENCE RISK FEATURES MV (scripts/10_materialized_features.py)
-- Purpose: Materialized patient-level risk features
-- Note: Has up to 25 rows per research_id; must be deduplicated
-- ============================================================================
CREATE OR REPLACE TABLE recurrence_risk_features_mv AS
SELECT
    research_id,
    recurrence_flag,
    first_recurrence_date,
    recurrence_risk_band,
    braf_positive,
    ras_positive,
    tg_first,
    tg_last,
    tg_max,
    tg_mean,
    tg_measurement_count,
    tg_annual_log_slope,
    ln_positive,
    ln_examined,
    ln_ratio
FROM recurrence_risk_cohort;
-- N ≈ 4,976 rows; 3,986 unique patients

-- ============================================================================
-- 4. RISK ENRICHED MV (scripts/13_performance_optimizations_pack.py)
-- Purpose: Final unified analysis table — feeds Cox/KM and expanded analyses
-- Manuscript role: AUTHORITATIVE upstream source
-- ============================================================================
CREATE OR REPLACE TABLE risk_enriched_mv AS
SELECT
    r.*,
    s.time_to_event_days,
    s.event_occurred,
    s.age_at_diagnosis,
    s.sex,
    s.overall_stage_ajcc8 AS stage_ajcc8,
    s.histology_1_type
FROM recurrence_risk_features_mv r
LEFT JOIN survival_cohort_ready_mv s
    ON r.research_id = s.research_id;

-- ============================================================================
-- 5. COX PH QUERY (proposal2_cox_regression.py)
-- Purpose: Load data for Cox proportional hazards model
-- ============================================================================
SELECT
    research_id,
    recurrence_risk_band,
    braf_positive,
    time_to_event_days,
    event_occurred,
    age_at_diagnosis,
    sex,
    stage_ajcc8,
    histology_1_type,
    tg_max,
    ln_ratio
FROM risk_enriched_mv
WHERE time_to_event_days > 0
  AND event_occurred IS NOT NULL;
-- N ≈ 5,794

-- ============================================================================
-- 6. MASTER PTC COHORT DENOMINATOR
-- Purpose: Verify total PTC population
-- ============================================================================
SELECT COUNT(DISTINCT research_id) AS ptc_total
FROM ptc_cohort;
-- Expected: ~6,630

-- ============================================================================
-- 7. EXPANDED COHORT (N=3,278) — reconstructed from CSV merge logic
-- Purpose: Expanded analysis population
-- Note: Actual construction is in Python (load_all_ptc) merging 3 CSVs
-- ============================================================================
-- The expanded cohort is derived by:
-- 1. Export ptc_full.csv from ptc_cohort
-- 2. Export recurrence_full.csv from recurrence_risk_features_mv (deduped)
-- 3. Export imaging_correlation.csv from imaging proxy data
-- 4. Merge in Python on research_id
-- 5. Deduplicate → N=3,278

-- Equivalent SQL (approximate):
SELECT DISTINCT ON (r.research_id)
    r.research_id,
    r.recurrence_risk_band,
    r.recurrence_flag,
    r.tg_max,
    r.ln_positive,
    r.ln_examined,
    r.ln_ratio,
    r.braf_positive,
    r.ras_positive,
    pc.age,
    pc.gender,
    pc.tumor_1_histologic_type,
    pc.tumor_1_size_greatest_dimension_cm,
    pc.tumor_1_extrathyroidal_extension,
    pc.tumor_1_gross_ete,
    pc.tumor_1_ete_microscopic_only,
    pc.overall_stage_ajcc8,
    pc.surg_date
FROM recurrence_risk_features_mv r
INNER JOIN ptc_cohort pc ON r.research_id = pc.research_id
ORDER BY r.research_id, r.tg_max DESC NULLS LAST;

-- ============================================================================
-- 8. COMPLETE-CASE ORDINAL MODEL COHORT (N≈593)
-- Purpose: Patients with non-missing ordinal regression covariates
-- ============================================================================
-- From the expanded cohort (or primary classic cohort):
-- WHERE ete_micro IS NOT NULL
--   AND ete_gross IS NOT NULL
--   AND age_at_surgery IS NOT NULL
--   AND female IS NOT NULL
--   AND largest_tumor_cm IS NOT NULL
--   AND ln_ratio IS NOT NULL
--   AND risk_ord IS NOT NULL

-- ============================================================================
-- 9. PSM ELIGIBLE POOL (N≈2,460)
-- Purpose: mETE vs NoETE patients eligible for propensity matching
-- ============================================================================
-- From expanded cohort:
-- WHERE ete_category IN ('No ETE', 'Microscopic ETE')  -- exclude Gross ETE
--   AND age_at_surgery IS NOT NULL
--   AND female IS NOT NULL
--   AND largest_tumor_cm IS NOT NULL
--   AND n_positive_flag IS NOT NULL

-- ============================================================================
-- 10. STAGE MIGRATION COHORT (N≈3,269)
-- Purpose: Patients with both AJCC7 and AJCC8 stages derivable
-- ============================================================================
-- From expanded cohort:
-- WHERE ajcc7_stage IS NOT NULL
--   AND ajcc8_stage IS NOT NULL
--   AND ajcc7_stage != 'Unknown'
--   AND ajcc8_stage != 'Unknown'

-- ============================================================================
-- 11. STRUCTURAL ENDPOINT DEFINITION
-- Purpose: Composite structural recurrence endpoint
-- ============================================================================
-- structural_recurrence = CASE
--     WHEN ct_pathologic_ln_flag = 1 THEN 1
--     WHEN surgery_count > 1 THEN 1
--     ELSE 0
-- END
-- Derived in Python from imaging_correlation.csv + surgery date counts

-- ============================================================================
-- 12. CT TIMING (NOT TEMPORAL — patient-level binary only)
-- Purpose: Clarify that CT timing intervals are NOT available
-- ============================================================================
-- The structural endpoint is a patient-level binary.
-- There are no ≤30d / 31-365d / >365d temporal breakdowns
-- in the frozen manuscript dataset.
-- CT timing columns in the export are set to NULL.
