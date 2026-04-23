-- ============================================================================
-- Migration 25 — TIR01/TIR02: ACR 2017 band + concordance guard
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     TIR01 (band vs points) + TIR02 (concordance flag vs categorical equality)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- ACR 2017 band thresholds: TR1=0, TR2=2, TR3=3, TR4=4-6, TR5=7+
--
-- Registry: 0 rows fail either rule currently. This view is a REGRESSION GUARD
-- so that future data loads or rebuilds cannot silently introduce band errors.
--
-- Output:
--   manuscript_workspace.canonical_us_nodule_v2_tirads_guard
--     + tirads_band_expected
--     + tirads_band_mismatch_flag (TIR01)
--     + tirads_concordance_mismatch_flag (TIR02)
--
-- Verified 2026-04-23: band_mm=0, conc_mm=0 on 37,579 rows.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_us_nodule_v2_tirads_guard AS
SELECT
  n.*,
  CASE
    WHEN n.acr2017_tirads_points IS NULL THEN NULL
    WHEN n.acr2017_tirads_points = 0 THEN 'TR1'
    WHEN n.acr2017_tirads_points = 2 THEN 'TR2'
    WHEN n.acr2017_tirads_points = 3 THEN 'TR3'
    WHEN n.acr2017_tirads_points BETWEEN 4 AND 6 THEN 'TR4'
    WHEN n.acr2017_tirads_points >= 7 THEN 'TR5'
    ELSE NULL
  END AS tirads_band_expected,
  (CASE
    WHEN n.acr2017_tirads_points IS NULL THEN NULL
    WHEN n.acr2017_tirads_points = 0 THEN 'TR1'
    WHEN n.acr2017_tirads_points = 2 THEN 'TR2'
    WHEN n.acr2017_tirads_points = 3 THEN 'TR3'
    WHEN n.acr2017_tirads_points BETWEEN 4 AND 6 THEN 'TR4'
    WHEN n.acr2017_tirads_points >= 7 THEN 'TR5'
    ELSE NULL
  END IS NOT NULL
   AND n.acr2017_tirads_category IS NOT NULL
   AND (CASE
    WHEN n.acr2017_tirads_points = 0 THEN 'TR1'
    WHEN n.acr2017_tirads_points = 2 THEN 'TR2'
    WHEN n.acr2017_tirads_points = 3 THEN 'TR3'
    WHEN n.acr2017_tirads_points BETWEEN 4 AND 6 THEN 'TR4'
    WHEN n.acr2017_tirads_points >= 7 THEN 'TR5'
    END) <> n.acr2017_tirads_category) AS tirads_band_mismatch_flag,
  (n.acr2017_vs_updated_concordant IS NOT NULL
     AND n.acr2017_tirads_category IS NOT NULL
     AND n.updated_tirads_category IS NOT NULL
     AND ((n.acr2017_tirads_category = n.updated_tirads_category AND NOT n.acr2017_vs_updated_concordant)
          OR (n.acr2017_tirads_category <> n.updated_tirads_category AND n.acr2017_vs_updated_concordant)))
    AS tirads_concordance_mismatch_flag
FROM main.canonical_us_nodule_v2 n;

COMMENT ON COLUMN main.canonical_us_nodule_v2.acr2017_tirads_category IS
'ACR 2017 TIRADS band. Guard view manuscript_workspace.canonical_us_nodule_v2_tirads_guard validates band against acr2017_tirads_points (TIR01) and cross-checks concordance flag against categorical equality (TIR02). 0 violations at cohort close 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_24';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_us_nodule_v2.(acr2017_tirads_category,acr2017_tirads_points,acr2017_vs_updated_concordant)','column_group',
   'manuscript_workspace.canonical_us_nodule_v2_tirads_guard',
   'TIR01,TIR02','prompt_24','column_only',DATE '2026-04-23',
   '0 TIR01 band mismatches and 0 TIR02 concordance mismatches on 37,579 rows at cohort close. View is a guard against regression as new data lands.',
   NULL,
   'tirads_band_expected + tirads_band_mismatch_flag + tirads_concordance_mismatch_flag. No queue emissions unless flags fire.');
