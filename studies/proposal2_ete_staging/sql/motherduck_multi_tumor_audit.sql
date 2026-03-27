-- MotherDuck multi-tumor pathology audit (canonical lineage)
-- Target DB: thyroid_research_2026 (prod) — path_synoptics, synoptic_tumor_long_v1,
--   tumor_episode_master_v2, extracted_multi_tumor_aggregate_v1, tumor_pathology,
--   lesion_analysis_resolved_v1 (optional).
--
-- Slot nonempty = OR over all path_synoptics columns listed in scripts/108 SLOT_MAP
-- (implementation generated in run_motherduck_multi_tumor_audit.py for schema drift).
--
-- Lineage:
--   All Diagnoses Excel -> path_synoptics (specimen / surgery-level wide rows)
--   path_synoptics -> scripts/108 -> synoptic_tumor_long_v1 (one row per occupied focus)
--   path_synoptics -> scripts/22 -> tumor_episode_master_v2 (tumor_ordinal fixed = 1 only)
--   path_synoptics -> Phase 10 v8 -> extracted_multi_tumor_aggregate_v1 (patient-level;
--        latest path_synoptics row per patient; n_tumors = histology-nonempty slots)
--   tumor_episode_master_v2 -> scripts/48 -> lesion_analysis_resolved_v1 (inherits single-ordinal limit)
--   scripts/03 ptc_cohort / exports/ptc_full.csv -> proposal2_ete_analysis.py
--        (tumor_1_extrathyroidal_ext, largest_tumor_cm from tumor_1 / tumor_pathology join)

-- Example: pathology-level slot distribution (after Python injects slot expressions into _mt_ps)
/*
CREATE OR REPLACE TEMP VIEW _mt_ps AS
SELECT
  research_id,
  surg_date,
  TRY_CAST(surg_date AS DATE) AS surg_d,
  ... AS n_slots_any,
  ... AS n_slots_histology_only,
  ...
FROM path_synoptics;

SELECT n_slots_any, COUNT(*) AS n_specimens
FROM _mt_ps
GROUP BY 1 ORDER BY 1;
*/
