-- mig_163 ANY-RECURRENCE — READ-ONLY INVESTIGATION PROBES
-- Database: thyroid_canonical_publication_v1_0
-- Do NOT execute as migration; reference / copy-paste into MotherDuck console.
-- Report: qc_framework_v1/reports/mig_163_any_recurrence_investigation_20260429.md

-- Parity
-- SELECT COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_rid
-- FROM main.canonical_patient_master;

-- Canonical confirmed count (STRICT numerator)
-- SELECT COUNT(*) AS n FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE;

-- 2x2 PM vs canonical_recurrence_v1
/*
SELECT
  SUM(CASE WHEN COALESCE(pm.any_recurrence_flag,FALSE) AND COALESCE(cv.recurrence_confirmed,FALSE) THEN 1 ELSE 0 END),
  SUM(CASE WHEN COALESCE(pm.any_recurrence_flag,FALSE) AND NOT COALESCE(cv.recurrence_confirmed,FALSE) THEN 1 ELSE 0 END),
  SUM(CASE WHEN NOT COALESCE(pm.any_recurrence_flag,FALSE) AND COALESCE(cv.recurrence_confirmed,FALSE) THEN 1 ELSE 0 END),
  SUM(CASE WHEN NOT COALESCE(pm.any_recurrence_flag,FALSE) AND NOT COALESCE(cv.recurrence_confirmed,FALSE) THEN 1 ELSE 0 END)
FROM main.canonical_patient_master pm
LEFT JOIN main.canonical_recurrence_v1 cv ON CAST(pm.research_id AS VARCHAR) = cv.research_id;
*/

-- §3.1 Profile pattern (repeat flag_expr for structural_recurrence_flag, distant_mets_proxy, biochemical_recurrence_flag)
/*
WITH crr_path AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='path_proven'
),
crr_imaging AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='imaging_only_unconfirmed'
),
cr_conf AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE
),
pm AS (
  SELECT CAST(research_id AS VARCHAR) AS rid,
         structural_recurrence_flag, biochemical_recurrence_flag, distant_mets_proxy
  FROM main.canonical_patient_master
)
SELECT ... -- see report
*/
