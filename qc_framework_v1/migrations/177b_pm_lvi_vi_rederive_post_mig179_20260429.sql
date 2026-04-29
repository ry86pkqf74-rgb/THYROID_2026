-- mig_177b — PM lvi_*/vi_* re-derive against refreshed canonical_invasion_events_v1 (post-mig_179)
-- Closes CF-mig177-PM-VASC-ALIAS-LVI (196 pts) + CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT (vi axis).
-- Logan-ratified R2 per exports/mig176_177_174_review_20260429/README.md per-bucket call.
--
-- Posture: Cowork applies. Data writes on canonical_patient_master (4 cols) + registry notes.
-- Pre-snapshot affected slice. Pre-flight: cohort parity 10,871; mig_179 events rebuild verified.
--
-- Target DB: thyroid_canonical_publication_v1_0
-- Methodology: derivation_vs_canonical_invasion_events_v1 (re-applied against refreshed source)
--
-- TIER 1 SCOPE (4 BOOLEAN cols):
--   lvi_any_present_path: 3,392 → 989 (TRUE→FALSE: 2,502; FALSE/NULL→TRUE: 99)
--   vi_any_present_path:  3,698 → 1,178 (TRUE→FALSE: 2,580; FALSE/NULL→TRUE: 60)
--   any_lymphatic_microscopic_anywhere (rollup snapshot): 780 → 989
--   any_vascular_microscopic_anywhere  (rollup snapshot): 1,109 → 1,178
--
-- DEFERRED TO LATER LANE (CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN):
--   lvi_grade, lvi_ordinal_worst, n_tumors_lvi_present (2,502 TRUE→FALSE flippers retain
--   non-null derivative values that no longer match lvi_any_present_path=FALSE)
--   vasc_grade, vasc_grade_final_v13, vasc_vessel_count_v13, vascular_invasion_final,
--   vascular_invasion_grade, vascular_vessel_count, vascular_who_2022_grade, vi_ordinal_worst,
--   vi_vessels_max, vasc_confidence_final_v13, vasc_source_final_v13 (similar issue on vasc side)
--   These need separate Logan ratification on extent/grade re-derivation rule for new TRUE flippers.

USE thyroid_canonical_publication_v1_0;

-- §A pre-snapshot
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 AS
SELECT
    research_id,
    lvi_any_present_path,
    vi_any_present_path,
    any_lymphatic_microscopic_anywhere,
    any_vascular_microscopic_anywhere,
    lvi_grade,
    lvi_ordinal_worst,
    n_tumors_lvi_present,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig177b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- §B Tier-1 re-derive: lvi_any_present_path against refreshed events
UPDATE main.canonical_patient_master pm
SET lvi_any_present_path = EXISTS (
    SELECT 1
    FROM main.canonical_invasion_events_v1 e
    WHERE e.research_id = pm.research_id
      AND e.invasion_type = 'lymphatic_microscopic'
      AND e.finding_status = 'present'
);

-- §C Tier-1 re-derive: vi_any_present_path against refreshed events
UPDATE main.canonical_patient_master pm
SET vi_any_present_path = EXISTS (
    SELECT 1
    FROM main.canonical_invasion_events_v1 e
    WHERE e.research_id = pm.research_id
      AND e.invasion_type = 'vascular_microscopic'
      AND e.finding_status = 'present'
);

-- §D Tier-1 re-sync: any_lymphatic_microscopic_anywhere from refreshed rollup
UPDATE main.canonical_patient_master pm
SET any_lymphatic_microscopic_anywhere = (
    SELECT COALESCE(r.any_lymphatic_microscopic_anywhere, FALSE)
    FROM main.canonical_invasion_patient_rollup_v1 r
    WHERE r.research_id = pm.research_id
);

-- §E Tier-1 re-sync: any_vascular_microscopic_anywhere from refreshed rollup
UPDATE main.canonical_patient_master pm
SET any_vascular_microscopic_anywhere = (
    SELECT COALESCE(r.any_vascular_microscopic_anywhere, FALSE)
    FROM main.canonical_invasion_patient_rollup_v1 r
    WHERE r.research_id = pm.research_id
);

-- §F Registry note appendix on the 4 touched cols
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_177b: PM lvi_*/vi_* re-derived against refreshed canonical_invasion_events_v1 (post-mig_179) 2026-04-29. ' ||
            'lvi_any_present_path: 3,392→989 (TRUE→FALSE: 2,502 PM-vasc-alias-LVI cleanups; FALSE/NULL→TRUE: 99 from mig_179 supplemental). ' ||
            'vi_any_present_path: 3,698→1,178 (TRUE→FALSE: 2,580; FALSE/NULL→TRUE: 60 from mig_179 combined-CAP duplication catches). ' ||
            'any_lymphatic_microscopic_anywhere/any_vascular_microscopic_anywhere: stale PM snapshot copies of rollup synced (780→989; 1,109→1,178). ' ||
            'Closes CF-mig177-PM-VASC-ALIAS-LVI (196 pts) + CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT (vi axis). ' ||
            'NEW open CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN: 2,502 LVI + 2,580 VI TRUE→FALSE flippers retain non-null derivative values (lvi_grade/lvi_ordinal_worst/n_tumors_lvi_present + vasc grade family) that no longer match the cleared boolean. Defer to follow-up lane after Logan ratifies extent re-derivation rule.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name IN ('lvi_any_present_path','vi_any_present_path','any_lymphatic_microscopic_anywhere','any_vascular_microscopic_anywhere');

-- §G Post-state verification probes (read-only)
SELECT
  'POST_LVI_TRUE_COUNT' AS gate,
  COUNT(*) FILTER (WHERE lvi_any_present_path=TRUE) AS metric,
  CASE WHEN COUNT(*) FILTER (WHERE lvi_any_present_path=TRUE) = 989 THEN 'PASS' ELSE 'FAIL' END AS status
FROM main.canonical_patient_master
UNION ALL
SELECT
  'POST_VI_TRUE_COUNT',
  COUNT(*) FILTER (WHERE vi_any_present_path=TRUE),
  CASE WHEN COUNT(*) FILTER (WHERE vi_any_present_path=TRUE) = 1178 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master
UNION ALL
SELECT
  'POST_ALMA_SYNC',
  COUNT(*) FILTER (WHERE any_lymphatic_microscopic_anywhere=TRUE),
  CASE WHEN COUNT(*) FILTER (WHERE any_lymphatic_microscopic_anywhere=TRUE) = 989 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master
UNION ALL
SELECT
  'POST_AVMA_SYNC',
  COUNT(*) FILTER (WHERE any_vascular_microscopic_anywhere=TRUE),
  CASE WHEN COUNT(*) FILTER (WHERE any_vascular_microscopic_anywhere=TRUE) = 1178 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master
UNION ALL
SELECT
  'POST_PM_LVI_VS_AVMA_PARITY',
  COUNT(*) FILTER (WHERE COALESCE(lvi_any_present_path,FALSE) IS DISTINCT FROM COALESCE(any_lymphatic_microscopic_anywhere,FALSE)),
  CASE WHEN COUNT(*) FILTER (WHERE COALESCE(lvi_any_present_path,FALSE) IS DISTINCT FROM COALESCE(any_lymphatic_microscopic_anywhere,FALSE)) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master
UNION ALL
SELECT
  'POST_COHORT_INVARIANT',
  COUNT(*),
  CASE WHEN COUNT(*) = 10871 AND COUNT(DISTINCT research_id) = 10871 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master;
