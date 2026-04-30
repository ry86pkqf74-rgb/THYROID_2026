-- mig_177c — LVI+VI derivative reclean scoping only
-- Posture: READ-ONLY probes. No DDL/DML. Logan ratifies Option A clear vs Option B rederive before any apply.
-- Target DB: thyroid_canonical_publication_v1_0
-- Context: mig_177b rederived LVI/VI booleans against refreshed canonical_invasion_events_v1.
-- Scope: Surface derivative inconsistency for 2,502 LVI + 2,580 VI TRUE->FALSE flippers.

USE thyroid_canonical_publication_v1_0;
USE thyroid_canonical_publication_v1_0.main;

-- §A. Flipper counts against mig_177b pre-snapshot.
WITH joined AS (
    SELECT
        CAST(pm.research_id AS VARCHAR) AS research_id,
        pre.lvi_any_present_path AS pre_lvi_any_present_path,
        pm.lvi_any_present_path,
        pre.vi_any_present_path AS pre_vi_any_present_path,
        pm.vi_any_present_path
    FROM main.canonical_patient_master pm
    JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
      ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
)
SELECT
    'lvi' AS family,
    COUNT(*) FILTER (WHERE COALESCE(pre_lvi_any_present_path, FALSE)) AS pre_true,
    COUNT(*) FILTER (WHERE COALESCE(lvi_any_present_path, FALSE)) AS post_true,
    COUNT(*) FILTER (WHERE COALESCE(pre_lvi_any_present_path, FALSE) AND NOT COALESCE(lvi_any_present_path, FALSE)) AS true_to_false_flippers,
    COUNT(*) FILTER (WHERE NOT COALESCE(pre_lvi_any_present_path, FALSE) AND COALESCE(lvi_any_present_path, FALSE)) AS false_or_null_to_true_flippers
FROM joined
UNION ALL
SELECT
    'vi' AS family,
    COUNT(*) FILTER (WHERE COALESCE(pre_vi_any_present_path, FALSE)) AS pre_true,
    COUNT(*) FILTER (WHERE COALESCE(vi_any_present_path, FALSE)) AS post_true,
    COUNT(*) FILTER (WHERE COALESCE(pre_vi_any_present_path, FALSE) AND NOT COALESCE(vi_any_present_path, FALSE)) AS true_to_false_flippers,
    COUNT(*) FILTER (WHERE NOT COALESCE(pre_vi_any_present_path, FALSE) AND COALESCE(vi_any_present_path, FALSE)) AS false_or_null_to_true_flippers
FROM joined;

-- §B. Option A clear-only scope by derivative column on TRUE->FALSE flippers.
WITH lvi_flippers AS (
    SELECT pm.*
    FROM main.canonical_patient_master pm
    JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
      ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    WHERE COALESCE(pre.lvi_any_present_path, FALSE) = TRUE
      AND COALESCE(pm.lvi_any_present_path, FALSE) = FALSE
),
vi_flippers AS (
    SELECT pm.*
    FROM main.canonical_patient_master pm
    JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
      ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    WHERE COALESCE(pre.vi_any_present_path, FALSE) = TRUE
      AND COALESCE(pm.vi_any_present_path, FALSE) = FALSE
)
SELECT 'lvi' AS family, 'lvi_grade' AS column_name, COUNT(*) AS flippers, COUNT(lvi_grade) AS non_null_on_flippers, COUNT(*) FILTER (WHERE TRIM(COALESCE(lvi_grade,'')) <> '') AS non_zero_or_non_blank_on_flippers, 'set_to_null' AS option_a_clear_target FROM lvi_flippers
UNION ALL SELECT 'lvi','lvi_ordinal_worst',COUNT(*),COUNT(lvi_ordinal_worst),COUNT(*) FILTER (WHERE COALESCE(lvi_ordinal_worst,0) <> 0),'set_to_null' FROM lvi_flippers
UNION ALL SELECT 'lvi','n_tumors_lvi_present',COUNT(*),COUNT(n_tumors_lvi_present),COUNT(*) FILTER (WHERE COALESCE(n_tumors_lvi_present,0) <> 0),'set_to_zero' FROM lvi_flippers
UNION ALL SELECT 'vi','vasc_grade',COUNT(*),COUNT(vasc_grade),COUNT(*) FILTER (WHERE TRIM(COALESCE(vasc_grade,'')) <> ''),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vasc_grade_final_v13',COUNT(*),COUNT(vasc_grade_final_v13),COUNT(*) FILTER (WHERE TRIM(COALESCE(vasc_grade_final_v13,'')) <> ''),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vascular_invasion_final',COUNT(*),COUNT(vascular_invasion_final),COUNT(*) FILTER (WHERE TRIM(COALESCE(vascular_invasion_final,'')) <> ''),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vascular_invasion_grade',COUNT(*),COUNT(vascular_invasion_grade),COUNT(*) FILTER (WHERE TRIM(COALESCE(vascular_invasion_grade,'')) <> ''),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vascular_who_2022_grade',COUNT(*),COUNT(vascular_who_2022_grade),COUNT(*) FILTER (WHERE TRIM(COALESCE(vascular_who_2022_grade,'')) <> ''),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vi_ordinal_worst',COUNT(*),COUNT(vi_ordinal_worst),COUNT(*) FILTER (WHERE COALESCE(vi_ordinal_worst,0) <> 0),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vasc_vessel_count_v13',COUNT(*),COUNT(vasc_vessel_count_v13),COUNT(*) FILTER (WHERE COALESCE(vasc_vessel_count_v13,0) <> 0),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vascular_vessel_count',COUNT(*),COUNT(vascular_vessel_count),COUNT(*) FILTER (WHERE COALESCE(vascular_vessel_count,0) <> 0),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vi_vessels_max',COUNT(*),COUNT(vi_vessels_max),COUNT(*) FILTER (WHERE COALESCE(vi_vessels_max,0) <> 0),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vasc_confidence_final_v13',COUNT(*),COUNT(vasc_confidence_final_v13),COUNT(*) FILTER (WHERE COALESCE(vasc_confidence_final_v13,0) <> 0),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','vasc_source_final_v13',COUNT(*),COUNT(vasc_source_final_v13),COUNT(*) FILTER (WHERE TRIM(COALESCE(vasc_source_final_v13,'')) <> ''),'set_to_null' FROM vi_flippers
UNION ALL SELECT 'vi','n_tumors_vi_present',COUNT(*),COUNT(n_tumors_vi_present),COUNT(*) FILTER (WHERE COALESCE(n_tumors_vi_present,0) <> 0),'set_to_zero' FROM vi_flippers
ORDER BY family, column_name;

-- §C. Event context on the TRUE->FALSE flippers. These should have zero PRESENT rows for the corresponding strict axis.
WITH flippers AS (
    SELECT 'lvi' AS family, CAST(pm.research_id AS VARCHAR) AS research_id, 'lymphatic_microscopic' AS invasion_type
    FROM main.canonical_patient_master pm
    JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
      ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    WHERE COALESCE(pre.lvi_any_present_path, FALSE) = TRUE
      AND COALESCE(pm.lvi_any_present_path, FALSE) = FALSE
    UNION ALL
    SELECT 'vi', CAST(pm.research_id AS VARCHAR), 'vascular_microscopic'
    FROM main.canonical_patient_master pm
    JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
      ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    WHERE COALESCE(pre.vi_any_present_path, FALSE) = TRUE
      AND COALESCE(pm.vi_any_present_path, FALSE) = FALSE
)
SELECT
    f.family,
    f.invasion_type,
    COALESCE(e.finding_status, '<NO_EVENT>') AS finding_status,
    COUNT(DISTINCT f.research_id) AS n_patients,
    COUNT(e.invasion_event_id) AS n_events
FROM flippers f
LEFT JOIN main.canonical_invasion_events_v1 e
  ON CAST(e.research_id AS VARCHAR) = f.research_id
 AND e.invasion_type = f.invasion_type
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- §D. Option B blocker: current event schema has finding status but no grade/ordinal/vessel-count lineage columns.
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'main'
  AND table_name = 'canonical_invasion_events_v1'
  AND column_name IN ('invasion_ordinal_grade', 'vessel_count', 'vascular_vessel_count', 'grade', 'invasion_grade', 'finding_status', 'evidence_qualifier')
ORDER BY column_name;
