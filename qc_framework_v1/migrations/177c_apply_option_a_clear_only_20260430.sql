-- =============================================================================
-- Migration 177c apply — Option A clear-only for LVI/VI derivative flippers
-- =============================================================================
-- Date: 2026-04-30
-- Batch: mig_177c_apply_option_a_clear_only_20260430
-- Target DB: thyroid_canonical_publication_v1_0
-- Primary table touched: main.canonical_patient_master
-- Registry touched: main.canonical_column_verification_registry_v1 (notes only)
-- Provenance touched: manuscript_workspace.cpm_reconciliation_provenance_v1
--
-- Logan-ratified scope:
--   Clear stale derivative fields only for mig_177b TRUE->FALSE flippers:
--     * LVI: 2,502 patients; 3 derivative columns; 7,464 non-null cells.
--     * VI:  2,580 patients; 12 derivative columns; 20,635 non-null cells.
--   Total expected cleared derivative cells: 28,099.
--
-- Governance:
--   Path-C apply artifact. Pre-snapshot is mandatory before UPDATEs.
--   No BEGIN TRANSACTION / COMMIT wrapper per prompt.
--   Do not touch the 99 LVI + 60 VI FALSE/NULL->TRUE flippers; these are
--   opened as CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS for future Option B.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;
USE thyroid_canonical_publication_v1_0.main;

-- §0 — pre-flight invariants and flipper confirmation.
SELECT COUNT(*) AS cpm_rows,
       COUNT(DISTINCT research_id) AS cpm_distinct_research_id,
       COUNT(*) FILTER (WHERE cpm_built_at IS NULL) AS null_cpm_built_at
FROM main.canonical_patient_master;

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

-- §1 — pre-snapshot of affected derivative slice.
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_derivatives_pre_mig177c_apply_20260430 AS
SELECT
    pm.research_id,
    pm.lvi_any_present_path,
    pm.vi_any_present_path,
    pm.lvi_grade,
    pm.lvi_ordinal_worst,
    pm.n_tumors_lvi_present,
    pm.vasc_grade,
    pm.vasc_grade_final_v13,
    pm.vascular_invasion_final,
    pm.vascular_invasion_grade,
    pm.vascular_who_2022_grade,
    pm.vi_ordinal_worst,
    pm.vasc_vessel_count_v13,
    pm.vascular_vessel_count,
    pm.vi_vessels_max,
    pm.vasc_confidence_final_v13,
    pm.vasc_source_final_v13,
    pm.n_tumors_vi_present,
    pm.cpm_built_at,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig177c_apply_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master pm
WHERE pm.lvi_grade IS NOT NULL
   OR pm.lvi_ordinal_worst IS NOT NULL
   OR COALESCE(pm.n_tumors_lvi_present, 0) > 0
   OR pm.vasc_grade IS NOT NULL
   OR pm.vasc_grade_final_v13 IS NOT NULL
   OR pm.vascular_invasion_final IS NOT NULL
   OR pm.vascular_invasion_grade IS NOT NULL
   OR pm.vascular_who_2022_grade IS NOT NULL
   OR pm.vi_ordinal_worst IS NOT NULL
   OR pm.vasc_vessel_count_v13 IS NOT NULL
   OR pm.vascular_vessel_count IS NOT NULL
   OR pm.vi_vessels_max IS NOT NULL
   OR pm.vasc_confidence_final_v13 IS NOT NULL
   OR pm.vasc_source_final_v13 IS NOT NULL
   OR COALESCE(pm.n_tumors_vi_present, 0) > 0;

SELECT COUNT(*) AS pre_snapshot_rows
FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_derivatives_pre_mig177c_apply_20260430;

-- §2 — Option A LVI clear-only on TRUE->FALSE flippers.
UPDATE main.canonical_patient_master AS pm
SET lvi_grade = NULL,
    lvi_ordinal_worst = NULL,
    n_tumors_lvi_present = 0,
    cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE COALESCE(pm.lvi_any_present_path, FALSE) = FALSE
  AND CAST(pm.research_id AS VARCHAR) IN (
      SELECT CAST(cur.research_id AS VARCHAR) AS research_id
      FROM main.canonical_patient_master cur
      JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
        ON CAST(pre.research_id AS VARCHAR) = CAST(cur.research_id AS VARCHAR)
      WHERE COALESCE(pre.lvi_any_present_path, FALSE) = TRUE
        AND COALESCE(cur.lvi_any_present_path, FALSE) = FALSE
  )
  AND (pm.lvi_grade IS NOT NULL
       OR pm.lvi_ordinal_worst IS NOT NULL
       OR COALESCE(pm.n_tumors_lvi_present, 0) > 0);

-- §3 — Option A VI clear-only on TRUE->FALSE flippers.
UPDATE main.canonical_patient_master AS pm
SET vasc_grade = NULL,
    vasc_grade_final_v13 = NULL,
    vascular_invasion_final = NULL,
    vascular_invasion_grade = NULL,
    vascular_who_2022_grade = NULL,
    vi_ordinal_worst = NULL,
    vasc_vessel_count_v13 = NULL,
    vascular_vessel_count = NULL,
    vi_vessels_max = NULL,
    vasc_confidence_final_v13 = NULL,
    vasc_source_final_v13 = NULL,
    n_tumors_vi_present = 0,
    cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE COALESCE(pm.vi_any_present_path, FALSE) = FALSE
  AND CAST(pm.research_id AS VARCHAR) IN (
      SELECT CAST(cur.research_id AS VARCHAR) AS research_id
      FROM main.canonical_patient_master cur
      JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
        ON CAST(pre.research_id AS VARCHAR) = CAST(cur.research_id AS VARCHAR)
      WHERE COALESCE(pre.vi_any_present_path, FALSE) = TRUE
        AND COALESCE(cur.vi_any_present_path, FALSE) = FALSE
  )
  AND (pm.vasc_grade IS NOT NULL
       OR pm.vasc_grade_final_v13 IS NOT NULL
       OR pm.vascular_invasion_final IS NOT NULL
       OR pm.vascular_invasion_grade IS NOT NULL
       OR pm.vascular_who_2022_grade IS NOT NULL
       OR pm.vi_ordinal_worst IS NOT NULL
       OR pm.vasc_vessel_count_v13 IS NOT NULL
       OR pm.vascular_vessel_count IS NOT NULL
       OR pm.vi_vessels_max IS NOT NULL
       OR pm.vasc_confidence_final_v13 IS NOT NULL
       OR pm.vasc_source_final_v13 IS NOT NULL
       OR COALESCE(pm.n_tumors_vi_present, 0) > 0);

-- §4 — registry note appendix only, with idempotent guards.
UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE
    WHEN notes IS NULL OR TRIM(notes) = '' THEN
      'mig_177c_apply CLOSED CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN via Option A clear-only on 5,082 flippers. CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS: 99 LVI + 60 VI patients flipped FALSE/NULL->TRUE in mig_177b lack derivatives; future Option B lane needed (requires grade/count cols on canonical_invasion_events_v1).'
    WHEN POSITION('mig_177c_apply CLOSED CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN' IN notes) > 0 THEN notes
    ELSE notes || ' | ' ||
      'mig_177c_apply CLOSED CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN via Option A clear-only on 5,082 flippers. CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS: 99 LVI + 60 VI patients flipped FALSE/NULL->TRUE in mig_177b lack derivatives; future Option B lane needed (requires grade/count cols on canonical_invasion_events_v1).'
  END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'lvi_grade', 'lvi_ordinal_worst', 'n_tumors_lvi_present',
    'vasc_grade', 'vasc_grade_final_v13', 'vascular_invasion_final', 'vascular_invasion_grade',
    'vascular_who_2022_grade', 'vi_ordinal_worst', 'vasc_vessel_count_v13',
    'vascular_vessel_count', 'vi_vessels_max', 'vasc_confidence_final_v13', 'vasc_source_final_v13',
    'n_tumors_vi_present'
  );

-- §5 — CPM reconciliation provenance row.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
SELECT
  'canonical_cleanup_mig177c_apply_option_a_clear_only_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'pre_snapshot_lvi_clear_vi_clear_post_state_probe_registry_notes',
  'CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN',
  'none',
  'none',
  'CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS'
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1
  WHERE run_id = 'canonical_cleanup_mig177c_apply_option_a_clear_only_20260430'
);

-- §6 — post-state verification probes.
WITH lvi_flippers AS (
    SELECT CAST(pm.research_id AS VARCHAR) AS research_id
    FROM main.canonical_patient_master pm
    JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
      ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    WHERE COALESCE(pre.lvi_any_present_path, FALSE) = TRUE
      AND COALESCE(pm.lvi_any_present_path, FALSE) = FALSE
),
vi_flippers AS (
    SELECT CAST(pm.research_id AS VARCHAR) AS research_id
    FROM main.canonical_patient_master pm
    JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429 pre
      ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    WHERE COALESCE(pre.vi_any_present_path, FALSE) = TRUE
      AND COALESCE(pm.vi_any_present_path, FALSE) = FALSE
)
SELECT
  (SELECT COUNT(*) FROM main.canonical_patient_master) AS pm_total,
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master) AS pm_distinct_rids,
  (SELECT COUNT(*) FROM main.canonical_patient_master WHERE cpm_built_at IS NULL) AS null_cpm_built_at,
  COUNT(*) FILTER (WHERE lf.research_id IS NOT NULL AND pm.lvi_grade IS NOT NULL) AS lvi_grade_residual,
  COUNT(*) FILTER (WHERE lf.research_id IS NOT NULL AND pm.lvi_ordinal_worst IS NOT NULL) AS lvi_ordinal_worst_residual,
  COUNT(*) FILTER (WHERE lf.research_id IS NOT NULL AND COALESCE(pm.n_tumors_lvi_present, 0) > 0) AS n_tumors_lvi_present_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vasc_grade IS NOT NULL) AS vasc_grade_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vasc_grade_final_v13 IS NOT NULL) AS vasc_grade_final_v13_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vascular_invasion_final IS NOT NULL) AS vascular_invasion_final_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vascular_invasion_grade IS NOT NULL) AS vascular_invasion_grade_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vascular_who_2022_grade IS NOT NULL) AS vascular_who_2022_grade_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vi_ordinal_worst IS NOT NULL) AS vi_ordinal_worst_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vasc_vessel_count_v13 IS NOT NULL) AS vasc_vessel_count_v13_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vascular_vessel_count IS NOT NULL) AS vascular_vessel_count_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vi_vessels_max IS NOT NULL) AS vi_vessels_max_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vasc_confidence_final_v13 IS NOT NULL) AS vasc_confidence_final_v13_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND pm.vasc_source_final_v13 IS NOT NULL) AS vasc_source_final_v13_residual,
  COUNT(*) FILTER (WHERE vf.research_id IS NOT NULL AND COALESCE(pm.n_tumors_vi_present, 0) > 0) AS n_tumors_vi_present_residual
FROM main.canonical_patient_master pm
LEFT JOIN lvi_flippers lf ON CAST(pm.research_id AS VARCHAR) = lf.research_id
LEFT JOIN vi_flippers vf ON CAST(pm.research_id AS VARCHAR) = vf.research_id;

SELECT COUNT(*) AS provenance_rows
FROM manuscript_workspace.cpm_reconciliation_provenance_v1
WHERE run_id = 'canonical_cleanup_mig177c_apply_option_a_clear_only_20260430';