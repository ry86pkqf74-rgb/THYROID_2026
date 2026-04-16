-- ============================================================================
-- Script 241 — Build path_size_adjudication_v241 (review artifact, not canonical)
-- Date:    2026-04-16
-- Author:  THYROID_2026 canonical-finalization run (v1_0 lock)
--
-- Purpose
-- -------
-- Surface 68 path-vs-max size discrepancies (ABS(path - max) > 2 cm) plus
-- 37 path sizes >10 cm (96 distinct patients in the union) into a new
-- review-artifact table `path_size_adjudication_v241`. This is a v1_0
-- adjudication feed; it does NOT modify canonical_patient_master in this
-- script (that happens later, once a clinician signs off).
--
-- Tables READ
--   thyroid_canonical_publication_v1_0.main.canonical_patient_master
--   thyroid_canonical_publication_v1_0.main.patient_tumor_rollup_v1
--   thyroid_canonical_publication_v1_0.main.specimen_tumor_focus_v1
--   thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1
--
-- Tables WRITTEN
--   thyroid_canonical_publication_v1_0.main.path_size_adjudication_v241   (NEW)
--   thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1
--       (one row added; feeds_master_columns = TODO until sign-off, per spec)
--
-- Backup / rollback plan
--   The table is NEW — no pre-script backup needed. To undo:
--     DROP TABLE path_size_adjudication_v241;
--     DELETE FROM manuscript_workspace.detail_table_registry_v1
--       WHERE detail_table_name = 'path_size_adjudication_v241';
--
-- Why no CPM edit in this script
--   Per the finalization spec: "DO NOT apply to canonical_patient_master
--   in this script. This is a review-artifact, not a canonical change."
--   Clinician sign-off is required before any of the `proposed_path_tumor_
--   size_cm_adjudicated` values flow back to CPM.
-- ============================================================================

-- LOG: PHASE 1 — pre-flight assertions on source data
-- ASSERT: source columns are present on canonical_patient_master
SELECT
  (SELECT COUNT(*) FROM information_schema.columns
   WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
     AND table_name='canonical_patient_master'
     AND column_name IN ('path_tumor_size_cm','tumor_size_cm_max')) = 2 AS ok;

-- ASSERT: outlier count (>2cm discrepancy) matches pre-flight baseline (68)
SELECT COUNT(*) = 68 AS ok
FROM canonical_patient_master
WHERE path_tumor_size_cm IS NOT NULL AND tumor_size_cm_max IS NOT NULL
  AND ABS(path_tumor_size_cm - tumor_size_cm_max) > 2;

-- ASSERT: path sizes >10cm count matches pre-flight baseline (37)
SELECT COUNT(*) = 37 AS ok
FROM canonical_patient_master
WHERE path_tumor_size_cm > 10;

-- LOG: PHASE 2 — build path_size_adjudication_v241 (union of both criteria)
CREATE OR REPLACE TABLE path_size_adjudication_v241 AS
WITH focus_counts AS (
  SELECT research_id, COUNT(DISTINCT tumor_index) AS n_foci_path
  FROM specimen_tumor_focus_v1
  GROUP BY research_id
),
rollup_counts AS (
  SELECT research_id, n_tumors_path
  FROM patient_tumor_rollup_v1
),
outlier AS (
  SELECT
    c.research_id,
    c.path_tumor_size_cm,
    c.tumor_size_cm_max,
    COALESCE(f.n_foci_path, 0)  AS n_foci_path,
    COALESCE(r.n_tumors_path, 0) AS n_tumors_path,
    CASE
      -- Anatomically implausible: large path sizes need manual review
      WHEN c.path_tumor_size_cm > 10                          THEN NULL
      -- Multifocal: prefer the max of the rollup (matches patient_tumor_rollup_v1 convention)
      WHEN COALESCE(f.n_foci_path, 0) > 1                     THEN c.tumor_size_cm_max
      -- Unifocal with a discrepancy: default to path until clinician adjudicates
      ELSE c.path_tumor_size_cm
    END AS proposed_path_tumor_size_cm_adjudicated,
    CASE
      WHEN c.path_tumor_size_cm > 10                          THEN 'outlier_manual_review_required'
      WHEN COALESCE(f.n_foci_path, 0) > 1                     THEN 'multifocal_use_rollup_max'
      ELSE 'unifocal_retain_path_size'
    END AS adjudication_rule,
    CASE
      WHEN c.path_tumor_size_cm > 10                          THEN 'HIGH'
      WHEN COALESCE(f.n_foci_path, 0) > 1                     THEN 'MEDIUM'
      ELSE 'MEDIUM'
    END AS review_priority
  FROM canonical_patient_master c
  LEFT JOIN focus_counts  f ON f.research_id = c.research_id
  LEFT JOIN rollup_counts r ON r.research_id = c.research_id
  WHERE
    (c.path_tumor_size_cm IS NOT NULL
     AND c.tumor_size_cm_max IS NOT NULL
     AND ABS(c.path_tumor_size_cm - c.tumor_size_cm_max) > 2)
    OR
    c.path_tumor_size_cm > 10
)
SELECT * FROM outlier;

-- LOG: PHASE 3 — annotate the table
COMMENT ON TABLE path_size_adjudication_v241 IS
  'Script 241 (2026-04-16): review-artifact surfacing 96 patients with path-vs-max tumor size discrepancies. Criteria: ABS(path_tumor_size_cm - tumor_size_cm_max) > 2cm OR path_tumor_size_cm > 10cm. Proposed adjudicated value + rule + priority emitted per row, but NOT applied to canonical_patient_master until clinician sign-off. v1_0 provisional.';

COMMENT ON COLUMN path_size_adjudication_v241.proposed_path_tumor_size_cm_adjudicated IS
  'Script 241 proposal (NOT canonical until clinician sign-off): NULL when path > 10cm (flagged for manual review); tumor_size_cm_max when n_foci_path > 1 (multifocal, prefer rollup); path_tumor_size_cm otherwise.';

COMMENT ON COLUMN path_size_adjudication_v241.adjudication_rule IS
  'Rule that produced the proposal: outlier_manual_review_required | multifocal_use_rollup_max | unifocal_retain_path_size.';

COMMENT ON COLUMN path_size_adjudication_v241.review_priority IS
  'HIGH for path > 10cm (anatomically implausible); MEDIUM otherwise.';

-- LOG: PHASE 4 — register in detail_table_registry_v1 (idempotent: delete-then-insert)
DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name = 'path_size_adjudication_v241';

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'path_size_adjudication_v241',
  'main',
  'research_id',
  'one row per patient with path-vs-max size outlier',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'Pathology/Adjudication',
  'TODO: clinician sign-off; will feed deprecated__tumor_size_cm / path_tumor_size_cm manual-review queue in v1_1',
  'Script 241 (2026-04-16): review-artifact surfacing 68 ABS(path-max)>2cm + 37 path>10cm outliers (union=96). Proposed adjudicated values + rule + priority. NOT canonical until clinician sign-off. v1_0 provisional.',
  'v1_0'
FROM path_size_adjudication_v241;

-- LOG: PHASE 5 — post-build assertions
-- ASSERT: row count is in the expected bracket (60-120)
SELECT COUNT(*) BETWEEN 60 AND 120 AS ok FROM path_size_adjudication_v241;

-- ASSERT: registry has the new entry (exactly one)
SELECT COUNT(*) = 1 AS ok
FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name = 'path_size_adjudication_v241';

-- ASSERT: canonical_patient_master untouched (still 10,871; no columns added)
SELECT COUNT(*) = 10871 AS ok FROM canonical_patient_master;

-- ASSERT: no NULL research_id in the adjudication table
SELECT COUNT(*) = 0 AS ok FROM path_size_adjudication_v241 WHERE research_id IS NULL;

-- ASSERT: every row carries a valid adjudication_rule from the closed set
SELECT
  COUNT(*) = SUM(CASE WHEN adjudication_rule IN (
    'outlier_manual_review_required',
    'multifocal_use_rollup_max',
    'unifocal_retain_path_size'
  ) THEN 1 ELSE 0 END) AS ok
FROM path_size_adjudication_v241;

-- ASSERT: every row carries a valid review_priority from the closed set
SELECT
  COUNT(*) = SUM(CASE WHEN review_priority IN ('HIGH','MEDIUM') THEN 1 ELSE 0 END) AS ok
FROM path_size_adjudication_v241;

-- LOG: PHASE 6 — diagnostic summary (non-blocking)
-- LOG: priority + rule breakdown
SELECT adjudication_rule, review_priority, COUNT(*) AS n
FROM path_size_adjudication_v241
GROUP BY 1, 2
ORDER BY 2 DESC, 1;

-- LOG: Script 241 complete. Review artifact built; CPM untouched; registry updated (TODO pending clinician sign-off).
