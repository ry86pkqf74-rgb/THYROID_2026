-- ============================================================================
-- mig_078_manuscript_cohort_health_view_and_assertion
-- ============================================================================
-- Date:    2026-05-06
-- Author:  Cursor agent (Sonnet 4.6) at Logan's request
-- DFL:     DFL-20260506-TA1  (Data Feedback Log, Airtable base appJYOnUb7KrHKwpV)
-- ============================================================================
--
-- PURPOSE:
--   1. Create pub_signoff.cohort_view_stats_v1
--      Pre-computed row counts for every cohort_m*_v1 table in pub_workspace.
--      Populated via BQ scripting (EXECUTE IMMEDIATE FOR loop over
--      INFORMATION_SCHEMA.TABLES) so the health VIEW avoids full-scan COUNT(*)
--      at query time.  Refreshed by re-running Part 1 of this migration.
--
--   2. Create pub_signoff.manuscript_cohort_health_v1 (VIEW)
--      Registry joining:
--        • pub_workspace.manuscript_dive_map_v1      (cohort_view_name)
--        • pub_workspace.manuscript_feasibility_v1   (status, feasibility_color)
--        • pub_workspace.INFORMATION_SCHEMA.TABLES   (existence check)
--        • pub_legacy_source_20260416.INFORMATION_SCHEMA.TABLES (legacy check)
--        • pub_signoff.cohort_view_stats_v1           (row count)
--      Columns:
--        manuscript_id, manuscript_title, cohort_view_name,
--        cohort_view_exists BOOL, cohort_view_row_count INT64,
--        feasibility_color STRING, status STRING, is_active BOOL
--
--   3. Insert QC assertion manuscript_active_cohort_view_must_exist
--      severity=warn; fires when any active manuscript has no cohort view.
--
-- ROLLBACK:
--   DROP TABLE IF EXISTS `thyroid-canonical-pub-2026.pub_signoff.cohort_view_stats_v1`;
--   DROP VIEW  IF EXISTS `thyroid-canonical-pub-2026.pub_signoff.manuscript_cohort_health_v1`;
--   DELETE FROM `thyroid-canonical-pub-2026.pub_signoff.qc_assertions_v1`
--     WHERE assertion_id = 'manuscript_active_cohort_view_must_exist';
-- ============================================================================


-- ============================================================================
-- PART 1 — Populate cohort_view_stats_v1
-- ============================================================================
-- Create the target table (empty schema first, then populate via script block)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_signoff.cohort_view_stats_v1` (
  cohort_view_name  STRING  NOT NULL,
  source_dataset    STRING  NOT NULL,
  row_count         INT64,
  counted_at        TIMESTAMP
);

-- BQ Scripting block: iterate over all cohort_m* tables in pub_workspace
-- and INSERT the row count for each.
BEGIN
  FOR t IN (
    SELECT table_name
    FROM `thyroid-canonical-pub-2026.pub_workspace.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'cohort_m%'
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
  )
  DO
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.cohort_view_stats_v1`
        (cohort_view_name, source_dataset, row_count, counted_at)
      SELECT
        '%s'            AS cohort_view_name,
        'pub_workspace' AS source_dataset,
        COUNT(*)        AS row_count,
        CURRENT_TIMESTAMP() AS counted_at
      FROM `thyroid-canonical-pub-2026.pub_workspace.%s`
    """, t.table_name, t.table_name);
  END FOR;
END;

-- ============================================================================
-- PART 2 — Create the health VIEW
-- ============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_signoff.manuscript_cohort_health_v1` AS
WITH
-- unique manuscript → cohort mapping (one row per manuscript_id; take the
-- non-null cohort_view_name when multiple dives exist for the same manuscript)
dive_map AS (
  SELECT
    manuscript_id,
    manuscript_title,
    cohort_view_name,
    ROW_NUMBER() OVER (
      PARTITION BY manuscript_id
      ORDER BY
        CASE WHEN cohort_view_name IS NOT NULL THEN 0 ELSE 1 END,
        cohort_view_name
    ) AS rn
  FROM `thyroid-canonical-pub-2026.pub_workspace.manuscript_dive_map_v1`
),

-- all tables/views that exist in pub_workspace (cohort BASE TABLEs live here)
exists_workspace AS (
  SELECT table_name AS cohort_view_name
  FROM `thyroid-canonical-pub-2026.pub_workspace.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE 'cohort_m%'
),

-- all views that exist in pub_legacy_source_20260416 (frozen MD reference)
exists_legacy AS (
  SELECT table_name AS cohort_view_name
  FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE 'cohort_m%'
)

SELECT
  dm.manuscript_id,
  dm.manuscript_title,
  dm.cohort_view_name,

  -- existence: TRUE if found in either pub_workspace or pub_legacy_source
  CASE
    WHEN dm.cohort_view_name IS NULL                    THEN FALSE
    WHEN ew.cohort_view_name IS NOT NULL                THEN TRUE
    WHEN el.cohort_view_name IS NOT NULL                THEN TRUE
    ELSE FALSE
  END AS cohort_view_exists,

  -- row count from pre-computed stats (NULL if view does not exist or unmapped)
  cvs.row_count AS cohort_view_row_count,

  -- feasibility fields (NULL when manuscript_id not scored yet)
  f.feasibility_color,
  f.status,

  -- is_active: the three statuses that constitute an actively worked manuscript
  CASE
    WHEN f.status IN ('Ready to Submit', 'In Progress', 'Proposed') THEN TRUE
    ELSE FALSE
  END AS is_active

FROM dive_map AS dm
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.manuscript_feasibility_v1` AS f
  ON dm.manuscript_id = f.manuscript_id
LEFT JOIN exists_workspace AS ew
  ON dm.cohort_view_name = ew.cohort_view_name
LEFT JOIN exists_legacy AS el
  ON dm.cohort_view_name = el.cohort_view_name
LEFT JOIN `thyroid-canonical-pub-2026.pub_signoff.cohort_view_stats_v1` AS cvs
  ON dm.cohort_view_name = cvs.cohort_view_name
WHERE dm.rn = 1   -- deduplicate to one row per manuscript
;


-- ============================================================================
-- PART 3 — Insert QC assertion
-- ============================================================================
-- Remove any prior version of this assertion (idempotent re-run safety)
DELETE FROM `thyroid-canonical-pub-2026.pub_signoff.qc_assertions_v1`
WHERE assertion_id = 'manuscript_active_cohort_view_must_exist';

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.qc_assertions_v1`
  (assertion_id, category, severity, affected_object, description,
   check_sql, expected_result, active, added_at, added_by, notes)
VALUES (
  'manuscript_active_cohort_view_must_exist',
  'governance',
  'warn',
  'pub_signoff.manuscript_cohort_health_v1',
  'Every active manuscript (Ready to Submit / In Progress / Proposed) must have a working cohort view in pub_workspace or pub_legacy_source_20260416.',
  -- check_sql: returns ROWS where violation exists (0 rows = pass per qc_runner)
  '''
    SELECT
      manuscript_id,
      manuscript_title,
      cohort_view_name,
      status,
      feasibility_color
    FROM `thyroid-canonical-pub-2026.pub_signoff.manuscript_cohort_health_v1`
    WHERE is_active = TRUE
      AND (cohort_view_exists IS NULL OR cohort_view_exists = FALSE)
    ORDER BY manuscript_id
  ''',
  'Zero rows (all active manuscripts have a working cohort view)',
  TRUE,
  CURRENT_TIMESTAMP(),
  'mig_078 / cursor-20260506-task-a',
  'DFL: DFL-20260506-TA1. Fires when is_active=TRUE and cohort_view_exists=FALSE. Expected to return 0-3 rows for manuscripts pending cohort view creation (e.g., new Proposed manuscripts not yet provisioned).'
);


-- ============================================================================
-- VERIFICATION (run after applying this migration)
-- ============================================================================
-- 1. Confirm stats table populated:
--    SELECT COUNT(*) AS n_cohort_tables, SUM(row_count) AS total_rows
--    FROM `thyroid-canonical-pub-2026.pub_signoff.cohort_view_stats_v1`;
--
-- 2. Preview the health view:
--    SELECT manuscript_id, manuscript_title, cohort_view_name,
--           cohort_view_exists, cohort_view_row_count, status, is_active
--    FROM `thyroid-canonical-pub-2026.pub_signoff.manuscript_cohort_health_v1`
--    WHERE is_active = TRUE
--    ORDER BY manuscript_id;
--
-- 3. Run QC:
--    python3 _scripts/qc_runner.py
--    -- expect manuscript_active_cohort_view_must_exist to appear; violation_count
--    -- should be 0-3 (active manuscripts with no cohort view yet).
-- ============================================================================
