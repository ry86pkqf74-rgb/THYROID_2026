-- mig_220 — Lane E5 (Round 2): auto-resolve high-priority TIRADS field conflicts (prefer tirads_v2)
-- run_id: mig_220_tirads_high_pri_conflict_resolution_20260430
-- Source: CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md — E5
-- Target DB: thyroid_canonical_publication_v1_0
--
-- Scope: rows in manuscript_workspace.us_nodule_conflict_queue_v1 with
--   review_priority = 'high' AND field_name IN
--     ('tirads_reported','tirads_category_v2','tirads_score_2017')
-- Logan-ratified rule: prefer tirads_v2_nodules_raw-derived value (queue column
-- value_tirads_v2) over cunc/cunm for category and score; for tirads_reported,
-- prefer text-extracted tier from v2 (same column).
--
-- Skips rows where value_tirads_v2 IS NULL or blank (deferred manual).
-- Provenance: new column main.canonical_us_nodule_v2.tirads_conflict_resolution_source
--
-- COWORK-DIRECT APPLY. Re-run: archive + ALTER may error if column exists — use one-time apply.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Pre-snapshot: nodule rows touched by high-priority TIRADS conflicts
-- =============================================================================
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_nodule_v2_pre_mig220_conflict_resolution_20260430 AS
SELECT n.*,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig220_snapshot_ts
FROM main.canonical_us_nodule_v2 AS n
WHERE EXISTS (
    SELECT 1
    FROM manuscript_workspace.us_nodule_conflict_queue_v1 AS q
    WHERE q.review_priority = 'high'
      AND q.field_name IN ('tirads_reported', 'tirads_category_v2', 'tirads_score_2017')
      AND CAST(n.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
      AND n.us_exam_id = q.us_exam_id
      AND n.nodule_index_within_exam = q.nodule_index_within_exam
  );

-- =============================================================================
-- §1 Provenance column (execute once)
-- =============================================================================
-- If re-applying against an environment that already has the column, comment out:
ALTER TABLE main.canonical_us_nodule_v2
  ADD COLUMN tirads_conflict_resolution_source VARCHAR;

COMMENT ON COLUMN main.canonical_us_nodule_v2.tirads_conflict_resolution_source IS
'Batch conflict resolution provenance. mig_220: when non-null, Logan-ratified rule applied from us_nodule_conflict_queue_v1 — prefer value_tirads_v2 for tirads_reported / updated_tirads_category / acr2017_tirads_points. Pipe-separated tags per field.';

DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_us_nodule_v2'
  AND column_name = 'tirads_conflict_resolution_source'
  AND batch_id = 'mig_220_tirads_high_pri_conflict_resolution_20260430';

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category,
   upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes, registered_ts)
SELECT
  'main',
  'canonical_us_nodule_v2',
  c.column_name,
  c.data_type,
  c.ordinal_position,
  'derived',
  'us_nodule_conflict_queue_v1',
  'verified',
  'mig_220',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'mig_220_TIRADS_conflict_auto_resolution',
  'mig_220_tirads_high_pri_conflict_resolution_20260430',
  'mig_220 Lane E5: batch prefer value_tirads_v2 for high-priority fields; pipe-separated tags.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns AS c
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'main'
  AND c.table_name = 'canonical_us_nodule_v2'
  AND c.column_name = 'tirads_conflict_resolution_source';

-- =============================================================================
-- §3 Apply: tirads_reported → tirads_reported_in_text
-- =============================================================================
UPDATE main.canonical_us_nodule_v2 AS n
SET
  tirads_reported_in_text = TRY_CAST(TRIM(CAST(q.value_tirads_v2 AS VARCHAR)) AS INTEGER),
  tirads_conflict_resolution_source = CASE
      WHEN COALESCE(n.tirads_conflict_resolution_source, '') = ''
        THEN 'mig_220:tirads_reported:prefer_tirads_v2'
      ELSE n.tirads_conflict_resolution_source || '|mig_220:tirads_reported:prefer_tirads_v2'
    END
FROM (
  SELECT research_id, us_exam_id, nodule_index_within_exam, value_tirads_v2,
         ROW_NUMBER() OVER (
           PARTITION BY research_id, us_exam_id, nodule_index_within_exam
           ORDER BY value_tirads_v2
         ) AS rn
  FROM manuscript_workspace.us_nodule_conflict_queue_v1
  WHERE review_priority = 'high'
    AND field_name = 'tirads_reported'
    AND value_tirads_v2 IS NOT NULL
    AND TRIM(CAST(value_tirads_v2 AS VARCHAR)) <> ''
) AS q
WHERE q.rn = 1
  AND CAST(n.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
  AND n.us_exam_id = q.us_exam_id
  AND n.nodule_index_within_exam = q.nodule_index_within_exam;

-- =============================================================================
-- §4 Apply: tirads_category_v2 → updated_tirads_category
-- =============================================================================
UPDATE main.canonical_us_nodule_v2 AS n
SET
  updated_tirads_category = TRIM(CAST(q.value_tirads_v2 AS VARCHAR)),
  tirads_conflict_resolution_source = CASE
      WHEN COALESCE(n.tirads_conflict_resolution_source, '') = ''
        THEN 'mig_220:tirads_category_v2:prefer_tirads_v2'
      ELSE n.tirads_conflict_resolution_source || '|mig_220:tirads_category_v2:prefer_tirads_v2'
    END
FROM (
  SELECT research_id, us_exam_id, nodule_index_within_exam, value_tirads_v2,
         ROW_NUMBER() OVER (
           PARTITION BY research_id, us_exam_id, nodule_index_within_exam
           ORDER BY value_tirads_v2
         ) AS rn
  FROM manuscript_workspace.us_nodule_conflict_queue_v1
  WHERE review_priority = 'high'
    AND field_name = 'tirads_category_v2'
    AND value_tirads_v2 IS NOT NULL
    AND TRIM(CAST(value_tirads_v2 AS VARCHAR)) <> ''
) AS q
WHERE q.rn = 1
  AND CAST(n.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
  AND n.us_exam_id = q.us_exam_id
  AND n.nodule_index_within_exam = q.nodule_index_within_exam;

-- =============================================================================
-- §5 Apply: tirads_score_2017 → acr2017_tirads_points
-- =============================================================================
UPDATE main.canonical_us_nodule_v2 AS n
SET
  acr2017_tirads_points = TRY_CAST(TRIM(CAST(q.value_tirads_v2 AS VARCHAR)) AS INTEGER),
  tirads_conflict_resolution_source = CASE
      WHEN COALESCE(n.tirads_conflict_resolution_source, '') = ''
        THEN 'mig_220:tirads_score_2017:prefer_tirads_v2'
      ELSE n.tirads_conflict_resolution_source || '|mig_220:tirads_score_2017:prefer_tirads_v2'
    END
FROM (
  SELECT research_id, us_exam_id, nodule_index_within_exam, value_tirads_v2,
         ROW_NUMBER() OVER (
           PARTITION BY research_id, us_exam_id, nodule_index_within_exam
           ORDER BY value_tirads_v2
         ) AS rn
  FROM manuscript_workspace.us_nodule_conflict_queue_v1
  WHERE review_priority = 'high'
    AND field_name = 'tirads_score_2017'
    AND value_tirads_v2 IS NOT NULL
    AND TRIM(CAST(value_tirads_v2 AS VARCHAR)) <> ''
) AS q
WHERE q.rn = 1
  AND CAST(n.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
  AND n.us_exam_id = q.us_exam_id
  AND n.nodule_index_within_exam = q.nodule_index_within_exam;

-- =============================================================================
-- §6 Re-derive ACR 2017 category from points where points were touched
-- =============================================================================
UPDATE main.canonical_us_nodule_v2
SET acr2017_tirads_category = CASE
      WHEN acr2017_tirads_points IS NULL THEN NULL
      WHEN acr2017_tirads_points = 0 THEN 'TR1'
      WHEN acr2017_tirads_points = 2 THEN 'TR2'
      WHEN acr2017_tirads_points = 3 THEN 'TR3'
      WHEN acr2017_tirads_points BETWEEN 4 AND 6 THEN 'TR4'
      WHEN acr2017_tirads_points >= 7 THEN 'TR5'
      ELSE NULL
    END
WHERE tirads_conflict_resolution_source LIKE '%mig_220:tirads_score_2017:prefer_tirads_v2%';

-- =============================================================================
-- §7 Concordance flag refresh when both category columns present
-- =============================================================================
UPDATE main.canonical_us_nodule_v2
SET acr2017_vs_updated_concordant =
      (acr2017_tirads_category = updated_tirads_category)
WHERE tirads_conflict_resolution_source LIKE '%mig_220%'
  AND acr2017_tirads_category IS NOT NULL
  AND updated_tirads_category IS NOT NULL;

-- =============================================================================
-- §8 Post checks (run after apply)
-- =============================================================================
-- SELECT field_name, COUNT(*) FROM manuscript_workspace.us_nodule_conflict_queue_v1
--   WHERE review_priority='high' AND field_name IN ('tirads_reported','tirads_category_v2','tirads_score_2017')
--   GROUP BY 1;
-- SELECT COUNT(*) FROM main.canonical_us_nodule_v2 WHERE tirads_conflict_resolution_source LIKE '%mig_220%';

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_220_tirads_high_pri_conflict_resolution_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'lane_e5_us_nodule_conflict_queue_high_pri_tirads_fields_prefer_tirads_v2',
   '0',
   'canonical_us_nodule_v2_UPDATE_tirads_reported_updated_category_acr_points',
   'archive_pre_mig220_snapshot_tirads_conflict_resolution_recompute_acr_category',
   'rows_with_NULL_value_tirads_v2_remain_in_queue_for_manual');
