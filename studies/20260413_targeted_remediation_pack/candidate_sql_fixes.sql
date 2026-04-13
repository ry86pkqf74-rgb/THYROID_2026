-- candidate_sql_fixes.sql — MotherDuck / DuckDB dialect
-- Review before execution. Token: use motherduck_client.get_token() from motherduck.local.toml (not embedded here).
-- Citable failing rows for linkage: studies/20260413_source_truth_completeness_audit/linkage_gap_worklist_unresolved_20260413_174900.csv lines 2-129 (nodule_id list below excerpt).

-- ---------------------------------------------------------------------------
-- 1) Deploy views (idempotent) — see full file in repo
--    scripts/sql/source_truth_confirmation_v1.sql
-- ---------------------------------------------------------------------------
-- Run via: .venv/bin/python scripts/151_source_truth_confirmation_v1.py --md

-- ---------------------------------------------------------------------------
-- 2) QA: list FNA candidates for a single audited gap row (example nodule_id)
--    Row: linkage_gap_worklist line 2 — nodule_id=576047cf50c3dd7cc903563f762f5846, research_id=6874
-- ---------------------------------------------------------------------------
/*
WITH img AS (
  SELECT nodule_id, research_id, exam_id, CAST(exam_date AS DATE) AS exam_d
  FROM imaging_nodule_master_v1
  WHERE nodule_id = '576047cf50c3dd7cc903563f762f5846'
)
SELECT i.*, f.fna_episode_id,
       COALESCE(f.fna_date_native, TRY_CAST(f.resolved_fna_date AS DATE)) AS fna_d,
       DATEDIFF('day', i.exam_d, COALESCE(f.fna_date_native, TRY_CAST(f.resolved_fna_date AS DATE))) AS day_gap
FROM img i
JOIN fna_episode_master_v2 f
  ON CAST(f.research_id AS BIGINT) = CAST(i.research_id AS BIGINT)
WHERE COALESCE(f.fna_date_native, TRY_CAST(f.resolved_fna_date AS DATE)) IS NOT NULL
  AND COALESCE(f.fna_date_native, TRY_CAST(f.resolved_fna_date AS DATE)) >= i.exam_d
  AND DATEDIFF('day', i.exam_d, COALESCE(f.fna_date_native, TRY_CAST(f.resolved_fna_date AS DATE))) BETWEEN 0 AND 90
ORDER BY day_gap;
*/

-- ---------------------------------------------------------------------------
-- 3) STAGING ONLY: primary-link candidates for the 128 gap nodules (DO NOT RUN
--    without human sign-off — inserts must be validated per nodule_id)
--    Replace :worklist with temp table loaded from linkage_gap_worklist CSV.
-- ---------------------------------------------------------------------------
/*
CREATE OR REPLACE TEMP TABLE tmp_linkage_gap_nodules AS
SELECT DISTINCT nodule_id
FROM read_csv('studies/20260413_source_truth_completeness_audit/linkage_gap_worklist_unresolved_20260413_174900.csv');

-- Example shape — actual INSERT must match imaging_fna_linkage_mm_v1 contract (see scripts/129...py).
-- INSERT INTO imaging_fna_linkage_mm_v1 SELECT ... WHERE nodule_id IN (SELECT nodule_id FROM tmp_linkage_gap_nodules);
*/

-- ---------------------------------------------------------------------------
-- 4) Bethesda: read-only resolution preview (conflicts file lines 2+)
--    Example: fna_bethesda_conflicts.csv lines 2-7 research_id 790
-- ---------------------------------------------------------------------------
SELECT
  e.research_id,
  e.fna_episode_id,
  e.bethesda_category AS ep_num,
  v.bethesda_resolved_num,
  v.bethesda_value_source,
  v.bethesda_unscorable_reason
FROM fna_episode_master_v2 e
LEFT JOIN v_fna_episode_bethesda_resolved_v1 v
  ON CAST(e.research_id AS BIGINT) = CAST(v.research_id AS BIGINT)
 AND CAST(e.fna_episode_id AS BIGINT) = CAST(v.fna_episode_id AS BIGINT)
WHERE CAST(e.research_id AS BIGINT) = 790
ORDER BY e.fna_episode_id;

-- ---------------------------------------------------------------------------
-- 5) Optional deterministic UPDATE (ONLY if institution declares fna_cytology.category_num gold)
--    Citable rows: fna_bethesda_conflicts.csv where episode and history agree but cytology differs — review each line.
-- ---------------------------------------------------------------------------
-- UPDATE fna_episode_master_v2 SET bethesda_category = <adjudicated>
-- WHERE research_id = ? AND fna_episode_id = ?;
