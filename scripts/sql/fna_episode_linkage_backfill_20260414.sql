-- FNA Episode Linkage Backfill — 2026-04-14
-- Deployed to MotherDuck "Thyroid 2026".main
--
-- Objects created:
--   VIEW  v_patient_surgery_timeline     (9,371 rows)
--   VIEW  v_fna_surgery_window           (8,119 rows)
--   VIEW  v_fna_bethesda_surface         (8,119 rows)
--   TABLE fna_episode_master_v2_backup_20260414
--
-- Columns backfilled on fna_episode_master_v2:
--   linked_imaging_nodule_id  — 1,598 of 8,119 (19.7%)
--   linked_surgery_episode_id — 5,886 of 8,119 (72.5%)
--   Remaining 2,233 NULL surgery links = patients with no surgery record
--
-- Surgery linkage rules:
--   - FNA with date + surgery with date → nearest surgery by date (no window)
--   - FNA with NULL date + patient has surgery → first surgery (pre-op assumption)
--   - Patient has no surgery → NULL (no_surgery era)
--
-- Imaging linkage rules:
--   - imaging_fna_linkage_v3 rank-1, tier IN (exact_match, high_confidence, plausible)

-- ================================================================
-- VIEW 1: v_patient_surgery_timeline
-- ================================================================
CREATE OR REPLACE VIEW v_patient_surgery_timeline AS
SELECT
  research_id,
  surgery_episode_id,
  TRY_CAST(resolved_surgery_date AS DATE) AS surgery_date,
  ROW_NUMBER() OVER (PARTITION BY research_id
    ORDER BY TRY_CAST(resolved_surgery_date AS DATE), surgery_episode_id) AS surgery_ordinal,
  COUNT(*) OVER (PARTITION BY research_id) AS total_surgeries,
  ROW_NUMBER() OVER (PARTITION BY research_id
    ORDER BY TRY_CAST(resolved_surgery_date AS DATE), surgery_episode_id) = 1 AS is_first_surgery,
  ROW_NUMBER() OVER (PARTITION BY research_id
    ORDER BY TRY_CAST(resolved_surgery_date AS DATE), surgery_episode_id)
    = COUNT(*) OVER (PARTITION BY research_id) AS is_last_surgery,
  LEAD(TRY_CAST(resolved_surgery_date AS DATE)) OVER (PARTITION BY research_id
    ORDER BY TRY_CAST(resolved_surgery_date AS DATE), surgery_episode_id) AS next_surgery_date,
  LAG(TRY_CAST(resolved_surgery_date AS DATE)) OVER (PARTITION BY research_id
    ORDER BY TRY_CAST(resolved_surgery_date AS DATE), surgery_episode_id) AS prev_surgery_date
FROM operative_episode_detail_v2;

-- ================================================================
-- VIEW 2: v_fna_surgery_window
-- ================================================================
CREATE OR REPLACE VIEW v_fna_surgery_window AS
WITH patient_surgery_bounds AS (
  SELECT
    research_id,
    MIN(surgery_date) AS first_surgery_date,
    MAX(surgery_date) AS last_surgery_date,
    MAX(total_surgeries) AS total_surgeries
  FROM v_patient_surgery_timeline
  GROUP BY research_id
),
nearest_surgery AS (
  SELECT
    fem.research_id,
    fem.fna_episode_id,
    pst.surgery_episode_id AS nearest_surgery_episode_id,
    pst.surgery_date AS nearest_surgery_date,
    DATE_DIFF('day', pst.surgery_date, fem.resolved_fna_date) AS days_fna_to_nearest_surgery,
    ROW_NUMBER() OVER (
      PARTITION BY fem.research_id, fem.fna_episode_id
      ORDER BY ABS(DATE_DIFF('day', fem.resolved_fna_date, pst.surgery_date)),
               pst.surgery_episode_id
    ) AS rn
  FROM fna_episode_master_v2 fem
  JOIN v_patient_surgery_timeline pst ON fem.research_id = pst.research_id
  WHERE fem.resolved_fna_date IS NOT NULL AND pst.surgery_date IS NOT NULL
),
first_surgery_fallback AS (
  SELECT research_id, surgery_episode_id, surgery_date
  FROM v_patient_surgery_timeline
  WHERE is_first_surgery = TRUE
)
SELECT
  fem.research_id,
  fem.fna_episode_id,
  fem.fna_date_native,
  fem.resolved_fna_date,
  fem.date_status,
  fem.date_confidence,
  fem.bethesda_raw,
  fem.bethesda_category,
  fem.pathology_diagnosis,
  fem.pathology_extended,
  fem.specimen_site_raw,
  fem.laterality,
  fem.linked_molecular_episode_id,
  fem.linked_imaging_nodule_id,
  fem.linked_surgery_episode_id,
  fem.source_table,
  fem.fna_confidence,
  COALESCE(ns.nearest_surgery_episode_id, fsf.surgery_episode_id) AS nearest_surgery_episode_id,
  COALESCE(ns.nearest_surgery_date, fsf.surgery_date) AS nearest_surgery_date,
  ns.days_fna_to_nearest_surgery,
  CASE
    WHEN psb.research_id IS NULL THEN 'no_surgery'
    WHEN fem.resolved_fna_date IS NULL THEN 'pre_first_op'
    WHEN fem.resolved_fna_date < psb.first_surgery_date THEN 'pre_first_op'
    WHEN psb.total_surgeries > 1
         AND psb.first_surgery_date <> psb.last_surgery_date
         AND fem.resolved_fna_date >= psb.first_surgery_date
         AND fem.resolved_fna_date < psb.last_surgery_date THEN 'inter_op'
    WHEN fem.resolved_fna_date >= COALESCE(psb.last_surgery_date, psb.first_surgery_date)
         THEN 'post_final_op'
    ELSE 'post_final_op'
  END AS surgery_era,
  CASE
    WHEN fem.resolved_fna_date IS NOT NULL
         AND psb.first_surgery_date IS NOT NULL
         AND fem.resolved_fna_date < psb.first_surgery_date
    THEN DATE_DIFF('day', fem.resolved_fna_date, psb.first_surgery_date)
    ELSE NULL
  END AS pre_op_days,
  CASE
    WHEN fem.resolved_fna_date IS NOT NULL
         AND psb.last_surgery_date IS NOT NULL
         AND fem.resolved_fna_date >= psb.last_surgery_date
    THEN DATE_DIFF('day', psb.last_surgery_date, fem.resolved_fna_date)
    ELSE NULL
  END AS post_op_days
FROM fna_episode_master_v2 fem
LEFT JOIN patient_surgery_bounds psb ON fem.research_id = psb.research_id
LEFT JOIN nearest_surgery ns
  ON fem.research_id = ns.research_id
  AND fem.fna_episode_id = ns.fna_episode_id
  AND ns.rn = 1
LEFT JOIN first_surgery_fallback fsf
  ON fem.research_id = fsf.research_id
  AND ns.nearest_surgery_episode_id IS NULL
  AND psb.research_id IS NOT NULL;

-- ================================================================
-- VIEW 3: v_fna_bethesda_surface
-- ================================================================
CREATE OR REPLACE VIEW v_fna_bethesda_surface AS
SELECT
  fem.research_id,
  fem.fna_episode_id,
  fem.resolved_fna_date,
  fem.laterality,
  fem.specimen_site_raw,
  fem.bethesda_raw,
  fem.bethesda_category,
  fc.bethesda_2010_num,
  fc.bethesda_2010_name,
  fc.bethesda_2015_num,
  fc.bethesda_2015_name,
  fc.bethesda_2023_num,
  fc.bethesda_2023_name,
  fc.confidence,
  fc.method,
  fc.subtype,
  fc.path_text_length,
  CASE WHEN fc.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_cytology_match
FROM fna_episode_master_v2 fem
LEFT JOIN fna_cytology fc
  ON fc.research_id::INT = fem.research_id
  AND fc.fna_index = fem.fna_episode_id;

-- ================================================================
-- BACKFILL: linked_imaging_nodule_id
-- ================================================================
-- Uses imaging_fna_linkage_v3 rank-1, tier IN (exact_match, high_confidence, plausible)
UPDATE fna_episode_master_v2 fem
SET linked_imaging_nodule_id = lnk.nodule_id
FROM imaging_fna_linkage_v3 lnk
WHERE fem.fna_episode_id = lnk.fna_episode_id
  AND fem.research_id = lnk.research_id
  AND lnk.score_rank = 1
  AND lnk.linkage_confidence_tier IN ('exact_match', 'high_confidence', 'plausible');

-- ================================================================
-- BACKFILL: linked_surgery_episode_id
-- Phase A: FNAs with dates → nearest surgery (no window constraint)
-- Phase B: FNAs without dates → first surgery (pre-op assumption)
-- ================================================================

-- Phase A
UPDATE fna_episode_master_v2 fem
SET linked_surgery_episode_id = sc.surgery_episode_id::VARCHAR
FROM (
  SELECT
    fem2.research_id,
    fem2.fna_episode_id,
    pst.surgery_episode_id,
    ROW_NUMBER() OVER (
      PARTITION BY fem2.research_id, fem2.fna_episode_id
      ORDER BY ABS(DATE_DIFF('day', fem2.resolved_fna_date, pst.surgery_date)),
               pst.surgery_episode_id
    ) AS match_rank
  FROM fna_episode_master_v2 fem2
  JOIN v_patient_surgery_timeline pst ON fem2.research_id = pst.research_id
  WHERE fem2.resolved_fna_date IS NOT NULL
    AND pst.surgery_date IS NOT NULL
) sc
WHERE fem.research_id = sc.research_id
  AND fem.fna_episode_id = sc.fna_episode_id
  AND sc.match_rank = 1;

-- Phase B: NULL-date FNAs → first surgery
UPDATE fna_episode_master_v2 fem
SET linked_surgery_episode_id = fsf.surgery_episode_id::VARCHAR
FROM (
  SELECT research_id, surgery_episode_id
  FROM v_patient_surgery_timeline
  WHERE is_first_surgery = TRUE
) fsf
WHERE fem.research_id = fsf.research_id
  AND fem.linked_surgery_episode_id IS NULL;
