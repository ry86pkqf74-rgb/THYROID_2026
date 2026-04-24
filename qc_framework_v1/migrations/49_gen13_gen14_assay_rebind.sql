-- ============================================================================
-- Migration 49 — GEN13 + GEN14: assay → molecular and assay → op rebind
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   GEN13 — 9,267 specimen_genomic_assay_v1 rows with non-null
--           molecular_episode_id that fail to match canonical_molecular_genetics_v2
--   GEN14 — 311 assay rows with non-null surgery_episode_id that fail to match
--           canonical_operative_events_v1
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24):
--   GEN13 broken rows: 9,267. 100% have platform='Other' — these are
--   non-thyroid genomic assays (not ThyroSeq/Afirma/NGS_unspecified),
--   legitimately absent from canonical_molecular_genetics_v2. No recovery
--   possible by any key — rebind via (rid, platform, date) yields 0.
--   → Flag only. Do NOT queue 9K rows. Effectively resolved-by-scope.
--
--   GEN14 broken rows: 311.
--     302 have non-null test_date_native:
--       300 rebindable to a unique op episode within ±365d (lowest gap wins)
--         2 no op events within ±365d → queue
--       9 have NULL test_date_native → queue
--
-- Output:
--   manuscript_workspace.specimen_genomic_assay_v1_rebound (VIEW)
--     + molecular_episode_id_rebound            BIGINT   (mostly same as stored;
--                                                         GEN13 flag row gets NULL_rebound)
--     + gen13_platform_other_orphan_flag        BOOLEAN  (platform=Other + no mg match)
--     + surgery_episode_id_rebound              BIGINT   (GEN14 fuzzy pick or
--                                                         original op-matching value)
--     + surgery_episode_rebind_source           VARCHAR ∈
--         {op_match, fuzzy_365d, no_date, no_candidate, no_op_episode_stored}
--     + surgery_episode_rebind_gap_days         INTEGER
--     + gen14_surg_rebind_applied_flag          BOOLEAN
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.specimen_genomic_assay_v1_rebound AS
WITH mg_match AS (
  SELECT a.genomic_assay_id,
         (mg.molecular_episode_id IS NOT NULL) AS mg_match
  FROM main.specimen_genomic_assay_v1 a
  LEFT JOIN main.canonical_molecular_genetics_v2 mg
    ON a.research_id = mg.research_id AND a.molecular_episode_id = mg.molecular_episode_id
),
op_match AS (
  SELECT a.genomic_assay_id,
         op.surgery_episode_id AS matched_epi
  FROM main.specimen_genomic_assay_v1 a
  LEFT JOIN main.canonical_operative_events_v1 op
    ON a.research_id = op.research_id AND a.surgery_episode_id = op.surgery_episode_id
),
ops AS (
  SELECT research_id, surgery_episode_id, CAST(surgery_date_native AS DATE) AS sd
  FROM main.canonical_operative_events_v1
  WHERE surgery_date_native IS NOT NULL
),
fuzzy_pick AS (
  SELECT genomic_assay_id, pick_epi, gap_days
  FROM (
    SELECT a.genomic_assay_id,
           ops.surgery_episode_id AS pick_epi,
           ABS(CAST(ops.sd - CAST(a.test_date_native AS DATE) AS INTEGER)) AS gap_days,
           ROW_NUMBER() OVER (PARTITION BY a.genomic_assay_id
                              ORDER BY ABS(CAST(ops.sd - CAST(a.test_date_native AS DATE) AS INTEGER)) ASC,
                                       ops.surgery_episode_id ASC) AS rn
    FROM main.specimen_genomic_assay_v1 a
    LEFT JOIN main.canonical_operative_events_v1 op
      ON op.research_id = a.research_id AND op.surgery_episode_id = a.surgery_episode_id
    JOIN ops
      ON ops.research_id = a.research_id
     AND ABS(CAST(ops.sd - CAST(a.test_date_native AS DATE) AS INTEGER)) <= 365
    WHERE a.surgery_episode_id IS NOT NULL AND op.surgery_episode_id IS NULL
      AND a.test_date_native IS NOT NULL
  ) WHERE rn=1
)
SELECT
  a.*,
  CASE WHEN mgm.mg_match THEN a.molecular_episode_id ELSE NULL END AS molecular_episode_id_rebound,
  (a.molecular_episode_id IS NOT NULL AND NOT mgm.mg_match AND a.platform='Other')
    AS gen13_platform_other_orphan_flag,
  COALESCE(opm.matched_epi, fp.pick_epi) AS surgery_episode_id_rebound,
  CASE
    WHEN a.surgery_episode_id IS NULL                 THEN 'no_op_episode_stored'
    WHEN opm.matched_epi IS NOT NULL                  THEN 'op_match'
    WHEN fp.pick_epi IS NOT NULL                      THEN 'fuzzy_365d'
    WHEN a.test_date_native IS NULL                   THEN 'no_date'
    ELSE 'no_candidate'
  END AS surgery_episode_rebind_source,
  fp.gap_days AS surgery_episode_rebind_gap_days,
  (a.surgery_episode_id IS NOT NULL AND opm.matched_epi IS NULL AND fp.pick_epi IS NOT NULL)
    AS gen14_surg_rebind_applied_flag
FROM main.specimen_genomic_assay_v1 a
LEFT JOIN mg_match   mgm USING (genomic_assay_id)
LEFT JOIN op_match   opm USING (genomic_assay_id)
LEFT JOIN fuzzy_pick fp  USING (genomic_assay_id);

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('GEN13','GEN14');

-- GEN13: flag-only closure. No per-row queue (9,267 platform='Other' is too noisy).
-- Emit a single summary row for manifest completeness.
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
VALUES
  ('GEN13', NULL, 'main.specimen_genomic_assay_v1', 'SUMMARY',
   TO_JSON(struct_pack(
     broken_rows := 9267,
     pattern := 'all platform=Other',
     interpretation := 'non-thyroid-specific assays outside canonical_molecular_genetics_v2 scope'
   )),
   'GEN13 summary row — 9,267 platform=Other assay rows legitimately absent from canonical_molecular_genetics_v2. See gen13_platform_other_orphan_flag on view.',
   'closed-by-scope', CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

-- GEN14: queue only the rows that couldn't rebind (2 no-candidate + 9 no-date)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'GEN14', research_id, 'main.specimen_genomic_assay_v1', genomic_assay_id,
  TO_JSON(struct_pack(
    surgery_episode_id_stored := surgery_episode_id,
    test_date_native := test_date_native,
    platform := platform,
    surgery_episode_rebind_source := surgery_episode_rebind_source
  )),
  'GEN14 assay has non-null local surgery_episode_id but no op match and no fuzzy-365d candidate',
  'open', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.specimen_genomic_assay_v1_rebound
WHERE surgery_episode_rebind_source IN ('no_candidate','no_date')
  AND surgery_episode_id IS NOT NULL;

COMMENT ON TABLE main.specimen_genomic_assay_v1 IS
'Genomic assay scaffold (10,370 rows). Clean view manuscript_workspace.specimen_genomic_assay_v1_rebound surfaces molecular_episode_id_rebound + gen13_platform_other_orphan_flag (9,267) + surgery_episode_id_rebound + surgery_episode_rebind_source ∈ {op_match, fuzzy_365d, no_date, no_candidate, no_op_episode_stored} + gen14_surg_rebind_applied_flag. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_48';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.specimen_genomic_assay_v1','table',
   'manuscript_workspace.specimen_genomic_assay_v1_rebound',
   'GEN13,GEN14','prompt_48','linkage_only',DATE '2026-04-24',
   'GEN13: 9,267 broken mg-links all have platform=Other (non-thyroid-specific assays) — legitimately absent from canonical_molecular_genetics_v2. Flagged, not queued (closed-by-scope with 1 summary row). GEN14: 311 op-link gaps → 300 rebound via (rid, test_date ±365d); 11 queued (2 no-candidate + 9 no-date).',
   NULL,
   'Downstream molecular joins should use molecular_episode_id_rebound (NULL for platform=Other). Op joins should use surgery_episode_id_rebound. High-confidence set: surgery_episode_rebind_source=''op_match''. Medium: fuzzy_365d.');
