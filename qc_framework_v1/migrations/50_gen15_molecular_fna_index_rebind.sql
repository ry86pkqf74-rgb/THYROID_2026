-- ============================================================================
-- Migration 50 — GEN15: molecular linked_fna_episode_id → canonical fna_index rebind
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      GEN15 — main.canonical_molecular_genetics_v2.linked_fna_episode_id
--                carries VARCHAR IDs in the 1..8117 range (legacy global FNA
--                row ordinal) that do NOT match canonical_fna_events_v1.fna_index
--                (patient-local 1..12) nor fna_event_id (hash). Per-patient
--                rebind to fna_index via (rid, nearest test_date_native ≈
--                fna_date_resolved) recovers 374 linked rows.
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24):
--   374 non-null linked_fna_episode_id rows, 355 distinct values.
--   All 374 have non-null test_date_native.
--   All 359 mol rids exist in canonical_fna_events_v1.
--
--   Per-patient nearest-FNA rebind (partition by (rid, molecular_episode_id),
--   order by |gap| ASC NULLS LAST, fna_index ASC):
--     290 exact-date match  (trusted)
--      17 within 7d          (high confidence)
--      35 within 30d         (high confidence)
--      24 within 90d         (medium confidence)
--       1 within 365d        (medium-low)
--       7 over 365d          (low — queue for audit)
--     total: 374
--
-- Output:
--   manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind (VIEW)
--     + fna_index_rebound                     BIGINT  (patient-local fna_index)
--     + fna_event_id_rebound                  VARCHAR (hash pk on fna_events)
--     + gen15_fna_rebind_gap_days             INTEGER
--     + gen15_fna_rebind_confidence           VARCHAR ∈ {exact, high_7d,
--                                                        high_30d, medium_90d,
--                                                        low_365d, over_365d,
--                                                        no_link_stored}
--     + gen15_rebind_applied_flag             BOOLEAN
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind AS
WITH ranked AS (
  SELECT m.research_id,
         m.molecular_episode_id,
         f.fna_event_id,
         f.fna_index,
         ABS(CAST(f.fna_date_resolved - CAST(m.test_date_native AS DATE) AS INTEGER)) AS gap_days,
         ROW_NUMBER() OVER (PARTITION BY m.research_id, m.molecular_episode_id
                            ORDER BY ABS(CAST(f.fna_date_resolved - CAST(m.test_date_native AS DATE) AS INTEGER)) ASC NULLS LAST,
                                     f.fna_index ASC) AS rn
  FROM main.canonical_molecular_genetics_v2 m
  JOIN main.canonical_fna_events_v1 f
    ON f.research_id = m.research_id
  WHERE m.linked_fna_episode_id IS NOT NULL
),
pick AS (
  SELECT research_id, molecular_episode_id, fna_event_id, fna_index, gap_days
  FROM ranked WHERE rn=1
)
SELECT
  m.*,
  p.fna_index      AS fna_index_rebound,
  p.fna_event_id   AS fna_event_id_rebound,
  p.gap_days       AS gen15_fna_rebind_gap_days,
  CASE
    WHEN m.linked_fna_episode_id IS NULL           THEN 'no_link_stored'
    WHEN p.gap_days IS NULL                        THEN 'no_candidate'
    WHEN p.gap_days = 0                            THEN 'exact'
    WHEN p.gap_days BETWEEN 1 AND 7                THEN 'high_7d'
    WHEN p.gap_days BETWEEN 8 AND 30               THEN 'high_30d'
    WHEN p.gap_days BETWEEN 31 AND 90              THEN 'medium_90d'
    WHEN p.gap_days BETWEEN 91 AND 365             THEN 'low_365d'
    ELSE 'over_365d'
  END AS gen15_fna_rebind_confidence,
  (m.linked_fna_episode_id IS NOT NULL AND p.fna_index IS NOT NULL) AS gen15_rebind_applied_flag
FROM main.canonical_molecular_genetics_v2 m
LEFT JOIN pick p
  ON p.research_id = m.research_id AND p.molecular_episode_id = m.molecular_episode_id;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='GEN15';

-- Queue only the 7 over_365d rows (low confidence — likely mis-linkage)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'GEN15', TRY_CAST(research_id AS INTEGER),
  'main.canonical_molecular_genetics_v2',
  CAST(molecular_episode_id AS VARCHAR),
  TO_JSON(struct_pack(
    linked_fna_episode_id_stored := linked_fna_episode_id,
    test_date_native := test_date_native,
    fna_index_rebound := fna_index_rebound,
    rebind_gap_days := gen15_fna_rebind_gap_days,
    rebind_confidence := gen15_fna_rebind_confidence
  )),
  'GEN15 molecular linked_fna_episode_id rebinds to nearest FNA >365d away — likely mis-linkage',
  'open', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind
WHERE gen15_fna_rebind_confidence='over_365d';

COMMENT ON TABLE main.canonical_molecular_genetics_v2 IS
'Molecular genetics canonical (1,384 events / 1,151 pts). Clean view manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind surfaces fna_index_rebound + fna_event_id_rebound + gen15_fna_rebind_gap_days + gen15_fna_rebind_confidence ∈ {exact 290, high_7d 17, high_30d 35, medium_90d 24, low_365d 1, over_365d 7, no_link_stored 1010} + gen15_rebind_applied_flag. 7 over_365d queued. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_49';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_molecular_genetics_v2.linked_fna_episode_id','column',
   'manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind.fna_index_rebound',
   'GEN15','prompt_49','column_only',DATE '2026-04-24',
   'GEN15: 374 linked_fna_episode_id values carry legacy 1..8117 global row ordinals that do not match canonical_fna_events_v1.fna_index (patient-local 1..12) nor fna_event_id (hash). Per-patient nearest-FNA rebind via (rid, |test_date_native - fna_date_resolved| ASC): 290 exact + 17 high_7d + 35 high_30d + 24 medium_90d + 1 low_365d + 7 over_365d. 7 queued for audit.',
   NULL,
   'Downstream molecular↔FNA joins should use fna_index_rebound (patient-local, consistent with canonical_fna_events_v1 primary key grain) or fna_event_id_rebound (hash). High-confidence analytical set: gen15_fna_rebind_confidence IN (''exact'',''high_7d'',''high_30d''). Original linked_fna_episode_id preserved on main for audit.');
