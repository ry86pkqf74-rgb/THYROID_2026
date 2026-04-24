-- ============================================================================
-- Migration 43 — FNA03: days_to_surgery recompute from earliest_surgery_date
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      FNA03 — canonical_fna_events_v1.days_to_surgery has
--                max=736,618 days (≈2,000 yrs — classic year-0014 parse bug)
--                and 198 rows with |dts| > 3650 days on the original column.
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Approach:
--   View layer over canonical_fna_events_v1_date_clean (mig 42) ×
--   canonical_operative_patient_rollup_v1.earliest_surgery_date.
--   Recomputed days = earliest_surgery_date − fna_date_resolved_final
--                     (positive ⇒ FNA before surgery; negative ⇒ surveillance).
--
-- Recompute probe (2026-04-24):
--   8,119 total | 8,018 will have dts_recomputed (99 pts no op rollup +
--   2 with NULL date post-mig-42).
--   n_recomputed_vs_existing match:     6,413 agree
--                 disagree by any Δ:      124
--                 newly computed (gained): 1,481 (from mig 42 reparse)
--   Range distribution of recomputed dts:
--     -730..-31    209   (long-range post-surgery surveillance FNAs)
--     -30..-1       17
--     0              9
--     1..30        886
--     31..180    3,765   (bulk surgery-ready)
--     181..365     696
--     366..1,095   900
--     1,096..3,650 1,006 (pre-diagnosis/benign→malignant conversion)
--     lt_-730      318   (>2yr post-op surveillance — valid)
--     gt_3650      212   (>10yr pre-op — usually data error → flag)
--
-- Outputs on manuscript_workspace.canonical_fna_events_v1_dts_clean:
--   + days_to_surgery_recomputed   INTEGER
--   + dts_source                   VARCHAR ∈ {recomputed, no_fna_date, no_surgery}
--   + fna03_dts_implausible_flag   BOOLEAN  (|recomputed| > 3650)
--   + fna03_dts_drift_flag         BOOLEAN  (|recomputed - existing| > 30 AND both present)
--   + fna03_surveillance_flag      BOOLEAN  (recomputed < -30 — informational)
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_fna_events_v1_dts_clean AS
SELECT
  c.*,
  op.earliest_surgery_date AS earliest_surgery_date_rollup,
  CASE
    WHEN c.fna_date_resolved_final IS NOT NULL AND op.earliest_surgery_date IS NOT NULL
      THEN CAST(op.earliest_surgery_date - c.fna_date_resolved_final AS INTEGER)
    ELSE NULL
  END AS days_to_surgery_recomputed,
  CASE
    WHEN c.fna_date_resolved_final IS NULL THEN 'no_fna_date'
    WHEN op.earliest_surgery_date IS NULL THEN 'no_surgery'
    ELSE 'recomputed'
  END AS dts_source,
  (c.fna_date_resolved_final IS NOT NULL
   AND op.earliest_surgery_date IS NOT NULL
   AND ABS(CAST(op.earliest_surgery_date - c.fna_date_resolved_final AS INTEGER)) > 3650
  ) AS fna03_dts_implausible_flag,
  (c.fna_date_resolved_final IS NOT NULL
   AND op.earliest_surgery_date IS NOT NULL
   AND c.days_to_surgery IS NOT NULL
   AND ABS(CAST(op.earliest_surgery_date - c.fna_date_resolved_final AS INTEGER) - c.days_to_surgery) > 30
  ) AS fna03_dts_drift_flag,
  (c.fna_date_resolved_final IS NOT NULL
   AND op.earliest_surgery_date IS NOT NULL
   AND CAST(op.earliest_surgery_date - c.fna_date_resolved_final AS INTEGER) < -30
  ) AS fna03_surveillance_flag
FROM manuscript_workspace.canonical_fna_events_v1_date_clean c
LEFT JOIN main.canonical_operative_patient_rollup_v1 op USING (research_id);

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='FNA03';

-- Queue: implausible (|dts| > 10yrs) — these are the classic year-0014 parse bugs
-- AND the <1960 OCR'd dates. Drift rows are NOT queued (audit-only: the old
-- column is derived from the old NULL-resolved date, so drift-for-drift's-sake is noise).
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'FNA03',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_fna_events_v1',
  CAST(fna_event_id AS VARCHAR),
  TO_JSON(struct_pack(
    fna_date_raw := fna_date_raw,
    fna_date_resolved_final := fna_date_resolved_final,
    earliest_surgery_date := earliest_surgery_date_rollup,
    days_to_surgery_existing := days_to_surgery,
    days_to_surgery_recomputed := days_to_surgery_recomputed,
    fna_date_resolved_source := fna_date_resolved_source
  )),
  'FNA03 |days_to_surgery| > 3650 after recompute — likely FNA date OCR/parse error',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_fna_events_v1_dts_clean
WHERE fna03_dts_implausible_flag;

COMMENT ON TABLE main.canonical_fna_events_v1 IS
'FNA event table (8,119 rows). Clean views: manuscript_workspace.canonical_fna_events_v1_date_clean (mig 42) → canonical_fna_events_v1_dts_clean (mig 43). Latter carries days_to_surgery_recomputed, dts_source, fna03_dts_implausible_flag, fna03_dts_drift_flag (|Δ|>30d vs original), fna03_surveillance_flag. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_42';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_fna_events_v1.days_to_surgery','column',
   'manuscript_workspace.canonical_fna_events_v1_dts_clean.days_to_surgery_recomputed',
   'FNA03','prompt_42','column_only',DATE '2026-04-24',
   'FNA03: 12 rows with dts > 100,000 days (year-0014 parse bug) + 198 with |dts| > 3650 on original column. Recompute from earliest_surgery_date - fna_date_resolved_final yields 8,018 dts_recomputed; 212 implausible rows queued.',
   NULL,
   'Recompute uses earliest_surgery_date_rollup (canonical_operative_patient_rollup_v1) as reference. 1,481 rows gain dts_recomputed via mig-42 reparse. fna03_dts_drift_flag surfaces the 124 row-level disagreements vs the stored column for audit; not queued (expected drift from date reparse).');
