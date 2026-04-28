-- =============================================================================
-- Migration 66 -- FNA Step A cleanup (post-mig_65 follow-up)
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   final-cleaning directive (no audit trail for "no value change" rows)
-- Scope:  main.canonical_fna_events_v1 + manuscript_workspace.canonical_logan_review_log_v1
--
-- After mig_65 signed off fna_date_raw with 8 disposition rows in the
-- audit log, Logan reset the protocol: "I want this to be final cleaning.
-- We don't need an audit trail for those FNAs I just resolved -- just get
-- rid of those values so the table is clean."
--
-- Net effect:
--   * 8 audit-log rows from mig_65 deleted (batch_id='mig_65_fna_date_raw')
--   * 5 phantom rows removed from canonical_fna_events_v1:
--       (research_id, fna_index)
--       ('10637', 3), ('1640', 1), ('1701', 1), ('1964', 1), ('2904', 1)
--     All had NULL fna_date_raw; either no source counterpart at all (2)
--     or source = literal 'n/a' (3). None represented an actual FNA event.
--
-- Row count before: 8,119 ; after: 8,114
-- Audit log rows before: 8 ; after: 0
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

DELETE FROM manuscript_workspace.canonical_logan_review_log_v1
WHERE batch_id = 'mig_65_fna_date_raw';

DELETE FROM main.canonical_fna_events_v1
WHERE (research_id, fna_index) IN (
  ('10637', 3),
  ('1640',  1),
  ('1701',  1),
  ('1964',  1),
  ('2904',  1)
);

-- =============================================================================
-- end of migration 66
-- =============================================================================
