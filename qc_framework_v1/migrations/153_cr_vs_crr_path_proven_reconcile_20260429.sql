-- =============================================================================
-- Migration 153 — canonical_recurrence_v1 vs canonical_recurrence_resolved_v1
--           PATH-PROVEN cross-SSOT reconcile (Lane 33c)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- batch_id: mig_153_cr_vs_crr_path_proven_reconcile_20260429
--
-- Context (Cowork / Lane 33c):
--   Expected up to **22** patients with
--     main.canonical_recurrence_v1.recurrence_confirmed = FALSE
--     AND main.canonical_recurrence_resolved_v1.recurrence_status_final = 'path_proven'.
--
-- Live pre-apply verification (thyroid_canonical_publication_v1_0, agent probe
-- 2026-04-29 via scripts._md_connect.connect_locked):
--   * Drift count for the predicate above: **0**
--   * recurrence_status_final: none 9,979 | imaging_only_unconfirmed 747 |
--     path_proven **145**
--   * canonical_recurrence_v1 recurrence_confirmed TRUE: **514** (broader SSOT:
--     FNA + reop pathology + legacy structural; path_proven in CRR is PME-filtered
--     per mig_62)
--
-- Disposition:
--   * **No Option A/B UPDATE** — no rows qualify; pre-snapshot archives **not**
--     required for this batch.
--   * verification_method: cross_ssot_reconcile_verified_zero_drift_live_20260429
--
-- Traceability: materializes the reproducer cohort (empty when drift=0).
--
-- Post-apply gate:
--   * still_drifting = 0 on the reproducer below.
-- =============================================================================
-- Single DDL statement (no BEGIN/COMMIT) for compatibility with one-shot
-- duckdb.execute() runners; atomic without explicit transaction.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 1 — Traceability table (reproducer cohort; 0 rows when SSOTs agree)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE manuscript_workspace.cr_crr_reconcile_candidates_20260429 AS
WITH cr AS (
  SELECT CAST(research_id AS VARCHAR) AS rid,
         recurrence_confirmed,
         recurrence_date,
         recurrence_definition,
         recurrence_evidence_source,
         recurrence_histology,
         recurrence_site,
         recurrence_type,
         time_to_recurrence_days,
         biochemical_tg_at_recurrence
  FROM main.canonical_recurrence_v1
),
crr AS (
  SELECT CAST(research_id AS VARCHAR) AS rid,
         recurrence_status_final,
         recurrence_path_proven,
         recurrence_path_proven_date,
         recurrence_path_proven_source,
         recurrence_path_proven_evidence
  FROM main.canonical_recurrence_resolved_v1
)
SELECT
  cr.*,
  crr.recurrence_status_final,
  crr.recurrence_path_proven,
  crr.recurrence_path_proven_date,
  crr.recurrence_path_proven_source,
  crr.recurrence_path_proven_evidence,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS reconcile_probe_ts,
  'mig_153_cr_vs_crr_path_proven_reconcile_20260429' AS batch_id
FROM cr
INNER JOIN crr USING (rid)
WHERE COALESCE(cr.recurrence_confirmed, FALSE) = FALSE
  AND crr.recurrence_status_final = 'path_proven';

COMMENT ON TABLE manuscript_workspace.cr_crr_reconcile_candidates_20260429 IS
'Lane 33c: patients where canonical_recurrence_v1 disagrees with canonical_recurrence_resolved_v1 path_proven (CR false, CRR path_proven). Empty table = 0 cross-SSOT drift. batch mig_153.';

-- ---------------------------------------------------------------------------
-- Step 2 — Post-verify (expect still_drifting = 0)
-- ---------------------------------------------------------------------------
-- SELECT COUNT(*) AS still_drifting
-- FROM (
--   WITH cr AS (
--     SELECT CAST(research_id AS VARCHAR) AS rid, recurrence_confirmed
--     FROM main.canonical_recurrence_v1
--   ),
--   crr AS (
--     SELECT CAST(research_id AS VARCHAR) AS rid, recurrence_status_final
--     FROM main.canonical_recurrence_resolved_v1
--   )
--   SELECT cr.rid
--   FROM cr
--   INNER JOIN crr USING (rid)
--   WHERE COALESCE(cr.recurrence_confirmed, FALSE) = FALSE
--     AND crr.recurrence_status_final = 'path_proven'
-- ) s;
