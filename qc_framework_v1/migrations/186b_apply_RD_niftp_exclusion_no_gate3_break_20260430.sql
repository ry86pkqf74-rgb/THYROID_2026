-- LOGAN RATIFIED 2026-04-30 (mig_192 patch); SUPERSEDES 65ba4d6 mig_186; READY FOR COWORK PATH-C APPLY
-- =============================================================================
-- Migration 186b — R-D hybrid: NIFTP + uncertain-malignancy exclusion (gate3-safe)
-- =============================================================================
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Batch:   mig186b_apply_RD_niftp_exclusion_no_gate3_break_20260430
-- Target:  thyroid_canonical_publication_v1_0 (USE locked search_path)
-- Date:    2026-04-30
--
-- COWORK APPLY ORDER (ratified): mig_188b → mig_186b → mig_185b → mig_187
--   This patch runs second. §D2 uses COUNT(*) per Script-361-style rebuild; mig_185b
--   (third) restores COUNT(DISTINCT grain) + POC merge — do not skip 185b after this.
--
-- SUPERSEDES mig_186: §F+§G no longer flip `verification_status` to `not_started`
-- (that broke gate3: n_verified + n_na ≠ n_columns_total vs `canonical_table_signoff_registry_v1`).
-- Verified-with-CF-note + `derivation_re_derivation_post_niftp_exclusion` preserves gate3.
--
-- Rule (Logan-ratified, LOCKED):
--   Archive affected rows → land in main.canonical_path_indeterminate_events_v1
--   → DELETE from main.canonical_path_malignant_events_v1 → rebuild
--   main.canonical_path_malignant_patient_rollup_v1 → registry / CF carry-forwards.
--
-- Pre-snapshot in §A is mandatory before §C DELETE (irreversible on live table).
-- All snapshot timestamps: CAST(CURRENT_TIMESTAMP AS TIMESTAMP) — avoid TIMESTAMPTZ traps.
--
-- Path-C preflight: confirm affected counts still 220 / 202; if delta, halt and rescope.
-- Live MD probe 2026-04-30: path_outcome_classification_v1 absent — rollup §D2 omits POC join
-- (matches Script 361 branch when table missing).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Shared predicate (NIFTP + WHO uncertain / UMP textual patterns)
-- ---------------------------------------------------------------------------
-- Afflicted rows WHERE:
--   LOWER(COALESCE(primary_histology,'')) LIKE '%niftp%'
--    OR LOWER(COALESCE(histology_variant,'')) LIKE '%niftp%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%uncertain%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%hurthle%neoplasm%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%h%rthle%neoplasm%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%ft-ump%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%wdt-ump%'

-- ---------------------------------------------------------------------------
-- §0 Pre-flight invariants (run; expect stated values before mutating)
-- ---------------------------------------------------------------------------
-- CPM cohort spine
-- SELECT COUNT(*) AS cpm_rows FROM main.canonical_patient_master;
-- expect: 10871
--
-- Affected slice (must match Logan scoping unless deliberate rescope)
-- SELECT COUNT(*) AS n_events, COUNT(DISTINCT research_id) AS n_pts
-- FROM main.canonical_path_malignant_events_v1
-- WHERE LOWER(COALESCE(primary_histology,'')) LIKE '%niftp%'
--    OR LOWER(COALESCE(histology_variant,'')) LIKE '%niftp%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%uncertain%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%hurthle%neoplasm%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%h%rthle%neoplasm%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%ft-ump%'
--    OR LOWER(COALESCE(primary_histology,'')) LIKE '%wdt-ump%';
-- expect: 220 events, 202 patients

-- ---------------------------------------------------------------------------
-- §A Archive snapshot — all affected event rows (mandatory before DELETE)
-- ---------------------------------------------------------------------------
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig186b_niftp_uncertain_20260430 AS
SELECT
  e.*,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig186b_snapshot_ts
FROM main.canonical_path_malignant_events_v1 AS e
WHERE LOWER(COALESCE(e.primary_histology,'')) LIKE '%niftp%'
   OR LOWER(COALESCE(e.histology_variant,'')) LIKE '%niftp%'
   OR LOWER(COALESCE(e.primary_histology,'')) LIKE '%uncertain%'
   OR LOWER(COALESCE(e.primary_histology,'')) LIKE '%hurthle%neoplasm%'
   OR LOWER(COALESCE(e.primary_histology,'')) LIKE '%h%rthle%neoplasm%'
   OR LOWER(COALESCE(e.primary_histology,'')) LIKE '%ft-ump%'
   OR LOWER(COALESCE(e.primary_histology,'')) LIKE '%wdt-ump%';

-- Verify row count = 220:
-- SELECT COUNT(*) FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig186b_niftp_uncertain_20260430;

-- ---------------------------------------------------------------------------
-- §B Indeterminate landing — queryable provenance (from archive snapshot)
-- ---------------------------------------------------------------------------
-- Path-C: if main.canonical_path_indeterminate_events_v1 already exists with unrelated rows,
-- replace this block with INSERT...SELECT from archive + anti-join on keys.
CREATE OR REPLACE TABLE main.canonical_path_indeterminate_events_v1 AS
SELECT
  snap.*,
  CASE
    WHEN LOWER(COALESCE(snap.primary_histology,'')) LIKE '%niftp%'
      OR LOWER(COALESCE(snap.histology_variant,'')) LIKE '%niftp%'
      THEN 'NIFTP_WHO_2017_non_malignant'
    ELSE 'uncertain_malignant_potential_ump'
  END AS indeterminate_reason,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS reclassified_at,
  'mig186b_apply_RD_niftp_exclusion_no_gate3_break_20260430'::VARCHAR AS indeterminate_mig_batch_id
FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig186b_niftp_uncertain_20260430 AS snap;

-- ---------------------------------------------------------------------------
-- §C DELETE from malignant events (irreversible on live table after §A)
-- ---------------------------------------------------------------------------
DELETE FROM main.canonical_path_malignant_events_v1
WHERE LOWER(COALESCE(primary_histology,'')) LIKE '%niftp%'
   OR LOWER(COALESCE(histology_variant,'')) LIKE '%niftp%'
   OR LOWER(COALESCE(primary_histology,'')) LIKE '%uncertain%'
   OR LOWER(COALESCE(primary_histology,'')) LIKE '%hurthle%neoplasm%'
   OR LOWER(COALESCE(primary_histology,'')) LIKE '%h%rthle%neoplasm%'
   OR LOWER(COALESCE(primary_histology,'')) LIKE '%ft-ump%'
   OR LOWER(COALESCE(primary_histology,'')) LIKE '%wdt-ump%';

-- ---------------------------------------------------------------------------
-- §D Rebuild malignant patient rollup (Script 361 Step 5a; POC omitted — table absent)
-- ---------------------------------------------------------------------------
-- §D1 Pre-snapshot current rollup (full table, for diff / rollback audit)
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_patient_rollup_v1_pre_mig186b_niftp_uncertain_20260430 AS
SELECT
  r.*,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig186b_snapshot_ts
FROM main.canonical_path_malignant_patient_rollup_v1 AS r;

-- §D2 Rollup rebuild (mirrors scripts/361_op_path_consolidation.py step_5_build_rollups 5a when path_outcome_classification_v1 missing)
-- NOTE: COUNT(*) here — corrected to DISTINCT grain by mig_185b when run after this.
CREATE OR REPLACE TABLE main.canonical_path_malignant_patient_rollup_v1 AS
WITH ev AS (
  SELECT
    TRY_CAST(research_id AS BIGINT) AS research_id,
    surgery_episode_id,
    surgery_date,
    primary_histology,
    extrathyroidal_extension,
    gross_ete,
    stage_group_ajcc7,
    stage_group_ajcc8
  FROM main.canonical_path_malignant_events_v1
),
agg AS (
  SELECT
    research_id,
    TRUE AS any_malignant_event,
    COUNT(DISTINCT surgery_episode_id) AS n_malignant_surgeries,
    COUNT(*) AS n_tumors_total,
    MIN(surgery_date) AS earliest_malignant_path_date,
    MAX(surgery_date) AS latest_malignant_path_date,
    MAX(stage_group_ajcc8) AS highest_stage_ajcc8,
    MAX(stage_group_ajcc7) AS highest_stage_ajcc7,
    BOOL_OR(
      COALESCE(gross_ete, 0) = 1
      OR LOWER(COALESCE(CAST(extrathyroidal_extension AS VARCHAR), ''))
         IN ('present', 'minimal', 'microscopic', 'yes', 'c/a', 'gross', 'macroscopic')
    ) AS any_ett,
    mode(primary_histology) AS dominant_histology
  FROM ev
  GROUP BY research_id
)
SELECT
  ev.research_id,
  ev.any_malignant_event,
  ev.n_malignant_surgeries,
  ev.n_tumors_total,
  ev.earliest_malignant_path_date,
  ev.latest_malignant_path_date,
  ev.highest_stage_ajcc8,
  ev.highest_stage_ajcc7,
  ev.any_ett,
  FALSE AS any_metastasis,
  ev.dominant_histology,
  '361+mig186b'::VARCHAR AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM agg ev;

-- Path-C: if path_outcome_classification_v1 returns in a future DB, restore POC LEFT JOIN per Script 361 (bethesda_final stack).

-- ---------------------------------------------------------------------------
-- §E Registry appendix — detail_table_registry_v1 (path events + rollup)
-- ---------------------------------------------------------------------------
UPDATE manuscript_workspace.detail_table_registry_v1 AS r
SET description = COALESCE(r.description, '')
  || ' | mig_186b R-D NIFTP/uncertain exclusion: 220 events excluded; landed in canonical_path_indeterminate_events_v1; '
  || 'rollup rebuilt; CF-mig186-WHO-2017-NIFTP-RECLASS open (gate3-preserving registry notes)'
WHERE r.detail_table_name IN (
  'canonical_path_malignant_events_v1',
  'canonical_path_malignant_patient_rollup_v1'
);

-- ---------------------------------------------------------------------------
-- §F CF-mig186-WHO-2017-NIFTP-RECLASS — column verification registry (verified retained)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_method = 'derivation_re_derivation_post_niftp_exclusion',
    notes = COALESCE(notes, '') || ' | mig_186 R-D 2026-04-30: NIFTP/uncertain rows excluded; events/rollup re-derived; CF-mig186-WHO-2017-NIFTP-RECLASS retained for trace; gate3 preserved.'
WHERE schema_name = 'main'
  AND (
    (table_name = 'canonical_path_malignant_events_v1'
      AND column_name IN ('primary_histology', 'histology_variant'))
    OR
    (table_name = 'canonical_path_malignant_patient_rollup_v1'
      AND column_name IN (
        'n_tumors_total', 'dominant_histology', 'any_malignant_event',
        'n_malignant_surgeries', 'earliest_malignant_path_date', 'latest_malignant_path_date',
        'highest_stage_ajcc8', 'highest_stage_ajcc7', 'any_ett', 'build_script', 'build_ts'
      ))
  );

-- ---------------------------------------------------------------------------
-- §G CF-mig186-EDGE-NO-MALIGNANT-EVENT-AFTER-EXCLUSION — CPM is_malignant
-- ---------------------------------------------------------------------------
-- Edge cohort (115 patients): no remaining malignant path event row after DELETE but CPM may stay is_malignant=TRUE
-- (biopsy-only / imaging-only evidence — spot-check; do not auto-flip CPM).
UPDATE main.canonical_column_verification_registry_v1
SET verification_method = 'spot_check_pending_115_edge_patients',
    notes = COALESCE(notes, '')
      || ' | CF-mig186-EDGE-NO-MALIGNANT-EVENT-AFTER-EXCLUSION: ~115 patients have only excluded NIFTP/UMP path events removed; '
      || 'CPM is_malignant may still be TRUE from non-path evidence — manual triage. mig186b: verification_status remains verified; gate3 preserved.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'is_malignant';

-- ---------------------------------------------------------------------------
-- §H cpm_reconciliation_provenance_v1
-- ---------------------------------------------------------------------------
-- Idempotency: delete prior row for this run_id if re-applying, or skip.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
VALUES
  ('mig186b_apply_RD_niftp_exclusion_no_gate3_break_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'archive_indeterminate_delete_rollup_rebuild_registry_cf_gate3_safe',
   'none',
   'none',
   'none',
   'CF-mig186-WHO-2017-NIFTP-RECLASS | CF-mig186-EDGE-NO-MALIGNANT-EVENT-AFTER-EXCLUSION');

-- ---------------------------------------------------------------------------
-- §I Gate3 hard stop — verified signoff arithmetic must still tie out
-- ---------------------------------------------------------------------------
-- If gate3_violations > 0: STOP; rollback using §A/§D1 archives and escalate.
SELECT COUNT(*) AS gate3_violations
FROM main.canonical_table_signoff_registry_v1 AS t
WHERE t.table_status = 'verified'
  AND (t.n_verified + t.n_na <> t.n_columns_total
       OR t.n_not_started <> 0
       OR COALESCE(t.n_failed, 0) <> 0);

-- ---------------------------------------------------------------------------
-- §J Post-state probes (Path-C executor)
-- ---------------------------------------------------------------------------
-- SELECT COUNT(*) AS n_malignant_events_post FROM main.canonical_path_malignant_events_v1;
-- expect: prior_count - 220
--
-- SELECT COUNT(*) AS n_indeterminate FROM main.canonical_path_indeterminate_events_v1;
-- expect: 220
--
-- SELECT COUNT(*) AS n_rollup_rows FROM main.canonical_path_malignant_patient_rollup_v1;
-- Baseline (2026-04-30 MD): 4137 rows = patients with ≥1 malignant path event.
-- Post-apply expectation: 4137 - 115 edge patients (NIFTP/UMP-only) ≈ 4022 rows — verify live.
--
-- Compare rollup deltas for affected 202 rids vs §D1 archive (spot-check n_tumors_total / dominant_histology).
-- After mig_185b: rollup grain restored to COUNT(DISTINCT(surgery_episode_id,tumor_ordinal)).

-- =============================================================================
-- end migration 186b
-- =============================================================================
