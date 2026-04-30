-- =============================================================================
-- Migration 183 — PM vessel_count last not_started col verify + apply authoring
-- =============================================================================
-- Date: 2026-04-30
-- Batch: mig_183_pm_vessel_count_verify_apply_20260430
-- Target DB: thyroid_canonical_publication_v1_0
-- Primary table touched: main.canonical_column_verification_registry_v1
-- Data tables touched: NONE (registry/signoff/provenance only)
--
-- Scope: verify and flip the final canonical_patient_master not_started column:
-- vessel_count. Read-only lineage showed vessel_count was created by the frozen
-- canonical master assembly/consolidation lane as g.vascular_vessel_count AS
-- vessel_count. Live CPM probes show exact equality on all 46 populated rows
-- against vascular_vessel_count, vasc_vessel_count_v13, and vi_vessels_max.
-- =============================================================================

-- §0 — pre-flight invariants (read-only)
SELECT COUNT(*) AS cpm_rows,
       COUNT(DISTINCT research_id) AS cpm_distinct_research_id,
       COUNT(*) FILTER (WHERE cpm_built_at IS NULL) AS null_cpm_built_at
FROM main.canonical_patient_master;

SELECT column_name, data_type, ordinal_position, verification_status, batch_id,
       verification_method, verified_by, notes
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
ORDER BY ordinal_position;

SELECT verification_status, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
GROUP BY 1
ORDER BY 1;

-- §0b — derivation/equality evidence (read-only)
SELECT
  vessel_count IS NOT NULL AS vc_nn,
  vasc_vessel_count_v13 IS NOT NULL AS v13_nn,
  vascular_vessel_count IS NOT NULL AS vvc_nn,
  vi_vessels_max IS NOT NULL AS vmax_nn,
  COUNT(*) AS n_pts
FROM main.canonical_patient_master
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC;

SELECT
  COUNT(*) AS n_all_nn,
  COUNT(*) FILTER (WHERE vessel_count = vasc_vessel_count_v13) AS n_match_v13,
  COUNT(*) FILTER (WHERE vessel_count = vascular_vessel_count) AS n_match_vvc,
  COUNT(*) FILTER (WHERE vessel_count = vi_vessels_max) AS n_match_vmax,
  COUNT(*) FILTER (WHERE vessel_count IS DISTINCT FROM vasc_vessel_count_v13) AS n_diff_v13,
  COUNT(*) FILTER (WHERE vessel_count IS DISTINCT FROM vascular_vessel_count) AS n_diff_vvc,
  COUNT(*) FILTER (WHERE vessel_count IS DISTINCT FROM vi_vessels_max) AS n_diff_vmax
FROM main.canonical_patient_master
WHERE vessel_count IS NOT NULL
  AND vasc_vessel_count_v13 IS NOT NULL
  AND vascular_vessel_count IS NOT NULL
  AND vi_vessels_max IS NOT NULL;

-- §A — pre-snapshot of the affected registry row
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig183_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig183_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'vessel_count';

-- §B — Path-C stamp on vessel_count
UPDATE main.canonical_column_verification_registry_v1
SET verified_by = 'Logan Glosser <logan.glosser@gmail.com>',
    batch_id = 'mig_183_pm_vessel_count_verify_apply_20260430',
    verification_method = 'derivation_vs_vascular_vessel_count',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = CASE
      WHEN notes IS NULL OR TRIM(notes) = '' THEN
        'mig_183 verified final PM not_started column vessel_count. Lineage: scripts/frozen/204_canonical_master_assembly.py and scripts/frozen/205_canonical_consolidation.py define g.vascular_vessel_count AS vessel_count. Live CPM probe: 46/10871 nonnull, sparse multi-valued integer measurement, exact equality to vascular_vessel_count, vasc_vessel_count_v13, and vi_vessels_max on all populated rows; not Type-A near-uniform and not Type-B placeholder.'
      WHEN POSITION('mig_183 verified final PM not_started column vessel_count' IN notes) > 0 THEN notes
      ELSE notes || ' | ' ||
        'mig_183 verified final PM not_started column vessel_count. Lineage: scripts/frozen/204_canonical_master_assembly.py and scripts/frozen/205_canonical_consolidation.py define g.vascular_vessel_count AS vessel_count. Live CPM probe: 46/10871 nonnull, sparse multi-valued integer measurement, exact equality to vascular_vessel_count, vasc_vessel_count_v13, and vi_vessels_max on all populated rows; not Type-A near-uniform and not Type-B placeholder.'
    END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'vessel_count'
  AND verification_status = 'not_started';

-- §C — status flip: vessel_count -> verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'vessel_count'
  AND verification_status = 'not_started';

-- §E — table-level signoff resync for canonical_patient_master
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/183_pm_vessel_count_verify_apply_20260430.sql',
    notes = CASE
      WHEN ts.notes IS NULL OR TRIM(ts.notes) = '' THEN
        'mig_183: verified vessel_count as exact alias of vascular_vessel_count; PM not_started reduced 1 to 0 and table_status becomes verified.'
      WHEN POSITION('mig_183: verified vessel_count as exact alias of vascular_vessel_count' IN ts.notes) > 0 THEN ts.notes
      ELSE ts.notes || ' | ' ||
        'mig_183: verified vessel_count as exact alias of vascular_vessel_count; PM not_started reduced 1 to 0 and table_status becomes verified.'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- §F — CPM reconciliation provenance row for registry-only closure
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
SELECT
  'canonical_cleanup_mig183_pm_vessel_count_verify_apply_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'pre_snapshot_derivation_audit_registry_stamp_status_flip_signoff_resync',
  'none',
  'CF-mig183-PM-VESSEL-COUNT-LAST-NOT-STARTED',
  'PM final not_started closure prerequisite for mig_162 finalization',
  'none'
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1
  WHERE run_id = 'canonical_cleanup_mig183_pm_vessel_count_verify_apply_20260430'
);

-- §G — post-state verification probes
SELECT verification_status, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
GROUP BY 1
ORDER BY 1;

SELECT table_status, n_columns_total, n_verified, n_not_started, n_failed, n_na,
       signoff_migration
FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master';