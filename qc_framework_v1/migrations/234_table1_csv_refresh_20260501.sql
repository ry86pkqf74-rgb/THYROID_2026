-- =============================================================================
-- mig_234 — Lane M manuscript Table 1 refresh + cohort flow analytic CSV lineage
-- Date:   2026-05-01 (UTC)
-- Runner: qc_framework_v1/scripts/build_mig234_lane_m_table1_refresh.py (writes CSVs)
-- MotherDuck DB: thyroid_canonical_publication_v1_0
--
-- Query definitions live under qc_framework_v1/manuscript/mig234_lane_m/*.sql
--
-- Optional filesystem exports (client-local paths — uncomment adjust cwd):
--   COPY (<see scripts/table fragment>) TO 'manuscript_outputs/v1_0_20260501/Table_1_cohort_demographics_v1_0_20260501.csv' (HEADER, DELIMITER ',');
--
-- Idempotent provenance stamp:
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1
WHERE run_id = 'mig_234_table1_refresh_v15';

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'mig_234_table1_refresh_v15',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'lane_m_mig234_csv_refresh_methods_bundle_v15_table1_to_table5_cohort_flow',
  '0',
  '0',
  '0',
  '0 | mig_234: regenerated manuscript_outputs/v1_0_20260501 CSV bundle via semantic_publication safe views + LN patient-safe rollup + recurrence-safe filter + supplemental canonical joins documented in docs/Methods_thyroid_canonical_pub_v1_0_20260501.md'
);
