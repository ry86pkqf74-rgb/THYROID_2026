-- mig_201 disposition-C: append CLOSED trace to 4 stale CF tags on verification registry (registry-only).
-- Target DB: thyroid_canonical_publication_v1_0
-- Predecessor: mig_190 CF triage — disposition C "already-closed-but-tag-stale".
-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY

USE thyroid_canonical_publication_v1_0;

-- §A CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any → CLOSED by mig_156b
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') || ' | CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any CLOSED by mig_156b 2026-04-29 (Type-B placeholder; companion PRM rows verified; per mig_190 disposition C).'
WHERE notes ILIKE '%CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any%'
  AND notes NOT ILIKE '%CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any CLOSED%';

-- §B CF-mig156-ANY-RECURRENCE- → CLOSED by mig_163b
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') || ' | CF-mig156-ANY-RECURRENCE- CLOSED by mig_163b 2026-04-29 (recurrence undercount fix; per mig_190 disposition C).'
WHERE notes ILIKE '%CF-mig156-ANY-RECURRENCE-%'
  AND notes NOT ILIKE '%CF-mig156-ANY-RECURRENCE- CLOSED%';

-- §C CF-mig134-PM-LAB-DATE-ANCHOR → CLOSED by mig_160
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') || ' | CF-mig134-PM-LAB-DATE-ANCHOR CLOSED by mig_160 + mig_160b 2026-04-29/30 (date-retype family; lab dates DATE-typed; per mig_190 disposition C).'
WHERE notes ILIKE '%CF-mig134-PM-LAB-DATE-ANCHOR%'
  AND notes NOT ILIKE '%CF-mig134-PM-LAB-DATE-ANCHOR CLOSED%';

-- §D CF-mig154-MARGIN-MM-VARCHAR-RETYPE → CLOSED by mig_154 (already CLEAR)
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') || ' | CF-mig154-MARGIN-MM-VARCHAR-RETYPE CLOSED by mig_154 2026-04-29 (margin DOUBLE mm triple per mig_154e; already CLEAR; per mig_190 disposition C).'
WHERE notes ILIKE '%CF-mig154-MARGIN-MM-VARCHAR-RETYPE%'
  AND notes NOT ILIKE '%CF-mig154-MARGIN-MM-VARCHAR-RETYPE CLOSED%';

-- §E Provenance row insert (idempotent)
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
SELECT
  'mig_201_disposition_c_cf_closure_apply_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'append_closed_notes_to_4_disposition_c_cf_tags',
  '4_cfs_closed',
  'CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any | CF-mig156-ANY-RECURRENCE- | CF-mig134-PM-LAB-DATE-ANCHOR | CF-mig154-MARGIN-MM-VARCHAR-RETYPE',
  'none',
  'none'
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1
  WHERE run_id = 'mig_201_disposition_c_cf_closure_apply_20260430'
);

-- §F Verify CLOSED count (post-apply)
SELECT 'rows_with_mig201_closure_suffix' AS metric, COUNT(*) AS n
FROM main.canonical_column_verification_registry_v1
WHERE notes LIKE '% CLOSED by %' AND notes LIKE '%per mig_190 disposition C%';
