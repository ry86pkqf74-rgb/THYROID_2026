# Cursor Prompt — mig_201 disposition-C CF closure apply (4 stale CFs; registry-only)

**Date:** 2026-04-30
**Lane:** mig_201 / disposition_c_cf_closure_apply
**Batch (proposed):** `mig_201_disposition_c_cf_closure_apply_20260430`
**Predecessor:** mig_190 (`f3d8d5d`) — triage classified 4 CFs as disposition C "already-closed-but-tag-stale".
**Posture:** **AUTHORING SKELETON SQL.** No execute against MotherDuck (Cowork applies via Path C).
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only); produces apply SQL.
**Tool recommendation:** **Cursor Composer** — small registry-only SQL, mechanical UPDATE statements with append-only notes.

---

## Background

mig_190's CF triage (sweep at `f3d8d5d`) classified 11 mid-tier CFs and found 4 disposition-C ("already-closed-but-stale"):

| CF tag | Closed by | n_cols | Affected table |
|---|---|---:|---|
| CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any | mig_156b | 17 (1 na, 16 verified) | canonical_patient_master |
| CF-mig156-ANY-RECURRENCE- | mig_163b (recurrence undercount fix) | 13 | canonical_patient_master |
| CF-mig134-PM-LAB-DATE-ANCHOR | mig_160 (date-retype family) | 13 | canonical_patient_master |
| CF-mig154-MARGIN-MM-VARCHAR-RETYPE | mig_154 (already explicitly noted CLEAR) | 12 | canonical_patient_master |

mig_201 appends a CLOSED note to each tagged registry row so future audits + manuscript appendix queries see the closure trace.

---

## Mission

Author 1 SQL file with 4 UPDATE statements. Each appends `| <CF tag> CLOSED by <closing-mig>` to existing notes. Registry-only; no data writes.

---

## Required scope

### §1 Apply SQL

Author `qc_framework_v1/migrations/201_disposition_c_cf_closure_apply_20260430.sql`:

```sql
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

-- §E Provenance row insert
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_201_disposition_c_cf_closure_apply_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'append_closed_notes_to_4_disposition_c_cf_tags',
   '4_cfs_closed',
   'CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any | CF-mig156-ANY-RECURRENCE- | CF-mig134-PM-LAB-DATE-ANCHOR | CF-mig154-MARGIN-MM-VARCHAR-RETYPE',
   'none',
   'none');

-- §F Verify CLOSED count
SELECT 'rows_closed_this_lane' AS metric, COUNT(*) AS n
FROM main.canonical_column_verification_registry_v1
WHERE notes LIKE '% CLOSED by %' AND notes LIKE '%per mig_190 disposition C%';
```

### §2 Audit/report

Author `qc_framework_v1/reports/mig_201_disposition_c_cf_closure_apply_20260430.md`:
- §1 4 disposition-C CFs closed (table)
- §2 expected post-state row counts per CF
- §3 manuscript appendix integration — these CFs can now be cited as CLOSED in the supplement

### §3 Mark READY

Header: `-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY`

---

## Governance reminders

- Read-only investigation only. No `query_rw`.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- Registry-only mutation; preserves existing notes via append-only `||`.

---

## Deliverables

1. `qc_framework_v1/migrations/201_disposition_c_cf_closure_apply_20260430.sql`
2. `qc_framework_v1/reports/mig_201_disposition_c_cf_closure_apply_20260430.md`

Commit message: `qc: mig_201 disposition-C CF closure apply (4 stale CFs CLOSED: mig_156b prm_high_risk + mig_163b any_recurrence + mig_160 lab_date_anchor + mig_154 margin_mm_retype)`

---

End of prompt.
