# mig_201 disposition-C CF closure apply — closeout

**Batch:** `mig_201_disposition_c_cf_closure_apply_20260430`  
**Date:** 2026-04-30  
**Scope:** Registry-only — append CLOSED trace to notes on `main.canonical_column_verification_registry_v1`; provenance row on `manuscript_workspace.cpm_reconciliation_provenance_v1`.  
**SQL:** `qc_framework_v1/migrations/201_disposition_c_cf_closure_apply_20260430.sql`

## 1. Four disposition-C CFs closed

| CF tag | Closed by | Expected matching rows (mig_190) |
| --- | --- | ---: |
| CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any | mig_156b | 17 |
| CF-mig156-ANY-RECURRENCE- | mig_163b | 13 |
| CF-mig134-PM-LAB-DATE-ANCHOR | mig_160 + mig_160b | 13 |
| CF-mig154-MARGIN-MM-VARCHAR-RETYPE | mig_154 | 12 |

Each UPDATE is idempotent: rows already containing `<tag> CLOSED` are skipped.

## 2. Expected post-state

- All registry rows that previously carried these four CF substrings in `notes` gain an append-only suffix documenting CLOSED-by migration and mig_190 disposition C.
- Verification query §F counts rows whose `notes` contain both ` CLOSED by ` and `per mig_190 disposition C` (expect **55** after first successful apply if every tagged row updated once).

## 3. Manuscript appendix

These four CFs may be cited as **CLOSED** in supplement / data-quality appendix, with trace to mig_156b, mig_163b, mig_160/mig_160b, mig_154 respectively and umbrella rationale mig_190 disposition C.
