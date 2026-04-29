# mig_103 — `canonical_medications_events_v1` close-out

**Date:** 2026-04-29 (UTC)  
**Author:** Logan Glosser <logan.glosser@gmail.com>  
**Database:** `thyroid_canonical_publication_v1_0`

## Scripts

| Script | Purpose |
|--------|---------|
| `qc_framework_v1/scripts/build_medications_review.py` | Classifier + `mig_103_decisions.json` |
| `qc_framework_v1/scripts/apply_mig_103_medications_decisions.py` | Snapshot, PMH insert, DELETE, rollup, registry, provenance |

## Disposition summary (from decisions JSON)

| Bucket | Count |
|--------|------:|
| KEEP | 6,181 |
| KEEP (no token match — trust structured extraction) | 292 |
| DELETE (finding_status=absent) | 442 |
| DELETE (template dominates) | 322 |
| DELETE (negation before mention) | 258 |
| PMH (pre-surgery supplement, dfs < −30d) | 6 |
| REVIEW | 0 |

## MotherDuck mutations

- **Archive:** `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_medications_events_v1_pre_mig103_<ts>`
- **PMH:** +6 rows (`source_table='mig_103_pmh_synthetic'`)
- **Meds events:** 7,501 → **6,473** rows
- **Rollup:** `canonical_medications_patient_rollup_v1` rebuilt (Script 365 step 2, meds only)
- **Registry:** `canonical_medications_events_v1` → `table_status='verified'` (15 verified + 4 na)
- **Provenance:** `manuscript_workspace.cpm_reconciliation_provenance_v1.run_id=mig103_medications_20260429`

## Carry-forwards

- **Note linkage:** Legacy `source_row_id` uses pre-consolidation content hashes; classifier scans `clinical_notes_long` per patient for med-family tokens (not `note_index` join).
- **Re-runs:** Re-applying without restoring from archive would duplicate PMH / miss DELETE targets — always restore from snapshot if retrying.
