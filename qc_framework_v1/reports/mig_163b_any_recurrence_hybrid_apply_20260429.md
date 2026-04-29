# mig_163b ANY-RECURRENCE HYBRID apply closeout (2026-04-29)

## Scope

Applied the Logan-ratified HYBRID definition for `main.canonical_patient_master.any_recurrence_flag` in `thyroid_canonical_publication_v1_0`:

```text
canonical_recurrence_v1.recurrence_confirmed = TRUE
OR canonical_recurrence_resolved_v1.recurrence_status_final = 'path_proven'
```

This closes `CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT`.

## Execution artifact

- SQL applied: `qc_framework_v1/migrations/163b_any_recurrence_hybrid_apply_20260429.sql`
- Execution log: `exports/mig163b_any_recurrence_hybrid_apply_logs/run.log`
- Connection path: `scripts/_md_connect.py::connect_locked()` with search path locked to `thyroid_canonical_publication_v1_0.main`

## Preflight evidence

| Check | Result |
|---|---:|
| HYBRID strict_n | 514 |
| HYBRID path_proven_n | 145 |
| HYBRID union_n | 514 |
| path_proven_added_by_hybrid | 0 |
| PM vs HYBRID: both | 165 |
| PM vs HYBRID: PM-only dropped | 219 |
| PM vs HYBRID: HYBRID-only added | 349 |
| PM vs HYBRID: neither | 10,138 |
| CPM rows / distinct `research_id` | 10,871 / 10,871 |
| Pre-state `any_recurrence_flag` TRUE / FALSE / NULL | 384 / 10,487 / 0 |
| Pre-state registry notes with `mig_163b` | 0 |

## Post-apply verification

| Check | Result |
|---|---:|
| PM vs HYBRID: both | 514 |
| PM vs HYBRID: PM true / HYBRID false | 0 |
| PM vs HYBRID: PM false / HYBRID true | 0 |
| PM vs HYBRID: neither | 10,357 |
| CPM rows / distinct `research_id` | 10,871 / 10,871 |
| Post-state `any_recurrence_flag` TRUE / FALSE / NULL | 514 / 10,357 / 0 |
| Registry rows / notes with `mig_163b` | 1 / 1 |
| Archive snapshots present | 2 / 2 |

## Archive snapshots

Two pre-mutation snapshots were created in `"Thyroid 2026 UPdated".archive_pub_v1_0`:

- `canonical_patient_master_any_recurrence_flag_pre_mig163b_20260429`
- `canonical_column_verification_registry_any_recurrence_flag_pre_mig163b_20260429`

## Status

PASS. `canonical_patient_master.any_recurrence_flag` now has zero mismatches vs the HYBRID recurrence definition, and CPM row/distinct-`research_id` invariants remain intact.