# mig_253 Surgical Procedure Type Fill

Date: 2026-05-01
Migration: `qc_framework_v1/migrations/253_surg_procedure_type_fill_20260501.sql`
Dry-run: `exports/mig253_surg_proc_type_dryrun_20260501T103539Z/`

## Scope

Filled `main.canonical_patient_master` surgical procedure fields only for rows where all three fields were NULL:

- `surg_procedure_type`
- `surg_total_thyroidectomy`
- `surg_hemithyroidectomy`

No other CPM rows were touched.

## Dry-run Summary

The signed dry-run used session-scoped TEMP tables only and produced:

| Metric | Before | Simulated after |
| --- | ---: | ---: |
| CPM all-three NULL surgical fields | 2,138 | 2 |
| M038 >=200g NULL procedure type | 121 | 0 |
| Type/flag consistency defects | n/a | 0 |

Proposed fills:

| Procedure type | Patients |
| --- | ---: |
| total_thyroidectomy | 1,438 |
| hemithyroidectomy | 623 |
| other | 68 |
| isthmusectomy | 7 |
| residual NULL | 2 |

## Source Precedence

1. `canonical_operative_events_v1.procedure_normalized` / `procedure_raw`
2. `canonical_patient_master.nsqip_cpt_code` / `nsqip_cpt_description`
3. `canonical_operative_procedure_codes_v1`
4. `path_synoptics.thyroid_procedure` / `procedure_other_description`
5. `note_entities_operative_detail`

## Post-apply Verification

All acceptance checks passed after applying mig_253:

| Check | Result |
| --- | --- |
| CPM rows / NULL procedure / all-three NULL | `10871 / 2 / 2` |
| Type/flag consistency defects | `0 / 0 / 0` |
| M038 >=200g distribution | total thyroidectomy 400; hemithyroidectomy 74; other 1; NULL 0 |
| Publication QC gate | `(218, 0, 0, 0, 0, cohort_parity_ok=True)` |
| Table signoff registry | `canonical_patient_master` verified; `signoff_migration` = mig_253 |
| CPM provenance | `mig_253_surg_procedure_type_fill_20260501` inserted |

Residual unresolved rows are exported to `manuscript_outputs/audit/mig253_residual_surg_proc_type_review.csv` for CF-SURG-RESIDUAL-CHART-REVIEW.

## Carry-forwards

- CF-SURG-RESIDUAL-CHART-REVIEW: 2 residual CPM rows with no procedure-type source after all mapped sources.
- CF-SURG-CPT-VOCAB-REGISTRY: canonicalize CPT/procedure vocabulary into a registry table for future refreshes.