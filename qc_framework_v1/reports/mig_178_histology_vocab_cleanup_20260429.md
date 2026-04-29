# mig_178 — histology vocabulary cleanup + cross-table uniformity audit

**Date:** 2026-04-29 20:58:18Z
**Run ID:** `mig_178_histology_vocab_cleanup_20260429`
**Posture:** applied to MotherDuck publication DB with archive snapshots and audit tables.

## Decision summary

- Read the mig_168 histology ratification notes.
- Applied Logan's updated decision to reject a synthetic `mtc_ptc_mixed` code.
- Mixed PTC/MTC patients are represented by separate canonical labels, usually `MTC | PTC`, derived from path malignant events.
- Raw strings such as `MTC\nPTC mixed composit` and `metastatic PTC?` were normalized away in the canonical histology surfaces.

## Archive snapshots

- CPM: `"Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_patient_master_histology_pre_mig178_20260429_205813"`
- Path malignant events: `"Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_path_malignant_events_v1_pre_mig178_20260429_205813"`
- Path malignant rollup: `"Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_path_malignant_patient_rollup_v1_pre_mig178_20260429_205813"`

## Execution metrics

| Metric | Value |
|---|---:|
| PM exact `PTC | MTC` / `MTC | PTC` patients before first pass | 32 |
| PM any PTC+MTC/medullary patients before | 38 |
| Event-derived PTC+MTC patients before | 38 |
| Bad raw-pattern values before first pass | 1452 |
| Path malignant event rows changed in first pass | 1831 |
| CPM rows changed in first pass | 1849 |
| Path malignant rollup rows changed in first pass | 302 |
| Additional path malignant event rows changed in residual cleanup pass | 228 |
| Event-derived PTC+MTC patients after | 38 |
| Bad raw-pattern values after | 0 |
| Uniformity failure values after | 0 |

## Audit table summary

| metric                         |   value |
|:-------------------------------|--------:|
| mtc_ptc_mixed_values_remaining |       0 |
| ptc_mtc_patient_audit_rows     |      38 |
| uniformity_fail_values         |       0 |
| uniformity_pass_values         |     297 |

## PTC/MTC patient audit

The full 38-patient audit is materialized in `manuscript_workspace.mig178_ptc_mtc_patient_audit_v1`. All rows have `pm_event_status = 'pm_matches_events'` after cleanup.

Named spot checks:

| research_id | cleaned PM histologic_types_all | cleaned PM variant list | rollup dominant | rollup tumor-1 histology |
|---:|---|---|---|---|
| 2168 | MTC \| PTC | microcarcinoma | MTC | PTC |
| 3331 | MTC \| PTC | NULL | MTC | MTC |

## Remaining uniformity failures

No remaining uniformity failures.

## Tables created

- `manuscript_workspace.mig178_ptc_mtc_patient_audit_v1`
- `manuscript_workspace.mig178_histology_vocab_uniformity_audit_v1`
- `manuscript_workspace.mig178_histology_cleanup_summary_v1`

## mig_172 unblock note

The multi-label/free-text histology lane is now mechanically separated from the four enum-column mig_172 apply scope. `mtc_ptc_mixed` is not present as a canonical value in the cleaned cross-table audit surface.
