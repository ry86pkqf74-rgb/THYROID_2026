# mig_181 — PM syn_*_size 15 cols verify + apply

**Date:** 2026-04-29
**Target DB:** `thyroid_canonical_publication_v1_0`
**Migration:** `qc_framework_v1/migrations/181_pm_syn_size_cols_verify_apply_20260429.sql`
**Execution log:** `exports/mig181_pm_syn_size_cols_apply_20260429/run_summary.json`
**Posture:** registry/signoff/provenance only; no `canonical_patient_master` value mutation.

## Mission result

The 15 typed/status synoptic lobe-size columns added by mig_173b were verified against the preserved `*_legacy_raw` parse pipeline and flipped from `not_started` to `verified`.

| metric | pre | post |
|---|---:|---:|
| PM verified columns | 1,575 | 1,590 |
| PM not_started columns | 16 | 1 |
| PM na columns | 24 | 24 |
| PM failed columns | 0 | 0 |
| CPM rows | 10,871 | 10,871 |
| CPM distinct research_id | 10,871 | 10,871 |
| CPM null cpm_built_at | 0 | 0 |

The 3 `*_legacy_raw` columns remain `na` under the mig_173 batch, as intended.

## Parse-status distribution

| lobe | NULL | parsed_3axis | parsed_partial | sentinel | unparsed |
|---|---:|---:|---:|---:|---:|
| right | 3,813 | 6,787 | 8 | 39 | 224 |
| left | 3,667 | 6,916 | 11 | 33 | 244 |
| isthmus | 6,890 | 3,679 | 107 | 2 | 193 |

## Multi-valued parse-status sweep

`parse_status` is a multi-valued audit enum, not a Type-A/Type-B placeholder. The dominant parsed state is clinically expected but not a boolean truth flag.

| lobe | dominant non-null value | dominant n | pct of non-null | conclusion |
|---|---|---:|---:|---|
| right | parsed_3axis | 6,787 | 96.16% | multi-valued enum; verified with caveat |
| left | parsed_3axis | 6,916 | 96.00% | multi-valued enum; verified with caveat |
| isthmus | parsed_3axis | 3,679 | 92.41% | multi-valued enum; verified |

Although right/left `parsed_3axis` exceeds 95% of non-null statuses, the columns have 4 non-null enum states (`parsed_3axis`, `parsed_partial`, `sentinel`, `unparsed`) and are therefore not Type-A near-uniform booleans.

## Derivative-column semantic checks

| lobe | parsed_any | parsed_3axis | length non-null | width non-null | height non-null | volume non-null | non-positive axis | negative volume | volume formula mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| right | 6,795 | 6,787 | 6,795 | 6,794 | 6,787 | 6,787 | 1 | 0 | 0 |
| left | 6,927 | 6,916 | 6,927 | 6,925 | 6,916 | 6,916 | 1 | 0 | 0 |
| isthmus | 3,786 | 3,679 | 3,786 | 3,763 | 3,679 | 3,679 | 1 | 0 | 0 |

**Volume formula:** PASS. For every row with all 3 axes and volume populated, `volume_cc = length_cm * width_cm * height_cm` exactly within tolerance.

**Zero-axis caveat:** 3 rows have a literal zero axis in the source raw string and therefore volume `0`. These were retained as source-faithful parser outputs, not silently imputed. Carry-forward tag: `CF-mig181-SYN-SIZE-ZERO-AXIS-EDGECASE`.

| lobe | research_id | legacy_raw | parsed axes | volume_cc |
|---|---:|---|---|---:|
| right | 11821 | `.0 cm superior to inferior by 4.0 cm transverse by 3.0 cm anterior to posterior` | 0.0 × 4.0 × 3.0 | 0 |
| left | 11448 | `4.5 x 3.8 x 0.` | 4.5 × 3.8 × 0.0 | 0 |
| isthmus | 6789 | `1.9 x 1.2 x 0.` | 1.9 × 1.2 × 0.0 | 0 |

## Deterministic spot-checks

### Right lobe

| research_id | legacy_raw | length_cm | width_cm | height_cm | volume_cc | status |
|---:|---|---:|---:|---:|---:|---|
| 4578 | `7.3 x 4.7 x 2.8` | 7.3 | 4.7 | 2.8 | 96.068 | parsed_3axis |
| 9639 | `5.7 x 3 x 2.3` | 5.7 | 3.0 | 2.3 | 39.33 | parsed_3axis |
| 11737 | `3.7 cm superior to inferior by 2.0 cm transverse by 1.0 cm anterior to posterior` | 3.7 | 2.0 | 1.0 | 7.4 | parsed_3axis |
| 9156 | `5.6 x 2.6 x 2.2` | 5.6 | 2.6 | 2.2 | 32.032 | parsed_3axis |
| 7030 | `6.2 x 4.7 x 3.6` | 6.2 | 4.7 | 3.6 | 104.904 | parsed_3axis |

### Left lobe

| research_id | legacy_raw | length_cm | width_cm | height_cm | volume_cc | status |
|---:|---|---:|---:|---:|---:|---|
| 7492 | `6.4 x 3.8 x 2.8` | 6.4 | 3.8 | 2.8 | 68.096 | parsed_3axis |
| 10081 | `4.1 x 2.3 x 2.1` | 4.1 | 2.3 | 2.1 | 19.803 | parsed_3axis |
| 3988 | `4.5 cm / 4 cm / 2 cm` | 4.5 | 4.0 | 2.0 | 36.0 | parsed_3axis |
| 4537 | `4.7 x 1.8 x 1.8` | 4.7 | 1.8 | 1.8 | 15.228 | parsed_3axis |
| 5323 | `3.5 cm / 2.5 cm / 1.5 cm` | 3.5 | 2.5 | 1.5 | 13.125 | parsed_3axis |

### Isthmus

| research_id | legacy_raw | length_cm | width_cm | height_cm | volume_cc | status |
|---:|---|---:|---:|---:|---:|---|
| 8991 | `1.6 x 1 x 0.7` | 1.6 | 1.0 | 0.7 | 1.12 | parsed_3axis |
| 2996 | `3.0 x 2.0 x 2.0` | 3.0 | 2.0 | 2.0 | 12.0 | parsed_3axis |
| 1504 | `2.0 x 2.5 x 0.5` | 2.0 | 2.5 | 0.5 | 2.5 | parsed_3axis |
| 4045 | `2.7 cm / 1.4 cm / 0.4 cm` | 2.7 | 1.4 | 0.4 | 1.512 | parsed_3axis |
| 6723 | `2.7 cm superior inferior, 0.8 cm medial lateral, 0.7 cm anterior posterior` | 2.7 | 0.8 | 0.7 | 1.512 | parsed_3axis |

## Apply details

- A pre-snapshot of all 18 mig_173 registry rows was materialized at `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig181_20260429`.
- The 15 typed/status columns were stamped with:
  - `verified_by = 'Logan Glosser <logan.glosser@gmail.com>'`
  - `batch_id = 'mig_181_pm_syn_size_cols_verify_apply_20260429'`
  - `verification_method = 'derivation_vs_syn_size_legacy_raw_parse_pipeline'`
- The table signoff registry was resynced to `n_verified=1590`, `n_not_started=1`.
- A CPM reconciliation provenance row was inserted for `canonical_cleanup_mig181_pm_syn_size_cols_verify_apply_20260429`.

## Acceptance summary

**Status:** PASS WITH SOURCE-VALUE CAVEAT.

mig_181 achieved the requested registry closeout: PM `not_started` decreased from 16 to 1, and the scoped 15 synoptic size columns are now verified. The only caveat is the 3 source-faithful zero-axis rows documented above; no parser formula mismatch or CPM invariant violation was observed.
