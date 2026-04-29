# mig_173b — syn_*_size_cm 3-axis decomposition apply

**Date:** 2026-04-29
**Source artifact:** `qc_framework_v1/migrations/173_syn_size_cm_dtype_reform_20260429.sql`
**Target:** `thyroid_canonical_publication_v1_0`
**Execution:** Applied after `git pull --ff-only origin main` (repo already up to date).

## Execution log

- First attempt used an explicit transaction and was rolled back because MotherDuck disallows one transaction writing to both `thyroid_canonical_publication_v1_0` and `Thyroid 2026 UPdated`.
- Re-ran without an explicit multi-database transaction; SQL completed successfully.
- Logs: `exports/mig173b_syn_size_apply_logs/run.log`, `exports/mig173b_syn_size_apply_logs/run_no_txn.log`.

## CPM invariant

| metric | value |
|---|---:|
| rows | 10871 |
| distinct research_id | 10871 |
| null cpm_built_at | 0 |

## Archive snapshots

| source_col | rows | distinct research_id |
|---|---:|---:|
| right | 10871 | 10871 |
| left | 10871 | 10871 |
| isthmus | 10871 | 10871 |

## Affected CPM columns present after apply

| column_name | data_type |
|---|---|
| syn_isthmus_size_cm_legacy_raw | VARCHAR |
| syn_left_lobe_size_cm_legacy_raw | VARCHAR |
| syn_right_lobe_size_cm_legacy_raw | VARCHAR |
| syn_right_lobe_length_cm | DOUBLE |
| syn_right_lobe_width_cm | DOUBLE |
| syn_right_lobe_height_cm | DOUBLE |
| syn_right_lobe_volume_cc | DOUBLE |
| syn_right_lobe_size_parse_status | VARCHAR |
| syn_left_lobe_length_cm | DOUBLE |
| syn_left_lobe_width_cm | DOUBLE |
| syn_left_lobe_height_cm | DOUBLE |
| syn_left_lobe_volume_cc | DOUBLE |
| syn_left_lobe_size_parse_status | VARCHAR |
| syn_isthmus_length_cm | DOUBLE |
| syn_isthmus_width_cm | DOUBLE |
| syn_isthmus_height_cm | DOUBLE |
| syn_isthmus_volume_cc | DOUBLE |
| syn_isthmus_size_parse_status | VARCHAR |

## Parse-status distribution

| source_col | parse_status | rows |
|---|---|---:|
| isthmus | NULL | 6890 |
| isthmus | parsed_3axis | 3679 |
| isthmus | parsed_partial | 107 |
| isthmus | sentinel | 2 |
| isthmus | unparsed | 193 |
| left | NULL | 3667 |
| left | parsed_3axis | 6916 |
| left | parsed_partial | 11 |
| left | sentinel | 33 |
| left | unparsed | 244 |
| right | NULL | 3813 |
| right | parsed_3axis | 6787 |
| right | parsed_partial | 8 |
| right | sentinel | 39 |
| right | unparsed | 224 |

## Coverage among non-sentinel non-null parse attempts

| source_col | parsed_any | parsed_3axis | unparsed | attempted | pct_3axis |
|---|---:|---:|---:|---:|---:|
| isthmus | 3786 | 3679 | 193 | 3979 | 92.46 |
| left | 6927 | 6916 | 244 | 7171 | 96.44 |
| right | 6795 | 6787 | 224 | 7019 | 96.69 |

## Volume sanity

| source_col | min_volume_cc | max_volume_cc | n_negative | n_ge_1000 |
|---|---:|---:|---:|---:|
| isthmus | 0.0 | 341.25 | 0 | 0 |
| left | 0.0 | 1836.75 | 0 | 15 |
| right | 0.0 | 2311.3199999999997 | 0 | 14 |

The `n_ge_1000` rows were reviewed spot-wise after apply. They are driven by very large but parse-consistent raw strings such as `17.0 x 13.2 x 10.3`, `14.8 x 13.5 x 9.3`, and `15.8 x 15.5 x 7.5`, not by regex axis mis-capture. They remain a clinical plausibility review caveat because the migration's conservative guard requested explicit review for rectangular volumes >=1000 cc.

## Registry/provenance

| check | value |
|---|---:|
| affected column registry rows | 18 |
| CPM provenance rows for mig_173 | 1 |

## Acceptance summary

**Status:** PASS WITH REVIEW CAVEAT

Core migration checks passed: CPM row invariants hold, archive snapshots were created, legacy raw VARCHAR fields were preserved via `_legacy_raw` renames, 15 typed/status columns are present, parse coverage exceeds the 85% target for all three fields, registry rows total 18/18, and CPM provenance has one mig_173 row. The only caveat is the expected volume plausibility review queue for 29 large-lobe rectangular volumes >=1000 cc.

