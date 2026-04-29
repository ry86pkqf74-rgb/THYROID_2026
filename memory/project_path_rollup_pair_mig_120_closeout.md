# Path malignant/benign patient rollup Protocol v2 close-out — mig_120

Date: 2026-04-29
Author: Logan Glosser (drafted by Copilot)

## Scope

Closed both path-family patient rollups under Protocol v2:

- `main.canonical_path_malignant_patient_rollup_v1`
- `main.canonical_path_benign_patient_rollup_v1`

Verified source event tables were already closed:

- `main.canonical_path_malignant_events_v1` — mig_89, 56/56 verified
- `main.canonical_path_benign_events_v1` — mig_97b, 51 verified + 4 `na`

## Migration

- Migration: `qc_framework_v1/migrations/120_path_rollup_pair_signoff.sql`
- Batch: `mig_120_path_rollup_pair_signoff_20260429`
- Method: stale-rollup rebuild plus derivation re-derivation against verified events

Pre-rebuild staleness probe:

| Table family | Events max build_ts | Rollup max build_ts | Disposition |
|---|---|---|---|
| malignant | 2026-04-22 00:00:00 | 2026-04-21 00:00:00-04 | stale → rebuild |
| benign | 2026-04-25 00:00:00 | 2026-04-21 00:00:00-04 | stale → rebuild |

**Live MotherDuck read-only confirmation (2026-04-29):** both rollups show `table_status='verified'` in `canonical_table_signoff_registry_v1`; each rollup `MAX(build_ts)` is after its source events `MAX(build_ts)`; `COUNT(*) WHERE any_concomitant_malignant` on benign rollup = `COUNT(DISTINCT …)` from `canonical_path_benign_events_v1` ∩ `canonical_path_malignant_events_v1` = 4,137.

Pre-rebuild snapshots were archived to:

- `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_patient_rollup_v1_pre_mig120_20260429`
- `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_benign_patient_rollup_v1_pre_mig120_20260429`

## Rebuild details

Malignant rollup:

- Rebuilt from `canonical_path_malignant_events_v1`.
- `highest_stage_ajcc7` / `highest_stage_ajcc8` use explicit AJCC severity ranking.
- `dominant_histology` uses deterministic count-descending, lexicographic tie-breaker.
- `bethesda_final`, `bethesda_final_name`, `regex_path_outcome`, and `poc_tumor_1_histologic_type` were replayed from the archived Script-361 path-outcome source:
  `"Thyroid 2026 UPdated".archive_pub_v1_0.path_outcome_classification_v1_pre361_20260422_002245`.

Benign rollup:

- Rebuilt from `canonical_path_benign_events_v1` plus the full `canonical_patient_master` cohort spine.
- `any_concomitant_malignant` now derives from the explicit `benign_events ∩ malignant_events` patient join.
- Bethesda/path-outcome fields were replayed from the same archived Script-361 path-outcome source.

## Validation results

Post-rebuild per-column drift checks against fresh derivations:

| Rollup | Target cols | Drift |
|---|---:|---:|
| canonical_path_malignant_patient_rollup_v1 | 14 | 0 |
| canonical_path_benign_patient_rollup_v1 | 13 | 0 |

Patient and cross-table sanity:

- Malignant rollup rows/patients: 4,137 / 4,137, matching distinct malignant-event patients.
- Benign rollup rows/patients: 10,871 / 10,871, matching `canonical_patient_master`.
- `any_benign_event = TRUE`: 8,846 patients.
- `any_concomitant_malignant = TRUE`: 4,137 patients, matching explicit benign∩malignant event join.
- Bethesda union across malignant+benign rollups: 5,249 patients, matching archived Script-361 path-outcome source.

## Registry final state

`canonical_column_verification_registry_v1`:

- `canonical_path_malignant_patient_rollup_v1`: 14 `verified` + 3 `na`
- `canonical_path_benign_patient_rollup_v1`: 13 `verified` + 3 `na`

`canonical_table_signoff_registry_v1`:

- Both tables: `table_status = verified`
- `signoff_migration = qc_framework_v1/migrations/120_path_rollup_pair_signoff.sql`

## Carry-forwards

- `CF-mig120-PATH-MALIG-DATE-RETYPE`: `earliest_malignant_path_date` and `latest_malignant_path_date` remain `TIMESTAMP` calendar-only fields to preserve the existing rollup schema. They are derived from `canonical_path_malignant_events_v1.surgery_date`; future clinical-date retyping can convert them to `DATE` with the broader CF-100 date-retype family.
- `build_ts` was retyped to plain `TIMESTAMP` in both rebuilt rollups.

## Path family status

Path family is closed for these rollups: malignant events (mig_89), benign events (mig_97b), gland rollup (mig_101), and malignant/benign patient rollups (mig_120).

## Rollup verification chain note

These are the **fifth and sixth** canonical patient rollups closed under Protocol v2 using the same derivation re-derivation (+ stale full rebuild when rollup `build_ts` lags verified events) pattern established on earlier rollups (e.g. PMH mig_114, complications mig_108, parathyroid mig_106).
