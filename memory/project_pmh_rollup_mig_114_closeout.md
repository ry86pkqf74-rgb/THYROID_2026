# canonical_pmh_patient_rollup_v1 Protocol v2 close-out — mig_114

Date: 2026-04-29
Author: Logan Glosser (drafted by Copilot)

## Scope

Closed `main.canonical_pmh_patient_rollup_v1` under Protocol v2 using the rebuild-then-verify pattern.

Why rebuild was required:

- Live rollup had `build_ts = 2026-04-22`.
- Current `canonical_pmh_events_v1` had 12,696 rows / 4,158 patients after mig_107.
- Events had grown by +252 synthetic rows after the stale rollup build:
  - +246 `mig_98*` PMH synthetic rows
  - +6 `mig_103_pmh_synthetic` rows

## Migration

- Migration: `qc_framework_v1/migrations/114_pmh_patient_rollup_signoff.sql`
- Archive snapshot: `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_pmh_patient_rollup_v1_pre_mig114_20260429`
- Rebuild source: Script 365 `_build_rollup_sql_for_domain('pmh')`
- Verification method: `derivation_re_derivation_post_events_repair`
- Batch: `mig_114_pmh_rollup_rebuild_signoff_20260429`

## Validation results

Post-rebuild exact comparison vs fresh Script 365 re-derivation, excluding `build_ts`:

- live-minus-fresh row diff: 0
- fresh-minus-live row diff: 0
- rollup rows: 10,871
- rollup patients: 10,871
- PMH event rows: 12,696
- PMH event patients: 4,158
- archive rows: 10,871
- archive patients: 10,871

Pre/post archive drift was limited to 7 aggregate/date columns:

- `n_findings_any`: 225 patients
- `n_findings_present`: 225 patients
- `n_findings_definitive`: 225 patients
- `n_findings_probable_or_better`: 225 patients
- `n_distinct_findings_norm`: 225 patients
- `first_finding_date`: 177 patients
- `last_finding_date`: 190 patients

No PMH phenotype boolean drifted (`prepost_max_pmh_phenotype_drift = 0`), consistent with the prompt expectation that mig_98/mig_103 synthetic rows increment finding counts/dates but do not map into the 22 PMH phenotype triads.

## Registry final state

`canonical_column_verification_registry_v1`:

- 77 `verified`
- 2 `na`
- 0 `not_started`
- 0 `failed`

`canonical_table_signoff_registry_v1`:

- `table_status = verified`
- `n_columns_total = 79`
- `n_verified = 77`
- `n_na = 2`
- `signoff_migration = qc_framework_v1/migrations/114_pmh_patient_rollup_signoff.sql`

## Reusable pattern

For stale patient rollups after verified event-table growth:

1. Archive the stale rollup to `archive_pub_v1_0`.
2. Rebuild using the original deterministic builder logic, not hand-written derivation variants.
3. Verify post-rebuild with an exact `EXCEPT`/anti-diff against a fresh temp re-derivation excluding provenance timestamps.
4. Compare pre/post archive drift to confirm expected scope.
5. Flip derivable columns only after post-rebuild exact drift is 0.
