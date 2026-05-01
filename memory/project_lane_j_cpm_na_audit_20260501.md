# Project memory — Lane J CPM 24-NA column audit (mig_235)

Date: 2026-05-01

## Scope

Lane J audited the 24 `verification_status='na'` rows for `main.canonical_patient_master` in `main.canonical_column_verification_registry_v1`.

## Key decisions

- Added durable registry column `na_rationale VARCHAR` because the registry did not have an explicit rationale field before mig_235.
- Pre-snapshotted registry to `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig235_20260501` before DDL/UPDATE.
- Classified 23/24 columns as `na_genuine` and retained them as NA with explicit rationales.
- Reclassified `pmhx_nlp_family_hx_thyroid` from `na` to `verified` because it exactly matches the companion rule among non-null rows:
  - TRUE=31, FALSE=259, NULL=10,581
  - `pmhx_nlp_family_hx_thyroid_n_mentions > 0` identifies 31 patients
  - 0 mismatches among non-null flag rows
- No `na_failed_in_disguise` columns were found; no Logan ratification required for failed reclassification.

## Post-apply expected counts

- CPM column registry: `verified=1607`, `na=23`, `failed=0`, `not_started=0`, total `1630`.
- CPM table signoff remains `table_status='verified'` with updated `signoff_migration='qc_framework_v1/migrations/235_cpm_na_col_audit_20260501.sql'`.
- Provenance run id: `mig_235_cpm_na_audit_v15`.

## Files

- SQL: `qc_framework_v1/migrations/235_cpm_na_col_audit_20260501.sql`
- Report: `qc_framework_v1/reports/cpm_24_na_audit_20260501.md`
- Memory: `memory/project_lane_j_cpm_na_audit_20260501.md`
