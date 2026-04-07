# Live MotherDuck audit (read-only)

- **UTC timestamp:** 2026-04-07T22:08:41Z
- **Connection mode:** `read_write` (preflight credential class: `read_write`)
- **User agent:** `THYROID_2026_live_audit`
- **Session hint:** `live_publication_validation_20260407`

## Token preflight (names and lengths only)

- `MD_READ_SCALING_TOKEN`: **MISSING** (length=0)
- `MOTHERDUCK_READ_SCALING_TOKEN`: **MISSING** (length=0)

_No read-scaling token in env; checking read/write env vars (then secrets file)._
- `MD_SA_TOKEN`: **MISSING** (length=0)
- `MOTHERDUCK_TOKEN`: **MISSING** (length=0)
- `motherduck_token`: **MISSING** (length=0)

**Effective read/write token:** **SET** (length=467), source `secrets.toml:MOTHERDUCK_TOKEN`.
**Selected credential:** read/write (`MotherDuckClient.connect_rw`, SELECT-only here).

## Environment
- `MOTHERDUCK_ENV`: `prod`
- Resolved database name (config): `Thyroid 2026`
- `current_database()` at connect: `Thyroid 2026`

## MotherDuck Business features exercised (this run)
- Read-only **SELECT** only; no DDL/DML; no `ATTACH 'md:'` workspace mode.
- md_information_schema.databases (8 rows)
- MD snapshots metadata (database_snapshots, 186 rows)
- Query history / query_history (count=120475)
- **Read/write** token path via `MotherDuckClient.connect_rw()` (queries were SELECT-only).

## Key metrics

| metric_id | value | detail |
|-----------|-------|--------|
| current_database | Thyroid 2026 | MOTHERDUCK_ENV=prod |
| planned_database_for_env | Thyroid 2026 | prod |
| n_schemas_total | 20 |  |
| release_schemas_count | 9 | release_20260406,release_20260407,release_20260407_final,release_20260407_final2,release_20260407_tier,release_20260408,release_20260409,release_20260410,release_20260411 |
| accessible:md_information_schema.databases | yes | row_count=8 |
| accessible:database_snapshots | yes | row_count=186 |
| accessible:query_history | yes | row_count=120475 |
| mrq_total_rows | 11244 |  |
| mrq_status:auto_accepted_standard | 6162 |  |
| mrq_status:auto_accepted_critical_sample_ok | 3292 |  |
| mrq_status:auto_accepted_informational | 1786 |  |
| mrq_status:confirmed_correct | 4 |  |
| mrq_synthetic_or_automation_status_rows | 0 |  |
| mrq_auto_accepted_prefix_rows | 11240 |  |
| mrq_confirmed_correct_rows | 4 |  |
| mrq_verification_status_null_rows | 0 |  |
| promotion_review_decisions_total | 3 |  |
| promotion_decision_batch_id_nonnull | 3 |  |
| promotion_decision_batch_id_null | 0 |  |
| promotion_batch:legacy_rc_tier_20260407 | 2 |  |
| promotion_batch:20260407_tier_policy | 1 |  |
| lab_wave:wave_tgab_structured_ehr | 39005 |  |
| lab_wave:wave_tg_structured_ehr | 37966 |  |
| lab_wave:final_institutional_20260407 | 989 |  |
| longitudinal_lab_rows_final_institutional_wave | 989 |  |
| val_specimen_contract_fail_rows | 0 |  |
| val_specimen_genomic_binding_fail_rows | 0 |  |
| v_diag_broken_fhir_refs | 0 |  |
| v_diag_review_burden_rows | 1 |  |
| release_manifest_row_0 | 20260411 | created_at=2026-04-07 19:15:39.106720, git_sha=de13c33 |
| release_manifest_row_1 | 20260410 | created_at=2026-04-07 16:22:53.465299, git_sha=618086b |
| release_manifest_row_2 | 20260407_tier | created_at=2026-04-07 15:25:17.363482, git_sha=7793059 |
| release_manifest_row_3 | 20260407_final2 | created_at=2026-04-07 05:11:41.171561, git_sha=4ad9052 |
| release_manifest_row_4 | 20260407_final | created_at=2026-04-07 05:08:12.328508, git_sha=4ad9052 |

## Publication readiness
- **Verdict:** **HOLD**
- **Live blockers (exact):**
  - Governance MRQ: 11240/11244 rows are auto_accepted*; only 4 confirmed_correct — README requires human-reviewed manuscript sign-off beyond automation-only acceptance (remaining 11240 rows).