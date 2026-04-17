# Script 270c — Phase B planning summary
Generated: 2026-04-17T06:46:38.421027+00:00  
Tag anchor: `v1_0_registry_locked` (commit 117f55d)  
Mode: **dry-run only — no destructive writes**
## Restore test
- Status: **PASS**
- Snapshot: `"Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_patient_master_prev233_snapshot_20260417T010115Z"`
- Round-tripped 10871 rows / 7 cols cleanly
## Budgets
- `archive_candidates_count_canonical_main`: ok=True — {'actual': 0, 'limit': 250, 'ok': True}
- `drop_candidates_count_stray_archive`: ok=True — {'actual': 39, 'limit': 250, 'ok': True}
- `total_rows_in_archive_candidates`: ok=True — {'actual': 0, 'limit': 50000000, 'ok': True}
- `single_archive_candidate_over_threshold`: ok=True — {'threshold': 10000000, 'tables_over_threshold': [], 'ok': True, 'ok_severity': 'none'}

## Canonical main — base-table dispositions
- `KEEP_REGISTRY_FEEDER`: 78
- `KEEP_PENDING_V1_1_DECISION`: 33
- `KEEP_KEEP_LIST`: 2
- `KEEP_SPINE`: 1

## Canonical main — view dispositions
- `KEEP_VIEW`: 13

Views compile-impact rows: **0**

## Stray archive-DB schemas
- `MIGRATE_TO_ARCHIVE_LEGACY`: 243
- `DROP_NO_RESTORE_VALUE`: 39

## Recommended execution order for 270d
1. Archive view DDL for VIEW_COMPILE_WILL_BREAK rows (write to `archive_pub_v1_0.view_ddl__<view>_pre270d_<UTC>`).
2. Drop those views.
3. For each ARCHIVE_CANDIDATE base table, snapshot to `archive_pub_v1_0.<table>_pre270d_<UTC>` then DROP TABLE.
4. For stray-schema DROP_ALREADY_SNAPSHOTTED + DROP_NO_RESTORE_VALUE rows, DROP without snapshot.
5. For stray-schema MIGRATE_TO_ARCHIVE_LEGACY rows, CREATE TABLE under `archive_legacy.<schema>__<name>_<UTC>` then DROP from stray schema.
6. Final audit row to `v1_1_finalization_audit_v1`.

## Counts to report back
- archive_candidates: **0**
- stray_drops: **39**
- view_impacts: **0**
- restore_test: **PASS** (10871 rows round-tripped)
