# Script 270c — Phase B planning summary
Generated: 2026-04-17T07:26:38.644897+00:00  
Tag anchor: `v1_0_registry_locked` (commit 117f55d)  
Mode: **dry-run only — no destructive writes**
## Restore test
- Status: **PASS**
- Snapshot: `"Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_patient_master_prev233_snapshot_20260417T010115Z"`
- Round-tripped 10871 rows / 7 cols cleanly
## Budgets
- `archive_candidates_count_canonical_main`: ok=True — {'actual': 0, 'limit': 250, 'ok': True}
- `drop_candidates_count_stray_archive`: ok=True — {'actual': 161, 'limit': 250, 'ok': True}
- `total_rows_in_archive_candidates`: ok=True — {'actual': 0, 'limit': 50000000, 'ok': True}
- `single_archive_candidate_over_threshold`: ok=True — {'threshold': 10000000, 'tables_over_threshold': [], 'ok': True, 'ok_severity': 'none'}
- `stray_divergent_rows_human_review`: ok=False — {'actual': 3, 'limit': 0, 'ok': False, 'ok_severity': 'human_review_required', 'rows': [{'schema': 'main', 'name': 'canonical_diagnosis_unified_v1', 'row_count': 11259, 'justification': 'snapshot-suffix match in archive_pub_v1_0 but row counts differ: stray=11259 vs [canonical_diagnosis_unified_v1_pre251_20260417T012311Z(rc=11028)]; halt for human review (270d refuses to migrate DIVERGENT rows)'}, {'schema': 'main', 'name': 'ln_master_rollup_v1', 'row_count': 4290, 'justification': 'snapshot-suffix match in archive_pub_v1_0 but row counts differ: stray=4290 vs [ln_master_rollup_v1_pre251_20260417T012311Z(rc=4273)]; halt for human review (270d refuses to migrate DIVERGENT rows)'}, {'schema': 'main', 'name': 'serial_imaging_us', 'row_count': 0, 'justification': 'snapshot-suffix match in archive_pub_v1_0 but row counts differ: stray=0 vs [serial_imaging_us_pre251_20260417T012311Z(rc=4162)]; halt for human review (270d refuses to migrate DIVERGENT rows)'}]}

## Canonical main — base-table dispositions
- `KEEP_REGISTRY_FEEDER`: 78
- `KEEP_PENDING_V1_1_DECISION`: 33
- `KEEP_KEEP_LIST`: 2
- `KEEP_SPINE`: 1

## Canonical main — view dispositions
- `KEEP_VIEW`: 13

Views compile-impact rows: **0**

## Stray archive-DB schemas
- `DROP_ALREADY_SNAPSHOTTED`: 123
- `MIGRATE_TO_ARCHIVE_LEGACY`: 118
- `DROP_NO_RESTORE_VALUE`: 38
- `DIVERGENT`: 3

## Recommended execution order for 270d
1. Archive view DDL for VIEW_COMPILE_WILL_BREAK rows (write to `archive_pub_v1_0.view_ddl__<view>_pre270d_<UTC>`).
2. Drop those views.
3. For each ARCHIVE_CANDIDATE base table, snapshot to `archive_pub_v1_0.<table>_pre270d_<UTC>` then DROP TABLE.
4. For stray-schema DROP_ALREADY_SNAPSHOTTED + DROP_NO_RESTORE_VALUE rows, DROP without snapshot.
5. For stray-schema MIGRATE_TO_ARCHIVE_LEGACY rows, CREATE TABLE under `archive_legacy.<schema>__<name>_<UTC>` then DROP from stray schema.
6. Final audit row to `v1_1_finalization_audit_v1`.

## Counts to report back
- archive_candidates: **0**
- stray_drops: **161** (already_snapshotted=123, no_restore_value=38)
- stray_migrate: **118**
- stray_DIVERGENT (halt for review): **3**
- view_impacts: **0**
- restore_test: **PASS** (10871 rows round-tripped)

## Post-plan patch: 3 DIVERGENT rows reclassified

After direct MotherDuck verification, 3 rows initially tagged `DIVERGENT` were reclassified to `DROP_ALREADY_SNAPSHOTTED`:
- `canonical_diagnosis_unified_v1` (+231 stray vs snapshot): pre-dedup same-source duplicates; canonical == snapshot.
- `ln_master_rollup_v1` (+17 stray vs snapshot): same pattern.
- `serial_imaging_us` (stray=0 vs snapshot=4162): empty shell; snapshot is authoritative.

**Updated DROP_ALREADY_SNAPSHOTTED: 123 → 126. DIVERGENT: 3 → 0.**
See audit row `divergent_reclassified_to_drop_3_rows` and tech_debt `stray_subset_matcher_v1_1`.
