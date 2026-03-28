# MotherDuck Full Export Report

**Date:** 2026-03-27
**Database:** thyroid_research_2026
**Export Location:** ~/Desktop/Thyroid_Export_20260327/

## Summary

| Metric | Value |
|---|---|
| Tables exported | 592 |
| Views exported | 67 |
| **Total objects** | **658** |
| Total rows | 4,678,536 |
| Export size | 165 MB (zstd compressed) |
| Schema DDL | schema.sql (2,147 lines) |
| Errors (stale views) | 21 |

## Export Structure

```
~/Desktop/Thyroid_Export_20260327/
  tables/          592 Parquet files (141 MB)
  views/            67 Parquet files (24 MB)
  schema.sql        Full DDL for all tables and views
  export_manifest.json  Complete audit trail with row counts
```

## Key Table Verification (Local Parquet Spot-Check)

| Table | Rows | Distinct research_ids |
|---|---|---|
| master_cohort (view) | 11,673 | 11,673 |
| molecular_testing | 10,126 | 10,026 |
| clinical_notes | 10,863 | 10,863 |
| synoptic_pathology | 11,688 | 10,871 |
| thyroglobulin_labs | 30,245 | 2,569 |
| fna_episode_master_v2 | 59,620 | 5,263 |
| tumor_episode_master_v2 | 11,691 | 10,871 |
| operative_episode_detail_v2 | 9,371 | 9,368 |
| survival_cohort | 6,359 | 3,048 |

## Errors (21 Stale Views)

All 21 errors are views with schema drift — their column definitions no longer match the underlying tables. These views were already broken in MotherDuck and cannot be queried. The DDL for these views is preserved in schema.sql for reference.

Affected views: analysis_episode_v1, analysis_lesion_v1, analysis_patient_v1, date_recovery_summary, date_rescue_rate_summary, enriched_master_timeline, enriched_note_entities_* (6 views), missing_date_associations_audit, patient_reconciliation_summary_v, patient_validation_rollup_mv, timeline_rescue_mv, timeline_rescue_v2_mv, timeline_unresolved_summary_mv, timeline_unresolved_summary_v2_mv, validation_failures_mv, validation_failures_v2.

## Excluded Objects

- **Rosflow tables/views** (6 tables, 7 views): These belong to the rosflow schema, not the thyroid research database
- **MotherDuck system views** (8): database_snapshots, databases, owned_shares, query_history, recent_queries, shared_with_me, storage_info, storage_info_history

## Next Steps

1. Copy export folder to encrypted local drive
2. In MotherDuck dashboard: drop the database and cancel subscription
3. Revoke any tokens/shares
