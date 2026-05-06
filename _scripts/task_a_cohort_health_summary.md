# Task A — Cohort Health View Summary

**Task:** Build `pub_signoff.manuscript_cohort_health_v1` + QC assertion  
**Date:** 2026-05-06  
**Migration:** `mig_078_manuscript_cohort_health_view_and_assertion.sql`  
**DFL:** DFL-20260506-TA1 (Airtable base appJYOnUb7KrHKwpV, table tblsiYKJtKcktkzze)

---

## What was built

Three objects were created as part of migration mig_078:

1. **`pub_signoff.cohort_view_stats_v1`** — Pre-computed row counts for all 70 `cohort_m*` tables in `pub_workspace`. Populated via a BQ scripting FOR loop (EXECUTE IMMEDIATE per table) to avoid full-scan COUNT(*) at view query time. Total: 70 tables, 423,591 combined rows.

2. **`pub_signoff.manuscript_cohort_health_v1`** (VIEW) — Registry joining `pub_workspace.manuscript_dive_map_v1` (cohort_view_name) × `pub_workspace.manuscript_feasibility_v1` (status, feasibility_color) × INFORMATION_SCHEMA existence checks across both `pub_workspace` and `pub_legacy_source_20260416` × `cohort_view_stats_v1` for row counts. Columns: manuscript_id, manuscript_title, cohort_view_name, cohort_view_exists (BOOL), cohort_view_row_count (INT64), feasibility_color, status, is_active (TRUE when status IN Ready to Submit / In Progress / Proposed).

3. **QC assertion `manuscript_active_cohort_view_must_exist`** (severity: warn) — check_sql returns rows from the health view where `is_active = TRUE AND cohort_view_exists = FALSE`. Per the qc_runner semantics, zero rows = PASS. Inserted into `pub_signoff.qc_assertions_v1`.

## Verification results

Active manuscript coverage (19 active manuscripts, all `cohort_view_exists=TRUE`):

| Status | Count | All have cohort view? |
|---|---|---|
| Ready to Submit | 2 (M025, M038) | Yes — M025: 3,375 rows; M038: 10,871 rows |
| In Progress | 12 (M028–M042) | Yes — sizes range 273 to 10,871 |
| Proposed | 5 (M043–M047) | Yes — sizes range 1,165 to 10,871 |

QC run `qc_20260506T082029Z_73d176`: **18/18 assertions PASS, 0 error-severity failures.**  
New assertion: `manuscript_active_cohort_view_must_exist` — PASS, violation_count=0.

## Governance trail

- DFL-20260506-TA1 logged in Airtable **before** any changes
- bq_migration_log_v1 row inserted after successful deployment
- Migration file committed to `bq_migrations/mig_078_manuscript_cohort_health_view_and_assertion.sql`
- PHI guardrail: view and QC assertion return counts and metadata only, never research_ids
