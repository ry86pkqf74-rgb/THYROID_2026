# mig_240 Closeout — vw_us_exam_safe_VIEW_v1

**Date:** 2026-05-01  
**Agent:** Cline (Sonnet 4.6)  
**Batch:** mig_240_us_exam_safe  
**Migration file:** `qc_framework_v1/migrations/240_vw_us_exam_safe_VIEW_v1_20260501.sql`

---

## What was done

Created `semantic_publication.vw_us_exam_safe_VIEW_v1` — the exam-level US safe view
that was previously missing from the semantic_publication layer.

**Source:** `main.canonical_us_exam_master_VIEW_v2` (11,880 rows)  
**Columns:** 25 total — `release_id` (CROSS JOIN from release_manifest_v1) + `research_id` (CAST BIGINT→VARCHAR per mig_239 convention) + 23 exam-level attributes  
**Column categories:** identifier (3), date (1), clinical_measure (14), clinical_flag (6), metadata (2), data_quality (1)

## Verification results

| Check | Result |
|---|---|
| Row count | 11,880 ✅ |
| gate1 (total QC-tracked views) | 212 ✅ (was 211) |
| gate2 (unverified columns) | 0 ✅ |
| gate3 | 0 ✅ |
| gate4 | 0 ✅ |
| gate5 | 0 ✅ |
| cohort_parity_ok | TRUE ✅ |
| Sample row (rid 628, 2016-03-21) | release_id + research_id::VARCHAR correct ✅ |

## Registries updated

- `main.canonical_column_verification_registry_v1` — 25 rows inserted  
- `main.canonical_table_signoff_registry_v1` — 1 row inserted (25/25 verified)

## Notes

- `last_signoff_migration` in QC status still shows `mig_239_...` — this is expected (that column reflects the last time a specific signoff_migration was written to the signoff registry key that the QC view watches; gate1 count did increment correctly to 212).
- `worst_tirads_category_this_exam = None` for sample row is expected — sparse TIRADS data for that exam date.
