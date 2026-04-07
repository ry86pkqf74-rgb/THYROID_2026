# Master verified views — live validation

**Connection:** MotherDuck **Thyroid 2026**, fail-closed (no local fallback).  
**Scripts:** Definitions in `scripts/125_master_verified_views.py`.

## Row counts

| Object | Rows |
|--------|-----:|
| `main.master_fact_long_verified_v1` | 123,577 |
| `main.master_patient_rollup_verified_v1` | 5,574 |
| `main.master_source_lineage_v1` | 123,577 |

Matches `main.canonical_extracted_fact_long_v2` count (123,577) for long/lineage views.

## Traceability column completeness (`master_fact_long_verified_v1`)

| Column | Null or empty rows | Notes |
|--------|-------------------:|-------|
| `research_id` | 0 | OK |
| `source_domain` | 0 | OK |
| `source_object_id` (alias of `note_row_id`) | 0 | OK |
| `extraction_run_id` | **24,421** | **Gap** — see `unresolved_blockers.md` |
| `reviewer_status` | 97,787 | Expected sparsity: `LEFT JOIN` to latest MRQ row per `(research_id, domain)`; no MRQ row → NULL |
| `release_tag` | 0 | Scalar from `latest_release` CTE; all rows = `20260406` at audit time |

### MRQ join sanity check

Query: count facts with `reviewer_status IS NULL` but an existing `qa.manual_review_queue` row for the same `(research_id, domain as source_domain)`.

**Result: 0** — NULL `reviewer_status` is consistent with absence of queue coverage for that pair, not a broken join.

## `reviewer_status` distribution (top values)

| reviewer_status | n (facts) |
|-----------------|----------:|
| NULL | 97,787 |
| auto_accepted_critical_sample_ok | 9,415 |
| auto_accepted_standard | 7,245 |
| SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF | 6,810 |
| auto_accepted_informational | 2,303 |
| confirmed_correct | 17 |

## `release_tag` on master views

All 123,577 rows carry `release_tag = 20260406` (from latest manifest row by `created_at`). See `release_manifest_summary.md` for tag-vs-schema reconciliation.
