# Master verified views validation

**Script:** `scripts/125_master_verified_views.py --md --md-sa`

## Row counts (2026-04-07)

| View | Rows |
|------|-----:|
| `main.master_fact_long_verified_v1` | 123,577 |
| `main.master_patient_rollup_verified_v1` | 5,574 |
| `main.master_source_lineage_v1` | 123,577 |

## Traceability fields

Per view DDL in `125_master_verified_views.py`, analyst-facing facts include:

- `research_id`
- `source_domain` (from `fact_domain`)
- `source_object_id` (from `note_row_id`)
- `extraction_run_id` — **COALESCE** of row-level `canonical_extracted_fact_long_v2.extraction_run_id` with fallback join to `note_extraction_runs` by `extracted_at`
- `reviewer_status` (from `qa.manual_review_queue` via `review_lookup`, latest per patient/domain)
- `release_tag` from latest `qa.release_manifest`

## Note on review join

`review_lookup` partitions `qa.manual_review_queue` by `(research_id, domain)`; ensure `domain` in QA matches `fact_domain` (use `source_domain` when hydrating from gate CSV — see `114_qa_schema_setup.py`).
