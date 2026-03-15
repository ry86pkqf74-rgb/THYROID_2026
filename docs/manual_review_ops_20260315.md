# Manual Review Operations — Unified Queue & Progress Tracking

**Date:** 2026-03-15  
**Script:** `scripts/101_review_ops.py`  
**Target:** MotherDuck `thyroid_research_2026` (prod)

## Purpose

Consolidates 18 heterogeneous review queue tables into a single governed schema
(`unified_review_queue_v1`) with standardised columns, domain-level progress
tracking, and a single-row KPI summary.

## Output Tables

| Table | Rows | Description |
|-------|------|-------------|
| `unified_review_queue_v1` | 18,866 | Unified review queue with 16-column governed schema |
| `review_ops_progress_v1` | 8 | Per-domain progress (total / reviewed / pending / % complete) |
| `review_ops_kpi_v1` | 1 | Single-row KPI: total items, reviewed, pending, overall % |

## Unified Queue Schema (16 columns)

| Column | Type | Description |
|--------|------|-------------|
| `review_item_id` | VARCHAR | MD5 hash of (source_table, research_id, review_reason) |
| `review_domain` | VARCHAR | histology / molecular / rai / episode_linkage / imaging / operative / thyroseq / complications / recurrence / provenance |
| `review_priority` | INTEGER | 1 (critical) – 100 (informational) |
| `research_id` | BIGINT | Patient identifier (NULL for aggregate-level items) |
| `surgery_episode_id` | BIGINT | Episode key (NULL if not episode-specific) |
| `source_table` | VARCHAR | Originating MotherDuck table name |
| `source_artifact_ids` | VARCHAR | Serialised artifact IDs from source table |
| `review_reason` | VARCHAR | Human-readable reason for review |
| `confidence_tier` | VARCHAR | exact_match / high_confidence / plausible / weak / unlinked |
| `recommended_evidence` | VARCHAR | Suggested data source or action for resolution |
| `reviewer_status` | VARCHAR | pending / in_review / resolved / deferred |
| `reviewed_by` | VARCHAR | Reviewer identifier (NULL until reviewed) |
| `reviewed_at` | TIMESTAMP | Review timestamp (NULL until reviewed) |
| `final_resolution` | VARCHAR | Resolution text (NULL until reviewed) |
| `limitation_type` | VARCHAR | source_limited / pipeline_limited / review_needed |
| `created_at` | TIMESTAMP | Row creation timestamp |

## Source Tables Integrated (18)

1. `histology_manual_review_queue_v` — histology discordance
2. `molecular_manual_review_queue_v` — molecular linkage/conflict
3. `rai_manual_review_queue_v` — RAI receipt uncertainty
4. `timeline_manual_review_queue_v` — timeline date conflicts
5. `qa_high_priority_review_v2` — cross-domain QA issues
6. `linkage_ambiguity_review_v1` — multi-candidate linkages
7. `recurrence_manual_review_queue_v1` — recurrence date/status
8. `val_episode_linkage_v2_review_queue` — episode linkage v2
9. `val_multi_surgery_review_queue_v3` — multi-surgery patients
10. `hardening_review_queue` — hardening issues
11. `manuscript_review_queue_v2` — manuscript-blocking issues
12. `thyroseq_review_queue` — ThyroSeq matching conflicts
13. `episode_duplicate_review_v1` — episode duplication
14. `canonical_backfill_ambiguity_review` — backfill linkage
15. `imaging_pathology_concordance_review_v2` — imaging/path mismatch
16. `review_provenance_gaps_v1` — provenance audit gaps
17. `review_surgery_path_discordance_v1` — surgery-path discordance
18. `operative_pathology_reconciliation_review_v2` — operative/path

## Domain Progress Summary

| Domain | Total | Pending | % Complete |
|--------|-------|---------|------------|
| episode_linkage | ~3,600 | ~3,600 | 0% |
| histology | ~2,200 | ~2,200 | 0% |
| recurrence | ~3,000 | ~3,000 | 0% |
| qa_issues | ~2,600 | ~2,600 | 0% |
| imaging | ~1,200 | ~1,200 | 0% |
| molecular | ~800 | ~800 | 0% |
| rai | ~500 | ~500 | 0% |
| other | ~4,900 | ~4,900 | 0% |

> All items currently pending — no reviews have been performed yet.

## Exports

`exports/review_ops_20260315_0742/`

- `unified_review_queue.csv`
- `review_ops_progress.csv`
- `review_ops_kpi.csv`
- `manifest.json`

## Promotion Gate

Gate **G8 (review_ops_freshness)** validates:
- All 3 review ops tables exist
- `unified_review_queue_v1.created_at` is within 30 days

Status: **PASS** (verified 2026-03-15)
