# Pipeline Gap Closure Sprint — 2026-03-14

**Script**: `scripts/94_pipeline_gap_closure.py`  
**Date**: 2026-03-14  
**Status**: COMPLETE  

---

## Executive Summary

Four targeted workstreams executed against live MotherDuck (`thyroid_research_2026`).
Only pipeline-limited and process-limited gaps addressed; source-limited gaps documented
as permanent limitations.

| Workstream | Domain | Before | After | Δ |
|---|---|---|---|---|
| A — Operative NLP | `parathyroid_autograft_flag` in op table | 40 | 40 | 0 (source-limited — 32 NLP pts have no op row) |
| A — Operative NLP | `val_operative_coverage_v2` table | absent | **created** | ✅ |
| B — Imaging Linkage | `fna_episode_master_v2.linked_imaging_nodule_id` | **0** | **1,454** | +1,454 |
| B — Imaging Linkage | `imaging_fna_linkage_summary_v1` table | absent | **created** | ✅ |
| C — Recurrence Queue | `recurrence_review_queue_v1` table | absent | **1,986 rows** | ✅ |
| C — Recurrence Queue | `recurrence_review_yield_tracker_v1` tracker | absent | **created** | ✅ |
| D — Adjudication KPI | `adjudication_progress_kpi_v1` table | absent | **created** | ✅ |
| D — Adjudication KPI | Starter-pack CSV exports | absent | **3 files, 1,972 rows** | ✅ |

---

## Workstream A: Operative NLP Coverage

### Goal
Supplement `operative_episode_detail_v2.parathyroid_autograft_flag` with
`note_entities_procedures` NLP extractions of `parathyroid_autotransplant`.

### Finding
- NLP table has **48 patients** with `parathyroid_autotransplant` present-polarity mentions
- Of those, **16 are already flagged** TRUE in `operative_episode_detail_v2`
- The remaining **32 have NO row** in `operative_episode_detail_v2` at all (not merely unflagged)
- Column type is native `BOOLEAN` — not text `'true'`/`'false'`; UPDATE was correctly targeted

### Conclusion
**Source-limited, not a pipeline bug.** The 32 patients had autotransplant documented in
clinical notes but lack an `operative_episode_detail_v2` row — they either had an operative
record outside the structured `operative_details` table or their procedure was not captured
in the V2 extraction pipeline. No fabrication of data performed.

### Deliverables
- `val_operative_coverage_v2` table (MotherDuck): field-level coverage counts with
  episode-level, patient-level, and NLP-supplement breakdowns
- Patient-level op_* aggregates in `patient_analysis_resolved_v1` and
  `manuscript_cohort_v1` refreshed with proper `LOWER(CAST(...))` boolean handling

---

## Workstream B: Imaging → FNA Linkage Backfill

### Goal
Populate `fna_episode_master_v2.linked_imaging_nodule_id` (was 0) from
`imaging_fna_linkage_v3`; correct stale documentation claiming 0-row linkage table.

### Finding
- `imaging_fna_linkage_v3` had **9,024 rows** (prior docs incorrectly stated 0 rows)
- Linkage tiers (score_rank=1): high_confidence=646, plausible=3,883, weak=3,395, unlinked=3
- Backfill threshold: **score >= 0.45** (plausible+) using composite key `(research_id, fna_episode_id)`

### Result
**`fna_episode_master_v2.linked_imaging_nodule_id`: 0 → 1,454** (16.2% of 8,978 unique FNA episodes)  
High-confidence only (score ≥ 0.65): 646

### Source Limitation Confirmed
- `imaging_nodule_long_v2` (10,866 rows): ALL feature columns = 0 (size_cm_max, composition, shape)
- This table is V2 NLP-populated and the extractor outputs were **never materialized** to MotherDuck
- Use `imaging_nodule_master_v1` (19,891 rows, 3,439 patients) as the canonical imaging source

### Deliverables
- `fna_episode_master_v2.linked_imaging_nodule_id` backfilled: **0 → 1,454**
- `imaging_fna_linkage_summary_v1` table (tier/score-band distribution)
- Documentation correction: `imaging_fna_linkage_v3` = 9,024 rows (not 0)

---

## Workstream C: Recurrence Manual Review Queue

### Goal
Build a prioritized, exportable review queue for all 1,764 unresolved recurrence dates;
track review yield over time.

### Recurrence Date Tier Distribution (unchanged at source)

| Tier | N | Notes |
|---|---|---|
| `not_applicable` | 8,885 | No recurrence; excluded from queue |
| `unresolved_date` | **1,764** | Primary target for chart review |
| `biochemical_inflection_inferred` | 168 | Tg-based inference — usable for TTE |
| `exact_source_date` | 54 | Gold standard — already dated |

### Priority Queue Breakdown

| Priority | N | Description |
|---|---|---|
| 1 (resolved) | 222 | Already exact or biochemical — no review needed |
| **2 HIGH** | **224** | Multiple sources, high confidence, just missing chart date |
| 3 MEDIUM | 791 | Multi-source, unresolved — moderate chart review effort |
| 4 LOW | 749 | Structured flag only — chart review needed to confirm |

**Most efficient review target**: Priority 2 (n=224) — likely to recover dates with minimal effort per case.

### Deliverables
- `recurrence_review_queue_v1` MotherDuck table (1,986 rows): research_id, priority,
  Tg values, RAI status, scan findings, surgery date, histology, stage, BRAF
- `recurrence_date_tier_summary_v1` table
- `recurrence_review_yield_tracker_v1` table (sprint baseline: 0 completed)
- `exports/pipeline_gap_closure_20260314_HHMM/recurrence_review_queue_unresolved.csv`
  (1,764 unresolved cases with full context — **primary chart-review artifact**)

---

## Workstream D: Adjudication Progress Instrumentation

### Goal
Verify adjudication tables, build progress KPI tracker, export starter packs for
high-priority cases.

### Status
- `adjudication_decisions`: **0 rows** — no reviewer decisions yet entered
- `histology_manual_review_queue_v`: **7,552 cases** queued
- `streamlit_patient_manual_review_v`: **7,552 items** across domains
- `streamlit_patient_conflicts_v`: **1,015 patients** with multi-domain conflicts

### Schema Note
`adjudication_decisions` uses `review_domain` (not `domain`); columns confirmed:
`decision_id`, `research_id`, `review_domain`, `conflict_type`, `reviewer_action`,
`reviewer_resolution_status`, `final_value_selected`, `active_flag`.

### Deliverables
- `adjudication_progress_kpi_v1` table: queue vs decisions tracking per domain
- `exports/.../adjudication_histology_starter_pack.csv` (500 rows by priority_score)
- `exports/.../adjudication_conflicts_starter_pack.csv` (1,000 rows by review_priority)
- `exports/.../adjudication_discordance_cases.csv` (500 multi-domain discordances)

---

## New MotherDuck Tables Created

| Table | Rows | Purpose |
|---|---|---|
| `val_operative_coverage_v2` | 2 | Field-level operative NLP coverage |
| `imaging_fna_linkage_summary_v1` | 4 | Imaging→FNA tier distribution |
| `recurrence_review_queue_v1` | 1,986 | Prioritized chart-review queue |
| `recurrence_date_tier_summary_v1` | 4 | Date-tier counts with yield flags |
| `recurrence_review_yield_tracker_v1` | 1 | Sprint baseline counter |
| `adjudication_progress_kpi_v1` | 3 | Queue vs decisions KPI |

---

## Source-Limited Gaps (No Fix Possible Without New Institutional Data)

| Gap | Root Cause |
|---|---|
| `imaging_nodule_long_v2` feature columns all zero | V2 extractor outputs never materialized to MotherDuck |
| `operative_episode_detail_v2` 32 NLP-only parathyroid patients | No operative_details row for these patients |
| Recurrence `unresolved_date` = 88.8% of recurrence flags | `recurrence_risk_features_mv` stores boolean only, no day-level date |
| RAI dose missingness ~59% | Nuclear medicine reports absent from clinical_notes_long corpus |
| `adjudication_decisions = 0` | No reviewer has entered decisions yet (process gap, not data gap) |

---

## Export Artifacts

```
exports/pipeline_gap_closure_20260314_2325/
  before_after_metrics.csv                    (34 metric rows)
  recurrence_review_queue_unresolved.csv      (1764 cases, 380 KB)
  adjudication_histology_starter_pack.csv     (500 rows, 80 KB)
  adjudication_conflicts_starter_pack.csv     (1000 rows, 83 KB)
  adjudication_discordance_cases.csv          (500 rows, 12 KB)
  manifest.json
```
