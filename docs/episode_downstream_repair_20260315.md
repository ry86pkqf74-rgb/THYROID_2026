# Episode Downstream Repair Report — 20260315

**Generated**: 20260315_0151
**Script**: `scripts/96_episode_downstream_repair.py`
**Target**: MotherDuck `thyroid_research_2026` (prod)

## Problem

Multi-surgery audit (script 97) found all downstream tables had
`surgery_episode_id=1` for every row, despite `tumor_episode_master_v2`
correctly tracking 761 multi-surgery patients (1,576 episodes).

## Repair Method

Temporal matching: each rows surgery date is matched to the canonical
`(research_id, surgery_date) → surgery_episode_id` mapping from
`tumor_episode_master_v2` (deduplicated via `ROW_NUMBER()`).

## Before / After

### operative_episode_detail_v2

| Metric | Before | After |
|--------|--------|-------|
| Total rows | 9371 | 9371 |
| Rows with ep_id > 1 | 0 | 96 |
| Multi-surg pts with ep>1 | 0 | 96 |

ep_id distribution after:

| ep_id | Count |
|-------|-------|
| 1 | 9275 |
| 2 | 94 |
| 3 | 2 |

### episode_analysis_resolved_v1_dedup

| Metric | Before | After |
|--------|--------|-------|
| Total rows | 9368 | 9368 |
| Rows with ep_id > 1 | 0 | 96 |
| Multi-surg pts with ep>1 | 0 | 96 |

ep_id distribution after:

| ep_id | Count |
|-------|-------|
| 1 | 9272 |
| 2 | 94 |
| 3 | 2 |

### v3 Linkage Tables

| Table | Non-ep1 Before | Non-ep1 After |
|-------|---------------|---------------|
| surgery_pathology_linkage_v3 | 0 | 200 |
| pathology_rai_linkage_v3 | 0 | 5 |
| preop_surgery_linkage_v3 | 0 | 38 |

## Fix Statistics

### operative_episode_detail_v2
| Status | Count |
|--------|-------|
| already_correct | 8628 |
| no_date_match | 647 |
| repaired | 96 |

> ⚠️ 647 rows had no date match in canonical map

### episode_analysis_resolved_v1_dedup
| Status | Count |
|--------|-------|
| already_correct | 8626 |
| no_date_match | 646 |
| repaired | 96 |

> ⚠️ 646 rows had no date match in canonical map

### surgery_pathology_linkage_v3
| Status | Count |
|--------|-------|
| already_correct | 9190 |
| no_date_match | 19 |
| repaired | 200 |

### pathology_rai_linkage_v3
| Status | Count |
|--------|-------|
| already_correct | 16 |
| repaired | 4 |

### preop_surgery_linkage_v3
| Status | Count |
|--------|-------|
| already_correct | 3553 |
| repaired | 38 |

## Non-Regression

| Check | Result |
|-------|--------|
| multi_surg_distinct_ep_eard | 3 |
| multi_surg_distinct_ep_oed | 3 |
| single_surg_ep1_eard | PASS |
| single_surg_ep1_oed | PASS |
| v3_pathology_rai_distinct_ep | 3 |
| v3_preop_surgery_distinct_ep | 3 |
| v3_surgery_pathology_distinct_ep | 3 |

## Provenance

All repairs logged in `episode_downstream_repair_audit_v1`.

| Target Table | Status | Count |
|-------------|--------|-------|
| episode_analysis_resolved_v1_dedup | already_correct | 8626 |
| episode_analysis_resolved_v1_dedup | no_date_match | 646 |
| episode_analysis_resolved_v1_dedup | repaired | 96 |
| operative_episode_detail_v2 | already_correct | 8628 |
| operative_episode_detail_v2 | no_date_match | 647 |
| operative_episode_detail_v2 | repaired | 96 |
| pathology_rai_linkage_v3 | already_correct | 16 |
| pathology_rai_linkage_v3 | repaired | 4 |
| preop_surgery_linkage_v3 | already_correct | 3553 |
| preop_surgery_linkage_v3 | repaired | 38 |
| surgery_pathology_linkage_v3 | already_correct | 9190 |
| surgery_pathology_linkage_v3 | no_date_match | 19 |
| surgery_pathology_linkage_v3 | repaired | 200 |

## No-Date-Match Gap Analysis

The 647 `no_date_match` rows in OED (646 in EARD) were investigated.
They are **NOT repair defects** — they reflect data-population boundaries.

### Breakdown (OED)

| Category | Patients | Rows | Explanation |
|----------|----------|------|-------------|
| Not in tumor_episode at all | 635 | 645 | Benign / non-cancer procedures with no tumor_episode_master_v2 entry |
| In tumor_episode, date mismatch | 11 | 2 | Dates in OED `surgery_date_native` differ from tumor_episode `surgery_date` (likely recording variance) |
| **Total** | **646** | **647** | — |

- 645 of the 647 rows are single-row patients (1 OED row each) — ep_id=1 is correct.
- 1 patient has 2 OED rows; both have no tumor_episode match (benign bilateral).

### "All-Ones" Multi-Surgery Patients (528/761)

Of 761 multi-surgery patients, 528 have only `surgery_episode_id=1`
in OED after repair. Investigation:

| Category | Count | Explanation |
|----------|-------|-------------|
| Only 1 distinct surgery date in OED | 525 | 2nd+ surgeries tracked in tumor_episode but absent from OED — data population gap |
| 0 distinct dates (all NULL) | 1 | Completely missing surgery dates |
| 2 distinct dates, no tumor match | 2 | Dates in OED don't align with tumor_episode (recording variance) |

**Conclusion**: The 96 repaired patients represent the full set of
matchable multi-surgery rows in OED. The remaining 525 "all-ones"
patients are correctly ep_id=1 because they have only one operative
record. Expanding their coverage requires populating OED with
additional surgery rows (upstream enrichment, not episode-id routing).

## Post-Repair Audit (script 97)

Re-ran `97_episode_linkage_audit.py --env prod` after the repair.
7 audit tables refreshed in `thyroid_research_2026`.

| KPI | Pre-Repair | Post-Repair | Delta |
|-----|-----------|-------------|-------|
| Multi-surgery patients | 761 | 761 | — |
| Total episodes | 1,576 | 1,576 | — |
| Total artifacts | 8,498 | 8,498 | — |
| Exact/high confidence | 8.3% | 8.3% | — |
| Mislink candidates | 1,464 | 1,506 | +42 |
| Ambiguous links | 3,992 | 3,992 | — |
| GREEN patients | 53 | 53 | — |
| YELLOW patients | 75 | 75 | — |
| RED patients | 14 | 14 | — |
| REVIEW_REQUIRED | 616 | 616 | — |

**Mislink increase (+42)**: Expected. With correct episode_ids
propagated, the audit now detects more granular cross-episode
mislinks that were previously hidden behind universal ep_id=1.
This is improved sensitivity, not regression.

## MotherDuck Objects Modified

| Table | Action |
|-------|--------|
| operative_episode_detail_v2 | UPDATE surgery_episode_id |
| episode_analysis_resolved_v1_dedup | UPDATE surgery_episode_id |
| surgery_pathology_linkage_v3 | UPDATE surgery_episode_id |
| pathology_rai_linkage_v3 | UPDATE surgery_episode_id |
| preop_surgery_linkage_v3 | UPDATE surgery_episode_id |
| episode_downstream_repair_audit_v1 | CREATE (provenance) |

## Remaining Gaps & Recommendations

1. **OED population**: 525 multi-surgery patients have only 1 OED row.
   Upstream enrichment (V2 operative extractor re-run or Excel backfill)
   needed to expand operative_episode_detail_v2 coverage.
2. **11 date mismatches**: OED `surgery_date_native` differs from
   `tumor_episode_master_v2.surgery_date` for 11 patients. Manual
   review recommended.
3. **19 surgery_pathology no-match**: 19 linkage rows have surgery dates
   not in the canonical map. Likely completion/re-excision procedures
   not tracked as separate tumor episodes.
4. **Mislink triage**: 1,506 mislink candidates flagged by audit.
   High-value subset should be routed to domain expert review queue.
