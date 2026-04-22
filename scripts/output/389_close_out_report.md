# Script 389 — close-out report

**Date:** 2026-04-22 21:00:22Z
**Script:** `scripts/389_us_zombie_view_rewrites_and_complications_audit.py`
**Rule selected:** `B`
**Pre-state probe:** `scripts/output/389_prestate_probe_report.md`

## Apply summary

| phase | summary |
|---|---|
| 2 (provenance) | classifier reset 2026-04-22 logged via `main.__readme` (already_done; prior rows: 1) — Phases 2A/2B/2C retired |
| 2D | rewrite `canonical_us_exam_master_VIEW_v2`: filter NULL exam_date in source CTEs (CPM CTE retained for `is_preop_exam`); 11,759 rows post (was 11,759); ~0 phantoms eliminated |
| 2E | rewrite `canonical_us_patient_master_VIEW_v2`: has_any_us TRUE=4,360 FALSE=0 (was 100% TRUE / 4,360) |
| 2F | rebuild complications rollup under Rule B; 694 patients flipped TRUE→FALSE at any_evidence tier (2 complication types affected) |
| 2G | re-bound 2 dependent view(s) |
| 2H | PUB object count: 289 -> 290 (Δ +1) |

## Phase 3 — post-state verification

| bucket | pre | post |
|---|---|---|
| `clean_dual_source` | 26,402 | 26,402 |
| `clean_base_only` | 8,919 | 8,919 |
| `needs_backfill` | 2,117 | 2,117 |
| `aggregate_rollup` | 141 | 141 |

* `canonical_us_exam_master_VIEW_v2` rows: 11,759 → 11,759 (phantoms: 0 → 0)
* `canonical_us_patient_master_VIEW_v2` has_any_us TRUE: 4,360 → 4,360
* Rollup invariant: 10,871 rows / 10,871 distinct research_ids (OK)

### Audit case research_id `9340`

#### `rln_injury` post-rebuild
* `ever_rln_injury_any_evidence` = `False`
* `ever_rln_injury_probable_or_better` = `False`
* `ever_rln_injury_definitive` = `False`

#### `hypoparathyroidism` post-rebuild
* `ever_hypoparathyroidism_any_evidence` = `True`
* `ever_hypoparathyroidism_probable_or_better` = `False`
* `ever_hypoparathyroidism_definitive` = `False`

## Per-complication TRUE→FALSE flips (Rule B applied)

| complication_type | any_evidence pre | any_evidence post | Δ (TRUE→FALSE) |
|---|---|---|---|
| `rln_injury` | 709 | 74 | +635 |
| `vocal_cord_paralysis` | 107 | 48 | +59 |
| `hypocalcemia_clinical` | 9 | 9 | +0 |
| `hypoparathyroidism` | 425 | 425 | +0 |
| `hematoma` | 169 | 169 | +0 |
| `seroma` | 873 | 873 | +0 |
| `chyle_leak` | 1,576 | 1,576 | +0 |
| `wound_infection` | 0 | 0 | +0 |
| `pneumothorax` | 0 | 0 | +0 |
| `airway_complication` | 0 | 0 | +0 |
| `wound_dehiscence` | 0 | 0 | +0 |
| `mortality` | 1 | 1 | +0 |

## Carry-forwards (declared, not auto-fixed)

1. **Complications rollup rule choice** — Rule B was selected; the other two rules' deltas remain in the pre-state probe report for reviewer reference.
2. **Upstream complication event de-duplication** — `complication_phenotype_v1` (structured) and `note_entities_complications` (legacy_entity) emit contradictory rows for the same (research_id, complication_type, finding_date) in some cases.  This is a builder-layer issue Script 389 does not address; flag for Script 390+.
3. **US view-stack column compatibility** — the rewritten `canonical_us_exam_master_VIEW_v2` preserves the full original column list (CPM CTE retained, only the `WHERE exam_date IS NOT NULL` guard added to source aggregations); column drops are not expected, but Phase 0D's dependent list should still be reviewed for behavioural changes downstream of the phantom elimination.
4. **Upstream NULL `exam_date` rows in US source tables** — Phase 0C measured ~6,785 NULL-date rows in `canonical_us_thyroid_gland_v2` and ~2,231 in `canonical_us_nodule_v2` (`canonical_us_lymph_node_v2` is clean).  Phase 2D filters them at the view layer, but the upstream data state is worth investigating in 390+: (a) legitimate "date unavailable" that shouldn't propagate, (b) ingestion bug, or (c) intentional pre-LLM backfill placeholders.  No row counts in `main.canonical_us_*_v2` are modified by 389.
5. **Pre-387 flag_event key collapses (7 tables)** — still carry-forward from Script 387; Script 389 does not touch these (separate upstream-builder fix).
6. **CF-7 — Phase 0B classifier reset 2026-04-22** — Script 389's classifier was reset from phantom baselines (18,310 / 17,090 / 2,152 / 27 = `clean_llm_parsed / clean_non_llm / zombie_parent / llm_parsed_but_blob`) to a live-derived source-flag partition (26,402 / 8,919 / 2,117 / 141 = `clean_dual_source / clean_base_only / needs_backfill / aggregate_rollup`).  The original "zombie / blob" concept is retired.  If a content-based multi-nodule-blob audit is still wanted (e.g. rows with `length(location_raw) >= 400 OR semicolons >= 2` that were never split by the v2 nodule splitter — ~750 candidates), draft as a standalone Script 389b after 389 closes.
7. **CF-8 — `needs_backfill` orphan cohort** — 2,117 rows on `canonical_us_nodule_v2` have neither `source_tirads_llm` nor `source_base` parsed: 2,061 with `nlp_backfill_pending=TRUE` (legitimate awaiting-NLP) plus 56 with `nlp_backfill_pending=FALSE` (orphan cohort — why were these ingested without any source flag set?).  The 56-row orphan cohort needs a separate probe in 390+; Script 389 makes no changes to either subset.

## Roll-back

Archive zone is **PUB-resident** (matches actual 387/388 landing pattern; no cross-DB CTAS used).

* `canonical_us_nodule_v2` was NOT modified by Script 389 (zombie phases retired 2026-04-22); no rollback needed for the nodule table.
* Prior complications rollup body: `thyroid_canonical_publication_v1_0.archive_pub_v1_0.canonical_complications_patient_rollup_v1_legacy_20260422` (CTAS back to restore).
* Prior exam_master view body: `thyroid_canonical_publication_v1_0.archive_pub_v1_0.canonical_us_exam_master_VIEW_v2_legacy_20260422_body` (execute the snapshot's SQL via CREATE OR REPLACE VIEW).
* Prior patient_master view body: `thyroid_canonical_publication_v1_0.archive_pub_v1_0.canonical_us_patient_master_VIEW_v2_legacy_20260422_body` (same).
* Classifier-reset provenance row in `main.__readme`: `DELETE FROM main.__readme WHERE content LIKE '%Phase 0B classifier reset 2026-04-22%'`.

