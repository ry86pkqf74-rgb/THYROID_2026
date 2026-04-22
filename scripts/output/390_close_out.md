# Script 390 — ETE Adjudication Reconciliation — Close-Out

**Stamp:** 20260422
**Rule:** Rule A (worst-of, AJCC8 T3b)
**DB:** `thyroid_canonical_publication_v1_0`

## Results

- CPM rows mutated: **1125** (expected 1125)
- Snapshot: `archive_pub_v1_0.cpm_ete_pre390_20260422` (10871 rows)
- Queue final row count: **2788**
- manuscript_cohort_v1 rows updated: **-1**
- 3 __readme provenance rows written

## Phase 3 Verification

### Status: ✅ PASS (all checks)

All post-state invariants satisfied.

## Carry-Forwards

- **CF-1** — Residual queue review: ~2,575 microscopic-no-signal rows need human pathology review.
- **CF-2** — Script 391: T-stage downstream reconciliation — rebuilds microscopic_ete_t3b_corrected and ajcc8_t_stage_* for the ~1,121 rows flipped to gross.
- **CF-3** — Phase 0 drift detective: spec frozen baselines had an arithmetic inconsistency (1,325 ≠ 190+1,091+0 flip-down). Live probe corrected to 1,311/1,121/0/4/1,125. Root cause: earlier-session probe's total_gross_after count was miscomputed (likely stray NULL row in the CASE expression). No live-data drift — the adjudication cohort and boolean-junk counts were byte-identical between the two probes one hour apart.

## Next Steps

Script 391: T-stage downstream reconciliation
  - Rebuilds microscopic_ete_t3b_corrected and ajcc8_t_stage_* columns
  - The ~1,091 rows flipped to gross in 390 should propagate to T3b

Script 392: Boolean-string extractor trace + normalization
  - 194 queued rows from 390 get their upstream fix

## Git Commit + Tag

```
git add scripts/390_ete_adjudication_reconciliation.py
git add scripts/output/390_probe_report.md
git add scripts/output/390_plan_approval.txt
git add scripts/output/390_run.log
git add scripts/output/390_close_out.md

git commit -m "Script 390: ETE adjudication reconciliation — Rule A applied; \
  1125 CPM rows mutated; sticky guard expanded for \
  unable_to_determine; boolean-string cohort routed to queue"

git tag v1_0-ete-reconciled-20260422
```
