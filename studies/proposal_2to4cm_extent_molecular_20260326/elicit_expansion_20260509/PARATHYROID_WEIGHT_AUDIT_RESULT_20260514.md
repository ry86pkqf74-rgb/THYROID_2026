# Parathyroid weight — synoptic source-data audit

**Date:** 2026-05-14
**Source:** `All Diagnoses & synoptic 12_1_2025.xlsx` (uploaded by Logan)
**Goal:** Confirm whether parathyroid weight is missing because (a) we never parsed it, or (b) it was never recorded in the institutional source data.

## Headline finding: parathyroid weight is **NOT recorded** in the synoptic source

The synoptic sheet contains **11 parathyroid-related columns** but **none of them is a weight column**:

| Col | Column name |
|---:|---|
| 25 | parathyroid operation |
| 91 | # parathyroid glands |
| 92 | Location of parathyroid glands |
| 93 | Parathyroid gland findings |
| 231 | Parathyroid Gland &/oR tissue included in resected specimen? |
| 235 | paraG 1 parathyroidectomy (excisional) |
| 242 | paraG 2 parathyroidectomy (excisional) |
| 249 | paraG 3 parathyroidectomy (excisional) |
| 256 | paraG_4 parathyroidectomy (excisional) |
| 263 | paraG 5 parathyroidectomy (excisional) |
| 270 | paraG 6 parathyroidectomy (excisional) |

## Coverage numbers

- **10,871** distinct research_ids in the synoptic file
- **548** patients have any parathyroid mention in these columns
- **0** patients have a weight value in proximity to the parathyroid mention in synoptic text (regex scan over `(weight|wt|weighed|weighing)\s*\d+\s*(mg|gm|gram|g)` within 200 chars of "parath")
- The mig_326 extraction (16 surgical patients with `parathyroid_weight_mg`) drew from **operative-note narrative text** parsed into `canonical_parathyroid_events_v1.evidence_quote`/`reasoning`/`parathyroid_pathology` — not from this synoptic sheet.

## Recommended action: mark as "no_parathyroid_weight_in_synoptics_record" and stop

The 548 patients with parathyroid mentions but no synoptic weight are **confirmed-missing-by-design**. The institution does not capture parathyroid weight as a synoptic field. The only path to more parathyroid weight values is operative-note narrative extraction (already done at mig_326, yielded n=16) or a future PDF/path-report deep-extraction pass.

Audit CSV at `parathyroid_weight_audit_20260514.csv` (research_id + flag only, no PHI):

| flag | n |
|---|---:|
| `has_parathyroid_weight` (weight near parath mention in synoptic) | 0 |
| `no_parathyroid_weight_in_synoptics_record` (parath mention but no weight) | 548 |
| `no_parathyroid_mention` (no parathyroid mention in synoptic at all) | 10,323 |

## Implications for M084 manuscript

The M084 parathyroid manuscript already exists with v10/v11 (n=125 cohort). The `parathyroid_weight_mg` covariate from mig_326 (n=16 surgical) is the most that can be extracted from current sources. Options:

1. **Accept n=16** — descriptive only; not enough for inferential modeling. Cite as a structural data-availability limitation.
2. **Chart-review the remaining 548 with parathyroid mentions** — manual pull from PDF path reports. ~1-2 person-days of work; might add 50-150 weights.
3. **Add a synoptic column going forward** — institutional process change; doesn't help retrospective data.

Recommend option 1 for the current M084 submission, with option 2 deferred to a v12 manuscript if a reviewer requests parathyroid-weight stratification.

## Audit-trail

- Source file: `All Diagnoses & synoptic 12_1_2025.xlsx` (PHI-containing; NOT committed to git)
- Output: `parathyroid_weight_audit_20260514.csv` (research_id + flag only, PHI-safe)
- Inspect scripts (`_xlsx_inspect.py`, `_xlsx_parathyroid_audit.py`) **deleted after run** to avoid persisting PHI-handling code in the public-facing repo. Re-runnable from the upload + this writeup if needed.
- Linked to: M084 manuscript (`recx6Jr6WFtF2hZxb`) — pending MFL row.
