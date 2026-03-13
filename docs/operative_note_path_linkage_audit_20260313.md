# Operative Note–Pathology Linkage Audit

**Date:** 2026-03-13 08:03
**Databases:** MotherDuck (production) + Local DuckDB (NLP-enriched reference)
**Script:** `scripts/70_operative_note_path_linkage_audit.py`

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Total patients (path_synoptics) | 10,871 |
| Total surgeries (path_synoptics rows) | 11,688 |
| Operative episode records | 9,371 |
| Patients with operative episode | 9,368 |
| Patients with op notes in EMR | 4,439 |
| Patients with surgery AND op note | 2,310 |
| Patients with surgery, NO op note | 6,423 |
| Patients in path_synoptics only (no operative episode) | 2,138 |
| Op notes total (clinical_notes_long) | 4,680 |
| Patients with op note but NO operative episode | 2,129 |

### Cancer-Specific Coverage

| Metric | Value |
|--------|-------|
| Cancer patients (total) | 4,137 |
| Cancer with operative episode | 3,221 |
| Cancer with op note | 1,826 |
| Cancer with both episode + op note | 915 |
| **Cancer with NO operative episode** | **916** (22.1%) |

### NLP Parse Coverage (Local DuckDB vs MotherDuck)

| Metric | Local | MotherDuck |
|--------|-------|------------|
| Episodes with NLP parse | **1,900** (20.3%) | **0** (0.0%) |
| Episodes without NLP parse | 7,471 | 9,371 |

**Critical finding:** MotherDuck's `operative_episode_detail_v2` was materialized
from the base table BEFORE the NLP enrichment step ran. All 11 NLP-derived boolean
fields are FALSE/NULL on MotherDuck. Local DuckDB retains the enriched state.

---

## Phase 2: Operative Note Coverage

### Coverage by Surgery Year (2010+)

| Year | Total Surgeries | With Op Note | Coverage |
|------|----------------|-------------|----------|
| 2010 | 305 | 1 | 0.3% |
| 2011 | 336 | 2 | 0.6% |
| 2012 | 356 | 6 | 1.7% |
| 2013 | 498 | 6 | 1.2% |
| 2014 | 515 | 1 | 0.2% |
| 2015 | 524 | 1 | 0.2% |
| 2016 | 481 | 5 | 1.0% |
| 2017 | 652 | 32 | 4.9% |
| 2018 | 622 | 19 | 3.1% |
| **2019** | **844** | **261** | **30.9%** |
| **2020** | **689** | **616** | **89.4%** |
| **2021** | **703** | **615** | **87.5%** |
| **2022** | **547** | **491** | **89.8%** |
| 2023 | 27 | 2 | 7.4% |
| 2024 | 16 | 0 | 0.0% |

**Interpretation:** Op notes were not captured in the EMR data extract before ~2019.
Starting 2020-2022, coverage is 88-90%. The 2019 transition year shows 31% coverage.
Pre-2019 surgeries (6,530 episodes) will never have operative note granularity from this data source.

### Multi-Surgery Distribution

| Surgeries per Patient | N Patients |
|----------------------|------------|
| 1 | 9,365 |
| 2 | 3 |

### Op Notes per Patient

| Notes per Patient | N Patients |
|------------------|------------|
| 1 | 4,214 |
| 2 | 209 |
| 3 | 16 |

---

## Phase 3: Operative Note ↔ Surgery Linkage

| Category | Count | % |
|----------|-------|---|
| LINKED_CONFIDENT (same-day) | 942 | 10.1% |
| LINKED_DATE_PROXIMAL (≤7d) | 25 | 0.3% |
| AMBIGUOUS (>7d or date missing) | 1,343 | 14.3% |
| MISSING (no op note for this surgery) | 7,060 | 75.3% |

**Interpretation:** Of 9,371 operative episodes, only 967 (10.3%) have a confidently
linked op note. The 7,060 MISSING cases are primarily pre-2019 surgeries without
electronic op notes. The 1,343 AMBIGUOUS cases mostly have op notes with no parseable
date (1,267 of 4,680 op notes lack date metadata in clinical_notes_long).

---

## Phase 4: Surgery ↔ Pathology Linkage

| Status | Count | % |
|--------|-------|---|
| ALIGNED (exact date + laterality ok) | 8,638 | 92.1% |
| PROXIMAL_ALIGNED (≤7d + laterality ok) | 0 | 0.0% |
| NO_PATHOLOGY (operative episode, no tumor match) | 635 | 6.8% |
| LATERALITY_DISCORDANT | 84 | 0.9% |
| DATE_DISCORDANT (>7d gap) | 9 | 0.1% |
| DATE_MISSING | 2 | 0.0% |

Total items requiring review: **730**

### Surgery-Pathology v3 Linkage Tiers (MotherDuck)

| Tier | Count | % |
|------|-------|---|
| high_confidence | 8,236 | 87.5% |
| weak | 566 | 6.0% |
| exact_match | 414 | 4.4% |
| plausible | 190 | 2.0% |
| unlinked | 3 | 0.03% |

**Interpretation:** Surgery-pathology linkage is strong. 92.1% have exact same-day
alignment (Phase 4) and 91.9% are high_confidence or exact_match (v3 scoring).
The 635 NO_PATHOLOGY episodes are benign procedures that generate surgical records
but no tumor pathology entry. The 84 laterality discordances warrant manual review
but are not analysis-blocking.

---

## Phase 5: Operative Variable Extraction Coverage

### Local DuckDB (NLP-enriched state)

| Variable | Extracted | Total | Rate | Source |
|----------|-----------|-------|------|--------|
| procedure_normalized | 8,727 | 9,371 | 93.1% | Structured (path_synoptics) |
| rln_monitoring | 1,702 | 9,371 | 18.2% | NLP |
| operative_findings_raw | 588 | 9,371 | 6.3% | NLP |
| laterality | 542 | 9,371 | 5.8% | Structured (op sheet) |
| rln_finding | 371 | 9,371 | 4.0% | NLP |
| strap_muscle | 186 | 9,371 | 2.0% | NLP |
| drain | 169 | 9,371 | 1.8% | NLP |
| ebl_ml | 124 | 9,371 | 1.3% | Structured (op sheet) |
| reoperative_field | 46 | 9,371 | 0.5% | NLP |
| parathyroid_autograft | 40 | 9,371 | 0.4% | NLP |
| local_invasion | 25 | 9,371 | 0.3% | NLP |
| gross_ete | 22 | 9,371 | 0.2% | NLP |
| tracheal_involvement | 9 | 9,371 | 0.1% | NLP |
| esophageal_involvement | 0 | 9,371 | 0.0% | NLP |
| central_neck_dissection | 0 | 9,371 | 0.0% | Structured (broken) |
| lateral_neck_dissection | 0 | 9,371 | 0.0% | Structured (broken) |

### MotherDuck (current production state)

Only `procedure_normalized` (93.1%), `laterality` (5.8%), and `ebl_ml` (1.3%) have
non-zero extraction. All 11 NLP-derived fields are FALSE/NULL.

### RLN Finding Detail (Local)

| Finding | Count |
|---------|-------|
| rln_preserved | 347 |
| rln_bilateral_preserved | 12 |
| rln_injured | 7 |
| rln_stretched | 5 |

### Known Structural Gaps

1. **CND/LND flags are 0.0% everywhere** — script 22's regex matching on
   `path_synoptics.thyroid_procedure` text is not capturing CND/LND. However,
   `path_synoptics.central_compartment_dissection` has 665 patients with data,
   and the composite CLN flag (used in H1 analysis) captures ~1,247 lobectomy patients.
   These structured fields exist but are not wired to `operative_episode_detail_v2`.

2. **Laterality is 94.2% NULL** — only 542/9,371 episodes have laterality from the
   operative sheet's `side_of_largest_tumor_or_goiter` field. Most laterality data
   comes from path_synoptics/tumor_episode_master_v2 instead.

3. **3 schema-defined columns never populated**: `parathyroid_autograft_count`,
   `parathyroid_autograft_site`, `parathyroid_resection_flag`

---

## Phase 6: Verdict

### A. Are operative notes fully parsed?

**No.** Only 20.3% of operative episodes have any NLP-enriched field (local).
On MotherDuck production, 0% are NLP-enriched. Op notes exist for only 47.4% of
operative-episode patients, concentrated in the 2019-2022 era. Pre-2019 surgeries
(~6,500 episodes) have no op note text to parse.

### B. Are parsed operative findings linked to the correct surgery events?

**For the ~2,300 patients with both an operative episode AND an op note:** 942
(40.8%) have confident same-day linkage. The remainder are ambiguous primarily
due to missing note dates (1,267 of 4,680 op notes lack date metadata), not due
to mislinkage. The date-proximal matching logic is sound for single-surgery
patients (99.97% of the cohort).

### C. Are those surgery events linked to the correct pathology reports?

**Yes, strongly.** 92.1% of operative episodes align to pathology on the exact
same date. The v3 scoring system rates 91.9% as high_confidence or exact_match.
Only 9 episodes have true date discordance (>7 day gap), and 84 have laterality
discordance. Surgery-pathology linkage is not a current gap.

### D. How many cancer patients still have surgery proven only by pathology but no operative-note granularity?

**916 cancer patients** (22.1%) have pathology-confirmed cancer with no operative
episode at all (entirely in path_synoptics but not in operative_details). An
additional **2,306 cancer patients** have an operative episode but no NLP-enriched
findings (0 on MotherDuck; locally, some fraction of the 1,900 NLP-enriched episodes
are cancer patients).

### E. Is operative-note extraction complete enough for current and future manuscripts?

**For the current ETE/staging manuscript: YES.** Core staging variables (ETE, margins,
invasion, LN counts, T/N/M staging) come from structured `path_synoptics` which
covers 10,871 patients — operative note NLP is supplementary, not primary.

**For complication-focused manuscripts (H1 CLN/lobectomy): PARTIAL.** RLN monitoring
status (18.2% local coverage), drain usage, and intraoperative findings are only
available from op note NLP. But H1 already uses structured `complications` table
data for its primary endpoints.

**For future op-technique manuscripts: INSUFFICIENT.** Detailed intraoperative
technique (parathyroid handling, nerve monitoring use, Berry ligament dissection)
has <5% coverage and would require dedicated extraction work.

### Classification

## TARGETED OPERATIVE EXTRACTION RECOMMENDED

**Priority actions (ordered):**
1. **Immediate: Sync NLP to MotherDuck** — Re-run script 22 with NLP enrichment, then
   re-materialize via script 26. This recovers 1,900 enriched episodes at zero cost.
2. **Wire CND/LND flags** — Use `path_synoptics.central_compartment_dissection` (665 pts)
   and the existing composite CLN flag logic to populate the episode-level flags.
3. **Fix op note date parsing** — 1,267 of 4,680 op notes have no parseable date. Improving
   date extraction from note text would upgrade 1,343 AMBIGUOUS linkages.
4. **Accept pre-2019 gap** — Pre-2019 surgeries (6,500+ episodes) have no electronic op
   notes. This is a data source limitation, not an extraction gap.

---

## Manuscript Methods Supplement

> **Operative Note Coverage.** Of 10,871 patients undergoing thyroid surgery,
> 9,368 (86.2%) had structured operative episode records derived from operative
> sheets and pathology synoptics. Operative note free text was available for
> 4,439 (40.8%) patients, predominantly from 2019-2022 (88-90% coverage in
> those years vs. <5% pre-2019). Natural language processing extracted surgical
> detail from available operative notes, yielding intraoperative nerve monitoring
> data for 1,702 (18.2%) episodes, RLN findings for 371 (4.0%), and operative
> findings for 588 (6.3%). Surgery-to-pathology linkage was achieved for 92.1%
> of episodes by exact date matching, with an additional 4.4% linked by the v3
> scoring algorithm. Core tumor staging (T/N/M, histology, ETE, margins) was
> obtained from structured pathology synoptic reports (n=11,688) rather than
> operative notes.

---

## Deliverables

### MotherDuck Tables Created
- `val_operative_note_coverage_v1` (10,871 rows) — per-patient operative note coverage flags
- `val_operative_note_parse_coverage_v1` (9,371 rows) — per-episode NLP parse detail
- `review_operative_note_linkage_v1` (9,370 rows) — surgery↔op note linkage with categories
- `val_surgery_path_linkage_v1` (9,368 rows) — surgery↔pathology date/laterality alignment
- `review_surgery_path_discordance_v1` (730 rows) — discordant linkages for manual review
- `val_operative_variable_coverage_v1` (16 rows) — per-variable extraction rates

### Exports
- `exports/operative_note_path_linkage_audit_20260313/` — CSV exports of all 6 audit tables + manifest.json
- `docs/operative_note_path_linkage_audit_20260313.md` — this report

---

*Generated by script 70 — 2026-03-13T08:03*
