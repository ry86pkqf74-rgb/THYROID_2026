# Operative Note–Pathology Linkage Audit

**Date:** 2026-03-13 10:29
**Database:** MotherDuck
**Script:** `scripts/70_operative_note_path_linkage_audit.py`

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Total patients (path_synoptics) | 10,871 |
| Total surgeries (path_synoptics rows) | 11,688 |
| Operative episode records | 9,371 |
| Patients with operative episode | 9,368 |
| Patients with op notes | 4,439 |
| Patients with surgery AND op note | 2,310 |
| Patients with surgery, NO op note | 6,423 |
| Patients in path_synoptics only (no operative episode) | 2,138 |

### Cancer-Specific Coverage

| Metric | Value |
|--------|-------|
| Cancer patients (total) | 4,137 |
| Cancer with operative episode | 3,221 |
| Cancer with op note | 1,826 |
| Cancer with both episode + op note | 915 |
| **Cancer with NO operative episode** | **916** (22.1%) |

### NLP Parse Coverage

| Metric | Value |
|--------|-------|
| Episodes with NLP parse | 1,900 |
| Episodes without NLP parse | 7,471 |
| **NLP parse rate** | **20.3%** |

---

## Phase 3: Operative Note ↔ Surgery Linkage

| Category | Count |
|----------|-------|
| LINKED_CONFIDENT (same-day) | 942 |
| LINKED_DATE_PROXIMAL (≤7d) | 25 |
| AMBIGUOUS (>7d or date missing) | 1,343 |
| MISSING (no op note) | 7,060 |

---

## Phase 4: Surgery ↔ Pathology Linkage

| Status | Count |
|--------|-------|
| ALIGNED (exact date + laterality ok) | 8,638 |
| PROXIMAL_ALIGNED (≤7d + laterality ok) | 0 |
| NO_PATHOLOGY (operative episode, no tumor match) | 635 |
| DATE_DISCORDANT | 9 |
| LATERALITY_DISCORDANT | 84 |
| DATE_MISSING | 2 |
| REVIEW_NEEDED | 0 |

Total discordance/review items: 730

---

## Phase 5: Operative Variable Extraction Coverage

| Variable | Extracted | Total | Rate |
|----------|-----------|-------|------|
| central_neck_dissection | 2,497 | 9,371 | 26.6% |
| drain | 169 | 9,371 | 1.8% |
| ebl_ml | 124 | 9,371 | 1.3% |
| esophageal_involvement | 0 | 9,371 | 0.0% |
| gross_ete | 22 | 9,371 | 0.2% |
| lateral_neck_dissection | 241 | 9,371 | 2.6% |
| laterality | 542 | 9,371 | 5.8% |
| local_invasion | 25 | 9,371 | 0.3% |
| operative_findings_raw | 588 | 9,371 | 6.3% |
| parathyroid_autograft | 40 | 9,371 | 0.4% |
| procedure_normalized | 8,727 | 9,371 | 93.1% |
| reoperative_field | 46 | 9,371 | 0.5% |
| rln_finding | 371 | 9,371 | 4.0% |
| rln_monitoring | 1,702 | 9,371 | 18.2% |
| strap_muscle | 186 | 9,371 | 2.0% |
| tracheal_involvement | 9 | 9,371 | 0.1% |

---

## Op Note Coverage by Surgery Year (2010+)

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
| 2019 | 844 | 261 | 30.9% |
| 2020 | 689 | 616 | 89.4% |
| 2021 | 703 | 615 | 87.5% |
| 2022 | 547 | 491 | 89.8% |
| 2023 | 27 | 2 | 7.4% |
| 2024 | 16 | 0 | 0.0% |

---

## Surgery-Pathology v3 Linkage Tiers

| Tier | Count |
|------|-------|
| high_confidence | 8,236 |
| weak | 566 |
| exact_match | 414 |
| plausible | 190 |
| unlinked | 3 |

---

## Phase 6: Verdict

### A. Are operative notes fully parsed?

**No.** NLP enrichment covers 20.3% of operative episodes. On MotherDuck, NLP
enrichment is **zero** — the materialized table was created before or without the
NLP enrichment step from script 22's `enrich_from_v2_extractors()`. Local DuckDB has
partial enrichment for episodes with matching clinical notes.

Op notes exist for only 4,439 / 9,368
operative-episode patients (47.4%).
Coverage is concentrated in 2019-2022 (88-90%); pre-2019 is near-zero.

### B. Are they fully linked to pathology by date and patient?

**Largely yes for existing episodes.** 8,638 / 9,371 (92.2%)
operative episodes have exact same-day pathology alignment. However, 2,138
patients (19.7%)
have pathology records but no operative episode, meaning they are known only through path_synoptics.

### C. Is missing operative detail a manuscript blocker?

**No, for the current ETE/staging manuscript.** The primary analyses use structured
path_synoptics data (ETE, margins, invasion) which covers 10,871
patients. Operative note NLP adds granularity (RLN monitoring, drain, parathyroid
management) but the core staging variables come from pathology synoptics.

For **complication-focused manuscripts** (H1 CLN/lobectomy), the gap is more relevant:
RLN monitoring status and intraoperative findings depend on parsed op notes.

### D. Is targeted additional extraction worthwhile?

**Yes, targeted MotherDuck sync is the priority.** The NLP enrichment already exists
in local DuckDB but was never propagated to MotherDuck. Re-running script 22 with
NLP enrichment then re-materializing via script 26 would immediately recover
1,900 enriched episodes.

### Classification

## **MAJOR OPERATIVE EXTRACTION GAP REMAINS**

**Recommended actions:**
1. Re-run script 22 `enrich_from_v2_extractors()` to ensure local NLP enrichment is current
2. Re-materialize `operative_episode_detail_v2` to MotherDuck via script 26
3. Populate CND/LND flags from `path_synoptics.central_compartment_dissection` (665 patients)
4. For pre-2019 surgeries without op notes, accept path_synoptics as sole surgery evidence

---

## Deliverables Created

### MotherDuck Tables
- `val_operative_note_coverage_v1` — per-patient operative note coverage flags
- `val_operative_note_parse_coverage_v1` — per-episode NLP parse detail
- `review_operative_note_linkage_v1` — surgery↔op note linkage with categories
- `val_surgery_path_linkage_v1` — surgery↔pathology date/laterality alignment
- `review_surgery_path_discordance_v1` — discordant linkages for manual review
- `val_operative_variable_coverage_v1` — per-variable extraction rates

### Exports
- `exports/operative_note_path_linkage_audit_20260313/` — CSV exports of all audit tables
- `docs/operative_note_path_linkage_audit_20260313.md` — this report

---

*Generated by script 70 — 2026-03-13T10:29:25.958993*
