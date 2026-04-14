# Fix Plan — 2026-04-14

## Investigation Summary

After comprehensive investigation of all domains, **no deterministic fixes are available** that would move the repo toward strong-standard completion. All remaining gaps are source-limited.

## Investigated Fix Opportunities

### 1. Bethesda Backfill (Scripts 152/154)
- **Script 152 dry-run:** 0 candidates (no matchable cytology→episode joins for the 23 NULL rows)
- **Script 154 dry-run:** 0 candidates (no parseable Bethesda from path_text for these rows)
- **Result:** No fix possible. All 23 NULLs have documented unscorable reasons.

### 2. TI-RADS Propagation for Imaging_12
- **Assessment:** 8,794 Imaging_12 nodules have no TI-RADS. Source has `n_criteria_available = 0` for all rows.
- **Cross-corpus overlap:** 304 have COMPLETE match, 3,802 have scored match within ±30d. But cross-row propagation would be a heuristic match (different workbooks, potentially different nodule identification).
- **Result:** No deterministic fix. Source does not provide TI-RADS or ACR features.

### 3. ACR Recalculation for Scored Corpus
- **Assessment:** 8,331 scored rows have reported TI-RADS but no ACR features. No composition/echogenicity/shape/margins/calcification columns exist.
- **Result:** No fix. ACR recalculation impossible without source features.

### 4. US Lymph Node Structured Extraction
- **Assessment:** Only `lymph_node_assessment` narrative text exists. No per-level structured fields.
- **Result:** No deterministic fix. Would require governed NLP pipeline or radiologist re-review.

### 5. Imaging→FNA Linkage (128 Candidate Rows)
- **Assessment:** From 20260413 remediation pack. These candidates are not deterministic (require human confirmation per script 129 relaxed-tier policy).
- **Result:** Routed to human_review_packet.csv. Not auto-promoted.

## Plan Decision
**NO_SAFE_DETERMINISTIC_FIX_AVAILABLE** — Document findings, produce audit artifacts, and clearly delineate repo-scoped vs strong-standard status.
