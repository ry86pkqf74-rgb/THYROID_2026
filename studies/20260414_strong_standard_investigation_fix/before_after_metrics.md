# Before/After Metrics — 2026-04-14 Strong Standard Investigation

## Baseline
- **Reference:** `studies/20260413_full_execution_reaudit/executive_verdict.md`
- **Baseline status:** SCOPED_CONFIRMED_ONLY

## US Nodule Coverage

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| Canonical total | 37,016 | 37,016 | 0 |
| COMPLETE corpus | 19,891 | 19,891 | 0 |
| Scored corpus | 8,331 | 8,331 | 0 |
| Imaging_12 corpus | 8,794 | 8,794 | 0 |
| Policy-aligned true gaps | 0 | 0 | 0 |

## TI-RADS Completeness

| Metric | Before | After | Delta | Note |
|--------|--------|-------|-------|------|
| Reported TI-RADS | 27,903 | 27,903 | 0 | |
| ACR recalculated | 19,891 | 19,891 | 0 | |
| No TI-RADS at all | 8,794 | 8,794 | 0 | SOURCE_LIMITED: Imaging_12 has 0 ACR criteria |

### Cross-corpus overlap (informational)
- 304 Imaging_12 rows have a COMPLETE match within ±30d (already has TI-RADS on separate canonical row)
- 3,802 Imaging_12 rows have a scored match within ±30d
- Cross-row propagation not implemented (heuristic match, not deterministic)

## Nodule Linkage

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| linked_to_fna | 6,359 | 6,359 | 0 |
| no_eligible_fna | 30,657 | 30,657 | 0 |
| unresolved | 0 | 0 | 0 |
| null reason codes | 0 | 0 | 0 |

## FNA Bethesda

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| Episode master total | 8,119 | 8,119 | 0 |
| Episode has numeric | 8,096 | 8,096 | 0 |
| Episode NULL numeric | 23 | 23 | 0 |
| Resolved view has numeric | 8,096 | 8,096 | 0 |
| Resolved view NULL | 23 | 23 | 0 |

### Backfill script results
- Script 152 (cytology→episode) dry-run: **0 candidates**
- Script 154 (path_text parse) dry-run: **0 candidates**
- All 23 NULLs confirmed unscorable (22 no source + 1 physician name in pathology field)

## US Lymph Node

| Metric | Before | After | Delta | Note |
|--------|--------|-------|-------|------|
| Exams with LN text | 6,793 | 6,793 | 0 | |
| Structured per-level | 0 | 0 | 0 | SOURCE_LIMITED |
| Negative/normal | 6,453 | 6,453 | 0 | |
| Other narrative | 340 | 340 | 0 | |

## Release Validation (119 --release-mode)

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| PASS | 35 | 40 | +5 |
| WARN | 4 | 5 | +1 |
| FAIL | 0 | 0 | 0 |

## Summary
**No deterministic fixes were available.** All remaining gaps are source-limited. The investigation confirmed that prior remediation efforts (scripts 152-154, policy alignment, view deployment) have exhausted all deterministic improvements possible with current data.
