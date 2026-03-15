# Multi-Surgery Artifact Linkage Audit — 20260315

**Generated**: 20260315_1310
**Script**: `scripts/98_multi_surgery_artifact_linkage_audit.py`
**Target**: MotherDuck `thyroid_research_2026` (prod)
**Predecessor**: `scripts/96_episode_downstream_repair.py` (ep-id fix)

## Purpose

Post-repair hardening audit to verify that clinical artifacts (op notes,
H&P, discharge summaries, pathology, FNA, molecular, RAI, imaging, labs)
are correctly linked to the right surgery episode in multi-surgery patients.

## Output Tables

| Table | Rows | Description |
|-------|------|-------------|
| `val_multi_surgery_artifact_linkage_v1` | 9,777 | Per-artifact linkage verdict (confidence + reason) |
| `multi_surgery_artifact_review_queue_v1` | 5,774 | Triaged queue of problematic artifacts |
| `multi_surgery_oed_coverage_gap_v1` | 1,577 | OED row ↔ canonical episode coverage mismatch |

## Artifact Linkage Confidence Distribution

| Confidence | Count |
|-----------|-------|
| exact | 2,404 |
| high_confidence | 1,120 |
| plausible | 1,457 |
| weak | 4,610 |
| no_match | 186 |

## Domain Breakdown

| Domain | Artifacts |
|--------|-----------|
| discharge_summary | 9 |
| fna | 572 |
| h_and_p | 185 |
| imaging | 1,067 |
| lab | 5,748 |
| molecular | 94 |
| op_note | 374 |
| pathology | 1,292 |
| rai | 436 |

## Reason Codes (artifacts with issues)

| Reason | Count |
|--------|-------|
| date_out_of_window | 4,593 |
| missing_anchor_date | 123 |

## Review Queue Priority

| Priority | Count |
|----------|-------|
| HIGH | 1,018 |
| MEDIUM | 4,630 |
| LOW | 126 |
| **Total** | **5,774** |

## OED Coverage Gap (multi-surgery patients)

| Status | Count |
|--------|-------|
| date_mismatch | 2 |
| exact_match | 620 |
| no_date | 2 |
| no_row | 953 |

Multi-surgery patients audited: **761**

## Scoring Definitions

### Confidence Tiers

| Tier | Definition |
|------|-----------|
| exact | Same-day (op note), within clinical window (H&P 0-7d pre, DC 0-7d post, RAI 14-180d post), or date match |
| high_confidence | Within 14 days (notes), 90 days (labs), 180 days (molecular/FNA) |
| plausible | Within 30-365 days depending on domain |
| weak | Beyond plausible window but still temporally relatable |
| no_match | No date, or beyond any reasonable window |

### Reason Codes

| Code | Meaning |
|------|---------|
| `date_out_of_window` | Artifact date falls outside temporal window for its matched surgery |
| `missing_anchor_date` | No usable date on artifact |
| `cross_episode_mismatch` | Pathology ep_id != surgery ep_id it was linked to |
| `only_single_oed_row` | Multi-surgery patient but only 1 operative row |
| `ambiguous_equidistant` | Artifact nearly equidistant between 2 surgeries |

## Recommended Triage Subset

For manual review, prioritize:

1. **HIGH priority** items in `multi_surgery_artifact_review_queue_v1`
   — these are cross-episode mismatches, ambiguous assignments, and
   no-match artifacts that may affect analytic integrity
2. **OED coverage gaps** (`multi_surgery_oed_coverage_gap_v1` where
   `oed_match_status = 'no_oed_row'`) — 525+ patients need upstream
   operative record population before their 2nd+ surgeries can be audited
3. **Pathology `cross_episode_mismatch`** — these indicate the
   surgery_pathology_linkage_v3 routing diverges from temporal expectation

## Provenance

- All tables are additive (CREATE OR REPLACE TABLE)
- No source table was modified
- Export bundle: `exports/multi_surgery_artifact_linkage_20260315_1310/`
- Audit timestamp: 2026-03-15T13:10:49.013005
