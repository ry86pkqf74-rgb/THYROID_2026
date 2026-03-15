# Release Governance Update — Gates G8–G11

**Date:** 2026-03-15  
**Script:** `scripts/91_promotion_gate.py` (extended)

## Purpose

Extends the promotion gate framework from 7 gates (G1–G7) to 11 gates (G1–G11),
covering review operations freshness, source-limited enforcement registry
completeness, multi-artifact governance, and non-regression proof.

## New Gates

### G8: Review Ops Freshness

**Checks:**
- `unified_review_queue_v1` exists
- `review_ops_progress_v1` exists
- `review_ops_kpi_v1` exists
- `unified_review_queue_v1.created_at` is within 30 days

**Status:** PASS  
**Fail action:** Run `scripts/101_review_ops.py --md`

### G9: Source-Limited Enforcement Registry

**Checks:**
- `source_limited_enforcement_registry_v2` exists with ≥30 rows
- Both CANONICAL and SOURCE_LIMITED tiers present
- `val_source_limited_enforcement_v1` has 0 FAIL assertions

**Status:** PASS (35 fields, all tiers covered)  
**Fail action:** Run `scripts/103_source_limited_enforcement.py --md`

### G10: Multi-Artifact Freshness

**Checks existence of 3 governance artifacts:**
- `val_multi_surgery_review_queue_v3` — multi-surgery audit
- `val_episode_linkage_v2_scorecard` — episode linkage v2 scorecard
- `val_hardening_summary` — hardening summary

**Status:** PASS  
**Fail action:** Run relevant upstream scripts (97, 100, hardening pipeline)

### G11: Non-Regression Proof

**Checks that 5 core tables remain above minimum row thresholds:**

| Table | Minimum | Actual |
|-------|---------|--------|
| `patient_analysis_resolved_v1` | 10,000 | 10,871 |
| `episode_analysis_resolved_v1_dedup` | 8,000 | 9,368 |
| `manuscript_cohort_v1` | 10,000 | 10,871 |
| `analysis_cancer_cohort_v1` | 3,500 | 4,136 |
| `thyroid_scoring_py_v1` | 10,000 | 10,871 |

**Status:** PASS  
**Fail action:** Investigate row loss; check scripts 48, 57, 51b

## Updated CRITICAL_TABLES List

3 new entries added to `CRITICAL_TABLES`:
- `unified_review_queue_v1`
- `review_ops_progress_v1`
- `source_limited_enforcement_registry_v2`

## Updated METRIC_BOUNDS

- `surgical_cohort` upper bound increased: 11,500 → 12,000 (demographics_harmonized_v2 increased patient spine to 11,673)

## Full Gate Summary (G1–G11)

| Gate | Name | Status | Detail |
|------|------|--------|--------|
| G1 | critical_tables | PASS | All 17 tables present |
| G2 | metric_bounds | PASS | All 8 metrics in range (bound updated) |
| G3 | no_row_multiplication | PASS | No row duplication |
| G4 | null_rate_ceilings | PASS | All 6 checks pass |
| G5 | validation_tables_pass | SKIP | Prod-only gate |
| G6 | ro_share_accessible | SKIP | Prod-only gate |
| G7 | canonical_metrics_drift | SKIP | Requires script 100 |
| G8 | review_ops_freshness | PASS | All review ops tables present and fresh |
| G9 | source_limited_registry | PASS | 35 fields, all tiers covered |
| G10 | multi_artifact_freshness | PASS | All 3 governance artifacts present |
| G11 | nonregression_proof | PASS | All 5 tables above minimum thresholds |

## Usage

```bash
# Validate prod (dry-run, skips prod-only gates if --to is not prod)
.venv/bin/python scripts/91_promotion_gate.py --from prod --to qa --dry-run

# Full prod promotion check
.venv/bin/python scripts/91_promotion_gate.py --from qa --to prod
```
