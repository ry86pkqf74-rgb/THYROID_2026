# Script 391 — T-stage Downstream Reconciliation: Phase 0 Probe Report

**Generated:** 2026-04-22T22:36:21.168535+00:00
**Database:** thyroid_canonical_publication_v1_0

---

## 1. Baseline Drift Check

| Metric | Expected | Actual | Drift | Status |
|--------|----------|--------|-------|--------|
| CPM_ROWCOUNT | 10,871 | 10,871 | 0.00% | ✅ OK |
| ETE_GRADE_FINAL_V2_GROSS | 1,311 | 1,311 | 0.00% | ✅ OK |
| ETE_GRADE_FINAL_V2_MICROSCOPIC | 2,580 | 2,580 | 0.00% | ✅ OK |
| ETE_GRADE_FINAL_V2_ABSENT | 16 | 16 | 0.00% | ✅ OK |
| ETE_GRADE_FINAL_V2_PRESENT_UNGRADED | 29 | 29 | 0.00% | ✅ OK |
| ETE_GRADE_FINAL_V2_JUNK_FALSE | 179 | 179 | 0.00% | ✅ OK |
| ETE_GRADE_FINAL_V2_JUNK_TRUE | 4 | 4 | 0.00% | ✅ OK |
| ETE_GRADE_FINAL_V2_NULL | 6,752 | 6,752 | 0.00% | ✅ OK |
| MICROSCOPIC_ETE_T3B_CORRECTED_TRUE | 906 | 906 | 0.00% | ✅ OK |
| AJCC8_T_STAGE_W_MICROETE_DEPRECATED_T3B | 1,146 | 1,146 | 0.00% | ✅ OK |

**Overall baseline gate:** ✅ ALL PASS

---

## 2. Pre-State Problem Summary

- Gross-ETE rows with non-T3b DEPRECATED stage (target for rebuild): **172**
- Semantic contradiction (gross + micro_t3b_corrected=TRUE): **906**

---

## 3. Dry-Run Step Projections

| Step | Operation | Rows Changing | Notes |
|------|-----------|---------------|-------|
| 2B  | ete_grade + ete_grade_final SYNC ← ete_grade_final_v2 | **1,144** | Both columns sync simultaneously |
| 2C  | DEPRECATED T3b upgrade for gross-ETE | **172** | Expected ~172 |
| 2D-B | micro_t3b_corrected=TRUE count (post-rebuild) | from 906 → **0** | Contradiction cleared |
| 2D-C/D | ajcc8_t_stage rebuild | **1,078** rows (+T3b=1078, -T3b=0) | |
| 2D-E | ajcc8_stage_group_corrected rebuild | **3,777** | AJCC8 staging rule |
| 2E  | manuscript_cohort_v1.ajcc8_t_stage rebuild | **172** (of 10871 total) | 100% join expected |

---

## 4. Next Steps

1. Review the projections above.
2. Write `APPROVED` to `scripts/output/391_plan_approval.txt`.
3. Run: `python3 scripts/391_t_stage_downstream_reconciliation.py --apply`
