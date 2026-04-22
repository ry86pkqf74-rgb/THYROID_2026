# Script 390 — ETE Adjudication Reconciliation — Phase 0 Probe Report

**Generated:** 2026-04-22T22:06:00.715383+00:00
**DB:** `thyroid_canonical_publication_v1_0`
**Rule selected:** Rule A (worst-of, AJCC8 T3b) — Logan's explicit call 2026-04-22

---

## Pre-State: CPM ETE Grade Distribution

| ete_grade_final_v2 | n | gross_ete_flag | op_intraop_gross | path_gross_ete |
|---|---|---|---|---|
| `None` | 6752 | 0 | 0 | 0 |
| `microscopic` | 2580 | 0 | 1 | 0 |
| `gross` | 1311 | 1139 | 1132 | 976 |
| `false` | 179 | 0 | 0 | 0 |
| `present_ungraded` | 29 | 6 | 10 | 6 |
| `absent` | 16 | 0 | 2 | 0 |
| `true` | 4 | 1 | 1 | 1 |

## Pre-State: ETE × Invasion Rollup Cross-Tab (selected rows)

| ete_grade_final_v2 | any_gross_ete_anywhere | any_micro_ete_anywhere | n |
|---|---|---|---|
| `absent` | False | False | 14 |
| `absent` | True | False | 2 |
| `false` | False | False | 179 |
| `gross` | True | False | 914 |
| `gross` | True | True | 218 |
| `gross` | False | False | 167 |
| `gross` | False | True | 12 |
| `microscopic` | False | False | 2530 |
| `microscopic` | False | True | 49 |
| `microscopic` | True | False | 1 |
| `present_ungraded` | False | False | 19 |
| `present_ungraded` | True | False | 10 |
| `true` | False | False | 3 |
| `true` | True | False | 1 |
| `None` | False | False | 6752 |

## Pre-State: Adjudication Cohort (CPM adjudicated_flag=TRUE)

ete_adjudication_v1 row count: **45** (must = 45)

| ete_grade_adjudicated | n |
|---|---|
| `unable_to_determine` | 26 |
| `absent` | 16 |
| `gross` | 2 |
| `microscopic` | 1 |

**Expanded sticky guard covers:** 26 unable_to_determine rows

## Pre-State: Boolean-String Junk Rows

Total junk rows (ete_grade_final_v2 in ('true','false')): **183**

| value | n |
|---|---|
| `false` | 179 |
| `true` | 4 |

NULL ete_grade_final_v2 rows (not graded in 390): 6752

## Pre-State: Contradiction Queue

Current row count: **2788**

| status | n |
|---|---|
| `awaiting_manual_review` | 2788 |

---

## Rule A Simulation (read-only)

Rule A (worst-of, AJCC8 T3b) with expanded sticky guard:
  - Sticky: `ete_adjudicated_flag=TRUE AND ete_grade_adjudicated IN (microscopic, absent, unable_to_determine)`
  - Gross branch: any of `gross_ete_flag`, `op_intraop_gross_ete_any`, `path_gross_ete_flag`, `any_gross_ete_anywhere`
  - NULL guard: NULL rows unchanged

| Metric | Live | Frozen Baseline | Drift | Status |
|---|---|---|---|---|
| would-be-gross post-rule | 1311 | 1311 | 0.00% | ✅ PASS |
| flip-up to gross | 0 | 1121 | 100.00% | ❌ FAIL |
| flip-down from gross | 0 | 0 | 0.00% | ✅ PASS |
| flip to microscopic | 0 | 4 | 100.00% | ❌ FAIL |
| TOTAL ROWS MUTATED | 0 | 1125 | 100.00% | ❌ FAIL |
| bool-string junk rows | 183 | 194 | 5.67% | ❌ FAIL |
| unable_to_determine sticky | 26 | 26 | 0.00% | ✅ PASS |

## Projected Residual Queue Population

- microscopic-no-signal rows (FF cohort): **0** (expected ~2,551)
- unable_to_determine sticky (expanded): **26** (expected 26)
- boolean-string junk: **183** (expected 194)
- **Total new rows**: **209** (expected ~2,771)

---

## Drift Gate

### ✅ All 8 frozen baselines within 2% tolerance

Plan approval: **Rule A** (pre-approved, Logan 2026-04-22)

---

## Phase 1 Gate

`scripts/output/390_plan_approval.txt` has been written with `Rule A`.
Re-run with `--apply` to execute phases 2–3.
