# M025 — TIRADS Diagnostic Performance Table
**Generated:** 2026-05-03 22:50:37
**Cohort:** 3,396 patients with at least one TIRADS-categorized US exam (max category across exams)
**Source:** CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT (post mig_260) joined to CPM
**Note:** Operative cohort — substantially enriched for malignancy vs published ACR cohort (manuscript footnote candidate per round-5 finding)

## Per-category breakdown

| TIRADS | n | malignant | ROM% | ACR-expected ROM% |
| --- | --- | --- | --- | --- |
| TR1 | 346 | 96 | 27.7% | <2% |
| TR2 | 300 | 97 | 32.3% | <2% |
| TR3 | 852 | 237 | 27.8% | <5% |
| TR4 | 495 | 235 | 47.5% | 5-20% |
| TR5 | 1,403 | 824 | 58.7% | >20% |

## Diagnostic performance — multiple decision thresholds

| Threshold (test+) | Sens | Spec | PPV | NPV | Accuracy | LR+ | LR- |
| --- | --- | --- | --- | --- | --- | --- | --- |
| TR ≥ TR2 | 0.936 | 0.131 | 0.457 | 0.723 | 0.484 | 1.08 | 0.49 |
| TR ≥ TR3 | 0.870 | 0.238 | 0.471 | 0.701 | 0.515 | 1.14 | 0.55 |
| TR ≥ TR4 | 0.711 | 0.560 | 0.558 | 0.713 | 0.626 | 1.62 | 0.52 |
| TR ≥ TR5 | 0.553 | 0.696 | 0.587 | 0.666 | 0.634 | 1.82 | 0.64 |

**Cohort:** 3,396 patients (malignant 1,489; benign 1,907)

## 2×2 table at TR ≥ TR4 (canonical decision threshold)

|  | malignant | benign | total |
| --- | --- | --- | --- |
| **TR ≥ TR4** | 1059 | 839 | 1898 |
| **TR < TR4** | 430 | 1068 | 1498 |
| **total** | 1489 | 1907 | 3396 |

## Methods

- **Cohort:** Patients with `MAX_TIRADS_CATEGORY_EVER` populated in `canonical_us_patient_master_VIEW_v2`. Excludes patients with US exams not categorized via TIRADS (e.g., older exams pre-2017 ACR adoption).
- **Outcome (gold standard):** `IS_MALIGNANT` from `canonical_patient_master` (path-confirmed). NIFTP currently coded `IS_MALIGNANT=TRUE` per pre-2017 convention; mig_264b plans to recategorize.
- **Test variable:** Patient-level `MAX_TIRADS_CATEGORY_EVER` (highest TR across all that patient's US exams).
- **Per-category ROM:** `n_malignant / n_total` within each TR category.
- **Diagnostic performance:** Standard 2×2 with TR ≥ T_threshold as test positive.
- **LR+:** Sensitivity / (1 − Specificity); LR-: (1 − Sensitivity) / Specificity.
- **Cohort caveat:** Operative bias inflates ROM at every TR category vs ACR-published expected ranges. See round-5 Prompt 7 finding for details.
