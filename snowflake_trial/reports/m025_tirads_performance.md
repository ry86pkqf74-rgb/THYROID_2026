# M025 — TIRADS Diagnostic Performance Table
**Generated:** 2026-05-01 18:26:51
**Cohort:** 3,396 patients with at least one TIRADS-categorized US exam (max category across exams)
**Source:** CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT (post mig_260) joined to CPM
**Note:** Operative cohort — substantially enriched for malignancy vs published ACR cohort (manuscript footnote candidate per round-5 finding)

## Per-category breakdown

| TIRADS | n | malignant | ROM% | ACR-expected ROM% |
| --- | --- | --- | --- | --- |
| TR1 | 346 | 105 | 30.3% | <2% |
| TR2 | 300 | 101 | 33.7% | <2% |
| TR3 | 852 | 255 | 29.9% | <5% |
| TR4 | 495 | 244 | 49.3% | 5-20% |
| TR5 | 1,403 | 843 | 60.1% | >20% |

## Diagnostic performance — multiple decision thresholds

| Threshold (test+) | Sens | Spec | PPV | NPV | Accuracy | LR+ | LR- |
| --- | --- | --- | --- | --- | --- | --- | --- |
| TR ≥ TR2 | 0.932 | 0.130 | 0.473 | 0.697 | 0.496 | 1.07 | 0.52 |
| TR ≥ TR3 | 0.867 | 0.238 | 0.488 | 0.681 | 0.525 | 1.14 | 0.56 |
| TR ≥ TR4 | 0.702 | 0.561 | 0.573 | 0.692 | 0.625 | 1.60 | 0.53 |
| TR ≥ TR5 | 0.545 | 0.697 | 0.601 | 0.646 | 0.628 | 1.80 | 0.65 |

**Cohort:** 3,396 patients (malignant 1,548; benign 1,848)

## 2×2 table at TR ≥ TR4 (canonical decision threshold)

|  | malignant | benign | total |
| --- | --- | --- | --- |
| **TR ≥ TR4** | 1087 | 811 | 1898 |
| **TR < TR4** | 461 | 1037 | 1498 |
| **total** | 1548 | 1848 | 3396 |

## Methods

- **Cohort:** Patients with `MAX_TIRADS_CATEGORY_EVER` populated in `canonical_us_patient_master_VIEW_v2`. Excludes patients with US exams not categorized via TIRADS (e.g., older exams pre-2017 ACR adoption).
- **Outcome (gold standard):** `IS_MALIGNANT` from `canonical_patient_master` (path-confirmed). NIFTP currently coded `IS_MALIGNANT=TRUE` per pre-2017 convention; mig_264b plans to recategorize.
- **Test variable:** Patient-level `MAX_TIRADS_CATEGORY_EVER` (highest TR across all that patient's US exams).
- **Per-category ROM:** `n_malignant / n_total` within each TR category.
- **Diagnostic performance:** Standard 2×2 with TR ≥ T_threshold as test positive.
- **LR+:** Sensitivity / (1 − Specificity); LR-: (1 − Sensitivity) / Specificity.
- **Cohort caveat:** Operative bias inflates ROM at every TR category vs ACR-published expected ranges. See round-5 Prompt 7 finding for details.
