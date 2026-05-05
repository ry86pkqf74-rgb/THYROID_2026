# M048 Handoff README — Racial Disparities in ACR TI-RADS Performance

> **SCOPE FOR THIS DOCUMENT:** Numbers, paths, and caveats ONLY.
> No narrative interpretation, no abstract, no discussion text.
> All values are pre-verified via independent_recompute.py (hard QA gate).

## Run Metadata

| Key | Value |
|-----|-------|
| Study ID | M048 |
| Database | thyroid_canonical_publication_v1_0 |
| DB Tag | pub_v1_1 |
| Mig ID | mig_315 |
| Run Timestamp (UTC) | 2026-05-05T08:40:26.594865+00:00 |
| Git SHA | 9221d3d |
| Bootstrap replicates | 1000 |
| QA Gates | ✓ ALL PASS |

## QA Gate Summary

| Gate | Status | Actual | Expected |
|------|--------|--------|---------|
| patient_black_n | PASS | 1535.0 | 1535.0 |
| patient_white_n | PASS | 1382.0 | 1382.0 |
| patient_asian_n | PASS | 204.0 | 204.0 |
| patient_total_n | PASS | 3375.0 | 3375.0 |
| nodule_strict_total_n | PASS | 3687.0 | 3687.0 |
| pooled_patient_auc_matches_m025 | PASS | 0.6478 | 0.6478 |
| pooled_nodule_auc_matches_m025 | PASS | 0.6399 | 0.6399 |
| wilson_ci_bounds_valid | PASS | 0.0 | 0.0 |

## AUC by Race and Grain (Bootstrap 95% CI; 1,000 replicates)

| Grain | Race | AUC [95% CI] | N | N positive |
|-------|------|-------------|---|-----------|
| nodule_strict | Asian | 0.6661 [0.5984–0.7333] | 205 | 77 |
| nodule_strict | Black | 0.6310 [0.5852–0.6782] | 1,701 | 144 |
| nodule_strict | Other | 0.4894 [0.3750–0.6154] ⚠️ | 87 | 24 |
| nodule_strict | POOLED | 0.6399 [0.6175–0.6625] | 3,687 | 631 |
| nodule_strict | Unknown | 0.6018 [0.5014–0.6996] | 162 | 31 |
| nodule_strict | White | 0.6078 [0.5772–0.6369] | 1,532 | 355 |
| patient | Asian | 0.6601 [0.5843–0.7275] | 204 | 128 |
| patient | Black | 0.6138 [0.5805–0.6424] | 1,535 | 441 |
| patient | Other | 0.6139 [0.4969–0.7304] ⚠️ | 87 | 55 |
| patient | POOLED | 0.6478 [0.6307–0.6656] | 3,375 | 1,479 |
| patient | Unknown | 0.6165 [0.5362–0.6924] | 167 | 81 |
| patient | White | 0.6441 [0.6184–0.6714] | 1,382 | 774 |

## ROM by Race × TI-RADS (Wilson 95% CI)

### Patient Grain

| Race | TR1 | TR2 | TR3 | TR4 | TR5 |
|------|-----|-----|-----|-----|-----|
| Black | 37/184 = 20.1% [15.0–26.5%] | 34/158 = 21.5% [15.8–28.6%] | 100/489 = 20.4% [17.1–24.2%] | 71/207 = 34.3% [28.2–41.0%] | 199/497 = 40.0% [35.8–44.4%] |
| White | 42/120 = 35.0% [27.1–43.9%] | 44/100 = 44.0% [34.7–53.8%] | 108/272 = 39.7% [34.1–45.6%] | 116/215 = 54.0% [47.3–60.5%] | 464/675 = 68.7% [65.2–72.1%] |
| Asian | 8/18 = 44.4% [24.6–66.3%] | 8/20 = 40.0% [21.9–61.3%] | 10/30 = 33.3% [19.2–51.2%] | 26/33 = 78.8% [62.2–89.3%] | 76/103 = 73.8% [64.5–81.3%] |
| POOLED | — | — | — | — | — |

### Nodule Strict Grain

| Race | TR1 | TR2 | TR3 | TR4 | TR5 |
|------|-----|-----|-----|-----|-----|
| Black | — | 0/20 = 0.0% [0.0–16.1%] | 48/893 = 5.4% [4.1–7.0%] | 33/354 = 9.3% [6.7–12.8%] | 63/434 = 14.5% [11.5–18.1%] |
| White | — | 4/8 = 50.0% [21.5–78.5%] | 69/520 = 13.3% [10.6–16.5%] | 95/395 = 24.1% [20.1–28.5%] | 187/609 = 30.7% [27.2–34.5%] |
| Asian | — | 0/1 = 0.0% [0.0–79.3%] | 13/61 = 21.3% [12.9–33.1%] | 17/56 = 30.4% [19.9–43.3%] | 47/87 = 54.0% [43.6–64.1%] |
| POOLED | — | — | — | — | — |

## Threshold Metrics (Wilson 95% CI) — TR≥TR4, Patient Grain

| Race | Sensitivity | Specificity | PPV | NPV |
|------|-------------|-------------|-----|-----|
| Black | 61.2% [56.6–65.7%] | 60.3% [57.4–63.2%] | 38.4% [34.8–42.0%] | 79.4% [76.5–82.0%] |
| White | 74.9% [71.8–77.9%] | 49.0% [45.1–53.0%] | 65.2% [62.0–68.2%] | 60.6% [56.2–64.8%] |
| Asian | 79.7% [71.9–85.7%] | 55.3% [44.1–65.9%] | 75.0% [67.1–81.5%] | 61.8% [49.9–72.4%] |

## Patient–Nodule ROM Inflation by Race × TR (percentage points)

| Race | TR4 Inflation (pp) | TR5 Inflation (pp) |
|------|-------------------|-------------------|
| Black | 25.0 | 25.5 |
| White | 29.9 | 38.0 |
| Asian | 48.4 | 19.8 |

## Feature Distribution Chi-Square Results (Bonferroni α=0.01)

| Feature | chi² | df | p (raw) | p (Bonferroni) | Cramér's V | Significant? |
|---------|------|----|---------|-----------------|-----------:|-------------|
| composition | 7.20 | 4 | 0.12554 | 0.62768 | 0.032 | no |
| echogenicity | 116.07 | 2 | 0.00000 | 0.00000 | 0.184 | YES ✓ |
| shape | 69.64 | 2 | 0.00000 | 0.00000 | 0.142 | YES ✓ |
| margin | 97.26 | 2 | 0.00000 | 0.00000 | 0.168 | YES ✓ |
| foci | 115.95 | 6 | 0.00000 | 0.00000 | 0.130 | YES ✓ |

## FNA Compliance Audit (per race, TR≥TR4 threshold)

| Race | N above thr | TP | FP | FN | TN |
|------|-------------|----|----|----|----|
| Asian | 136 | 102 | 34 | 26 | 42 |
| Black | 704 | 270 | 434 | 171 | 660 |
| Other | 65 | 46 | 19 | 9 | 13 |
| Unknown | 96 | 56 | 40 | 25 | 46 |
| White | 890 | 580 | 310 | 194 | 298 |

## File Paths

| File | Path |
|------|------|
| m048_run_snapshot.json | `studies/m048_racial_disparities_tirads/m048_run_snapshot.json` |
| m048_qa_gates.csv | `studies/m048_racial_disparities_tirads/m048_qa_gates.csv` |
| m048_diagnostic_performance.csv | `studies/m048_racial_disparities_tirads/m048_diagnostic_performance.csv` |
| m048_rom_by_race_x_tr.csv | `studies/m048_racial_disparities_tirads/m048_rom_by_race_x_tr.csv` |
| m048_auc_by_race.csv | `studies/m048_racial_disparities_tirads/m048_auc_by_race.csv` |
| m048_threshold_metrics.csv | `studies/m048_racial_disparities_tirads/m048_threshold_metrics.csv` |
| m048_feature_distribution.csv | `studies/m048_racial_disparities_tirads/m048_feature_distribution.csv` |
| m048_feature_chi_square.csv | `studies/m048_racial_disparities_tirads/m048_feature_chi_square.csv` |
| m048_fna_compliance_by_race.csv | `studies/m048_racial_disparities_tirads/m048_fna_compliance_by_race.csv` |
| m048_bethesda_x_race_x_tr.csv | `studies/m048_racial_disparities_tirads/m048_bethesda_x_race_x_tr.csv` |
| m048_inflation_by_race.csv | `studies/m048_racial_disparities_tirads/m048_inflation_by_race.csv` |
| verification/m025_reconciliation.csv | `studies/m048_racial_disparities_tirads/verification/m025_reconciliation.csv` |
| verification/independent_recompute_results.csv | `studies/m048_racial_disparities_tirads/verification/independent_recompute_results.csv` |
| verification/cortex_smoke_tests.md | `studies/m048_racial_disparities_tirads/verification/cortex_smoke_tests.md` |
| Figure_1_Cohort_Flow_by_Race.png | `M048_submission_package/figures/Figure_1_Cohort_Flow_by_Race.png` |
| Figure_2_ROC_by_Race.png | `M048_submission_package/figures/Figure_2_ROC_by_Race.png` |
| Figure_3_ROM_by_Race_Patient.png | `M048_submission_package/figures/Figure_3_ROM_by_Race_Patient.png` |
| Figure_3b_ROM_by_Race_Nodule.png | `M048_submission_package/figures/Figure_3b_ROM_by_Race_Nodule.png` |
| Figure_4_Inflation_by_Race.png | `M048_submission_package/figures/Figure_4_Inflation_by_Race.png` |
| Figure_5_Feature_Distribution.png | `M048_submission_package/figures/Figure_5_Feature_Distribution.png` |
| Figure_S1_Bethesda_x_Race_x_TR.png | `M048_submission_package/figures/Figure_S1_Bethesda_x_Race_x_TR.png` |

## Mandatory Caveats (Writer Must Acknowledge)

1. **Asian stratum power:** n=204 patients; AUC CIs are wide. Flag any Asian stratum result where the bootstrap 95% CI includes 0.5.
2. **Race is self-reported** from EHR at time of clinical encounter. Use standard disclosure language (e.g., 'self-reported race').
3. **Multiple comparisons across per-TR ROM comparisons:** Per-race per-TR ROM comparisons are descriptive and not formally corrected for multiple testing. The Bonferroni correction applies only to the 5 feature-distribution chi-square tests.
4. **Observational cohort bias:** This is a surgical cohort enriched for suspicious nodules. Absolute ROM values are higher than screening populations.
5. **Feature score completeness:** Chi-square tests restricted to strict-eligible nodules with complete feature component data. Missing data pattern should be described in Methods.
