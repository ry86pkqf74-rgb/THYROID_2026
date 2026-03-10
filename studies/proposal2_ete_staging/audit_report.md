# Statistical Audit Report — Proposal 2 ETE Staging Manuscript

*Generated: 2026-03-10 00:22*

*Cohort: N=3,278 all PTC (expanded)*

*Seed: 42 (all stochastic operations)*

## Audit Findings Summary

- **[CRITICAL] LN_RATIO_QUALITY**: ln_examined is effectively binary for 83% of available values (583/3278). LN ratio acts as a binary variable, not continuous. Verify whether this column represents total nodes examined or a binary indicator.
- **[IMPORTANT] OUTCOME_CIRCULARITY**: 100.0% of gross ETE patients are classified as high risk. The recurrence_risk_band includes gross ETE in its derivation, inflating the gross ETE OR. The mETE OR is the clinically meaningful coefficient.
- **[MINOR] M_STAGE_MISSING**: m_stage_ajcc8 available for only 18.0% of the expanded cohort. Missing M-stage defaulted to M0.
- **[CRITICAL] AJCC7_T3b_MAP**: T3b→T4a mapping affected 346 patients. Corrected to T3b→T3: 346 T-stage and 46 overall-stage reclassifications.
- **[IMPORTANT] PROP_ODDS**: Proportional odds assumption may be violated: largest coefficient difference across cut-points is 8.12 for 'ete_gross'. Consider partial proportional odds model.

## Table 1. Cohort Demographics by ETE Group

| Variable                      | No ETE        | Microscopic ETE   | Gross ETE     | p-value   |
|:------------------------------|:--------------|:------------------|:--------------|:----------|
| N (%)                         | 724 (22.1%)   | 1736 (53.0%)      | 818 (25.0%)   |           |
| Age, mean +/- SD              | 48.0 +/- 15.3 | 50.1 +/- 15.1     | 50.8 +/- 16.0 | <0.001    |
| Age >= 55, n (%)              | 249 (34.4%)   | 693 (39.9%)       | 352 (43.0%)   | 0.002     |
| Female sex, n (%)             | 540 (74.6%)   | 1331 (76.7%)      | 587 (71.8%)   | 0.027     |
| Tumor size (cm), median [IQR] | 2.0 [1.0-4.1] | 2.0 [1.0-4.4]     | 2.0 [1.0-4.0] | 0.077     |
| <=1 cm, n (%)                 | 185 (25.6%)   | 456 (26.3%)       | 230 (28.1%)   | <0.001    |
| 1.1-2 cm, n (%)               | 178 (24.6%)   | 450 (25.9%)       | 224 (27.4%)   |           |
| 2.1-4 cm, n (%)               | 167 (23.1%)   | 334 (19.2%)       | 197 (24.1%)   |           |
| >4 cm, n (%)                  | 185 (25.6%)   | 496 (28.6%)       | 167 (20.4%)   |           |
| T1a, n (%)                    | 186 (25.7%)   | 455 (26.2%)       | 96 (11.7%)    | <0.001    |
| T1b, n (%)                    | 179 (24.7%)   | 450 (25.9%)       | 65 (7.9%)     |           |
| T2, n (%)                     | 165 (22.8%)   | 336 (19.4%)       | 47 (5.7%)     |           |
| T3a, n (%)                    | 185 (25.6%)   | 495 (28.5%)       | 14 (1.7%)     |           |
| T3b, n (%)                    | 0 (0.0%)      | 0 (0.0%)          | 346 (42.3%)   |           |
| T4a, n (%)                    | 0 (0.0%)      | 0 (0.0%)          | 250 (30.6%)   |           |
| Stage I, n (%)                | 545 (75.3%)   | 1198 (69.0%)      | 464 (56.7%)   | <0.001    |
| Stage II, n (%)               | 170 (23.5%)   | 535 (30.8%)       | 241 (29.5%)   |           |
| Stage III, n (%)              | 0 (0.0%)      | 0 (0.0%)          | 107 (13.1%)   |           |
| Stage IVB, n (%)              | 0 (0.0%)      | 3 (0.2%)          | 6 (0.7%)      |           |
| N1 (any), n (%)               | 412 (56.9%)   | 1166 (67.2%)      | 611 (74.7%)   | <0.001    |
| Risk: low, n (%)              | 368 (50.8%)   | 992 (57.1%)       | 0 (0.0%)      | <0.001    |
| Risk: intermediate, n (%)     | 210 (29.0%)   | 547 (31.5%)       | 0 (0.0%)      |           |
| Risk: high, n (%)             | 146 (20.2%)   | 197 (11.3%)       | 818 (100.0%)  |           |

## Stage Migration (Corrected AJCC 7th Derivation)

- mETE T-stage downstaged: 1241/1736 (71.5%)
- Overall downstaged: 1872/3269 (57.3%)
- Upstaged: 12
- McNemar (stage>=III): stat=0.0, p=<0.001

### Impact of T3b Mapping Correction

- Original analysis mapped AJCC 8th T3b -> AJCC 7th T4a (incorrect; strap muscle = T3 in AJCC 7th)
- Correction: T3b -> T3 (AJCC 7th). Affected 346 patients.
- T-stage reclassified: 346, Overall stage reclassified: 46
- Original downstaging: 57.3% | Corrected: 57.3%

### Migration Cross-Tabulation (Corrected)

| overall_stage_ajcc7   |    I |   II |   III |   IVB |   All |
|:----------------------|-----:|-----:|------:|------:|------:|
| I                     | 1384 |    3 |     0 |     0 |  1387 |
| II                    |   35 |    1 |     0 |     0 |    36 |
| III                   |  427 |  328 |     0 |     1 |   756 |
| IVA                   |  361 |  614 |   107 |     8 |  1090 |
| All                   | 2207 |  946 |   107 |     9 |  3269 |

## Ordinal Logistic Regression

| Variable         |          OR | 95% CI      | p-value   |
|:-----------------|------------:|:------------|:----------|
| ete_micro        | 0.6         | (0.51-0.72) | <0.001    |
| ete_gross        | 4.18529e+09 | (0.00-inf)  | 0.979     |
| age_at_surgery   | 1.05        | (1.04-1.06) | <0.001    |
| female           | 0.83        | (0.69-1.00) | 0.049     |
| largest_tumor_cm | 1.07        | (1.03-1.10) | <0.001    |
| ln_ratio         | 1.31        | (1.02-1.67) | 0.032     |

### Proportional Odds Diagnostic

Binary logistic coefficients at each cut-point:

| Variable | low\|mid+high | low+mid\|high | Difference |
|----------|-------------|-------------|------------|
| age_at_surgery | 0.068 | 0.004 | 0.064 |
| ete_gross | 28.418 | 20.297 | 8.121 |
| ete_micro | -0.468 | -0.702 | 0.234 |
| female | -0.233 | -0.138 | 0.095 |
| largest_tumor_cm | 0.103 | 0.018 | 0.085 |
| ln_ratio | 0.602 | -0.360 | 0.962 |

## Model Discrimination (AUC)

| Model | Apparent AUC | CV AUC (5-fold) | Optimism |
|-------|-------------|-----------------|----------|
| Base | 0.8611 | 0.8510 +/- 0.0196 | 0.0101 |
| Full | 0.8791 | 0.8762 +/- 0.0096 | 0.0029 |

delta AUC (mETE contribution): Apparent=0.0180, CV=0.0252

## Sensitivity Analyses

| Subgroup                 |   mETE OR | 95% CI      | p-value   |
|:-------------------------|----------:|:------------|:----------|
| Primary (CC, expanded)   |      0.6  | (0.51-0.72) | <0.001    |
| MI (m=20, Rubin's rules) |      0.6  | (0.50-0.72) | <0.001    |
| Age >= 55                |      0.87 | (0.64-1.17) | 0.352     |
| Age < 55                 |      0.44 | (0.35-0.56) | <0.001    |
| Tumor <= 4 cm            |      0.68 | (0.56-0.84) | <0.001    |
| Original classic (N=596) |      0.44 | (0.29-0.65) | <0.001    |

## Data Quality Flags

- ln_examined_available: 583
- ln_examined_binary_pct: 83.2
- gross_ete_high_risk_pct: 100.0
- m_stage_available: 589
- m_stage_pct: 18.0
