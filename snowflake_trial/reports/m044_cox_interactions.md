# M044 Cox PH — Interaction Terms
**Generated:** 2026-05-01 19:32:04
Tests whether ETE's effect on recurrence varies by T-stage, N-stage, or tumor size.
Comparison vs base model (`m044_cox_ph.md`): if interaction term significant, ETE's effect depends on the moderator.

## Model — base

**N = 2,620; events = 496; c-index = 0.717; AIC = 6827.5**

| Predictor | HR | 95% CI | p |
| --- | --- | --- | --- |
| AGE_AT_SURGERY | 0.99 | (0.99–1.00) | 0.0277 |
| SEX_MALE | 1.12 | (0.91–1.37) | 0.2923 |
| ETE_MICRO | 1.15 | (0.65–2.05) | 0.6279 |
| ETE_GROSS | 1.34 | (0.70–2.54) | 0.3754 |
| T_HIGH | 1.04 | (0.73–1.50) | 0.8152 |
| N_POS | 1.11 | (0.91–1.35) | 0.3101 |
| BRAF | 1.20 | (0.87–1.66) | 0.2707 |
| SURG_TOTAL | 0.36 | (0.30–0.43) | <0.0001 |
| RAI | 1.49 | (1.19–1.85) | 0.0004 |
| TUMOR_SIZE_CM_MAX | 1.13 | (1.08–1.18) | <0.0001 |

## Model — ete_x_thigh

**N = 2,620; events = 496; c-index = 0.717; AIC = 6837.7**

| Predictor | HR | 95% CI | p |
| --- | --- | --- | --- |
| AGE_AT_SURGERY | 0.99 | (0.99–1.00) | 0.0345 |
| SEX_MALE | 1.11 | (0.91–1.35) | 0.3033 |
| ETE_MICRO | 1.06 | (0.69–1.62) | 0.7926 |
| ETE_GROSS | 1.10 | (0.60–2.02) | 0.7655 |
| T_HIGH | 1.07 | (0.78–1.47) | 0.6684 |
| N_POS | 1.10 | (0.91–1.33) | 0.3280 |
| BRAF | 1.19 | (0.87–1.64) | 0.2826 |
| SURG_TOTAL | 0.38 | (0.31–0.45) | <0.0001 |
| RAI | 1.46 | (1.18–1.81) | 0.0005 |
| TUMOR_SIZE_CM_MAX | 1.12 | (1.07–1.17) | <0.0001 |
| ETE_X_THIGH | 1.10 | (0.60–2.02) | 0.7655 |

## Model — ete_x_npos

**N = 2,620; events = 496; c-index = 0.717; AIC = 6837.6**

| Predictor | HR | 95% CI | p |
| --- | --- | --- | --- |
| AGE_AT_SURGERY | 0.99 | (0.99–1.00) | 0.0338 |
| SEX_MALE | 1.11 | (0.91–1.35) | 0.3079 |
| ETE_MICRO | 1.03 | (0.68–1.55) | 0.8911 |
| ETE_GROSS | 1.21 | (0.75–1.94) | 0.4381 |
| T_HIGH | 1.09 | (0.79–1.49) | 0.6096 |
| N_POS | 1.12 | (0.90–1.40) | 0.3088 |
| BRAF | 1.19 | (0.87–1.64) | 0.2789 |
| SURG_TOTAL | 0.38 | (0.31–0.45) | <0.0001 |
| RAI | 1.46 | (1.18–1.81) | 0.0005 |
| TUMOR_SIZE_CM_MAX | 1.12 | (1.07–1.17) | <0.0001 |
| ETE_X_NPOS | 0.94 | (0.66–1.34) | 0.7322 |

## Model — ete_x_large

**N = 2,620; events = 496; c-index = 0.714; AIC = 6861.6**

| Predictor | HR | 95% CI | p |
| --- | --- | --- | --- |
| AGE_AT_SURGERY | 1.00 | (0.99–1.00) | 0.0682 |
| SEX_MALE | 1.10 | (0.92–1.31) | 0.3074 |
| ETE_MICRO | 0.97 | (0.75–1.25) | 0.8112 |
| ETE_GROSS | 1.09 | (0.83–1.43) | 0.5440 |
| T_HIGH | 1.05 | (0.82–1.34) | 0.6930 |
| N_POS | 1.08 | (0.91–1.29) | 0.3605 |
| BRAF | 1.16 | (0.87–1.56) | 0.3155 |
| SURG_TOTAL | 0.44 | (0.37–0.52) | <0.0001 |
| RAI | 1.39 | (1.14–1.69) | 0.0013 |
| TUMOR_SIZE_CM_MAX | 1.07 | (1.02–1.12) | 0.0086 |
| LARGE_TUMOR | 1.28 | (0.97–1.70) | 0.0864 |
| ETE_X_LARGE | 1.04 | (0.76–1.43) | 0.8066 |

## AIC comparison

| Model | n | events | AIC | c-index |
| --- | --- | --- | --- | --- |
| base | 2,620 | 496 | 6827.5 | 0.717 |
| ete_x_thigh | 2,620 | 496 | 6837.7 | 0.717 |
| ete_x_npos | 2,620 | 496 | 6837.6 | 0.717 |
| ete_x_large | 2,620 | 496 | 6861.6 | 0.714 |

Lower AIC = better fit. ΔAIC > 2 = meaningful improvement; > 10 = strongly preferred.

## Methods

- Same base predictors as `m044_cox_ph.md` Cox model. Each interaction model adds 1 product term.
- LARGE_TUMOR = TUMOR_SIZE_CM_MAX ≥ 4.0 (clinically relevant cutoff per AJCC).
- Interactions of interest: ETE_GROSS × T_HIGH (does gross ETE matter more in T3-4?), ETE_GROSS × N_POS (LN-positive amplifier?), ETE_GROSS × LARGE_TUMOR (size + invasion synergy).
- p-value on interaction term tests H0: no synergy beyond main effects.
