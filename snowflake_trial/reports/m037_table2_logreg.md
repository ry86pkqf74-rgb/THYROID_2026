# Table 2 — Manuscript M037: Logistic Regression Predictors of Lymph-Node Positivity
**Generated:** 2026-05-01 17:50:56
**Cohort:** 4,137 malignant patients (LN+ = 1,126; 27.2%)
**Source:** THYROID_VALIDATION.PUBLIC.COHORT_M037_LN_PREDICTORS
**Method:** statsmodels Logit; ORs with 95% CI (Wald)

_Note: Prior M037 Table 1 found BRAF was identical between LN+ and LN- (6.9% in both, p=1.00). Multivariable adjustment may surface effects masked at univariable._

## Univariable

| Predictor | n | OR | 95% CI | p |
| --- | --- | --- | --- | --- |
| Age (per year) | 4,137 | 0.98 | (0.98–0.98) | <0.0001 |
| Male sex (vs female) | 4,137 | 1.64 | (1.41–1.90) | <0.0001 |
| Tumor size (per cm) | 3,993 | 1.09 | (1.05–1.13) | <0.0001 |
| Non-PTC histology (vs PTC) | 4,137 | 0.71 | (0.60–0.84) | <0.0001 |
| T3-4 (vs T1-T2) | 4,137 | 2.05 | (1.78–2.35) | <0.0001 |
| Any ETE (vs none) | 4,137 | 0.64 | (0.50–0.84) | 0.0010 |
| BRAF positive (vs neg) | 4,137 | 1.00 | (0.76–1.31) | 0.9874 |
| Total thyroidectomy (vs partial) | 4,137 | 3.87 | (3.23–4.65) | <0.0001 |
| LN examined (per node) | 3,951 | 1.26 | (1.24–1.29) | <0.0001 |

## Multivariable (adjusted for all listed predictors)

**N = 3,836; pseudo R² = 0.426; log-lik = -1284.7**

| Predictor | aOR | 95% CI | p |
| --- | --- | --- | --- |
| Age (per year) | 0.98 | (0.97–0.99) | <0.0001 |
| Male sex (vs female) | 1.63 | (1.31–2.05) | <0.0001 |
| Tumor size (per cm) | 1.05 | (0.99–1.12) | 0.0946 |
| Non-PTC histology (vs PTC) | 0.34 | (0.25–0.47) | <0.0001 |
| T3-4 (vs T1-T2) | 1.38 | (1.11–1.73) | 0.0044 |
| Any ETE (vs none) | 0.78 | (0.41–1.51) | 0.4631 |
| BRAF positive (vs neg) | 1.23 | (0.86–1.77) | 0.2563 |
| Total thyroidectomy (vs partial) | 2.64 | (2.01–3.46) | <0.0001 |
| LN examined (per node) | 1.23 | (1.21–1.26) | <0.0001 |

**Constant (intercept):** β = -1.918, OR = 0.147

## Methods

- **Outcome:** `LN_POSITIVE` from `COHORT_M037_LN_PREDICTORS` (= LN_POSITIVE_FLAG=1 OR LN_TOTAL_POSITIVE>0; per mig_258/259 caveat, manuscripts requiring numeric LN positivity should restrict to `ln_status_source='both'`).
- **Predictors:** continuous (age, tumor size, LN examined) entered linearly. Categorical reference levels: female, PTC histology, T1a-T2 (vs T3-T4), no ETE, BRAF negative, partial thyroidectomy.
- **Method:** Maximum likelihood logistic regression (statsmodels Logit). Wald CI for ORs.
- **Caveat:** rows with any predictor NULL excluded from multivariable fit (complete-case). Sensitivity analysis with multiple imputation recommended for publication.
