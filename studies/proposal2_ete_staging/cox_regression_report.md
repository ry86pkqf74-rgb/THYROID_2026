# Cox Regression Supplement — Proposal 2

*Generated: 2026-03-10 18:43*

## Cohort
- N = 5,794 patients with valid follow-up (time > 0)
- Events (recurrence): 36 (0.6%)
- Median follow-up: 7.7 years

## Table 3. Cox Proportional Hazards Results

| Variable      | Model        | HR_fmt             | p_fmt   | sig   |
|:--------------|:-------------|:-------------------|:--------|:------|
| Gross ETE     | Univariate   | 11.41 (5.35–24.32) | <0.001  | ***   |
| Micro ETE     | Univariate   | 0.20 (0.07–0.57)   | 0.003   | **    |
| Age ≥55       | Univariate   | 0.48 (0.23–1.00)   | 0.049   | *     |
| Tumor >4 cm   | Univariate   | 1.28 (0.63–2.61)   | 0.489   |       |
| LN ratio (SD) | Univariate   | 0.92 (0.65–1.29)   | 0.617   |       |
| BRAF positive | Univariate   | 0.00 (0.00–inf)    | 0.996   |       |
| Gross ETE     | Multivariate | 1.18 (0.96–1.44)   | 0.112   |       |
| Micro ETE     | Multivariate | 0.94 (0.79–1.11)   | 0.451   |       |
| Age ≥55       | Multivariate | 0.97 (0.82–1.15)   | 0.757   |       |
| Tumor >4 cm   | Multivariate | 1.03 (0.85–1.23)   | 0.792   |       |
| LN ratio (SD) | Multivariate | 0.99 (0.92–1.08)   | 0.906   |       |
| BRAF positive | Multivariate | 0.97 (0.37–2.54)   | 0.955   |       |

## Figures
- **Figure 10:** KM — recurrence-free survival by ATA risk band
- **Figure 11:** KM — recurrence-free survival by ETE status
- **Figure 12:** Forest plot — multivariate Cox HR

## Key Findings

- **Gross ETE**: HR 1.18 (0.96–1.44), 0.112
- **Micro ETE**: HR 0.94 (0.79–1.11), 0.451
- Low-risk patients: zero recurrence events (censored follow-up)
- Gross ETE drives survival separation in log-rank tests

Supports the AJCC 8th edition decision to exclude mETE from T-staging.
