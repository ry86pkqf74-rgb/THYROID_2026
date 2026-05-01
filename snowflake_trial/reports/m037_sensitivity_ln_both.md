# M037 Sensitivity Analysis — ln_status_source='both' Subset
**Generated:** 2026-05-01 18:20:04
**Cohort:** Malignant + ln_status_source='both' (n=2,628; LN+ = 1,126; 42.8%)
**Comparison:** Re-fit M037 Table 2 logreg on the cleanest LN-positivity subset (concordant N-stage AND structured count).
Use this to confirm or refute the full-cohort Table 2 conclusions.

## Univariable

| Predictor | n | OR | 95% CI | p |
| --- | --- | --- | --- | --- |
| Age (per year) | 2,628 | 0.98 | (0.98–0.99) | <0.0001 |
| Male sex | 2,628 | 1.72 | (1.45–2.05) | <0.0001 |
| Tumor size (per cm) | 2,511 | 1.12 | (1.07–1.17) | <0.0001 |
| Non-PTC (vs PTC) | 2,628 | 0.64 | (0.53–0.76) | <0.0001 |
| T3-4 (vs T1-T2) | 2,628 | 2.35 | (2.00–2.76) | <0.0001 |
| Any ETE (vs none) | 2,628 | 0.88 | (0.66–1.18) | 0.4004 |
| BRAF positive | 2,628 | 1.34 | (0.97–1.85) | 0.0751 |
| Total thyroidectomy | 2,628 | 3.15 | (2.58–3.84) | <0.0001 |

## Multivariable (n=2,511; pseudo R²=0.134; log-lik=-1477.2)

| Predictor | aOR | 95% CI | p |
| --- | --- | --- | --- |
| Age (per year) | 0.98 | (0.98–0.99) | <0.0001 |
| Male sex | 1.83 | (1.51–2.23) | <0.0001 |
| Tumor size (per cm) | 1.08 | (1.02–1.14) | 0.0065 |
| Non-PTC (vs PTC) | 0.46 | (0.36–0.58) | <0.0001 |
| T3-4 (vs T1-T2) | 2.30 | (1.89–2.80) | <0.0001 |
| Any ETE (vs none) | 1.44 | (0.84–2.44) | 0.1817 |
| BRAF positive | 1.28 | (0.89–1.83) | 0.1794 |
| Total thyroidectomy | 4.19 | (3.27–5.37) | <0.0001 |

## Comparison vs full-cohort Table 2

Full cohort (n=4,137; LN+=1,126; 27.2%) vs `ln_status_source='both'` (n=2,628; LN+=1,126; 42.8%):

Full-cohort multivariable highlighted: BRAF aOR=1.23 (NS), Non-PTC aOR=0.34, T3-4 aOR=1.38, Total thyroidectomy aOR=2.64.
Restricted-cohort findings should be compared row-by-row above. Direction-preserving + same-significance = robust effect; sign-flip or significance loss = full-cohort artifact.

## Methods

- Outcome: LN_TOTAL_POSITIVE > 0 (count-based; cleaner than the LN_POSITIVE_FLAG which mixes staging+count)
- Cohort: malignant + ln_status_source='both' (per mig_258/259 reconciliation)
- Method: statsmodels Logit
- Caveat: smaller subset means wider CIs; restricted cohort may bias toward better-LN-counted patients (selection)
