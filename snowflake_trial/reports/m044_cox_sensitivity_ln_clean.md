# M044 Cox PH — Sensitivity on cleaner LN cohort (ln_status_source ≠ 'staging')
**Generated:** 2026-05-01 18:26:29
**Cohort:** 1,703 malignant + ETE-graded + ln_status_source ≠ 'staging'; 328 recurrence events
**Comparison:** Same model as `m044_cox_ph.md` but restricted to remove staging-only LN+ patients

## Cox PH multivariable

| Predictor | HR | 95% CI | p |
| --- | --- | --- | --- |
| Age (per year) | 1.00 | (0.99–1.01) | 0.8072 |
| Male sex | 1.03 | (0.80–1.33) | 0.8079 |
| ETE microscopic (vs none) | 1.82 | (0.85–3.86) | 0.1209 |
| ETE gross (vs none) | 2.10 | (0.92–4.81) | 0.0795 |
| T3-T4 | 1.29 | (0.80–2.08) | 0.2932 |
| N1 | 1.52 | (1.19–1.95) | 0.0009 |
| BRAF positive | 1.33 | (0.89–1.98) | 0.1617 |
| Total thyroidectomy | 0.27 | (0.21–0.35) | <0.0001 |
| RAI received | 1.55 | (1.19–2.01) | 0.0010 |
| Tumor size (per cm) | 1.09 | (1.02–1.15) | 0.0063 |

**c-index:** 0.721; AIC = 4195.9; partial log-lik = -2087.9

## Compare vs full M044 (m044_cox_ph.md, n=2,626)

Direction-preserved + same significance = robust. Sign-flip / significance change = full-cohort artifact.
Particular interest: ETE microscopic + gross HR direction; tumor size; total thyroidectomy.
