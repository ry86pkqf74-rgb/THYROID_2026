# Manuscript M044 — Cox Proportional Hazards: Time-to-Recurrence by ETE
**Generated:** 2026-05-01 18:17:28
**Cohort:** 2,626 malignant patients (ETE in none/micro/gross), 496 recurrence events
**Method:** lifelines CoxPHFitter; events = any_recurrence_flag (post-mig_255 hybrid B′/A′)

_Note: Time-to-event uses time_to_recurrence_days for events, followup_years*365.25 for censored. mig_255 cleared 740 flag/timing mismatches; ln_status_source not yet integrated as covariate (would tighten LN N1 measurement)._

## Cox PH multivariable

| Predictor | HR | 95% CI | p |
| --- | --- | --- | --- |
| Age (per year) | 0.99 | (0.99–1.00) | 0.0252 |
| Male sex | 1.12 | (0.91–1.38) | 0.2768 |
| ETE microscopic (vs none) | 1.15 | (0.65–2.05) | 0.6355 |
| ETE gross (vs none) | 1.34 | (0.71–2.56) | 0.3668 |
| T3-T4 (vs T1-T2) | 1.04 | (0.72–1.49) | 0.8437 |
| N1 (vs N0) | 1.11 | (0.91–1.35) | 0.2981 |
| BRAF positive | 1.20 | (0.87–1.67) | 0.2662 |
| Total thyroidectomy | 0.36 | (0.30–0.43) | <0.0001 |
| RAI received | 1.49 | (1.20–1.85) | 0.0004 |
| Tumor size (per cm) | 1.13 | (1.08–1.18) | <0.0001 |

**Concordance index:** 0.717; AIC = 6829.5; partial log-lik = -3404.7

## Log-rank test across ETE strata

- Statistic: 13.79
- p-value: 0.0010

## Kaplan-Meier per ETE stratum

| ETE | n | events | median followup (yr) | 5-yr surv | 10-yr surv |
| --- | --- | --- | --- | --- | --- |
| none | 106 | 32 | 2.3 | 0.640 | 0.579 |
| microscopic | 1,621 | 273 | 2.3 | 0.832 | 0.806 |
| gross | 979 | 212 | 2.4 | 0.776 | 0.678 |

## Methods

- **Outcome:** any_recurrence_flag (post-mig_255 hybrid B′/A′ disposition; path_proven flips applied)
- **Time:** time_to_recurrence_days for events, followup_years*365.25 for censored
- **Covariates:** age, sex, ETE strata, T-stage, N-stage, BRAF, surgery type, RAI, tumor size
- **Software:** lifelines.CoxPHFitter (semiparametric Cox model)
- **Caveats:** complete-case; no time-varying covariates; no competing risks adjustment; ln_status_source filter not applied (would tighten N-stage measurement to ln_status_source='both' subset)
