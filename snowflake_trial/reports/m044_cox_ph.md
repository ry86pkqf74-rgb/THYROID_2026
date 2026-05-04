# Manuscript M044 — Cox Proportional Hazards: Time-to-Recurrence by ETE
**Generated:** 2026-05-03 22:54:35
**Cohort:** 2,481 malignant patients (ETE in none/micro/gross), 349 recurrence events
**Method:** lifelines CoxPHFitter; events = any_recurrence_flag (post-mig_255 hybrid B′/A′)

_Note: Time-to-event uses time_to_recurrence_days for events, followup_years*365.25 for censored. mig_255 cleared 740 flag/timing mismatches; ln_status_source not yet integrated as covariate (would tighten LN N1 measurement)._

## Cox PH multivariable

| Predictor | HR | 95% CI | p |
| --- | --- | --- | --- |
| Age (per year) | 0.99 | (0.99–1.00) | 0.0176 |
| Male sex | 1.22 | (0.96–1.54) | 0.0985 |
| ETE microscopic (vs none) | 0.98 | (0.49–1.99) | 0.9640 |
| ETE gross (vs none) | 1.10 | (0.51–2.38) | 0.8118 |
| T3-T4 (vs T1-T2) | 1.05 | (0.67–1.65) | 0.8346 |
| N1 (vs N0) | 1.35 | (1.06–1.71) | 0.0166 |
| BRAF positive | 1.23 | (0.83–1.82) | 0.2983 |
| Total thyroidectomy | 0.33 | (0.27–0.41) | <0.0001 |
| RAI received | 1.62 | (1.26–2.07) | 0.0001 |
| Tumor size (per cm) | 1.12 | (1.06–1.18) | <0.0001 |

**Concordance index:** 0.717; AIC = 5035.6; partial log-lik = -2507.8

## Log-rank test across ETE strata

- Statistic: 13.10
- p-value: 0.0014

## Kaplan-Meier per ETE stratum

| ETE | n | events | median followup (yr) | 5-yr surv | 10-yr surv |
| --- | --- | --- | --- | --- | --- |
| none | 105 | 21 | 2.4 | 0.729 | 0.659 |
| microscopic | 1,518 | 183 | 2.3 | 0.862 | 0.843 |
| gross | 943 | 158 | 2.4 | 0.806 | 0.700 |

## Methods

- **Outcome:** any_recurrence_flag (post-mig_255 hybrid B′/A′ disposition; path_proven flips applied)
- **Time:** time_to_recurrence_days for events, followup_years*365.25 for censored
- **Covariates:** age, sex, ETE strata, T-stage, N-stage, BRAF, surgery type, RAI, tumor size
- **Software:** lifelines.CoxPHFitter (semiparametric Cox model)
- **Caveats:** complete-case; no time-varying covariates; no competing risks adjustment; ln_status_source filter not applied (would tighten N-stage measurement to ln_status_source='both' subset)
