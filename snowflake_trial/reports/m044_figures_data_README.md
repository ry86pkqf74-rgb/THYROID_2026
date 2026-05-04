# M044 Manuscript Figures Data

**Generated:** 2026-05-03 22:54:31

## Files

- `m044_km_curves_data.csv` — Kaplan-Meier survival probabilities per ETE strata at 8 timepoints (0.5/1/2/3/5/7/10/15 years). Columns: ete_strata, n_total, time_years, survival, ci_lo, ci_hi, n_at_risk, cumulative_events. Plot directly with matplotlib/ggplot for Figure 2 of M044 manuscript.

- `m044_forest_plot_data.csv` — Cox PH multivariable HRs with 95% CI for forest plot (Figure 3). Columns: predictor, hr, ci_lo, ci_hi, p_value, log_hr (for plotting on log scale), se.

## Cox model
n = 2,481; events = 349; c-index = 0.717; AIC = 5035.6

## KM strata sizes
- **ETE none:** n=105, events=21
- **ETE microscopic:** n=1,518, events=183
- **ETE gross:** n=943, events=158
