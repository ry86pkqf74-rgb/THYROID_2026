# M044 Manuscript Figures Data

**Generated:** 2026-05-01 18:34:40

## Files

- `m044_km_curves_data.csv` — Kaplan-Meier survival probabilities per ETE strata at 8 timepoints (0.5/1/2/3/5/7/10/15 years). Columns: ete_strata, n_total, time_years, survival, ci_lo, ci_hi, n_at_risk, cumulative_events. Plot directly with matplotlib/ggplot for Figure 2 of M044 manuscript.

- `m044_forest_plot_data.csv` — Cox PH multivariable HRs with 95% CI for forest plot (Figure 3). Columns: predictor, hr, ci_lo, ci_hi, p_value, log_hr (for plotting on log scale), se.

## Cox model
n = 2,626; events = 496; c-index = 0.717; AIC = 6829.5

## KM strata sizes
- **ETE none:** n=106, events=32
- **ETE microscopic:** n=1,621, events=273
- **ETE gross:** n=979, events=212
