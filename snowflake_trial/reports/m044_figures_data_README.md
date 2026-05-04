# M044 Manuscript Figures Data

**Generated:** 2026-05-03 22:50:27

## Files

- `m044_km_curves_data.csv` — Kaplan-Meier survival probabilities per ETE strata at 8 timepoints (0.5/1/2/3/5/7/10/15 years). Columns: ete_strata, n_total, time_years, survival, ci_lo, ci_hi, n_at_risk, cumulative_events. Plot directly with matplotlib/ggplot for Figure 2 of M044 manuscript.

- `m044_forest_plot_data.csv` — Cox PH multivariable HRs with 95% CI for forest plot (Figure 3). Columns: predictor, hr, ci_lo, ci_hi, p_value, log_hr (for plotting on log scale), se.

## Cox model
n = 2,598; events = 493; c-index = 0.719; AIC = 6776.4

## KM strata sizes
- **ETE none:** n=106, events=32
- **ETE microscopic:** n=1,594, events=271
- **ETE gross:** n=978, events=211
