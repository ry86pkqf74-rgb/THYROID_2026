# Cursor Composer Dispatch — mig_274: M044 figure render pipeline (CSVs → PNG/SVG submission package)

**Generated:** 2026-05-02 by Cowork.
**Lane:** mig_274 — Take the 2 manuscript-figure data CSVs (KM curves + Forest plot) and render publication-quality PNG/SVG with consistent styling. Drop into `M044_submission_package_v1_0/06_figures/`. Reuse existing M044 figure-build patterns from `scripts/m044_make_figures.py` if applicable.
**Recommended agent:** **Cursor Composer**.
**Estimated runtime:** 60–90 min (figure tweaks + Logan review)
**Triggered by:** Round 9 KM/Forest data + manuscript completion roadmap.
**Severity:** MED. M044 needs Figure 2 (KM by ETE strata) + Figure 3 (Forest plot of Cox HRs).

---

## §0 — First message to paste into Cursor Composer

> mig_274 dispatch. Two CSVs in `snowflake_trial/reports/`: `m044_km_curves_data.csv` (KM survival per ETE strata at 8 timepoints) and `m044_forest_plot_data.csv` (10 predictors with HR + 95% CI). Render publication-ready PNG (300 DPI) + SVG using matplotlib, drop into `M044_submission_package_v1_0/06_figures/`. Match the visual style of the existing M044 figures already in that folder.

---

## §1 — Files to produce

```
M044_submission_package_v1_0/06_figures/m044_fig2_km_by_ete.png
M044_submission_package_v1_0/06_figures/m044_fig2_km_by_ete.svg
M044_submission_package_v1_0/06_figures/m044_fig3_forest_cox_multivariable.png
M044_submission_package_v1_0/06_figures/m044_fig3_forest_cox_multivariable.svg
```

Plus the producing script (idempotent, re-runnable from CSV inputs):
```
scripts/m044_render_figures_round10.py
```

## §2 — Figure 2 spec (KM curves by ETE strata)

- X-axis: time in years (0-15)
- Y-axis: Recurrence-free survival probability (0-1)
- 3 lines: ETE none / microscopic / gross
- Shaded 95% CI bands per line
- Number-at-risk table beneath the chart at 0/2/5/10 years (per CONSORT)
- Log-rank p-value annotation (=0.001 from m044_cox_ph.md log-rank test)
- Strata sizes in legend: none n=106 / micro n=1,621 / gross n=979

## §3 — Figure 3 spec (Forest plot of Cox multivariable)

- 10 predictors on Y-axis (one row each); Cowork ordered them sensibly
- X-axis: HR on log scale, range 0.1 to 10
- Diamond/box at HR with horizontal whiskers for 95% CI
- Vertical reference line at HR=1.0
- Right-side annotation column: HR (95% CI), p-value
- Color cells: predictors with p<0.05 in bold/colored, rest neutral

## §4 — Methodology defaults

```python
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.size'] = 11

# KM curves
km = pd.read_csv("snowflake_trial/reports/m044_km_curves_data.csv")
fig, ax = plt.subplots(figsize=(7, 5.5))
for ete in ['none', 'microscopic', 'gross']:
    sub = km[km['ete_strata'] == ete]
    ax.step(sub['time_years'], sub['survival'], where='post', label=f"{ete} (n={int(sub.iloc[0]['n_total'])})")
    ax.fill_between(sub['time_years'], sub['ci_lo'], sub['ci_hi'], step='post', alpha=0.15)
# (more polish — number-at-risk table, log-rank annotation, axis labels, legend)

# Forest plot
fp = pd.read_csv("snowflake_trial/reports/m044_forest_plot_data.csv")
fig, ax = plt.subplots(figsize=(8, 6))
y = range(len(fp))
ax.errorbar(fp['hr'], y, xerr=[fp['hr']-fp['ci_lo'], fp['ci_hi']-fp['hr']],
            fmt='o', color='black', ecolor='black', markersize=8)
ax.set_xscale('log')
ax.axvline(1.0, color='red', linestyle='--', alpha=0.5)
ax.set_yticks(list(y))
ax.set_yticklabels(fp['predictor'])
ax.invert_yaxis()
ax.set_xlabel('Hazard Ratio (95% CI, log scale)')
```

## §5 — Verify

```bash
ls -la M044_submission_package_v1_0/06_figures/m044_fig{2,3}*.png
# Open and eyeball — should be publication-quality, 300 DPI, styled consistent with prior M044 figures
```

## §6 — Surgical git add
```
scripts/m044_render_figures_round10.py
M044_submission_package_v1_0/06_figures/m044_fig2_*
M044_submission_package_v1_0/06_figures/m044_fig3_*
```
