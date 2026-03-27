# Figure legends — submission v2 (2026-03-26, publication-ready redraw)

**Canonical for journal upload:** use this file (v2) + **`fig1_cohort_flow_publication.*`** + **`fig2_forest_primary_publication.*`**. See v1 only for legacy pipeline raster documentation.

## Submission disposition (main text)

| Filename | Role | Resolution |
|----------|------|------------|
| `fig1_cohort_flow_publication.png` | **Figure 1** (main) — CONSORT-style cohort flow | 300 DPI, RGB |
| `fig1_cohort_flow_publication.pdf` | **Figure 1** — vector/press-ready | PDF |
| `fig2_forest_primary_publication.png` | **Figure 2** (main) — forest plot, primary parsimonious model | 300 DPI, RGB |
| `fig2_forest_primary_publication.pdf` | **Figure 2** — vector/press-ready | PDF |

## Do NOT submit (retained for internal reference only)

| File | Reason |
|------|--------|
| `fig_cohort_flow.png` | Legacy bar-chart export (150 DPI, horizontal bars, truncated labels). Replaced by `fig1_cohort_flow_publication.*`. |
| `fig_forest_total_vs_lobectomy.png` | Legacy forest plot (150 DPI, 902×327 px, raw axis title "Forest: primary_parsimonious"). Replaced by `fig2_forest_primary_publication.*`. |
| `fig_completion_rates.png` | OED pipeline (**0/238** ever) vs path-synoptic definite (**25/238** ever; windowed counts) from `table7_completion_thyroidectomy.csv`. Optional supplemental figure. |
| `fig_molecular_result_by_extent.png` | Exploratory; tiny denominators by molecular class. Do not submit. |
| `fig_platform_specific_extent.png` | Overlapping x-axis labels; descriptive n=20 context only. Do not submit. |
| `fig_bethesda_by_extent.png` | Redundant with Table 1. Do not submit. |
| `fig_initial_to_ultimate_extent.png` | Descriptive transition counts; redundant with text. Do not submit. |

---

## Figure 1. Study cohort flow

**File:** `fig1_cohort_flow_publication.png` / `.pdf`

**Legend:** Cohort selection cascade for the index surgical thyroid cohort (2000–2024), structured as a CONSORT-style flow diagram. Beginning from 9,368 patients who underwent a first thyroid procedure, successive exclusion steps identified 8,370 patients with hemithyroidectomy or total thyroidectomy; of these, 635 had a pre-operative imaging nodule size of 2.0–4.0 cm (broad cohort). Two analytic cohorts were derived: the **primary cohort** (N = 558) after strict pre-operative lymph node exclusion, used for all primary analyses; and the **broad cohort** (N = 635) after broad suspicious-node exclusion, used as a robustness check. A planned sensitivity arm using pathology-defined nodule size yielded **N = 0** throughout—no patients in this freeze met the pathology-size criterion—and this arm is displayed explicitly with N = 0 to maintain transparency of the analysis plan. Counts derive from `cohort_flow.csv` and are consistent with `analysis_manifest.json` (run UTC 2026-03-26).

---

## Figure 2. Adjusted odds ratios — primary parsimonious multivariable model

**File:** `fig2_forest_primary_publication.png` / `.pdf`

**Legend:** Forest plot of adjusted odds ratios (OR) with 95% confidence intervals (CI) for the primary outcome of **initial total thyroidectomy** (vs. lobectomy) in the primary cohort (N = 558). Predictors shown are: **age at surgery** (continuous, per year), **female sex** (reference: male), **Bethesda category ≥ 4** (reference: Bethesda < 4; missing Bethesda treated as not ≥ 4 per pre-specified rule), and **any pre-operative molecular testing** (binary). Estimates derive from `logistic_primary_parsimonious.csv`. Horizontal bars indicate 95% CIs; diamonds indicate point estimates. The vertical dashed reference line is drawn at OR = 1. Bold estimates and red p-values denote variables reaching nominal significance (p < 0.05). Bethesda ≥ 4 was the only statistically significant predictor (OR 2.74, 95% CI 1.81–4.15, p < 0.001). Age at surgery showed a modest inverse association (OR 0.986 per year, 95% CI 0.975–0.998, p = 0.026). Female sex and any molecular testing were not significant (p = 0.905 and p = 0.295, respectively). OR = odds ratio; CI = confidence interval.

---

## Tables

Primary numeric displays remain tabular as listed in `manuscript_submission_v1.md` (Tables 1–6).

---

## Supplemental figure (optional, not exported)

The extended-model forest plot (predictors in `logistic_primary_extended.csv`, including bilateral nodule indicator and TIRADS score) was not generated in this publication pass; generate from the same pipeline with human approval if required for submission or supplementary materials.
