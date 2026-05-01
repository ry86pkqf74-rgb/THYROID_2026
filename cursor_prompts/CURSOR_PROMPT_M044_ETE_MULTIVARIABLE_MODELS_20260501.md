# Cursor Prompt — M044 ETE Manuscript: Fit Multivariable Models and Populate Tables

**Date:** 2026-05-01
**Manuscript:** M044 — Microscopic vs Gross Extrathyroidal Extension
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Cohort view:** `manuscript_workspace.cohort_m044_ajcc_ete_v1` (n=4,128)
**Recurrence column-of-record:** `main.canonical_recurrence_resolved_v1` (build mig_62 2026-04-27)
**Companion artifacts (already in repo, read these first):**
- `M044_ETE_validation_report.md`
- `M044_ETE_analysis.sql`
- `M044_ETE_analysis_plan.md`
- `M044_ETE_demographics_addendum.md`
- `M044_ETE_manuscript_draft.md`
- `M044_ETE_supplement.md`
- `M044_ETE_tables.xlsx`
- `M044_ETE_claude_handoff_notes.md`

---

## Your job

Fit the pre-specified multivariable models for the M044 manuscript and populate Tables 3, S2–S5, and the forest plot. The validation pass produced reproducible SQL and an analytic-file scaffold; you need to materialize the analytic file from MotherDuck, fit the models in Python (or R), and write the OR/95% CI/p-values back into the Excel workbook and the manuscript draft.

The thesis to evaluate (read `M044_ETE_manuscript_draft.md` for context): gross ETE is associated with substantially higher path-proven recurrence than microscopic ETE under AJCC 8th edition, while microscopic ETE behaves like the no-ETE referent on most measures. The no/negative ETE subgroup is a confounded comparator (lateral-LN-positive, completion-pathway-driven). The previously reported "protective LVI" signal does not reproduce when lymphatic and vascular invasion are kept separate.

---

## Step 1 — Materialize the analytic file from MotherDuck

Run the master analytic-file query in `M044_ETE_analysis.sql` (the WITH-block in §1, "Master analytic table — one row per research_id") against `thyroid_canonical_publication_v1_0` and export to `data/m044/analytic_file_v1.parquet`. Confirm row count = 4,128.

Then run the supplementary CPM pull from `M044_ETE_demographics_addendum.md` and append the following columns to the analytic file (LEFT JOIN on `research_id` from `main.canonical_patient_master`):

```sql
-- additional CPM columns to merge
SELECT
  research_id,
  race,
  bmi_combined,
  multifocal_flag_path,
  bilateral_disease_flag,
  aggressive_variant_flag,
  margin_involved_any,
  closest_margin_mm,
  syn_hashimoto,
  syn_graves,
  pmhx_nlp_diabetes,
  pmhx_nlp_hypertension,
  pmhx_nlp_hypothyroidism,
  pmhx_nlp_obesity,
  braf_positive_final,
  tert_positive_final,
  ras_positive_final,
  ret_positive_unified,
  surg_total_thyroidectomy,
  ages_score
FROM main.canonical_patient_master;
```

Do NOT collapse missing-as-absent for `lvi_clean`, `vasc_clean`, `ajcc8_n_stage`, or `multifocal_flag_path` — retain explicit `missing` categories. Store as `pd.Categorical` with explicit levels.

---

## Step 2 — Primary multivariable model

Outcome: `recurrence_path_proven` (binary, 1=event).
Reference category for ETE group: `Microscopic ETE`.
Excluded from primary fit: `Present ungraded`, `Missing/other` (n=94 total).

Covariates (in order):
- `ete_group` (`No/negative ETE`, `Microscopic ETE` ref, `Gross ETE`)
- `age_at_surgery` (continuous, per 10 years — divide by 10 before fitting)
- `sex` (`female` ref, `male`)
- `tumor_size_cm` (continuous; consider log1p transform if Box-Tidwell suggests non-linearity, otherwise keep linear and document)
- `ajcc8_n_stage` (`N0` ref; `N1a`, `N1b`, `Nx`, `missing`)
- `histology_grouped` (PTC ref; follicular-like, MTC-like, other) — derive `histology_grouped` from `histology_final` using the rules in `M044_ETE_analysis.sql` §2
- `rai_received_flag` (binary)
- `lvi_clean` (`missing` ref; `present`, `extensive`, `focal`, `indeterminate`)
- `vasc_clean` (`missing` ref; `present_ungraded`, `focal`, `extensive`, `indeterminate`)

Fit with Python `statsmodels`:

```python
import pandas as pd
import statsmodels.formula.api as smf

df = pd.read_parquet('data/m044/analytic_file_v1.parquet')
df = df[df['ete_group'].isin(['No/negative ETE','Microscopic ETE','Gross ETE'])].copy()
df['age10'] = df['age_at_surgery'] / 10
df['ete_group'] = pd.Categorical(df['ete_group'],
    categories=['Microscopic ETE','No/negative ETE','Gross ETE'])

m_primary = smf.glm(
    'recurrence_path_proven ~ C(ete_group, Treatment(reference="Microscopic ETE")) '
    '+ age10 + C(sex, Treatment(reference="female")) + tumor_size_cm '
    '+ C(ajcc8_n_stage, Treatment(reference="N0")) '
    '+ C(histology_grouped, Treatment(reference="PTC")) + rai_received_flag '
    '+ C(lvi_clean, Treatment(reference="missing")) '
    '+ C(vasc_clean, Treatment(reference="missing"))',
    data=df, family=sm.families.Binomial()).fit()
print(m_primary.summary())
```

Convert log-odds coefficients to ORs (exponentiate), 95% CIs (exp of `summary().conf_int()`), and Wald p-values. Record n_obs, n_events, AIC, pseudo-R² (McFadden's), and the LR test vs the null model.

---

## Step 3 — Secondary endpoint models

Repeat Step 2 with two alternative outcomes:
- `recurrence_status_final == 'imaging_only_unconfirmed'` (binary).
- Composite: `recurrence_status_final.isin(['path_proven','imaging_only_unconfirmed'])`.

Save coefficient tables to `M044_ETE_tables.xlsx` "Model outputs" tab.

---

## Step 4 — Pre-specified sensitivity analyses

S1. Primary model excluding `followup_years == 0` patients (n drops from 4,034 to ~2,634 in the 3-group model).
S2. Primary model restricted to `surg_first_date BETWEEN '1999-01-01' AND '2024-12-31'`.
S3. Primary model with central- and lateral-LN-positive flags substituted for `ajcc8_n_stage`.
S4. **Pooled-LVI sensitivity (artifact reproduction)** — combine `lvi_clean` and `vasc_clean` into a single binary `lvi_pooled = (lvi_clean == 'present' | 'extensive' | 'focal') | (vasc_clean != 'missing' & vasc_clean != 'indeterminate')`, treating missing as absent. The hypothesis is that this is the configuration that produces the spurious "protective LVI" association reported in earlier modeling. Document whether the pooled coefficient appears protective.
S5. Vascular invasion as ordinal: `missing < indeterminate < focal < present_ungraded < extensive`. Fit using ordered integer or polynomial contrasts.
S6. Drop `ete_grade_final == 'true'` rows (n=4) entirely.
S7. Add CPM-derived covariates as a sensitivity: `multifocal_flag_path`, `bilateral_disease_flag`, `margin_involved_any`, `aggressive_variant_flag`, `braf_positive_final`, `surg_total_thyroidectomy`. Document whether the gross-vs-microscopic ETE coefficient attenuates or strengthens.
S8. Replace primary outcome with legacy `any_recurrence_flag` for transparency. Document the divergence vs the path-proven model.

---

## Step 5 — Cox time-to-event sensitivity

Restrict to patients with `surg_first_date IS NOT NULL AND followup_years > 0` (≈2,300 patients). Define event time as `days_to_path_proven` for events and `followup_years * 365.25` for censored patients. Fit:

```python
from lifelines import CoxPHFitter
cph = CoxPHFitter()
# survival data prep with `time_days`, `event` columns
cph.fit(surv_df, duration_col='time_days', event_col='recurrence_path_proven',
        formula='C(ete_group, ...) + age10 + ... + C(lvi_clean, ...) + C(vasc_clean, ...)')
print(cph.summary)
```

Report HR with 95% CI for the primary contrasts (gross vs microscopic; no/neg vs microscopic). Compare to the logistic OR.

---

## Step 6 — No/negative ETE subgroup model

Restrict to the 192 no/negative ETE patients. Outcome: composite recurrence (path_proven OR imaging_only_unconfirmed; n=29 events). Fit a logistic regression on:

- `tumor_size_cm`
- `ajcc8_n_stage` (N0 ref, N1a, N1b, Nx, missing)
- `central_pos_flag`
- `lateral_pos_flag`
- `rai_received_flag`
- `n_surgeries >= 2` (binary)
- `days_to_2nd` (continuous, per 100 days; missing imputed as 0 with separate `had_2nd_surgery` flag)

Report ORs to confirm whether the lateral-LN and ≥2-surgery effects dominate within this subgroup.

---

## Step 7 — Tumor-size-stratified panel

Compute path-proven recurrence rate for each (ETE group × size bin) cell using `<=1`, `1.1–2`, `2.1–4`, `>4` cm bins. Verify against Supplement Table S1 in `M044_ETE_supplement.md`. Should reproduce:
- Microscopic: 1.1%, 2.7%, 2.3%, 5.6%.
- Gross: 2.6%, 4.1%, 7.0%, 8.6%.
- No/neg: 3.8%, 11.4%, 7.0%, 3.8%.

If any cell disagrees by >0.5%, flag in the "QA" tab of the workbook.

---

## Step 8 — Forest plot (Figure 5)

Create `figures/m044_forest_primary.png` showing the adjusted ORs (95% CI) for all variables in the primary model. Use the `forestplot` Python package or matplotlib. ETE-group coefficients should be highlighted. Save the figure data as `figures/m044_forest_primary_data.csv`.

---

## Step 9 — Update Excel workbook

Open `M044_ETE_tables.xlsx` and:

1. Populate the "Table 3 — Multivariable" tab with primary-model ORs, 95% CIs, and p-values. Replace `'TBD'` placeholders.
2. Populate the "Model outputs" tab with one block per fitted model (primary, secondary 1, secondary 2, S1–S8, Cox, no-neg-subgroup). Each block has: model name, n, n_events, AIC, pseudo-R², coefficient table.
3. Add a "Figures" tab with thumbnail references to figures/m044_*.png and the underlying data CSVs.

Run formula recalculation (`scripts/recalc.py M044_ETE_tables.xlsx`) to confirm zero formula errors.

---

## Step 10 — Update manuscript draft

In `M044_ETE_manuscript_draft.md` Results §"Multivariable analysis", replace the bracketed `[VERIFY]` placeholders with the fitted ORs and 95% CIs from Step 2. Specifically populate:

- The "Crude path-proven OR for gross vs microscopic ETE" sentence.
- The "Crude path-proven OR for no/negative vs microscopic ETE" sentence.
- The full Multivariable analysis paragraph (replace TBD with fitted values).
- The Discussion paragraph 1 fold the adjusted-OR result back in.

Also append a sub-paragraph documenting whether the S4 pooled-LVI sensitivity reproduces a protective coefficient (which is the explicit hypothesis test for the prior literature artifact).

---

## Step 11 — Update SQL package

If you find any SQL bugs while running, open `M044_ETE_analysis.sql` and patch them. Re-export the analytic file and re-fit. Document each patch in a brief commit message.

---

## Step 12 — Stage, commit, push

```bash
git add M044_ETE_*.md M044_ETE_*.xlsx M044_ETE_*.sql data/m044/ figures/m044_*
git commit -m "manuscript(M044): fit primary multivariable models, populate Table 3, sensitivity analyses, forest plot

- Primary logistic regression of path-proven recurrence on ETE group + covariates
- Secondary endpoints: imaging-only-unconfirmed, composite
- Sensitivity panel: zero-FU exclusion, surgery-date restriction, LN-flag substitution,
  pooled-LVI artifact reproduction, ordinal vascular, ETE='true' exclusion, CPM-augmented model,
  legacy any_recurrence transparency model
- Cox time-to-event on surgery-date-known subset
- No/negative ETE subgroup model with reoperative covariates
- Tumor-size-stratified panel reproduction check
- Forest plot for primary model
- Excel workbook: Table 3 populated, Model outputs tab built, QA tab updated"
git push origin main
```

---

## Constraints and conventions

- **Do not collapse missing into absent** for any LVI / vascular / N-stage covariate.
- **Do not use** `any_recurrence_flag` or `structural_recurrence_flag` as primary endpoints — only in the explicit S8 transparency model.
- **Use cleaned `lvi_clean` and `vasc_clean`** from `M044_ETE_analysis.sql`, not raw `lvi_grade` / `vascular_invasion_final`.
- **MAX(...) per `research_id`** when joining `ln_master_rollup_v1` and `cohort_m040_reoperative_v1` (one row per patient).
- **Do not invent citations.** All references in the manuscript draft are placeholders pulled from the Elicit report and must be verified in Zotero.
- **Document every assumption** that diverges from the analysis plan with a one-line note in the model-outputs tab.

---

## Acceptance criteria

You are done when:

1. `data/m044/analytic_file_v1.parquet` exists with 4,128 rows.
2. `M044_ETE_tables.xlsx` has Table 3 populated, "Model outputs" tab populated for all pre-specified models, and zero formula errors.
3. `figures/m044_forest_primary.png` exists with paired CSV data.
4. `M044_ETE_manuscript_draft.md` has all `[VERIFY]` placeholders in the Results section replaced with fitted numbers.
5. The S4 pooled-LVI sensitivity result is documented (whether or not a protective coefficient reappears).
6. The Cox model HR for gross-vs-microscopic ETE is reported and compared to the logistic OR.
7. Commit message follows the convention shown in Step 12.
8. Branch is pushed to `origin/main`.

---

## Common pitfalls to avoid

- Recoding `lvi_clean == 'missing'` as the absent reference and dropping the explicit missing category — this reproduces the prior artifact you are trying to debunk. Keep `missing` as its own factor level.
- Using `bmi_combined` as a primary covariate when 80% are missing. Report descriptively only.
- Using `pmhx_nlp_smoking_status` — it is 99.7% NULL with free-text variants; cannot be used.
- Treating `surg_first_date` as available for all patients — 22.1% are missing. Cox model must restrict to date-known.
- Using `cohort_m044_ajcc_ete_v1.ln_total_positive` directly — this view-level value can disagree with the LN rollup. Use the rollup-derived `ln_positive` from the analytic file.
- Forgetting that `ete_grade_final == 'true'` (n=4) is in the cohort by ChatGPT's definition. Either drop or document.

End of cursor prompt.
