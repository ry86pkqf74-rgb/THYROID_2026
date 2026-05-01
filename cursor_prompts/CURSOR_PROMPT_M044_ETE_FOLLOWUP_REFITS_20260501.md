# Cursor Prompt — M044 ETE Manuscript: Follow-up Model Refits

**Date:** 2026-05-01
**Manuscript:** M044 — Microscopic vs Gross ETE
**Companion:** Read `M044_ETE_manuscript_draft.md` (post-Cursor populated), `M044_ETE_validation_report.md`, `M044_ETE_demographics_addendum.md`, and the previous Cursor prompt `cursor_prompts/CURSOR_PROMPT_M044_ETE_MULTIVARIABLE_MODELS_20260501.md`.

This prompt addresses five issues identified after reviewing the populated Table 3 and forest plot. Direct MotherDuck findings supporting each item are documented in the chat log on 2026-05-01.

---

## Background

The primary multivariable logistic regression of path-proven recurrence on ETE group attenuated the gross-vs-microscopic OR to 1.39 (p=0.098), while the Cox model preserved an HR of 2.10 (p=0.007). Five covariate-related issues are likely contributing to this split. Address them with the refits below and update the manuscript Discussion to reflect the corrected findings.

---

## Refit 1 — Strict-DTC sensitivity (exclude non-DTC, exclude non-cancer)

The current `histology_grouped` covariate lumps anaplastic, MTC, NIFTP, FTUMP, follicular adenoma, and rare other tumors into "other," and the "follicular-like" group includes NIFTP/FTUMP. These categories should be excluded from the primary DTC analysis.

**Build a strict-DTC analytic file** by filtering out:

```python
EXCLUDE_HISTOLOGIES = {
    'MTC', 'metastatic MTC', 'recurrent MTC', 'MTC/PTC mixed composite',
    'anaplastic carcinoma', 'metastatic anaplastic carcinoma',
    'NIFTP', 'FTUMP', 'atypical follicular adenoma', 'follicular adenoma',
    'Atypical hurthle cell neoplasm',
    'NUT carcinoma', 'adenoid cystic carcinoma',
    'metastatic PTC/anaplastic carcinoma',
}
df_strict = df[~df['histology_final'].isin(EXCLUDE_HISTOLOGIES)].copy()
```

This reduces the cohort to ~3,783 patients with malignant DTC (PTC, FTC, poorly-differentiated DTC, high-grade DTC, metastatic PTC).

**Refit the primary logistic regression** on `df_strict` and report the gross-vs-microscopic and no/neg-vs-microscopic ORs. Compare to the original model. Document any attenuation or strengthening.

**Refit the Cox model** on the strict-DTC + surgery-date-known + FU>0 subset.

Update the manuscript Methods to specify the strict-DTC inclusion criteria. Move the broader-cohort fit to Supplement.

---

## Refit 2 — Drop RAI as a covariate (confounding by indication)

RAI receipt is a treatment that follows perceived recurrence risk, not a risk factor. Including it as a covariate generates an aOR of 4.52 that is not causally interpretable.

**Refit primary logistic and Cox** dropping `rai_received_flag` from the covariate set. Report the resulting ETE group ORs/HRs alongside the with-RAI version. Hypothesis: dropping RAI will increase the gross-vs-microscopic OR (toward the crude 2.61) because RAI was absorbing variance that belonged to gross ETE.

Add a sensitivity row to Table 3 and the forest plot for "Primary model — without RAI."

---

## Refit 3 — Split histology_grouped into clean DTC categories

Within the strict-DTC cohort (Refit 1), split `histology_grouped` into:

- `PTC` (reference; n=3,075)
- `FTC` (n=491)
- `Metastatic-PTC` (n=166) — explicit category, do not collapse with PTC
- `Poorly-differentiated DTC` (n=39)
- `High-grade DTC` (n=12) — combine with poorly-differentiated if cell too small

Refit the primary model with this expanded histology factor. Confirm whether the "other" coefficient now resolves into a metastatic-PTC + poorly-differentiated effect.

---

## Refit 4 — N1b interaction with ETE (small but striking)

Within each N stage:
- N0: micro 1.7%, gross 4.2%, no/neg 4.3%
- N1a: micro 2.4%, gross 5.9%, no/neg 7.2%
- N1b: **micro 27.6%** (n=29, 8 events), gross 14.6%, no/neg 16.7%

The N1b stratum inverts the gross-vs-microscopic gradient — microscopic ETE with N1b has a higher path-proven rate than gross-ETE N1b. This is a small cell but clinically meaningful.

**Test an `ete_group × ajcc8_n_stage` interaction** in the strict-DTC primary logistic model. If the interaction p-value is <0.05 or the N1b stratum effect is markedly heterogeneous, report stratified ORs in Table 3 and add a sub-paragraph in the Results.

---

## Refit 5 — Tumor-size adjustment within no/neg ETE

The no/neg-N1a cell has mean tumor size 3.99 cm — the largest of any group×stage cell. This is residual size-confounding within the no/neg-vs-microscopic contrast.

**Within the strict-DTC cohort**, refit the no/negative-ETE-subgroup logistic regression (n=192 in this group; events ≈ 12) on path-proven recurrence with covariates:
- `tumor_size_cm` (continuous)
- `ajcc8_n_stage` (4-level)
- `lateral_pos_flag` (binary)
- `central_pos_flag` (binary)
- `n_surgeries >= 2` (binary)
- `days_to_2nd` (per 100 days; with `had_2nd_surgery` flag)

Confirm whether the N1a + larger-size + ≥2-surgery combination explains the no/neg recurrence rate. Report the resulting ORs in a new Supplement Table.

---

## Output requirements

1. Re-fit all primary, secondary, and Cox models on the strict-DTC cohort (Refit 1) and re-populate Table 3 with the strict-DTC results as primary and the broader cohort as a sensitivity row.
2. Add a new "Primary model — without RAI" row in Table 3 (Refit 2).
3. Update the histology factor levels in Table 3 (Refit 3).
4. Add an "ETE × N stage interaction" subsection in the Results and report stratified ORs (Refit 4).
5. Add a Supplement Table with the no/negative-ETE subgroup logistic (Refit 5).
6. Regenerate `figures/m044_forest_primary.png` from the strict-DTC primary model. Keep the broader-cohort version as `figures/m044_forest_primary_broad.png` in the supplement.
7. Update `M044_ETE_manuscript_draft.md`:
   - **Methods:** specify strict-DTC inclusion criteria; specify that RAI is reported as a sensitivity variable, not primary covariate.
   - **Results — Multivariable analysis:** lead with the strict-DTC primary model. Report both with-RAI and without-RAI versions. Discuss the ETE × N stage interaction if significant. Report the histology-split coefficients clearly.
   - **Discussion:** revise the paragraph on "the logistic OR attenuated to 1.39, the Cox HR is 2.10" to reflect the strict-DTC + no-RAI primary model. State explicitly that the original logistic attenuation was largely driven by RAI confounding-by-indication and by the heterogeneous "histology-other" bucket containing non-DTC and non-malignant tumors.
8. Re-run `scripts/recalc.py M044_ETE_tables.xlsx` and confirm zero formula errors.
9. Commit with message:

```
manuscript(M044): strict-DTC refit, drop RAI as covariate, histology split, N1b interaction

- Refit 1: strict-DTC cohort (~3,783) excluding MTC/anaplastic/NIFTP/FTUMP/adenoma/rare-other.
- Refit 2: primary model without RAI (RAI = confounding by indication).
- Refit 3: histology split into PTC/FTC/Metastatic-PTC/Poorly-diff/High-grade.
- Refit 4: ETE × N stage interaction tested; N1b inverts gross-vs-microscopic gradient.
- Refit 5: no/negative-ETE subgroup logistic with size + N stage + LN compartments + reoperative covariates.
- Updated Table 3, forest plot, Results section. Discussion revised to reflect strict-DTC primary.
```

10. Push to `origin/main`.

---

## Acceptance criteria

- Strict-DTC primary logistic and Cox results in Table 3.
- "Primary model — without RAI" row in Table 3.
- Histology factor split into 5 levels with corresponding coefficients.
- ETE × N stage interaction tested and reported (whether significant or not).
- No/negative-ETE subgroup model in supplement with full coefficients.
- Manuscript Discussion paragraph revised to reflect strict-DTC + no-RAI primary findings.
- Forest plot regenerated.
- Excel workbook clean (no formula errors).
- Commit pushed.

---

## Reference: direct MotherDuck queries supporting each refit

Saved in chat log 2026-05-01 (Claude session). The five queries that produced the supporting evidence are:

```sql
-- Q1: RAI confounding probe
WITH cohort AS (...) SELECT ete_g, AVG(rai_received_flag), AVG(CASE WHEN rai_received_flag AND recurrence_path_proven ...) FROM cohort GROUP BY 1;

-- Q2: histology breakdown for inclusion-criterion audit
SELECT histology_final, COUNT(*) AS n FROM cohort_m044_ajcc_ete_v1 GROUP BY 1 ORDER BY n DESC;

-- Q3: Follicular-like subtype split
SELECT histology_final, n, path_proven_n, pp_rate FROM ... WHERE histology_final ILIKE '%follicular%' OR ...;

-- Q4: Lymphatic invasion cleaned-category cell counts
SELECT lvi_clean, n, path_proven_n, pp_rate FROM ...;

-- Q5: No/neg ETE × N stage stratified rates
SELECT ete_g, ajcc8_n_stage, n, path_proven_n, pp_rate FROM ... GROUP BY 1, 2;
```

End of follow-up prompt.
