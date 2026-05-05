# Cursor prompt — M048 v2: Covariate-Adjusted Racial Disparities

**Scope.** Cursor's job here is to extend the v1 M048 analysis (mig_315) with
covariate-adjusted models that test whether the observed per-race per-TR ROM
disparities (Black 14.5% / White 30.7% / Asian 54.0% at nodule-grain TR5)
are confounded by genetics testing access, multinodular burden, imaging
utilization, benign-diagnosis distribution, and surgical procedure type.
**Manuscript writing remains out of scope** and will be authored separately
once v2 analytic outputs are verified.

**Repo:** `THYROID_2026`
**Database:** `thyroid_canonical_publication_v1_0` @ release tag `pub_v1_1`
**Builds on:** M048 v1 (mig_315) + M025 v2 (mig_307b)
**Status of inputs:** READY — covariate tables already canonical
(`canonical_molecular_genetics_v2`, `nuclear_med`, `m025_analytic_master_*`)

---

## Why v2 exists

Senior-author review of M048 v1 surfaced this critique: a 3.7× per-race
nodule-grain TR5 ROM gradient is striking but uninterpretable without
controlling for:

1. **Multinodular burden.** M025 already showed that multinodular attribution
   inflates patient-grain ROM by ~30 pp. If multinodular-disease prevalence
   differs by race, the per-race patient-grain disparities are partially
   mechanical.
2. **Genetics testing access.** Preop molecular testing changes the surgical
   pipeline (lobectomy vs total, surveillance vs operate). If genetics access
   is differentially available by race, the operative cohort itself is
   differentially selected.
3. **Imaging utilization.** Functional imaging (RAI uptake, scintigraphy)
   identifies hyperfunctioning / autonomous nodules. Differential access
   shifts which patients reach surgery for which indication.
4. **Background path / benign-diagnosis distribution.** CLT/Hashimoto's,
   Graves, MNG, follicular adenoma, NIFTP, and FT-UMP all confound the
   operative-cohort denominator. Race-stratified differences in these
   benign-diagnosis rates can drive apparent TI-RADS calibration shifts
   without any race-specific effect on TI-RADS itself.
5. **Surgical procedure type.** Lobectomy vs total thyroidectomy vs
   completion has different post-op pathology yields and is patterned by
   payer / institution / surgeon preference.

If the per-race TR5 ROM disparity attenuates substantially after adjustment,
that's the headline ("apparent racial disparity in TI-RADS calibration is
explained by access to molecular workup and multinodular burden"). If a
residual race effect persists, that's a different (stronger) headline
("after adjustment for [X, Y, Z], a [magnitude] residual TR5 ROM disparity
persists, suggesting [interpretive frame to be supplied by writing chat]").
**Either result is publishable — but raw v1 numbers without v2 adjustment
are not.**

---

## Pre-specified analytic plan (do NOT modify without senior-author sign-off)

### Inputs (all built by the appended SQL in `M048_motherduck_queries.sql`)
- `m048_nodule_count_per_patient_v1` — per-patient nodule count + max TR
- `m048_nodule_count_by_race_v1` — descriptive per-race
- `m048_genetics_access_by_race_v1` — preop molecular testing % by race
- `m048_imaging_utilization_by_race_v1` — nuclear-med + US burden by race
- `m048_background_path_by_race_v1` — CLT/Graves/MNG/adenoma/NIFTP/FT-UMP by race
- `m048_procedure_by_race_v1` — lobectomy / TT / completion by race
- `m048_extended_patient_master_v1` — one row per patient with all covariates
- `m048_extended_nodule_master_v1` — one row per nodule with patient covariates

### Pre-specified primary models

#### Model 1 — Patient-grain logistic regression
```python
import statsmodels.api as sm
from statsmodels.formula.api import logit

m1 = logit(
    "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int "
    "+ n_nodules_total + had_any_genetics + had_any_nm "
    "+ has_clt + has_mng + has_graves + has_niftp + has_ftump "
    "+ age_at_surgery + C(sex) + surg_year "
    "+ C(surg_procedure_type)",
    data=df_extended_patient,
).fit(disp=False)
```
Report ORs (95% CI) for each race level vs White reference, plus the partial
effect of each covariate. Report the change in the race-effect OR from the
unadjusted model (race_strat only) to the fully-adjusted model — this is the
**covariate attenuation** number.

#### Model 2 — Nodule-grain logistic regression (strict-eligible only)
Same RHS as Model 1, with `nodule_path_proven_malignant` as outcome and
`acr2017_tirads_int` as the per-nodule TI-RADS predictor. Cluster standard
errors on `research_id` (patients are clustered).

```python
m2 = logit(
    "nodule_path_proven_malignant ~ C(race_strat, Treatment('White')) + acr2017_tirads_int "
    "+ n_nodules_total + had_any_genetics + had_any_nm "
    "+ has_clt + has_mng + has_graves "
    "+ age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)",
    data=df_extended_nodule.query("analytic_eligible_strict_acr_pernodule == True"),
).fit(disp=False, cov_type="cluster", cov_kwds={"groups": df["research_id"]})
```

#### Model 3 — Race × TR interaction (test for differential calibration)
Adds `C(race_strat) * acr2017_tirads_int` to Model 2. Significant
interaction terms indicate that the race effect on ROM **varies by TR
category** — i.e., calibration is differentially miscalibrated for one race
in a TR-specific way. Report each interaction OR and Wald test p-value.

#### Model 4 — Race × multinodular-burden interaction (mediation check)
Adds `C(race_strat) * nodule_burden_cat` to Model 1. Tests whether
multinodular burden mediates the race effect.

### Pre-specified mediation analyses

For each candidate mediator (`n_nodules_total`, `had_any_genetics`,
`had_any_nm`, `has_clt`, `has_mng`):
- Compute the indirect effect via Baron-Kenny / product-of-coefficients with
  bootstrapped 95% CI (1,000 reps).
- Report the proportion of the total race effect mediated by each candidate.
- Use the `mediation` library (R rpy2 wrapper) or `pingouin.mediation_analysis`
  in Python.

### Pre-specified sensitivity arms

- **S048v2-A:** restrict to post-2017 era only (mirrors M025 era split).
- **S048v2-B:** restrict to single-nodule patients (n=782 in M025 S1C).
- **S048v2-C:** include the genetics-tested subset only (n=1,151) — tests
  whether the residual race effect changes when genetics confounding is
  eliminated by restriction rather than adjustment.
- **S048v2-D:** exclude CLT/Hashimoto's patients — tests whether background
  autoimmune disease mediates the race effect.

### Statistical conventions
- Use Firth penalized logistic regression for any model where a race × TR
  cell has 0 events (small-cell instability protection).
- Cluster SE on `research_id` for all nodule-grain models.
- Report all ORs with 95% CI and Wald p-values; do not rely on raw
  significance — report effect sizes.
- Multiple-comparison correction (Bonferroni) for the 4 race × TR
  interaction terms in Model 3.

---

## Files to produce

```
studies/m048_racial_disparities_tirads/v2/
  ├── m048_v2_run_snapshot.json
  ├── m048_v2_qa_gates.csv             (covariate coverage + reconciliation)
  ├── m048_v2_unadjusted_or.csv        (race ORs, no adjustment — should
  │                                     reproduce v1 raw disparities)
  ├── m048_v2_adjusted_or.csv          (race ORs, full adjustment)
  ├── m048_v2_attenuation.csv          (% attenuation per covariate added
  │                                     stepwise)
  ├── m048_v2_interaction_race_x_tr.csv
  ├── m048_v2_interaction_race_x_nodulect.csv
  ├── m048_v2_mediation.csv            (per-mediator indirect effect + CI)
  ├── m048_v2_covariate_balance_table.csv (Table 1-style by race)
  ├── m048_v2_sensitivity_arms.csv     (S048v2-A through D)
  └── verification/
      ├── cortex_smoke_tests_v2.md     (NL queries on covariate semantics)
      ├── independent_recompute_v2.py  (re-derives 3 headline adjusted ORs
      │                                 from raw m025_analytic_master rows)
      └── m025_reconciliation_v2.csv   (re-confirm patient n=3,375)

M048_submission_package/figures/v2/
  ├── Figure_6_Adjusted_OR_Forest.{png,pdf}        (race ORs, unadjusted vs
  │                                                  fully adjusted, forest)
  ├── Figure_7_Attenuation_Cascade.{png,pdf}       (waterfall of OR change
  │                                                  as covariates added)
  ├── Figure_8_Race_x_TR_Interaction.{png,pdf}     (per-TR ORs by race)
  ├── Figure_9_Mediation_Diagram.{png,pdf}         (path diagram of
  │                                                  mediator effects)
  └── Figure_10_Covariate_Balance.{png,pdf}        (Love plot pre/post
                                                    adjustment)
```

---

## Implementation steps

### Step 1 — Run the appended SQL on MotherDuck
The SQL appendix (sections 9–16 in `M048_motherduck_queries.sql`) builds
the extended analytic master. Run it section-by-section and dump each table.

### Step 2 — Build covariate balance table
Standard "Table 1 by race" with all candidate adjusters; report SMDs
(standardized mean differences) for each covariate by race. Any SMD > 0.10
flags a covariate that requires adjustment.

### Step 3 — Run unadjusted then stepwise-adjusted logistic regressions
- Model 0: race only.
- Model 1: + max_tr_int (the predictor of interest).
- Model 2: + n_nodules_total.
- Model 3: + had_any_genetics + had_any_nm.
- Model 4: + has_clt + has_mng + has_graves + has_niftp + has_ftump.
- Model 5: + age + sex + surg_year + procedure_type (Full).

For each step, log the race-effect OR and 95% CI. Plot as Figure 7
(attenuation cascade).

### Step 4 — Interaction tests + mediation

Run Models 2, 3, 4 from the pre-spec. For mediation, use
`pingouin.mediation_analysis` per mediator, bootstrap = 1000 reps.

### Step 5 — Sensitivity arms

Run all four sensitivity arms; report the race ORs side-by-side with the
primary fully-adjusted model in `m048_v2_sensitivity_arms.csv`.

### Step 6 — Cortex Analyst NL verification (covariate semantics)

If the M025-bound semantic model doesn't expose the new covariates,
scaffold `m048_v2_covariates_semantic_model.yaml` and bind in mig_316
(separate task; not blocking).

Pre-specified NL prompts to verify against CSVs:
1. *"What proportion of patients of each race had preoperative molecular
   genetics testing?"*
2. *"What is the median number of US-detected nodules per patient by race?"*
3. *"How does background Hashimoto's prevalence vary by race in the
   operative cohort?"*
4. *"What is the lobectomy-to-total-thyroidectomy ratio by race?"*

### Step 7 — Independent recompute

Write `verification/independent_recompute_v2.py` that re-derives 3 headline
numbers (fully-adjusted Black-vs-White OR, attenuation % from Model 0 to
Model 5, race × TR4 interaction OR) from raw `m025_analytic_master_*` and
`canonical_*` joins, without using any `m048_extended_*` tables. Hard-assert
agreement with the pipeline output to ≤2% relative.

### Step 8 — Update QA gates

Verify (block sign-off if any fail):
- `m048_extended_patient_master_v1` row count = 3,375 (matches M025).
- Genetics coverage matches EERC (1,151 ± 5 patients with `had_any_genetics`).
- Nuclear-med coverage matches EERC (~1,148 ± 5 patients with `had_any_nm`).
- All Model fits converged; no Hessian-singular warnings unaddressed.
- Independent-recompute assertions pass.

### Step 9 — Produce updated handoff README

Append to `m048_handoff_README.md` (or create `m048_handoff_README_v2.md`):
- All v1 numbers (unchanged).
- All v2 numbers (unadjusted vs fully adjusted ORs per race; attenuation %;
  significant interaction terms; mediation indirect effects).
- Explicit framing guidance for the writing chat:
  - If race effect attenuates >70%: frame as "apparent disparity explained
    by [dominant mediator]."
  - If race effect attenuates <30%: frame as "residual disparity persists
    after adjustment, suggesting [interpretation deferred to senior author]."
  - If 30–70%: report both the attenuation and the residual; present the
    attenuation cascade as the central finding.

---

## Migration sign-off

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_316',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig316',
  'mig_316: M048 v2 covariate-adjusted racial-disparities analysis. Built
   m048_extended_patient_master_v1 (n=3,375, joined to molecular genetics,
   nuclear medicine, nodule count, background path, procedure type) and
   m048_extended_nodule_master_v1. Ran stepwise logistic regression
   attenuation cascade, race x TR interaction, race x nodule-burden
   interaction, mediation analyses for 5 candidate mediators, and 4
   sensitivity arms (post-2017, single-nodule, genetics-tested subset,
   CLT-excluded). All QA gates pass; independent recompute verified.
   Manuscript authoring DEFERRED. Writing-chat framing guidance in
   m048_handoff_README_v2.md.'
);
```

Update `MASTER_MANUSCRIPT_LIST_DATA_READINESS_20260504.xlsx` row 48
Status → "v2 Adjusted Analysis Complete — Awaiting Writing".

---

## Definition of done

- [ ] All 8 v2 SQL tables (sections 9–16) populated and CSV-dumped.
- [ ] Covariate balance table with SMDs by race produced.
- [ ] Stepwise logistic regression attenuation cascade run; race-effect OR
      logged at each step.
- [ ] Interaction Models 3 and 4 run with Bonferroni correction.
- [ ] Mediation analysis run for 5 candidate mediators with bootstrap CIs.
- [ ] All 4 sensitivity arms (S048v2-A through D) reported.
- [ ] Cortex NL verification logged (or deferred with mig_316 covariate
      semantic model scaffolded).
- [ ] `independent_recompute_v2.py` assertions pass.
- [ ] All 5 v2 figures rendered as 300 dpi PNG + vector PDF.
- [ ] `m048_v2_qa_gates.csv` written; all gates pass.
- [ ] `m048_handoff_README_v2.md` written with framing guidance for the
      writing chat.
- [ ] mig_316 signed off; MASTER row 48 status updated.

When finished, post a one-paragraph Cowork summary including:
- Race-effect OR (Black vs White, Asian vs White) at unadjusted vs fully
  adjusted; attenuation %.
- Dominant mediators ranked by indirect-effect magnitude.
- Significant race × TR interactions (if any).
- Sensitivity-arm robustness (which arms preserve / kill the residual race
  effect).
- One-line headline framing recommendation for the writing chat.
