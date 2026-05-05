# Cursor prompt — M048 v3: Full Covariate-Adjusted Racial Disparities

**Supersedes:**
- `M048_racial_disparities_cursor_prompt_20260505.md` (v1)
- `M048_v2_covariate_adjustment_cursor_prompt_20260505.md` (v2)

This v3 prompt is the single authoritative analytic plan for M048. It
incorporates v1 (raw per-race ROM/AUC), v2 (multinodular burden, genetics
access, nuclear medicine, background path, procedure type), AND adds v3
(FNA pattern, FNA-path concordance, US-to-surgery interval, tumor biology
descriptors, ETE/LN, frozen section, histology subtype distribution).

**Scope.** Cursor's job is data engineering, statistics, tables, figures,
and verification ONLY. Manuscript writing remains out of scope and is
deferred to a separate Cowork/Grok/ChatGPT chat once v3 outputs are
verified.

**Repo:** `THYROID_2026`
**Database:** `thyroid_canonical_publication_v1_0` @ release tag `pub_v1_1`
**Builds on:** M025 v2 (mig_307b)
**Migration:** mig_317 (v3 covariate-adjusted analysis)
**Status of inputs:** READY — all covariate tables canonical

---

## Why v3 exists (build-up of senior-author critique)

| Version | Critique | Response |
|---|---|---|
| v1 | Raw per-race ROM/AUC alone is too thin to publish | Added v2 covariates |
| v2 | Need to control for FNA pattern + tumor biology context too | This v3 |

After v3 we have a defensible analytic package: pre-surgery confounders are
adjusted in the regression; post-surgery tumor biology is reported as a
**disparity-direction interpretive table** that distinguishes
"over-referral of indolent disease" (high TR + low ROM + small tumors)
from "under-referral until aggressive presentation" (high TR + high ROM +
large/advanced tumors).

---

## CRITICAL CAUSAL DIAGRAM (read before coding)

```
              [pre-surgery, eligible regression adjusters]
                                |
race  -->  multinodular burden  -->  FNA decision  -->  Bethesda  -->  surgery decision  -->  is_malignant (outcome)
       \   genetics access      /                                       /
        \  nuclear-med utiliz   /   us-to-surg interval  /  procedure type
         \ background path     /                        /
                                                      /
                              [post-surgery; DESCRIPTORS only]
                              tumor size, multifocality, ETE, LN+, histology subtype

REGRESSION ADJUSTERS:  multinodular burden, genetics access, nuclear-med use,
                       background path (CLT/MNG/Graves/etc), procedure type,
                       FNA pattern (any/repeat/n), Bethesda category,
                       US-to-surgery interval, age, sex, surg_year
DESCRIPTORS (NEVER ADJUST FOR THESE):  tumor size band, multifocality,
                       histology subtype, ETE grade, LN positivity
```

If Cursor adds tumor-biology variables to the right-hand side of the
malignancy regression, that's adjusting for the outcome. Don't do it.
Tumor biology is reported separately.

---

## Pre-specified analytic plan (do NOT modify without senior-author sign-off)

### Inputs (built by `M048_motherduck_queries.sql` sections 9–26)

v2 (already specified):
- `m048_nodule_count_*`, `m048_genetics_access_*`,
  `m048_imaging_utilization_*`, `m048_background_path_*`,
  `m048_procedure_*`, `m048_extended_patient_master_v1`

v3 (this prompt):
- `m048_fna_pattern_by_race_v1`
- `m048_fna_to_surgery_interval_by_race_v1`
- `m048_fna_path_concordance_by_race_v1`
- `m048_tumor_biology_descriptors_by_race_v1`     (DESCRIPTIVE)
- `m048_histology_subtype_by_race_v1`             (DESCRIPTIVE)
- `m048_aggressive_features_by_race_v1`           (DESCRIPTIVE; ETE + LN)
- `m048_frozen_section_by_race_v1`
- `m048_us_to_surgery_interval_by_race_v1`
- `m048_v3_patient_master_v1`                     (joins everything)

### Pre-specified primary models

#### Model F (Full v3 patient grain)
```python
import statsmodels.api as sm
from statsmodels.formula.api import logit

m_full = logit(
    "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int "
    # multinodular burden
    "+ C(nodule_burden_cat) "
    # genetics + imaging access
    "+ had_any_genetics + had_any_nm "
    # background path
    "+ has_clt + has_mng + has_graves + has_niftp + has_ftump "
    # FNA pattern (PRE-SURGERY)
    "+ had_any_fna + had_repeat_fna + n_fnas_total "
    "+ C(bethesda_bucket) "
    # workup-pathway proxies
    "+ days_us_to_surg_approx "
    # demographics + procedure
    "+ age_at_surgery + C(sex) + surg_year "
    "+ C(surg_procedure_type)",
    data=df_v3,
).fit(disp=False)
```

#### Model F-Nodule (Full v3 nodule grain)
Same RHS, with `nodule_path_proven_malignant` outcome and
`acr2017_tirads_int` predictor. Cluster SE on `research_id`.
Restrict to `analytic_eligible_strict_acr_pernodule = TRUE`.

#### Model B-Stratified (Bethesda-stratified per-race per-TR ROM)
For each Bethesda category (II, III, IV, V, VI), fit a separate logistic
of `is_malignant ~ race_strat + max_tr_int`. Tests whether the per-race
TR-ROM gradient persists *within* cytologic strata. This is the cleanest
test of "does TI-RADS calibrate equally by race conditional on what the
FNA showed?" — because it conditions on the parallel test rather than
treating it as a confounder.

Report ORs and per-cell counts; flag any cell with N<10.

#### Model I (Race × TR interaction) and Model M (Race × multinodular)
Same as v2 spec.

#### Mediation models
For each candidate mediator (n_nodules_total, had_any_genetics,
had_any_nm, has_clt, has_mng, n_fnas_total, days_us_to_surg_approx),
compute Baron-Kenny indirect effect with 1,000-rep bootstrap CIs.
Bethesda is treated as a parallel diagnostic test, not a mediator.

### Stepwise attenuation cascade (Models 0–6)
- Model 0: race only
- Model 1: + max_tr_int
- Model 2: + nodule_burden_cat
- Model 3: + genetics + nuclear_med
- Model 4: + background path
- Model 5: + FNA pattern + Bethesda + US-to-surgery interval **[v3 NEW]**
- Model 6: + age + sex + surg_year + procedure_type (Full)

For each step log race-effect OR + 95% CI. Plot as Figure 7 (waterfall).

### Disparity-direction interpretive table [v3 NEW]
For each (race × max_tr_category_ever) cell, report from
`m048_tumor_biology_descriptors_by_race_v1` and
`m048_aggressive_features_by_race_v1`:
- n_malignant in cell (denominator from M025)
- ROM% (already known)
- mean tumor size (cm)
- % multifocal
- % any ETE (micro + gross)
- % LN-positive
- dominant histology subtype

Then compute the **disparity-direction signature** for each race × TR4 and
TR5 cell:
- "Over-referral signature": low ROM + small tumors + low ETE + indolent
  histology
- "Under-referral signature": high ROM + large tumors + high ETE + LN+
- "Calibrated": ROM in ACR band, tumor characteristics typical

Report as `m048_v3_disparity_direction_table.csv` and Figure 11
(quadrant plot: ROM vs tumor-size-Z; race-colored).

### Sensitivity arms (v3 superset)
- S048v2-A: post-2017 era only
- S048v2-B: single-nodule patients only
- S048v2-C: genetics-tested only (n=1,151)
- S048v2-D: CLT-excluded
- **S048v3-E: Bethesda VI-excluded** (remove pre-confirmed cancers; tests
  whether the disparity is driven by definitively-malignant cytology cells)
- **S048v3-F: TR4-only** (the ACR clinical-action threshold; reports
  adjusted OR for race within TR4 patients)
- **S048v3-G: post-FNA cohort only** (excludes patients brought directly
  to surgery without FNA — which is most of M025's TR1/TR2 surgical pts)

### Statistical conventions (v3 unchanged from v2)
- Wilson 95% CIs on all proportions.
- Firth penalized logistic for any cell with 0 events.
- Cluster SE on research_id for all nodule-grain models.
- Report ORs + 95% CI + Wald p-values; effect sizes always.
- Bonferroni for the 4 race × TR interaction tests (Model I).
- Multi-mediator analysis: report each mediator's indirect effect both
  univariately and in the joint model (statsmodels `mediation` extension
  or `pingouin.mediation_analysis`).

---

## Files to produce

```
studies/m048_racial_disparities_tirads/v3/
  ├── m048_v3_run_snapshot.json
  ├── m048_v3_qa_gates.csv
  # v3-specific outputs
  ├── m048_v3_fna_pattern_by_race.csv
  ├── m048_v3_fna_to_surgery_interval.csv
  ├── m048_v3_fna_path_concordance.csv
  ├── m048_v3_tumor_biology_descriptors.csv
  ├── m048_v3_histology_subtype_by_race.csv
  ├── m048_v3_aggressive_features_by_race.csv
  ├── m048_v3_frozen_section_by_race.csv
  ├── m048_v3_us_to_surgery_interval.csv
  ├── m048_v3_bethesda_stratified_TR_ROM.csv      (Model B-Stratified)
  ├── m048_v3_disparity_direction_table.csv      (over- vs under-referral)
  # regression outputs
  ├── m048_v3_attenuation_cascade.csv             (Models 0-6 race-OR)
  ├── m048_v3_full_model_OR.csv                   (Model F race + each adj OR)
  ├── m048_v3_interaction_race_x_tr.csv           (Model I)
  ├── m048_v3_interaction_race_x_nodulect.csv     (Model M)
  ├── m048_v3_mediation.csv                       (per-mediator indirect FX)
  ├── m048_v3_sensitivity_arms.csv                (S-A through S-G)
  ├── m048_v3_covariate_balance.csv               (Table 1-by-race + SMDs)
  └── verification/
      ├── cortex_smoke_tests_v3.md
      ├── independent_recompute_v3.py
      └── m025_reconciliation_v3.csv

M048_submission_package/figures/v3/
  ├── Figure_6_Adjusted_OR_Forest.{png,pdf}
  ├── Figure_7_Attenuation_Cascade.{png,pdf}
  ├── Figure_8_Race_x_TR_Interaction.{png,pdf}
  ├── Figure_9_Mediation_Diagram.{png,pdf}
  ├── Figure_10_Covariate_Balance_Love.{png,pdf}
  ├── Figure_11_Disparity_Direction_Quadrant.{png,pdf}      [v3 NEW]
  ├── Figure_12_Bethesda_Stratified_TR_ROM.{png,pdf}        [v3 NEW]
  └── Figure_13_FNA_Pattern_by_Race.{png,pdf}               [v3 NEW]
```

---

## Implementation steps

### Step 1 — Run all SQL on MotherDuck
Sections 0–26 in `studies/m048_racial_disparities_tirads/M048_motherduck_queries.sql`.
Dump each `CREATE TABLE` to CSV under `studies/m048_racial_disparities_tirads/v3/`.

### Step 2 — Build covariate balance table
Standard "Table 1 by race" with all v2+v3 candidate adjusters. Report SMDs
by race; flag any > 0.10. Write to `m048_v3_covariate_balance.csv`.

### Step 3 — Run stepwise attenuation cascade (Models 0–6)
Log race-effect OR + 95% CI at each step. Plot as Figure 7.

### Step 4 — Run primary v3 models
Model F, F-Nodule, B-Stratified, I, M.

### Step 5 — Mediation analysis
7 candidate mediators (was 5 in v2; add n_fnas_total and
days_us_to_surg_approx). Univariate + joint.

### Step 6 — Sensitivity arms (7 arms now: A–G)

### Step 7 — Disparity-direction interpretive table + quadrant plot
This is the v3 highlight. For each race × TR4 and TR5 cell:
| race | TR | n | ROM% | mean tumor size | % multifocal | % any ETE | % LN+ | signature |
|---|---|---|---|---|---|---|---|---|
| Black | TR4 | ... | 9.3% | ? | ? | ? | ? | over-referral?  |
| Black | TR5 | ... | 14.5% | ? | ? | ? | ? | over-referral?  |
| White | TR4 | ... | 24.1% | ? | ? | ? | ? | calibrated      |
| White | TR5 | ... | 30.7% | ? | ? | ? | ? | calibrated      |
| Asian | TR4 | ... | 30.4% | ? | ? | ? | ? | under-referral? |
| Asian | TR5 | ... | 54.0% | ? | ? | ? | ? | under-referral? |

The "signature" assignment is rule-based (algorithm in `independent_recompute_v3.py`):
- **Over-referral**: ROM < ACR-expected-mid AND mean tumor size < cohort median
  AND % any ETE < cohort median
- **Under-referral**: ROM > ACR-expected-high AND mean tumor size > cohort median
  AND (% any ETE > cohort median OR % LN+ > cohort median)
- **Calibrated**: neither

Plot as Figure 11 (quadrant plot, x = ROM%, y = mean tumor size Z-score,
color = race, marker = TR).

### Step 8 — Cortex Analyst NL verification (v3-specific)

If covariate semantic model still not bound, scaffold
`m048_v3_covariates_semantic_model.yaml` and bind in mig_317.

NL prompts to verify:
1. *"What proportion of patients of each race had any FNA performed
   before surgery?"*
2. *"What is the median Bethesda category by race?"* (should be similar
   across races IF FNA selection is unbiased)
3. *"In Black patients with TR5 max category, what is the mean tumor
   size at pathology?"*
4. *"How many race × TR4 cells have at least 10 malignant patients in the
   v3 disparity-direction table?"*
5. *"Among genetics-tested patients only, what is the per-race patient
   AUC?"* (Sensitivity Arm C reproducer)

### Step 9 — Independent recompute v3

Re-derive 4 v3 headline numbers from raw `m025_analytic_master_*` +
`canonical_*` joins, without using any `m048_v3_*` tables. Hard-assert
agreement to ≤2% relative.

Headline numbers to assert:
- Fully-adjusted Black-vs-White OR from Model F
- Attenuation % from Model 0 to Model 6
- Asian-TR5 mean tumor size cm (descriptive)
- Bethesda-stratified Model B race OR within Bethesda IV (most diagnostic
  for the calibration question)

### Step 10 — Update QA gates (v3)

Verify all 11 gates (v1) + 9 v2 gates + new v3 gates:
- v3 master row count = 3,375 (matches M025).
- FNA coverage matches EERC-documented 5,229 across whole warehouse;
  expect 70.5% coverage in M025 cohort = ~2,380.
- Repeat-FNA prevalence within biopsied subset reasonable (~15-25%).
- Multifocal flag coverage among malignant patients: ~61% (per M025
  multifocality stat).
- Tumor-size coverage: most malignant patients should have non-NULL
  `max_tumor_size_cm`.
- ETE canonical join doesn't lose patients (left join, NULL = no ETE).
- Independent-recompute v3 assertions all pass.

### Step 11 — Produce v3 handoff README

`m048_handoff_README_v3.md` containing **only**:
- Run metadata + git sha + mig_317 sign-off.
- All v1 + v2 + v3 numbers in a single reference table.
- The disparity-direction interpretive table for the writing chat to pull
  from directly.
- Updated framing guidance:
  - **If race effect attenuates >70% in Model 6 AND Bethesda-stratified
    analysis kills the gradient:** headline is "apparent disparity is
    fully explained by FNA selection + multinodular burden + access."
  - **If <30% AND Bethesda-stratified analysis preserves the gradient:**
    "residual race effect persists across cytologic strata, suggesting
    [TR-feature interpretation × race interaction]."
  - **30–70%:** report attenuation cascade as central finding;
    Bethesda-stratified per-race TR-ROM as supplementary;
    disparity-direction quadrant as the clinical-interpretation figure.
  - **In all cases:** the disparity-direction signature (over- vs
    under-referral) per race × TR is the headline-clarifying finding for
    the endocrine-surgery audience.

---

## Migration sign-off

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_317',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig317',
  'mig_317: M048 v3 racial-disparities full covariate-adjusted analysis.
   Built m048_v3_patient_master_v1 (n=3,375; v2 covariates + FNA pattern,
   FNA-path concordance, US-to-surgery interval, tumor-biology descriptors,
   ETE/LN, frozen section, histology subtype). Ran stepwise attenuation
   cascade Models 0-6, Bethesda-stratified Model B, race x TR Model I,
   race x multinodular Model M, mediation analysis for 7 candidate
   mediators, 7 sensitivity arms (A-G). Produced disparity-direction
   interpretive table distinguishing over-referral vs under-referral
   signatures by race x TR. Independent recompute verified. Manuscript
   authoring DEFERRED.'
);
```

Update `MASTER_MANUSCRIPT_LIST_DATA_READINESS_20260504.xlsx` row 48 status
→ "v3 Adjusted Analysis Complete — Awaiting Writing".

---

## Definition of done

- [ ] All v3 SQL tables (sections 17–26 in `M048_motherduck_queries.sql`)
      populated and CSV-dumped.
- [ ] `m048_v3_covariate_balance.csv` built; all SMDs reported.
- [ ] Stepwise attenuation cascade (Models 0–6) run; race-OR logged at
      each step.
- [ ] Model F, F-Nodule, B-Stratified, I, M all run with appropriate
      Firth/cluster SE conventions.
- [ ] Mediation analysis run for all 7 candidate mediators (univariate +
      joint).
- [ ] All 7 sensitivity arms (S-A through S-G) reported.
- [ ] Disparity-direction interpretive table built and Figure 11
      (quadrant) rendered.
- [ ] Bethesda-stratified Model B output written to
      `m048_v3_bethesda_stratified_TR_ROM.csv` and Figure 12 rendered.
- [ ] Cortex NL verification logged (or covariate semantic model
      scaffolded for mig_317 bind).
- [ ] `independent_recompute_v3.py` assertions all pass.
- [ ] Eight v3 figures (Figs 6–13) rendered as 300 dpi PNG + vector PDF.
- [ ] All v3 QA gates pass.
- [ ] `m048_handoff_README_v3.md` written; no prose, just numbers + paths
      + framing rules.
- [ ] mig_317 signed off; MASTER row 48 status updated.

When finished, post a one-paragraph Cowork summary including:
- Race-effect OR (Black/Asian vs White) at unadjusted, post-v2 adjustment,
  and post-v3 full adjustment; attenuation %.
- Top 3 mediators by indirect-effect magnitude.
- Bethesda-stratified Model B finding (does the gradient persist within
  cytologic strata?).
- Disparity-direction signature per race × TR4/TR5 cell.
- Significant race × TR interactions surviving Bonferroni.
- One-line headline framing recommendation for the writing chat.
