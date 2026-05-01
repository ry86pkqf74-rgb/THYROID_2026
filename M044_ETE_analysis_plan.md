# M044 — Statistical Analysis Plan: Microscopic vs Gross ETE Manuscript

**Version:** 1.0 (post-validation, 2026-05-01)
**Database:** `thyroid_canonical_publication_v1_0`
**Cohort view:** `manuscript_workspace.cohort_m044_ajcc_ete_v1` (n=4,128)
**Authoring:** Independent verification by Claude; intended for ChatGPT and the human study team to review and amend.

---

## 1. Study question

Does microscopic extrathyroidal extension (mETE) carry a different prognostic burden from gross extrathyroidal extension (gETE) under AJCC 8th edition staging in a contemporary, single-institution thyroid-cancer cohort, and does the updated dataset support the AJCC 8 decision to stop upstaging mETE to T3?

Secondary questions, all pre-specified:

- What is the median follow-up of the analytic cohort, and how should follow-up censoring be handled given a substantial fraction of zero-follow-up patients?
- In the no/negative ETE subgroup, what explains the apparent recurrence signal — biology, ascertainment bias, second-surgery pathway, or coding/selection effects?
- Is the previously reported "protective" lymphovascular invasion association reproducible when lymphatic invasion (`lvi_grade`) and vascular invasion (`vascular_invasion_final`) are modeled separately and missing/indeterminate categories are retained?
- Do path-proven and imaging-only-suspicious recurrences behave differently?

---

## 2. Data sources and derived analytic file

The analytic file is built by the SQL package `M044_ETE_analysis.sql` and consists of one row per `research_id` with the following constructed columns:

- `ete_group ∈ {'No/negative ETE', 'Microscopic ETE', 'Gross ETE', 'Present ungraded', 'Missing/other'}` derived from `ete_grade_final`.
- `lvi_clean ∈ {'extensive', 'present', 'focal', 'indeterminate', 'missing'}` (collapses spelling variants in `lvi_grade`).
- `vasc_clean ∈ {'extensive', 'focal', 'present_ungraded', 'indeterminate', 'missing'}` (preserves `vascular_invasion_final` with explicit missing).
- LN burden via `ln_master_rollup_v1` pre-aggregated to one row per patient with `central_pos_flag` and `lateral_pos_flag` derived.
- Reoperative context via `cohort_m040_reoperative_v1` pre-aggregated to one row per patient.
- **Recurrence column-of-record from `main.canonical_recurrence_resolved_v1`**: `recurrence_path_proven`, `recurrence_imaging_suspicious`, `recurrence_status_final`, `recurrence_imaging_then_path_confirmed`, `days_to_path_proven`, `days_to_imaging_suspicious`.

---

## 3. Primary exposure

`ete_group` with `Microscopic ETE` as the analytic reference (per the manuscript's clinical comparison: does mETE carry the prognostic weight of gETE, or does it behave like the no-ETE group?).

Primary contrasts of interest:

- **Gross ETE vs Microscopic ETE** (mechanistic core).
- **No/negative ETE vs Microscopic ETE** (boundary check — should be similar or microscopic should be slightly worse).

`Present ungraded` and `Missing/other` are excluded from the primary multivariable model and reported only in sensitivity analyses.

---

## 4. Primary outcome

**Path-proven recurrence** as defined by `recurrence_path_proven = TRUE` in `canonical_recurrence_resolved_v1`. This corresponds to biopsy-proven, op-pathology-proven, FNA-Bethesda-5/6 (>30 days post-op), or LLM-extracted entity with explicit pathology keyword evidence.

Justification: the canonical convention prohibits collapsing path-proven and imaging-suspicious into a single any_recurrence variable; the cohort view's `any_recurrence_flag` (n=503) and `structural_recurrence_flag` (n=1,819) include large subsets without canonical evidence and are therefore unreliable as primary endpoints (see Validation Report §3.2).

Pre-specified secondary endpoints:

- **Imaging-only-unconfirmed recurrence** (`recurrence_status_final = 'imaging_only_unconfirmed'`).
- **Composite recurrence** (`recurrence_status_final ∈ {path_proven, imaging_only_unconfirmed}`).
- **Imaging-then-path-confirmed recurrence** (`recurrence_imaging_then_path_confirmed = TRUE`) reported descriptively as a quality-control endpoint (it counts patients whose imaging suspicion was later confirmed by pathology and is included in path_proven).

Tertiary descriptive endpoints:

- All-cause death (`death_occurred`) — reported only if the cohort's death ascertainment is judged complete enough; will be flagged as exploratory in the manuscript.
- RAI receipt (`rai_received_flag`) — descriptive baseline characteristic, not an outcome.

---

## 5. Covariates and adjustment

The primary multivariable model adjusts for, in order of clinical priority:

1. Age at surgery (continuous, per 10 years).
2. Sex (female reference).
3. Tumor size in cm (continuous, log-transformed if non-linear).
4. AJCC8 N stage (`N0` reference; `N1a`, `N1b`, `Nx`, missing).
5. Histology (PTC reference; follicular-like, MTC-like, other).
6. RAI receipt (binary).
7. Lymphatic invasion (cleaned categorical).
8. Vascular invasion (cleaned categorical).

Pre-specified alternative model uses central-LN-positive flag and lateral-LN-positive flag in place of AJCC8 N stage to capture compartment-specific nodal disease.

---

## 6. Analysis methods

### 6.1 Descriptive

- Continuous variables: mean (SD) and median (IQR), reported by ETE group.
- Categorical variables: n (%), reported by ETE group.
- Follow-up reported in two ways: all-row median (IQR) including zeros, and positive-FU-only median (IQR). Maximum and zero-FU count reported in text.
- Surgery-date window: 1999–2024 among the 3,212 patients with non-missing dates; 914 patients (22.1%) have missing surgery date and 2 have pre-1999 dates that will be flagged in the cohort flow diagram.

### 6.2 Unadjusted recurrence comparisons

For each ETE group: n, path-proven n (rate), imaging-only n (rate), composite n (rate), recurrence per 100 person-years (using all follow-up time and again restricted to follow-up >0). Crude odds ratios with 95% CI (Wald or exact) using Microscopic ETE as reference.

### 6.3 Multivariable models

**Primary model.** Logistic regression of path-proven recurrence (binary) on `ete_group` + covariates listed in §5. Reported as adjusted odds ratios with 95% CI and likelihood-ratio p-values.

**Time-to-event sensitivity model.** If `days_to_path_proven` is reliably populated for the path-proven subset and surgery-date data are sufficient to compute time-zero, fit a Cox proportional-hazards model with `Surv(followup_years, recurrence_path_proven)` as outcome. This is pre-specified as a sensitivity model because the cohort view does not currently expose a unified time-zero, and 22% of surgery dates are missing. The Cox model will be run on the surgery-date-known subset.

**Secondary models.** Repeat the primary logistic regression with the imaging-only-unconfirmed and composite endpoints.

### 6.4 Sensitivity analyses (pre-specified)

S1. Exclude the 1,400 zero-follow-up patients and refit.
S2. Restrict to patients with surgery dates in 1999–2024 (n=3,212).
S3. Exclude `Present ungraded` and `Missing/other` (already absent from primary).
S4. Lymphatic and vascular invasion modeled as separate categorical variables (already in primary; this is the explicit sensitivity check that the prior "protective LVI" association does not reappear).
S5. Vascular invasion as ordinal: missing < indeterminate < focal < present_ungraded < extensive.
S6. Drop the `'true'` ambiguous ETE rows (n=4) from the primary model entirely.
S7. Replace AJCC8 N stage with central- and lateral-LN-positive flags.
S8. Use the legacy `any_recurrence_flag` as outcome to demonstrate robustness or document the divergence (this is a transparency check, not a primary analysis).
S9. No/negative ETE subgroup model: among the 192 no/negative ETE patients, fit a logistic regression of path-proven recurrence on size, N stage, central/lateral compartments, RAI, ≥2 surgery indicator, and median first→second surgery interval as a continuous predictor.

### 6.5 Subgroup analyses

By tumor size: ≤1 cm, 1.1–2 cm, 2.1–4 cm, >4 cm.
By age: <55, ≥55 (AJCC 8 cutoff).
By histology: PTC, follicular-like, other.
Forest-plotted adjusted ORs reported in the supplement.

### 6.6 Missing data

Missing covariates retained as their own category for categorical variables (LVI, vascular invasion, N stage, T stage). Tumor size missingness (n=6, none recurred) handled by complete-case for the primary model and by mean-imputation in a sensitivity analysis. No multiple imputation in the primary plan because the missingness is systematically tied to ETE source (not ignorable within the model variables).

### 6.7 Statistical software and significance threshold

R or Python; primary modeling fits in `statsmodels` (Python) or `survival`/`glm` (R). Two-sided p-values; α = 0.05; multiple-comparison adjustment is descriptive (Bonferroni) for the secondary subgroup grid because the pre-specified primary contrast is single (mETE vs gETE).

---

## 7. Endpoint definitions table

| Endpoint | Source | Definition | n (cohort=4,128) |
|---|---|---|---:|
| Path-proven recurrence (PRIMARY) | `canonical_recurrence_resolved_v1.recurrence_path_proven` | Biopsy/op-path/cytology-positive evidence (status='path_proven') | 145 |
| Imaging-only-unconfirmed | `recurrence_status_final='imaging_only_unconfirmed'` | Imaging finding without path confirmation | 195 |
| Composite | `recurrence_status_final IN ('path_proven','imaging_only_unconfirmed')` | Either evidence track | 340 |
| Imaging-then-path-confirmed (descriptive) | `recurrence_imaging_then_path_confirmed` | Imaging suspicion later confirmed by path; subset of path-proven | 33 |
| Legacy any_recurrence (sensitivity only) | `cohort_m044_ajcc_ete_v1.any_recurrence_flag` | Inherited cohort-view flag | 503 |
| Legacy structural_recurrence (audit only) | `cohort_m044_ajcc_ete_v1.structural_recurrence_flag` | Inherited cohort-view flag; n=1,819 contains 1,467 patients without canonical evidence — not used | 1,819 |
| Death (descriptive) | `death_occurred` | Recorded death | TBD |

---

## 8. Pre-specified figures

1. Cohort flow diagram (n=4,128 final; flag missing surgery dates 914 and 2 pre-1999 outliers).
2. ETE group distribution bar chart (5 categories, with primary 3 highlighted).
3. Path-proven recurrence rate by ETE group with 95% CI bars.
4. Path-proven recurrence per 100 person-years by ETE group.
5. Kaplan-Meier of path-proven recurrence-free survival on the surgery-date-known subset (sensitivity).
6. Forest plot of adjusted ORs from the primary multivariable logistic model.
7. No/negative ETE explanatory panel: tumor size, central/lateral nodes, ≥2-surgery indicator, recurred vs not.

---

## 9. Reporting checklist

- STROBE for retrospective cohort studies will guide section structure.
- Cohort flow, ETE definitions, source hierarchy, and missingness disclosed.
- Pre-specified vs post-hoc analyses clearly labeled.
- All primary and secondary endpoints reported; legacy endpoint(s) reported in sensitivity for transparency only.

End of analysis plan.
