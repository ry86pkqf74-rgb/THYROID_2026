# M048 Manuscript Changelog — v1.2.2 → v1.3

**Version bumped:** 2026-05-07
**Reason for bump:** Background-pathology data-extraction bug fix (mig_318)
+ full cascade recompute (mig_318b) + comorbidity-arm expansion.

---

## What was wrong in v1.2.2 (and earlier)

The v3 adjustment-cascade master (`m048_v3_patient_master_v1`,
mig_317b) derived three M4-step background-pathology covariates
(`has_clt`, `has_mng`, `has_graves`) by regex on
`m048_patient_master_v1.histology_final`:

```sql
CASE WHEN p.histology_final ILIKE '%hashimoto%' THEN 1 ELSE 0 END AS has_clt
CASE WHEN p.histology_final ILIKE '%multinodular%' THEN 1 ELSE 0 END AS has_mng
CASE WHEN p.histology_final ILIKE '%graves%' THEN 1 ELSE 0 END AS has_graves
```

But `histology_final` only carries malignant histology subtype labels
(PTC, follicular carcinoma, NIFTP, FTUMP, MTC, anaplastic, ...) and is
NULL for 1,837 / 3,375 (54%) of the cohort — every benign-only patient.
The regex therefore matched zero rows.

In v1.2.2 this was diagnosed as biology ("the source field captures
only malignant categorizations") and the M4 background-pathology step
was collapsed into M3 with the original numbering preserved. The
manuscript explicitly disclosed this in:

- Methods §2.3 (cascade): "M0, M1, M2, M3, M5, M6 — note the M4 gap"
- Methods §2.7 (covariates): "Pre-specified covariates dropped after
  data audit … `has_clt`, `has_mng`, `has_graves` (all-zero columns
  because the source field captures only malignant categorizations)"
- Limitations §5: "Three background-pathology covariates (`has_clt`,
  `has_mng`, `has_graves`) were dropped (all-zero source columns)."

It was a data-extraction bug, not biology. The correct source tables
(`pub_canonical.canonical_path_benign_patient_rollup_v1`,
`pub_canonical.canonical_benign_diagnosis_v1`) carry these flags and
have full population coverage.

---

## What v1.3 fixes

### Data layer (mig_318)

`pub_workspace.m048_v4_patient_master_v1` rebuilt from
`m048_v3_patient_master_v1` with corrected derivations:

- `has_clt = any_lymphocytic_thyroiditis OR any_hashimotos`
  (`canonical_path_benign_patient_rollup_v1`)
- `has_mng = any_mng` (same source)
- `has_graves = any_graves` (same source)
- `has_follicular_adenoma = any_follicular_adenoma` (same source)

Plus 11 granular benign-diagnosis flags from
`canonical_benign_diagnosis_v1` (substernal goiter, nodular hyperplasia,
adenomatoid nodules, Hurthle adenoma, etc.).

Plus a 25-flag PMH panel from `canonical_pmh_patient_rollup_v1` at the
"probable_or_better" tier (hypertension, diabetes, obesity, CKD, CAD,
COPD, depression, hyperthyroidism, hypothyroidism, autoimmune thyroid
hx, family hx thyroid, family hx cancer, prior cancer hx, radiation
exposure, smoking, etc.).

A dedup bug in `canonical_benign_diagnosis_v1` (84 duplicate
research_id rows) was caught during validation and resolved with
`LOGICAL_OR` aggregation in the v4 master CTE.

DDL: `studies/m048_racial_disparities_tirads/M048_v4_bigquery_queries.sql`.

### Analysis layer (mig_318b)

Cursor prompt at
`cursor_prompts/M048_v4_full_recompute_cursor_prompt_20260507.md`.

- Cascade restored to M0 → M1 → M2 → M3 → **M4** → M5 → M6 (M4 is back
  with `has_clt + has_mng + has_graves + has_follicular_adenoma`).
- Comorbidity sensitivity arm expanded from {hyper, htn, dm} to the
  full 13-flag PMH panel; refit per-patient (no aggregation needed at
  v4).
- Bethesda-stratified Model B + B-interaction re-fit on v4.
- Per-nodule cluster-robust Model F-Nodule re-fit on v4 nodule master
  with M4 added.
- All 7 sensitivity arms re-fit on v4 (arm D "no-CLT" now actually
  excludes a meaningful fraction of the cohort — was a no-op in v3).
- Mediation bootstrap extended from 5 to 8 mediators (adds `has_mng`,
  `has_clt`, `has_graves`) × 2 races = 16 rows. Seed = 42 unchanged.
- Independent recompute extended from 5 to 7 assertions (adds Black
  M3→M4 and M4→M5 attenuation magnitude).

### Manuscript layer

This file. Sections requiring text edits:

| Section | v1.2.2 says | v1.3 must say |
|---|---|---|
| Title page | "manuscript v1.2.2" | "manuscript v1.3" |
| Title-page word counts | Tables 16 / Figures 10 | Recount after Table 8c added |
| Abstract | "M5+comorb OR 0.443" | New v4 number after expanded PMH panel |
| §2.3 Cascade | "six-fit cascade … M0, M1, M2, M3, M5, M6 — note the M4 gap" | "seven-fit cascade M0 → M6, M4 = background pathology" |
| §2.3 13 predictor terms in M6 | 13 terms | Recount (becomes 17 with M4 covariates added) |
| §2.7 Covariates dropped | "has_clt, has_mng, has_graves (all-zero columns)" | DELETE; add note that v1.2.2 dropped these in error and v1.3 corrects |
| §2.6 Sensitivity arms | "v1.2 added a comorbidity sensitivity arm" | "v1.3 expands comorbidity arm to full PMH panel" |
| §3.2 Cascade | M0=0.317 → M6=0.442 | Same M0; M4 row added; new M5 / M6 |
| §3.8a Comorbidity arm | hyper+htn+dm only | Full 13-flag panel result |
| §3.9 Covariate balance | has_clt/mng/graves SMDs not reported | Now reported (MNG SMD ≈ +0.50 for Black) |
| §4.2 Interpretation | "Black M6 OR resists … even comorbidity adjustment" | Strengthen: "and now also resists adjustment for the largest measured background-pathology imbalance (MNG)" or document attenuation if M4 attenuates the Black OR meaningfully |
| §5 Limitations | "Three background-pathology covariates were dropped (all-zero source columns)" | DELETE entirely |
| Table 1c | hyper, hypo, autoimmune thy, dm, htn, obesity, ckd, cad, copd, depression | Add prior cancer hx, radiation exposure, family hx cancer, smoking from the v4 PMH panel |
| Table 2 | M0, M1, M2, M3, M5, M6 | M0..M6 (M4 restored) |
| Table 3 | 24 fitted coefficients in M6 | Recount (becomes ≈28 with M4 covariates) |
| Table 8b | hyper + htn + dm only | Full 13-flag PMH panel |
| **NEW Table 8c** | — | Background-pathology distribution by race (from `m048_v4_background_path_by_race_v1`) |
| Cover letter | "six-fit adjustment cascade with 13 predictor terms" | "seven-fit cascade with 17 predictor terms" |

---

## Why this matters scientifically

The MNG signal is the most meaningful single change. v3-era covariate
balance reported zero SMDs on `has_mng`. v4 reveals that Black patients
in the operative cohort are **23 percentage points more likely** to
carry a MNG diagnosis than White patients (75.7% vs 52.8%, SMD +0.50).
This is direct quantitative support for the existing manuscript
interpretation that Black patients are over-represented in the
operative cohort because of benign indications (compressive MNG,
substernal goiter, symptomatic large nodules) at lower imaging risk.
v1.2.2 told this story qualitatively in the Discussion — v1.3 will be
able to anchor it to a specific covariate.

If M4 attenuates the Black M6 OR meaningfully (i.e., Black OR moves
from ~0.44 toward 1.0), that strengthens the indication-mix
interpretation and deflates the residual-disparity claim. If M4 does
**not** attenuate the Black M6 OR meaningfully, that strengthens the
upstream-pathway-routing interpretation: even after accounting for
the indication mix, the residual disparity persists.

Either way, v1.3 produces a sharper paper than v1.2.2.

---

## Reproducibility

- BQ release tag: `pub_v1_1` (unchanged from v1.2.2)
- Migrations layered on: mig_318 (DDL), mig_318b (analysis)
- Bootstrap seed: 42 (unchanged)
- Race color encoding: unchanged (`Black=#1f4e79`, `White=#7a7a7a`,
  `Asian=#c55a11`)
- v3 BQ tables retained for traceability (do NOT drop)
- v1.2.2 manuscript files retained at
  `M048_submission_package_v1.2.2/`

## Audit trail

- Data Feedback Log: `DFL-M048-20260507-01` (THYROID_MANUSCRIPT base,
  Data Feedback Log table). Logged before any data edit per HARD RULE
  3 in CLAUDE.md.
- Cowork chat: 2026-05-07 (M048 background-path bugfix session)
- Linked Linear issue: TBD — open a `type:bugfix severity:major
  source:cowork-audit` issue against the M048 project after Cursor
  completes mig_318b. Title: "M048 v3 background-pathology covariates
  all-zero — fixed in v4 / mig_318".
