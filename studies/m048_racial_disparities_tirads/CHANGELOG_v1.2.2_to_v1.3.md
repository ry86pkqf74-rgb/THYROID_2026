# Changelog: M048 v1.2.2 → v1.3

## Summary

v1.3 fixes a data-extraction bug (Bug C from the v3 log) that caused the M4
"background pathology" step of the adjustment cascade to collapse into M3 in
manuscript v1.2.2. The fix re-derives the three background-pathology covariates
from the correct canonical tables (mig_318 BQ DDL) and re-runs the full
analytic pipeline (mig_318b Python refit).

---

## Bug fixed: has_clt / has_mng / has_graves all-zero in v3

**Root cause.** `m048_v3_patient_master_v1` (mig_317b) derived these covariates
using `CASE WHEN histology_final ILIKE '%hashimoto%'` (and similar regexes).
`histology_final` only carries malignant histology subtype labels (PTC,
follicular carcinoma, NIFTP, FTUMP, MTC, anaplastic, …) and is NULL for all
1,837 / 3,375 patients who are benign-only. The regex therefore never matched
and the three covariates were 0 for every analytic patient.

**Consequence.** The M4 cascade step (`M3 + has_clt + has_mng + has_graves
+ has_follicular_adenoma`) was numerically identical to M3, so it was dropped
in the v1.2.2 cascade analysis (manuscript §2.7 documents the collapse).

**Fix (mig_318).** Re-derived from:
- `pub_canonical.canonical_path_benign_patient_rollup_v1` → `has_clt` (any
  lymphocytic thyroiditis OR Hashimoto's), `has_mng`, `has_graves`,
  `has_follicular_adenoma`
- `pub_canonical.canonical_benign_diagnosis_v1` → granular benign-diagnosis
  flags (substernal goiter, nodular hyperplasia, hurthle adenoma, etc.)
- `pub_canonical.canonical_pmh_patient_rollup_v1` → full PMH panel

**Post-fix prevalences (analytic cohort, n = 3,121):**
| Variable | Black | White | Asian |
|---|---|---|---|
| has_mng | 75.7% | 52.8% | 42.6% |
| has_clt | 12.0% | 25.2% | 22.5% |
| has_graves | 4.0% | 1.9% | 1.5% |

---

## Cascade changes (v1.2.2 → v1.3)

| Step | v1.2.2 (Black OR) | v1.3 (Black OR) | Note |
|---|---|---|---|
| M0 | 0.317 | 0.317 | Unchanged |
| M1 | 0.352 | 0.352 | Unchanged |
| M2 | 0.352 | 0.352 | Unchanged |
| M3 | 0.359 | 0.359 | Unchanged |
| **M4** | *skipped (all-zero)* | **0.444** | **Restored** |
| M5 | — (was M4 in v1.2.2) | 0.516 | +has_clt/mng/graves |
| M6 | ~0.442 (v1.2.2) | 0.513 | Full model |

**New finding.** Adding background pathology (M3→M4) attenuates the Black
disparity by **23.7%** (OR 0.359→0.444). MNG is the dominant contributor given
its large prevalence imbalance (Black 75.7% vs White 52.8%, SMD +0.50).
The residual Black M6 OR (0.513, 95% CI 0.421–0.625, p<0.001) is slightly
larger than v1.2.2 (~0.442) because v1.3's M5/M6 include the has_mng/clt/graves
regressors that partially absorb the race signal.

---

## Other changes in v1.3

### Expanded comorbidity sensitivity arm (Table 8b)
- v1.2 Table 8b used aggregated cell-level regression (race × Bethesda × TR on
  summary counts). v1.3 runs per-patient logistic regression on the v4 master.
- Two formulas: v1.2-equivalent (race + TR + Bethesda + DM + HTN + hyperthyroid)
  and a full 17-covariate PMH panel.

### Mediation extended to 8 mediators
- Original 5 mediators retained; adds `has_mng`, `has_clt`, `has_graves`.
- 8 mediators × 2 race targets (Black, Asian) = 16 rows.

### Sensitivity arm D now non-trivial
- v1.2 arm D ("no-CLT") removed 0 patients (has_clt was all-zero). v1.3 removes
  684 CLT-positive patients (analytic n drops from 3,121 → 2,496). Black M6 OR
  in arm D: 0.491 (0.393–0.613, p<0.001) — disparity persists after excluding
  autoimmune thyroiditis patients.

### New Table 8c — Background Pathology by Race
- Reports has_clt, has_mng, has_graves, has_follicular_adenoma, and granular
  benign-diagnosis flags by race strat, providing clinical context for the M4 step.

### Table 1c — Comorbidities/PMH regenerated
- Now sourced from `canonical_pmh_patient_rollup_v1` (17 variables, structured
  extraction) rather than v1.2 NLP-derived ad-hoc joins.

### Covariate balance (Table 11) — has_mng/has_clt/has_graves SMDs now non-zero
- has_mng: Black SMD vs White = +0.50 (large imbalance, the key confounder)
- has_clt: Black SMD vs White = -0.34 (Black patients less likely to have CLT)
- has_graves: Black SMD vs White = +0.13

---

## What is UNCHANGED in v1.3

- Analytic cohort: n = 3,375 total, n = 3,121 analytic (race_strat ∈
  Black/White/Asian AND max_tirads_category_ever IS NOT NULL)
- Race strat coding (Black / White / Asian; reference = White)
- Race color encoding (Black=#1f4e79, White=#7a7a7a, Asian=#c55a11)
- Sensitivity arm definitions A–G (scope unchanged; only inputs differ for D)
- Disparity-direction table (TR4/TR5 × race biology descriptors)
- Table 1 demographics, Table 1b pre-op modality/timing, Table 9 disparity
  direction — numbers unchanged
- Bootstrap seed = 42
- Per-nodule cluster-robust Model F-Nodule approach (formula updated to include
  M4 background terms)

---

## Files updated

| File | Change |
|---|---|
| `v4/m048_v4_cascade.csv` | M0..M6 with M4 restored |
| `v4/m048_v4_cascade_attenuation.csv` | M3→M4, M3→M6, M4→M5 per race |
| `v4/m048_v4_full_model_OR.csv` | All M6 coefficients |
| `v4/m048_v4_bethesda_stratified_TR_ROM.csv` | Model B |
| `v4/m048_v4_bethesda_stratified_TR_interaction.csv` | Model B-int |
| `v4/m048_v4_per_nodule_cluster_robust.csv` | F-Nodule (M4 added) |
| `v4/m048_v4_comorbidity_sensitivity.csv` | v1.2-equiv + full PMH |
| `v4/m048_v4_sensitivity_arms.csv` | Arms A–G (arm D now non-trivial) |
| `v4/m048_v4_mediation.csv` | 16 rows (8 med × 2 races) |
| `v4/m048_v4_background_path_by_race.csv` | Background path by race |
| `v4/m048_v4_pmh_by_race.csv` | PMH panel by race |
| `v4/m048_v4_covariate_balance.csv` | SMD table (has_mng SMD +0.50 now shown) |
| `v4/m048_v4_qa_gates.csv` | 4 new v4-specific gates |
| `M048_submission_package_v1.3/M048_v1.3_tables_for_manuscript.xlsx` | All tables regen'd |
| `v4/verification/independent_recompute_v4_report.md` | 7/7 assertions |

---

## Migration signoffs

- `mig_318` — BQ DDL (v4 patient/nodule/QA/background-path/PMH tables)
- `mig_318b` — Python analysis pipeline re-fit
