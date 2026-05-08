# Phase B — Deterministic TIRADS Multi-System Scorers

**Tracking table:** `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
**Phase B scope:** B.1 ACR TI-RADS 2017, B.2 Kwak 2011, B.3 K-TIRADS 2021, B.4 C-TIRADS 2020, B.5 SRU 2005, B.6 Park / T-US 2009 (this document).

---

## B.6 — Park / T-US 2009 Logistic-Regression Model

### Overview

Park JY et al. (*Thyroid* 2009;19:1257–1264; DOI:10.1089/thy.2008.0021; PMID:19754280) proposed a logistic-regression model to predict the probability of thyroid nodule malignancy from 12 binary ultrasound features. The model computes:

```
logit z = β₀ + β₁X₁ + β₂X₂ + … + β₁₂X₁₂
Pus = 1 / (1 + exp(−z))
```

Results are categorised into T-US / TI-RADS P1–P5 bands.

This cohort implements the model **three ways** (all persisted in `park_coefs_v1.json`):
1. **park_2009_original** — original published β values (awaiting Logan to supply from paper PDF).
2. **park_cosmos_validation** — external validation refit (awaiting Logan to cite source paper).
3. **park_cohort_refit** — held-out GLM on this cohort, `random_state=42`, 70/30 split.

### 12 Binary X Variables

| Var | Description | BQ mapping |
|---|---|---|
| X1 `park_x1_taller` | Taller-than-wide shape | `shape = 'taller_than_wide'` |
| X2 `park_x2_halo` | Perinodular halo | `JSON_VALUE(halo_jsonb,'$.presence') = 'present'` |
| X3 `park_x3_well_circumscribed` | Well-circumscribed margin (smooth) | `margins = 'smooth'` |
| X4 `park_x4_microlobulation` | Microlobulated margin | `margins = 'microlobulated'` |
| X5 `park_x5_infiltrative_margin` | Infiltrative margin (irregular/ill-defined/ETE on US) | `margins IN ('irregular','ill_defined','extrathyroidal_extension') OR ete_us_jsonb.presence IN (...)` |
| X6 `park_x6_marked_hypo` | Marked hypoechogenicity | `echogenicity = 'very_hypoechoic'` |
| X7 `park_x7_hypo` | Hypoechogenicity | `echogenicity = 'hypoechoic'` |
| X8 `park_x8_homogeneous` | Homogeneous echotexture | `homogeneous_echotexture = TRUE` |
| X9 `park_x9_mainly_cystic` | Mainly cystic | `composition IN ('cystic','predominantly_cystic')` |
| X10 `park_x10_solid` | Solid | `composition IN ('solid','predominantly_solid')` |
| X11 `park_x11_microcalc` | Microcalcification | `'punctate_echogenic_foci' IN echogenic_foci JSON array` |
| X12 `park_x12_abnormal_ln` | Abnormal lymph node at exam | `us_nodule_ln_context_v1.has_suspicious_ln_within_60d = TRUE` |

**NULL-default policy:** when a primitive feature column is NULL for a nodule, the corresponding X variable defaults to `FALSE` (absence-of-finding = negative). This is a deliberate clinical choice per Park's original framing (absence of feature documentation ≈ feature not present). Coverage counts are tracked in `pub_workspace.qc_phase_b6_park_x_completeness_v1`.

### T-US Probability Category Bands

| Category | Probability range | Clinical meaning |
|---|---|---|
| P0 | — | Normal exam (no nodule; skip — scorer is per-nodule) |
| P1 | 0–7% | Highly benign |
| P2 | 8–23% | Probably benign |
| P3 | 24–50% | Indeterminate |
| P4 | 51–90% | Probably malignant |
| P5 | 91–100% | Highly malignant |

Boundary rule: probability exactly at boundary → current category (≤ threshold). For example, probability = 0.07 → P1; probability = 0.08 → P2.

### Coefficient Sources

All three sets are stored in `scripts/manifests/park_coefs_v1.json`.

**park_2009_original:** β values from the original Park JY 2009 *Thyroid* paper (Table 3, multivariate logistic regression). **STATUS: PENDING — Logan must supply from paper PDF.** The coefficients are paywalled (Mary Ann Liebert publisher) and could not be retrieved from open-access sources during Phase B.6 agent run.

**park_cosmos_validation:** External validation refit. **STATUS: PENDING — Logan to identify source paper.** If no published refit with explicit βs is available, this set will be aliased to park_2009_original with a note.

**park_cohort_refit:** Populated by `scripts/417b_park_cohort_refit.py` after the above two sets are filled. Trains on all nodules in `canonical_us_nodule_v2` with all 12 X non-null AND a final-pathology label within ±90 days of the US exam.

### Cohort Refit Specification

- Split: 70/30 train/test, `random_state=42`
- Model: `sklearn.linear_model.LogisticRegression(penalty='l2', C=1.0, solver='lbfgs', max_iter=1000)`
- No standardization (binary inputs)
- Acceptance: train AUC ≥ 0.75, test AUC ≥ 0.70. If below → `confidence='low'` in manifest; do not sign off.
- Train/test split persisted to `pub_workspace.park_cohort_refit_split_v1` for reproducibility.

### Audit Results *(populated after scorer runs)*

**Distribution per coefficient set:**
```
[TO BE FILLED after Logan supplies park_2009_original coefficients and 417_canonical_us_nodule_tirads_park_v1.py runs]
```

**Inter-set three-way concordance:**
```
[TO BE FILLED]
```
Expected: ≥ 70% three-way agreement on benign-vs-suspicious binary.

**Per-set AUC vs final pathology:**
```
[TO BE FILLED]
```
Expected: cohort-refit AUC highest (trained on this cohort). If park_2009_original beats cohort-refit → β miscoding flag.

### Rollback Plan

1. Identify the issue (wrong β, wrong BQ column mapping, etc.).
2. Restore `canonical_us_nodule_tirads_multisystem_v1` from the pre-Park snapshot in `pub_workspace.cpm_pre_tirads_multisystem_acr_snapshot_v1`.
3. Correct `scripts/manifests/park_coefs_v1.json` or the scorer SQL.
4. Re-run `scripts/417_canonical_us_nodule_tirads_park_v1.py`.
5. Re-run audit and update this README.

### Scripts

| Script | Purpose |
|---|---|
| `scripts/417_canonical_us_nodule_tirads_park_v1.py` | Main scorer: computes X vars + 3×logit/prob/cat, CTAS-rebuilds table |
| `scripts/417b_park_cohort_refit.py` | Trains cohort GLM, updates manifest, writes split table |
| `tests/test_park_scorer.py` | 38 unit tests (green) |
| `scripts/manifests/park_coefs_v1.json` | Coefficient manifest (park_2009_original = PENDING) |

---

## Action Required

**Logan must supply the following before Phase B.6 can be completed:**

1. **Park 2009 original β values** — from Table 3 (or the equivalent multivariate logistic regression table) in: Park JY, Lee HJ, Jang HW, Kim HK, Yi JH, Lee W, Kim SH. *Thyroid.* 2009;19(11):1257–1264. The paper requires institutional journal access (Mary Ann Liebert). The 12 values needed: β₀ (intercept), β₁–β₁₂ for X1–X12.

2. **Cosmos validation paper citation** — any peer-reviewed paper that explicitly refits the Park 2009 model on a different cohort and reports the β values. If none is available, note "no published external refit found" and park_cosmos_validation will be aliased to park_2009_original.

Once Logan supplies these values, update `scripts/manifests/park_coefs_v1.json` and run:
```bash
# 1. Cohort refit
python scripts/417b_park_cohort_refit.py --project thyroid-canonical-pub-2026

# 2. Score all nodules
python scripts/417_canonical_us_nodule_tirads_park_v1.py --project thyroid-canonical-pub-2026

# 3. Tests (already passing)
pytest tests/test_park_scorer.py -v
```
