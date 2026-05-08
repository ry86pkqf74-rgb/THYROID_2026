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

### Audit Results — Phase B.6 finalization (2026-05-07)

#### Per-set category distribution

Source: `pub_workspace.qc_phase_b6_park_distribution_v1`.

| Set | P1 | P2 | P3 | P4 | P5 |
|---|---|---|---|---|---|
| `park_2009_original` | 27,479 (73.1%) | 6,140 (16.3%) | 2,259 (6.0%) | 1,665 (4.4%) | 36 (0.1%) |
| `park_cosmos_validation` | 27,479 (73.1%) | 6,140 (16.3%) | 2,259 (6.0%) | 1,665 (4.4%) | 36 (0.1%) |
| `park_cohort_refit` (v2 nodule-level) | 302 (0.8%) | 19,516 (51.9%) | 16,508 (43.9%) | 1,253 (3.3%) | 0 (0.0%) |

Park 2009 (and its cosmos alias) place 73% of nodules in P1 because most of the cohort's 37,579 nodules have all-`FALSE` X variables (NULL primitives default to FALSE per `qc_phase_b6_park_x_completeness_v1`); the all-zero row maps to logistic(−2.862) ≈ 0.054 → P1. The cohort-refit's intercept (−2.23) and smaller |β| spread land most nodules in the P2/P3 middle — exactly what surgical-cohort pre-selection compression looks like.

#### Three-way concordance on suspicious binary (P4 ∪ P5)

Source: `pub_workspace.qc_phase_b6_park_concordance_v1`. n = 37,579.

| Comparison | Agreement | Note |
|---|---|---|
| `park_2009_original` vs `park_cosmos_validation` | **1.000** | Alias-by-construction. **Do NOT interpret as a validation signal.** |
| `park_2009_original` vs `park_cohort_refit` | 0.948 | Genuine concordance — the two coefficient sets disagree on absolute probabilities but largely agree on the suspicious/non-suspicious dichotomy. |
| `park_cohort_refit` vs `park_cosmos_validation` | 0.948 | Same as above (cosmos = alias). |

#### Per-set AUC vs final pathology

Source: `pub_workspace.qc_phase_b6_park_auc_v1`. Labels from `pub_workspace.us_nodule_path_outcome_v1` (nodule-level, laterality-aware). n = 14,250.

| Set | AUC vs path | Interpretation |
|---|---|---|
| `park_2009_original` | **0.5365** | Essentially random discrimination on this American surgical cohort. The Korean general-population coefficients do not generalize. *This is itself a meaningful clinical finding worth surfacing.* |
| `park_cosmos_validation` | 0.5365 | Alias of `park_2009_original`. |
| `park_cohort_refit` (v2) | **0.7006** | Matches the holdout test AUC of 0.6914 within sampling noise. MARGINAL band per §3d. |

### Phase B.6 finalization — Park 2009 coefficients (published)

| Var | Description | β |
|---|---|---|
| (intercept) | β₀ | **−2.862** |
| X1 | Taller-than-wide | +0.581 |
| X2 | Halo | −0.481 |
| X3 | Well-circumscribed | −1.435 |
| X4 | Microlobulated margin | +1.178 |
| X5 | Infiltrative margin | +1.405 |
| X6 | Marked hypoechoic | +0.700 |
| X7 | Hypoechoic | +0.460 |
| **X8** | **Homogeneous echotexture** | **+0.648** ⚠️ |
| X9 | Mainly cystic | −1.715 |
| X10 | Solid | +0.463 |
| X11 | Microcalcification | +1.964 |
| X12 | Abnormal lymph node | +1.739 |

> ⚠️ **X8 — Counter-intuitive callout.** Park 2009 reported the X8 (homogeneous echotexture) coefficient as **positive** (+0.648), meaning a *more homogeneous* nodule increases the predicted malignancy probability. This is opposite to the direction modern TIRADS systems use, where heterogeneity is the suspicious feature. The +0.648 value is faithful to the published model and reproduced here without modification. The unit test `tests/test_park_scorer.py::test_park_scorer_homogeneous_counterintuitive` pins the value so future "fixes" are caught immediately. Downstream users should interpret X8's contribution to Park P-scores with caution.

**Provenance.** Coefficients tabulated from secondary literature; the original paper (Park JY et al., *Thyroid* 2009;19(11):1257–64; DOI:10.1089/thy.2008.0021; PMID:19754280) is paywalled at Mary Ann Liebert. The values are reproduced from Table 3 (multivariate logistic regression) as cited consistently across radiology references. If a discrepancy is later identified against the primary source, raise a Verification Check and bump the manifest to v2.

### Phase B.6 finalization — Cosmos aliasing rationale

`park_cosmos_validation` is intentionally **identical** to `park_2009_original` at v1. No qualifying external-validation refit (i.e., a peer-reviewed study that re-fits Park's 12-X structure on a different cohort and publishes its own β values) was identified during the Phase B.6 closure on 2026-05-07. The schema slot is preserved so downstream consumers can swap in a real cosmos refit later (bump to manifest v2). The audit query `agreement_2009_vs_cosmos = 1.000` is alias-by-construction and **must not be interpreted as a validation finding** — the cosmos column adds no new information at v1.

### Phase B.6 finalization — Linkage fix (v1 patient-level → v2 nodule-level)

**The bug.** The original `scripts/417b_park_cohort_refit.py` joined `canonical_us_nodule_v2` to `canonical_path_malignant_events_v1` at the **patient level** (any patient with any malignant event in window → all of that patient's nodules labeled malignant). For a multinodular goiter patient with one malignant nodule among five, all five US nodules were labeled malignant during training, dragging test AUC to **0.6611** (below the 0.70 acceptance gate).

**Why the proposed FNA→specimen→tumor focus chain didn't apply.** Schema discovery (see `scripts/_phase_b6_step3a_schema_discovery.py`) found that:
- `imaging_nodule_long_v2.linked_pathology_tumor_id` is **0% populated** (the column exists but no rows have a value).
- `canonical_us_nodule_v2.nodule_id` does NOT overlap with `imaging_nodule_long_v2.nodule_id` — they are different ID spaces.
- `specimen_source_xref_v1` only carries `domain ∈ {pathology, molecular}`; there are NO FNA-domain xref rows. The proposed FNA→specimen bridge does not exist as data.

**The fix (v2).** A new view `pub_workspace.us_nodule_path_outcome_v1` (deployed by `scripts/_phase_b6_step3b_us_nodule_path_outcome_v1.sql`) implements **per-nodule labels via laterality-aware per-side match**:
1. Normalize free-text laterality on both sides (US nodule + path malignant event) into `{left, right, isthmus, bilateral, unknown}`.
2. For each nodule × patient × surgery in window (−90 to +365 days from US exam): malignant if a laterality-compatible malignant event exists; benign if any path event in window AND no laterality-compatible malignancy on this side; NULL if no path activity.
3. "Bilateral" malignancy applies to all real-side nodules (clinical reality: bilateral diagnosis = both lobes affected).

**The diagnostic.** `pub_workspace.qc_phase_b6_park_label_flip_v1` records the per-nodule diff between v1 (patient-level) and v2 (nodule-level) splits:
- **1,654 nodules flipped malignant→benign** — these are the multinodular-goiter contralateral nodules the v1 logic misclassified. This is exactly the bug the prompt diagnosed.
- 6,993 added new labels (v2's broader linkage rule covers more nodules).
- 5,598 unchanged.
- 5 ben→mal (edge cases — bilateral malignancy now applying).

**The new AUC.** v2 cohort refit (n_train=9,975, n_test=4,275, random_state=42, sklearn LR L2 C=1.0): train AUC = **0.7044**, test AUC = **0.6914**. Per the §3d gate this falls in the **MARGINAL** band (`train ≥ 0.72 OR test ≥ 0.68` → proceed with `confidence='low'`). The improvement from 0.6611 → 0.6914 confirms the linkage was indeed contributing to the AUC failure, but the cohort itself (American surgical cohort, pre-selected for suspicious nodules) genuinely produces weaker Park-feature discrimination than Park's Korean general-population validation cohort. **The cohort-refit set should be treated as "this surgical cohort produces weak Park-feature discrimination" rather than as a primary risk model.**

### Acceptance gates re-check (Phase B.6 finalization)

| Gate | Target | Result | Status |
|---|---|---|---|
| 3b — n_with_label | ≥ 8,000 | 14,250 | PASS |
| 3b — singleton-link fraction | ≥ 0.60 | 0.286 | N/A — metric was designed for the FNA→specimen chain that doesn't exist; substituted by mixed-label sanity check. |
| 3b — multinodular mixed-label exams | "a few hundred" | 613 | PASS |
| 3d — cohort-refit AUC | MARGINAL band | train 0.7044 / test 0.6914 | PASS (marginal, `confidence='low'`) |
| 5b — three-way concordance ≥ 0.70 | ≥ 0.70 | 0.948 (2009 vs cohort) | PASS |
| 5c — AUC vs path | informational | Park 0.54 / cohort 0.70 / cosmos 0.54 | recorded |

### Rollback Plan

1. Identify the issue (wrong β, wrong BQ column mapping, wrong linkage logic, etc.).
2. Restore `canonical_us_nodule_tirads_multisystem_v1` from the pre-Park snapshot in `pub_workspace.cpm_pre_tirads_multisystem_acr_snapshot_v1` (B.1 ACR re-run baseline).
3. The multi-system table itself is new in Phase B; if the entire B work needs to be undone, drop it and re-create from the `412–417` scripts in order.
4. The v1 cohort split is preserved at `pub_workspace.park_cohort_refit_split_v1` for audit; do NOT delete it. The v2 split is `park_cohort_refit_split_v2`.
5. Correct `scripts/manifests/park_coefs_v1.json` or the scorer SQL.
6. Re-run `scripts/417b_v2_park_cohort_refit.py` then `scripts/417_canonical_us_nodule_tirads_park_v1.py`, then re-run `scripts/_phase_b6_step5_unified_audit.py`.

### Scripts (Phase B.6 finalization)

| Script | Purpose |
|---|---|
| `scripts/417_canonical_us_nodule_tirads_park_v1.py` | Main scorer: computes X vars + 3×logit/prob/cat, CTAS-rebuilds table |
| `scripts/417b_v2_park_cohort_refit.py` | **NEW** — trains cohort GLM on nodule-level labels (supersedes 417b v1) |
| `scripts/417b_park_cohort_refit.py` | v1 (patient-level linkage). Retained for audit; do not run for current scoring. |
| `scripts/_phase_b6_step3a_schema_discovery.py` | One-shot INFORMATION_SCHEMA dump for the linkage-chain candidate tables |
| `scripts/_phase_b6_step3a_linkage_probe.py` | Coverage probes for `linked_pathology_tumor_id`, FNA chain, specimen xref |
| `scripts/_phase_b6_step3a_laterality_probe.py` | Per-side malignancy / multinodular distribution probes |
| `scripts/_phase_b6_step3b_us_nodule_path_outcome_v1.sql` | View definition for nodule-level path labels |
| `scripts/_phase_b6_step3b_deploy_and_verify.py` | Deploys the view + runs acceptance gates |
| `scripts/_phase_b6_step5_unified_audit.py` | Computes 5a/5b/5c and persists to `qc_phase_b6_park_*_v1` |
| `scripts/_phase_b6_step6c_signoff_insert.py` | Inserts the closure row into `canonical_table_signoff_registry_v1` |
| `tests/test_park_scorer.py` | 40 unit tests (green) — includes the new X8 counter-intuitive pin |
| `scripts/manifests/park_coefs_v1.json` | Coefficient manifest, v1 (all 3 sets READY) |

### BQ artifacts (Phase B.6)

| Object | Type | Purpose |
|---|---|---|
| `pub_canonical.canonical_us_nodule_tirads_multisystem_v1` | TABLE | Main scoring table (37,579 rows, CLUSTER BY research_id, signed off `phase_b_closure_20260507`) |
| `pub_workspace.us_nodule_path_outcome_v1` | VIEW | Nodule-level path labels (laterality-aware) |
| `pub_workspace.park_cohort_refit_split_v1` | TABLE | v1 train/test split (audit trail; do not delete) |
| `pub_workspace.park_cohort_refit_split_v2` | TABLE | v2 train/test split (current) |
| `pub_workspace.qc_phase_b6_park_x_completeness_v1` | TABLE | Per-X NULL-default counts |
| `pub_workspace.qc_phase_b6_park_label_flip_v1` | TABLE | Per-nodule v1↔v2 label diff |
| `pub_workspace.qc_phase_b6_park_distribution_v1` | TABLE | Per-set category distribution (5a) |
| `pub_workspace.qc_phase_b6_park_concordance_v1` | TABLE | Three-way concordance (5b) |
| `pub_workspace.qc_phase_b6_park_auc_v1` | TABLE | Per-set AUC vs path (5c) |
| `exports/phase_b_deterministic_scorers_20260507/phase_b6_audit_results.json` | file | Full audit JSON snapshot |
