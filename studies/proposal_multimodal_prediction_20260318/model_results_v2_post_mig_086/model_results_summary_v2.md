# Multimodal Prediction Study — V2 Results (Post-mig_086, Auditable)

**Generated:** 2026-05-06 16:31  
**Version:** v2 (post-mig_086 pub_canonical)  
**Seed:** 42  
**Evaluation:** HELD-OUT test split (70/15/15 stratified by outcome)  
**Outcome:** `any_recurrence_flag` (binary)  
**Cohort:** `manuscript_workspace.cohort_multimodal_recurrence_v1`  

---

## 1. Why V2?

V1 (2026-03-18) reported AUC=0.975–0.999 using 5-fold CV only on a local parquet
file. V2 addresses three audit concerns:

| Concern | V1 | V2 Fix |
|---------|-----|--------|
| No held-out test set | CV pooling only — all data seen during training | Stratified 70/15/15 split; test set untouched during model selection |
| Feature leakage | tg_rising_flag, tg_nadir, tg_last_value — postop Tg IS part of the biochemical recurrence definition | Excluded; see leakage table below |
| Source data | Local parquet (pre-mig_086, pre-canonical) | pub_canonical via MotherDuck, post-mig_086 |

**Note:** V1 results are not deleted — they are annotated as deprecated pending v2 confirmation.

---

## 2. Leakage Exclusions (V1 → V2)

| Feature | Leakage Type | Reason |
|---------|-------------|--------|
| `tg_rising_flag` | POSTOP_LAB | part of biochemical recurrence definition (Tg > 1.0 ng/mL without structural disease) |
| `tg_nadir` | POSTOP_LAB | derived from same Tg surveillance window as recurrence outcome |
| `tg_last_value` | POSTOP_LAB | postoperative measurement, not preoperative predictor |
| `lab_completeness_score` | POSTOP_LAB | measures completeness of postoperative Tg surveillance |
| `n_lab_values` | POSTOP_LAB | count of postoperative lab measurements |
| `n_analyte_groups` | POSTOP_LAB | postoperative analyte diversity |
| `ata_risk` | DERIVED_SCORE | ATA risk is calibrated to predict recurrence; circular |
| `macis_score` | DERIVED_SCORE | MACIS calibrated to predict recurrence; circular |
| `ames_risk_group` | DERIVED_SCORE | AMES calibrated to predict mortality/recurrence; circular |
| `ages_score` | DERIVED_SCORE | AGES calibrated to predict mortality; quasi-circular |
| `ajcc8_stage` | DERIVED_SCORE | AJCC8 stage is a composite that summarizes T/N/M; T+N+M included directly in Set A |

---

## 3. Cohort

| Criterion | Value |
|-----------|-------|
| Source | pub_canonical (thyroid_canonical_publication_v1_0) |
| Filter | Malignant + ≥6mo FU + any_recurrence_flag non-NULL + ≥1 multimodal source |
| Total N | 2231 |
| Recurrence events | 393 (17.6%) |
| Train N | 1561 |
| Val N | 335 |
| Test N | 335 (held-out, never seen during training) |
| Split method | Stratified random 70/15/15 by outcome |

**Note:** V1 prevalence was 46.7% (1,933/4,136) on a local parquet file without
the ≥6mo follow-up filter and without the multimodal source requirement.
V2 prevalence is 17.6% (393/2231), which is more
clinically plausible for thyroid cancer (published rates: 5–30%).

---

## 4. Model Performance — HELD-OUT TEST AUC (primary endpoint)

| Feature Set | Model | Train AUC | Val AUC | **Test AUC** | Brier (test) | Avg Prec (test) |
|-------------|-------|-----------|---------|------------|--------------|-----------------|
| A_structured | logistic | 0.8015 | 0.7235 | **0.8328** | 0.1120 | 0.5462 |
| A_structured | xgboost | 0.9834 | 0.7046 | **0.8500** | 0.1172 | 0.5047 |
| B_struct_imaging | logistic | 0.7997 | 0.7353 | **0.8336** | 0.1105 | 0.5528 |
| B_struct_imaging | xgboost | 0.9894 | 0.6990 | **0.8504** | 0.1156 | 0.4984 |
| C_struct_img_notes | logistic | 0.8063 | 0.7366 | **0.8604** | 0.1092 | 0.5668 |
| C_struct_img_notes | xgboost | 0.9941 | 0.7459 | **0.8449** | 0.1241 | 0.4707 |

⚠ = test AUC > 0.90; requires leakage investigation before acceptance.

---

## 5. Incremental Gain by Modality (Test AUC)

| Comparison | Model | ΔTest AUC |
|------------|-------|-----------|
| A→B (+imaging) | logistic | +0.0008 |
| B→C (+mol/FNA) | logistic | +0.0268 |
| A→C (total) | logistic | +0.0276 |
| A→B (+imaging) | xgboost | +0.0004 |
| B→C (+mol/FNA) | xgboost | -0.0055 |
| A→C (total) | xgboost | -0.0051 |

---

## 6. Feature Importance (Top 5 per model, Feature Set C)

### Logistic

| Rank | Feature | Importance | Leakage Note |
|------|---------|------------|--------------|
| 1 | `n_tumors_path` | 1.203543 | none |
| 2 | `multifocal_flag_path` | 0.569653 | none |
| 3 | `t_stage_encoded` | 0.480113 | none |
| 4 | `ete_encoded` | 0.330223 | none |
| 5 | `worst_bethesda_num` | 0.320824 | none |

### Xgboost

| Rank | Feature | Importance | Leakage Note |
|------|---------|------------|--------------|
| 1 | `n_tumors_path` | 0.252148 | none |
| 2 | `age_at_surgery` | 0.127530 | none |
| 3 | `tumor_size_cm` | 0.108576 | none |
| 4 | `multifocal_flag_path` | 0.085472 | none |
| 5 | `nodule_size_max_mm` | 0.071438 | none |

---

## 7. Discussion: Why V1 AUC Was Likely Inflated

V1 reported AUC=0.999 for XGBoost (Set C, CV-pooled). The most likely causes:

1. **No true held-out test set.** Cross-validation pools *predicted* probabilities
   across folds, but each fold uses 80% of the data for training. The model has
   effectively seen all patients during training. This is methodologically valid
   for reporting CV AUC, but it cannot substitute for a held-out test set when the
   goal is to generalize to unseen patients.

2. **Feature leakage.** `tg_rising_flag` is part of the *definition* of
   biochemical recurrence (rising Tg > 1.0 ng/mL without structural disease). When
   this feature appears in Set C, the model is being given the outcome itself as a
   predictor. `tg_nadir` and `tg_last_value` are similarly postoperative measures
   derived from the same surveillance window that generates the recurrence flag.

3. **Derived scoring systems.** Set A included `ata_initial_risk`, `macis_score`,
   `ames_risk_group`, and `ages_score`. These composite scoring systems are
   *calibrated* to predict recurrence and mortality in thyroid cancer — they
   contain essentially the same information as the outcome, which explains why
   Set A alone achieved AUC=0.975 with 5-fold CV.

4. **Pre-canonical parquet.** V1 pulled from a local parquet file that may have
   included patients from a development/derivation set used when building the
   canonical tables.

V2 does not claim v1 was 'wrong' — it may have been correct on its derivation
set. V2 is the **auditable version**: transparent data source (pub_canonical),
held-out test set designed before training, and explicit leakage exclusions.

---

## 8. Reproducibility

- **Random seed:** 42
- **Split:** Stratified 70/15/15 (train/val/test)
- **Split file:** /tmp/multimodal_v2_splits.parquet (transient, not tracked)
- **Cohort view:** `manuscript_workspace.cohort_multimodal_recurrence_v1`
- **DB:** thyroid_canonical_publication_v1_0 (post-mig_086)
- **Imputation:** Median (sklearn SimpleImputer)
- **Scaling:** StandardScaler
- **Logistic:** L2, C=1.0, saga solver, max_iter=2000
- **XGBoost:** GradientBoostingClassifier, n_estimators=300, max_depth=4, lr=0.05, subsample=0.8
- **Script:** `train_multimodal_models_v2.py`

---

## 9. V1 Deprecation Notice

`studies/proposal_multimodal_prediction_20260318/model_results/model_results_summary.md`
is **deprecated**. Numbers from that file should not appear in any manuscript
submission or conference abstract. Use v2 results from this file. V1 file is
preserved for audit trail but annotated as deprecated in the Manuscript Feedback Log.

---

*Generated by train_multimodal_models_v2.py — post-mig_086 canonical re-run*