# Multimodal Prediction Study — Model Results Summary

**Generated:** 2026-03-18 06:30  
**Seed:** 42  
**Evaluation:** 5-fold stratified cross-validation  
**Outcome:** `recurrence_flag` (any recurrence, binary)  

## 1. Outcome Justification

| Criterion | Value |
|-----------|-------|
| Outcome | `recurrence_flag` |
| Prevalence | 46.7% (1,933 / 4,136) |
| Missingness | 0% |
| Type | Binary (0/1) |
| Manuscript-safe | YES |

**Rationale:** `recurrence_flag` was selected as the primary endpoint because it is:
- Fully available (0% missing) across the entire analysis-eligible cancer cohort
- Has adequate event prevalence (46.7%) for stable model training
- Clinically meaningful — recurrence is the primary outcome of interest for thyroid cancer prognosis
- Manuscript-safe with documented provenance (see outcome_prevalence.csv)
- Alternative endpoints either had excessive missingness (`structural_recurrence_flag`: 53% missing),
  extreme class imbalance (`has_complication_record`: 1.0%), or were molecular markers rather than outcomes

## 2. Feature Set Definitions

### Set A — Structured Clinical Only (baseline)

| # | Feature |
|---|---------|
| 1 | `age_at_surgery` |
| 2 | `sex` |
| 3 | `race` |
| 4 | `histology_final` |
| 5 | `t_stage` |
| 6 | `n_stage` |
| 7 | `m_stage` |
| 8 | `ete_grade` |
| 9 | `tumor_size_cm` |
| 10 | `ln_examined_count` |
| 11 | `margin_status` |
| 12 | `vascular_invasion` |
| 13 | `ajcc8_stage` |
| 14 | `ata_risk` |
| 15 | `macis_score` |
| 16 | `ames_risk_group` |
| 17 | `ages_score` |
| 18 | `surg_procedure_type` |

### Set B — Structured + Imaging

Set A plus:

- `tirads_worst`
- `tirads_worst_category`
- `imaging_nodule_size_cm`
- `n_nodules_imaged`
- `has_tirads_validated`
- `tirads_nodule_max_mm`

### Set C — Structured + Imaging + Notes-Derived

Set B plus:

- `braf_positive`
- `ras_positive`
- `tert_positive`
- `molecular_platform`
- `molecular_risk_tier`
- `bethesda_worst`
- `tg_nadir`
- `tg_last_value`
- `tg_rising_flag`
- `lab_completeness_score`
- `n_lab_values`
- `n_analyte_groups`
- `has_fna_data`
- `has_molecular_data`
- `n_molecular_tests`

## 3. Model Performance Comparison

| Feature Set | Model | AUC (CV mean±SD) | AUC (pooled) | Brier Score | Avg Precision | N Features |
|-------------|-------|-------------------|--------------|-------------|---------------|------------|
| A_structured | logistic | 0.9752±0.0045 | 0.9750 | 0.0413 | 0.9809 | 18 |
| A_structured | xgboost | 0.9802±0.0027 | 0.9801 | 0.0301 | 0.9856 | 18 |
| B_struct_imaging | logistic | 0.9750±0.0048 | 0.9748 | 0.0417 | 0.9808 | 24 |
| B_struct_imaging | xgboost | 0.9806±0.0042 | 0.9805 | 0.0299 | 0.9857 | 24 |
| C_struct_img_notes | logistic | 0.9955±0.0008 | 0.9955 | 0.0173 | 0.9956 | 39 |
| C_struct_img_notes | xgboost | 0.9988±0.0006 | 0.9988 | 0.0071 | 0.9989 | 39 |

## 4. Incremental Gain by Modality

| Comparison | Model | AUC Δ | Brier Δ |
|------------|-------|-------|---------|
| A→B (+ imaging) | logistic | -0.0002 | +0.0004 |
| B→C (+ notes) | logistic | +0.0205 | -0.0244 |
| A→C (total) | logistic | +0.0203 | -0.0240 |
| A→B (+ imaging) | xgboost | +0.0004 | -0.0002 |
| B→C (+ notes) | xgboost | +0.0182 | -0.0228 |
| A→C (total) | xgboost | +0.0186 | -0.0230 |

## 5. Calibration Metrics

| Feature Set | Model | Brier Score | ECE |
|-------------|-------|-------------|-----|
| A_structured | logistic | 0.0413 | 0.1663 |
| A_structured | xgboost | 0.0301 | 0.2430 |
| B_struct_imaging | logistic | 0.0417 | 0.1657 |
| B_struct_imaging | xgboost | 0.0299 | 0.2461 |
| C_struct_img_notes | logistic | 0.0173 | 0.1439 |
| C_struct_img_notes | xgboost | 0.0071 | 0.1622 |

## 6. Top Predictors (Feature Set C, Full Model)

### Logistic

| Rank | Feature | Importance |
|------|---------|------------|
| 1 | `ajcc8_stage` | 6.8431 |
| 2 | `age_at_surgery` | 4.0997 |
| 3 | `ages_score` | 3.0162 |
| 4 | `tg_rising_flag` | 2.6047 |
| 5 | `ata_risk` | 1.2280 |
| 6 | `macis_score` | 1.1506 |
| 7 | `ames_risk_group` | 0.9821 |
| 8 | `tumor_size_cm` | 0.5695 |
| 9 | `n_analyte_groups` | 0.5223 |
| 10 | `n_lab_values` | 0.4649 |
| 11 | `histology_final` | 0.3884 |
| 12 | `tg_nadir` | 0.3879 |
| 13 | `race` | 0.3605 |
| 14 | `m_stage` | 0.2925 |
| 15 | `molecular_risk_tier` | 0.2837 |

### Xgboost

| Rank | Feature | Importance |
|------|---------|------------|
| 1 | `ajcc8_stage` | 0.6652 |
| 2 | `ata_risk` | 0.1523 |
| 3 | `tg_rising_flag` | 0.0688 |
| 4 | `age_at_surgery` | 0.0675 |
| 5 | `ames_risk_group` | 0.0178 |
| 6 | `n_lab_values` | 0.0076 |
| 7 | `ages_score` | 0.0035 |
| 8 | `macis_score` | 0.0031 |
| 9 | `tumor_size_cm` | 0.0017 |
| 10 | `tg_last_value` | 0.0017 |
| 11 | `ln_examined_count` | 0.0012 |
| 12 | `n_nodules_imaged` | 0.0010 |
| 13 | `t_stage` | 0.0010 |
| 14 | `has_tirads_validated` | 0.0008 |
| 15 | `molecular_risk_tier` | 0.0007 |

## 7. Figures

### ROC Curves
![ROC Curves](model_results/figures/roc_curves.png)

### Calibration Plots
![Calibration](model_results/figures/calibration_plot.png)

## 8. Reproducibility

- **Random seed:** 42
- **CV:** 5-fold stratified
- **Imputation:** Median (via `SimpleImputer`)
- **Scaling:** StandardScaler
- **Logistic:** L2, C=1.0, saga solver, max_iter=2000
- **XGBoost:** GradientBoostingClassifier, n_estimators=300, max_depth=4, lr=0.05, subsample=0.8
- **Script:** `train_multimodal_models.py`
- **Input:** `candidate_modeling_dataset.parquet` (N=4136)
