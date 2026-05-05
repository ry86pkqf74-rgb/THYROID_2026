# Cursor Prompt: M025 ACR TI-RADS Diagnostic Performance Analysis

**Agent:** Sonnet 4.6 (Composer 2.0) — diagnostic test performance with ROC/AUC; Sonnet handles this efficiently  
**Estimated time:** 2–2.5 hours  
**Date:** 2026-05-04

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, cohort view `manuscript_workspace.cohort_m025_tirads_performance_v1` (N=3,375). This cohort includes patients with ACR TI-RADS ultrasound classification and surgical pathology outcomes.

### TI-RADS × Malignancy Distribution (pre-computed):

| TI-RADS | Benign | Malignant | Total | ROM |
|---|---|---|---|---|
| TR1 (benign) | 244 | 96 | 340 | 28.2% |
| TR2 (not suspicious) | 203 | 96 | 299 | 32.1% |
| TR3 (mildly suspicious) | 612 | 233 | 845 | 27.6% |
| TR4 (moderately suspicious) | 259 | 233 | 492 | 47.4% |
| TR5 (highly suspicious) | 578 | 821 | 1,399 | 58.7% |

**Total:** 1,896 benign, 1,479 malignant (43.8% overall malignancy rate)

### Key Columns:
- `tirads_resolved` — VARCHAR (TR1–TR5)
- `tirads_best_category_v12`, `tirads_worst_category_v12` — alternative TI-RADS scores
- `tirads_best_score_v12`, `tirads_worst_score_v12` — numeric scores
- `tirads_reliability_v12` — quality flag
- `tirads_source_v12` — data source
- `tirads_n_sources_v12` — number of TI-RADS assessments
- `imaging_nodule_size_cm` — ultrasound nodule size
- `dominant_nodule_size_cm` — dominant nodule size (may differ from imaging)
- `n_us_exams` — number of ultrasound exams
- `is_malignant` — boolean (surgical pathology gold standard)

## Task

### 1. Diagnostic Performance at Each TI-RADS Threshold

For each possible threshold (≥TR2, ≥TR3, ≥TR4, ≥TR5):
- Sensitivity, specificity, PPV, NPV with 95% Wilson CI
- Positive/negative likelihood ratios
- Diagnostic odds ratio

### 2. ROC Curve and AUC

Using the numeric TI-RADS score (`tirads_best_score_v12` or `tirads_worst_score_v12`):
- Generate ROC curve
- Calculate AUC with 95% CI (DeLong method or bootstrap)
- Youden's J statistic for optimal threshold

### 3. ROM by TI-RADS Category

For each TR1–TR5:
- ROM with 95% Wilson CI
- ROM excluding NIFTP
- Compare against ACR published expected ROMs:
  - TR1: <2%, TR2: <5%, TR3: <5%, TR4: 5–20%, TR5: >20%

### 4. Nodule Size Analysis

- Mean/median nodule size by TI-RADS category
- ROM stratified by TI-RADS × nodule size (<1cm, 1–2cm, 2–4cm, >4cm)
- Does TI-RADS perform differently for small vs large nodules?

### 5. Multi-TI-RADS Assessment

For patients with `tirads_n_sources_v12 > 1`:
- Compare `tirads_best_category_v12` vs `tirads_worst_category_v12`
- Agreement/discordance rate
- Which (best vs worst) has better diagnostic performance?
- `tirads_reliability_v12` impact on performance

### 6. Subgroup Analyses

Stratify diagnostic performance by:
- Age (<45, 45–65, >65)
- Sex
- Histology subtype (PTC vs FTC vs other)
- Bethesda category (if available — `bethesda_final`)
- Time period

### 7. Unnecessary FNA Analysis (ACR guideline)

Per ACR TI-RADS recommendations, FNA is indicated for:
- TR5: ≥10mm, TR4: ≥15mm, TR3: ≥25mm, TR2/TR1: not recommended

Calculate:
- How many FNAs were "unnecessary" by ACR guidelines (below size threshold)?
- How many cancers would have been missed if strict ACR size thresholds were followed?

### 8. Output

Save to `studies/m025_tirads_performance/`:
- `tirads_diagnostic_performance.csv` — sensitivity/specificity at each threshold
- `roc_data.csv` — ROC curve points
- `rom_by_tirads.csv` — ROM with CI
- `nodule_size_analysis.csv` — size × TI-RADS cross-tab
- `unnecessary_fna_analysis.csv` — guideline compliance
- `tirads_performance_summary.tex` — LaTeX tables
- `roc_curve.png` — ROC curve figure

### 9. Upload to MotherDuck

Create `manuscript_workspace.m025_tirads_analysis_v1` with patient-level diagnostic fields.

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR
- `tirads_resolved` is the primary TI-RADS column (VARCHAR: TR1–TR5)
- High overall malignancy rate (43.8%) reflects surgical selection bias — all patients went to surgery
- ACR expected ROMs assume general population; our cohort is surgical, so direct comparison is for context only
- Boolean columns: use `IS TRUE`
- Check `tirads_reliability_v12` — exclude low-reliability scores in sensitivity analysis
