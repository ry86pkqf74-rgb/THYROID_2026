# Cursor Prompt: M029 FNA Cytology Concordance Analysis

**Agent:** Sonnet 4.6 (Composer 2.0) — straightforward diagnostic performance metrics; Sonnet handles sensitivity/specificity/PPV/NPV calculations efficiently  
**Estimated time:** 1.5–2 hours  
**Date:** 2026-05-04

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, cohort view `manuscript_workspace.cohort_m029_fna_concordance_v1` (N=2,401). This cohort includes patients with FNA cytology results (Bethesda classification) and subsequent surgical pathology.

### Bethesda × Malignancy Distribution (pre-computed):

| Bethesda | Benign (not malignant) | Malignant | Total | ROM |
|---|---|---|---|---|
| I (non-diagnostic) | 2 | 69 | 71 | 97.2% |
| II (benign) | 24 | 360 | 384 | 93.8% |
| III (AUS/FLUS) | 23 | 304 | 327 | 93.0% |
| IV (FN/SFN) | 9 | 298 | 307 | 97.1% |
| V (suspicious) | 3 | 240 | 243 | 98.8% |
| VI (malignant) | 7 | 1,062 | 1,069 | 99.3% |

**NOTE:** These ROM values are extremely high across ALL categories (>93%), which is unusual and reflects selection bias — this cohort was selected for concordance analysis, meaning most patients went to surgery (enriching for malignancy). The manuscript must acknowledge this and compare against published ROM benchmarks.

### Key Columns:
- `bethesda_final` — INT (1–6), Bethesda category
- `histology_final` — final surgical pathology diagnosis
- `is_malignant` — boolean
- `bethesda_index_nodule` — index nodule FNA result
- FNA-related NLP columns may exist — check `information_schema`

## Task

### 1. Bethesda–Histology Concordance Matrix

Create a cross-tabulation of `bethesda_final` × `histology_final` (using publication-friendly histology rollup):
- PTC (all PTC variants including metastatic PTC)
- FTC (follicular carcinoma)
- MTC (medullary thyroid carcinoma)
- NIFTP
- FTUMP
- Other malignant
- Benign

### 2. Risk of Malignancy (ROM) by Bethesda Category

For each Bethesda category:
- ROM = malignant / total × 100 with 95% Wilson CI
- ROM excluding NIFTP (per 2023 Bethesda guidelines)
- Compare against published benchmarks (2017 Bethesda System 3rd edition implied ROMs):
  - I: 5–10%, II: 0–3%, III: 6–18%, IV: 10–40%, V: 45–60%, VI: 94–96%

### 3. Diagnostic Performance Metrics

Using Bethesda V–VI as "positive" and I–IV as "negative":
- Sensitivity, specificity, PPV, NPV with 95% CI
- Also calculate with Bethesda IV–VI as "positive" (alternative threshold)

Using Bethesda VI only as "positive":
- Same metrics

### 4. Subgroup Analyses

Stratify ROM by:
- Time period (pre-2015, 2015–2019, 2020+) — use `surgery_year` or `date_of_surgery`
- Age group (<45, 45–65, >65)
- Tumor size (<1cm, 1–2cm, 2–4cm, >4cm) — note: FNA size ≠ surgical size
- Molecular testing status (`molecular_tested_confirmed`)

### 5. Concordance with Molecular

For patients with both FNA and molecular testing:
- Cross-tabulate Bethesda III/IV (indeterminate) × molecular result
- What fraction of indeterminate FNA patients had molecular testing?
- Did molecular testing change the ROM for Bethesda III/IV?

### 6. Output

Save to `studies/m029_fna_concordance/`:
- `bethesda_histology_crosstab.csv`
- `rom_by_bethesda.csv` — ROM with CI, including vs published benchmarks
- `diagnostic_performance.csv` — sensitivity/specificity/PPV/NPV at each threshold
- `rom_subgroup_analyses.csv` — by time period, age, size, molecular
- `fna_concordance_summary.tex` — LaTeX tables for manuscript

### 7. Upload to MotherDuck

Create `manuscript_workspace.m029_fna_analysis_v1` with patient-level concordance fields.

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR
- `bethesda_final` is INT (1–6), not string
- The extremely high ROM across all categories is expected due to surgical selection bias — address in manuscript discussion
- NIFTP (N=117 total, 116 benign) should be analyzed both included and excluded from malignancy
- Boolean columns: use `IS TRUE`, never compare with strings
