# Cursor Prompt: Generate Table 1 Demographics for Tier 1 Manuscripts

**Agent:** Claude Sonnet 4.6 (Composer 2.0)  
**Estimated time:** 2 hours  
**Date:** 2026-05-04

## Context

We have a MotherDuck database `thyroid_canonical_publication_v1_0` with 10,871 thyroid surgery patients. Five Tier 1 manuscripts are ready for Table 1 generation. The cohort views already exist in `manuscript_workspace` schema.

## Pre-computed Table 1 Summary Data (from Cowork session 2026-05-04)

### Full Cohort (M032 — 25-Year Descriptive, N=10,871)
- Age: mean 51.6 ± 15.2, median 52 (IQR 40–63)
- Sex: female 8,459 (77.8%), male 2,412 (22.2%) — note: stored as lowercase `female`/`male`
- Race: White 5,266 (48.5%), Black/AA 4,168 (38.4%), Asian 476 (4.4%), Other 143 (1.3%), Unknown 721 (6.6%)
- BMI (n=2,085, 19%): mean 30.5 ± 7.6, median 29.2
- Surgery: total thyroidectomy 5,999 (55.2%), hemithyroidectomy 4,432 (40.8%), other 422 (3.9%)
- Malignant: 4,019 (37.0%)
- Histology (of malignant): PTC 3,075 (74.3%), follicular 486 (11.7%), MTC 149 (3.6%), NIFTP 117 (2.8%)
- AJCC 8th (n=4,016): Stage I 1,539 (38.3%), II 1,652 (41.1%), III 9 (0.2%), IVB 816 (20.3%)
- Recurrence: 514 (4.7%)
- Complications: 400 (3.7%)

### Cohort-Specific Summaries

| Manuscript | N | Age (mean±SD) | Female % | Malignant % | Recurrence | Complications |
|---|---|---|---|---|---|---|
| M029 — FNA concordance | 2,401 | 51.0±16.0 | 72.1% | 97.2% | 277 | 111 |
| M025 — TI-RADS performance | 3,375 | 53.6±14.9 | 79.7% | 43.8% | 171 | 249 |
| M037 — LN metastasis | 2,234 | 48.9±15.8 | 71.7% | 100% | 290 | 94 |
| M047 — Frozen section | 10,871 | 51.6±15.2 | 77.8% | 37.0% | 514 | 400 |

## Task

For each of the 5 manuscripts, create a publication-quality Table 1 as a Python script that:

1. Connects to MotherDuck using `duckdb.connect('md:thyroid_canonical_publication_v1_0')`
2. Queries the cohort view (e.g., `manuscript_workspace.cohort_m032_descriptive_25yr_v1`) joined to `canonical_patient_master`
3. Generates a formatted Table 1 with:
   - Continuous variables: mean ± SD, median (IQR)
   - Categorical variables: n (%)
   - Missing data reported as n (%) missing

### Variables to include (use exact column names):
- `age_at_surgery` — continuous
- `sex` — categorical (values: `female`, `male`)
- `race` — categorical
- `bmi_combined` — continuous (sparse: ~19%)
- `surg_procedure_type` — categorical (values: `total_thyroidectomy`, `hemithyroidectomy`, `other`, `isthmusectomy`, `unknown`)
- `is_malignant` — boolean
- `histology_final` — categorical (malignant patients only)
- `ajcc8_stage_group` — categorical (values: `I`, `II`, `III`, `IVA`, `IVB`)
- `tumor_size_cm_dominant` — continuous
- `ln_positive_final` — continuous (count)
- `ete_grade_final` — categorical
- `any_recurrence_flag` — boolean
- `any_confirmed_complication_flag` — boolean
- `rai_received_reconciled` — boolean (note: 862 TRUE in full cohort, better coverage than `rai_received_flag` which only has 583)
- `molecular_tested_confirmed` — boolean
- `braf_positive_final` — boolean (among tested)

### Type casting note:
`canonical_patient_master.research_id` is VARCHAR. Cohort view research_id may also be VARCHAR. Cast as needed.

### Output format:
- Save each Table 1 as CSV to `studies/table1_outputs/`
- Also generate a combined LaTeX table for each manuscript

### Manuscript-specific notes:
- **M032**: Include year-of-surgery distribution (decade bins)
- **M029**: Stratify by Bethesda category (`bethesda_final`) and concordance status
- **M025**: Stratify by TI-RADS category and include nodule characteristics
- **M037**: Stratify by LN positive vs negative; include `ln_rollup_total_examined`, `ln_rollup_total_positive`, dissection type from `manuscript_workspace.ln_dissection_lnd_resolved_v2`
- **M047**: Stratify by frozen section performed vs not (`frozen_any_performed_flag`)

## Connection

```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```

Use the MotherDuck token from `.env.motherduck` or environment variable `MOTHERDUCK_TOKEN`.
