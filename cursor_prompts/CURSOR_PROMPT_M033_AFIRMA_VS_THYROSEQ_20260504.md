# Cursor Prompt: M033 Afirma vs ThyroSeq Molecular Platform Comparison

**Agent:** Sonnet 4.6 (Composer 2.0) — straightforward platform comparison with diagnostic performance metrics; Sonnet handles this efficiently  
**Estimated time:** 2–2.5 hours  
**Date:** 2026-05-04

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, analytic view `manuscript_workspace.m033_afirma_thyroseq_analytic_v1` (N=1,286). This cohort includes all molecularly tested patients with resolved platform assignments.

### Platform Family Distribution:
| Platform | N | BRAF+ | Malignant | Recurred |
|---|---|---|---|---|
| ThyroSeq | 632 | 96 | 382 (60.4%) | 45 (7.1%) |
| Afirma | 197 | 55 | 105 (53.3%) | 12 (6.1%) |
| ThyroSeq+Afirma (dual) | 167 | 132 | 111 (66.5%) | 20 (12.0%) |
| unknown | 168 | 0 | 111 (66.1%) | 17 (10.1%) |
| multi_panel_unknown | 66 | 38 | 62 (93.9%) | 10 (15.2%) |
| single_gene (BRAF only) | 55 | 55 | 49 (89.1%) | 8 (14.5%) |
| Quest | 1 | 0 | 1 | 1 |

### Resolved Platform Versions:
- Afirma: GEC (N=72, pre-2017), GSC (N=53, post-2017), version_unknown (N=72)
- ThyroSeq: v2 (N=17, pre-2018), v3 (N=533, post-2018), version_unknown (N=82)
- Transition dates: Afirma GEC→GSC ~2017-06, ThyroSeq v2→v3 ~2018-01

### BRAF Audit Tiers:
- tier_2_single_source_confirmed: 376
- tier_3_ihc_only: 1
- tier_5_negative_only: 1

### Available Columns (32):
`research_id`, `age_at_surgery`, `sex`, `race`, `histology_final`, `histology_pub_category`, `tumor_size_cm_dominant`, `is_malignant`, `ajcc8_stage_group`, `mol_platform_original`, `mol_platform_resolved`, `mol_platform_family`, `mol_platform_confidence`, `braf_positive_final`, `braf_variant`, `braf_detection_method_v11`, `ras_positive_final`, `ras_subtype`, `mol_has_fusion`, `molecular_risk_tier`, `mol_n_distinct_genes`, `mol_first_test_date`, `braf_audit_tier`, `braf_method_audit`, `braf_discordance_flag`, `ete_grade_clean`, `surg_procedure_type`, `any_recurrence_flag`, `any_confirmed_complication_flag`, `rai_received_reconciled`, `ata_risk_category`, `ln_positive_final`

## Task

### 1. Platform-Specific Diagnostic Performance

For ThyroSeq-only (N=632) and Afirma-only (N=197) patients:
- Malignancy rate (ROM) with 95% Wilson CI
- If Bethesda data is accessible from CPM (`bethesda_final`), stratify ROM by Bethesda III/IV (the indeterminate categories where molecular testing adds value)
- Sensitivity and specificity of each platform for malignancy prediction (requires defining "positive" test — for ThyroSeq: any mutation detected; for Afirma: suspicious result)
- NPV for each platform (the clinically actionable metric — a negative test = can avoid surgery)

**NOTE:** We may not have the raw molecular test result (positive/negative/suspicious) in the view. Check if `molecular_risk_tier` can serve as a proxy (high/intermediate = positive, low/wild_type = negative). If not, note this limitation.

### 2. Molecular Mutation Spectrum by Platform

For each platform:
- BRAF V600E detection rate
- RAS mutation detection rate
- Fusion detection rate (`mol_has_fusion`)
- Distribution of `molecular_risk_tier`
- Number of genes tested (`mol_n_distinct_genes`) — ThyroSeq v3 tests ~80+ genes, Afirma GSC tests ~10 genes

### 3. Version Comparison

**Afirma GEC vs GSC:**
- Compare malignancy rate, specificity (GSC should have better specificity)
- Proportion needing surgery (GSC should reduce unnecessary surgeries)

**ThyroSeq v2 vs v3:**
- Compare mutation detection rates, malignancy rate
- v3 includes RNA fusions — does fusion detection differ?

### 4. Dual-Platform Patients (N=167)

- Cross-tabulate ThyroSeq result vs Afirma result
- Concordance rate for BRAF (note: 99/160 are discordant per M083 audit — investigate why)
- Concordance rate for RAS
- When discordant, which platform was "correct" (matched final pathology)?
- Clinical impact of discordance — did discordant results lead to different management?

### 5. Outcomes by Platform

Compare ThyroSeq-only vs Afirma-only:
- Recurrence rate with 95% CI
- Surgical procedure type distribution (total vs hemi)
- RAI receipt rate
- ATA risk category distribution
- **Adjust for confounders:** Use logistic regression controlling for age, sex, tumor size, histology, AJCC stage

### 6. Utilization Trends

Using `mol_first_test_date`:
- Plot molecular testing volume by year
- Show platform adoption curves (Afirma vs ThyroSeq over time)
- Identify institutional switch points

### 7. BRAF Detection Method Analysis

Using `braf_detection_method_v11` and `braf_audit_tier`:
- Compare BRAF detection by NGS vs IHC vs NLP
- For BRAF+ patients: what fraction were detected by each method?
- Concordance between detection methods

### 8. Output

Save to `studies/m033_platform_comparison/`:
- `platform_diagnostic_performance.csv` — ROM, sensitivity, specificity by platform
- `mutation_spectrum_by_platform.csv` — BRAF/RAS/fusion rates
- `version_comparison.csv` — GEC vs GSC, v2 vs v3
- `dual_platform_concordance.csv` — agreement analysis
- `outcomes_by_platform.csv` — recurrence, surgery type, RAI
- `utilization_trends.csv` — volume by year and platform
- `platform_comparison_summary.tex` — LaTeX tables

### 9. Upload to MotherDuck

Create `manuscript_workspace.m033_platform_analysis_v1` with patient-level platform performance fields.

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR
- Boolean columns: use `IS TRUE` / `IS NOT TRUE`
- `mol_platform_family` is the primary grouping column (ThyroSeq, Afirma, ThyroSeq+Afirma, unknown, etc.)
- `mol_platform_resolved` has version-specific labels (ThyroSeq_v3, Afirma_GSC, etc.)
- `mol_platform_confidence` has values: low, ambiguous, medium, high — consider excluding low-confidence assignments in sensitivity analysis
- The "unknown" platform group (N=168) should be excluded from primary ThyroSeq vs Afirma comparison but reported separately
- `single_gene` (N=55, all BRAF-only) are NOT platform-tested — they had targeted BRAF testing only, not a comprehensive panel
- Dual-platform patients (N=167) had BOTH tests — analyze separately from single-platform
- Sex: lowercase `female`/`male`
- Bethesda data may need to be joined from CPM: `SELECT bethesda_final FROM canonical_patient_master WHERE research_id = ...`
