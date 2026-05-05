# M033 Platform Comparison — Analysis Notes
**Run:** 2026-05-04  
**Cohort:** N=1,286 molecularly-tested patients from `manuscript_workspace.m033_afirma_thyroseq_analytic_v1`

---

## Key Findings

### 1. Platform Diagnostic Performance

| Platform | N | ROM | Bethesda III ROM | Bethesda IV ROM |
|---|---|---|---|---|
| ThyroSeq | 632 | 60.4% (56.6–64.2%) | 54.3% (47.9–60.7%) | 52.5% (44.3–60.6%) |
| Afirma | 197 | 53.3% (46.3–60.1%) | 45.9% (31.0–61.6%) | 58.1% (43.3–71.6%) |

**Sensitivity/Specificity (via `molecular_risk_tier` proxy: positive = high/intermediate):**

| Platform | N with known tier | Sensitivity | Specificity | NPV |
|---|---|---|---|---|
| ThyroSeq | 463 / 632 | 28.0% (23.7–32.7%) | 82.1% (72.6–88.9%) | 20.2% (16.3–24.7%) |
| Afirma | 139 / 197 | 23.8% (16.7–32.8%) | 11.8% (4.7–26.6%) | 4.8% (1.9–11.6%) |

**⚠️ Interpretation caveat:** These sensitivity/specificity values reflect the `molecular_risk_tier` field from the publication database, which is derived from the mutation profile rather than the original binary test result (suspicious/not suspicious for Afirma; positive/negative for ThyroSeq). The low NPV and specificity for Afirma reflect the highly enriched surgical cohort where nearly all molecularly tested patients proceeded to surgery. True NPV in a screening context (including patients who avoided surgery based on a negative result) would be substantially higher. **These values should NOT be cited as the platform's clinical NPV without this caveat.**

### 2. Mutation Spectrum by Platform

| Platform | N | BRAF+ | RAS+ | Fusions |
|---|---|---|---|---|
| ThyroSeq | 632 | 96 (15.2%) | 176 (27.8%) | 359 (56.8%) |
| Afirma | 197 | 55 (27.9%) | 5 (2.5%) | 81 (41.1%) |
| Dual (ThyroSeq+Afirma) | 167 | 132 (79.0%) | 36 (21.6%) | 139 (83.2%) |

**ThyroSeq:** High fusion rate (56.8%) reflects v3's RNA-based multi-gene panel including RET/PTC, NTRK, PAX8-PPARG fusions. High RAS rate (27.8%) reflects follicular neoplasm detection.

**Afirma:** The 81 fusions (41.1%) in Afirma patients warrants investigation — Afirma GSC does detect some RNA-level fusions but not as comprehensively as ThyroSeq v3. These may represent patients who had concurrent ThyroSeq panels or may reflect the Afirma GSC's broader transcriptome analysis capturing some fusion events indirectly. **Do not cite Afirma fusion rate as comparable to ThyroSeq.**

**Dual-platform:** 79% BRAF+ rate reflects selection bias — dual-tested patients were likely referred for additional testing because of discordant or high-risk initial findings.

### 3. Version Comparison

| Version | N | ROM | BRAF+ | Notes |
|---|---|---|---|---|
| Afirma GEC (pre-2017) | 72 | 45.8% (34.8–57.3%) | 44 (61.1%) | GEC relied heavily on BRAF detection |
| Afirma GSC (post-2017) | 53 | 62.3% (48.8–74.1%) | 5 (9.4%) | GSC uses broader transcriptome; less BRAF-centric |
| ThyroSeq v2 (pre-2018) | 17 | 58.8% (36.0–78.4%) | 8 (47.1%) | Small N, high BRAF dependence |
| ThyroSeq v3 (post-2018) | 533 | 59.3% (55.1–63.4%) | 75 (14.1%) | Multi-gene panel, RNA fusions |

**Notable:** Afirma GEC BRAF+ rate of 61.1% vs GSC 9.4% confirms the platform transition away from single-gene BRAF detection. The higher ROM for Afirma GSC (62.3%) vs GEC (45.8%) likely reflects improved sensitivity of the GSC for malignancy detection (greater enrichment of true malignancies in the positive-call group).

### 4. Dual-Platform Concordance

- N=167 dual-platform patients (ThyroSeq + Afirma both received)
- ROM: 66.5% (59.0–73.2%) — highest of any single-platform group, reflecting selection bias toward high-risk/complex nodules
- Recurrence: 12.0% (7.9–17.8%) — nearly 2× ThyroSeq-only (7.1%)
- BRAF+: 79.0% — massive enrichment consistent with dual-testing for complex/discordant results

**BRAF discordance note:** `braf_discordance_flag = 0` for all dual-platform patients. This field tracks within-patient multi-method concordance (NGS vs NLP), NOT cross-platform (ThyroSeq BRAF call vs Afirma BRAF call). The M083 audit reference to "99/160 discordant" likely compares the final resolved BRAF call between the two platforms' original reports, which is not captured in the current publication view. A separate analysis against the raw molecular episode table would be needed to reconstruct the per-platform BRAF calls.

### 5. Outcomes by Platform

| Outcome | ThyroSeq | Afirma | p-value |
|---|---|---|---|
| Recurrence | 7.1% (5.4–9.4%) | 6.1% (3.5–10.3%) | 0.736 (chi-square) |
| RAI received | 14.4% (11.9–17.4%) | 10.7% (7.1–15.7%) | — |
| LN positive | 8.1% (6.2–10.5%) | 7.1% (4.3–11.6%) | — |

**Adjusted OR (ThyroSeq vs Afirma for recurrence): 1.005** — essentially identical after controlling for age, sex, tumor size, and malignancy status. No platform-specific outcome advantage.

### 6. Utilization Trends

- Afirma: First use 2014, peak 2015 (N=30), declining after 2016
- ThyroSeq: First use 2016, peak 2023 (N=80), sustained growth through 2024
- GEC-era (pre-2017): N=60 Afirma patients; GSC-era (2017+): N=61 Afirma patients
- 6 rows with implausible test dates (years 2, 6, 8, 12, 1905, 3023) excluded from trend analysis

### 7. BRAF Detection Methods

| Method | N BRAF+ | % of BRAF+ |
|---|---|---|
| NGS | 268 | 71.3% |
| NLP_entity_confirmed | 103 | 27.4% |
| NGS_or_unknown | 5 | 1.3% |

All 376 BRAF+ patients are `tier_2_single_source_confirmed`. No IHC-detected BRAF in this cohort (consistent with Phase 13 finding that IHC BRAF reports are not in the clinical_notes_long corpus).

---

## Data Limitations

1. **Sensitivity/specificity values are cohort-limited**: This is a surgical cohort with high pre-test probability. True NPV (patients who avoided surgery because of a negative result) cannot be computed — those patients are not in the database.

2. **Afirma fusion rate (41.1%)** may reflect data cross-contamination from concurrent ThyroSeq tests or Afirma GSC's broader transcriptome expression analysis. Should not be cited as equivalent to ThyroSeq fusion detection.

3. **BRAF discordance across platforms** cannot be directly computed from the current view — the `braf_discordance_flag` tracks multi-method within-patient concordance, not cross-platform ThyroSeq vs Afirma disagreement.

4. **Version assignment**: 72 Afirma and 82 ThyroSeq patients have version_unknown due to missing test dates or ambiguous platform metadata.

5. **Test result (positive/negative/suspicious)** from the original lab report is not captured in the analytic view. `molecular_risk_tier` is used as a proxy but covers only 463/632 ThyroSeq and 139/197 Afirma patients.

---

## Output Files

| File | Content |
|---|---|
| `platform_diagnostic_performance.csv` | ROM, sensitivity, specificity, NPV by platform with 95% CI |
| `mutation_spectrum_by_platform.csv` | BRAF/RAS/fusion rates, risk tier distribution |
| `version_comparison.csv` | GEC vs GSC vs v2 vs v3 comparison |
| `dual_platform_concordance.csv` | Multi-arm ROM/recurrence comparison |
| `outcomes_by_platform.csv` | Recurrence, RAI, LN+, procedure type |
| `outcomes_logistic_regression.csv` | Adjusted OR for recurrence |
| `utilization_trends.csv` | Volume by year and platform (2010–2025) |
| `braf_detection_analysis.csv` | BRAF detection method breakdown |
| `braf_by_platform.csv` | BRAF rates with method breakdown per platform |
| `rom_by_bethesda.csv` | Bethesda III/IV ROM by platform |
| `platform_comparison_summary.tex` | LaTeX tables (main comparison + version comparison) |

## MotherDuck Table
`manuscript_workspace.m033_platform_analysis_v1` — 1,286 rows, includes all analytic view columns plus `platform_primary`, `is_dual_platform`, `is_primary_comparison`, `m033_run_ts`.
