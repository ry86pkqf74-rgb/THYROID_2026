# M098 — Statistical Methods Detail

A reference document describing the statistical methodology of the M098 study (RAS-Mutated Thyroid Surgical Cohort) at a level of detail sufficient for an independent methodologist to re-implement.

---

## 1. Software environment

All analyses were performed in Python 3.10 (CPython, x86_64 Linux) using the following library versions:

- pandas 2.3.3
- numpy 1.26.x
- scipy 1.15.3
- statsmodels 0.14.6
- lifelines 0.30.0
- matplotlib 3.10.8 (for figures)

No randomness affected the primary analyses; therefore no random seed was required for reproducibility. The deterministic ATA risk-score engine (Mo36 RSS v2) and the evidence-corrected hybrid molecular flags were drawn from the institution's canonical BigQuery layer (`thyroid-canonical-pub-2026.pub_canonical.*` and `pub_workspace.*`) which is version-controlled and append-only. The analytic dataset CSV (`data/m098_analytic.csv`) is a locked snapshot of the cohort as of the analysis date (May 2026).

Reproducibility is supported by three scripts, all in `analysis/`:

- `m098_run_analysis.py` — parses the BigQuery patient-level dataset into a typed DataFrame and writes `data/m098_analytic.csv`.
- `m098_tables.py` — generates Tables 1–10 (descriptive statistics).
- `m098_inferential.py` — runs the inferential comparison family with FDR correction and the four multivariable logistic regressions.
- `m098_survival_and_figures.py` — Kaplan-Meier survival estimation, sensitivity analyses, and the seven figures.

A single command (`python3 m098_run_analysis.py && python3 m098_tables.py && python3 m098_inferential.py && python3 m098_survival_and_figures.py`) reproduces the entire analysis end-to-end from the source BigQuery extract.

## 2. Cohort definition and inclusion criteria

The analytic cohort was defined by the following BigQuery selection:

```sql
SELECT research_id
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
WHERE ras_positive_final IS TRUE
  AND surg_first_date IS NOT NULL
```

The flag `ras_positive_final` is the institution's adjudicated patient-level RAS positivity indicator. It combines two evidence streams: (a) the structured molecular reports flagged with `ras_flag = TRUE` at the molecular event level, and (b) NLP-extracted gene mentions in clinical notes after passing a negation-cue filter. A patient is included if either stream is positive and no contradicting negation cue is present in source text. The cohort size is locked at N = 292 for all primary analyses.

The analytic dataset is structured as one row per patient with 61 columns. Variant-level molecular detail is held in a separate, longer-format table (mentioned in §3) but is keyed by `research_id` so that any patient's full molecular history is recoverable by join.

## 3. Variable derivation

### Gene presence

Gene-level positivity used the hybrid-evidence columns (`nras_positive_hybrid`, `hras_positive_hybrid`, `kras_positive_hybrid`) from `canonical_patient_master_hybrid_evidence_v1`. The hybrid logic is: a gene is set true if either the patient-master gene flag is true OR the structured variant array contains a positive record for that gene, and no negation cue ("not detected", "negative", "not identified", "not ordered") appears in the source report text within a ±100-character window of the gene mention.

### Co-mutation status

The columns `evidence_braf_positive` and `evidence_tert_positive` from the hybrid evidence layer were used for BRAF and TERT co-mutation status. These columns apply the negation-cue filter described above to source text before declaring a co-mutation positive, which protects against the well-known cross-gene parser bug whereby a "BRAF V600E was not detected" line could otherwise be miscoded as BRAF-positive.

Co-mutation group was derived as:

- `Isolated`: neither BRAF nor TERT positive
- `RAS+BRAF`: BRAF positive, TERT negative
- `RAS+TERT`: TERT positive, BRAF negative
- `RAS+TERT+BRAF`: both BRAF and TERT positive

### Gene priority and single-gene-only labels

For analyses requiring a single gene per patient, two labels were derived:

- `gene_priority` ∈ {NRAS, HRAS, KRAS}: takes the first positive gene in the priority order NRAS > HRAS > KRAS. Used for multivariable regression covariate.
- `gene_single` ∈ {NRAS, HRAS, KRAS, MULTI}: NRAS / HRAS / KRAS only if exactly one gene is positive; MULTI if more than one. Used for the per-gene malignancy rate analysis to avoid mixing single-gene and multi-gene patients in the same stratum.

### Histology classification

Per-patient histology category was derived deterministically from `histology_final`:

- `Malignant`: PTC, follicular carcinoma, MTC, PDTC, differentiated high-grade thyroid carcinoma
- `Borderline`: NIFTP, FTUMP
- `Benign`: `histology_final` is NULL (no malignant or borderline diagnosis recorded)
- `Unclassified`: any other non-null value

In this cohort, 176 patients were Malignant, 22 Borderline, 94 Benign, and 0 Unclassified.

### Tumor size

`tumor_size_safe` = `GREATEST(IFNULL(path_tumor_size_cm, 0), IFNULL(tumor_size_cm_max, 0))`. The `GREATEST` is necessary because `tumor_size_cm_max` is known to under-report in multi-surgery patients in this institution's registry.

### Lymph-node positivity

A binary `ln_positive` indicator was set per patient:

- 1 if `COALESCE(ln_rollup_total_positive, total_ln_positive_v10, path_ln_positive_raw, 0) > 0`
- 0 if any LN was examined (i.e., `COALESCE(ln_rollup_total_examined, path_ln_examined_raw, 0) > 0`) but none was positive
- NULL otherwise (no LN data)

### ATA risk-score derivation

Both the 2015 (3-tier) and the 2025 (4-tier) ATA risk-of-recurrence categories were drawn from `m036_ata_2025_rss_v2`, a deterministic rule engine that scores each patient against the official rule set with full input-completeness checks. Categories returned by this engine in this cohort are: `low`, `intermediate`, `high`, `uncalculable` (some required input field missing). Patients not eligible for ATA scoring (non-DTC histology, NIFTP, FTUMP, MTC) do not have a row in this table. For analyses requiring a binary outcome (multivariable regression Outcome 2), ATA-2025 was dichotomized as intermediate-or-high (1) vs low (0); uncalculable was treated as missing.

### Reclassification direction

Drawn directly from the `reclassification_direction` column of the m036 RSS engine, with values `up`, `same`, `down`. This column is populated for all 181 ATA-eligible patients even when the 2015 category is listed as `uncalculable`, because the engine reconstructs a 2015 category for the comparison whenever the required inputs are sufficient.

### Survival censoring

Time-to-event variables were computed against `followup_years` (in years from `surg_first_date` to last contact or death) for the survival analysis. For the analytic Kaplan-Meier subset, patients were restricted to those with `followup_years ≥ 1.0`. This restriction was necessary because the registry's `last_known_alive_date` defaults to `surg_first_date` when no other contact data exists, which inflates the apparent number of zero-follow-up patients. The analytic subset has n = 103 with median follow-up 2.89 years (IQR 1.74–5.07).

### Path-synoptics rollup

Per-patient worst-of synoptic features were aggregated across the tumor_1 through tumor_5 slots in `path_synoptics`. The decoding rule is that the literal string `'x'` is taken to mean absent (verified empirically: 101 of 105 PTC patients have `'x'` in their tumor_1 ETE field, with only 3 carrying an explicit "present" value, consistent with the institution's synoptic-template convention). A NULL (NaN) is taken to mean not assessed. Explicit terms (`present`, `microscopic`, `gross`, `minimally invasive`, `widely invasive`, `focal`, `extensive`, `involved`) are taken as positive. For the analyses reported in the manuscript, the adjudicated CPM columns (`capsular_invasion_refined`, `vascular_invasion_final`, `lvi_any_present_path`, `pni_any_present_path`, `ete_grade_final_v2`, `gross_ete_flag`, `margin_involved_any`) were preferred over the raw synoptic rollup because they incorporate post-adjudication corrections.

## 4. Descriptive statistics

Continuous variables were summarized as both median (IQR) and mean ± SD, with missingness reported per variable as a count. Categorical variables were summarized as n (%) with the denominator explicitly stated for each subgroup. Missing values were counted as a separate category when meaningful (e.g., Bethesda — Missing was an analytically meaningful stratum because 20.2% of the cohort had no Bethesda result on the FNA cytology referenced for surgical decision-making).

## 5. Inferential statistics

### Test selection

- **Categorical × categorical**: χ² test of independence using `scipy.stats.chi2_contingency`, two-sided, with the default behavior (Yates' continuity correction not applied). When any expected cell count fell below 5, or when the total table sample size was below 30, the test was switched to Fisher's exact (`scipy.stats.fisher_exact`) for 2 × 2 tables; for larger tables where Fisher's exact is computationally costly, the χ² result was retained but flagged.

- **Continuous-versus-two-group**: Mann-Whitney U test (`scipy.stats.mannwhitneyu`, two-sided alternative).

- **Continuous-versus-≥3-group**: Kruskal-Wallis H test (`scipy.stats.kruskal`).

### Multiple-comparisons correction

A pre-specified family of 21 comparisons was identified before data inspection. Raw p-values were corrected using Benjamini-Hochberg false-discovery rate (`statsmodels.stats.multitest.multipletests(method='fdr_bh')`) at α = 0.05. The corrected (q) values are reported alongside the raw p-values in `analysis/inferential_results.csv`. Multivariable regression p-values are not folded into this family; they are reported as raw model-level p-values.

### Family of 21 pre-specified comparisons

1. Gene (single-only) × Malignancy
2. Gene (single-only) × ROM (malignant + borderline)
3. Co-mutation group × Malignancy
4. Any co-mutation (binary) × Malignancy
5. TERT × Malignancy
6. BRAF × Malignancy
7. Co-mutation × ATA-2025 high (scored only)
8. Co-mutation × ATA-2025 intermediate-or-high
9. TERT × ATA-2025 high
10. Co-mutation × Surgery type (TT)
11. Co-mutation × LN positive
12. Bethesda × Malignancy
13. Age × Gene (single-only) — KW
14. Age × Co-mutation — KW
15. Age × Malignancy (vs Benign) — MW
16. Tumor size × Gene (malignant subset) — KW
17. Tumor size × Co-mutation (malignant) — KW
18. Tumor size × ATA-2025 (malignant) — MW
19. Max RAS VAF × Malignancy — MW
20. Max RAS VAF × Co-mutation — MW
21. Max RAS VAF × Gene — KW

After FDR correction, two comparisons retained q < 0.05: Co-mutation × ATA-2025 high (q = 0.018) and TERT × ATA-2025 high (q = 0.009).

## 6. Multivariable logistic regression

### Specification

Each model used `statsmodels.api.Logit` with a constant intercept and the listed covariates. Categorical predictors were one-hot encoded with the explicit reference level dropped: `NRAS` was the reference for `gene_priority`; `Isolated` was the reference for `comut_group`. Maximum iterations were set to 300. Convergence was monitored and reported per model.

For each model the reported quantities are:

- Coefficient (β) and standard error (SE) on the log-odds scale
- Odds ratio = exp(β) with 95% confidence interval = exp(β ± 1.96 · SE)
- Two-sided p-value from the Wald z-statistic

### Outcome 1 — Malignancy (1 = Malignant, 0 = Borderline or Benign)

Covariates: gene_priority, comut_group, age at surgery, Bethesda category (1–6), max RAS VAF.

Convergence: yes. Analytic n: 119 (drops occur because of missing VAF and Bethesda).

No covariate reached individual statistical significance. The co-mutation `RAS+TERT` coefficient was positive (OR = 3.4) with wide CI (0.12–101.7). Age, Bethesda, and VAF were all directionally consistent with prior literature but non-significant.

### Outcome 2 — ATA-2025 intermediate-or-high (vs low; scored only)

Covariates: gene_priority, comut_group, age, max RAS VAF.

Convergence: **failed (singular matrix)**. The failure is mechanistic: of the 170 scored patients, only 1 was categorized as Low under ATA-2025, making the binary outcome near-constant. The model is therefore reported descriptively from the cross-tab in Table 9.

### Outcome 3 — Any LN positive (LN-data subset)

Covariates: gene_priority, comut_group, age, tumor size.

Convergence: yes. Analytic n = 50.

Tumor size was an independent predictor with OR = 1.30 per cm (95% CI 1.09–1.56, p = 0.004). KRAS gene priority showed a trend toward lower LN positivity (OR = 0.24, 95% CI 0.05–1.20, p = 0.08). Co-mutation `RAS+TERT+BRAF` coefficient was extremely large (3.1 × 10¹³ OR) with infinite CI — this reflects perfect separation in a stratum of n = 1 and should not be interpreted as a biological effect.

### Outcome 4 — Total thyroidectomy (vs lobectomy)

Covariates: gene_priority, comut_group, age, Bethesda, tumor size.

Convergence: **failed (singular matrix)**. Likely due to separation in one of the rare gene-by-co-mutation strata. Reported descriptively (Table 7 surgery × ATA-2025 and × histology cross-tabs).

### Sensitivity

For the two failed models, Firth-penalized logistic regression was considered as a fallback but was not implemented in the present pipeline because the descriptive cross-tabs in Tables 7 and 9 already convey the comparable information; this is noted as a deferred analytic enhancement.

## 7. Survival and recurrence analysis

### Endpoint definitions

Two recurrence endpoints were considered:

- **Path-proven recurrence**: `recurrence_path_proven` = TRUE (recurrence confirmed by surgical pathology or core biopsy after the index surgery).
- **Any recurrence (strict + imaging)**: `recurrence_path_proven` OR `recurrence_imaging_suspicious` (i.e., includes imaging-detected suspicious findings that were not yet biopsy-confirmed).

The strict path-proven endpoint is the primary one for the descriptive incidence-rate calculation; the broader any-recurrence endpoint is used for the Kaplan-Meier curves to give a less censored view across the relatively short follow-up.

### Person-years and incidence rate

For each patient, person-years contributed = `followup_years`. Summed across the cohort: 371.7 PY total, 345.4 PY in the ≥1y subset.

Full-cohort incidence rate:
- Path-proven: 7 events / 371.7 PY = 1.88 per 100 PY
- Any recurrence: 29 events / 371.7 PY = 7.80 per 100 PY

≥1y subset:
- Path-proven: 5 events / 345.4 PY = 1.45 per 100 PY
- Any recurrence: 13 events / 345.4 PY = 3.76 per 100 PY

### Kaplan-Meier estimation

Kaplan-Meier curves were estimated with `lifelines.KaplanMeierFitter` stratified by co-mutation group on the ≥1y follow-up subset. The plotted endpoint is the any-recurrence event for inferential power. Each group's curve includes its 95% confidence interval band. At-risk numbers and event counts per group are shown in the figure legend. Groups with fewer than 3 patients in the subset are omitted.

### Cox regression

Cox proportional hazards regression was considered but deferred. The rule of thumb of at least 10 events per covariate was not met for any planned multivariable Cox model: with 13 any-recurrence events in the ≥1y subset, only a single-covariate Cox would be defensible, which adds nothing beyond the stratified Kaplan-Meier estimate. This is a transparent limitation of the present analysis and points to a need for either pooled multi-institution data or a longer follow-up window before survival multivariable inference becomes appropriate.

## 8. Sensitivity analyses

Six pre-specified sensitivity cohorts were defined before primary inferential analyses were run:

1. **Drop MTC**: exclude patients with `histology_final = 'MTC'`. n = 287. Co-mut × Malig p = 0.046 (nominal significance retained).
2. **DTC-only**: exclude MTC, PDTC, high-grade DTC. n = 281. Co-mut × Malig p = 0.054.
3. **Drop NIFTP and FTUMP**: treat them as not-malignant by dropping rather than reclassifying. n = 270. Co-mut × Malig p = 0.062.
4. **Platform stratification**: ThyroSeq only (n = 201), Afirma only (n = 36). Both directionally consistent.
5. **Era stratification**: surgery year pre-2020 vs 2020+. Available in the script; results not materially different.
6. **Complete cases**: drop patients missing age, Bethesda, or gene assignment. n = 233. Co-mut × Malig p = 0.144 (loses power without changing direction).

The conclusion is that the co-mutation × malignancy gradient is directionally stable across all sensitivity cohorts but does not robustly cross the q < 0.05 threshold; the more granular co-mutation × ATA-2025 high finding (which survives FDR in the primary analysis) is the more reproducible signal.

## 9. Missing data

The pattern of missingness in this cohort is non-random and tracks the modality of evidence. Most missing values fall into three patterns:

- Codon-level molecular detail (variant, VAF) is missing for the ~30% of patients whose molecular evidence came through NLP extraction of clinical notes rather than through structured ThyroSeq or Afirma reports. The patient master flags these as `mol_platform = 'unknown'`. The hybrid gene-level flag is still defined for these patients (from the NLP path), but variant-level fields are NULL.
- Tumor size and pathology drill-down (capsular invasion grade, vascular invasion count) are missing for non-malignant lobectomy specimens because the institution's synoptic template does not require these for benign or NIFTP outcomes.
- TIRADS and BMI are missing for the ~35% and ~50% of patients respectively where preoperative imaging or BMI were not documented in a structured way.

No imputation was performed in the primary analysis. Sensitivity analysis 6 (complete cases) addresses any concern about the impact of list-wise drop in the regression models; the finding is that complete-case restriction reduces power without changing the direction of any primary finding.

## 10. Reproducibility statement

The analysis is fully reproducible from the BigQuery snapshot used in this study. Specifically:

- The cohort SQL is locked in `analysis/m098_cohort.sql`.
- The patient-level analytic dataset is locked in `data/m098_analytic.csv` (61 columns × 292 rows).
- The four analysis scripts (`m098_run_analysis.py`, `m098_tables.py`, `m098_inferential.py`, `m098_survival_and_figures.py`) reproduce all tables, all inferential statistics, all four regression models, and all seven figures from `m098_analytic.csv` alone.
- All output artifacts (tables in `tables/`, regression coefficient CSVs in `analysis/`, figures in `figures/`) are append-only outputs of those scripts and can be regenerated end-to-end with a single command.

A one-line reproduction command is:

```bash
python3 m098_run_analysis.py && python3 m098_tables.py && \
python3 m098_inferential.py && python3 m098_survival_and_figures.py
```

The canonical BigQuery layer is versioned by the institution's data team and the snapshot referenced in this manuscript is the one in effect on the analysis date. A re-run against a later snapshot may produce different patient-level numbers as additional patients are accrued or as the canonical layer's adjudication evolves; the per-script logic, however, is invariant.

---

*This methods document is referenced from the M098 manuscript Methods section and travels with the manuscript through internal review, journal submission, and any subsequent revision.*


## 11. Variant-level cleanup pipeline — QC results

The §3 description of variable derivation outlined the rules used for cleaning the variant-level long table that backs the codon-level analyses (Table 4 protein-change rows and the per-codon ROM block of Table 10). The cleanup pipeline (`analysis/m098_variant_cleanup.py`) was applied to the full set of variant records returned for the 292-patient cohort and produced the table `data/m098_variant_long.csv` (611 rows × 17 columns) accompanied by `data/m098_variant_long_qc_summary.csv`.

Pipeline outcome:

- Raw variant rows pulled from `canonical_molecular_genetics_v2.gene_mutations_variants`: 611
- Distinct patients with structured variant-level data: 218 of 292 (74.7%; the remaining 74 patients have gene-level evidence drawn from NLP-extracted clinical notes, with codon and VAF not recoverable at the variant level)
- OCR normalization (§6.3): 12 rows had OCR-style errors in the protein column corrected (e.g., `pO61R` → `p.Q61R`, `pQ6IR` → `p.Q61R`, `p.QG1Kc` → `p.Q61K`); 1 row flagged as truncated (AA missing)
- Biologically impossible gene-protein pairs (§6.1): 16 rows flagged (e.g., V600E reported on NRAS, Q61 reported on BRAF)
- Negation cues in source text (§6.2): 32 rows where the ±100-character window of `gene_mutations_raw` around the gene mention contained a negation cue without a countervailing positive marker
- Recovered from raw text (§6.4): 8 of the 16 impossibility-flagged rows had a parseable canonical RAS codon in the raw report text and were re-assigned to that recovered variant (`protein_norm_status = 'recovered_from_raw'`)
- Final analytic kept: 576 of 611 rows (94.3%)

After cleanup, the kept-row top variants in the cohort are: NRAS/HRAS/KRAS p.Q61R (92), BRAF p.V600E (38), p.Q61K (15), TERT p.C228T (7), with smaller counts of G13R, G12V, G12C, Q13R, and TERT C250T. This distribution is consistent with the published RAS-thyroid literature (Q61 dominance, V600E as the canonical BRAF event, C228T as the dominant TERT promoter change) and gives confidence that the cleanup pipeline did not over- or under-prune the variant set.

The kept variant table is the source for the per-codon ROM block of Table 10 and for the molecular-detail rows of Table 4. The full set of dropped rows is preserved (with `analytic_keep = FALSE` and a `drop_reason` string) for auditability — nothing is deleted from the variant table.
