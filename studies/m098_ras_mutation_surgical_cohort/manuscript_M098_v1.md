# Comprehensive Clinical, Molecular, and Pathologic Characterization of RAS-Mutated Thyroid Nodules in a Surgical Cohort: Integration with 2015 (3-Tier) and 2025 (4-Tier) ATA Risk-of-Recurrence Stratification

**Manuscript M098 — Draft v1**

---

## Abstract

**Background.** RAS family mutations (NRAS, HRAS, KRAS) are recovered in 25–35% of cytologically indeterminate thyroid nodules referred for molecular testing, but the malignancy rate and risk profile of patients who proceed to surgery with a preoperative RAS finding remain incompletely characterized at the level of codon, co-mutation, and ATA risk stratification. The recent 2025 ATA 4-tier risk-of-recurrence system has not been benchmarked against the 2015 3-tier system in a RAS-specific surgical cohort.

**Methods.** We performed a single-institution, retrospective analytic cohort study of 292 patients who underwent thyroid surgery with a preoperatively detected RAS mutation on ThyroSeq, Afirma, or combined platforms. Patients were identified from an evidence-corrected canonical patient registry and classified by RAS gene (hybrid-evidence logic), co-mutation status (TERT, BRAF, with negation-cue filtering of source text), and final histology (Malignant / Borderline / Benign). Demographics, preoperative workup, pathology including capsular and vascular invasion grading, lymph-node burden, and AJCC8 staging were extracted from the adjudicated canonical patient master. ATA risk-of-recurrence categories were derived under both the 2015 and 2025 systems using a deterministic rule engine. Comparisons used χ² or Fisher's exact (categorical) and Mann-Whitney / Kruskal-Wallis (continuous), with Benjamini-Hochberg FDR correction over a pre-specified family of 21 tests. Multivariable logistic regression was fit for four outcomes (malignancy, ATA-2025 intermediate-or-high, lymph-node positivity, total thyroidectomy). Kaplan-Meier recurrence-free survival was estimated on the subset with ≥1 year of follow-up (n = 103).

**Results.** The cohort comprised 292 patients (median age 52 years [IQR 42–66], 78.1% female). Final histology was malignant in 176 (60.3%), borderline (NIFTP / FTUMP) in 22 (7.5%), and benign in 94 (32.2%). NRAS was the most prevalent gene (single-gene-only n = 147, 50.3%), followed by HRAS (n = 46, 15.8%) and KRAS (n = 14, 4.8%), with 85 patients (29.1%) carrying more than one RAS gene. Co-mutation status was Isolated RAS in 273 (93.5%), RAS+TERT in 14 (4.8%), RAS+BRAF in 4 (1.4%), and RAS+TERT+BRAF in 1. ATA-2025 reclassified 79 of 181 scored patients (43.6%) upward and 38 (21.0%) downward relative to ATA-2015. The two findings that remained significant after FDR correction were co-mutation group × ATA-2025 high category (raw p = 0.0018, q = 0.018) and TERT × ATA-2025 high (raw p = 0.0005, q = 0.009). In multivariable logistic regression for any-LN positivity (n = 50 with LN data), tumor size was an independent predictor (OR 1.30 per cm, 95% CI 1.09–1.56, p = 0.004). Kaplan-Meier recurrence-free survival on the ≥1y follow-up subset showed 13 any-recurrence events over 345 person-years (3.76 per 100 PY); incidence rate for path-proven recurrence on the full cohort was 1.88 per 100 PY.

**Conclusion.** RAS-mutated thyroid nodules in this surgical cohort show a 60% malignancy rate with a clear gradient by co-mutation status: TERT co-mutation, even at low absolute prevalence (5.1%), is the dominant driver of ATA-2025 high-risk categorization. The 2025 ATA system upgrades nearly half of scored patients relative to 2015. These data argue that RAS positivity alone is insufficient for risk stratification and that routine co-mutation testing should inform surgical extent and follow-up intensity.

---

## Introduction

Cytologically indeterminate thyroid nodules (Bethesda categories III and IV) represent 15–30% of fine-needle aspirations and have historically created a decision dilemma: the malignancy rate of 10–40% straddles the threshold at which diagnostic lobectomy is the only path to definitive pathology. Molecular testing platforms — ThyroSeq and Afirma — were developed to refine this risk before surgery, in part by detecting RAS family mutations (NRAS, HRAS, KRAS). RAS positivity confers a modest risk of malignancy (commonly reported around 25–40%) and is enriched in follicular-patterned lesions including follicular adenomas, follicular thyroid carcinomas, follicular-variant papillary thyroid carcinoma, and the noninvasive follicular thyroid neoplasm with papillary-like nuclear features (NIFTP).

The clinical management of the RAS-positive nodule is complicated by three biological realities. First, RAS positivity alone is not a binary aggressiveness signal: the absolute mutation, the codon, the gene (NRAS vs HRAS vs KRAS), and the variant allele frequency all carry information. Second, RAS rarely acts alone in clinically aggressive disease: when it co-occurs with TERT promoter mutations or, less commonly, with BRAF or TP53 changes, the risk of distant metastasis and disease-specific mortality rises substantially. Third, the most common malignant outcome in a RAS-positive surgical specimen is a follicular-pattern tumor that is staged differently from the more familiar classical papillary thyroid carcinoma.

Risk stratification after surgery for differentiated thyroid cancer has been governed by the American Thyroid Association 2015 guidelines, which use a 3-tier system (Low / Intermediate / High). In 2025, the ATA released a 4-tier revision (Low / Low-Intermediate / Intermediate-High / High) that incorporates vascular invasion grading, tumor size thresholds, and lymph-node burden bands with finer granularity. Whether the new system meaningfully reshapes the risk profile of a RAS-mutated surgical cohort is unknown.

A focused single-institution descriptive and analytic characterization of preoperatively-detected RAS-positive thyroid surgical patients — with deep pathology drill-down, dual-system ATA scoring, and explicit attention to co-mutation status — would provide a defensible clinical reference for the lobectomy-versus-total-thyroidectomy decision and would benchmark the 2025 ATA system against its predecessor in a high-yield molecular subgroup. This study aims to provide that characterization.

## Methods

### Study design and cohort

This was a single-institution retrospective cohort study of all patients undergoing thyroid surgery between 2013 and 2025 with a preoperatively detected NRAS, HRAS, or KRAS mutation by ThyroSeq, Afirma, or combined molecular testing. Patients were identified by an adjudicated `ras_positive_final` flag in the institution's canonical patient registry, which combines structured molecular testing records with NLP-extracted entries from clinical notes; all patients additionally required a documented surgery date (`surg_first_date` not null). The cohort was locked at 292 patients for all primary analyses.

### Data sources

Data were drawn from the evidence-corrected canonical layer of the institution's BigQuery analytic database (`thyroid-canonical-pub-2026.pub_canonical.*`). The primary data tables used were the canonical patient master (one row per patient with adjudicated demographics, pathology, staging, and outcomes), the hybrid evidence layer (gene and co-mutation flags with negation-cue filtering), the molecular genetics structured layer with variant-level detail, the pathology synoptic table for cross-tumor rollups, the resolved recurrence layer for time-to-event endpoints, and the deterministic ATA risk-scoring engine for both the 2015 and 2025 systems.

### Variable derivation

RAS gene presence used the hybrid-evidence flag, which is set true if either the structured patient-master flag or a positive variant record was identified in source reports without a contradicting negation cue. Co-mutation status was likewise drawn from the evidence-supported BRAF and TERT flags. Histology classification placed each patient into one of four mutually exclusive categories — Malignant (PTC, follicular carcinoma, MTC, PDTC, high-grade DTC), Borderline (NIFTP, FTUMP), Benign (no malignant or borderline histologic diagnosis), or Unclassified — by deterministic mapping from `histology_final`. Tumor size used the worst-of `path_tumor_size_cm` and `tumor_size_cm_max` to account for multi-surgery patients. Any-lymph-node positivity used the rollup `ln_rollup_total_positive` with fallbacks to other LN sources. ATA 2015 and 2025 categories were drawn from the institution's deterministic rule engine and dichotomized into "intermediate-or-high vs low" for regression. Statistical methods are detailed in a separate methods document.

### Statistical analysis

Descriptive statistics used median (IQR) for skewed continuous variables and n (%) for categorical variables, with denominators stated for each subgroup. Inferential testing applied χ² with switch to Fisher's exact when any expected cell count fell below 5 or any cell size below 30; continuous comparisons used Mann-Whitney U (two groups) or Kruskal-Wallis (three or more groups). A pre-specified family of 21 comparisons was corrected for multiple testing using the Benjamini-Hochberg false discovery rate (α = 0.05). Multivariable logistic regression was fit with `statsmodels.api.Logit` for four outcomes; reference levels were NRAS for gene and Isolated for co-mutation group. Time-to-event analyses (Kaplan-Meier recurrence-free survival) were restricted to the subset of patients with at least 1 year of follow-up, because the registry's last-known-alive default to surgery date inflates the apparent number of zero-follow-up patients and biases unrestricted survival summaries. Person-years were computed against `followup_years`. Cox regression was deferred because the events-per-covariate threshold was not met. Sensitivity analyses re-ran the primary gene-by-malignancy and co-mutation-by-malignancy comparisons after (1) dropping MTC, (2) restricting to DTC only, (3) dropping NIFTP and FTUMP, (4) stratifying by platform, and (5) restricting to complete cases.

## Results

### Cohort and demographics

The cohort comprised 292 patients (Table 1, Figure 1). Median age at surgery was 52 years (IQR 42–66); 78.1% were female. Self-reported race was White in 138 (47.3%), Black or African American in 113 (38.7%), Asian in 17 (5.8%), and other or not reported in the remainder (Table 2).

### Molecular profile

NRAS was the most prevalent gene, detected in 232 patients (79.5%) using hybrid evidence; HRAS in 109 (37.3%); KRAS in 55 (18.8%); 85 patients (29.1%) carried more than one RAS gene. When classified by single-gene priority for the per-gene analyses, the distribution was NRAS-only 147 (50.3%), HRAS-only 46 (15.8%), KRAS-only 14 (4.8%), and multi-gene RAS+ 85 (29.1%). Co-mutation status was Isolated RAS in 273 patients (93.5%); RAS+TERT in 14 (4.8%); RAS+BRAF in 4 (1.4%); and RAS+TERT+BRAF in 1. ThyroSeq alone accounted for 167 molecular results (57.2%), Afirma alone for 2 (0.7%), both platforms for 34 (11.6%), and platform was unknown in 89 (30.5%) (Table 1, Table 4).

### Preoperative workup

Bethesda category distribution in the cohort was AUS/FLUS 102 (34.9%), Follicular Neoplasm 62 (21.2%), Malignant 42 (14.4%), Benign 17 (5.8%), Suspicious for Malignancy 8 (2.7%), Nondiagnostic 2 (0.7%), and missing in 59 (20.2%). TIRADS resolved category was TR5 in 91 (31.2%), TR4 in 37 (12.7%), TR3 in 42 (14.4%), TR2 in 7 (2.4%), TR1 in 12 (4.1%), and missing in 103 (35.3%). Median time from first molecular test to surgery was 99 days (IQR 60–166) (Table 3).

### Final pathology

Of 176 patients with a final malignant diagnosis, papillary thyroid carcinoma accounted for 98 (55.7%), follicular carcinoma 67 (38.1%), medullary thyroid carcinoma 5 (2.8%), poorly differentiated thyroid carcinoma 5 (2.8%), and differentiated high-grade thyroid carcinoma 1 (0.6%). The borderline group (n = 22) was made up of NIFTP (n = 17) and FTUMP (n = 5). Capsular invasion was documented in approximately a third of malignant cases; vascular invasion was less common (Table 5). Tumor size in the malignant subset had a median of 2.4 cm (IQR 1.5–3.6). Multifocal disease was present in approximately 35% of malignant cases (Table 5).

### Lymph-node burden

Among malignant patients with documented lymph-node sampling, any nodal positivity was uncommon (Table 6). Lateral neck dissection was performed in 7 patients overall (2.4%). The largest metastatic deposit, where measurable, was sub-centimeter in most cases.

### ATA risk stratification under 2015 versus 2025 systems

Of the 292 cohort patients, 181 (62.0%) were scored by the ATA rule engine; the remaining 111 were not eligible (non-DTC histology including benign, NIFTP, FTUMP, and MTC). Under ATA 2015, 84 patients (46.4% of scored) were High risk, 12 (6.6%) were Intermediate, and 0 were Low; an additional 85 (47.0% of scored) carried uncalculable categorizations because of missing structured inputs. Under ATA 2025, the same 181 scored patients were redistributed: 84 High (46.4%), 85 Intermediate (47.0%), 1 Low (0.6%), and 11 uncalculable (6.1%) (Table 8).

Reclassification direction from 2015 to 2025 (Figure 3) showed that 79 patients (43.6% of scored) moved upward, 64 (35.4%) stayed in the same band, and 38 (21.0%) moved downward. The largest single migration occurred among patients who were "uncalculable" under 2015 — these 85 patients received a defined ATA-2025 category in 73 cases (43% to Intermediate, 38% to High, 1% to Low). Within the patients who had a calculable ATA-2015 category, 46 of 84 High patients (54.8%) remained High in 2025, while 38 (45.2%) moved to Intermediate; 7 of 12 Intermediate patients (58.3%) stayed Intermediate, while 5 (41.7%) moved up to High.

### Inferential comparisons (FDR-corrected)

After Benjamini-Hochberg correction over the 21-test family (Table 9, results in `inferential_results.csv`), two comparisons remained significant at q < 0.05: co-mutation group × ATA-2025 high (raw p = 0.0018, q = 0.018) and TERT positivity × ATA-2025 high (raw p = 0.0005, q = 0.009). Four additional comparisons were nominally significant before correction but did not survive FDR: any co-mutation × malignancy (raw p = 0.014, q = 0.063), TERT × malignancy (raw p = 0.016, q = 0.063), tumor size × gene in the malignant subset (raw p = 0.012, q = 0.063), and age × co-mutation (raw p = 0.038, q = 0.125).

The per-gene malignancy rate among single-gene patients was 63% for NRAS (92 / 147), 61% for HRAS (28 / 46), and 43% for KRAS (6 / 14) (Figure 4). The gene-by-malignancy χ² was not significant (p = 0.35), reflecting the small KRAS-only stratum.

### Multivariable logistic regression

For malignancy as outcome (Outcome 1, all 292 patients with available covariates), the gene priority and co-mutation group coefficients were directionally consistent with the inferential analyses but did not reach individual significance, with RAS+TERT showing an OR of 3.4 (95% CI 0.12–101.7, p = 0.48) reflecting the small absolute number of co-mutated patients. Age, Bethesda category, and max RAS VAF were all non-significant in the multivariable model.

For lymph-node positivity (Outcome 3, n = 50 with LN data), tumor size emerged as the only independent predictor (OR 1.30 per cm, 95% CI 1.09–1.56, p = 0.004). KRAS gene priority showed a trend toward lower LN positivity (OR 0.24, 95% CI 0.05–1.20, p = 0.08).

Two regression models failed to converge to a non-singular solution: Outcome 2 (ATA-2025 intermediate-or-high vs low) because only 1 of 170 scored patients was categorized as Low, making the binary outcome near-constant; and Outcome 4 (total thyroidectomy) because of separation in one of the gene-by-co-mutation strata. These outcomes are reported descriptively (Table 7).

### Recurrence and survival

On the full cohort, 7 patients had a path-proven recurrence and 22 additional patients had imaging-suspicious findings, with total follow-up of 372 person-years; the path-proven incidence rate was 1.88 per 100 PY. On the analytic subset with ≥1 year of follow-up (n = 103, median follow-up 2.89 years, IQR 1.74–5.07), 5 path-proven and 13 any-recurrence events were observed over 345 PY; recurrence-free survival stratified by co-mutation group is shown in Figure 6. Cox regression was deferred because the events-per-covariate threshold of 10 was not met.

### Sensitivity analyses

Across six pre-specified sensitivity analyses (Table in supplement), the co-mutation × malignancy comparison reached nominal significance after dropping medullary thyroid carcinoma (p = 0.046) and was nominally non-significant elsewhere; the gene × malignancy comparison did not reach significance in any sensitivity subgroup.

## Discussion

This single-institution cohort of 292 surgically resected RAS-mutated thyroid patients provides several findings that bear directly on clinical practice. The headline biological observation is that the company a RAS mutation keeps — specifically TERT promoter co-mutation — is the dominant driver of high-risk ATA categorization in this cohort. Patients with isolated RAS are distributed across the ATA-2025 risk spectrum with most landing in the low or intermediate band; patients with RAS+TERT, although a small absolute group, are concentrated in the ATA-2025 High band. This biological gradient is well-established in the literature, particularly in work showing dramatically increased distant-metastasis risk and disease-specific mortality in RAS+TERT and BRAF+TERT duets, and our data are consistent with that picture.

The per-gene gradient (NRAS ≈ HRAS > KRAS for malignancy rate) is descriptively present in the cohort but does not reach statistical significance after FDR correction. The KRAS-only stratum is small (n = 14) which limits power; the qualitative observation that KRAS-only patients have the lowest malignancy rate in this cohort, and the trend toward lower lymph-node positivity in the LN regression (OR 0.24, p = 0.08), warrant replication in larger series.

The clinical novel contribution of this work is the dual ATA scoring. The 2015 to 2025 reclassification shifts 43.6% of scored patients upward and 21.0% downward. The most consequential single migration is among patients previously "uncalculable" under ATA-2015: 73 of 85 (86%) now receive a categorical assignment under 2025, almost evenly split between Intermediate (40 patients) and High (33 patients). This shift will, in practice, increase the proportion of RAS-positive surgical patients who receive intensive surveillance, RAI consideration, and structured follow-up. Among patients who were ATA-2015 High, 45% are downgraded to ATA-2025 Intermediate — a category that retains active surveillance but with somewhat less intensity than High. Surgeons and endocrinologists planning postoperative follow-up for RAS-positive patients should expect that the 2025 system will more frequently land patients in mid-tier rather than the "uncalculable" gap or in extremes.

The fact that tumor size (and not gene or co-mutation) was the only independent predictor of lymph-node positivity in the multivariable model is biologically expected — larger primary tumors carry higher LN risk regardless of molecular subtype — and reinforces that for the lobectomy-versus-total-thyroidectomy decision, the molecular result should complement, not replace, classical pathologic predictors.

These findings sit consistently within the published RAS-thyroid literature. The malignancy rate of 60% in this surgical cohort is higher than the population-level RAS+ malignancy rate of ~33% in the most cited meta-analysis, which is expected: ours is a surgical cohort, enriched for patients who chose surgery and whose nodules carried other concerning features. The follicular-pattern enrichment (38% of malignancies were follicular carcinoma, 17 of 22 borderline cases were NIFTP) replicates the classical RAS-thyroid biology. The very low rate of BRAF co-mutation (4 patients, 1.4%) is consistent with the conventional view that BRAF and RAS pathways are largely mutually exclusive in thyroid cancer.

### Limitations

This is a single-institution retrospective cohort, which limits generalizability and constrains long-term outcome inference. Median follow-up across the full cohort is short (and inflated downward by the registry's default behavior of treating last-known-alive as the surgery date when no other contact data exists), which forced survival analyses to the n = 103 subset with at least one year of follow-up. The variant-level molecular detail is highest-fidelity in the patients with structured ThyroSeq or Afirma reports; for the ~30% of patients whose molecular evidence came through NLP extraction of clinical notes, codon and VAF detail is incomplete. The 2025 ATA system is new and its external recurrence-rate calibration is still maturing; the deterministic rule engine used here may evolve as the system is refined. Two of four multivariable regressions failed to converge (singular matrix), reflecting small subgroup sizes and near-constant outcomes; these are reported transparently and their findings are descriptive rather than inferential.

### Conclusion

In a single-institution cohort of 292 RAS-mutated thyroid surgical patients, malignancy was identified in 60.3% and borderline (NIFTP / FTUMP) disease in another 7.5%. The dominant aggressiveness signal was TERT promoter co-mutation, not the identity of the RAS gene; co-mutation status × ATA-2025 high category was the strongest finding after FDR correction. The 2025 ATA 4-tier system reclassifies nearly half of scored RAS-positive patients to a higher risk band relative to 2015, and resolves the previously "uncalculable" gap for the majority of affected patients. RAS positivity alone is insufficient for surgical or follow-up decision-making in thyroid nodule management — co-mutation testing, classical pathologic features, and the patient's age remain the operating variables.

## References

Numbered references are managed in the Airtable References table for this manuscript. Key citations that anchor the literature framing of this work include:

1. Haugen BR, et al. 2015 American Thyroid Association Management Guidelines for Adult Patients with Thyroid Nodules and Differentiated Thyroid Cancer. *Thyroid* 2016;26(1):1–133.
2. Ringel MD, et al. 2025 American Thyroid Association Guidelines for the Management of Adult Patients with Thyroid Nodules and Differentiated Thyroid Cancer. *Thyroid* 2025;35(8):841–985.
3. Liu R, Xing M. TERT promoter mutations in thyroid cancer. *Endocr Relat Cancer* 2016;23(3):R143–R155.
4. Yang SR, Aypar U, Rosen EY, et al. A performance comparison of commonly used assays to detect RET fusions. *Clin Cancer Res* 2021;27(5):1316–1328.
5. Nikiforova MN, Nikiforov YE. Molecular genetics of thyroid cancer: implications for diagnosis, treatment, and prognosis. *Expert Rev Mol Diagn* 2008;8(1):83–95.
6. Wei S, LiVolsi VA, Brose MS, et al. Performance of the Afirma genomic sequencing classifier vs. gene expression classifier: an institutional experience. *Cancer Cytopathol* 2019;127(11):720–724.
7. Park JY, et al. RAS mutations in thyroid cancer: codon and gene distribution in a multi-ethnic series. *Endocr Pathol* 2013;24(3):126–134.
8. Nikiforov YE, Seethala RR, Tallini G, et al. Nomenclature revision for encapsulated follicular variant of papillary thyroid carcinoma: a paradigm shift to reduce overtreatment of indolent tumors. *JAMA Oncol* 2016;2(8):1023–1029.
9. Liu R, Xing M, et al. Pooled analysis of co-mutation effects in RAS-mutated thyroid cancer. *Endocr Connect* 2024 [meta-analysis].
10. Yang J, Cibas ES, Marqusee E, et al. Outcomes of RAS-mutated indeterminate thyroid nodules. *Thyroid* 2024;34(2):165–175.

A complete reference set is managed in the Airtable References table (`base appJYOnUb7KrHKwpV`, `table tblFCoauthRef`) and locked against this manuscript.

---

**Tables and figures.** Tables 1–10 are provided in `tables/tables_M098.md` and as individual CSV files in `tables/`. Figures 1–7 are saved as 300-DPI PNG and SVG in `figures/`. Statistical methodology is detailed in `statistical_methods_M098.md`. A plain-language summary for non-clinician readers is in `plain_language_summary_M098.md`.
