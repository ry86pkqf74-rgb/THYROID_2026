# Association of clinicopathologic features and preoperative molecular testing with extent of initial thyroidectomy among patients with 2–4 cm thyroid nodules and radiologically negative cervical lymph nodes: a retrospective cohort study using integrated local DuckDB analytics

**Running title:** Lobectomy vs total thyroidectomy in 2–4 cm N0-equivalent nodules

**Data and code:** THYROID_2026 research database (local DuckDB `thyroid_master.duckdb`); archived release Zenodo [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510); Git tag `v2026.03.10-publication-ready`.

---

## Abstract

**Background.** For biopsy-proven or clinically concerning thyroid nodules in the 2–4 cm range without sonographically or radiographically suspicious lymphadenopathy, both lobectomy and total thyroidectomy remain guideline-appropriate initial options in selected patients. Molecular classifiers (e.g., Afirma GEC, ThyroSeq) may influence preoperative risk perception and operative planning, but real-world integration of platform-specific results with surgical extent is incompletely quantified at scale.

**Objective.** Among adults undergoing initial thyroidectomy for dominant nodules measuring 2.0–4.0 cm with structured preoperative imaging evidence against pathologic cervical lymphadenopathy on cross-sectional imaging, we evaluated (1) correlates of initial total thyroidectomy versus lobectomy; (2) concordance between preoperative molecular risk and final histology / American Thyroid Association (ATA) risk category; and (3) feasibility of quantifying completion thyroidectomy after initial lobectomy from structured operative episodes.

**Methods.** We queried a de-identified institutional research lakehouse (DuckDB on local DuckDB). Eligible patients had a first recorded procedure of hemithyroidectomy or total thyroidectomy, preoperative maximum nodule dimension between 2 and 4 cm (dominant ultrasound-based nodule master measurements prior to surgery, with fallback to resolved-layer imaging size), no preoperative CT or MRI flagged for pathologic cervical lymph nodes prior to surgery, and no distant metastasis flags on structured pathology staging or metastatic histology descriptors. We classified preoperative molecular tests (ThyroSeq or Afirma) occurring before surgery and defined **genetics-guided** elevation as suspicious/positive overall classifier calls or documented high-risk marker flags. We used the repository `ThyroidStatisticalAnalyzer` for Table 1 and automated univariable tests (Benjamini–Hochberg false discovery rate for the feature bundle) and fit multivariable logistic regression via formula interfaces (`statsmodels`) to accommodate categorical factors. Concordance used Cohen’s κ stratified by platform.

**Results.** The analytic cohort comprised **574** patients (**322** total thyroidectomy; **252** lobectomy). Median age was 56 years (interquartile range 43–67). Bethesda III versus IV/V category distributions differed markedly by surgical extent (Table 1). Only **21** patients (3.7%) had a preoperative ThyroSeq or Afirma result; **9** (1.6%) met genetics-guided high-risk criteria. In multivariable analysis (n=574; area under the receiver operating characteristic curve **0.645**), higher Bethesda category (IV/V versus reference) associated with increased odds of total thyroidectomy (adjusted odds ratio **4.01**, 95% CI 2.30–7.02; P<0.001), whereas older age associated with lower odds (adjusted OR 0.987 per year, 95% CI 0.975–0.998; P=0.027). Preoperative molecular availability and genetics-guided flags were not independently associated with surgical extent in this sparse testing subset. Overall Cohen’s κ for molecular high-risk versus pathology/ATA-defined high-risk outcomes was **−0.13** among the 16 patients with paired classifications (exploratory; severely underpowered). **No** completion thyroidectomies were identifiable within this subgroup using sequential structured pathology procedure text; operative episode tables contained essentially single episodes per patient.

**Conclusions.** In this integrated, multimodal retrospective cohort, **cytologic stratification dominated** the measured preoperative predictors of initial extent of surgery, while **preoperative ThyroSeq/Afirma utilisation was too infrequent** to draw platform-specific causal inferences about operative choice. Concordance and completion outcomes **cannot** be responsibly benchmarked without denser molecular coverage and richer multi-episode operative documentation. These findings highlight actionable data-governance priorities: structured capture of multistage thyroid procedures and contemporaneous molecular orders/results linked to surgical timestamps.

**Trial registration.** Not applicable (retrospective database study).

---

## Introduction

Differentiated thyroid cancer management increasingly emphasises risk-adapted extent of surgery, balancing oncologic control against morbidity of bilateral resection and future radioactive iodine eligibility(1,2). For unifocal or dominant nodules in the 2–4 cm interval, lobectomy may be adequate for intrathyroidal, clinically node-negative disease classified as ATA low or select intermediate risk, while total thyroidectomy may be favoured for higher preoperative suspicion, multifocality, familial syndromes, planned adjuvant therapy, or patient preference(1–3). Fine-needle aspiration Bethesda categories III–VI create a decision-rich zone where molecular testing is guideline-endorsed to reclassify risk(4,5). Commercial platforms differ in biomarker content, reporting lexicon, and historical validation cohorts; direct behavioural comparisons in real-world, multimodally documented populations remain limited(6,7).

Prior single-institution series report wide variation in lobectomy rates among molecularly “benign” or “suspicious” subsets, often confounded by nodule size, patient comorbidity, surgeon era, and incomplete capture of preoperative testing(8,9). Moreover, concordance between molecular “rule-out/rule-in” constructs and final surgical pathology is well studied for **diagnostic** accuracy but less so for **surgical behaviour** conditional on integrated imaging and cytology(10,11). Finally, completion thyroidectomy after unexpected index findings is a key safety net outcome; its incidence is systematically undercounted when administrative datasets collapse multistage thyroid operations into single encounters(12).

The THYROID_2026 corpus couples de-identified pathology, structured imaging nodule metrics, operative episode extracts, and molecular episode tables in a versioned DuckDB instance on local DuckDB, with reproducible querying and statistical helpers in the public code archive(13). Here we exploit that stack to operationalise a **2–4 cm, cross-sectional imaging N0-equivalent** window and describe associations with initial surgical extent, exploratory molecular–pathology concordance when sample size permits, and the **feasibility** (not the epidemiology) of completion surgery detection from currently structured operative feeds.

---

## Methods

### Design and data source

This is a retrospective cohort study of consecutively documented patients appearing in the research database underlying the THYROID_2026 analytic freeze. local DuckDB houses `thyroid_master.duckdb`, a PHI-stripped lakehouse linked by integer `research_id`. The present analysis executed read-only SQL and Python (pandas, statsmodels, scikit-learn) on 2026-03-25 from a workstation with authorised local DuckDB credentials. **No production tables were modified.** Analyses follow STROBE reporting guidance for observational studies(14); a mapping to STROBE items appears in Appendix B.

### Study cohort

**Operative anchor.** Patients must have exactly **one first operative episode** in `operative_episode_detail_v2` (row `rn=1` by ascending resolved surgical date) with `procedure_normalized` in {`hemithyroidectomy`, `total_thyroidectomy`}. Procedures recorded as `unknown` or `other` were excluded to preserve a clean lobectomy-versus-total contrast consistent with the prespecified comparative scope.

**Tumour size 2.0–4.0 cm.** For each patient we computed the maximum `imaging_nodule_master_v1.max_dimension_cm` among ultrasound-derived nodule rows with `exam_date` on or before the surgery anchor date (Coalesce of operative episode date and resolved-layer `surg_first_date` / `first_surgery_date`). If no qualifying preoperative imaging nodule row existed, we used `patient_analysis_resolved_v1.imaging_nodule_size_cm` as a secondary source. Patients entered the risk set if this **primary composite size** lay in \[2.0, 4.0\] cm inclusive.

**Preoperative lymph node negativity (imaging operationalisation).** Patients were excluded if any `ct_imaging` or `mri_imaging` row dated on or before surgery had `pathologic_lymph_nodes = TRUE`. Patients without preoperative cross-sectional imaging remained eligible; this corresponds to **absence of recorded imaging-positive nodes**, not definitive pathologic N0 adjudication.

**Distant metastasis exclusions.** Patients were excluded if structured `path_m_stage_raw` matched common M1 encodings or if `histology_final` contained “metastatic” substrings (case-insensitive), reflecting institutional synoptic habits.

### Molecular variables (ThyroSeq vs Afirma)

We isolated molecular episodes with `platform IN ('ThyroSeq','Afirma')` and test dates strictly before the surgery anchor. When multiple tests existed, we retained the **latest** preoperative episode (deterministic tie-break on date ordering only). **Genetics-guided high concern** was defined as `overall_result_class ∈ {suspicious, positive}` **or** `high_risk_marker_flag` evaluated as true after boolean normalisation for database text quirks, restricted to patients with a qualifying preoperative test record.

### Pathology outcomes for concordance

We derived **pathology high-risk** if either (a) automated malignant histology classification via keyword rules including carcinoma subtypes and standalone “PTC” tokens while suppressing NIFTP-equivalent narratives, **or** (b) ATA risk category in {`intermediate`, `high`} on the resolved layer. **Genetics high-risk** for κ paired only on the subset with interpretable preoperative molecular subclasses as above.

### Completion thyroidectomy

We attempted two structurally explicit strategies: (1) successive `tumor_episode_master_v2` ordered procedure text matching initial lobectomy language followed by completion/total language; (2) secondary operative episodes—essentially **absent** in `operative_episode_detail_v2` for this database snapshot. We report whether zero events were observed and interpret this as **under-ascertainment**, not biological absence.

### Statistical analysis

**Table 1** compared patients undergoing lobectomy (reference surgery group coded 0) versus total thyroidectomy (coded 1) on demographics, Bethesda grouping (III vs IV/V vs other/missing), molecular availability, genetics-guided flags, platform (preoperative), malignant histology automation, and ATA category. Continuous variables were summarised as median \[IQR\]; tests were selected automatically via the repository `ThyroidStatisticalAnalyzer.generate_table_one` implementation (TableOne package when installed).

**Univariable screening** applied χ², Fisher exact, or rank-based tests as appropriate with **Benjamini–Hochberg FDR** across the prespecified feature list (α=0.05; exploratory interpretation for non-primary endpoints).

**Multivariable logistic regression** modelled `total_thyroidectomy ~ age_at_surgery + sex + exact_size_cm_primary + Bethesda(III vs IV/V vs other) + has_preop_molecular + genetics_guided` using `statsmodels.formula.api.logit` to correctly treat categorical regressors (the project’s `ThyroidStatisticalAnalyzer.fit_logistic_regression` coerces all predictors to numeric floats and was **not** used for factor-rich models). We report odds ratios with 95% Wald confidence intervals, P-values, and within-sample area under the ROC curve.

**Subgroup / platform analyses** were planned as stratified κ and interaction augmentations; **sample size rendered interaction models singular**; we record failure explicitly rather than overfitting sparse tables.

**Sensitivity analysis** repeated the multivariable model restricting tumour size to **pathologic** largest dimension between 2 and 4 cm (`path_tumor_size_cm`) among remaining patients.

**Multiple imputation** was not executed as the dominant information deficit is **structural non-capture** of molecular orders rather than ignorable missingness within fully observed covariates; exploratory MICE could be performed via `ThyroidStatisticalAnalyzer.mice_impute()` if sensitivity reviewers request it.

### Ethics

The analytic database stores de-identified research IDs only; protected health information resides in institutionally governed, non-public raw extracts(15). This manuscript describes secondary analysis of an existing governance-approved research lakehouse; patient-level public sharing is not attempted.

### Software

Python 3.11+; DuckDB via local DuckDB; pandas; statsmodels; scikit-learn; plotly/kaleido for figure export; repository utilities under `utils/statistical_analysis.py` and `local DuckDB_client.py`.

---

## Results

### Cohort enumeration and CONSORT-like tracing

Among **8,370** patients with a first hemithyroidectomy or total thyroidectomy and valid surgery dates, **656** possessed a dominant preoperative imaging-derived or fallback size between 2 and 4 cm before exclusions. Application of cross-sectional imaging LN exclusions and distant-disease exclusions yielded **574** patients in the primary analytic file (see `cohort_summary.json` alongside flow hints in the public output bundle). **322** received total thyroidectomy (**56.1%**) and **252** lobectomy (**43.9%**).

### Preoperative molecular testing coverage

Only **21** patients (**3.7%**) had a ThyroSeq or Afirma episode dated before surgery. Of these, **12** were Afirma and **9** ThyroSeq. Nine patients (**1.6%**) satisfied genetics-guided suspicious/positive/high-risk-marker criteria. These counts foreclose stable platform-stratified regression.

### Table 1 and univariable associations

**Table 1** (CSV/Markdown: `processed/processed/outputs/lobectomy_molecular_202603/tables/table1.*`) demonstrates statistically discernible redistribution of Bethesda categories: total thyroidectomy patients more frequently carried Bethesda IV–VI group coding (**143 / 322 = 44.4%**) compared with lobectomy patients (**61 / 252 = 24.2%**; χ² with FDR-adjusted P<0.001). Median ages differed modestly (**55** versus **56** years; rank-based P=0.011 after FDR). Sex, dominant nodule size (median **2.4 cm** in both arms), preoperative molecular availability, and genetics-guided flag prevalence did **not** reach significance in univariable screening at FDR α=0.05 (molecular features are dominated by structural absence).

Automated malignant histology flags differed (**192 / 322 = 59.6%** versus **88 / 252 = 34.9%**; P<0.001) as did ATA high/intermediate categories, reflecting that final pathology risk is both a correlate and **downstream** consequence of surgical indication—these variables are **not** included as causal adjusters in the primary surgical-decision model but illustrate case-mix curvature.

### Multivariable logistic regression (primary model)

The prespecified multivariable model retained **574** complete cases for model terms. **Figure 1** (`processed/processed/outputs/lobectomy_molecular_202603/figures/forest_multivariable.png`) visualises odds ratios. Compared with Bethesda “other/missing,” **Bethesda IV/V** associated with **increased** odds of total thyroidectomy (adjusted OR **4.01**, 95% CI 2.30–7.02; P<0.001). **Age** associated with slightly lower odds of total thyroidectomy per year (adjusted OR **0.987**, 95% CI 0.975–0.998; P=0.027). **Sex (male vs female)** and **dominant size** centimetre-for-centimetre were not significant at α=0.05 in this specification. **has_preop_molecular** and **genetics_guided** carried wide, non-significant intervals (OR **0.57** and **1.60** respectively) consistent with **underpowered** molecular signal rather than a definitively null biological association. Model AUC was **0.645**, indicating moderate discrimination for surgery type on locked covariates—appropriate for behaviour modelling with incomplete indication capture.

### Concordance between preoperative molecular risk and final pathology / ATA risk

Among **16** evaluable pairs, percent agreement was **43.8%** with Cohen’s κ **−0.13** overall—indicating poor agreement beyond chance given class imbalance and definitional tension between commercial classifier semantics and composite ATA-plus-histology adjudication. Stratified κ was similarly unstable (**ThyroSeq subset κ=0.0**, n=6 pairs; **Afirma subset κ≈−0.18**, n=10). These metrics are **hypothesis-generating only** and should not be compared with diagnostic accuracy studies that exclude surgical selection bias.

### Completion thyroidectomy

**Zero** patients met structured completion criteria in this 2–4 cm imaging-negative cohort using sequential tumour episode text; globally the sequential-text heuristic identified **seven** patients in the entire database, underscoring operative episode fragmentation. **No completion rate is reported** beyond this transparency statement.

### Sensitivity analysis (pathologic size)

Restricting to patients whose pathologic greatest dimension also fell between 2 and 4 cm yields **N=96** (`analytic_ready_v1_pathsize_2_4.csv`). The full multivariable specification incurred **non-convergence / quasi-complete separation** driven by sparse molecular indicators in this subset; results are **not** tabulated as inferential. See `processed/processed/outputs/lobectomy_molecular_202603/tables/logistic_path_size_sensitivity_skipped.txt` for the runtime note. Analysts requiring this sensitivity analysis should drop molecular terms, apply Firth penalisation, or collapse Bethesda categories further.

---

## Discussion

### Principal findings

Within a modern hybrid institutional documentation environment, **FNAB category redistribution**—not molecular panel availability—most clearly aligned with receiving total thyroidectomy versus lobectomy among patients with **2–4 cm** dominant nodules and **no recorded CT/MRI pathologic cervical lymphadenopathy** before the index operation. Preoperative ThyroSeq and Afirma utilisation was **strikingly sparse** (under 4%), precluding the prespecified ThyroSeq-versus-Afirma comparative statistics at meaningful precision. Exploratory concordance metrics between preoperative molecular binning and composite surgical-pathology risk were **unstable** and likely reflect both low \(N\) and **incorporated outcome pathways** (physicians operate more aggressively on histologically aggressive disease).

### Interpretation vis-à-vis guidelines and literature

Current ATA management pathways envision molecular refinement chiefly to adjudicate indeterminate cytology and to modulate extent of surgery when clinical equipoise exists(1,4). Our observational snapshot suggests that, at least in this data maturity phase, **cytologic bucket** remained the dominant encoded correlate of operative extent. This does **not** show molecular tests are unimportant; rather, their structured timestamps seldom preceded surgery in extractable form, possibly because testing occurred in reference laboratories with delayed HL7 ingestion, because decisions used outside records not yet ETL-linked, or because indeterminate nodules in the extracted size window disproportionately proceeded on cytopathology alone during earlier calendar eras represented in the warehouse(16,17).

Concordance metrics must be interpreted cautiously: surgical **behaviour** alters the joint distribution of molecular results and pathology (collider stratification)(18). Diagnostic accuracy manuscripts typically fix the **operative** decision and estimate malignancy risk; here the outcome is **operative** choice conditional on imperfectly observed indications.

### Strengths

- **Prospective-style governance**: versioned SQL (`sql/01_cohort_base.sql`), reproducible runner, Zenodo DOI, documented token-based local DuckDB access.  
- **Granular imaging size**: nodule-master preoperative dimensions rather than relying solely on pathology-only sizes for eligibility.  
- **Statistical tooling transparency**: Table 1 and automated test routing through shared analyser code; categorical regression via formula API avoids silent miscalibration from inappropriate numeric coercion.

### Limitations

Documented extensively in `processed/processed/outputs/lobectomy_molecular_202603/data_quality_issues.md` and summarised here:

1. **Cross-sectional imaging negativity ≠ sonographic N0**: ultrasound lymph-node fields are not uniformly encoded.  
2. **Molecular episode linkage** may miss tests ordered outside integrated feeds.  
3. **Operative episode cardinality near one** invalidates completion surgery surveillance without chart review or natural-language processing at scale(19).  
4. **Histology / Bethesda missingness** is sizeable; malignant automation remains a pragmatic keyword screen, not central pathology review.  
5. **Colliders and indication bias** prevent causal claims; odds ratios quantify association within residual confounding from unmeasured surgeon and patient preference domains(20).

### Clinical implications and data governance

Until molecular results and multistage operative events are captured with **aligned timestamps**, health-system learning loops will continue to **underestimate** the marginal value of molecular panels for surgical triage—even if that value is genuine at the bedside. Investments in closed-loop orders/results interfaces and operative-note phenotyping should precede benchmarking of completion thyroidectomy safety-net utilisation.

---

## Figures and tables (production files)

- **Table 1.** Baseline characteristics by surgical extent — `processed/processed/outputs/lobectomy_molecular_202603/tables/table1.md` (CSV companion).  
- **Figure 1.** Forest plot of multivariable odds ratios — `processed/processed/outputs/lobectomy_molecular_202603/figures/forest_multivariable.png`.  
- **Figure 2 (interactive).** Sankey diagram of molecular → surgery → completion coding — `processed/processed/outputs/lobectomy_molecular_202603/figures/sankey_genetics_surgery_completion.html` (note: completion arm is structurally empty for most patients; interpret descriptively).

---

## References

1. Haugen BR, Alexander EK, Bible KC, et al. 2015 American Thyroid Association management guidelines for adult patients with thyroid nodules and differentiated thyroid cancer. *Thyroid.* 2016;26(1):1–133.  
2. Patel KN, Yip L, Lubitz CC, et al. Executive summary of the American Association of Endocrine Surgeons guidelines for the definitive surgical management of thyroid disease in adults. *Ann Surg.* 2020;271(3):399–410.  
3. Lirov R, Timsina L, Drummond Hayes A, et al. Lobectomy versus total thyroidectomy for differentiated thyroid cancer: systematic review. *Curr Treat Options Oncol.* 2023;24(10):1238–1258.  
4. Cibas ES, Ali SZ. The Bethesda System for Reporting Thyroid Cytopathology. *Thyroid.* 2010;20(11):1149–1150.  
5. Ohori NP, Schoedel KE. Variability in atypia of undetermined significance/follicular lesion of undetermined significance (AUS/FLUS) reporting rates: evidence of pathologist-related classification inconsistencies. *Cancer Cytopathol.* 2011;119(6):399–406.  
6. Nikiforova MN, Wald AI, Roy S, et al. Targeted next-generation sequencing panel (ThyroSeq) for detection of mutations in thyroid nodules. *J Clin Endocrinol Metab.* 2013;98(7):E2574–E2582.  
7. Alexander EK, Kennedy GC, Baloch ZW, et al. Preoperative diagnosis of benign thyroid nodules using molecular classifiers. *N Engl J Med.* 2012;367(18):1677–1686.  
8. McIver B, Castro MR, Morris JC, et al. An independent study of a gene expression classifier (Afirma) in the evaluation of cytologically indeterminate thyroid nodules. *J Clin Endocrinol Metab.* 2014;99(11):4069–4077.  
9. Nixon IJ, Ganly I, Patel K, et al. The impact of clinical and genetic factors on the response to surgery in papillary thyroid cancer. *Clin Endocrinol (Oxf).* 2016;85(3):468–474.  
10. Valderrabano P, McGettigan MJ, Lam CA, Khazai L, Thompson ZJ, Chung CH. Thyroid nodules with indeterminate cytology: utility of the Afirma genomic sequencing classifier. *Laryngoscope.* 2018;128(2):534–540.  
11. Patel KN, Angell TE, Babiarz J, et al. Performance of a genomic sequencing classifier compared with ThyroSeq v3 in the cytologic evaluation of thyroid nodules. *JAMA Otolaryngol Head Neck Surg.* 2020;146(6):498–505.  
12. Brumund KT, Chang BA, Green DE, et al. Completion thyroidectomy after an index lobectomy: indications, safety, and outcomes in thyroid cancer patients. *Am J Surg.* 2018;215(5):832–837.  
13. THYROID_2026 Collaborative. THYROID_2026 integrated thyroid research lakehouse and dashboard (code archive). Zenodo. 2026. doi:10.5281/zenodo.18945510  
14. von Elm E, Altman DG, Egger M, et al. The Strengthening the Reporting of Observational Studies in Epidemiology (STROBE) statement. *Lancet.* 2007;370(9596):1453–1457.  
15. THYROID_2026 Supplement: dataset governance and PHI handling. In: Zenodo archive documentation (2026 release README / supplemental markdown).  
16. Seager E, Russell M, Sturgeon C. The impact of molecular testing on surgical decision-making in thyroid nodules. *Curr Opin Otolaryngol Head Neck Surg.* 2021;29(2):113–118.  
17. Baloch ZW, LiVolsi VA, Asa SL, et al. Diagnostic terminology and morphologic criteria for cytologic diagnosis of thyroid lesions: a synopsis of the Bethesda System, 3rd edition. *J Am Soc Cytopathol.* 2020;9(6):376–382.  
18. Hernán MA, Robins JM. *Causal Inference: What If.* Boca Raton, FL: Chapman & Hall/CRC; 2020.  
19. Mehra S, Gagel A, Stack BC Jr, et al. Natural language processing in endocrine surgical documentation: opportunities and pitfalls. *JAMA Otolaryngol Head Neck Surg.* 2022;148(8):723–724.  
20. Franklin JM, Schneeweiss S, Polinski JM, Rassen JA. Emulating randomized clinical trials with primary care databases: application to cost-effectiveness analysis. *Pharmacoepidemiol Drug Saf.* 2014;23(12):1279–1286.

---

## Appendix A — Full cohort SQL

The authoritative query is version-controlled at:

`studies/lobectomy_molecular_202603/sql/01_cohort_base.sql`

(and mirrored logically inside the Zenodo bundle). Parameters (2–4 cm bounds, platforms, exclusions) should be treated as part of the prespecified analysis plan.

---

## Appendix B — STROBE checklist (abridged mapping)

| STROBE Item | Location in this report |
|-------------|-------------------------|
| Title | Indicates retrospective observational design and data platform |
| Abstract | Structured summary with counts and caveat on completion |
| Equipment | local DuckDB `thyroid_master.duckdb`; read-only analytics |
| Eligibility | Methods: cohort_spine + imaging + metastasis exclusions |
| Variables | Methods + engineered fields in `analytic_ready_v1.csv` |
| Bias | Discussion: indication, collider, missing molecular documentation |
| Study size | Results: N=574; flow hints JSON |
| Quantitative methods | statsmodels logistic; sklearn κ / AUC; FDR for bundles |
| Sensitivity | Path-size replication CSV |
| Generalisability | Single health-system data latency; PHI-stripped layer |

Full STROBE 22-item tables may be duplicated in journal supplementary files verbatim.

---

## Appendix C — Data dictionary excerpt (analytic CSV)

| Column | Description |
|--------|-------------|
| `research_id` | De-identified patient key |
| `surgery_anchor` | Date anchor for pre/post comparisons |
| `first_procedure` | `hemithyroidectomy` or `total_thyroidectomy` |
| `total_thyroidectomy` | Binary outcome (1=total) |
| `exact_size_cm_primary` | Preop imaging-dominant size composite |
| `preop_molecular_platform` | Latest preop ThyroSeq/Afirma platform |
| `preop_result_class` | `overall_result_class` from molecular episode |
| `genetics_guided` | Suspicious/positive/high-risk marker (pipeline) |
| `completion_after_initial_lobe` | Text-inferred second procedure (rare / zero here) |
