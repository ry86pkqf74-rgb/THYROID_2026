---
manuscript_id: M038
title: Massive Goiter at a Tertiary Referral Center — A Composite-Definition Descriptive Cohort of 2,501 Patients (Emory University, 1999–2025)
authors: Glosser L, [Senior author], et al.
status: First draft v1 (Cowork-assisted, 2026-05-01)
target_journal: Surgery / Annals of Surgical Oncology / Thyroid (TBD)
priority: Medium (per manuscript_feasibility_v1; promoted to first-draft after RQ definition)
cohort_view: manuscript_workspace.cohort_m038_massive_goiter_v1
underlying_data: thyroid_canonical_publication_v1_0 (release pub_v1_1_20260504)
canonical_version_at_scoring: v1_0_post_mig_248
research_question: >-
  Among patients undergoing thyroid surgery, what is the demographic, pathologic,
  surgical, and perioperative-complication profile of "massive goiter" defined as
  a composite of (i) gland weight ≥100 g, (ii) substernal extension on CT or MRI,
  or (iii) imaging-confirmed airway compromise (tracheal deviation, narrowing,
  or airway compromise)?
exposure_definition: >-
  is_massive = (gland_weight_final_g ≥ 100)
              OR (ct_substernal_extension_any OR mri_substernal_any)
              OR (ct_tracheal_deviation_any OR ct_tracheal_narrowing_any
                  OR ct_airway_compromise_any)
exposure_n: 2501
comparator_n: 8370
tables_used: M038_Table_1, M038_Table_2, M038_Table_3, M038_Table_4 (all to be materialized in subsequent Lane M refresh; current draft cites query-time aggregates)
---

# Massive Goiter at a Tertiary Referral Center: A Composite-Definition Descriptive Cohort of 2,501 Patients (Emory University, 1999–2025)

## Abstract

**Background.** "Massive" or "complex" goiter — variably defined in the surgical literature as gland weight ≥100 g, substernal/retrosternal extension, or symptomatic airway compromise — represents a clinically distinct surgical population, but published cohorts often rely on single-axis definitions and modest sample sizes, limiting cross-study comparability and outcome generalization. We describe a 25-year, single-institution cohort assembled under a composite anatomic-imaging definition.

**Methods.** Patients undergoing thyroid surgery at Emory University Hospital between 1999 and 2025 (N=10,871 distinct `research_id`) were retrospectively identified from the institutional electronic health record, synoptic surgical pathology workbooks, radiology-derived CT/MRI extractions, and natural language processing (NLP) airway pipelines, harmonized into the publication-tier MotherDuck database `thyroid_canonical_publication_v1_0` (release `pub_v1_1_20260504`). A composite "massive goiter" exposure was defined as gland weight ≥100 g OR substernal extension on CT or MRI OR imaging-confirmed airway compromise (CT tracheal deviation, narrowing, or airway compromise). Demographic, pathologic, surgical, and perioperative-complication distributions were computed for the massive vs non-massive cohorts and reported as n (%) for categorical variables and mean ± SD or median for continuous variables.

**Results.** Of 10,871 patients, **2,501 (23.0%)** met the composite massive criterion: 1,429 (57.1%) by weight ≥100 g, 1,047 (41.9%) by substernal extension, and 1,440 (57.6%) by airway compromise (overlapping flags permitted). The massive cohort was older (mean age 55.4 vs 50.5 years), was less female-predominant (70.8% vs 79.9% female), and had a substantially different self-reported race distribution (62.2% Black or African American vs 31.2% in the non-massive cohort; 28.5% White vs 54.4%). The malignancy rate was lower in the massive cohort (25.8% vs 41.7%), consistent with the predominant referral being benign multinodular and substernal goiter; among the 646 malignant massive cases, papillary thyroid carcinoma (PTC) was less dominant (64.6% vs 80.9% in the broader cohort) with relative enrichment of follicular (15.0%), medullary (5.0%), and aggressive variants (poorly differentiated, anaplastic). Surgical procedure type, when documented, favored total thyroidectomy in the massive cohort (1,113/1,741 known = 64.0%) versus near-parity in non-massive cases (3,448/6,983 known = 49.4%). The any-confirmed-complication flag was elevated in the massive cohort (35.5% vs 19.1%); confirmed RLN injury and hematoma counts remained sparse in both arms (0.32% / 0.28% massive vs 0.37% / 0.25% non-massive). All-cause in-record mortality was 2.36% in massive vs 1.59% in non-massive patients. Era-stratified prevalence of the composite massive flag rose from approximately 12% in 1999–2014 to 24.9% (2015–2019) and 28.5% (2020–2025), reflecting a combination of expanded cross-sectional imaging documentation, the institutional NLP airway pipeline rollout, and possible referral-pattern evolution.

**Conclusions.** Under a transparent composite anatomic-imaging definition, massive goiter accounted for nearly one-quarter of thyroid surgical volume at this tertiary referral center over 25 years, with distinct demographic, pathologic, and surgical-procedure profiles versus non-massive cases and an elevated any-complication burden. The composite-flag construction is portable to other publication-tier institutional cohorts and provides a richer denominator for downstream substudies of operative approach, airway management, and disparities than any single-axis definition.

---

## 1. Introduction

Goiter — diffuse or nodular enlargement of the thyroid gland — encompasses a wide clinical spectrum from incidentally noted multinodular disease to symptomatic, airway-displacing, retrosternal masses requiring complex surgical exposure. The terms "massive," "giant," "complex," and "challenging" goiter appear interchangeably in the surgical literature, with operational definitions ranging from gland weight ≥100 g, ≥200 g, or ≥1 kg, to the presence of substernal/retrosternal extension on cross-sectional imaging, to clinical or imaging features of airway compromise. The heterogeneity of these definitions complicates aggregation across published series and limits the precision of operative-risk and resource-utilization estimates derived from any single-axis definition.

Single-institution cohorts are well-suited to characterize this surgically distinct population because they integrate granular pathology, imaging, and operative-detail extraction at the patient level — content that population-based registries (SEER, NCDB) cannot provide. Recent advances in structured extraction of cross-sectional imaging features and NLP processing of operative and radiology narratives further enable composite anatomic-imaging definitions that capture the surgically meaningful "massive goiter" phenotype across multiple complementary axes simultaneously.

We describe a 25-year (1999–2025), single-institution thyroid surgery cohort at Emory University Hospital and apply a composite "massive goiter" exposure definition spanning gland weight, substernal extension on CT or MRI, and imaging-confirmed airway compromise. Within this composite-defined denominator we report demographic, pathologic, surgical-procedure, and perioperative-complication distributions, with comparison to the non-massive thyroid surgical population. The objective is to provide a transparent, reproducible, multi-axis descriptive baseline that supports downstream substudies in operative approach (extracervical exposure, sternotomy decision-making), airway management, malignant histology subtypes preferentially encountered in massive disease, and demographic disparity in massive-disease referral.

## 2. Methods

### 2.1 Study design and setting

Retrospective single-institution descriptive cohort study of patients undergoing thyroid surgery at Emory University Hospital between 1999 and 2025. The protocol was approved by the Emory University Institutional Review Board (IRB # — *to be inserted*).

### 2.2 Data sources and harmonization

All structured fields were extracted from the institutional electronic health record, synoptic surgical pathology workbooks, laboratory feeds, radiology-derived CT and MRI characterizations, and NLP pipelines coordinated under the Thyroid 2026 research registry. Analytic-ready tables reside in the MotherDuck database `thyroid_canonical_publication_v1_0` (`main` schema + derivative `semantic_publication` manuscript-safe projections + selected `manuscript_workspace` cohort views). The canonical patient spine is `main.canonical_patient_master` (10,871 distinct `research_id` rows). The manuscript-tier database freeze for this analysis is `release_id='pub_v1_1_20260504'` (`semantic_publication.release_manifest_v1`); the manuscript feasibility table was last scored at `canonical_version_at_scoring='v1_0_post_mig_248'`. Detailed Methods boilerplate and reproducibility anchors mirror Section 1–2 of `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`.

### 2.3 Cohort and exposure definition

The denominator is the full surgical cohort (n=10,871) read directly from `manuscript_workspace.cohort_m038_massive_goiter_v1`, which surfaces patient-grain demographics, gland weight, CT/MRI substernal and airway findings, NLP airway extractions, surgical procedure type, perioperative complication flags, and follow-up duration.

The composite **massive goiter** exposure was defined as the disjunction of three component flags:

1. **Weight component**: `gland_weight_final_g ≥ 100`
2. **Substernal component**: `ct_substernal_extension_any = TRUE OR mri_substernal_any = TRUE`
3. **Airway component**: `ct_tracheal_deviation_any = TRUE OR ct_tracheal_narrowing_any = TRUE OR ct_airway_compromise_any = TRUE`

A patient was classified as **massive** if any component flag was TRUE. Missing values for any component were coalesced to FALSE; sensitivity analysis to a complete-data sub-cohort is recommended at peer-review stage.

### 2.4 Variable specifications

Demographic, surgical, and outcome variables follow the canonical specifications of the v1.0 publication Methods document (Section 2 and Section 8). Surgical procedure type is harmonized to total_thyroidectomy / hemithyroidectomy / other / unknown buckets per `canonical_operative_events_v1` lineage. Perioperative complications are reported from confirmed-event flags (`comp_rln_injury_confirmed`, `comp_hematoma_confirmed`) and from the rolled-up `any_confirmed_complication_flag`. Era stratification uses `surg_first_date` binned into 5-year periods (1999–2004, 2005–2009, 2010–2014, 2015–2019, 2020–2025). Follow-up duration is `followup_years` from `main.canonical_survival_followup_v1` (Protocol v2, mig_123 lineage).

### 2.5 Statistical analysis

Descriptive statistics: n (%) for categorical variables; mean ± SD or median (IQR) for continuous variables. No comparative inferential testing is preregistered for this descriptive draft; reviewer-driven hypothesis tests would be added in Methods §2.6 (currently reserved). Aggregations were computed at query time against `cohort_m038_massive_goiter_v1`; a follow-on Lane M refresh will materialize the four manuscript tables (Table 1–4) as CSV exports under `manuscript_outputs/v1_0_20260501/M038_Table_*_v1_0_20260501.csv`.

## 3. Results

### 3.1 Cohort assembly and composite-flag composition

Of 10,871 patients, 2,501 (23.0%) met the composite massive criterion. Component contributions and overlaps:

| Component / overlap class | n |
|---|---:|
| Weight ≥100 g (any cause) | 1,429 |
| Substernal (CT or MRI, any cause) | 1,047 |
| Airway compromise (CT, any cause) | 1,440 |
| Weight ∩ Substernal | 404 |
| Weight ∩ Airway | 513 |
| Substernal ∩ Airway | 884 |
| All three | 386 |
| Weight only | 898 |
| Substernal only | 114 |
| Airway only | 309 |

Inclusion-exclusion check: 1,429 + 1,047 + 1,440 − 404 − 513 − 884 + 386 = 2,501 (consistent with the cohort flag).

### 3.2 Demographics and malignancy rate (Table 1)

| Variable | Massive (n=2,501) | Non-massive (n=8,370) |
|---|---:|---:|
| Mean age at surgery (yrs) | 55.4 | 50.5 |
| Median age | 56 | 50 |
| Female | 1,771 (70.8%) | 6,688 (79.9%) |
| Male | 730 (29.2%) | 1,682 (20.1%) |
| Black or African American | 1,555 (62.2%) | 2,613 (31.2%) |
| White | 714 (28.5%) | 4,552 (54.4%) |
| Asian | 57 (2.3%) | 419 (5.0%) |
| Unknown / Not Reported | 130–131 (≈5.2%) | 591–599 (≈7.2%) |
| Malignant (`is_malignant=TRUE`) | 646 (25.8%) | 3,491 (41.7%) |

Two patterns merit emphasis. First, the male proportion is higher in the massive cohort (29.2% vs 20.1%), an enrichment consistent with prior reports that anatomically large or substernal goiter occurs more frequently in men despite the overall female predominance of thyroid disease. Second, the racial composition of the massive cohort differs substantively from the non-massive arm (Black or African American 62.2% vs 31.2%); this referral-pattern observation is discussed in §4 in the context of disparity of access to early surgical management of nodular thyroid disease.

### 3.3 Histology distribution within the malignant subset (Table 2)

Among the 646 malignant patients in the massive cohort, the histologic distribution differs from the broader malignant cohort previously reported (M032; n=4,022): PTC accounted for 417 patients (64.6%) versus 80.9% in M032; follicular carcinoma 97 (15.0%); medullary carcinoma 32 (5.0%); poorly differentiated thyroid carcinoma 22 (3.4%); anaplastic carcinoma 13 (2.0%); NIFTP 25 (3.9%); FTUMP 9 (1.4%); plus a long tail of metastatic and rare variants (NUT carcinoma n=1; "infiltrating carcinoma with thymus-like differentiation" n=1). The reduction in PTC dominance and relative enrichment of follicular, medullary, and aggressive (poorly-differentiated, anaplastic) variants is consistent with the clinical impression that locally advanced or anatomically expansive disease over-represents non-PTC histologies; subtype-stratified surgical outcome analyses are an appropriate downstream substudy.

### 3.4 Surgical procedure type (Table 3)

Procedure type was documented for 1,741 of 2,501 massive patients (69.6%) and for 6,983 of 8,370 non-massive patients (83.4%). Among documented cases:

| Procedure | Massive | Non-massive |
|---|---:|---:|
| Total thyroidectomy | 1,113 (64.0% of known) | 3,448 (49.4% of known) |
| Hemithyroidectomy | 604 (34.7%) | 3,205 (45.9%) |
| Other | 24 (1.4%) | 330 (4.7%) |
| Unknown | 0 | 9 |

Total thyroidectomy was the predominant procedure in the massive cohort, consistent with practical recommendations to avoid contralateral re-operation when bilateral substernal or anatomically complex disease is present. The 30.4% null-procedure-type rate in the massive cohort (vs 16.5% in non-massive) is a notable data-quality observation discussed under §5 Limitations.

### 3.5 Perioperative complications (Table 4)

| Outcome | Massive (n=2,501) | Non-massive (n=8,370) |
|---|---:|---:|
| Any confirmed complication flag | 888 (35.5%) | 1,602 (19.1%) |
| Confirmed RLN injury | 8 (0.32%) | 31 (0.37%) |
| Confirmed hematoma | 7 (0.28%) | 21 (0.25%) |
| All-cause in-record mortality | 59 (2.36%) | 133 (1.59%) |
| Mean follow-up (years, all) | 1.22 | 1.84 |
| Follow-up >0 years | 997 (39.9%) | 4,131 (49.4%) |
| Mean follow-up (yrs, FU>0) | 3.06 | 3.71 |

Two methodological notes: (i) the confirmed-flag denominators for RLN injury and hematoma are sparse across both arms — the absolute counts are too small to support precise relative-risk estimation in this descriptive draft, and reflect both true low complication frequency and the conservative confirmation criteria embedded in the canonical complication adjudication; the broader `any_confirmed_complication_flag` is the more sensitive endpoint, where the relative-risk pattern (massive vs non-massive ≈ 1.86) is consistent with surgical literature on anatomically complex thyroidectomy. (ii) Follow-up duration is right-skewed and 60% of massive-cohort patients have a recorded `followup_years` of zero — a pattern that reflects the institutional follow-up pipeline rather than absence of survival data and that is acknowledged in §5.

### 3.6 Era stratification

| Era | Total n | Massive n | % Massive |
|---|---:|---:|---:|
| 1999–2004 | 901 | 110 | 12.2% |
| 2005–2009 | 1,191 | 142 | 11.9% |
| 2010–2014 | 1,885 | 240 | 12.7% |
| 2015–2019 | 2,935 | 731 | 24.9% |
| 2020–2025 | 1,817 | 517 | 28.5% |
| Surgical date unknown | 2,140 | 761 | 35.6% |

The roughly two-fold rise in measured massive-flag prevalence from the pre-2015 to the post-2015 era is unlikely to reflect true clinical change of equivalent magnitude. The most plausible drivers are increased cross-sectional imaging documentation (more CT/MRI surgical-planning studies entered into structured workflow), the institutional NLP airway pipeline rollout (which adds the airway-component flag for cases that pre-existed in the cohort but lacked structured airway documentation), and possible incremental shift in tertiary-referral case complexity. The 19.7% (2,140/10,871) of patients with no recorded surgical date is a known cohort-flow caveat carried in `cohort_m032_descriptive_25yr_v1` lineage and inherited here.

## 4. Discussion

This descriptive analysis documents that under a transparent composite anatomic-imaging definition, "massive goiter" accounts for nearly a quarter of all thyroid surgical volume at a single tertiary referral center over a 25-year span (n=2,501 of 10,871). The composite definition deliberately spans three complementary axes — gland weight, substernal extension, and imaging-confirmed airway compromise — to avoid the misclassification risk inherent in single-axis literature definitions. The component overlap pattern is informative: of the 2,501 massive cases, 386 (15.4%) carry all three flags (the "anatomically complete" massive-goiter phenotype), while substantial single-flag subsets exist (898 weight-only, 114 substernal-only, 309 airway-only), each of which would be missed by alternative single-axis screens. This argues for a composite definition as the more clinically inclusive baseline for surgical-cohort epidemiology.

Three demographic and pathologic patterns deserve emphasis. First, the male enrichment of the massive cohort (29.2% vs 20.1%) replicates prior surgical-series findings and is biologically consistent with the longer disease duration that anatomically expansive nodular disease often requires. Second, the marked over-representation of Black or African American patients in the massive cohort (62.2% vs 31.2% of non-massive surgical patients at the same institution) is a striking and previously underreported referral-pattern signal in the U.S. South tertiary surgical context. While our data cannot disentangle access-to-care, primary-care surveillance, surgical-referral-threshold, and biological factors, the observation merits further investigation as a candidate driver of advanced-disease surgical disparity. Third, the histologic profile of the malignant subset within the massive cohort is shifted away from PTC dominance and toward follicular, medullary, and aggressive variants — consistent with the inferior-pole-extending, multinodular, and anatomically infiltrating phenotype that often co-occurs with non-PTC histologies.

The increased any-complication rate in the massive arm (35.5% vs 19.1%) confirms expectation but the confirmed-RLN and confirmed-hematoma counts are too small in either arm to support robust relative-risk estimation; future analyses will need to leverage either (i) the broader confirmed/probable adjudication tier, (ii) external pooling, or (iii) extended chart-review remediation for the small number of complication-suspicious cases lacking structured confirmation. The total-thyroidectomy preference in the massive cohort (64.0% of known procedures vs 49.4% non-massive) is consistent with operative practice in bilateral substernal disease, but the 30.4% null-procedure-type rate in the massive cohort is a data-quality limitation that requires Lane M follow-up before definitive procedure-distribution claims are made.

The era-stratified rise in measured massive prevalence is most plausibly explained by improved structured documentation rather than true clinical incidence change of similar magnitude, particularly in light of the 19.7% surgical-date-unknown subset (which itself shows 35.6% massive flag — likely reflecting a pre-2010 documentation tier that disproportionately surfaces only the most clinically conspicuous cases).

## 5. Limitations

1. **Composite-definition ascertainment asymmetry.** The three component flags rely on different underlying documentation streams: gland weight on synoptic pathology, substernal extension on CT/MRI structured extraction, and airway compromise on CT-derived imaging features. Coverage is heterogeneous across eras (gland weight 86.3% known in massive cohort; surgical date 69.6% known; procedure type 69.6% known), and a sensitivity analysis restricted to complete-data sub-cohorts is recommended at peer review.

2. **Era confounding with documentation expansion.** The pre-2015 vs post-2015 rise in measured massive prevalence is partially attributable to the introduction of structured NLP airway extraction and increased CT/MRI documentation in the institutional workflow rather than to true clinical incidence change.

3. **Procedure-type missingness in massive cohort.** 30.4% of massive-cohort patients lack a documented procedure type (vs 16.5% non-massive), reducing precision of approach-specific claims; Lane M follow-up to recover procedure detail from operative narrative NLP is recommended.

4. **Confirmed-complication flag sparsity.** Confirmed RLN injury (n=8 massive, 31 non-massive) and confirmed hematoma (n=7 massive, 21 non-massive) counts preclude precise relative-risk estimation; the broader `any_confirmed_complication_flag` is the more sensitive endpoint at the cost of specificity.

5. **Follow-up duration distribution.** The mean recorded `followup_years` is 1.22 (massive) vs 1.84 (non-massive), with 60% of massive-cohort patients carrying a recorded value of zero. This reflects pipeline-level capture rather than absence of long-term outcome data; the mean among patients with FU>0 is 3.06 years (massive) vs 3.71 years (non-massive). Time-to-event substudies must use censoring rules tied to `canonical_recurrence_resolved_v1` and `canonical_survival_followup_v1`.

6. **No ECMO column.** The original manuscript framing (per `manuscript_feasibility_v1` title "ECMO Support for Massive Goiter Surgery") implied an ECMO-supported surgical sub-cohort; no structured ECMO indicator is present in `cohort_m038_massive_goiter_v1`. Any ECMO-specific claim would require chart-review remediation or a dedicated NLP extraction of operative airway-support narratives, neither of which is in the present cohort scope.

7. **No safe-view recurrence/survival join in the present draft.** The cohort view does not currently expose `canonical_recurrence_resolved_v1`-derived recurrence flags or detailed `canonical_survival_followup_v1` fields beyond aggregate `followup_years` and `death_occurred`; downstream survival substudies must explicitly join through the publication-tier safe views.

## 6. Conclusions

Massive goiter, defined by a transparent composite of gland weight ≥100 g, substernal extension on CT or MRI, or imaging-confirmed airway compromise, accounts for 23.0% of thyroid surgical volume at this 25-year, single-institution tertiary referral cohort. The composite-defined population differs from the broader thyroid surgical denominator on age, sex, race, malignancy rate, malignant histologic distribution, surgical procedure type, and perioperative complication burden. The composite definition is portable to other publication-tier institutional cohorts and provides a richer denominator for downstream substudies of surgical approach, airway management, malignant histology subtype, and demographic disparity in massive-disease referral. Subsequent substudies — specifically, sternotomy decision-making, NLP-extracted operative airway-support details, and disparity-in-referral analysis — are the priority follow-on work.

---

## Tables (to be materialized as CSV in subsequent Lane M refresh)

- **Table 1 — Demographics and Malignancy Rate (Massive vs Non-massive)** (`M038_Table_1_demographics_v1_0_20260501.csv`)
- **Table 2 — Histology Distribution within the Malignant Subset** (`M038_Table_2_histology_v1_0_20260501.csv`)
- **Table 3 — Surgical Procedure Type Distribution** (`M038_Table_3_procedure_v1_0_20260501.csv`)
- **Table 4 — Perioperative Complications and Follow-up** (`M038_Table_4_complications_v1_0_20260501.csv`)

## Figures

- **Figure 1 — Composite Massive-Goiter Flag Composition** (Venn or UpSet diagram of weight / substernal / airway component overlap; deferred to publication submission)
- **Figure 2 — Era-Stratified Massive Flag Prevalence** (line plot 1999–2025; deferred)

## Reproducibility

Cohort view: `manuscript_workspace.cohort_m038_massive_goiter_v1` (auto-generated stub; source per `detail_table_registry_v1` Script 281)
Underlying database: MotherDuck `thyroid_canonical_publication_v1_0` (release `pub_v1_1_20260504`)
Feasibility row at draft generation: `manuscript_feasibility_v1` row `manuscript_id=38`, `canonical_version_at_scoring='v1_0_post_mig_248'`, `feasibility_color=GREEN`, `gating_issues='Define research question; refreshed post-mig_248'`, scored 2026-05-01 09:18 UTC
Composite exposure SQL (executable): see Methods §2.3
Methods boilerplate: `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` (Section 1–2 + Section 9 limitations)
Cowork reference commit at draft generation: HEAD `e821e97` (`docs(qc): v21 handoff — post-mig_250, M044 ETE work moves to ChatGPT lane`)
Companion canonical-publication descriptive paper: M032 (`manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md`)

---

## Drafting Notes (NOT FOR SUBMISSION)

This first draft was generated 2026-05-01 by Cowork using query-time aggregates against `manuscript_workspace.cohort_m038_massive_goiter_v1` (full canonical n=10,871) plus the v1.0 Methods doc as boilerplate source. Manuscript-specific CSV tables (Table_1–Table_4) are NOT yet materialized — a follow-on Lane M refresh should produce them under `manuscript_outputs/v1_0_20260501/M038_Table_*_v1_0_20260501.csv` with executable SELECTs in `qc_framework_v1/manuscript/lane_m_m038/`.

**Composite-exposure decision recap.** The original v21 §5(B) options were volume-stratified, substernal, airway, or composite. Composite was selected to avoid the single-axis misclassification risk inherent in literature definitions and to retain analytic power across the three documentation streams. A sensitivity analysis to a "weight ≥100 g only" or "substernal only" alternative definition is recommended at peer review.

**Author-input gaps requiring resolution before submission:**

1. **Title finalization** — current working title is descriptive; consider tightening to a journal scope (e.g., a Surgery / Thyroid 60-character constraint).
2. **Authorship list** — placeholder only; PI to confirm author order, affiliations, and corresponding author. Likely co-authors include thyroid surgical and endocrine-oncology faculty, plus the Thyroid 2026 informatics team.
3. **Target journal selection** — Surgery (well-suited for surgical descriptive series), Annals of Surgical Oncology (if histology and oncologic-outcome emphasis is added), or Thyroid (if a more endocrine-pathology framing is preferred).
4. **IRB approval number** — placeholder text in §2.1; insert specific protocol number from M032 lineage.
5. **Composite-flag sensitivity analysis** — produce alternative-definition tables (weight-only ≥100 g, weight-only ≥200 g, substernal-only, airway-only) and report concordance/discordance with the composite flag. Estimated 1 query-pass plus one supplementary table.
6. **Procedure-type recovery for the massive cohort** — 760 of 2,501 massive patients lack documented `surg_procedure_type`; chart-review or operative-narrative NLP recovery would tighten the procedure-distribution claims in §3.4.
7. **Disparity discussion (race composition)** — the §4 paragraph on Black or African American over-representation in the massive cohort is a defensible observation but requires (i) confirmation that institutional EHR registration ascertainment is comparable across eras, and (ii) a literature scan for prior reports of demographic disparity in nodular-thyroid surgical referral. Recommended depth-add at peer review.
8. **Era-rise interpretation** — the §3.6 / §4 attribution of post-2015 prevalence rise to documentation expansion vs true incidence change should be supported by a side analysis showing era-stratified component-flag coverage (i.e., what fraction of pre-2015 vs post-2015 cases have CT documentation at all).
9. **Figure 1 rendering** — produce a Venn or UpSet diagram of the three-component overlap from §3.1 numbers; matplotlib-venn or UpSetR are reasonable rendering paths.
10. **References** — BibTeX stubs at `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib`; expand for substernal/retrosternal goiter surgical series (Adam et al., White et al., Cohen et al.), airway compromise in goiter surgery, anaplastic and aggressive thyroid carcinoma in massive disease, and disparity in thyroid surgical referral.

**Optional supplementary content (per reviewer feedback or word-count availability):**

- Stratified analysis by exposure_class (1_all_three / 2_weight_substernal / … / 7_airway_only) with outcome-counts table — already computed at draft generation, can be lifted directly from §3.1 output.
- ICU length-of-stay, intraoperative blood loss, operative duration — available only via supplemental chart review or `op_findings_summary` NLP parsing; not in present scope.
- Sternotomy or extracervical approach indicator — not present as structured flag in cohort view; would require operative-narrative NLP extraction.

**Cross-references with other manuscripts in the cohort:**

This manuscript cites M032 as the anchor descriptive cohort for the broader 25-year denominator. Substudy linkages include M037/M043 (LN predictors — relevant for the 25.8% malignant subset within massive), M046 (NIFTP-era Bethesda — addresses the NIFTP/FTUMP n=34 subset surfaced here), and the proposed but not-yet-active M038_followon set on operative approach, airway management, and disparity.

---

**Status:** First draft v1, 2026-05-01. Ready for PI review, RQ-confirmation pass, and Lane M Table CSV materialization.
**Cowork reference commit:** `e821e97` (`docs(qc): v21 handoff — post-mig_250, M044 ETE work moves to ChatGPT lane`).
**Cowork session context:** mig_249 (manuscript_feasibility_v1 re-refresh) landed mid-session at 2026-05-01 09:18 UTC; this draft is scored against `v1_0_post_mig_248`.
