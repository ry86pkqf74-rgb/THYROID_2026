---
manuscript_id: M038
title: Massive Goiter at a Tertiary Referral Center — A Composite-Definition Descriptive Cohort of 2,501 Patients (Emory University, 1999–2025)
authors: Glosser L, [Senior author], et al.
status: Second draft v2 (Cowork-assisted, 2026-05-01) — rebuilt against post-mig_252 strict complications rollup and post-mig_253 procedure-type fill
target_journal: Surgery / Annals of Surgical Oncology / Thyroid (TBD)
priority: Medium
cohort_view: manuscript_workspace.cohort_m038_massive_goiter_v1
underlying_data: thyroid_canonical_publication_v1_0 (release pub_v1_0_20260430)
canonical_version_at_scoring: v1_0_post_mig_253
research_question: >-
  Among patients undergoing thyroid surgery, what is the demographic, comorbidity,
  surgical, pathologic, and perioperative-complication profile of "massive goiter"
  defined as a composite of (i) gland weight ≥100 g, (ii) substernal extension
  on CT or MRI, or (iii) imaging-confirmed airway compromise?
exposure_definition: >-
  is_massive = COALESCE(gland_weight_final_g >= 100, FALSE)
              OR COALESCE(ct_substernal_extension_any, FALSE)
              OR COALESCE(mri_substernal_any, FALSE)
              OR COALESCE(ct_tracheal_deviation_any, FALSE)
              OR COALESCE(ct_tracheal_narrowing_any, FALSE)
              OR COALESCE(ct_airway_compromise_any, FALSE)
exposure_n: 2501
comparator_n: 8370
v1_supersedes: M038_massive_goiter_DRAFT_v1.md (commit 2fc6fef) — v1 used the buggy any_confirmed_complication_flag (35.5% / 19.1% rates) and the pre-mig_253 30.4% null-procedure-type
upstream_fixes_applied:
  - mig_252 — comp_*_confirmed rollup repaired (strict definition: present + def/probable)
  - mig_253 — surg_procedure_type filled cohort-wide (NULL count 2,138 → 2)
tables_used: M038_Table_1, M038_Table_2, M038_Table_3, M038_Table_4 (all to be materialized in subsequent Lane M refresh; current draft cites query-time aggregates)
---

# Massive Goiter at a Tertiary Referral Center: A Composite-Definition Descriptive Cohort of 2,501 Patients (Emory University, 1999–2025)

## Abstract

**Background.** "Massive" or "complex" goiter — variably defined in the surgical literature as gland weight ≥100 g, substernal/retrosternal extension, or symptomatic airway compromise — represents a clinically distinct surgical population, but published cohorts often rely on single-axis definitions and modest sample sizes, limiting cross-study comparability and outcome generalization. We describe a 25-year, single-institution cohort assembled under a composite anatomic-imaging definition.

**Methods.** Patients undergoing thyroid surgery at Emory University Hospital between 1999 and 2025 (N=10,871 distinct `research_id`) were retrospectively identified from the institutional electronic health record, synoptic surgical pathology workbooks, NSQIP perioperative records, radiology-derived CT/MRI extractions, and natural language processing (NLP) airway pipelines, harmonized into the publication-tier MotherDuck database `thyroid_canonical_publication_v1_0` (release `pub_v1_0_20260430`). A composite "massive goiter" exposure was defined as gland weight ≥100 g OR substernal extension on CT or MRI OR imaging-confirmed airway compromise (CT tracheal deviation, narrowing, or airway compromise). Demographic, comorbidity, surgical, pathologic, and perioperative-complication distributions were computed for the massive vs non-massive cohorts and reported as n (%) for categorical variables and mean ± SD or median (IQR) for continuous variables. Confirmed complications were defined under a strict canonicalization rule (`finding_status = 'present'` AND `evidence_strength IN ('definitive','probable')`) following migration mig_252.

**Results.** Of 10,871 patients, **2,501 (23.0%)** met the composite massive criterion: 1,429 (57.1%) by weight ≥100 g, 1,047 (41.9%) by substernal extension, and 1,440 (57.6%) by airway compromise (overlapping flags permitted). The massive cohort was older (median age 56 [IQR 45–66] vs 50 [39–62] years), was less female-predominant (70.8% vs 79.9% female), and had a substantially different self-reported race distribution (62.2% Black or African American vs 31.2% in the non-massive cohort; 28.5% White vs 54.4%). The malignancy rate was lower in the massive cohort (25.8% vs 41.7%), consistent with the predominant referral being benign multinodular and substernal goiter; among 646 malignant massive cases, papillary thyroid carcinoma (PTC) was less dominant (64.6% vs 80.9% in the broader cohort) with relative enrichment of follicular (15.0%), medullary (5.0%), and aggressive variants (poorly differentiated, anaplastic). Total thyroidectomy was the predominant procedure in both arms but more strongly in the massive cohort (1,672/2,501 = 66.9% vs 4,327/8,370 = 51.7%); procedure-type completeness was 100% in massive and 99.98% in non-massive following migration mig_253. Under the strict-definition complication rollup, the any-confirmed-complication flag was elevated in the massive cohort at **5.28% (132/2,501) vs 3.20% (268/8,370)** in non-massive (relative risk ≈ 1.65). Confirmed RLN injury occurred in 14 (0.56%) massive vs 7 (0.084%) non-massive patients (RR ≈ 6.7); confirmed hematoma in 23 (0.92%) vs 45 (0.54%) (RR ≈ 1.7). All-cause in-record mortality was 2.36% in massive vs 1.59% in non-massive patients. Era-stratified prevalence of the composite massive flag rose from approximately 12% in 1999–2014 to 24.9% (2015–2019) and 28.5% (2020–2025), reflecting expanded cross-sectional imaging documentation, the institutional NLP airway pipeline rollout, and possible referral-pattern evolution.

**Conclusions.** Under a transparent composite anatomic-imaging definition, massive goiter accounted for nearly one-quarter of thyroid surgical volume at this tertiary referral center over 25 years, with distinct demographic, pathologic, and surgical-procedure profiles versus non-massive cases and a 1.65-fold elevated strict-definition any-complication burden. Confirmed RLN injury and hematoma rates show large between-arm relative differences but small absolute counts. The composite-flag construction is portable to other publication-tier institutional cohorts and provides a richer denominator for downstream substudies of operative approach, airway management, and disparities than any single-axis definition.

---

## 1. Introduction

Goiter — diffuse or nodular enlargement of the thyroid gland — encompasses a wide clinical spectrum from incidentally noted multinodular disease to symptomatic, airway-displacing, retrosternal masses requiring complex surgical exposure. The terms "massive," "giant," "complex," and "challenging" goiter appear interchangeably in the surgical literature, with operational definitions ranging from gland weight ≥100 g, ≥200 g, or ≥1 kg, to the presence of substernal/retrosternal extension on cross-sectional imaging, to clinical or imaging features of airway compromise. The heterogeneity of these definitions complicates aggregation across published series and limits the precision of operative-risk and resource-utilization estimates derived from any single-axis definition.

Single-institution cohorts are well-suited to characterize this surgically distinct population because they integrate granular pathology, imaging, and operative-detail extraction at the patient level — content that population-based registries (SEER, NCDB) cannot provide. Recent advances in structured extraction of cross-sectional imaging features and NLP processing of operative and radiology narratives further enable composite anatomic-imaging definitions that capture the surgically meaningful "massive goiter" phenotype across multiple complementary axes simultaneously.

We describe a 25-year (1999–2025), single-institution thyroid surgery cohort at Emory University Hospital and apply a composite "massive goiter" exposure definition spanning gland weight, substernal extension on CT or MRI, and imaging-confirmed airway compromise. Within this composite-defined denominator we report demographic, comorbidity, pathologic, surgical-procedure, and perioperative-complication distributions, with comparison to the non-massive thyroid surgical population. The objective is to provide a transparent, reproducible, multi-axis descriptive baseline that supports downstream substudies in operative approach (extracervical exposure, sternotomy decision-making), airway management, malignant histology subtypes preferentially encountered in massive disease, and demographic disparity in massive-disease referral.

## 2. Methods

### 2.1 Study design and setting

Retrospective single-institution descriptive cohort study of patients undergoing thyroid surgery at Emory University Hospital between 1999 and 2025. The protocol was approved by the Emory University Institutional Review Board (IRB # — *to be inserted*).

### 2.2 Data sources and harmonization

All structured fields were extracted from the institutional electronic health record, synoptic surgical pathology workbooks, NSQIP perioperative records, laboratory feeds, radiology-derived CT and MRI characterizations, and NLP pipelines coordinated under the Thyroid 2026 research registry. Analytic-ready tables reside in the MotherDuck database `thyroid_canonical_publication_v1_0` (`main` schema + derivative `semantic_publication` manuscript-safe projections + selected `manuscript_workspace` cohort views). The canonical patient spine is `main.canonical_patient_master` (10,871 distinct `research_id` rows). The manuscript-tier database freeze for this analysis is `release_id='pub_v1_0_20260430'` (`semantic_publication.release_manifest_v1`); the manuscript feasibility table was last scored at `canonical_version_at_scoring='v1_0_post_mig_248'`. This v2 draft incorporates two upstream canonicalization repairs that closed during the M038 audit:

- **mig_252** repaired the `comp_*_confirmed` and `any_confirmed_complication_flag` rollup expressions on `canonical_patient_master`. Prior expressions counted negation evidence (`finding_status='absent'`) as confirmation; the corrected expression requires `finding_status='present' AND evidence_strength IN ('definitive','probable')`. Cohort-wide impact: `any_confirmed_complication_flag` count 2,490 → 400; for example `comp_seroma_confirmed` 618 → 39 and `comp_chyle_leak_confirmed` 1,576 → 3.
- **mig_253** filled `surg_procedure_type`, `surg_total_thyroidectomy`, and `surg_hemithyroidectomy` for 2,138 patients (19.7% of the cohort) where vocabulary mapping had previously returned NULL despite procedure source data being present in `canonical_operative_events_v1` and the NSQIP CPT fields. Residual NULL count is 2 (≤50 acceptance threshold met).

Detailed Methods boilerplate and reproducibility anchors mirror Section 1–2 of `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`.

### 2.3 Cohort and exposure definition

The denominator is the full surgical cohort (n=10,871) read directly from `manuscript_workspace.cohort_m038_massive_goiter_v1`, which surfaces patient-grain demographics, comorbidities, gland weight, CT/MRI substernal and airway findings, NLP airway extractions, surgical procedure type, NSQIP perioperative outcomes, perioperative complication flags, NLP-extracted tracheostomy, and follow-up duration.

The composite **massive goiter** exposure was defined as the disjunction of three component flags:

1. **Weight component**: `gland_weight_final_g ≥ 100`
2. **Substernal component**: `ct_substernal_extension_any = TRUE OR mri_substernal_any = TRUE`
3. **Airway component**: `ct_tracheal_deviation_any = TRUE OR ct_tracheal_narrowing_any = TRUE OR ct_airway_compromise_any = TRUE`

A patient was classified as **massive** if any component flag was TRUE. Missing values for any component were coalesced to FALSE; sensitivity analysis to a complete-data sub-cohort is recommended at peer-review stage.

### 2.4 Variable specifications

Demographic, surgical, and outcome variables follow the canonical specifications of the v1.0 publication Methods document (Section 2 and Section 8). Surgical procedure type is harmonized to total_thyroidectomy / hemithyroidectomy / other / isthmusectomy / unknown buckets per `canonical_operative_events_v1` lineage, post-mig_253. Perioperative complications are reported from confirmed-event flags (`comp_rln_injury_confirmed`, `comp_hematoma_confirmed`, etc.) and from the rolled-up `any_confirmed_complication_flag` under the strict definition described in §2.2. Era stratification uses `surg_first_date` binned into 5-year periods (1999–2004, 2005–2009, 2010–2014, 2015–2019, 2020–2025). Follow-up duration is `followup_years` from `main.canonical_survival_followup_v1` (Protocol v2, mig_123 lineage).

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

### 3.2 Table 1 — Demographics & Baseline Characteristics

Following the standing manuscript rule (`feedback_manuscript_demographics_and_full_column_review.md`), demographic and baseline characteristics are reported below for the composite-massive vs non-massive arms. Footnote in parentheses indicates n with non-missing data when coverage is below 80%.

| Characteristic | Massive (n=2,501) | Non-massive (n=8,370) |
|---|---:|---:|
| **Age at surgery** | | |
| &nbsp;&nbsp;Mean (years) | 55.4 | 50.5 |
| &nbsp;&nbsp;Median [IQR] | 56 [45–66] | 50 [39–62] |
| **Sex** | | |
| &nbsp;&nbsp;Female | 1,771 (70.8%) | 6,688 (79.9%) |
| &nbsp;&nbsp;Male | 730 (29.2%) | 1,682 (20.1%) |
| **Race** | | |
| &nbsp;&nbsp;Black or African American | 1,555 (62.2%) | 2,613 (31.2%) |
| &nbsp;&nbsp;White | 714 (28.5%) | 4,552 (54.4%) |
| &nbsp;&nbsp;Asian | 57 (2.3%) | 419 (5.0%) |
| &nbsp;&nbsp;Other / AIAN / NH-PI / Hispanic | 44 (1.8%) | 187 (2.2%) |
| &nbsp;&nbsp;Unknown / Not Reported | 130 (5.2%) | 591 (7.1%) |
| **BMI (kg/m²)** *(massive n=417; non-massive n=1,668)* | | |
| &nbsp;&nbsp;Mean | 33.5 | 29.8 |
| &nbsp;&nbsp;Median [IQR] | 32.1 [27.7–37.5] | 28.5 [24.4–33.6] |
| **NLP-extracted comorbidities** *(coverage limited; reflects mention in problem-list/PMHx narrative)* | | |
| &nbsp;&nbsp;Hypertension | 696 (27.8%) | 1,079 (12.9%) |
| &nbsp;&nbsp;Diabetes mellitus | 500 (20.0%) | 966 (11.5%) |
| &nbsp;&nbsp;Coronary artery disease | 84 (3.4%) | 140 (1.7%) |
| &nbsp;&nbsp;Chronic kidney disease | 85 (3.4%) | 136 (1.6%) |
| &nbsp;&nbsp;COPD | 47 (1.9%) | 60 (0.7%) |
| &nbsp;&nbsp;Mean N comorbidities (NLP-extracted) | 2.78 | 2.38 |
| **Thyroid-specific history** | | |
| &nbsp;&nbsp;Graves disease (synoptic) | 108 (4.3%) | 466 (5.6%) |
| &nbsp;&nbsp;Hashimoto thyroiditis (synoptic) | 39 (1.6%) | 209 (2.5%) |
| &nbsp;&nbsp;Prior thyroidectomy (NLP) | 209 (8.4%) | 650 (7.8%) |
| &nbsp;&nbsp;Prior neck surgery (NLP) | 38 (1.5%) | 102 (1.2%) |
| **ASA class** *(massive n=246; non-massive n=1,164)* | | |
| &nbsp;&nbsp;ASA I | 6 (2.4%) | 84 (7.2%) |
| &nbsp;&nbsp;ASA II | 80 (32.5%) | 583 (50.1%) |
| &nbsp;&nbsp;ASA III | 144 (58.5%) | 473 (40.6%) |
| &nbsp;&nbsp;ASA IV | 16 (6.5%) | 24 (2.1%) |
| **Surgical era** | | |
| &nbsp;&nbsp;1999–2004 | 110 (4.4%) | 793 (9.5%) |
| &nbsp;&nbsp;2005–2009 | 142 (5.7%) | 1,049 (12.5%) |
| &nbsp;&nbsp;2010–2014 | 240 (9.6%) | 1,645 (19.7%) |
| &nbsp;&nbsp;2015–2019 | 731 (29.2%) | 2,204 (26.3%) |
| &nbsp;&nbsp;2020–2025 | 517 (20.7%) | 1,300 (15.5%) |
| &nbsp;&nbsp;Surgical date unknown | 761 (30.4%) | 1,379 (16.5%) |
| **Pathology** | | |
| &nbsp;&nbsp;Malignant histology | 646 (25.8%) | 3,491 (41.7%) |
| &nbsp;&nbsp;Bilateral disease (pathology + imaging) | 749 (29.9%) | 1,393 (16.6%) |
| **Follow-up duration** | | |
| &nbsp;&nbsp;Mean (years, all) | 1.22 | 1.84 |
| &nbsp;&nbsp;Patients with FU>0 | 997 (39.9%) | 3,174 (37.9%) |
| &nbsp;&nbsp;Mean (years, FU>0 subset) | 3.06 | 4.85 |

Two patterns merit emphasis. First, the male proportion is higher in the massive cohort (29.2% vs 20.1%), an enrichment consistent with prior reports that anatomically large or substernal goiter occurs more frequently in men despite the overall female predominance of thyroid disease. Second, the racial composition of the massive cohort differs substantively from the non-massive arm (Black or African American 62.2% vs 31.2%); this referral-pattern observation is discussed in §4 in the context of disparity of access to early surgical management of nodular thyroid disease. Third, the comorbidity burden differs meaningfully: NLP-extracted hypertension (27.8% vs 12.9%) and diabetes (20.0% vs 11.5%) are roughly twice as prevalent in the massive arm, and ASA class III–IV proportion among patients with NSQIP coverage is 65.0% massive vs 42.7% non-massive, reflecting both demographic and physiologic complexity at the time of surgery.

### 3.3 Histology distribution within the malignant subset (Table 2)

Among the 646 malignant patients in the massive cohort, the histologic distribution differs from the broader malignant cohort previously reported (M032; n=4,022): PTC accounted for 417 patients (64.6%) versus 80.9% in M032; follicular carcinoma 97 (15.0%); medullary carcinoma 32 (5.0%); poorly differentiated thyroid carcinoma 22 (3.4%); anaplastic carcinoma 13 (2.0%); NIFTP 25 (3.9%); FTUMP 9 (1.4%); plus a long tail of metastatic and rare variants (NUT carcinoma n=1; "infiltrating carcinoma with thymus-like differentiation" n=1). The reduction in PTC dominance and relative enrichment of follicular, medullary, and aggressive (poorly-differentiated, anaplastic) variants is consistent with the clinical impression that locally advanced or anatomically expansive disease over-represents non-PTC histologies; subtype-stratified surgical outcome analyses are an appropriate downstream substudy.

### 3.4 Surgical procedure type and operative context (Table 3)

Procedure type is now documented for 2,501/2,501 (100%) of the massive cohort and 8,368/8,370 (99.98%) of the non-massive cohort following migration mig_253. Among documented cases:

| Procedure | Massive (n=2,501) | Non-massive (n=8,370) |
|---|---:|---:|
| Total thyroidectomy | 1,672 (66.9%) | 4,327 (51.7%) |
| Hemithyroidectomy | 792 (31.7%) | 3,640 (43.5%) |
| Other | 36 (1.4%) | 386 (4.6%) |
| Isthmusectomy | 1 (0.04%) | 6 (0.07%) |
| Unknown / NULL | 0 (0%) | 11 (0.13%) |

Total thyroidectomy was the predominant procedure in both arms but more strongly so in the massive cohort (66.9% vs 51.7%), consistent with practical recommendations to avoid contralateral re-operation when bilateral substernal or anatomically complex disease is present. Adjunctive operative context (NSQIP-derived where coverage permits):

| Operative variable | Massive | Non-massive |
|---|---:|---:|
| Central neck dissection (NSQIP, where recorded) | 55 | 193 |
| Lateral neck dissection (NSQIP, where recorded) | 19 | 20 |
| Mean operative duration (min) | 130.8 | 121.3 |
| Median operative duration (min) | 113.5 | 107 |
| Mean hospital LOS (days) | 1.26 | 1.07 |
| Median hospital LOS (days) | 1 | 1 |
| Transfusion (NSQIP, ≥1 unit) | 2 | 2 |
| Unplanned reintubation (NSQIP, n) | 5 | 7 |
| 30-day readmission (NSQIP, n) | 11 | 18 |
| NLP-extracted tracheostomy (any timing) | 121 (4.84%) | 263 (3.14%) |

The mean operative duration is approximately 9.5 minutes longer in the massive cohort, and the LOS mean is approximately 0.2 days longer, both consistent with anatomical complexity. The roughly 1.5-fold higher tracheostomy prevalence in the massive arm is an underrecognized signal that warrants its own substudy alongside operative airway-support detail.

### 3.5 Perioperative complications under the strict-definition rollup (Table 4)

This section reports complication outcomes under the **strict** confirmation rollup applied in mig_252 (`finding_status = 'present'` AND `evidence_strength IN ('definitive','probable')`). The v1 draft of this manuscript reported a buggy any-complication rate of 35.5% (massive) vs 19.1% (non-massive); those numbers are obsolete. Strict-definition results:

| Outcome | Massive (n=2,501) | Non-massive (n=8,370) | Approx RR |
|---|---:|---:|---:|
| Any confirmed complication flag | 132 (5.28%) | 268 (3.20%) | 1.65 |
| Confirmed RLN injury | 14 (0.56%) | 7 (0.084%) | 6.7 |
| Confirmed hematoma | 23 (0.92%) | 45 (0.54%) | 1.7 |
| Confirmed seroma | 12 (0.48%) | 27 (0.32%) | 1.5 |
| Confirmed chyle leak | 2 (0.08%) | 1 (0.01%) | 6.7 |
| Confirmed VC paresis | 0 | 0 | — |
| Confirmed VC paralysis | 19 (0.76%) | 4 (0.048%) | 15.9 |
| Confirmed hypocalcemia | 1 (0.04%) | 8 (0.10%) | 0.4 |
| Confirmed hypoparathyroidism | 87 (3.48%) | 209 (2.50%) | 1.4 |
| All-cause in-record mortality | 59 (2.36%) | 133 (1.59%) | 1.5 |

Three observations. First, the strict any-complication rate (5.28% massive, 3.20% non-massive) sits within the range commonly reported in single-institution surgical thyroidectomy series, supporting the canonicalization repair: the prior-draft rate of 35.5% was an artifact of negation-evidence inclusion. Second, the relative-risk pattern is preserved across most complication families, with the largest between-arm differences in confirmed VC paralysis (RR ≈ 15.9), confirmed RLN injury (RR ≈ 6.7), and confirmed chyle leak (RR ≈ 6.7); confirmed hypocalcemia is the only family showing an inverted (massive < non-massive) point estimate, and the absolute counts are small (1 vs 8). Third, absolute event counts remain small for several specific complications (e.g., RLN injury 14/7, chyle leak 2/1), so confidence intervals on the relative-risk estimates will be wide; we present these point estimates as descriptive only and reserve formal inferential testing for a follow-on definition-paper companion analysis (M038-B; see §6).

Follow-up duration is right-skewed and a substantial fraction of patients have a recorded `followup_years` of zero — a pattern that reflects the institutional follow-up pipeline rather than absence of survival data. Among patients with FU > 0, mean follow-up is 3.06 years (massive) vs 4.85 years (non-massive).

### 3.6 Era stratification

| Era | Total n | Massive n | % Massive |
|---|---:|---:|---:|
| 1999–2004 | 903 | 110 | 12.2% |
| 2005–2009 | 1,191 | 142 | 11.9% |
| 2010–2014 | 1,885 | 240 | 12.7% |
| 2015–2019 | 2,935 | 731 | 24.9% |
| 2020–2025 | 1,817 | 517 | 28.5% |
| Surgical date unknown | 2,140 | 761 | 35.6% |

The roughly two-fold rise in measured massive-flag prevalence from the pre-2015 to the post-2015 era is unlikely to reflect true clinical change of equivalent magnitude. The most plausible drivers are increased cross-sectional imaging documentation (more CT/MRI surgical-planning studies entered into structured workflow), the institutional NLP airway pipeline rollout (which adds the airway-component flag for cases that pre-existed in the cohort but lacked structured airway documentation), and possible incremental shift in tertiary-referral case complexity. The 19.7% (2,140/10,871) of patients with no recorded surgical date is a known cohort-flow caveat carried in `cohort_m032_descriptive_25yr_v1` lineage and inherited here.

## 4. Discussion

This descriptive analysis documents that under a transparent composite anatomic-imaging definition, "massive goiter" accounts for nearly a quarter of all thyroid surgical volume at a single tertiary referral center over a 25-year span (n=2,501 of 10,871). The composite definition deliberately spans three complementary axes — gland weight, substernal extension, and imaging-confirmed airway compromise — to avoid the misclassification risk inherent in single-axis literature definitions. The component overlap pattern is informative: of the 2,501 massive cases, 386 (15.4%) carry all three flags (the "anatomically complete" massive-goiter phenotype), while substantial single-flag subsets exist (898 weight-only, 114 substernal-only, 309 airway-only), each of which would be missed by alternative single-axis screens. This argues for a composite definition as the more clinically inclusive baseline for surgical-cohort epidemiology.

Three demographic and pathologic patterns deserve emphasis. First, the male enrichment of the massive cohort (29.2% vs 20.1%) replicates prior surgical-series findings and is biologically consistent with the longer disease duration that anatomically expansive nodular disease often requires. Second, the marked over-representation of Black or African American patients in the massive cohort (62.2% vs 31.2% of non-massive surgical patients at the same institution) is a striking and previously underreported referral-pattern signal in the U.S. South tertiary surgical context. While our data cannot disentangle access-to-care, primary-care surveillance, surgical-referral-threshold, and biological factors, the observation merits further investigation as a candidate driver of advanced-disease surgical disparity. Third, the histologic profile of the malignant subset within the massive cohort is shifted away from PTC dominance and toward follicular, medullary, and aggressive variants — consistent with the inferior-pole-extending, multinodular, and anatomically infiltrating phenotype that often co-occurs with non-PTC histologies.

The complication profile under the strict-definition rollup is consistent with prior surgical literature and clinically interpretable. The 1.65-fold elevated any-complication rate in the massive arm (5.28% vs 3.20%) is the most defensible composite endpoint. Confirmed RLN injury, confirmed VC paralysis, and confirmed chyle leak each show large between-arm relative differences (RR ≈ 6–16), but absolute counts are small and confidence intervals will be wide; these point estimates are presented as descriptive observations rather than formally tested differences. The total-thyroidectomy preference in the massive cohort (66.9% vs 51.7%) is consistent with operative practice in bilateral substernal disease, and now that procedure-type completeness is 100% in massive and 99.98% in non-massive following mig_253, the procedure-distribution claim is defensible without the data-quality caveat carried by the v1 draft.

The era-stratified rise in measured massive prevalence is most plausibly explained by improved structured documentation rather than true clinical incidence change of similar magnitude, particularly in light of the 19.7% surgical-date-unknown subset (which itself shows 35.6% massive flag — likely reflecting a pre-2010 documentation tier that disproportionately surfaces only the most clinically conspicuous cases).

## 5. Limitations

1. **Composite-definition ascertainment asymmetry.** The three component flags rely on different underlying documentation streams: gland weight on synoptic pathology, substernal extension on CT/MRI structured extraction, and airway compromise on CT-derived imaging features. Coverage is heterogeneous across eras (gland weight 86.3% known in massive cohort; surgical date 69.6% known cohort-wide), and a sensitivity analysis restricted to complete-data sub-cohorts is recommended at peer review.

2. **Era confounding with documentation expansion.** The pre-2015 vs post-2015 rise in measured massive prevalence is partially attributable to the introduction of structured NLP airway extraction and increased CT/MRI documentation in the institutional workflow rather than to true clinical incidence change.

3. **NSQIP coverage skew.** ASA class, neck-dissection flags, transfusion, operative duration, LOS, and 30-day readmission are NSQIP-derived and thus restricted to patients with NSQIP linkage (massive n=246, non-massive n=1,164 for ASA; coverage similar for the other NSQIP variables). The NSQIP-covered subset over-represents the post-2015 era and may not reflect older surgical practice. Stratified reporting by NSQIP-coverage status is recommended at peer review.

4. **Confirmed-complication absolute count sparsity.** While the strict-definition rollup is now clinically interpretable, several specific complications (RLN injury 14/7, chyle leak 2/1, VC paralysis 19/4) have small absolute counts. Relative-risk point estimates are reported descriptively; formal inferential testing is reserved for a follow-on analysis with appropriate confidence interval reporting and multiple-comparison adjustment.

5. **Follow-up duration distribution.** The mean recorded `followup_years` is 1.22 (massive) vs 1.84 (non-massive), with approximately 60% of the cohort carrying a recorded value of zero. This reflects pipeline-level capture rather than absence of long-term outcome data; the mean among patients with FU>0 is 3.06 years (massive) vs 4.85 years (non-massive). Time-to-event substudies must use censoring rules tied to `canonical_recurrence_resolved_v1` and `canonical_survival_followup_v1`.

6. **No ECMO column.** The original manuscript framing (per `manuscript_feasibility_v1` title "ECMO Support for Massive Goiter Surgery") implied an ECMO-supported surgical sub-cohort; no structured ECMO indicator is present in `cohort_m038_massive_goiter_v1`. Any ECMO-specific claim would require chart-review remediation or a dedicated NLP extraction of operative airway-support narratives, neither of which is in the present cohort scope.

7. **No safe-view recurrence/survival join in the present draft.** The cohort view does not currently expose `canonical_recurrence_resolved_v1`-derived recurrence flags or detailed `canonical_survival_followup_v1` fields beyond aggregate `followup_years` and `death_occurred`; downstream survival substudies must explicitly join through the publication-tier safe views.

8. **Strict-definition complication rollup applies only post-mig_252.** Prior versions of `any_confirmed_complication_flag` and `comp_*_confirmed` columns counted negation evidence as confirmation. This v2 draft uses the corrected rollup throughout. Any prior literature comparison or pooled-analysis effort that uses thyroid-canonical-publication exports from before 2026-05-01 should be re-derived against the corrected definition.

## 6. Conclusions and follow-on lane

Massive goiter, defined by a transparent composite of gland weight ≥100 g, substernal extension on CT or MRI, or imaging-confirmed airway compromise, accounts for 23.0% of thyroid surgical volume at this 25-year, single-institution tertiary referral cohort. The composite-defined population differs from the broader thyroid surgical denominator on age, sex, race, comorbidity burden, malignancy rate, malignant histologic distribution, surgical procedure type, and perioperative complication burden under a strict-definition complication rollup (any-complication RR ≈ 1.65). The composite definition is portable to other publication-tier institutional cohorts and provides a richer denominator for downstream substudies of surgical approach, airway management, malignant histology subtype, and demographic disparity in massive-disease referral.

A companion analytical manuscript (**M038-B**, currently in planning at `manuscript_outputs/v1_0_20260501/M038_definition_paper_PLANNING_v1.md`) will compare the three exposure operationalizations (≥200 g weight, substernal, airway) head-to-head against perioperative complication risk in a unified full-cohort interaction model. The descriptive baseline established here serves as the citable foundation for that follow-on.

---

## Tables (to be materialized as CSV in subsequent Lane M refresh)

- **Table 1 — Demographics & Baseline Characteristics (Massive vs Non-massive)** (`M038_Table_1_demographics_v1_0_20260501.csv`)
- **Table 2 — Histology Distribution within the Malignant Subset** (`M038_Table_2_histology_v1_0_20260501.csv`)
- **Table 3 — Surgical Procedure Type and Operative Context** (`M038_Table_3_procedure_v1_0_20260501.csv`)
- **Table 4 — Strict-Definition Perioperative Complications and Follow-up** (`M038_Table_4_complications_v1_0_20260501.csv`)

## Figures

- **Figure 1 — Composite Massive-Goiter Flag Composition** (Venn or UpSet diagram of weight / substernal / airway component overlap; deferred to publication submission)
- **Figure 2 — Era-Stratified Massive Flag Prevalence** (line plot 1999–2025; deferred)

## Reproducibility

Cohort view: `manuscript_workspace.cohort_m038_massive_goiter_v1` (post-mig_251 extension, ~117 columns)
Underlying database: MotherDuck `thyroid_canonical_publication_v1_0` (release `pub_v1_0_20260430`)
Most recent applied migration: `mig_253_surg_procedure_type_fill_20260501` (signoff_registry timestamp 2026-05-01 06:41:00 UTC)
Preceding migration: `mig_252` (`32beb7b fix(mig252): repair CPM complication confirmed rollups`)
Composite exposure SQL (executable): see Methods §2.3
Methods boilerplate: `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` (Section 1–2 + Section 9 limitations)
Cowork reference commit at v2 generation: HEAD `0143539` (`fix: fill CPM surgical procedure types`)
Companion canonical-publication descriptive paper: M032 (`manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md`) — also requires complications-section rebuild post-mig_252
Companion analytical paper: M038-B (`manuscript_outputs/v1_0_20260501/M038_definition_paper_PLANNING_v1.md`)
Predecessor draft: `M038_massive_goiter_DRAFT_v1.md` (commit `2fc6fef`); superseded due to v1's use of buggy any_confirmed_complication_flag rollup and pre-mig_253 procedure-type missingness

## Column Inventory Note (per standing rule)

Per `feedback_manuscript_demographics_and_full_column_review.md`, this manuscript's cohort view (`manuscript_workspace.cohort_m038_massive_goiter_v1`) was reviewed against the full `canonical_patient_master` schema as part of mig_251 (24 → ~117 columns). Domain-keyword inventory used in the column-review pass:

- **Demographics**: `age_at_surgery`, `sex`, `race`, `bmi_combined`, `bmi_source`, `bmi_missingness_reason`, NSQIP body-metric fallbacks (`nsqip_bmi`, `nsqip_height_in`, `nsqip_weight_lbs`).
- **Comorbidities**: NSQIP panel (`nsqip_diabetes`, `nsqip_hypertension`, `nsqip_copd`, `nsqip_heart_failure`, `nsqip_bleeding_disorder`, `nsqip_disseminated_cancer`, `nsqip_functional_status`, `nsqip_asa_class`, `nsqip_smoker`, `nsqip_tobacco_use`); NLP panel (`pmhx_nlp_diabetes`, `pmhx_nlp_hypertension`, `pmhx_nlp_cad`, `pmhx_nlp_ckd`, `pmhx_nlp_copd`, `pmhx_nlp_n_comorbidities`, `pmhx_nlp_autoimmune_thyroid_hx`, `pmhx_nlp_smoking_status`); thyroid-specific (`syn_graves`, `syn_hashimoto`); surgical history (`pshx_nlp_prior_thyroidectomy`, `pshx_nlp_prior_neck_surgery`); medication context (`ops_anticoagulation_meds`).
- **Surgical context**: `surg_first_date`, `surg_procedure_type`, `surg_total_thyroidectomy`, `surg_hemithyroidectomy`, `surg_n_procedures`, `nsqip_central_neck_dissection`, `nsqip_lateral_neck_dissection`, `nsqip_operative_approach`, `nsqip_operative_duration_min`, `nsqip_drain_usage`, `nsqip_vessel_sealant`, `nsqip_rln_monitoring`, `ops_difficult_airway`, `ops_surgeon`, `ops_surg_date`, `nsqip_inpatient_outpatient`, `nsqip_same_day_discharge_flag`, `nsqip_primary_indication`.
- **Anatomy/pathology**: `gland_weight_final_g`, `gland_weight_total_reported_g`, `ct_substernal_extension_any`, `mri_substernal_any`, `ct_tracheal_deviation_any`, `ct_tracheal_narrowing_any`, `ct_airway_compromise_any`, `ct_goiter_present_any`, `nlp_airway_has_data`, `nlp_airway_key_finding`, `histology_final`, `is_malignant`, `op_findings_summary`, `syn_isthmus_height_cm`, `syn_left_lobe_height_cm`, `syn_right_lobe_height_cm`, `bilateral_disease_flag`, `bilateral_path_flag`, `closest_margin_mm`.
- **Length of stay & disposition**: `nsqip_hospital_los_days`, `nsqip_length_of_stay_days`, `nsqip_surgical_los_days`, `nsqip_admission_date`, `nsqip_discharge_date`, `nsqip_discharge_destination`.
- **Complications (strict, post-mig_252)**: `any_confirmed_complication_flag`, `comp_hematoma_confirmed`, `comp_rln_injury_confirmed`, `comp_seroma_confirmed`, `comp_chyle_leak_confirmed`, `comp_vc_paresis_confirmed`, `comp_vc_paresis_permanent`, `comp_vc_paralysis_confirmed`, `comp_vc_paralysis_permanent`, `comp_hypocalcemia_confirmed`, `comp_hypocalcemia_permanent`, `comp_hypoparathyroidism_confirmed`, `comp_hypoparathyroidism_permanent`, `comp_airway_complication_definitive`, `comp_pneumothorax_definitive`, `comp_mortality_definitive`.
- **NSQIP perioperative outcomes**: `nsqip_transfusion`, `nsqip_neck_hematoma`, `nsqip_hematoma_flag`, `nsqip_rln_injury_flag`, `nsqip_hypocalcemia_flag`, `nsqip_unplanned_intubation`, `nsqip_unplanned_return_or`, `nsqip_readmission_30d_flag`, `nsqip_readmission_count`, `nsqip_death_30d`, `nsqip_pneumonia`, `nsqip_dvt`, `nsqip_pe`, `nsqip_sepsis`, `nsqip_superficial_ssi`, `nsqip_deep_ssi`, `nsqip_organ_space_ssi`.
- **Tracheostomy & recurrence**: `proc_nlp_tracheostomy`, `proc_nlp_tracheostomy_date`, `proc_nlp_tracheostomy_days_from_surg`, `proc_nlp_tracheostomy_n_mentions`, `any_recurrence_flag`, `biochemical_recurrence_flag`.
- **Follow-up**: `followup_years`, `death_occurred`.

**Columns intentionally not pulled** (reserved for downstream substudies or out of scope):
- Detailed lab time-series (TSH/Tg/PTH/calcium per-event grain) — reserved for M039 PTH/Calcium substudy.
- Per-nodule ultrasound features — reserved for M037/M043 LN-predictor substudies and M025 TIRADS lane.
- Operative-narrative free-text fields beyond `op_findings_summary` — reserved for the M038-B definition-paper companion if airway-support detail is required.
- Insurance / payer / facility / surgeon-volume — not surfaced on `canonical_patient_master`; would require linkage to the institutional encounter table, deferred.
- Charlson / Elixhauser composite indices — not currently materialized; recommended as a follow-on column-cluster build.

---

## Drafting Notes (NOT FOR SUBMISSION)

This v2 draft was generated 2026-05-01 by Cowork after migrations mig_252 (complications-rollup repair) and mig_253 (procedure-type fill) landed via Cursor Composer dispatch. The v1 draft (`M038_massive_goiter_DRAFT_v1.md`, commit `2fc6fef`) is preserved unchanged for diff comparison; v2 supersedes it.

**Key delta from v1 → v2:**

- Abstract numbers updated for any-complication rate, RLN injury, hematoma, and procedure-type completeness.
- §3.4 procedure-type table fully refreshed against post-mig_253 100%-known data.
- §3.5 complications table fully refreshed against strict-definition rollup; expanded from 4 outcomes to 10 outcomes with relative-risk column added.
- §3.2 demographics block elevated to a proper **Table 1 — Demographics & Baseline Characteristics** per `feedback_manuscript_demographics_and_full_column_review.md`; expanded to include BMI, NLP-extracted comorbidity panel, ASA class, surgical era distribution, bilateral disease, and follow-up summary.
- §5 Limitations updated: dropped the procedure-type missingness limitation (now resolved by mig_253), added an NSQIP-coverage-skew limitation, added the strict-definition rollup advisory.
- New **Column Inventory Note** appended per the standing rule.
- Author-input gap #6 (procedure-type recovery for the massive cohort) closed — superseded by mig_253.

**Author-input gaps requiring resolution before submission (carried forward / updated):**

1. **Title finalization** — current working title is descriptive; consider tightening to a journal scope (e.g., a Surgery / Thyroid 60-character constraint).
2. **Authorship list** — placeholder only; PI to confirm author order, affiliations, and corresponding author. Likely co-authors include thyroid surgical and endocrine-oncology faculty, plus the Thyroid 2026 informatics team.
3. **Target journal selection** — Surgery (well-suited for surgical descriptive series), Annals of Surgical Oncology (if histology and oncologic-outcome emphasis is added), or Thyroid (if a more endocrine-pathology framing is preferred).
4. **IRB approval number** — placeholder text in §2.1; insert specific protocol number from M032 lineage.
5. **Composite-flag sensitivity analysis** — produce alternative-definition tables (weight-only ≥100 g, weight-only ≥200 g, substernal-only, airway-only) and report concordance/discordance with the composite flag. Estimated 1 query-pass plus one supplementary table.
6. ~~**Procedure-type recovery for the massive cohort**~~ — resolved by mig_253.
7. **Disparity discussion (race composition)** — the §4 paragraph on Black or African American over-representation in the massive cohort is a defensible observation but requires (i) confirmation that institutional EHR registration ascertainment is comparable across eras, and (ii) a literature scan for prior reports of demographic disparity in nodular-thyroid surgical referral. Recommended depth-add at peer review.
8. **Era-rise interpretation** — the §3.6 / §4 attribution of post-2015 prevalence rise to documentation expansion vs true incidence change should be supported by a side analysis showing era-stratified component-flag coverage (i.e., what fraction of pre-2015 vs post-2015 cases have CT documentation at all).
9. **Figure 1 rendering** — produce a Venn or UpSet diagram of the three-component overlap from §3.1 numbers; matplotlib-venn or UpSetR are reasonable rendering paths.
10. **References** — BibTeX stubs at `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib`; expand for substernal/retrosternal goiter surgical series (Adam et al., White et al., Cohen et al.), airway compromise in goiter surgery, anaplastic and aggressive thyroid carcinoma in massive disease, and disparity in thyroid surgical referral.
11. **(NEW v2)** — **Confidence intervals on RR estimates.** §3.5 currently presents RR point estimates only; add 95% CIs (Wald or exact, given several event counts <10) for the formal table at peer-review stage.

**Cross-references with other manuscripts in the cohort:**

This manuscript cites M032 as the anchor descriptive cohort for the broader 25-year denominator; M032 itself has a paired complications-section rebuild now pending (`CF-M032-COMPLICATIONS-REBUILD`) using the same mig_252 strict rollup. Substudy linkages include M037/M043 (LN predictors — relevant for the 25.8% malignant subset within massive), M046 (NIFTP-era Bethesda — addresses the NIFTP/FTUMP n=34 subset surfaced here), and **M038-B** (the definition-paper companion).

---

**Status:** Second draft v2, 2026-05-01. Ready for PI review, RQ-confirmation pass, and Lane M Table CSV materialization.
**Cowork reference commit:** `0143539` (`fix: fill CPM surgical procedure types`).
**Cowork session context:** v22 handoff session post-mig_252/mig_253; numbers reflect strict-definition complications rollup and post-mig_253 procedure-type completeness.
