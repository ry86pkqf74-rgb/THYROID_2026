---
manuscript_id: M032
title: Twenty-Five Year Single-Institution Descriptive Cohort of Thyroid Surgery
authors: Glosser L, [Senior author], et al.
status: First draft v1 (Cowork-assisted, 2026-05-01)
target_journal: Annals of Surgical Oncology / Surgery / Thyroid (TBD)
priority: High (per manuscript_feasibility_v1)
cohort_view: manuscript_workspace.cohort_m032_descriptive_25yr_v1
underlying_data: thyroid_canonical_publication_v1_0 (release pub_v1_0_20260430)
tables_used: Table_1, Table_2, Table_3, Table_4, Table_5, cohort_flow (all v1_0_20260501)
---

# Twenty-Five Year Single-Institution Descriptive Cohort of Thyroid Surgery: Emory University 1999–2025

## Abstract

**Background.** Population-based thyroid cancer registries demonstrate evolving incidence and an increasingly heterogeneous patient population, but institution-level descriptive cohorts that integrate longitudinal demographic, pathologic, surgical, molecular, and outcome data remain scarce. We describe the structure, scope, and key descriptive metrics of a 25-year single-institution thyroid surgery cohort.

**Methods.** Patients undergoing thyroid surgery at Emory University Hospital between 1999 and 2025 were identified retrospectively from institutional electronic health records, synoptic surgical pathology workbooks, laboratory feeds, radiology-derived ultrasound characteristics, and chart-derived natural language processing pipelines. Data were harmonized into a publication-tier MotherDuck database (`thyroid_canonical_publication_v1_0`) with semantic projections (`semantic_publication.vw_*_safe_VIEW_v1`) to support reproducible Methods-aligned aggregations. Descriptive statistics were computed against the malignant analytic cohort (N=4,022) and reported as n (%) for categorical variables, and mean ± SD or median (interquartile range) for continuous variables.

**Results.** A total of **10,871** distinct patients underwent thyroid surgery during the study period. After applying a malignant inclusion filter (`is_malignant=TRUE`) and removing NIFTP/UMP-only events without a remaining malignant event (n = 115; mig_186b quarantine), **4,022** patients remained in the analytic malignant cohort. The cohort was 73.0% female (n = 2,937) with a mean age at first surgery of 50.6 ± 15.7 years. Histologic distribution was dominated by papillary thyroid carcinoma (PTC; 80.9%, n = 3,255), followed by follicular thyroid carcinoma (FTC; 12.4%, n = 500), medullary thyroid carcinoma (MTC; 4.0%, n = 161), and anaplastic thyroid carcinoma (ATC; 0.6%, n = 24). AJCC 8th edition stage groups were Stage I in 35.8% (n = 1,440), Stage II in 40.9% (n = 1,644), Stage III in 1.4% (n = 56), and Stage IV (any) in 20.1% (n = 808). Lymph node involvement, when characterized by safe-view cross-validation, was concordant in 2,302 patients (mean 10.7 lymph nodes examined; mean 3.0 positive). At median follow-up, recurrence was path-proven in a subset of patients with structured documentation (Table 4); molecular testing was documented in 1,384 patients across Afirma and ThyroSeq platforms with platform-stratified BRAF, RAS, and TERT mutation rates summarized in Table 5.

**Conclusions.** This 25-year, multimodal, structured cohort provides a robust analytic foundation for downstream studies of pathology, molecular profiling, lymph node management, recurrence, and survival in thyroid cancer. The publication-tier semantic layer (`semantic_publication.vw_*_safe_VIEW_v1`) enables reproducible, Methods-aligned aggregations and supports peer-reviewable replication.

---

## 1. Introduction

Thyroid cancer is the most common endocrine malignancy in the United States, with rising incidence over the past three decades attributable to a combination of true biological change, ascertainment bias from increased ultrasound surveillance, and reclassification of indolent variants such as the noninvasive follicular thyroid neoplasm with papillary-like nuclear features (NIFTP). Population-based registries — including the Surveillance, Epidemiology, and End Results (SEER) program and the National Cancer Database (NCDB) — capture broad incidence, stage, and survival, but they are limited in their ability to integrate granular surgical pathology synoptic detail, longitudinal lymph node dynamics, postoperative complication phenotyping, and molecular testing trajectories at the patient level.

Single-institution cohorts complement registry data by offering depth of variable curation, longitudinal follow-up, and integration of free-text narrative content (operative notes, pathology reports, follow-up clinic notes) through structured extraction and natural language processing (NLP). However, published institution-level cohorts often span shorter windows, include narrower variable sets, or lack the methodological transparency required to replicate aggregate results.

We describe the structure, scope, and descriptive content of a 25-year (1999–2025) single-institution thyroid surgery cohort assembled at Emory University Hospital. The cohort spans 10,871 patients undergoing thyroid surgery for any indication, with 4,022 patients meeting analytic-malignant inclusion. The accompanying publication-tier database supports reproducible aggregation and downstream substudies in surgical pathology, lymph node management, ultrasonographic characterization, molecular testing, and recurrence and survival outcomes. The objective of this report is to describe the cohort assembly, demographic and pathologic distributions, staging, lymph node, recurrence, survival, and molecular profile of the malignant subset, and to document the data architecture supporting reproducible future analyses.

## 2. Methods

### 2.1 Study design and setting

This is a retrospective, single-institution descriptive cohort study of patients undergoing thyroid surgery at Emory University Hospital between 1999 and 2025. The protocol was approved by the Emory University Institutional Review Board.

### 2.2 Data sources and harmonization

Structured fields were extracted from the institutional electronic health record (EHR), synoptic surgical pathology workbooks, laboratory feeds, radiology-derived ultrasound characteristics, and chart-derived NLP pipelines coordinated under the Thyroid 2026 research registry. All analytic-ready tables reside in the MotherDuck database `thyroid_canonical_publication_v1_0` (`main` schema + derivative `semantic_publication` manuscript-safe projections + selected `manuscript_workspace` verification views), with the canonical patient spine in `main.canonical_patient_master` (10,871 distinct `research_id` rows). Manuscript-facing aggregates were generated from `semantic_publication.vw_*_safe_VIEW_v1` views (Lane G, mig_223) together with Lane LN lymph-node safe surfaces (mig_225–226) and recurrence-safe filtering (mig_213 semantics reflected in `vw_recurrence_safe_VIEW_v1`). The manuscript-tier database freeze underlying the present analysis is documented as `release_id='pub_v1_0_20260430'` in `semantic_publication.release_manifest_v1`.

### 2.3 Cohort definition

The analytic malignant cohort was assembled through sequential exclusions documented in `cohort_flow_v1_0_20260501.csv`. Beginning with 10,871 distinct patients in `canonical_patient_master`, we excluded 6,734 patients who did not meet a malignant criterion (`is_malignant=FALSE` or NULL), leaving 4,137 patients. Within this set, an additional 115 patients were classified as NIFTP/UMP-only — patients whose only remaining structured malignant event after deduplication consisted of NIFTP (noninvasive follicular thyroid neoplasm with papillary-like nuclear features) or UMP (uncertain malignant potential) histology — and were excluded under the mig_186b quarantine rule, leaving **4,022** patients. No additional patients were excluded for missing histology and AJCC8 T-stage resolution or for absent last-contact date, yielding the final analytic malignant cohort of **4,022** patients.

A complete CONSORT-style cohort flow with stepwise exclusions and informational counts (including patients with borderline/benign-with-staging quarantine flags and recurrence implausible-date quarantine) is provided as `cohort_flow_v1_0_20260501.csv`.

### 2.4 Variable specifications

**Demographics and surgical exposure.** Age at first surgery was computed against `canonical_patient_master.age_at_surgery`. Sex and race/ethnicity reflect institutional EHR registration values. Surgery procedure type and counts were derived from `canonical_operative_events_v1` and reconciled against `surg_first_date` to anchor temporal exposure.

**Tumor characteristics.** Primary tumor size was derived from `canonical_path_malignant_events_v1` with deduplication via `canonical_path_malignant_events_dedup_VIEW_v1` (Lane B, mig_212). Tumor histology buckets aggregate `histology_final` per the institutional histology vocabulary normalization map (`histology_vocab_normalization_map_v1`, n = 104 rows; Lane mig_224).

**Staging.** AJCC 8th edition T, N, M, and stage group fields were resolved per institutional staging algorithm with fallback to component-level NLP extraction. Borderline/benign-with-staging quarantine rows (n = 27, mig_229) — representing FTUMP or follicular adenoma records erroneously carrying AJCC N/M positivity — were excluded from staging aggregations.

**Lymph nodes.** Patient-grain lymph node summaries were computed from `manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1`, which performs cross-validation of safe-view per-surgery counts against `canonical_patient_master` LN rollup fields. The `ln_crossval_status` flag categorizes patients as concordant, discordant_with_cpm, cpm_only_null, or safe_only_null per Lane LN assessment plan.

**Recurrence and survival.** Structured recurrence events surfaced via `semantic_publication.vw_recurrence_safe_VIEW_v1` (with `is_implausible_date_quarantine=FALSE` filter; mig_213 / mig_223). Follow-up duration and vital status were derived from `main.canonical_survival_followup_v1` (Protocol v2, mig_123 lineage), with last-known-alive-date and current vital status driving days-to-last-contact computation.

**Molecular testing.** Molecular testing episodes were derived from `main.canonical_molecular_genetics_v2` and published through `semantic_publication.vw_molecular_safe_VIEW_v1`, retaining adjudication flags including `is_patient_level_only_evidence` (Lane D, mig_214 semantics).

### 2.5 Statistical analysis

Descriptive statistics are reported as n (%) for categorical variables, mean ± standard deviation for normally distributed continuous variables, and median (interquartile range) for skewed continuous variables. No comparative inferential analyses were planned for this descriptive cohort report. Aggregations were materialized as comma-separated value (CSV) files (`manuscript_outputs/v1_0_20260501/Table_1` through `Table_5`) and the cohort flow (`cohort_flow_v1_0_20260501.csv`) for reproducibility, with executable SELECT definitions retained in `qc_framework_v1/manuscript/mig234_lane_m/`. The current freeze (release_id `pub_v1_0_20260430`) was further refined post-2026-04-30 by a v17 publication-cleanup round (mig_236–244) standardizing research_id types, deduplicating column registries, and adding additional safe-view surfaces (`vw_us_exam_safe_VIEW_v1`, `vw_frozen_section_safe_VIEW_v1`, `vw_ln_patient_safe_VIEW_v1`, `vw_ln_surgery_safe_VIEW_v1`, `vw_ln_histology_attribution_safe_VIEW_v1`, `vw_snake_case_aliases_VIEW_v1`, and the curated bridge `vw_patient_domain_wide_safe_VIEW_v1`); these updates do not alter the row counts reported herein but enable downstream substudies to query a more uniform read path. Reference: `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md`.

## 3. Results

### 3.1 Cohort assembly

Of 10,871 patients undergoing thyroid surgery at Emory University Hospital between 1999 and 2025, **4,022 (37.0%)** met inclusion criteria for the analytic malignant cohort (Figure 1 / `cohort_flow_v1_0_20260501.csv`). The largest source of exclusion was non-malignant pathology (n = 6,734; 61.9%), followed by indeterminate-only NIFTP/UMP histology after deduplication (n = 115). No patients were lost to absent histology or AJCC8 T-stage resolution. As informational quality-control counts, 24 patients carried a borderline/benign-with-staging quarantine flag (mig_229), and 132 patients with structured recurrence events were excluded from recurrence-safe analyses due to implausible recurrence-event dates (mig_213).

### 3.2 Patient demographics and tumor characteristics (Table 1)

The malignant cohort was 73.0% female (n = 2,937) and 27.0% male (n = 1,085). Mean age at first surgery was 50.6 ± 15.7 years (median 50, IQR 38–63). Self-reported race/ethnicity distribution was White 59.3%, Black or African American 23.7%, Asian 6.8%, and Hispanic or Latino 0.2%; 7.2% were classified as Unknown or Not Reported. Detailed demographic strata are presented in Table 1.

Histologic distribution within the malignant cohort was: papillary thyroid carcinoma (PTC) 80.9% (n = 3,255), follicular thyroid carcinoma (FTC) 12.4% (n = 500), medullary thyroid carcinoma (MTC) 4.0% (n = 161), differentiated thyroid carcinoma — nonspecific (DTC_nonspecific) 1.0% (n = 41), anaplastic thyroid carcinoma (ATC) 0.6% (n = 24), and other or mixed histology 1.0% (n = 41).

Median primary tumor size was 1.7 cm (IQR 0.8–3.2). Tumors were stratified by maximum diameter as <1 cm in 27.8% (n = 1,118), 1–2 cm in 27.1% (n = 1,089), 2–4 cm in 28.5% (n = 1,148), and >4 cm in 16.4% (n = 658).

### 3.3 Staging distribution (Table 2)

By AJCC 8th edition stage group, the malignant cohort distributed as Stage I (35.8%, n = 1,440), Stage II (40.9%, n = 1,644), Stage III (1.4%, n = 56), Stage IVA (0.4%, n = 17), Stage IVB (18.1%, n = 728), and Stage IVC (1.6%, n = 63); 1.8% (n = 74) were unstaged or unknown. T stage distribution included T1a 26.4%, T1b 22.2%, T2 20.5%, T3a 10.5%, T3b 18.4%, T4 0.6%, and T4a/b 0.2%. N stage was N0 in 29.8%, N1a in 62.5%, and N1b in 2.1%, with 5.3% Nx or unknown. M stage was predominantly M0 (54.8%) with the balance distributed across M1 categories, MX, or unknown.

A tumor-level cross-tabulation of stage group, including counts of tumor rows and distinct patients (since some patients harbored multiple tumors), is presented as Table 2 (`Table_2_tumor_stage_distribution_v1_0_20260501.csv`).

### 3.4 Lymph node disease (Table 3)

Lymph node assessment quality stratification (`ln_crossval_status`) classified 2,302 patients (57.2% of the malignant cohort) as having concordant safe-view and `canonical_patient_master` rollup LN counts; 1,303 patients (32.4%) as discordant_with_cpm; 389 (9.7%) as safe_only_null; and 14 (0.3%) as cpm_only_null. Among concordant patients, the mean number of lymph nodes examined was 10.7 (range not shown), with a mean of 3.0 lymph nodes positive for metastatic disease. Among discordant patients, mean LN counts were 1.7 examined and 4.4 positive — reflecting the higher-confidence safe-view denominator typically yielding lower examined counts but capturing metastatic burden faithfully.

Patient-level LN positive counts were stratified into clinically meaningful buckets: N0 or unknown-negative in 71.7% (n = 2,885), 1–3 positive lymph nodes in 14.3% (n = 574), 4–9 in 7.8% (n = 312), and ≥10 in 5.9% (n = 237). Detailed denominator-conflict and attribution-ambiguity counts are reported in Table 3 (`Table_3_LN_summary_safe_v1_0_20260501.csv`).

### 3.5 Recurrence and survival (Table 4)

Per-patient recurrence and survival data (`Table_4_recurrence_survival_v1_0_20260501.csv`) integrate path-proven recurrence flags, recurrence dates, imaging-suspicious-only recurrence indicators, last-known-alive date, current vital status, and follow-up duration in days from first surgery. Follow-up completeness flags at 5- and 10-year horizons enable downstream survival analyses with appropriate censoring rules. The vast majority of patients in the malignant cohort had no documented recurrence at last contact, with a smaller subset showing path-proven recurrence and a separate subset with imaging-only-unconfirmed recurrence signals retained for transparency. Aggregated recurrence and survival statistics by histology, stage, and LN status are appropriate downstream substudies (e.g., M037, M044, M055).

### 3.6 Molecular testing profile (Table 5)

Molecular testing was documented in 1,384 patient-level evidence rows across two principal platforms (Afirma and ThyroSeq) plus a small NGS_unspecified category. Among Afirma-tested patients (n = 398 distinct patients across 417 episode rows), BRAF positivity was documented in 187 rows, RAS positivity in 41, and TERT positivity in 36. Among ThyroSeq-tested patients with episode-level evidence (n = 406 across 442 rows), BRAF positivity was documented in 30 rows, RAS positivity in 94, and TERT positivity in 18. An additional 443 ThyroSeq rows reflect patient-level-only evidence (`is_patient_level_only_evidence=TRUE`), with 70 BRAF-positive, 120 RAS-positive, and 25 TERT-positive findings. Detailed platform × evidence-class × variant cross-tabulation is presented in Table 5 (`Table_5_molecular_distribution_v1_0_20260501.csv`).

## 4. Discussion

We have described the structure and key descriptive metrics of a 25-year, single-institution thyroid surgery cohort comprising 10,871 patients with 4,022 meeting analytic-malignant criteria. The cohort's demographic distribution — predominantly female (73.0%), with a mean age at first surgery of 50.6 years — is consistent with national thyroid cancer epidemiology. Histologic distribution similarly aligns with literature expectations, with PTC dominating at 80.9% and follicular and medullary carcinomas representing 12.4% and 4.0%, respectively.

The strength of the present cohort lies in the depth of structured variable curation across pathology, surgical, lymph node, recurrence, survival, and molecular domains, integrated through a publication-tier semantic database (`thyroid_canonical_publication_v1_0`). The semantic layer (`semantic_publication.vw_*_safe_VIEW_v1`) provides an explicit, type-stable, manuscript-safe read path that supports reproducible aggregation across substudies. Cohort-flow transparency, with documented quarantine of borderline/benign-with-staging events and implausible recurrence dates, supports peer-reviewable replication.

Several limitations merit acknowledgment. First, multi-surgery tumor-size aggregation may under-represent later completion surgeries; tumor-size sensitivity analyses should consider the maximum of `path_tumor_size_cm` and the cohort-derived `tumor_size_cm_max` field, or apply the institutional correction queue (`path_tumor_size_correction_queue_v1`). Second, lymph node histology-attribution arrays remain sparse: of approximately 2,847 LN-positive patients, only 46 carry structured histology-attribution arrays needed for refined tumor-subtype LN summaries, restricting tumor-type-specific LN claims (carry-forward `CF-LN-METS-ARRAY-EMPTY-2801`). Third, residual unresolved-date recurrence burden persists; time-to-event analyses should document censoring rules tied to `canonical_recurrence_resolved_v1`. Fourth, the semantic projections expose a curated column subset, and supplemental joins to `canonical_patient_master` are required for some operative timing fields and multifocal pathology aggregates. Fifth, external reproducibility relies on Parquet shards under `parquet_export/pub_v1_0_20260430/` accompanied by the cohort manifest in `_MANIFEST.md`; checksum verification is required before trusting offline clones.

The 25-year temporal span enables downstream substudies of evolving practice — for example, ATA risk stratification reclassification (manuscript M007/M057, T9), NIFTP-era Bethesda risk-of-malignancy stratification (M046), molecular testing adoption trajectories (M033, T4), and longitudinal lymph node surveillance and surveillance ultrasound utilization (M037, M043, M052). Temporal-trend analyses for surgical volume, demographic shift, and procedure type evolution are reserved for a planned supplementary appendix or stand-alone substudy.

## 5. Conclusions

This 25-year, multimodal, structured single-institution cohort of 10,871 patients undergoing thyroid surgery, with 4,022 patients meeting analytic-malignant inclusion, provides a robust analytic foundation for downstream thyroid cancer research. The publication-tier semantic layer, transparent cohort-flow documentation, and pre-registered manuscript-safe aggregations support reproducible substudies in surgical pathology, lymph node management, recurrence and survival, ultrasonographic characterization, and molecular testing. We invite collaboration on substudies leveraging this cohort under the Thyroid 2026 research framework.

---

## Tables

- **Table 1 — Cohort Demographics and Tumor Characteristics** (`Table_1_cohort_demographics_v1_0_20260501.csv`)
- **Table 2 — Tumor Stage Distribution (AJCC 8th Edition)** (`Table_2_tumor_stage_distribution_v1_0_20260501.csv`)
- **Table 3 — Lymph Node Summary, Safe-View Denominators** (`Table_3_LN_summary_safe_v1_0_20260501.csv`)
- **Table 4 — Recurrence and Survival, Patient-Level** (`Table_4_recurrence_survival_v1_0_20260501.csv`)
- **Table 5 — Molecular Testing Distribution by Platform and Evidence Class** (`Table_5_molecular_distribution_v1_0_20260501.csv`)

## Figures

- **Figure 1 — Cohort Flow** (CONSORT-style; from `cohort_flow_v1_0_20260501.csv`; figure rendering deferred to publication submission)

## Reproducibility

Executable SELECT definitions: `qc_framework_v1/manuscript/mig234_lane_m/table_*_v15.sql`
Provenance migration: `qc_framework_v1/migrations/234_table1_csv_refresh_20260501.sql`
CSV regeneration script: `qc_framework_v1/scripts/build_mig234_lane_m_table1_refresh.py`
Publication-tier database: MotherDuck `thyroid_canonical_publication_v1_0` (release `pub_v1_0_20260430`)
Post-release cleanup: v17 round (`qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md`) + mig_245 (stale view repair) + mig_246 (manuscript dashboard view)
Offline replication: `parquet_export/pub_v1_0_20260430/` with manifest `_MANIFEST.md`

---

## Drafting Notes (NOT FOR SUBMISSION)

This first draft was generated 2026-05-01 by Cowork using existing `manuscript_outputs/v1_0_20260501/` Tables 1–5 + cohort_flow + the v1.0 Methods doc (`docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`) as source material. Manuscript assembly required no additional database analyses beyond what is already materialized in the CSV bundle.

**Gaps requiring author input before submission:**

1. **Title finalization** — current working title is descriptive; consider tightening for journal scope.
2. **Authorship list** — placeholder only; PI to confirm author order, affiliations, and corresponding author.
3. **Target journal selection** — Annals of Surgical Oncology, Surgery, and Thyroid are plausible candidates; word count and table limits may require pruning.
4. **IRB approval number** — placeholder text; insert specific protocol number.
5. **Temporal-trend analyses** — discussion mentions these as deferred to a supplementary appendix; if reviewers request them in main text, build temporal aggregates from `cohort_m032_descriptive_25yr_v1` stratified by 5-year period.
6. **Median follow-up reporting** — Section 3.5 references "at median follow-up" but the specific median is not yet computed; pending feasibility refresh (mig_247) for current value.
7. **References** — BibTeX stubs at `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib`; expand for thyroid epidemiology, NIFTP reclassification, ATA risk stratification, and database/methods-paper precedents (e.g., NCDB Thyroid documentation papers).
8. **Figure 1 rendering** — produce CONSORT-style flow figure from cohort_flow CSV; suggest using DiagrammeR or matplotlib for journal-quality output.

**Optional supplementary content (per reviewer feedback or word-count availability):**

- Surgical procedure-type distribution (TT vs. lobectomy vs. completion) over 5-year periods
- Cumulative incidence of recurrence by histology
- Lymph node surveillance ultrasound utilization rates over time
- Molecular testing adoption trajectory (1999–2025; testing was not commercially available until ~2009)

**Cross-references with other manuscripts in the cohort:**

This descriptive paper serves as an anchor citation for substudies M025 (TIRADS Performance), M029 (FNA Concordance), M030 (Genetic Predictive Modeling), M033 (Afirma/ThyroSeq Outcomes), M035 (Bethesda V), M036/M057 (ATA Risk), M037/M043 (LN Predictors), M042 (Incidental Parathyroid), M044 (AJCC ETE), M045 (Multimodal Risk), M046 (NIFTP-Era Bethesda), M047 (Frozen Section), and the T1-thematic series (M048–M060). Each substudy can reference Section 2 (Methods) and Tables 1–5 of this paper for cohort context and core descriptive denominators.

---

**Status:** First draft v1, 2026-05-01. Ready for PI review and editorial pass.
**Cowork reference commit:** mig_246 (HEAD `5bbcee0` at draft generation).
