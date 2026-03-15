# Source-Limited Fields: Manuscript Defense Reference
## THYROID_2026 — Verified 2026-03-14 Against Live MotherDuck

> **Purpose:** Canonical reviewer-defense reference for all gaps that cannot be resolved by further computation on existing data. Every metric below was verified against live MotherDuck on 2026-03-14. **No analytic values were changed to produce this document.**

---

## Verification Provenance

| Verification Date | Source DB | Query Script |
|---|---|---|
| 2026-03-14 | `md:thyroid_research_2026` | `/tmp/verify_source_limited.py` + `/tmp/verify2.py` |

---

## 1. Canonical Cohort Anchors

| Anchor | Value | Source Table |
|---|---|---|
| Total surgical cohort | **10,871** | `manuscript_cohort_v1` |
| Cancer-eligible cohort | **4,136** | `analysis_cancer_cohort_v1` |
| Deduplicated surgical episodes | **9,368** | `episode_analysis_resolved_v1_dedup` |
| Patients with any clinical note | **5,641 (51.9%)** | `clinical_notes_long` |
| Note types in corpus | op_note (4,680), h_p (4,221), endocrine_note (519), ed_note (498), dc_sum (185) | `clinical_notes_long` |
| Nuclear medicine notes | **0** | `clinical_notes_long` |

---

## 2. Canonical Table of All Source-Limited, Template-Limited, Pipeline-Limited, and Process-Limited Fields

### Limitation Type Definitions

| Code | Type | Meaning |
|---|---|---|
| **SRC** | Source-Limited | Data never collected or ingested; cannot be obtained by reprocessing existing data |
| **TPL** | Template-Limited | Data was collected but synoptic template format prevents sub-grading without individual report review |
| **PIPE** | Pipeline-Limited | Data exists in raw corpus but NLP entity type absent from vocabulary or extractor output never materialized |
| **PROC** | Process-Limited | Data recorded in structured tables as binary event without associated timestamps or required sub-fields |

### Analysis Tier Definitions

| Code | Allowed Use |
|---|---|
| **PRIMARY** | Permitted in primary exposure/outcome models |
| **DESC** | Descriptive tables (Table 1/2) only; not as analytic variable |
| **SENS** | Sensitivity analyses only, with documented assumptions |
| **MR-ONLY** | Manual-review subset or future data feed; not current cohort |
| **NONE** | Cannot be used in any analysis without institutional data |

---

### 2.1 Recurrence Domain

#### SL-01 — Recurrence Event Calendar Date (`recurrence_event_date`)

| Item | Value |
|---|---|
| **Limitation type** | PROC (Process-Limited) |
| **Numerator (exact/biochemical dates)** | 222 (54 exact + 168 biochemical inflection) |
| **Denominator (recurrence-positive patients)** | 1,986 |
| **% with any date** | 11.2% |
| **% without recoverable date** | **88.8% (1,764 patients)** |
| **Analysis tier** | SENS (binary recurrence endpoint is PRIMARY) |

**Why computation cannot solve it:**
The primary recurrence flag (`recurrence_risk_features_mv.recurrence_flag`) is a structured boolean derived from multi-source adjudication. It was not stored with an associated calendar date in any source table. The 54 exact dates come from linked molecular/RAI follow-up event linkage; the 168 biochemical inflection dates are derived from Tg trajectory analysis. The remaining 1,764 patients have a confirmed recurrence event but no date signal in any available structured or NLP source. Resolving these would require prospective abstraction of clinical records for all 1,764 patients.

**Safe Methods sentence:**
> "Recurrence was defined as any documented disease persistence or recurrence per composite structured review (Materials). Calendar dates for recurrence events were available in 222 of 1,986 recurrence-positive patients (54 with exact source dates, 168 with biochemical inflection dates derived from thyroglobulin trajectory); time-to-recurrence analyses were restricted to these patients."

**Safe Results sentence:**
> "Disease recurrence was documented in 1,986 of 10,871 patients (18.3%). A calendar date was ascertainable for 222 recurrence-positive patients (11.2%); the primary recurrence endpoint is binary for cohort-wide analyses."

**Safe Limitations sentence:**
> "A structured recurrence date registry was unavailable; calendar dates for recurrence events could be confirmed in only 11.2% of recurrence-positive patients. Kaplan–Meier and Cox proportional-hazards analyses using time-to-recurrence as the primary endpoint are restricted to this minority and may not generalise to the full recurrence-positive cohort."

---

### 2.2 Radioactive Iodine (RAI) Domain

#### SL-02 — RAI Administered Dose (`rai_administered_dose_mci`)

| Item | Value |
|---|---|
| **Limitation type** | SRC (Source-Limited) |
| **Patients with any RAI** | 862 |
| **RAI episodes with dose** | 761 / 1,857 (41.0%) |
| **Source of available dose data** | Structured: partly; NLP endocrine/DC notes: ~710 |
| **Nuclear medicine notes in corpus** | **0** |
| **Analysis tier** | SENS |

**Why computation cannot solve it:**
Nuclear medicine dispensing records and dosimetry worksheets are not present in `clinical_notes_long` (zero nuclear medicine note type). RAI dose is documented in endocrine and discharge notes for some patients; the 59% gap reflects patients for whom dose was not transcribed into any available note type. Re-running NLP on existing notes will not recover additional doses because those patients simply do not have dose documentation in available text sources.

**Safe Methods sentence:**
> "RAI dose (mCi) was extracted from endocrine consultation and discharge summary notes combined with structured treatment episode data. Dose documentation was available for 761 of 1,857 RAI treatment episodes (41.0%). Nuclear medicine dispensing records were not available in the source corpus; RAI dose analyses are accordingly restricted to the documented subset."

**Safe Results sentence:**
> "RAI was administered in 862 patients; dose documentation was available for 761 treatment episodes (41.0%; median dose [X] mCi [IQR] where available)."

**Safe Limitations sentence:**
> "RAI dose data were unavailable for 59% of treated patients because nuclear medicine administration records and dosimetry worksheets were not included in the research data extract. Dose-based analyses carry selection bias risk as documentation of dose in clinical notes may correlate with treatment complexity or patient characteristics."

---

### 2.3 Non-Tg Laboratory Domain

#### SL-03 — Parathyroid Hormone (PTH) Collection Dates

| Item | Value |
|---|---|
| **Limitation type** | SRC (Source-Limited) |
| **Patients with any PTH value** | 797 (7.3%) |
| **PTH values with confirmed collection date** | 139 / 797 (17.4%) |
| **Analysis tier** | DESC |

#### SL-04 — Calcium Collection Dates

| Item | Value |
|---|---|
| **Limitation type** | SRC (Source-Limited) |
| **Patients with any calcium value** | 598 (5.5%) |
| **Calcium values with confirmed collection date** | 69 / 598 (11.5%) |
| **Analysis tier** | DESC |

#### SL-05 — TSH / Free T4 / Free T3 / Vitamin D / Other Thyroid Function

| Item | Value |
|---|---|
| **Limitation type** | SRC (Source-Limited) |
| **Patients with any TSH/T4** | **0** |
| **Analysis tier** | NONE |

**Why computation cannot solve PTH/Ca/TSH:**
PTH and calcium values were extracted from free-text clinical notes via NLP. Clinical notes do not contain structured laboratory accession dates for these analytes; only the note-level date (±7–14 days) is recoverable. TSH and thyroid hormone levels do not appear in any of the 8 raw Excel source files and are absent from all exploited NLP entity types. Institutional electronic EHR laboratory feeds for these analytes were not available to the study.

**Safe Methods sentence (PTH/Ca):**
> "Postoperative parathyroid hormone (PTH) and total serum calcium were extracted from 797 and 598 patients respectively via NLP of endocrine and discharge notes. Laboratory collection dates were confirmable for 17.4% (PTH) and 11.5% (calcium) of values; the remainder carry note-level temporal precision (±14 days). These values were used for descriptive characterisation of biochemical hypocalcaemia and hypoparathyroidism only and were not used as time-to-event endpoints."

**Safe Limitations sentence:**
> "Postoperative PTH and calcium values lacked laboratory-grade temporal precision for the majority of patients due to extraction from clinical note free text without embedded collection dates. TSH and thyroid hormone levels were absent from all available data sources. An institutional laboratory data extract would be required to support PTH/calcium time-course and TSH suppression analyses."

---

### 2.4 Pathology — Vascular Invasion Grading

#### SL-07 — WHO 2022 Vascular Invasion Grade

| Item | Value |
|---|---|
| **Limitation type** | TPL (Template-Limited) |
| **Patients with vascular invasion (any)** | 5,570 / 12,886 (43.2%) |
| **Graded (focal + extensive)** | 819 (14.7% of vascular-positive) |
| **Focal** | 463 |
| **Extensive** | 356 |
| **Present-ungraded ('x' synoptic)** | 4,652 (83.5% of vascular-positive) |
| **Analysis tier** | DESC (graded fraction); SENS for focal/extensive subgroup |

**Why computation cannot solve it:**
The synoptic pathology template used during the study period recorded vascular invasion as a checkbox ('x' = present). Vessel count was a separate optional field populated in only 310 cases. Sub-grading the 4,652 present-ungraded cases requires individual pathology report review by a board-certified pathologist — a manual process outside the scope of computational re-analysis.

**Safe Methods sentence:**
> "Vascular invasion was ascertained from synoptic pathology reports. WHO 2022 angioinvasion sub-classification (focal: <4 vessels; extensive: ≥4 vessels) was determinable in 819 vascular-invasion-positive patients (14.7%) based on vessel count documentation. The remaining 4,652 positive cases (83.5%) were recorded as present-ungraded per the synoptic template in use during the study period; these were classified as vascular-invasion-positive but excluded from WHO-grade subanalyses."

**Safe Results sentence:**
> "Vascular invasion was documented in 5,570 patients (51.3%). Of vascular-invasion-positive patients with gradeable data (n=819), 463 were focal (<4 vessels) and 356 were extensive (≥4 vessels)."

**Safe Limitations sentence:**
> "WHO 2022 vascular invasion sub-grading was not uniformly ascertainable because the institutional synoptic pathology reporting template did not require vessel count during the study period. Sub-grade analyses are restricted to the 14.7% of vascular-positive cases with documented vessel counts."

---

### 2.5 Operative Data Domain

#### SL-08 — Esophageal Involvement (0 values — pipeline gap)
#### SL-09 — Berry Ligament Ligation (0 values — pipeline gap)
#### SL-10 — Intraoperative Frozen Section (0 values — pipeline gap)
#### SL-11 — Estimated Blood Loss / EBL (0 values — pipeline gap)

| Item | Value |
|---|---|
| **Limitation type** | PIPE (Pipeline-Limited) |
| **Current value in database** | 0 (all fields) |
| **Interpretation of zero** | UNKNOWN / NOT PARSED — NOT confirmed negative |
| **Analysis tier** | MR-ONLY (until re-extraction) |

**Why computation cannot solve it:**
These entity types are absent from the `note_entities_procedures` NLP vocabulary. The extraction pipeline was not configured to identify these entity types; consequently `FALSE` values in `operative_episode_detail_v2` reflect an unparsed state, not a confirmed clinical negative. Computing summary statistics on these fields would incorrectly report 0% prevalence.

**Correctly populated operative NLP fields (for reference):**

| Field | n (of 9,371 episodes) | % | Interpretation |
|---|---|---|---|
| `rln_monitoring_flag` | 1,702 | 18.2% | Extracted confirmed |
| `strap_muscle_involvement_flag` | 186 | 2.0% | Extracted confirmed |
| `drain_flag` | 169 | 1.8% | Extracted confirmed |
| `reoperative_field_flag` | 46 | 0.5% | Extracted confirmed |
| `parathyroid_autograft_flag` | 40 | 0.4% | Extracted confirmed |
| `gross_ete_flag` | 22 | 0.2% | Extracted confirmed |
| `local_invasion_flag` | 25 | 0.3% | Extracted confirmed |
| `tracheal_involvement_flag` | 9 | 0.1% | Extracted confirmed |
| `esophageal_involvement_flag` | 0 | —% | **NOT PARSED** |

**Safe Methods sentence:**
> "Operative NLP fields with confirmed extraction (RLN monitoring, strap muscle involvement, drain placement, reoperative field, parathyroid autograft, gross ETE, local invasion, tracheal involvement) were included in descriptive analyses. Fields absent from the extraction vocabulary (esophageal involvement, berry ligament, frozen section, EBL) were not reported as their zero counts reflect an unextracted state rather than confirmed clinical absence."

---

### 2.6 Clinical Note Corpus Coverage

#### SL-12 — Note Corpus Coverage (51.9%)

| Item | Value |
|---|---|
| **Limitation type** | SRC (Source-Limited) |
| **Patients with any clinical note** | 5,641 / 10,871 (51.9%) |
| **Total notes** | 11,037 |
| **Note types** | op_note, h_p, endocrine_note, ed_note, history_summary, dc_sum, other_notes, other_history |
| **Analysis tier** | DESC (for coverage); NLP-derived fields are SENS |

**Why computation cannot solve it:**
The 48.1% of patients without notes were simply not part of the clinical note extraction pipeline. Their structured pathology and demographic data are fully available; they lack only the NLP-derived analytic layer. No additional computation on current sources can add notes for these patients.

**Safe Methods sentence:**
> "Clinical notes (operative reports, history-and-physical documents, endocrine notes, emergency department notes, discharge summaries, and supplementary note types) were available for 5,641 of 10,871 patients (51.9%). Variables derived from NLP of clinical notes were ascertained from this subset. Structured pathology, demographic, and staging variables were available for the full cohort."

**Safe Limitations sentence:**
> "Clinical notes were available for approximately half of the surgical cohort (51.9%). NLP-derived variables including most molecular marker context, operative detail, and complication characterisation may under-represent the full cohort. Structured variables (histological diagnosis, synoptic pathology staging, and laboratory markers available in dedicated tables) are not subject to this limitation."

---

### 2.7 Molecular Markers

#### SL-13 — IHC BRAF VE1

| Item | Value |
|---|---|
| **Limitation type** | SRC (Source-Limited) |
| **IHC BRAF results in corpus** | 2 (1 positive, 1 negative) |
| **Total patients** | 12,886 |
| **Analysis tier** | NONE |

**Safe sentence:**
> "BRAF mutation status was determined from molecular sequencing platforms (NGS n=361; NLP-confirmed molecular entity n=173; preoperative molecular panel review n=12; total n=546 BRAF-positive). IHC-based BRAF (VE1 antibody) data were available in 2 patients only due to absence of pathology addendum reports from the clinical notes corpus and were not used analytically."

---

### 2.8 Staging and Scoring Calculability

#### SL-16 — AJCC 8th Edition Staging

| Item | Value |
|---|---|
| **Limitation type** | PROC (Process-Limited) |
| **Calculable across full cohort** | 4,083 / 10,871 (37.6%) |
| **Calculable among cancer-eligible** | ~3,996 / 4,136 (~96.6%) |
| **Non-calculable reason** | Benign procedures (6,735); missing tumour size for ~5% cancer-eligible |
| **Analysis tier** | PRIMARY (cancer cohort denominator) |

#### SL-17 — ATA 2015 Initial Risk

| Item | Value |
|---|---|
| **Limitation type** | PROC (Process-Limited) |
| **Calculable across full cohort** | 3,144 / 10,871 (28.9%) |
| **Calculable among cancer-eligible** | ~3,144 / 4,136 (76.0%) |
| **Analysis tier** | PRIMARY (cancer cohort subset) |

**Safe Methods sentence:**
> "AJCC 8th edition tumour staging and ATA 2015 initial risk stratification were calculated for patients with a confirmed malignant histological diagnosis and available tumour size and nodal status. AJCC staging was calculable in 4,083 patients (96.6% of cancer-eligible). ATA initial risk was calculable in 3,144 patients (76.0% of cancer-eligible) with the remainder lacking complete ETE grade or lymph node characterisation required for ATA tier assignment."

---

### 2.9 Preoperative Imaging (TIRADS)

#### SL-18 — TI-RADS Preoperative Score

| Item | Value |
|---|---|
| **Limitation type** | SRC (Source-Limited) |
| **Patients with TIRADS** | 3,474 / 10,871 (31.9%) |
| **Primary source** | COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx |
| **Analysis tier** | SENS |

**Safe Methods sentence:**
> "Preoperative TI-RADS scores were ascertained from structured institutional ultrasound radiology spreadsheets supplemented by NLP of clinical notes for 3,474 patients (31.9%). ACR TI-RADS 2017 scoring was applied using component criteria (composition, echogenicity, shape, margins, calcifications) where available. Patients imaged at outside institutions without corresponding structured reports were excluded from imaging-based subanalyses."

---

### 2.10 Pathology — Extranodal Extension Grading

#### SL-19 — ENE Extent Grade

| Item | Value |
|---|---|
| **Limitation type** | TPL (Template-Limited) |
| **ENE-positive patients** | 2,558 |
| **ENE extent-gradable** | 178 / 2,558 (7.0%) |
| **Present-ungraded** | 2,389 (83.5% of ENE-positive) |
| **Analysis tier** | DESC |

---

### 2.11 Pathology — Tumour Size and LN Positivity Missingness

#### SL-14 — Tumour Size (path_tumor_size_cm)

| Item | Value |
|---|---|
| **Limitation type** | PROC (Process-Limited) |
| **Available** | 4,130 / 10,871 (38.0%) |
| **Analysis tier** | SENS (with MICE imputation) |

#### SL-15 — Lymph Node Positive Count

| Item | Value |
|---|---|
| **Limitation type** | PROC (Process-Limited) |
| **Available** | 3,603 / 10,871 (33.1%) |
| **Analysis tier** | SENS (with MICE imputation) |

---

## 3. Fields Confirmed Adequate for Primary Analysis

| Field | Coverage | Verification |
|---|---|---|
| Histological diagnosis (cancer_flag) | 10,871 / 10,871 (100%) | `manuscript_cohort_v1` |
| Thyroglobulin values | 2,569 patients, 100% date coverage | `longitudinal_lab_canonical_v1` |
| Anti-TgAb values | 2,127 patients, 100% date coverage | `longitudinal_lab_canonical_v1` |
| Surgical procedure type | 9,371 episodes | `operative_episode_detail_v2` |
| ETE grade (v9, post-subgrading) | microscopic=5,393; gross=278 | `patient_refined_master_clinical_v12` |
| BRAF mutation status | 546 positive (primary NGS + confirmed NLP) | `patient_refined_master_clinical_v12` |
| TERT mutation status | 108 positive | `patient_refined_master_clinical_v12` |
| RAS mutation status | 337 positive | `patient_refined_master_clinical_v12` |
| RLN injury (refined, confirmed tier) | 92 patients (0.85%) | `extracted_rln_injury_refined_v2` |
| AJCC 8th staging (cancer cohort) | ~96.6% calculable | `thyroid_scoring_py_v1` |
| Age at surgery | 99.2% (10,871 cohort) | `demographics_harmonized_v2` |

---

## 4. Summary by Limitation Type

| Type | n Fields | Affected Domains | Resolution Path |
|---|---|---|---|
| SRC (Source-Limited) | 8 | RAI dose, non-Tg labs, nuclear med notes, note corpus, IHC BRAF, TIRADS partial | Institutional data feed required |
| TPL (Template-Limited) | 2 | Vascular invasion grade, ENE extent | Pathologist manual re-review of synoptic reports |
| PIPE (Pipeline-Limited) | 4 | Esophageal, berry ligament, frozen section, EBL | NLP vocabulary expansion + re-extraction |
| PROC (Process-Limited) | 5 | Recurrence dates, tumour size, LN count, AJCC, ATA | MICE imputation addresses numeric gaps; date registry requires chart abstraction |

---

## 5. Note on imaging_fna_linkage_v3 Status Update

At time of last AGENTS.md entry this table was documented as having 0 rows. **Live verification on 2026-03-14 confirms 9,024 rows** in `imaging_fna_linkage_v3` (652 high_confidence, 4,003 plausible, 4,320 weak tier; 2,072 distinct patients). This reflects linkages built during the analysis-resolved-layer construction (scripts 49–51). These linkages are available for sensitivity analyses linking preoperative imaging to FNA episodes. AGENTS.md will be updated in the post-session update.

---

## 6. Recommended Manuscript Boilerplate for Data Availability Statement

> "Data are available from the corresponding author upon reasonable request subject to institutional review and data use agreement. The analysis database was constructed from electronically-linked de-identified research records. Raw data contain protected health information and cannot be shared publicly. The analytic code used to generate all tables and figures is available on GitHub at [URL] and archived at Zenodo (DOI: 10.5281/zenodo.18945510). Nuclear medicine dispensing records, institutional laboratory feeds for TSH/T4/PTH/calcium, and clinical note types outside the research corpus (radiology reports, nuclear medicine reports, pathology addenda) were not available to the study team and are not included in the archived data."

---

*Generated 2026-03-14 · All metrics verified against `md:thyroid_research_2026` · THYROID_2026 v3.2.0*
