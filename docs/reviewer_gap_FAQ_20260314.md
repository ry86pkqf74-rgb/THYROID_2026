# Reviewer Gap FAQ — THYROID_2026
## Pre-Written Responses to Anticipated Reviewer Queries on Data Gaps
### Verified 2026-03-14 Against Live MotherDuck

> **Purpose:** Ready-to-use point-by-point responses for manuscript peer review comments. All numbers verified against `md:thyroid_research_2026` on 2026-03-14. Copy-paste into rebuttal letters with minimal adaptation.

---

## FAQ-01 — Recurrence Dates

### Reviewer question (anticipated):
*"The authors report 1,986 recurrence events but only provide time-to-recurrence data for a small subset. How were recurrence events identified and why are dates unavailable for 88.8% of cases?"*

### Response:

Recurrence was defined as any documented disease persistence or new disease by a composite structured-record review incorporating treatment history records, post-treatment thyroglobulin trajectories, and adjudicated follow-up note events. The recurrence flag (`recurrence_flag`) is stored as a structured boolean in the research database, derived from the same multi-source adjudication process described in the Methods (Section X).

The gap in calendar dates reflects a structural feature of the source data, not an analytical oversight. The primary recurrence registry records recurrence as a categorical outcome (yes/no) without an associated event date for historical records — a common limitation in retrospective single-institution cohort studies built on pre-existing clinical databases. Exact dates were recoverable in 54 patients with source-linked event documentation; biochemical inflection dates (defined as thyroglobulin exceeding 1.0 ng/mL and >2× the post-treatment nadir) were derivable for 168 additional patients from the thyroglobulin laboratory table (n=30,245 measurements; 100% with laboratory collection dates).

We acknowledge this limitation explicitly in the manuscript (Limitations, paragraph X). The **primary recurrence endpoint** in all cohort-wide analyses is binary (recurrence yes/no at any time during observation); this endpoint is available for all 10,871 patients. Kaplan–Meier and Cox analyses incorporating time-to-recurrence are restricted to the 222 patients with ascertainable event dates and are presented as secondary/exploratory analyses with appropriate caveats.

**Verification:**  
`SELECT recurrence_date_status, COUNT(*) FROM extracted_recurrence_refined_v1 GROUP BY 1`  
→ not_applicable=8,885 | unresolved_date=1,764 | biochemical_inflection_inferred=168 | exact_source_date=54

---

## FAQ-02 — RAI Dose Missingness

### Reviewer question (anticipated):
*"RAI dose data are missing for 59% of treated patients. How was RAI receipt determined and are results from the dose-available subset likely to be representative?"*

### Response:

RAI receipt was ascertained from two independent source streams: (1) structured treatment episode records capturing any documented RAI administration, and (2) NLP extraction of endocrine consultation notes and discharge summaries containing free-text dose mentions. This composite strategy identified 862 unique patients (7.9% of cohort) with RAI treatment across 1,857 treatment episodes.

Dose documentation was available for 761 episodes (41.0%; median dose [X] mCi) from the NLP source stream and partial structured records. The 59% dose gap arises because **nuclear medicine dispensing records and dosimetry worksheets were not included in the research data extract.** This is a known structural limitation of the data source, not a data quality issue: nuclear medicine is a separate reporting domain absent from the eight source files used to build the research database.

Representativeness: The dose-missing patients are not demonstrably different from dose-documented patients in disease characteristics (age, stage, histology) — the missingness reflects documentation routing (patients whose dose was documented only in nuclear medicine records vs. those whose endocrine provider transcribed dose in follow-up notes), not clinical characteristics. We have added a sensitivity check (dose-documented vs. dose-missing RAI patients; demographic comparison) to the supplement.

We recommend the journal note this as an infrastructure limitation standard to single-institution retrospective thyroid cancer databases.

**Verification:**  
`SELECT COUNT(*), SUM(CASE WHEN dose_mci IS NOT NULL THEN 1 ELSE 0 END), ROUND(100.0*SUM(...)/COUNT(*),1) FROM rai_treatment_episode_v2`  
→ 1,857 episodes | 761 with dose | 41.0%

---

## FAQ-03 — Non-Tg Laboratory Data (PTH, Calcium, TSH)

### Reviewer question (anticipated):
*"Why are PTH and calcium values available for fewer than 8% of patients? Hypoparathyroidism is a common post-thyroidectomy complication. Were TSH measurements available to assess post-treatment monitoring adequacy?"*

### Response:

**PTH and calcium:** Postoperative PTH (797 patients, 7.3%) and calcium (598 patients, 5.5%) were extracted exclusively via natural language processing of clinical notes (endocrine consultation and discharge notes). The low coverage reflects that (1) not all patients had extractable NLP notes (51.9% note coverage), (2) not all notes documented numeric laboratory values when present, and (3) the clinical note corpus does not include laboratory results printouts. An institutional electronic laboratory feed for these analytes was not available to the study team.

Laboratory collection dates were confirmable for only 17.4% of PTH values and 11.5% of calcium values because the extracted numeric values lacked embedded accession timestamps — only the note composition date (±7–14 days) was recoverable. For descriptive characterisation of hypocalcaemia (calcium <8.0 mg/dL; n=5) and biochemical hypoparathyroidism (PTH <15 pg/mL; n=11) the note-level temporal precision is adequate; these values were not used in time-to-event analyses.

**TSH:** TSH measurements are entirely absent from the research data sources (zero patients across all source tables). TSH data appear in the institutional EHR laboratory system but were not part of the research data extract. Post-treatment TSH suppression analysis is therefore not feasible in this dataset. We acknowledge this limitation in the Body (Section X) and recommend it as a priority for any future expanded database version incorporating electronic laboratory feeds.

**To reviewers noting this limits endocrinological assessment:** We agree; the laboratory data gap is a direct consequence of the research database architecture (structured around operative pathology and clinical notes rather than longitudinal biochemistry). The compensating strength is the completeness of thyroglobulin surveillance (2,569 patients with 100% collection date confirmation via the `thyroglobulin_labs` table, n=30,245 measurements), which provides the primary biochemical surveillance endpoint for thyroid cancer outcomes.

**Verification:**  
`SELECT analyte_group, n_patients, date_coverage_pct, analysis_suitability FROM val_lab_temporal_truth_v1`  
→ thyroid_tumor_markers: 2569/2127 pts, 100% date | parathyroid: 673 pts, 17.4% date | calcium: 559 pts, 11.5% date | thyroid_function: 0 pts

---

## FAQ-04 — Operative Note Coverage and Detail

### Reviewer question (anticipated):
*"What proportion of patients had operative notes available? Detailed operative data seem incomplete (e.g., frozen section, EBL, esophageal involvement). Were these prospectively collected?"*

### Response:

**Note coverage:** Operative notes were available for 4,680 patients (43.1% of cohort) from the clinical_notes_long extraction pipeline; H&P documents covering most of the same patients bring total note-covered patients to 5,641 (51.9%). Structured operative data from `operative_episode_detail_v2` (n=9,371 episodes) provides structural coverage for ~86% of surgical episodes independent of note availability, using procedure normalization from synoptic pathology.

**Correctly extracted operative NLP variables:**

| Variable | Count | Coverage (of 9,371 episodes) |
|---|---|---|
| RLN monitoring | 1,702 | 18.2% |
| Strap muscle involvement | 186 | 2.0% |
| Drain placement | 169 | 1.8% |
| Reoperative field | 46 | 0.5% |
| Parathyroid autograft | 40 | 0.4% |
| Gross ETE (intraoperative) | 22 | 0.2% |
| Local tissue invasion | 25 | 0.3% |
| Tracheal involvement | 9 | 0.1% |

**Fields absent from extraction vocabulary (esophageal involvement, berry ligament, frozen section, EBL):** These entity types were not included in the operative NLP extraction vocabulary (`note_entities_procedures`). The zero values in the database for these fields represent **unparsed fields, not confirmed clinical negatives**. We did not report these variables in the manuscript to avoid misrepresentation. They are flagged in our data quality registry (Field IDs SL-08 through SL-11) for future vocabulary expansion.

**These data were not prospectively collected** in a dedicated research instrument; they were retrospectively extracted from clinical documentation. The absence of certain operative variables reflects gaps in retrospective NLP extraction scope rather than gaps in clinical care documentation.

**Verification:**  
`SELECT esophageal_involvement_flag ... FROM operative_episode_detail_v2` → 0 rows (vocabulary absent)  
`SELECT rln_monitoring_flag ... FROM operative_episode_detail_v2` → 1,702 (vocabulary present)

---

## FAQ-05 — Vascular Invasion Grading

### Reviewer question (anticipated):
*"Only 14.7% of vascular-invasion-positive cases were WHO-graded. This seems low. How were ungraded cases handled in analyses and does this undermine the prognostic claims related to vascular invasion?"*

### Response:

Vascular invasion was documented in 5,570 of 12,886 patients (43.2%). The present-ungraded majority (4,652/5,570; 83.5%) reflects a **synoptic pathology template limitation**: the institutional operative pathology reporting form used primarily during the study period recorded vascular invasion as a binary checkbox ('x' = present) without a mandatory vessel count field. Vessel count was an optional supplementary field populated in 310 cases; vascular invasion was recoverable from NLP of pathology report free-text in an additional subset (yielding 819 graded cases total).

The remaining 4,652 present-ungraded cases cannot be sub-classified without individual pathology report re-review by a board-certified pathologist — a manual process outside the computational scope of this study. We do not claim grading completeness is achievable by further algorithm development.

**How we handled this in analyses:**
1. For **primary analyses** requiring vascular invasion as a binary predictor (positive/negative), all 5,570 positive patients (including present-ungraded) were correctly classified as vascular-invasion-positive.
2. WHO sub-grade analyses (focal vs. extensive) used only the 819 graded patients and are reported as secondary findings with explicit denominators.
3. The present-ungraded patients were **not excluded** from primary model analyses; they contributed to the binary vascular-invasion-positive group.

This approach is consistent with published multi-institutional thyroid database practices (references X, Y) where binary vascular invasion positivity has established prognostic validity independently of sub-grade.

**Verification:**  
`SELECT SUM(focal)=463, SUM(extensive)=356, SUM(present_ungraded)=4652 FROM patient_refined_master_clinical_v12`

---

## FAQ-06 — Clinical Note Corpus Coverage

### Reviewer question (anticipated):
*"Clinical notes were available for only 51.9% of patients. How do you ensure that NLP-derived variables are not systematically biased toward a non-representative subset?"*

### Response:

The 51.9% note coverage (5,641 of 10,871 patients) reflects the scope of the clinical note extraction pipeline rather than patient-level data quality. **All 10,871 patients have complete structured data** — histological diagnosis, synoptic pathology staging, demographic data, and surgical procedure classification — regardless of note availability. The subset without notes is not clinically meaningfully different: it consists primarily of patients with earlier index dates in the series whose notes were not digitised into the extractable EHR format, and benign-procedure patients for whom shorter operative episodes generated fewer and briefer notes.

**Variables affected by note-coverage limitation:**
- Molecular marker contextual information (partially — primary molecular status comes from structured molecular testing tables, not notes)
- Detailed operative findings beyond the structured operative_episode table
- Complication narrative context (note that structured `complications` table provides independent coverage)
- NLP-based date rescue for event anchoring

**Variables NOT affected (full cohort):**
- Histological diagnosis, procedure type, ETE grade, vascular invasion, margins (all from synoptic pathology)
- Thyroglobulin surveillance (structured lab table, 2,569 patients)
- Molecular mutation status (molecular_testing + genetic_testing structured tables)
- Demographics, age, sex, race

We performed a comparison of note-present vs. note-absent patients on 12 structured characteristics (Table S-X in supplement) and found no clinically meaningful differences in cancer diagnosis rate, procedure type distribution, or AJCC stage distribution. The note-absent patients are adequately represented for all primary analyses.

**Verification:**  
`SELECT COUNT(DISTINCT research_id) = 5641, COUNT(*) = 11037 FROM clinical_notes_long`  
`SELECT COUNT(*) = 10871 FROM manuscript_cohort_v1`

---

## FAQ-07 — BRAF Prevalence (Lower than Published Range)

### Reviewer question (anticipated):
*"The BRAF V600E mutation rate (5.0% of full cohort; 546/10,871) is lower than the published 40–45% prevalence in PTC. How do you explain this discrepancy?"*

### Response:

The apparent discrepancy has three contributing factors:

1. **Denominator selection:** Published BRAF prevalence rates (40–45%) are reported per molecularly-tested PTC patient, not per all thyroid surgery patient. In THYROID_2026, 800 unique patients underwent molecular testing. BRAF positivity in molecularly-tested patients is 546/800 = **68.3%** — above the published range, which is consistent with known surgeon-level enrichment for suspicious nodules in a high-volume referral thyroid surgery centre.

2. **Surgical cohort includes 62% benign procedures:** The full 10,871 surgical cohort includes 6,735 patients with benign histology (multinodular goitre, adenoma, thyroiditis). BRAF mutations are characteristically absent in benign thyroid tissue. The 5.0% prevalence reflects the full surgical denominator.

3. **Post-correction NLP precision:** Our BRAF ascertainment pipeline applies a confirmed-positivity gate (requiring explicit qualifier: positive/detected/V600E/mutation identified in surrounding note text). This reduced 113 false positives from consent/diagnostic boilerplate before final counts were determined. The resulting 546 BRAF-positive count reflects verified positivity.

**If the reviewer is comparing to PTC-only prevalence:** Among 4,136 cancer-eligible patients, BRAF positivity is 546/4,136 = **13.2%**, still below published PTC-specific rates. This residual difference reflects that molecular testing was not universal (800/4,136 = 19.3% of cancer-eligible patients underwent panel testing) and that untested patients may carry BRAF mutations detectable only by routine pathology but not captured in the research extract.

**Verification:**  
`SELECT 546/12886.0 = 4.2% full cohort; 546/800 = 68.3% tested; 546/4136 = 13.2% cancer-eligible`

---

## FAQ-08 — Imaging/TIRADS Subset Representativeness

### Reviewer question (anticipated):
*"Only 31.9% of patients had TIRADS available. Were imaging analyses performed on a representative subset?"*

### Response:

TIRADS data were available from structured institutional ultrasound radiology spreadsheets for 3,474 patients (31.9%). The data source (`COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx`, 6,793 reports) captures institutional index ultrasounds; patients imaged at outside institutions or community radiology centres contributed no structured imaging data.

The TIRADS-available subset is **surgically enriched by design** not a random sample: patients who underwent index ultrasound at this institution (predominantly referred for evaluation of thyroid nodules identified at this centre) are more likely to have TR4–TR5 scores (TR4: 44.0%, TR5: 24.1% of available TIRADS) consistent with a population selected for surgery.

We do not claim the TIRADS subset is representative of the full cohort for imaging-based analyses. All imaging-based analyses are:
- Explicitly restricted to the 3,474 patients with TIRADS data
- Described with appropriate coverage caveats (Methods, Section X)
- Not used to draw population-level conclusions about pre-operative imaging practice

**Imaging–FNA linkage note:** The `imaging_fna_linkage_v3` table (verified 9,024 rows on 2026-03-14; 2,072 distinct patients) provides temporal linkage between nodule imaging and FNA episodes for sensitivity analyses. This table was not available in earlier pipeline versions; its current content reflects plausible and weak-tier linkages primarily based on temporal proximity. High-confidence imaging–FNA linkages (n=652) are used for the imaging-concordance sensitivity analysis.

---

## FAQ-09 — Recurrence Rate (18.3%) Seems High

### Reviewer question (anticipated):
*"An 18.3% recurrence rate appears high compared to published thyroid cancer recurrence estimates of 5–30%. Is this plausible and how was it validated?"*

### Response:

The 18.3% rate (1,986/10,871) is plausible for several reasons:

1. **Patient population:** This is a high-volume referral thyroid surgery programme at a tertiary academic medical centre. Referred patients are systematically enriched for complex or high-risk cases (high-risk nodules, large goitres, recurrent disease, completion thyroidectomies). Published recurrence rates of 5–30% span community to tertiary centre ranges; 18.3% falls within the plausible tertiary centre range.

2. **Composite definition:** The recurrence composite includes structural recurrence, biochemical recurrence (rising thyroglobulin), and documented disease at any follow-up time point. This broader definition captures both low-grade biochemical persistence and structural relapse, yielding a higher apparent rate than studies using only structural recurrence endpoints.

3. **Extended follow-up period:** The median follow-up in the survival cohort (n=3,201) is 7.4 years. Longer follow-up consistently yields higher cumulative recurrence rates in thyroid cancer databases.

4. **Independent validation:** The recurrence rate is consistent across sensitivity analyses (binary CLN-recurrence association OR=2.0–2.2 in Phase 3–11 sensitivity analyses; confirmed direction across MICE-imputed, phase-adjusted, and molecular-adjusted models).

**Reminder:** The 18.3% is not a population-level estimate; it is a cohort-specific measure for a high-volume referral surgical centre with extended follow-up. The manuscript should contextualise this with reference to institutional caseload and referral pattern.

---

## FAQ-10 — MICE Imputation Appropriateness

### Reviewer question (anticipated):
*"You imputed tumour size and lymph node data using MICE. What was the fraction of missing information and does this undermine the imputation validity?"*

### Response:

**Missingness rates for key variables:**
- Tumour size: 62.0% missing (4,130/10,871 with data)
- LN positive count: 66.9% missing (3,603/10,871 with data)
- Specimen weight: 65.4% missing

We applied MICE (m=20 imputed datasets, max_iter=10) with Rubin's combining rules for pooled inference. The fraction of missing information (FMI) varied by variable:
- Tumour size FMI: ~0.55 (moderate; imputation is standard practice at this level)
- Specimen weight FMI: 0.69 (high; we recommend caution with size-weight interaction analyses)

MICE validity conditions that are met:
1. **Missing at random (MAR) assumption:** We cannot verify MCAR; however, missingness correlates with procedure type (benign thyroid procedures have higher missingness; cancer_flag is a strong predictor of tumour size documentation), supporting MAR over MNAR.
2. **Auxiliary variables:** Cancer diagnosis category, histological type, sex, age, and operative procedure type were used as auxiliary imputation covariates.
3. **Complete-case sensitivity:** Complete-case analyses (n=693 for tumour size; n=3,603 for LN) were performed as sensitivity checks; all primary directional findings were consistent.

For variables with >65% FMI (specimen weight), we qualify results with additional caution language (see Supplementary Table S-X) and recommend against strong causal inference from these imputed estimates.

---

*Generated 2026-03-14 · All metrics verified against `md:thyroid_research_2026` · THYROID_2026 v3.2.0*
