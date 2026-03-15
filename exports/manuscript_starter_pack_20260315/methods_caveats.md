# Methods & Caveats — Copy-Paste Manuscript Language

**Generated:** 2026-03-15  
**Source:** `docs/MANUSCRIPT_CAVEATS_20260313.md` (8 caveats)  
**Rule:** Each block is designed to drop into the indicated section of an IMRAD manuscript. Do NOT combine separate caveats into a single paragraph — each has distinct scope.

---

## Methods Section Language

### Study Population & Data Sources

> Thyroid cancer staging was performed using the AJCC 8th Edition, ATA 2015 risk stratification, and MACIS scoring systems. Scoring was restricted to patients with confirmed thyroid malignancy and sufficient pathological data; patients undergoing thyroid surgery for benign indications were classified as not applicable rather than missing.

### Laboratory Data

> Thyroglobulin laboratory values were linked to structured specimen collection dates (99.5% temporal accuracy). Post-operative calcium and parathyroid hormone values were abstracted from clinical notes with limited temporal precision; precise postoperative-day analysis was not feasible for these analytes. Comprehensive thyroid function testing (TSH, free T4) was not available in structured format.

### Recurrence Definition

> Recurrence was defined as structural or biochemical disease recurrence identified from structured institutional registry flags. Day-level recurrence dates were available for 222 of 1,986 recurrence events (11.2%); the remainder were identified by recurrence flags without precise timing. Recurrence-free survival analyses were restricted to the subset with temporal precision.

### RAI Ascertainment

> RAI treatment data were derived from institutional electronic health records. Nuclear medicine reports were not available in the clinical notes corpus; RAI receipt and dosing were ascertained from endocrine clinic notes, discharge summaries, and medication records.

### NLP Extraction

> Clinical notes were available for approximately 50% of the cohort, from which supplementary NLP-extracted variables were derived. All primary outcome variables (recurrence status, molecular testing results, complication events) were sourced from structured institutional databases rather than NLP extraction.

### Molecular Testing

> BRAF mutation status was determined from structured molecular testing results and confirmed NLP-extracted entities requiring explicit positive qualifiers in clinical note text. Ambiguous mentions (e.g., "tested for BRAF") without positive result language were excluded.

### Vascular Invasion

> Vascular invasion was classified as focal (<4 foci of vascular invasion) or extensive (≥4 foci) per WHO 2022 criteria when vessel count data were available. In the majority of cases, the institutional pathology synoptic report documented presence or absence of vascular invasion without quantification.

### Operative Details

> Operative details were extracted from structured surgical records. Specific operative technique variables (e.g., recurrent laryngeal nerve monitoring, parathyroid autograft) were not reliably captured in the institutional operative reporting template and are not reported.

---

## Limitations Section Language

### Labs

> The absence of a structured institutional laboratory feed for non-thyroglobulin analytes (PTH, calcium, TSH) limited our ability to assess post-operative biochemical outcomes with day-level precision.

### Recurrence

> Recurrence dates were available for a minority of events, reflecting the retrospective single-institution design and the absence of a structured recurrence registry with day-level event capture. This limits time-to-recurrence analyses to the available subset.

### RAI

> RAI dose documentation was available for 41% of treatment episodes, likely reflecting the absence of nuclear medicine reports in the extracted clinical notes corpus rather than true missing data.

### Notes

> Clinical note availability was limited to approximately half of the cohort, restricting NLP-based enrichment to this subset.

### Vascular Invasion

> WHO 2022 vascular invasion grading was limited to 21.3% of vascular-positive cases due to the institutional synoptic template recording vascular invasion as present/absent without vessel quantification.

### Operative

> Several operative technique variables were documented inconsistently in the institutional operative records, precluding analysis of their relationship to outcomes.

### Staging

> Formal cancer staging was calculable for approximately one-third of the full surgical cohort, reflecting the substantial proportion of patients who underwent surgery for benign thyroid conditions and therefore lack cancer-specific staging variables.

---

## Discussion Section Language

### Recurrence Precision

> The high proportion of recurrences identified by flag rather than dated event is consistent with the clinical practice of documenting recurrence status during surveillance without systematic event-date recording.

### BRAF Prevalence

> The relatively low BRAF positivity rate (5.4%) compared with published PTC prevalence (40–45%) reflects our inclusive surgical cohort denominator encompassing benign and non-PTC histologies, selective rather than universal molecular testing, and the stringent NLP confirmation criteria applied to exclude false-positive mentions.

---

## Caveat Summary Reference

| # | Domain | Severity | Sections |
|---|--------|----------|----------|
| 1 | Non-Tg lab dates | Moderate | Methods, Limitations |
| 2 | Recurrence date sparsity | High | Methods, Limitations, Discussion |
| 3 | Nuclear medicine absence | High | Methods, Limitations |
| 4 | Clinical note coverage | Moderate | Methods, Limitations |
| 5 | Vascular invasion grading | Moderate | Methods, Limitations |
| 6 | Operative boolean defaults | Low–Moderate | Methods, Limitations |
| 7 | BRAF prevalence context | Low | Methods, Discussion |
| 8 | Scoring system calculability | Low | Methods, Limitations |

---

*All numbers above reflect the 2026-03-13 dataset freeze. Update if re-running pipelines.*
