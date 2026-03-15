# Manuscript Caveats Pack — Pre-Written Limitation Language

**Date:** 2026-03-13  
**Project:** THYROID_2026  
**Purpose:** Ready-to-use limitation, methods, and discussion language for each known data caveat. Copy directly into manuscript drafts.

---

## Summary Table

| # | Caveat | Domain | Severity | Manuscript Section(s) |
|---|--------|--------|----------|----------------------|
| 1 | Source-limited non-thyroglobulin lab dates | Labs | Moderate | Methods, Limitations |
| 2 | Recurrence date sparsity (88.8% unresolved) | Outcomes | High | Methods, Limitations, Discussion |
| 3 | Nuclear medicine text absence | RAI | High | Methods, Limitations |
| 4 | Partial clinical note coverage (~50%) | NLP | Moderate | Methods, Limitations |
| 5 | Vascular invasion present_ungraded | Pathology | Moderate | Methods, Limitations |
| 6 | Operative detail boolean defaults | Operative | Low–Moderate | Methods, Limitations |
| 7 | BRAF prevalence context | Molecular | Low | Methods, Discussion |
| 8 | Scoring system calculability | Staging | Low | Methods, Limitations |

---

## Caveat 1: Source-Limited Non-Thyroglobulin Lab Dates

### What is true

- Thyroglobulin and anti-thyroglobulin labs have **99.5% date accuracy** via structured `specimen_collect_dt`.
- PTH coverage expanded from 131 → 673 patients (5.1×); calcium from 69 → 559 patients (8.1×). These dates are **NLP-extracted or note-anchored only** — not from a structured lab feed.
- TSH, free T4/T3, vitamin D, albumin, phosphorus, magnesium, calcitonin, and CEA have **zero structured data**. No institutional lab feed exists for these analytes.

### What NOT to claim

- Precise postoperative-day analysis for non-Tg labs.
- Complete biochemical monitoring data across the cohort.

### Suggested manuscript wording

**Methods:**
> Thyroglobulin laboratory values were linked to structured specimen collection dates (99.5% temporal accuracy). Post-operative calcium and parathyroid hormone values were abstracted from clinical notes with limited temporal precision; precise postoperative-day analysis was not feasible for these analytes. Comprehensive thyroid function testing (TSH, free T4) was not available in structured format.

**Limitations:**
> The absence of a structured institutional laboratory feed for non-thyroglobulin analytes (PTH, calcium, TSH) limited our ability to assess post-operative biochemical outcomes with day-level precision.

---

## Caveat 2: Recurrence Date Sparsity (88.8% Unresolved)

### What is true

- 1,986 patients flagged for recurrence (18.3% of 10,871).
- Only **54 (2.7%)** have exact source-dates suitable for time-to-event analysis.
- **168 (8.5%)** have biochemically-inferred dates (rising Tg trajectory).
- **1,764 (88.8%)** have a recurrence flag only — no day-level date.
- Recurrence events were identified from `recurrence_risk_features_mv.recurrence_flag` (structured registry), **not** from NLP.

### What NOT to claim

- Precise recurrence-free survival for the full cohort.
- Day-level recurrence dating for all patients.

### Suggested manuscript wording

**Methods:**
> Recurrence was defined as structural or biochemical disease recurrence identified from structured institutional registry flags. Day-level recurrence dates were available for 222 of 1,986 recurrence events (11.2%); the remainder were identified by recurrence flags without precise timing. Recurrence-free survival analyses were restricted to the subset with temporal precision.

**Limitations:**
> Recurrence dates were available for a minority of events, reflecting the retrospective single-institution design and the absence of a structured recurrence registry with day-level event capture. This limits time-to-recurrence analyses to the available subset.

**Discussion:**
> The high proportion of recurrences identified by flag rather than dated event is consistent with the clinical practice of documenting recurrence status during surveillance without systematic event-date recording.

---

## Caveat 3: Nuclear Medicine Text Absence

### What is true

- **Zero** nuclear medicine reports exist in the `clinical_notes_long` corpus (11,037 total clinical notes across 5 note types).
- RAI data sourced entirely from `rai_treatment_episode_v2` (1,857 episodes), `extracted_rai_validated_v1` (35 confirmed with dose), and NLP extraction from endocrine/DC/operative notes.
- RAI dose coverage: **41%** (761 / 1,857 episodes).
- This is a **first-class structural limitation** — nuclear medicine departments often use separate documentation systems not integrated into the general clinical notes corpus.

### What NOT to claim

- Comprehensive RAI treatment documentation.
- Complete dose data for all RAI-treated patients.

### Suggested manuscript wording

**Methods:**
> RAI treatment data were derived from institutional electronic health records. Nuclear medicine reports were not available in the clinical notes corpus; RAI receipt and dosing were ascertained from endocrine clinic notes, discharge summaries, and medication records.

**Limitations:**
> RAI dose documentation was available for 41% of treatment episodes, likely reflecting the absence of nuclear medicine reports in the extracted clinical notes corpus rather than true missing data.

---

## Caveat 4: Partial Clinical Note Coverage (~50% of Patients)

### What is true

- 11,037 clinical notes cover approximately **5,500 unique patients (~50%** of 10,871).
- Note type breakdown: op_note (4,680), h_p (4,221), other_history (525), endocrine_note (519), dc_sum (185).
- The remaining ~50% of patients have **only** structured data (path_synoptics, tumor_pathology, molecular_testing).
- NLP-derived variables are supplementary — all manuscript-critical metrics come from structured sources.

### What NOT to claim

- Comprehensive clinical notes for all patients.
- NLP variables as primary endpoints.

### Suggested manuscript wording

**Methods:**
> Clinical notes were available for approximately 50% of the cohort, from which supplementary NLP-extracted variables were derived. All primary outcome variables (recurrence status, molecular testing results, complication events) were sourced from structured institutional databases rather than NLP extraction.

**Limitations:**
> Clinical note availability was limited to approximately half of the cohort, restricting NLP-based enrichment to this subset.

---

## Caveat 5: Vascular Invasion `present_ungraded` Limitation

### What is true

- 3,846 patients with vascular invasion positive.
- **819 (21.3%)** graded per WHO 2022 criteria: 463 focal, 356 extensive.
- **4,652 (78.7%)** remain `present_ungraded` — the institutional synoptic pathology template uses 'x' as a present/positive marker without vessel count.
- Only 310 patients have the quantified vessel count needed for WHO 2022 grading (< 4 = focal, ≥ 4 = extensive).
- This is a **synoptic template limitation**, not a data quality gap.

### What NOT to claim

- Full WHO 2022 vascular invasion grading.
- Precise vessel count for all patients with vascular invasion.

### Suggested manuscript wording

**Methods:**
> Vascular invasion was classified as focal (<4 foci of vascular invasion) or extensive (≥4 foci) per WHO 2022 criteria when vessel count data were available. In the majority of cases, the institutional pathology synoptic report documented presence or absence of vascular invasion without quantification.

**Limitations:**
> WHO 2022 vascular invasion grading was limited to 21.3% of vascular-positive cases due to the institutional synoptic template recording vascular invasion as present/absent without vessel quantification.

---

## Caveat 6: Operative Detail Boolean Defaults

### What is true

- 10 operative boolean fields in `operative_episode_detail_v2` are set to **FALSE as a script default**, not confirmed negative values:
  - `rln_monitoring_flag`, `parathyroid_autograft_flag`, `gross_ete_flag`, `local_invasion_flag`, `tracheal_involvement_flag`, `esophageal_involvement_flag`, `strap_muscle_involvement_flag`, `reoperative_field_flag`, `drain_flag`, `parathyroid_resection_flag`
- The V2 NLP extractor was run (13,186 entities extracted) but COALESCE guards in the schema prevented overwriting the FALSE defaults.
- **FALSE = unknown/not parsed**, not confirmed negative.

### What NOT to claim

- "No RLN monitoring was performed" or "no parathyroid autograft" based on FALSE values.
- Operative technique frequencies derived from these fields.

### Suggested manuscript wording

**Methods:**
> Operative details were extracted from structured surgical records. Specific operative technique variables (e.g., recurrent laryngeal nerve monitoring, parathyroid autograft) were not reliably captured in the institutional operative reporting template and are not reported.

**Limitations:**
> Several operative technique variables were documented inconsistently in the institutional operative records, precluding analysis of their relationship to outcomes.

---

## Caveat 7: BRAF Prevalence Context

### What is true

- BRAF positive: **546 / 10,025 molecular-tested (5.4%)** (canonical from `patient_refined_master_clinical_v12.braf_positive_final`).
- Corrected from 659 after NLP false-positive removal (113 FP: 34 confirmed negatives, 68 ambiguous mentions, 11 conflicting context); then augmented by confirmed NLP recovery (+175 patients beyond structured 266).
- Published PTC BRAF V600E prevalence is **40–45%**.
- Our 5.4% rate reflects three factors:
  1. Denominator includes **all surgical patients**, not just PTC.
  2. Molecular testing was **selective**, not universal.
  3. Detection methods varied (NGS, NLP entity confirmation, IHC).

### What NOT to claim

- BRAF prevalence among a PTC-specific cohort without adjusting the denominator.
- BRAF prevalence as representative of PTC biology.

### Suggested manuscript wording

**Methods:**
> BRAF mutation status was determined from structured molecular testing results and confirmed NLP-extracted entities requiring explicit positive qualifiers in clinical note text. Ambiguous mentions (e.g., "tested for BRAF") without positive result language were excluded.

**Discussion:**
> The relatively low BRAF positivity rate (5.4%) compared with published PTC prevalence (40–45%) reflects our inclusive surgical cohort denominator encompassing benign and non-PTC histologies, selective rather than universal molecular testing, and the stringent NLP confirmation criteria applied to exclude false-positive mentions.

---

## Caveat 8: Scoring System Calculability

### What is true

- AJCC 8th Edition calculable for **37.6%** of the full cohort; ATA 2015 for **28.9%**; MACIS for **37.5%**.
- AGES and AMES calculable for **100%**.
- Among the **4,136 analysis-eligible cancer patients**, calculability is substantially higher.
- Low full-cohort rates reflect the proportion of patients **without malignancy** — benign thyroid surgery patients (multinodular goiter, Graves disease, etc.) have no cancer staging data by definition.

### What NOT to claim

- Staging data available for all patients.
- Missing staging as a data quality issue.

### Suggested manuscript wording

**Methods:**
> Thyroid cancer staging was performed using the AJCC 8th Edition, ATA 2015 risk stratification, and MACIS scoring systems. Scoring was restricted to patients with confirmed thyroid malignancy and sufficient pathological data; patients undergoing thyroid surgery for benign indications were classified as not applicable rather than missing.

**Limitations:**
> Formal cancer staging was calculable for approximately one-third of the full surgical cohort, reflecting the substantial proportion of patients who underwent surgery for benign thyroid conditions and therefore lack cancer-specific staging variables.

---

## Usage Notes

1. **Copy directly** — each wording block is designed to be inserted into a manuscript draft as-is or with minor adaptation.
2. **Section tags** (Methods / Limitations / Discussion) indicate where each block belongs in a standard IMRAD manuscript.
3. **Do not mix caveats** — each caveat has distinct scope. Combining them into a single paragraph obscures the specific limitation being disclosed.
4. **Update counts** if re-running pipelines after this date — the numbers above reflect the 2026-03-13 dataset freeze.
