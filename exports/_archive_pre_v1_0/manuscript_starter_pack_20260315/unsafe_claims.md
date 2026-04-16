# Unsafe Claims — THYROID_2026 Manuscript

**Generated:** 2026-03-15  
**Rule:** Do NOT include any of the following claims in the manuscript without explicit qualification. Each entry explains WHY the claim is unsafe and what, if anything, can be said instead.

---

## 1. Time-to-Recurrence Precision

**UNSAFE:** "Median time to recurrence was X years."  
**WHY:** 88.8% of recurrence events (1,764 / 1,986) lack day-level dates. Only 54 have exact source dates and 168 have biochemical inflection dates.  
**INSTEAD SAY:** "Recurrence was identified in 1,986 patients (18.3%); however, only 222 (11.2%) had day-level temporal resolution. Time-to-event analyses should be interpreted with this limitation." Use recurrence flag + Tg trajectory as a surrogate endpoint.

---

## 2. RAI Dose-Response Analyses

**UNSAFE:** "Higher RAI dose was associated with..."  
**WHY:** RAI dose is available for only 761 of 1,857 episodes (41.0%). Nuclear medicine reports are entirely absent from the clinical note corpus (0 notes). Only 35 patients meet the strict "confirmed with dose" criterion.  
**INSTEAD SAY:** "RAI receipt was identified in 1,857 episodes; dose was available for 761 (41.0%). The absence of nuclear medicine reports from the institutional note archive limits dose-response analysis."

---

## 3. "All Patients Received RAI" / Comprehensive RAI Analysis

**UNSAFE:** "RAI was administered to N patients" (implying complete capture).  
**WHY:** RAI identification relies on NLP extraction from endocrine notes, discharge summaries, and operative notes. Without nuclear medicine department records, RAI receipt is necessarily undercounted. The 35 strict-confirmed figure represents a lower bound, not a true population rate.  
**INSTEAD SAY:** "A minimum of 35 patients had RAI receipt confirmed with dose verification; an additional 1,822 episodes were identified at lower certainty tiers."

---

## 4. Operative Detail Negation

**UNSAFE:** "RLN monitoring was not used in X% of cases" or "No drain was placed" or any operative boolean used as confirmed-negative.  
**WHY:** Prior to script 104 hardening (2026-03-15), 10 operative boolean fields (rln_monitoring_flag, drain_flag, parathyroid_autograft_flag, etc.) defaulted to FALSE, meaning UNKNOWN, not confirmed-negative. Post-hardening, these are NULL (unknown) or TRUE (confirmed). FALSE should never be interpreted as "did not occur."  
**INSTEAD SAY:** "RLN monitoring was documented in 1,702 episodes (18.2%). For the remaining episodes, operative reports did not contain extractable documentation of monitoring."

---

## 5. WHO 2022 Vascular Invasion Grading Prevalence

**UNSAFE:** "Focal vascular invasion was present in X% and extensive in Y%."  
**WHY:** 83.5% of vascular-positive patients (4,652 / 5,570) have "present, ungraded" status because the synoptic pathology template uses "x" as a placeholder without recording vessel count. Only 819 have WHO 2022 focal/extensive grading. Prevalence among the graded subset is NOT generalizable to the full vascular-positive population.  
**INSTEAD SAY:** "Among 819 patients with WHO 2022-classifiable data, 463 (56.5%) had focal and 356 (43.5%) had extensive vascular invasion. A further 4,652 patients had vascular invasion documented without grading."

---

## 6. Population-Level TSH / Free T4 / Vitamin D Trends

**UNSAFE:** "Postoperative TSH levels showed..." or any claim about non-Tg lab analytes at a population level.  
**WHY:** TSH, free T4, free T3, vitamin D, albumin, phosphorus, magnesium, calcitonin, and CEA have 0% coverage in the current dataset. The canonical lab schema has placeholders but no data.  
**INSTEAD SAY:** Nothing — these analyses are deferred pending institutional lab extract delivery.

---

## 7. Complete Clinical Note NLP Coverage

**UNSAFE:** "NLP extraction was applied to all patients" or any claim implying universal note coverage.  
**WHY:** Only 5,641 / 10,871 (51.9%) patients have >=1 clinical note in the corpus. NLP-derived variables (complication entities, molecular mentions, operative findings) are limited to patients with notes.  
**INSTEAD SAY:** "NLP extraction was performed on available clinical notes (5,641 patients, 51.9%); patients without clinical notes relied exclusively on structured data sources."

---

## 8. Nuclear Medicine-Dependent Claims

**UNSAFE:** "Whole-body iodine scan findings demonstrated..." or "Post-therapy scan was positive in..."  
**WHY:** Zero nuclear medicine reports exist in `clinical_notes_long`. All RAI-related data is derived from secondary mentions in endocrine notes, discharge summaries, and operative notes.  
**INSTEAD SAY:** Do not make claims about scan findings. Limit RAI discussion to receipt, dose (where available), and assertion status from clinical context.

---

## 9. Using Master Clinical Table (v12) Molecular Counts

**UNSAFE:** "BRAF was positive in 546 patients..."  
**WHY:** `patient_refined_master_clinical_v12` aggregates from all sources including unvalidated ThyroSeq counts and broad NLP without FP-correction. The curated extraction tables apply stricter gates.  
**INSTEAD SAY:** Use ONLY curated counts (BRAF = 376, RAS = 292, TERT = 108) from `extracted_braf_recovery_v1`, `extracted_ras_patient_summary_v1`, and `patient_refined_master_clinical_v12.tert_positive_v9` respectively.

---

## 10. BRAF Prevalence Compared to Published Literature

**UNSAFE:** "Our BRAF prevalence of 3.8% is consistent with published rates of 40-45% in PTC."  
**WHY:** The 3.8% is against all 10,025 tested patients (including benign disease). Among confirmed PTC patients only, BRAF prevalence would be substantially higher and closer to published ranges.  
**INSTEAD SAY:** "Among all molecularly tested patients (N = 10,025), BRAF positivity was 3.8%. This reflects the inclusion of benign disease in the surgical cohort; among confirmed PTC patients, prevalence aligns more closely with published estimates."

---

## 11. Adjudication-Dependent Claims

**UNSAFE:** "All discordant results were adjudicated by an expert panel."  
**WHY:** The adjudication framework is deployed (script 19, review queues, persistence layer), but 0 decisions have been entered by clinical reviewers. All current values are algorithmically derived.  
**INSTEAD SAY:** "Discordances were resolved algorithmically using source-reliability scoring and temporal concordance. An adjudication framework for expert review has been implemented but was not utilized for this analysis."

---

## 12. IHC BRAF (VE1) Results

**UNSAFE:** "Immunohistochemistry confirmed BRAF V600E in..."  
**WHY:** Only 2 IHC BRAF results were extractable from the clinical note corpus. VE1 addendum reports are not present in `clinical_notes_long`. This is insufficient for any population-level claim.  
**INSTEAD SAY:** Nothing — IHC BRAF data is too sparse for reporting.

---

## 13. Scoring System Calculability as "Complete Staging"

**UNSAFE:** "All patients were staged using AJCC 8th Edition."  
**WHY:** AJCC8 is calculable for 37.6% of the full cohort — the remainder are benign or lack required inputs. Even among analysis-eligible cancer (N = 4,136), ~2% lack sufficient data.  
**INSTEAD SAY:** "AJCC 8th Edition staging was computable for 4,083 patients (98.7% of analysis-eligible cancer; 37.6% of the full surgical cohort)."
