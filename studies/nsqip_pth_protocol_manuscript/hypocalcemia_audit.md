# Hypocalcemia Rate Audit — NSQIP/PTH Protocol Manuscript

**Auditor:** Automated pipeline audit  
**Date:** 2026-03-13 (revised)  
**Target manuscript:** Institutional NSQIP/PTH protocol for *The American Surgeon*  
**Manuscript claim under audit:** "clinically significant hypocalcemia occurred in 7.5% (57/763) among institutional total/completion thyroidectomies"

---

## 1. Executive Summary

| Question | Answer |
|----------|--------|
| Is 57/763 = 7.5% reproducible? | **YES** — at the surgery-level analysis of Case Details |
| Is 57 the correct numerator? | **NO** — 57 is *any postoperative hypocalcemia* (NSQIP module Yes/No). The institutional SAR shows **19 hypocalcemia-related readmissions** across all thyroidectomies. These are different metrics. |
| Is the denominator correct? | **NO** — 763 is surgery-level; patient-level is 755. Additionally, if using SAR readmission definition, denominator should be 1,393 total thyroidectomies. |
| Is "clinically significant" the right label? | **NO** — the NSQIP module field captures *any* postoperative hypocalcemia (mild/biochemical/outpatient), not clinically significant events |
| Is the rate overestimated? | **YES** — 7.5% conflates mild biochemical hypocalcemia with clinically significant events. The SAR-verified readmission rate is **19/1,393 = 1.4%** |
| Is the metric manuscript-safe? | **NO as stated** — the numerator, denominator, clinical label, and parent cohort all require correction |

### Critical Discrepancy: Two Data Products, Two Definitions

| Data source | What it counts | Count | Denominator | Rate |
|-------------|---------------|-------|-------------|------|
| NSQIP SAR (institutional QI dashboard) | **Readmissions for hypocalcemia** | **19** | 1,393 total Tx | **1.4%** |
| NSQIP Case Details module field | Any postop hypocalcemia (Yes/No) | 57 | 755 TC patients with module | 7.5% |

The SAR "Hypocalcemia" column = Readmissions × ReadmFromHypocalcemia (verified: all 14 years match exactly). The Case Details module field captures any biochemical or symptomatic hypocalcemia within 30 days, regardless of severity or need for readmission.

**Recommended replacement:** Use SAR-verified 19/1,393 = 1.4% for readmission-requiring hypocalcemia, or report both metrics with clear definitions.

---

## 2. Source of Truth

### 2.1 Data File

```
studies/nsqip_linkage/nsqip_thyroid_linkage_final.csv
```

This is the raw NSQIP Case Details linkage file containing 1,813 total rows (1,803 with "Perfect deterministic match"). It includes all thyroid CPT codes (60220–60271), including lobectomies.

### 2.2 Relevant NSQIP Columns

| Column | Content | Non-null (TC cases) |
|--------|---------|---------------------|
| `Thyroidectomy Postoperative Hypocalcemia` | Yes/No/Unknown/NaN | 763/1086 (70.3%) |
| `Thyroidectomy IV Calcium` | Yes-{detail}/No-Unknown/NaN | 72/1086 (6.6%) |
| `Thyroidectomy Postop Hypocalcemia-related Event` | Yes/No/NaN | 689/1086 (63.4%) |
| `Thyroidectomy Postop Hypocalcemia-related Event Type` | Free text | 33/1086 (3.0%) |
| `Thyroidectomy Postoperative Calcium Level Checked` | Yes/No-unknown/NaN | 760/1086 (70.0%) |
| `Thyroidectomy Postoperative Parathyroid Level Checked` | Yes/No-unknown/NaN | 758/1086 (69.8%) |

### 2.3 Filtering Logic That Produces 57/763

```
1. Load nsqip_thyroid_linkage_final.csv (1,813 rows)
2. Filter to match_status == "Perfect deterministic match" (1,803 rows)
3. Filter to total/completion CPTs [60240, 60252, 60254, 60260, 60270, 60271] (1,086 rows)
4. Count rows where Thyroidectomy Postoperative Hypocalcemia IN ('Yes','No') → 763 rows
5. Count rows where Thyroidectomy Postoperative Hypocalcemia == 'Yes' → 57 rows
6. Rate = 57/763 = 7.47%, rounds to 7.5%
```

**Critical note:** Step 3–5 operate at SURGERY LEVEL (1,086 cases), not patient level (1,075 patients). Eleven patients have two total/completion surgeries each, contributing 22 extra rows to the denominator.

---

## 3. Data Lineage Map

```
RAW SOURCE
  raw/Case_Details_and_Custom_Fields_Report-14-Dec-2025-1204.xlsx
  (1,281 NSQIP Case Details rows)
      │
      ▼
LINKAGE SCRIPT
  nsqip_case_details_linkage.py
  (deterministic MRN+date matching to institutional lakehouse)
      │
      ▼
LINKED FILE
  studies/nsqip_linkage/nsqip_thyroid_linkage_final.csv
  (1,813 rows; 1,803 matched; includes lobectomies)
      │
      ├──▶ ENRICHMENT SCRIPT: scripts/nsqip_enrichment.py
      │        exports/nsqip/nsqip_enrichment.parquet (1,275 rows, TC only)
      │        exports/nsqip/nsqip_patient_summary.parquet (1,261 patients, TC only)
      │        → manuscript_statistics.md: 82/939 = 8.7% (DIFFERENT COHORT)
      │
      └──▶ LOBECTOMY EXCLUSION SCRIPT:
           studies/nsqip_pth_protocol_manuscript/nsqip_lobectomy_exclusion_analysis.py
           (filters to TC CPTs, surgery-level analysis)
           → Manuscript claim: 57/763 = 7.5%

MotherDuck Tables (NOT used for this metric):
  complications.hypocalcemia → ALL NULL (10,864 rows)
  extracted_hypocalcemia_refined_v2 → 82 patients (NLP-derived, different source)
  extracted_postop_labs_expanded_v1 → 559 patients with calcium, 5 < 7.5 mg/dL
  vw_postop_lab_nadir → 41 patients with calcium nadir
```

**Key finding:** The manuscript mixes two incompatible data versions:
- `manuscript_statistics.md` reports 82/939 = 8.7% from `nsqip_patient_summary.parquet` (1,261 patients)
- The manuscript text claims 57/763 = 7.5% from the raw linkage CSV (1,086 surgery-level cases)

These are different linkage rounds. All 57 hypo patients appear in both, but the patient summary has 25 additional hypo patients not in the raw linkage total/completion subset.

---

## 4. Clinical Definition Audit

### 4.1 What the manuscript says

> "clinically significant hypocalcemia occurred in 7.5% (57/763)"

### 4.2 What the code actually implements

```python
hypo_valid = df_tc["Thyroidectomy Postoperative Hypocalcemia"].isin(["Yes", "No"])
n_hypo = (df_tc.loc[hypo_valid, hypo_col] == "Yes").sum()
```

This is simply the ACS-NSQIP thyroidectomy module field "Thyroidectomy Postoperative Hypocalcemia" coded as Yes/No. It captures **any** postoperative hypocalcemia within 30 days, whether biochemical or symptomatic, mild or severe.

### 4.3 NSQIP module definition

Per the ACS-NSQIP Thyroidectomy PUF data dictionary, this field is defined as:
> "Occurrence of postoperative hypocalcemia (symptomatic or biochemical) from the date of surgery through 30 days after the operation."

This is NOT restricted to:
- Serum calcium < 7.5 mg/dL (severe biochemical)
- Symptomatic hypocalcemia requiring IV calcium (clinically significant)
- Any specific severity threshold

### 4.4 Verdict

**The label "clinically significant" is NOT supported by the data.** The NSQIP field captures any postoperative hypocalcemia. The correct label is "postoperative hypocalcemia (NSQIP module)."

If the authors intend "clinically significant" to mean hypocalcemia requiring intervention, the correct numbers are:

| Definition | n/N | Rate |
|-----------|-----|------|
| Hypocalcemia-related event (any) | 33/689 | 4.8% |
| Hypo AND (event OR IV calcium) | 31/755 | 4.1% |
| IV calcium supplementation event | 17/755 | 2.3% |

---

## 5. Denominator Audit: Why 763 and Not 755

| Level | Parent cohort | Module available | Hypo Yes | Rate |
|-------|---------------|-----------------|----------|------|
| Surgery-level | 1,086 cases | 763 | 57 | 7.47% |
| Patient-level | 1,075 patients | 755 | 57 | 7.55% |

The difference (8 denominator records) comes from 11 patients with 2 total/completion surgeries each. Of these 11 multi-surgery patients, 8 had hypocalcemia module data on both surgeries, inflating the denominator from 755 to 763. No multi-surgery patient had hypo=Yes on either surgery, so the numerator (57) is unaffected.

### Multi-surgery patient detail

| research_id | Surgeries | CPTs | Hypo values |
|-------------|-----------|------|-------------|
| 651 | 2 | 60260, 60240 | No, No |
| 1676 | 2 | 60271, 60260 | NaN, No |
| 1854 | 2 | 60260, 60254 | NaN, NaN |
| 2024 | 2 | 60270, 60260 | NaN, NaN |
| 3499 | 2 | 60271, 60260 | NaN, NaN |
| 4438 | 2 | 60271, 60260 | No, No |
| 4661 | 2 | 60260, 60252 | No, No |
| 6082 | 2 | 60260, 60252 | No, No |
| 6087 | 2 | 60270, 60260 | No, No |
| 7306 | 2 | 60260, 60252 | No, No |
| 8045 | 2 | 60240, 60240 | No, No |

**Verdict:** The denominator should be 755 (patient-level) not 763 (surgery-level). Practically, the rate rounds to 7.5% either way, but the manuscript should report patient-level for methodological correctness.

---

## 6. Missingness Analysis

### 6.1 Overall

Of 1,075 total/completion patients:
- 755 (70.2%) have hypocalcemia status → analyzable
- 320 (29.8%) lack hypocalcemia status → excluded from rate

### 6.2 By year

| Year | N | Missing | Missing % | Hypo rate |
|------|---|---------|-----------|-----------|
| 2010 | 64 | 64 | 100% | N/A |
| 2011 | 90 | 90 | 100% | N/A |
| 2012 | 105 | 105 | 100% | N/A |
| 2013 | 102 | 52 | 51% | 14.0% |
| 2014 | 99 | 1 | 1% | 18.4% |
| 2015 | 83 | 0 | 0% | 9.6% |
| 2016 | 40 | 0 | 0% | 12.5% |
| 2017 | 80 | 1 | 1% | 6.3% |
| 2018 | 54 | 4 | 7% | 0.0% |
| 2019 | 70 | 0 | 0% | 4.3% |
| 2020 | 55 | 0 | 0% | 1.8% |
| 2021 | 35 | 1 | 3% | 0.0% |
| 2022 | 34 | 0 | 0% | 0.0% |
| 2023 | 164 | 2 | 1% | 6.2% |

- **2010–2012:** 100% missing (pre-module era; module introduced ~2013)
- **2013:** 51% missing (partial rollout year)
- **2014+:** 0–7% missing (module operational)
- **2013–2014:** Rates of 14.0–18.4% are substantially higher than 2015+ rates (0–12.5%), raising concern for early ascertainment bias (module may have been preferentially completed for complicated cases during rollout)

### 6.3 By surgery type

| Type | N | Missing | Hypo Yes | Rate |
|------|---|---------|----------|------|
| Total | 953 | 274 (28.8%) | 52 | 7.7% |
| Completion | 122 | 46 (37.7%) | 5 | 6.6% |

### 6.4 Missingness in other fields

| Field | N available | N missing | Missing % |
|-------|------------|-----------|-----------|
| IV calcium | 72 | 1,003 | 93.3% |
| Hypocalcemia event | 689 | 386 | 35.9% |
| Calcium checked | 760 | 315 | 29.3% |
| PTH checked | 758 | 317 | 29.5% |

IV calcium data are available for only 6.7% of the cohort — far too sparse for an independent severity analysis.

---

## 7. Sensitivity Analyses (Patient-Level, N=1,075)

| Definition | Numerator | Denominator | Rate | 95% CI |
|-----------|-----------|-------------|------|--------|
| A. NSQIP hypo=Yes (manuscript method) | 57 | 755 | 7.5% | 5.9–9.7% |
| B. Hypo=Yes OR IV calcium=Yes | 57 | 755 | 7.5% | 5.9–9.7% |
| C. Hypocalcemia-related event (any) | 33 | 689 | 4.8% | 3.4–6.7% |
| D. Hypo=Yes AND (event OR IV) | 31 | 755 | 4.1% | 2.9–5.8% |
| E. IV calcium supplementation event | 17 | 755 | 2.3% | 1.4–3.6% |
| F. IV calcium required | 2 | 72 | 2.8% | 0.8–9.5% |

**Observations:**
- Definitions A and B are identical because all IV calcium patients already have hypo=Yes
- Definitions C–E represent increasingly stringent "clinically significant" thresholds
- If the authors mean "requiring intervention," the rate is 4.1% (D), not 7.5% (A)
- The IV-calcium-only definition (F) has an unreliable denominator (only 72 patients)

---

## 8. Lobectomy Exclusion Verification

- Zero lobectomy CPTs (60220, 60225) exist in the total/completion filter: **PASS**
- 55 patients have BOTH a lobectomy AND a total/completion surgery in the same dataset. These patients appear only under their total/completion CPT in the tc subset: **PASS**

---

## 9. Multi-Surgery Double-Count Verification

- 57 hypo=Yes rows correspond to 57 unique patients: **NO numerator double-counting**
- 763 module-available rows correspond to 755 unique patients: **8 denominator records are duplicated**
- No multi-surgery patient has hypo=Yes on any surgery: double-counting does not inflate the numerator

---

## 10. Institutional Data Cross-Check (MotherDuck)

| Source | Hypocalcemia patients | Notes |
|--------|----------------------|-------|
| `complications.hypocalcemia` | 0 | ALL NULL — field was never populated |
| `extracted_hypocalcemia_refined_v2` | 82 | NLP-derived (18 confirmed, 47 probable, 17 uncertain) |
| `patient_refined_complication_flags_v2` | 82 refined, 18 confirmed | Different methodology from NSQIP |
| `vw_postop_lab_nadir` (calcium) | 41 patients total, 5 < 7.5 mg/dL | Extremely sparse coverage |
| `extracted_postop_labs_expanded_v1` | 559 patients with calcium values | But only 41 with usable nadirs |

The institutional lakehouse has NO direct source for the NSQIP hypocalcemia metric. The NSQIP module field is the sole source of truth for this measurement.

---

## 11. SAR vs Case Details Reconciliation

### 11.1 Verified arithmetic

The SAR "Hypocalcemia" column = Readmissions × Readmission-from-Hypocalcemia proportion. All 14 years verify exactly:

| Year | Readmissions | ReadmFromHypo | SAR Hypo | Computed | Match |
|------|-------------|---------------|----------|----------|-------|
| 2010 | 5 | 0.80 | 4 | 4 | Yes |
| 2011 | 4 | 0.75 | 3 | 3 | Yes |
| 2012 | 4 | 1.00 | 4 | 4 | Yes |
| 2013 | 2 | 1.00 | 2 | 2 | Yes |
| 2014 | 2 | 1.00 | 2 | 2 | Yes |
| 2015 | 1 | 0.00 | 0 | 0 | Yes |
| 2016 | 1 | 0.00 | 0 | 0 | Yes |
| 2017 | 4 | 0.25 | 1 | 1 | Yes |
| 2018 | 0 | 0.00 | 0 | 0 | Yes |
| 2019 | 1 | 1.00 | 1 | 1 | Yes |
| 2020 | 0 | 0.00 | 0 | 0 | Yes |
| 2021 | 0 | 0.00 | 0 | 0 | Yes |
| 2022 | 0 | 0.00 | 0 | 0 | Yes |
| 2023 | 4 | 0.50 | 2 | 2 | Yes |
| **Total** | **28** | | **19** | **19** | **All** |

19 of 28 total readmissions (67.9%) were attributable to hypocalcemia.

### 11.2 Why the Case Details cannot reproduce 19

| Reason | Detail |
|--------|--------|
| Pre-module gap | SAR includes 11 events from 2010–2012; Case Details module fields are ALL NULL for those years |
| Different data product | SAR is an institutional aggregate; Case Details is patient-level |
| Definition mismatch | SAR tracks readmissions; Case Details tracks any postop hypocalcemia |
| Case Details closest match | Hypo=Yes AND readmitted = 10 patients; "Readmitted for low calcium" event = 6 patients |

### 11.3 What 57 actually represents

Of the 57 Case Details hypo=Yes patients (total/completion, 2013–2023):
- **10** were readmitted (17.5%) — closest to SAR concept
- **31** had a hypocalcemia-related event (54.4%) — ER, IV calcium, or readmission
- **26** had hypo=Yes but NO event and NO readmission (45.6%) — mild/outpatient/biochemical-only

---

## 12. Manuscript-Safe Recommendations

### A. RECOMMENDED: Report both metrics with clear definitions

> "Among 1,393 total and completion thyroidectomies in the NSQIP institutional cohort (2010–2023), hypocalcemia requiring 30-day readmission occurred in 19 patients (1.4%). In the subset of 755 patients with NSQIP thyroidectomy module data available (2013–2023), any postoperative hypocalcemia was documented in 57 (7.5%; 95% CI, 5.9%–9.7%), of whom 31 (4.1%) experienced a clinically significant event requiring emergency evaluation, intravenous calcium supplementation, or hospital readmission."

### B. If only one metric (simplest fix, SAR-aligned)

> "Hypocalcemia requiring 30-day readmission occurred in 19 of 1,393 total and completion thyroidectomy patients (1.4%)."

### C. If only one metric (Case Details, corrected labels)

> "Postoperative hypocalcemia, as captured by the NSQIP thyroidectomy module, was documented in 57 of 755 patients (7.5%; 95% CI, 5.9%–9.7%) with module data available among 1,075 total and completion thyroidectomy patients."

### D. Recommended limitations language

> "Hypocalcemia ascertainment varied by data source. The NSQIP thyroidectomy module field, which captures any postoperative hypocalcemia (biochemical or symptomatic) within 30 days, was available for 755 of 1,075 total and completion thyroidectomy patients (70.2%); module data were absent for all patients before 2013. The institutional NSQIP Semiannual Report, which tracks only readmissions attributable to hypocalcemia, identified 19 events among 1,393 total thyroidectomies (1.4%). The discrepancy between the module-based rate (7.5%) and the readmission-based rate (1.4%) reflects the inclusion of mild, outpatient-managed hypocalcemia in the module field."

---

## 13. Specific Errors in the Current Manuscript Statement

| Element | Manuscript | Correct | Issue |
|---------|-----------|---------|-------|
| Numerator | 57 | **19** (if readmission-requiring) or 57 (if any postop) | 57 is any postop hypocalcemia, NOT "clinically significant"; SAR-verified readmission count is 19 |
| Denominator | 763 | **1,393** (SAR total Tx) or 755 (patient-level module) | 763 = surgery-level Case Details; must match the numerator's definition |
| Parent cohort | 1,086 | 1,075 patients (Case Details) or 1,393 total Tx (SAR) | 1,086 = surgery-level Case Details count |
| Rate | 7.5% | **1.4%** (readmission) or 7.5% (any postop) | 7.5% is NOT clinically significant; 1.4% is the readmission rate |
| Label | "clinically significant" | Must match definition used | NSQIP module = any hypocalcemia; SAR = readmission-requiring |
| Unit | not stated | patient-level required | Must specify |

---

## 14. Verdict

The manuscript statement "clinically significant hypocalcemia occurred in 7.5% (57/763)" is **NOT manuscript-safe** due to:

1. **Inflated numerator for the stated definition.** "Clinically significant" conventionally means requiring intervention or readmission. The institutional SAR confirms only **19 readmissions for hypocalcemia** across 1,393 total thyroidectomies = **1.4%**. The 57 includes mild/outpatient/biochemical cases that did not require readmission.

2. **Surgery-level denominator.** 763 = surgery-level; 755 = patient-level. Multi-surgery patients inflate the denominator by 8.

3. **Data source mismatch.** The 57 comes from Case Details module data (2013–2023 only, 70% coverage). The SAR's 19 spans the full 2010–2023 period including 11 pre-module-era events the Case Details cannot capture.

4. **The rate is 5× higher than institutional reality.** Surgeons at this institution know 19 readmissions for hypocalcemia in 14 years. Reporting 7.5% as "clinically significant" would be misleading and would not survive peer review scrutiny.

**Bottom line:** Replace 57/763 = 7.5% with either:
- **19/1,393 = 1.4%** (readmission for hypocalcemia, SAR-verified, full cohort)
- **31/755 = 4.1%** (hypocalcemia-related event requiring intervention, Case Details)
- Or report both with explicit definitions per §12A above.
