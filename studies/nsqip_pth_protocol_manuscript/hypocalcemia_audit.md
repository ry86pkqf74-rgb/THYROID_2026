# Hypocalcemia Rate Audit — NSQIP/PTH Protocol Manuscript

**Auditor:** Automated pipeline audit  
**Date:** 2026-03-12  
**Target manuscript:** Institutional NSQIP/PTH protocol for *The American Surgeon*  
**Manuscript claim under audit:** "clinically significant hypocalcemia occurred in 7.5% (57/763) among institutional total/completion thyroidectomies"

---

## 1. Executive Summary

| Question | Answer |
|----------|--------|
| Is 57/763 = 7.5% reproducible? | **YES** — at the surgery-level analysis |
| Is the denominator correct? | **NO** — 763 is surgery-level; patient-level is 755 |
| Is the numerator correct? | **YES** — 57 unique patients, no double-counting |
| Is the parent cohort 1,086? | **Partially** — 1,086 = surgery-level cases; 1,075 = unique patients |
| Is "clinically significant" the right label? | **NO** — the NSQIP field captures *any* postoperative hypocalcemia, not just clinically significant |
| Is the rate overestimated? | **DEPENDS ON DEFINITION** — see §7 below |
| Is the metric manuscript-safe? | **NO as stated** — requires correction of denominator unit, clinical label, and parent cohort count |

**Recommended replacement rate:** 57/755 = 7.5% (patient-level, any NSQIP-captured postoperative hypocalcemia)

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

## 11. Manuscript-Safe Recommendations

### A. If 57/755 is accepted (correct patient-level rate, any NSQIP hypocalcemia)

> "Postoperative hypocalcemia, as captured by the NSQIP thyroidectomy module, occurred in 57 of 755 patients (7.5%; 95% CI, 5.9%–9.7%) with module data available among 1,075 total and completion thyroidectomy patients."

### B. If the intent is truly "clinically significant" (requiring intervention)

> "Clinically significant hypocalcemia, defined as a hypocalcemia-related event requiring emergency evaluation, intravenous calcium, or readmission, occurred in 31 of 755 patients (4.1%; 95% CI, 2.9%–5.8%) with module data available."

### C. Recommended limitations language

> "The NSQIP thyroidectomy module fields, including postoperative hypocalcemia and calcium/vitamin D replacement, were available for 755 of 1,075 total and completion thyroidectomy patients (70.2%). Module data were absent for all patients operated before 2013 (pre-module era, n=259) and partially available during the 2013 rollout year. Consequently, hypocalcemia rates represent an available-case analysis restricted to the 2013–2023 subset with non-missing module data and may not reflect the experience of the full cohort."

---

## 12. Specific Errors in the Current Manuscript Statement

| Element | Manuscript | Correct | Issue |
|---------|-----------|---------|-------|
| Parent cohort | 1,086 | 1,075 patients | 1,086 = surgery cases (includes 11 double-counted patients) |
| Denominator | 763 | 755 | 763 = surgery-level; 755 = patient-level |
| Numerator | 57 | 57 | Correct (same either way) |
| Rate | 7.5% | 7.5% | Numerically identical after rounding |
| Label | "clinically significant" | "postoperative" | NSQIP field is any hypocalcemia, not severity-gated |
| Unit | not stated | patient-level | Must specify patient-level vs surgery-level |

---

## 13. Verdict

The rate 57/763 = 7.5% is **reproducible** but **not manuscript-safe as stated** due to:

1. **Surgery-level denominator** (763 cases vs 755 patients)
2. **Mislabeled clinical severity** ("clinically significant" vs "any postoperative")
3. **Parent cohort miscounted** (1,086 cases vs 1,075 patients)

The numerator (57) and the practical rate (7.5%) are both correct at the patient level. The fix is primarily **labeling and denominator reporting**, not a change in the actual hypocalcemia count.
