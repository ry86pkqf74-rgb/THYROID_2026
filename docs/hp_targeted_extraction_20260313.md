# Targeted H&P Extraction Report: Smoking Status + BMI

**Date**: 2026-03-13 09:51
**Source notes**: h_p + endocrine_note from clinical_notes_long

---

## Extraction Summary

| Variable | Mentions | Patients | Conflicting | Review Queue |
|----------|----------|----------|-------------|--------------|
| smoking_status | 3,241 | 2,775 | 274 | 622 |
| pack_years | 83 | 79 | 0 | 0 |
| packs_per_day | 111 | 97 | 0 | 0 |
| bmi_value | 2,409 | 2,116 | 0 | 3 |
| bmi_category | 166 | 156 | 0 | 0 |

---

## Smoking Status Distribution

| Status | Patients | % |
|--------|----------|---|
| current_smoker | 151 | 5.4% |
| former_smoker | 528 | 19.0% |
| never_smoker | 2,092 | 75.4% |
| passive_exposure | 0 | 0.0% |
| unknown | 4 | 0.1% |

**Pack-years extracted**: 79 patients
**PPD extracted**: 97 patients

---

## BMI Statistics

| Metric | Value |
|--------|-------|
| mean | 31.0 |
| median | 30.0 |
| min | 16.2 |
| max | 72.1 |
| std | 7.8 |

**BMI patients**: 2,195 (numeric: 2,116, category-only: 79)
**Outlier BMI (< 15 or > 65)**: 2 patients

---

## BMI Category Distribution

| Category | Patients |
|----------|----------|
| obese | 82 |
| normal_weight | 47 |
| overweight | 26 |
| underweight | 1 |

---

## Precision Review

### Smoking Status — Random Sample (n=50)

Evidence spans are truncated to 80 chars for PHI safety.

| research_id | norm | evidence (truncated) | confidence |
|-------------|------|----------------------|------------|
| 10116 | never_smoker | nal History • Not on file Tobacco Use • Smoking status: Never • Smokeless tobacc | 0.90 |
| 8737 | never_smoker | Tobacco Use / Exposure              Never smoker, No                        Immu | 0.90 |
| 10599 | former_smoker | ndmother  Social History  Tobacco Use • Smoking status: Former • Smokeless tobac | 0.90 |
| 7506 | never_smoker | posure     -     11/06/2019             Never smoker, No                    Trav | 0.90 |
| 11381 | former_smoker | History Social History    Tobacco Use • Smoking status: Former   Packs/day: 0.50 | 0.90 |
| 8442 | never_smoker | Tobacco Use / Exposure             Never smoker, No                    Travel | 0.90 |
| 8815 | never_smoker | Recreational Drug Use              No                    Tobacco Use / Exposure  | 0.90 |
| 8644 | never_smoker | Tobacco Use / Exposure             Never smoker, No                        Famil | 0.90 |
| 8778 | never_smoker | Recreational Drug Use         No          Tobacco Use / Exposure         Never s | 0.90 |
| 9798 | never_smoker | a        Tobacco Use / Exposure         Never smoker, No        Travel         Y | 0.90 |
| 7374 | former_smoker | posure     -     07/17/2019             Former smoker, No                        | 0.90 |
| 11480 | never_smoker | rgies: none Medications: none SocialHx: No smoking, alcohol or drug use.    REVI | 0.90 |
| 8842 | never_smoker | Recreational Drug Use             No                    Tobacco Use / Exposure   | 0.90 |
| 9250 | current_smoker | Tobacco Use / Exposure              Current every day smoker, Yes, Cigarettes, 4 | 0.90 |
| 7495 | never_smoker | Non-medical: Not on file Tobacco Use • Smoking status: Never Smoker • Smokeless  | 0.90 |
| 8181 | never_smoker | Tobacco Use / Exposure             Never smoker, No                        Famil | 0.90 |
| 9501 | never_smoker | Recreational Drug Use              No                    Tobacco Use / Exposure  | 0.90 |
| 7944 | never_smoker | posure     -     09/05/2019             Never smoker, No                    Trav | 0.90 |
| 7713 | former_smoker | ture repairs (GMC, 2013)            SH: Former smoker, quit 17y ago. Denies alco | 0.90 |
| 8395 | never_smoker | Vaping / eCigarettes             E-Cigarette Use: Never.                         | 0.90 |

*Showing 20 of 50 sampled rows.*

### BMI — Random Sample (n=50)

| research_id | value | evidence (truncated) | confidence |
|-------------|-------|----------------------|------------|
| 10392 | 38.62 | 204 lb 6.4 oz)  / SpO2 96%  / BMI 38.62 kg/m² General: Alert and orie | 0.95 |
| 10751 | 23.53 | 126 lb 9.6 oz)  / SpO2 99%  / BMI 23.53 kg/m²   General: NAD. Alert a | 0.95 |
| 9595 | 35.8 | WT:     100.6 kg         BMI:     35.8 | 0.95 |
| 11754 | 56 | on is a 52 y.o. male with PMH BMI 56, LE recurrent lymphedema and | 0.95 |
| 8152 | 42.4 | cm      WT:     122.8 kg      BMI:     42.4            General:     Alert | 0.95 |
| 11255 | 26.27 | t 78.4 kg (172 lb 12.8 oz)  / BMI 26.27 kg/m² Physical Exam Constitut | 0.95 |
| 9550 | 30.5 | length   BSA - Form 1.68 m2   Body Mass Index 30.5 kg/m2      General:  No acute | 0.95 |
| 9342 | 36.1 | /length   BSA - Form 2.3 m2   Body Mass Index 36.1 kg/m2      Intake and Output | 0.95 |
| 9229 | 24.5 | 5.3 cm   BSA - Form 1.91 m2   Body Mass Index 24.5 kg/m2  12/06/2021 06:30 EST W | 0.95 |
| 8979 | 30.3 | psoriasis and thyroid nodule, BMI 30.3   o/w healthy COVID NEG HCG N | 0.95 |
| 8981 | 42.3 | WT:     145.45 kg         BMI:     42.3 | 0.95 |
| 9836 | 49.83 | 81 lb 4.8 oz)  / SpO2 100%  / BMI 49.83 kg/m²   Gen: No acute distres | 0.95 |
| 7581 | 43.20 | LMP 05/10/2022 (Approximate) BMI 43.20 kg/m²  Wt Readings from Last | 0.95 |
| 9426 | 28.3 | WT:     72.5 kg         BMI:     28.3                           NAD | 0.95 |
| 7315 | 40.61 | 10")	05/17/2019 3:52 PM EDT	  Body Mass Index	40.61	05/17/2019 3:52 PM EDT	   Pa | 0.95 |
| 7556 | 46.96 | Wt 299 lb 12.8 oz (136 kg) / BMI 46.96 kg/m² GENERAL APPEARANCE: No | 0.95 |
| 7905 | 37.8 | WT:     94.3 kg         BMI:     37.8 | 0.95 |
| 9030 | 23 | WT:     68.5 kg         BMI:     23 | 0.95 |
| 7901 | 32.1 | WT:     62.7 kg         BMI:     32.1 | 0.95 |
| 10434 | 32.39 | t 93.8 kg (206 lb 12.8 oz)  / BMI 32.39 kg/m² General - Alert, age-ap | 0.95 |

*Showing 20 of 50 sampled rows.*

---

## Deliverables

### MotherDuck Tables
1. `extracted_smoking_status_v1` — per-mention smoking extraction
2. `extracted_bmi_v1` — per-mention BMI extraction
3. `patient_smoking_status_summary_v1` — one row per patient
4. `patient_bmi_summary_v1` — one row per patient
5. `review_smoking_ambiguous_v1` — conflicting/unknown cases
6. `review_bmi_outlier_v1` — outlier BMI values for review
7. `val_hp_targeted_extraction_v1` — validation summary

### Export Bundle
`exports/hp_targeted_extraction_20260313_0951/`
