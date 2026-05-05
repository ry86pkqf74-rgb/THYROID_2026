# Snowflake Cortex Validation — Prompt 7: TIRADS / Bethesda Diagnostic Accuracy
**Generated:** 2026-05-04 22:07:40
**Sources:** CANONICAL_PATIENT_MASTER_FLAT (Bethesda / malignancy) + CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT (MAX_TIRADS_CATEGORY_EVER)

---
## Bethesda category × Risk of Malignancy (ROM)

**Expected ranges (Bethesda 2023):** I 5-10%, II 0-3%, III 6-18%, IV 10-40%, V 45-60%, VI 94-96%

| BETHESDA | N | N_MALIGNANT | ROM_PCT |
| --- | --- | --- | --- |
| 1.0 | 233 | 69 | 29.6 |
| 2.0 | 2032 | 360 | 17.7 |
| 3.0 | 642 | 304 | 47.4 |
| 4.0 | 625 | 298 | 47.7 |
| 5.0 | 273 | 240 | 87.9 |
| 6.0 | 1221 | 1062 | 87.0 |

## TIRADS × malignancy

**ACR TI-RADS expected:** TR1 ~0%, TR2 <2%, TR3 <5%, TR4 5-20%, TR5 >20%

| TIRADS | N | N_MALIGNANT | RATE_PCT |
| --- | --- | --- | --- |
| TR1 | 346 | 96 | 27.7 |
| TR2 | 300 | 97 | 32.3 |
| TR3 | 852 | 237 | 27.8 |
| TR4 | 495 | 235 | 47.5 |
| TR5 | 1403 | 824 | 58.7 |

## Bethesda VI patients with benign final histology

**N flagged:** 159

| RESEARCH_ID | BETHESDA | IS_MALIGNANT | HISTOLOGY_FINAL |
| --- | --- | --- | --- |
| 3289 | 6.0 | False |  |
| 637 | 6.0 | False |  |
| 6563 | 6.0 | False |  |
| 5682 | 6.0 | False |  |
| 5255 | 6.0 | False |  |
| 6197 | 6.0 | False |  |
| 6285 | 6.0 | False |  |
| 5933 | 6.0 | False |  |
| 4628 | 6.0 | False |  |
| 5719 | 6.0 | False |  |
| 6086 | 6.0 | False |  |
| 4517 | 6.0 | False |  |
| 4537 | 6.0 | False |  |
| 4600 | 6.0 | False |  |
| 4713 | 6.0 | False |  |

## Bethesda II patients with malignant final histology

**N flagged:** 360

| RESEARCH_ID | BETHESDA | IS_MALIGNANT | HISTOLOGY_FINAL |
| --- | --- | --- | --- |
| 1889 | 2.0 | True | PTC |
| 768 | 2.0 | True | PTC |
| 1460 | 2.0 | True | PTC |
| 1779 | 2.0 | True | PTC |
| 2159 | 2.0 | True | PTC |
| 712 | 2.0 | True | PTC |
| 788 | 2.0 | True | PTC |
| 1941 | 2.0 | True | PTC |
| 2045 | 2.0 | True | PTC |
| 6270 | 2.0 | True | PTC |
| 6713 | 2.0 | True | PTC |
| 4145 | 2.0 | True | PTC |
| 1682 | 2.0 | True | PTC |
| 2012 | 2.0 | True | PTC |
| 6723 | 2.0 | True | PTC |

## AI_CLASSIFY: TIRADS observed vs ACR expected

| tirads | n | observed_rate_pct | ai_verdict |
| --- | --- | --- | --- |
| TR1 | 346 | 27.7 | Within ACR expected range |
| TR2 | 300 | 32.3 | Within ACR expected range |
| TR3 | 852 | 27.8 | Within ACR expected range |
| TR4 | 495 | 47.5 | Within ACR expected range |
| TR5 | 1403 | 58.7 | Within ACR expected range |

