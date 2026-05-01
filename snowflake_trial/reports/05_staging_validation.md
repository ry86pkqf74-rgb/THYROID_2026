# Snowflake Cortex Validation — Prompt 5: AJCC 8 Staging Consistency
**Generated:** 2026-05-01 10:47:39
**Source:** CANONICAL_PATIENT_MASTER_FLAT (malignant subset)

---
## Top T/N/M × stage_group combinations

| AJCC8_T_STAGE | AJCC8_N_STAGE | AJCC8_M_STAGE | AJCC8_STAGE_GROUP | N |
| --- | --- | --- | --- | --- |
| T3b | N1a | M0 | I | 260 |
| T1a | N0 | M0 | I | 243 |
| T3b | N1a | M1 | II | 240 |
| T3b | N1a | M0 | II | 213 |
| T1a | N1a | M0 | I | 187 |
| T3b | N1a | M1 | IVB | 176 |
| T1b | N0 | M0 | I | 158 |
| T1a | N1a | M0 | II | 158 |
| T1b | N1a | M0 | I | 151 |
| T1b | N1a | M1 | II | 144 |
| T2 | N1a | M1 | II | 137 |
| T2 | N1a | M0 | I | 131 |
| T1a | N1a | M1 | II | 130 |
| T2 | N0 | M0 | I | 111 |
| T3b | N0 | M0 | I | 98 |
| T1a | N1a | M1 | IVB | 91 |
| T2 | N1a | M1 | IVB | 86 |
| T1b | N1a | M1 | IVB | 85 |
| T3b | N0 | M1 | II | 82 |
| T2 | N1a | M0 | II | 80 |
| T3b | N0 | M0 | II | 79 |
| T1b | N1a | M0 | II | 78 |
| T1a | N0 | M1 | II | 71 |
| T1a | N0 | M1 | IVB | 62 |
| T1b | N0 | M1 | II | 62 |

## Rule probe: M1 patients should all be Stage IVB

| AJCC8_STAGE_GROUP | N |
| --- | --- |
| II | 1058 |
| IVB | 759 |
|  | 1 |

## Rule probe: PTC + age <55 should be Stage I or II only

| AJCC8_STAGE_GROUP | N |
| --- | --- |
| I | 1023 |
| II | 837 |

## AI_COMPLETE staging audit (100-pt sample, llama3.1-8b)

- **CONSISTENT:** 1
- **INCONSISTENT:** 36
- **UNCERTAIN:** 0
- **Other:** 63

### Patients flagged INCONSISTENT

| rid | T | N | M | stage | age | histology | size | ete | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 6178 | T1b | N1a | M1 | IVB | 64.0 | PTC | 1.3 | microscopic | INCONSISTENT |
| 9021 | T1a | N1a | M1 | II | 40.0 | PTC | 0.8 | microscopic | INCONSISTENT |
| 6266 | T3b | N0 | M1 | IVB | 68.0 | PTC | 4.7 | gross | INCONSISTENT |
| 4554 | T3b | N1a | M0 | II | 75.0 | PTC | 4.5 | gross | INCONSISTENT |
| 8627 | T1b | N1a | M1 | II | 49.0 | PTC | 1.1 | microscopic | INCONSISTENT |
| 531 | T1a | N1a | M0 | I | 47.0 | metastatic PTC | 0.2 | none | INCONSISTENT |
| 8880 | T2 | N1a | M1 | IVB | 71.0 | follicular carcinoma | 2.8 | microscopic | INCONSISTENT |
| 5655 | T1a | N1a | M1 | II | 21.0 | MTC | 0.1 | microscopic | INCONSISTENT |
| 3352 | T1a | N1a | M0 | II | 76.0 | PTC | 0.1 | microscopic | INCONSISTENT |
| 8293 | T3a | N1a | M1 | IVB | 66.0 | PTC | 5 | microscopic | INCONSISTENT |
| 9632 | T2 | N1a | M1 | II | 41.0 | follicular carcinoma | 2.9 | microscopic | INCONSISTENT |
| 1632 | T1a | N1a | M0 | II | 62.0 | metastatic PTC | 0.3 | microscopic | INCONSISTENT |
| 6314 | T3b | N1a | M1 | IVB | 63.0 | follicular carcinoma | 7.1 | gross | INCONSISTENT |
| 8017 | T1b | N1a | M1 | IVB | 60.0 | PTC | 1.2 | microscopic | INCONSISTENT |
| 9165 | T1a | N0 | M1 | IVB | 71.0 | PTC | 0.2 | microscopic | INCONSISTENT |
| 7107 | T1a | N0 | M1 | IVB | 61.0 | metastatic PTC | 1 | none | INCONSISTENT |
| 8105 | T1b | N1a | M1 | II | 35.0 | metastatic PTC | 2.6 | microscopic | INCONSISTENT |
| 11793 | T1b | N0 | M1 | II | 46.0 | MTC | 1.5 | microscopic | INCONSISTENT |
| 8716 | T3a | N0 | M1 | IVB | 70.0 | PTC | 5.1 | microscopic | INCONSISTENT |
| 5377 | T1a | N1a | M0 | II | 66.0 | PTC | 0.1 | microscopic | INCONSISTENT |

