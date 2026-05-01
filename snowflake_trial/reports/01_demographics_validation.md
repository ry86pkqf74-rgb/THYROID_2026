# Snowflake Cortex Validation — Prompt 1: Demographics + Table 1
**Generated:** 2026-05-01 10:38:25
**Source:** THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER (10,871 patients)

---
## Cohort overview

| N_TOTAL | N_MALIGNANT | N_BENIGN | PCT_MALIGNANT |
| --- | --- | --- | --- |
| 10871 | 4137 | 6734 | 38.1 |

## Age at surgery

| MEAN_AGE | MEDIAN_AGE | MIN_AGE | MAX_AGE | N_NULL |
| --- | --- | --- | --- | --- |
| 51.6 | 52.0 | 5.0 | 93.0 | 0 |

## Sex distribution

| sex | n | pct |
| --- | --- | --- |
| female | 8459 | 77.8 |
| male | 2412 | 22.2 |

## Race distribution

| race | n | pct |
| --- | --- | --- |
| White | 5266 | 48.4 |
| Black or African American | 4168 | 38.3 |
| Unknown or Not Reported | 721 | 6.6 |
| Asian | 476 | 4.4 |
| Other | 143 | 1.3 |
| American Indian or Alaska Native | 39 | 0.4 |
| Native Hawaiian or Other Pacific Islander | 27 | 0.2 |
| Hispanic or Latino | 22 | 0.2 |
| None | 9 | 0.1 |

## AJCC 8 stage group (malignant only)

| stage | n |
| --- | --- |
| I | 1633 |
| II | 1727 |
| III | 9 |
| IVB | 759 |
| None | 9 |

## Histology — top 10 (raw)

| histology | n |
| --- | --- |
| None | 6734 |
| PTC | 3075 |
| follicular carcinoma | 486 |
| MTC | 149 |
| metastatic PTC | 144 |
| NIFTP | 117 |
| poorly differentiated thyroid carcinoma | 37 |
| FTUMP | 34 |
| anaplastic carcinoma | 22 |
| differentiated high grade thyroid carcinoma | 10 |

## AI_CLASSIFY: histology standardization (50 unique samples, llama3.1-70b)

| raw_histology | classified_label |
| --- | --- |
| Atypical hurthle cell neoplasm | NIFTP |
| FTUMP | NIFTP |
| MTC | Medullary thyroid carcinoma |
| MTC/PTC mixed composite | Other malignant |
| NIFTP | NIFTP |
| NUT carcinoma | Other malignant |
| PTC | Papillary thyroid carcinoma (classic/conventional) |
| adenoid cystic carcinoma | Other malignant |
| anaplastic carcinoma | Anaplastic thyroid carcinoma |
| angiosarcoma of the thyroid | Other malignant |
| atypical follicular adenoma | Benign |
| differentiated high grade thyroid carcinoma | Poorly differentiated thyroid carcinoma |
| differentiated thyroid carcinoma | Poorly differentiated thyroid carcinoma |
| follicular adenoma | Benign |
| follicular carcinoma | Follicular thyroid carcinoma |
| high grade carcinoma with focal squamous features | Poorly differentiated thyroid carcinoma |
| high-grade PTC with thymic-like features | Papillary thyroid carcinoma — other variant |
| infiltrating carcinoma with thymus-like differentiation | Other malignant |
| metastatic MTC | Medullary thyroid carcinoma |
| metastatic PTC | ? |
| metastatic PTC classical | ? |
| metastatic PTC classical with extensive follicular growth pattern & oncocytic & focal tall cell features <5% | Papillary thyroid carcinoma (classic/conventional) |
| metastatic PTC classical with focal tall cell features | Papillary thyroid carcinoma — tall cell variant |
| metastatic PTC follicular | ? |
| metastatic PTC tall cell variant | Papillary thyroid carcinoma — tall cell variant |
| metastatic PTC with focal tall cell features | Papillary thyroid carcinoma — tall cell variant |
| metastatic PTC with tall cell features | Papillary thyroid carcinoma — tall cell variant |
| metastatic PTC/anaplastic carcinoma | Anaplastic thyroid carcinoma |
| metastatic anaplastic carcinoma | Anaplastic thyroid carcinoma |
| metastatic follicular carcinoma | Follicular thyroid carcinoma |
| metastatic thyroid carcinoma | Other malignant |
| metastatic thyroid carcinoma with hurthle cell and paillary features | Hurthle cell / oncocytic carcinoma |
| poorly differentiated PTC | Poorly differentiated thyroid carcinoma |
| poorly differentiated carcinoma with neuroendocrine differntiation | Poorly differentiated thyroid carcinoma |
| poorly differentiated thyroid carcinoma | Poorly differentiated thyroid carcinoma |
| recurrent MTC | Medullary thyroid carcinoma |
| recurrent/metastatic PTC | ? |
| recurrent/metastatic follicular carcinoma | Follicular thyroid carcinoma |
| None | None |

*39 rows classified via Cortex AI.*

## AI_FILTER: implausible age (100-row sample)

- Sample size: 100
- Flagged implausible: 1

| rid | age |
| --- | --- |
| 1568 | 17 |

