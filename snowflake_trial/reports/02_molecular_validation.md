# Snowflake Cortex Validation — Prompt 2: Molecular Testing Audit
**Generated:** 2026-05-01 10:46:53
**Source:** CANONICAL_PATIENT_MASTER_FLAT + CANONICAL_MOLECULAR_GENETICS_V2_FLAT

---
## Molecular testing rate by era

| ERA | N | N_TESTED | PCT_TESTED |
| --- | --- | --- | --- |
| 2015-2019 | 2948 | 375 | 12.7 |
| 2020+ | 3935 | 868 | 22.1 |
| <2015 | 3988 | 43 | 1.1 |

## Mutation positivity (among tested)

| N_TESTED | N_BRAF_POS | N_RAS_POS | N_BOTH_BRAF_RAS |
| --- | --- | --- | --- |
| 1286 | 376 | 292 | 49 |

## Molecular platforms (among tested)

| MOL_PLATFORM | N |
| --- | --- |
| ThyroSeq | 618 |
| unknown | 316 |
| Afirma | 188 |
| ThyroSeq+Afirma | 163 |
| Quest | 1 |

## Internal contradictions: tested=FALSE but BRAF or RAS positive

**N flagged:** 0

## AI_CLASSIFY: era bucket for sample of BRAF+ patients (n=30)

| research_id | surgery_date | AI_era_bucket |
| --- | --- | --- |
| 5743 | 2017-01-12 | 2015-2019 |
| 10486 | 2024-09-19 | 2020-2024 |
| 8017 | 2020-02-21 | 2020-2024 |
| 5174 | 2015-12-29 | 2015-2019 |
| 10787 | 2023-10-27 | 2020-2024 |
| 8105 | 2020-05-11 | 2020-2024 |
| 5902 | 2017-03-30 | 2015-2019 |
| 7353 | 2019-05-23 | 2015-2019 |
| 5377 | 2016-05-10 | 2015-2019 |
| 9020 | 2021-08-03 | 2020-2024 |
| 10138 | 2023-06-29 | 2020-2024 |
| 9071 | 2021-08-30 | 2020-2024 |
| 10814 | 2023-06-06 | 2020-2024 |
| 6057 | 2017-06-14 | 2015-2019 |
| 10049 | 2023-07-27 | 2020-2024 |

## Detail-table concordance (molecular_genetics_v2 vs CPM rollup)

| N_WITH_DETAIL | N_MASTER_SAYS_TESTED | N_MASTER_SAYS_NOT_TESTED |
| --- | --- | --- |
| 1151 | 1055 | 96 |

