# Snowflake Cortex Validation — Prompt 6: ETE / Vascular Invasion
**Generated:** 2026-05-01 11:28:46
**Sources:** CANONICAL_PATIENT_MASTER_FLAT + CANONICAL_INVASION_EVENTS_V1_FLAT

---
## ETE grade × malignancy

| ETE_GRADE | N | N_MALIGNANT |
| --- | --- | --- |
|  | 6752 | 64 |
| microscopic | 2580 | 2578 |
| gross | 1313 | 1269 |
| none | 179 | 179 |
| present_ungraded | 29 | 29 |
| absent | 16 | 16 |
| true | 2 | 2 |

## Invasion event schema

20 columns: `INVASION_EVENT_ID`, `RESEARCH_ID`, `INVASION_TYPE`, `FINDING_STATUS`, `SOURCE_MODALITY`, `SOURCE_KIND`, `SOURCE_TABLE`, `SOURCE_ROW_ID`, `FINDING_DATE`, `LINKED_SURGERY_EPISODE_ID`, `LINKED_PATH_MALIGNANT_EVENT_ID`, `LINKAGE_METHOD`, `N_CANDIDATE_EPISODES`, `LINKAGE_AMBIGUOUS_MULTI_FINDING`, `CONFIDENCE`, `EVIDENCE_SPAN_HASH`, `EVIDENCE_QUALIFIER`, `EXTRACTION_RUN_ID`, `BUILD_SCRIPT`, `BUILD_TS`

## Invasion event types (`INVASION_TYPE`)

| INVASION_TYPE | N | N_PTS |
| --- | --- | --- |
| vascular_microscopic | 15066 | 4203 |
| esophageal | 11773 | 10871 |
| lymphatic_microscopic | 10088 | 3541 |
| gross_ete | 7486 | 4065 |
| soft_tissue | 4802 | 3150 |
| capsular | 4270 | 2195 |
| perineural | 4083 | 1626 |
| microscopic_ete | 457 | 282 |
| ete_present_not_further_specified | 415 | 291 |
| airway | 128 | 82 |
| tracheal | 14 | 14 |

## ETE-grade vs invasion-event concordance

| ETE_GRADE | N_PTS | N_WITH_INVASION_EVENT | PCT_WITH_EVENT |
| --- | --- | --- | --- |
| absent | 16 | 16 | 100.0 |
| gross | 1269 | 1269 | 100.0 |
| microscopic | 2578 | 2578 | 100.0 |
| none | 179 | 179 | 100.0 |
| present_ungraded | 29 | 29 | 100.0 |
| true | 2 | 2 | 100.0 |
|  | 64 | 64 | 100.0 |

## AI_CLASSIFY: T-stage × ETE consistency (50-pt sample)

- **Consistent:** 46
- **Size-stage mismatch:** 2
- **Insufficient data:** 2

### Flagged cases

| rid | t_stage | ete | size_cm | verdict |
| --- | --- | --- | --- | --- |
| 924 | T3b | gross | 0.7 | Size-stage mismatch |
| 337 | T2 | none |  | Insufficient data |
| 861 | T3a | none |  | Insufficient data |
| 596 | T3b | gross | 1 | Size-stage mismatch |

