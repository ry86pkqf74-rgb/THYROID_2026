# N1 — SF AI_CLASSIFY Smoking Pilot
**Generated:** 2026-05-04T03:11:35.771571Z
**Cohort:** 100 random smoking-keyword-positive notes from `main.clinical_notes_long`
**Pipeline:** MD export → SF PUT/COPY → AI_CLASSIFY → results table
**Elapsed (AI_CLASSIFY only):** 3.5s

## Yield

| Total | Actionable (never/former/current) | Unknown | NULL |
|---:|---:|---:|---:|
| 100 | 93 (93.0%) | 7 (7.0%) | 0 |

## Distribution

| smoking_status | n |
|---|---:|
| never_smoker | 64 |
| former_smoker | 19 |
| current_smoker | 10 |
| unknown_or_not_mentioned | 7 |

## Tables created in Snowflake

- `THYROID_VALIDATION.PUBLIC.NLP_SMOKING_PILOT_NOTES` (100 rows; PHI text — keep in SF)
- `THYROID_VALIDATION.PUBLIC.NLP_SMOKING_PILOT_RESULTS_v1` (research_id + classification only)

## Decision gate

- **Actionable yield 93.0%**: 
  - >= 70% → green-light full smoking refresh on all 3,541 smoking-keyword notes
  - 40-70% → tune prompt (more granular categories, packing pack-year buckets)
  - <40% → re-scope; AI_CLASSIFY may not be the right primitive (try AI_COMPLETE structured-extraction)

PHI policy: no note text written to this report or any committed file.