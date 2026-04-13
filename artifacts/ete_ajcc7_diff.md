# AJCC7 before/after diff (T-stage only)

**Source:** `exports/ptc_full.csv` (N = 2,844)

**Mapping:** stale `T3b -> T4a` vs canonical `T3b -> T3`.

**Rows with T-stage change:** 276

## Counts — stale (before)

|         |   n |
|:--------|----:|
| T1a     | 645 |
| T1b     | 592 |
| T2      | 515 |
| T3      | 576 |
| T4a     | 498 |
| Unknown |  18 |


## Counts — canonical (after)

| t_stage_ajcc7   |   n |
|:----------------|----:|
| T1a             | 645 |
| T1b             | 592 |
| T2              | 515 |
| T3              | 852 |
| T4a             | 222 |
| Unknown         |  18 |


## Crosswalk (rows = stale, cols = canonical)

| row_0   |   T1a |   T1b |   T2 |   T3 |   T4a |   Unknown |
|:--------|------:|------:|-----:|-----:|------:|----------:|
| T1a     |   645 |     0 |    0 |    0 |     0 |         0 |
| T1b     |     0 |   592 |    0 |    0 |     0 |         0 |
| T2      |     0 |     0 |  515 |    0 |     0 |         0 |
| T3      |     0 |     0 |    0 |  576 |     0 |         0 |
| T4a     |     0 |     0 |    0 |  276 |   222 |         0 |
| Unknown |     0 |     0 |    0 |    0 |     0 |        18 |


## Unit-impact check

Audit report (`studies/proposal2_ete_staging/audit_report.md`) states 346 T-stage reclassifications under the T3b->T3 correction. Observed here from ptc_full.csv (tumor-1 ETE field only): **276** changes. Any shortfall versus 346 is expected: ptc_full.csv is tumor-1-centric, while the audit operates on the wider cohort with worst-ETE aggregation.
