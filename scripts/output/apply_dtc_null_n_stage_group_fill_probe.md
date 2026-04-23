# Script 396 — Phase 0 probe (DTC NULL-N stage_group fill)

## Cohort summary

| metric | value |
|---|---:|
| dtc_total | 3742 |
| dtc_null_stage_group | 6 |
| H1 scope (NULL N in write set, NULL stage_group) | 4 |
| H3 CPM total | 10871 |
| H6 convention precedent (age>=55 M0 T1-T2 N NULL stage I) | 59 |
| H7 snapshot prefix table count | 0 |

## Halt gates (H1–H7)

| all_pass | True |

## Per-row current state

| research_id | age | t_prim | t_v2 | n_prim | n_v2 | m_prim | m_v2 | m_dom | stage_group | corrected | path_raw | planned_write | rationale |
|---:|---:|---|---|---|---|---|---|---|---|---|---|---|
| 2480 | 63 | NULL | T1 | NULL | Nx | M0 | M0 | M0 | NULL | NULL | NULL | I | derive_t_v2_nx_convention |
| 2837 | 15 | NULL | T1a | NULL | Nx | M0 | M0 | M0 | NULL | I | I | I | builder_sync_age_lt_55_m0 |
| 4245 | 69 | T3b | T3a | NULL | Nx | M0 | M0 | M0 | NULL | II | NULL | II | builder_sync_t3a_t3b_both_yield_ii |
| 6772 | 49 | NULL | T3a | NULL | Nx | M1 | M0 | M1 | NULL | II | II | II | m1_primary_path_corroborated |

## Planned UPDATEs (stage_group only)

| research_id | SET ajcc8_stage_group | rationale_code |
|---|---|---|
| 2480 | I | derive_t_v2_nx_convention |
| 2837 | I | builder_sync_age_lt_55_m0 |
| 4245 | II | builder_sync_t3a_t3b_both_yield_ii |
| 6772 | II | m1_primary_path_corroborated |

## Projected completeness

DTC NULL stage_group: 6 → 2 (remaining 1404, 12198 in manual review).
DTC completeness: 3736/3742 (99.8397%) → 3740/3742 (99.9466%) if denominators unchanged.

---HASH-BOUNDARY---

## Generation footer (excluded from PROBE_REPORT_SHA256)

Written UTC: 2026-04-23T02:44:12.852997+00:00
