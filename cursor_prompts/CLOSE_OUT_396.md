# Script 396 — Close-out

- **Git commit / SHA:** `(after git commit)` — a blob cannot embed its own full SHA; resolve with `git rev-parse v1_0-dtc-null-n-stage-groups-filled-20260423_024412` once tagged.
- **Tag:** `v1_0-dtc-null-n-stage-groups-filled-20260423_024412`
- **UTC timestamp:** 2026-04-23T02:44:18.498441+00:00
- **Probe SHA256 (consumed):** `676107dafb6795fcdfd63bbcf2a75dcef4ce868c1065ddb015e51bca476b68c1`
- **Snapshot FQN:** `thyroid_canonical_publication_v1_0.archive_pub_v1_0.cpm_pre_dtc_null_n_stage_group_fill_20260423_024412`

## Halt-gate verdicts (Phase 0)

| gate | verdict |
|---|---|
| H1 | PASS (4) |
| H2 | PASS |
| H3 | PASS |
| H4 | PASS |
| H5 | PASS |
| H6 | PASS |
| H7 | PASS |

## Writes — stage_group before → after

| research_id | before | after |
|---:|---|---|
| 2480 | None | I |
| 2837 | None | I |
| 4245 | None | II |
| 6772 | None | II |

## Disagreement log

- rid_4245_t_disagreement: T3b_primary_vs_T3a_v2_both_yield_II
- rid_6772_m_disagreement: M1_primary_vs_M0_v2_path_stage_II_and_dominant_M1_corroborate_M1

## DTC completeness

- Before: 3736/3742 (99.8397%)
- After: 3740/3742 (99.9466%)

## Remaining NULL DTC rows

research_ids **1404** and **12198** — confirmed in `manuscript_workspace.cpm_stage_group_manual_review_v1` (AJCC-edition adjudication).

## CF-396 followups (proposed)

- **CF-396-1:** rid 4245 T3b/T3a primary-vs-v2 — root-cause in 240-builder's T-column source precedence.
- **CF-396-2:** rid 6772 M1/M0 primary-vs-v2 — root-cause in M-column builder (primary M1 correct per path_raw + dominant; v2 M0 anomalous).
- **CF-396-3:** 240-builder COALESCE fallback from `ajcc8_t_stage_v2` when `ajcc8_t_stage IS NULL` (structural fix deferred).

## Phase 3 verification

- **all_pass:** True
