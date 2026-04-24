# Migration 54 / 55 — ETE subgrade tier-2 canonical close-out

**Date:** 2026-04-24  
**Database:** `thyroid_canonical_publication_v1_0`  
**SQL artifact:** `qc_framework_v1/migrations/55_ete_subgrade_canonical_tier2_v1.sql` (tier-2 canonical layer; complements `54_ete_subgrade_fresh_llm_layer.sql` manuscript views)  
**Build tag in tables:** `mig_54_ete_subgrade_20260424`

## Final commit SHA

`36e6446` (main)

## Source

- `main.note_entities_llm_ete_subgrade_v1` — 287 rows, `error=0`, `parsed_json` JSON (gpt-oss-120b run).

## Delivered objects

| Object | Role |
|--------|------|
| `main.v_note_entities_llm_ete_subgrade_v1` | Flattener view over loader `parsed_json` |
| `main.canonical_ete_subgrade_events_v1` | One row per note entity (`ete_event_id` = `note_row_id`) |
| `main.canonical_ete_subgrade_patient_rollup_v1` | One row per `research_id` |
| `manuscript_workspace.detail_table_registry_v1` | Rows for the three objects above; `ete_adjudication_v1` description updated (curated 45-case layer noted; document-scale grade uses canonical tables) |

## Counts (verified 2026-04-24)

| Metric | Value |
|--------|--------|
| Events | 287 |
| Distinct patients (rollup) | 151 |
| Event `ete_grade` | gross=142, unknown=92, microscopic=40, absent=13 |
| Rollup `worst_ete_grade` | gross=96, microscopic=35, unknown=16, absent=4 |
| Patients with `any_gross_ete` | 96 |
| Rollup `worst_ajcc8_implication` (non-null order) | pT3b=54, pT4a=39, pT4b=3, pT3a_size_only=4; null=51 |

## 167 PTC `unspec_remaining` cohort (Migration 53 target)

- Cohort size (`ete_manuscript_analytic_v2`: `cohort_ptc` AND `analytic_eligible` AND `ete_grade_final='unspec_remaining'`): **167**
- All **151** patients with a tier-2 rollup row are inside that 167 (loader coverage **151/167**; **16** cohort patients have no `note_entities_llm_ete_subgrade_v1` rows).
- Cohort patients with `worst_ete_grade IN ('gross','microscopic','absent')`: **135 / 167** (of covered patients: **135 / 151** have decisive worst; **16 / 151** remain worst=`unknown`).

## Carry-forward

- **`ete_adjudication_v1`:** Retain for the 45 adjudicated cases; ETE *grade from clinical notes* for manuscript scale is **`canonical_ete_subgrade_*`**.
- **`canonical_invasion_patient_rollup_v1` / `canonical_invasion_events_v1`:** Re-source `any_gross_ete_*` / `any_microscopic_ete_*` (and related ETE subgrade fields) from `canonical_ete_subgrade_patient_rollup_v1` in a future migration (Script 363+), per project plan.
- **Registry:** `detail_table_name` is the key; new canonical tables are registered; optional manuscript views from `54_ete_subgrade_fresh_llm_layer.sql` are not in MotherDuck on this run — apply that file separately if `ete_manuscript_analytic_v3` is still required.
