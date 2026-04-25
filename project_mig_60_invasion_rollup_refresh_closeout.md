# Migration 60 - Invasion rollup v2 refresh - close-out

**Date:** 2026-04-24  
**Database:** `thyroid_canonical_publication_v1_0`  
**Git (`main`):** core migration commit `5454cf55cbc0e18dc4b73b6d1c944e4f03971f2a`  
**`build_script` / provenance tag:** `mig_60_invasion_rollup_v2_refresh_20260424`

## Repo artifacts

| Artifact | Role |
| --- | --- |
| `qc_framework_v1/migrations/60_invasion_rollup_v2_refresh.sql` | CREATE OR REPLACE rollup; registry UPSERT; parathyroid glands=5 side-car patch |
| `qa/qa_mig_60_invasion_rollup_refresh.json` | G1-G6 QA evidence and pre/post count deltas |
| `cursor_prompts/CURSOR_PROMPT_MIG_60_INVASION_ROLLUP_REFRESH_20260424.md` | Source prompt / implementation spec |
| `project_mig_60_invasion_rollup_refresh_closeout.md` | This close-out |

## Objects delivered

| Object | Grain / action |
| --- | --- |
| `main.canonical_invasion_patient_rollup_v1` | one row per `research_id`; 10,871 rows; v2 feeder additive OR refresh |
| `manuscript_workspace.detail_table_registry_v1` | single updated row for `canonical_invasion_patient_rollup_v1` |
| `archive_pub_v1_0.detail_table_registry_v1_pre_mig60_20260424` | pre-migration registry snapshot table; created with IF NOT EXISTS for idempotent reruns |
| `main.canonical_parathyroid_events_v1` | side-car contract patch: one glands_identified_count=5 event clamped to 4 |
| `main.canonical_parathyroid_patient_rollup_v1` | side-car rollup cell refreshed for `research_id=9371` |

## Source & overlap summary

| Metric | Value |
| --- | --- |
| current rollup rows before migration | 10,871 |
| current rollup build tag before migration | `363` / `2026-04-21 23:30:02.514209` |
| vascular v2 feeder patients | 3,745 |
| airway v2 feeder patients | 2,820 |
| ETE subgrade feeder patients | 151 |
| T4b invasion feeder patients | 434 |
| feeder patients outside invasion rollup cohort | 0 across all four feeders |

Overlap within the 10,871-patient invasion rollup cohort:

| total | in_vascular_v2 | in_airway_v2 | in_ete_subgrade | in_t4b_invasion |
| --- | --- | --- | --- | --- |
| 10871 | 3745 | 2820 | 151 | 434 |

## Baseline vs post

| Flag | Pre | Post | Delta |
| --- | --- | --- | --- |
| gross_ete | 1146 | 1146 | 0 |
| microscopic_ete | 279 | 318 | 39 |
| vascular_microscopic | 1109 | 1114 | 5 |
| lymphatic_microscopic | 780 | 902 | 122 |
| capsular | 941 | 941 | 0 |
| perineural | 122 | 124 | 2 |
| soft_tissue | 493 | 493 | 0 |
| airway | 1 | 119 | 118 |
| tracheal | 14 | 101 | 87 |
| esophageal | 69 | 92 | 23 |

No `_anywhere` retractions remain. Implementation note: the final SQL preserves Script 363 `finding_status = 'present'` positives and additively ORs v2 feeder positives. A structured-only replacement was tested and rejected because it retracted `vascular_microscopic` and `perineural`, violating G2.

## New columns

| Column / metric | Truthy patients |
| --- | --- |
| `any_rln_invasion_anywhere` | 81 |
| `any_pT4a_final_anywhere` | 148 |
| `any_pT4b_final_anywhere` | 12 |
| `any_carotid_encasement_anywhere` | 9 |
| `any_mediastinal_vessel_anywhere` | 4 |
| `any_prevertebral_fascia_anywhere` | 0 |

## QA gates

| Gate | Observed | Verdict |
| --- | --- | --- |
| G1 row-count invariance | 10,871 | PASS |
| G2 no v2-induced retractions | all post counts >= pre counts | PASS |
| G3 new-column spot rates | RLN=81; pT4a=148; pT4b=12; carotid=9; mediastinal=4; prevertebral=0 | PASS |
| G4 hierarchy sanity | 0 violations | PASS |
| G5 feeder round-trip | vascular_v2_to_rollup=0; airway_tracheal_v2_to_rollup=0; airway_rln_v2_to_rollup=0; t4b_final_v2_to_rollup=0; ete_gross_v2_to_rollup=0 | PASS |
| G6 build tag cutover | `mig_60_invasion_rollup_v2_refresh_20260424` at `2026-04-24 23:32:01.219507` | PASS |

## Side-car: glands=5 patch

| Field | Value |
| --- | --- |
| research_id | `9371` |
| event id | `9371|OPNOTE|1` |
| evidence quote | Five parathyroid glands identified with the right supernumerary gland being a 6 mm adenoma. |
| decision | Clamp to contract max 4; evidence documents a supernumerary fifth gland, but canonical gland-count contract is 0-4. |
| post-patch `glands_identified_count = 5` rows | 0 |
| post-patch `max_glands_identified` | 4 |

## Deferred (mig 58 carry-forwards b + c)

- CF-b remains deferred: `any_incidental_parathyroidectomy = TRUE` appears over-liberal and needs a sample-based specificity pass before downstream complications canonicalization.
- CF-c remains deferred: 85 autotransplant patients with unknown location should be documented as unknown site unless a later focused mini-extractor is justified.
- Candidate prompt names carried forward: `cursor_prompts/CURSOR_PROMPT_MIG_61_PARATHYROID_SPECIFICITY_TBD.md` and `cursor_prompts/CURSOR_PROMPT_MIG_64_COMPLICATIONS_CANONICAL_SCOPING_TBD.md`.

## Carry-forward to Script 364+

- Re-home `canonical_invasion_events_v1` LLM CTEs on the v2 loaders instead of the deprecated vascular/airway LLM tables.
- Drop or fully retire `_deprecated_note_entities_llm_{vascular,airway}_invasion` after downstream consumers are repointed.
- Design the complications canonical around parathyroid + RLN + wound + hypocalcemia, likely per surgery episode if `surgery_episode_id` coverage supports it.
- Keep the rollup-level v2 additive OR pattern documented here until the events table itself is rebuilt.
