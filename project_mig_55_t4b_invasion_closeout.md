# Migration 55 — T4b invasion tier-2 canonical — close-out

**Date:** 2026-04-24  
**Database:** `thyroid_canonical_publication_v1_0`  
**Repo SHA (migration + docs applied):** `55b962266cfe04e3098ef7f44b4c4ff65de028f6` (re-run `git rev-parse HEAD` after your commit)  
**SQL file:** `qc_framework_v1/migrations/56_t4b_invasion_canonical_tier2_v1.sql`  
**`build_script` / provenance tag:** `mig_55_t4b_invasion_20260424`

## Objects delivered

| Object | Grain | Notes |
|--------|-------|--------|
| `main.canonical_t4b_invasion_events_v1` | one row per loader row (`t4b_event_id` = `note_row_id`) | From `main.note_entities_llm_t4b_invasion_v1` where `error=0` |
| `main.canonical_t4b_invasion_patient_rollup_v1` | one row per `research_id` | `any_pT4b_final` = direct `t4b_implication='pT4b'` OR any anatomic component `present` |
| `main.v_note_entities_llm_t4b_invasion_v1` | VIEW | Flat `parsed_json` for QA |
| `main.canonical_ete_subgrade_patient_rollup_v1` | rebuilt | Adds `any_pT4b_from_t4b_invasion`, `pT4b_ete_vs_t4b_invasion_discordant` |
| `manuscript_workspace.detail_table_registry_v1` | — | Inserts for T4b objects; refresh row for ETE patient rollup |

## QA counts (MotherDuck, post-migration)

| Check | Result |
|--------|--------|
| `COUNT(*)` on `canonical_t4b_invasion_events_v1` | **944** |
| Patients on `canonical_t4b_invasion_patient_rollup_v1` | **434** |
| `SUM(any_pT4b_final::INT)` | **10** (rarer than the 30–40 sketch in the prompt; cohort-specific) |
| `SUM(any_pT4b_direct::INT)` | **10** (all positive patients had at least one explicit `t4b_implication='pT4b'` event) |
| ETE rollup patients | **151** |
| `SUM(any_pT4b::INT)` on ETE rollup | **3** |
| `pT4b_ete_vs_t4b_invasion_discordant = TRUE` | **0** (among rows where ETE `any_pT4b` is non-null and tier-2 row exists) |
| Rows with non-null discordant column (comparable) | **34** |

## ETE ↔ T4b overlap (boolean ETE path)

- Patients with ETE `any_pT4b` true **and** tier-2 `any_pT4b_from_t4b_invasion` true: **1**
- ETE `any_pT4b` true, tier-2 false: **2**
- Tier-2 true, ETE `any_pT4b` false: **0** (among ETE rollup rows)

## Path malignant sanity (AJCC8 `t_stage_ajcc8` + `gross_ete`)

Among the **10** patients with `any_pT4b_final`, **10** had at least one path-malignant row with `t_stage_ajcc8 ∈ {T4a,T4b}` or `gross_ete=1`. **0** outside that high-risk pool (manual review queue empty on this rule).

## Component audit — patients with `any_pT4b_final`

All **10** patients: `any_pT4b_direct = TRUE` (explicit `pT4b` implication in at least one note). None had `any_prevertebral_fascia` true at patient rollup; carotid and/or mediastinal `present` counts reflect supporting anatomic calls:

| research_id | carotid (any) | mediastinal (any) | notes |
|-------------|---------------|-------------------|--------|
| 2139 | yes | yes | multiple events |
| 5114 | yes | no | |
| 5378 | yes | no | |
| 6493 | yes | no | |
| 6694 | yes | yes | |
| 7083 | yes | no | |
| 7424 | yes | no | |
| 8088 | no | yes | mediastinal-only anatomic present |
| 8254 | yes | no | high `n_carotid_events` |
| 9502 | yes | yes | |

## Crosswalk semantics

- `any_pT4b` on the ETE rollup remains **BOOL_OR(`ajcc8_implication='pT4b'`)** and can be **NULL** when every event has null `ajcc8_implication`.
- `pT4b_ete_vs_t4b_invasion_discordant` is **NULL** unless the patient appears in `canonical_t4b_invasion_patient_rollup_v1` **and** ETE `any_pT4b` is not null; then it is true when ETE and tier-2 boolean pT4b disagree.

## Carry-forward

- Retire 411 Tier-1 `nlp_*_t4b_*` flags on CPM after verification (per `project_cpm_llm_parse_architecture`).
- Future: rebuild `canonical_invasion_patient_rollup_v1` to ingest `canonical_t4b_invasion_patient_rollup_v1` alongside other tier-2 layers.
