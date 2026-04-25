# Migration 57 (project) — Airway invasion v2 tier-2 canonical — close-out

**Date:** 2026-04-24  
**Database:** `thyroid_canonical_publication_v1_0`  
**Repo SHA (migration committed):** `28445db`  
**`build_script` / provenance tag:** `mig_57_airway_invasion_v2_20260424`  
**Source LLM table:** `main.note_entities_llm_airway_invasion_v2` (`error = 0`; build `9b82651` family)

## Repo numbering note

| Concept | Value |
|---------|--------|
| Sequential SQL file in `qc_framework_v1/migrations/` | **58** (`58_airway_invasion_v2_canonical_tier2_v1.sql`) — **57** is reserved for vascular v2 in this repo |
| Project / provenance id | **mig_57_airway** (this document, `build_script` column) |

## Repo artifacts

| Artifact | Role |
|----------|------|
| `qc_framework_v1/migrations/58_airway_invasion_v2_canonical_tier2_v1.sql` | `CREATE OR REPLACE` for events + patient rollup; `COMMENT`; `manuscript_workspace.detail_table_registry_v1` for the two canonicals |
| `scripts/apply_mig_58_airway_invasion_v2_extras.py` | Idempotent: rename legacy `note_entities_llm_airway_invasion` → `_deprecated_note_entities_llm_airway_invasion`, register deprecated row in the registry |

## Objects delivered

| Object | Grain |
|--------|--------|
| `main.canonical_airway_invasion_events_v1` | one row per `note_row_id` from `error = 0` on v2 source (`airway_event_id` = `note_row_id`) |
| `main.canonical_airway_invasion_patient_rollup_v1` | one row per `research_id` |
| `main._deprecated_note_entities_llm_airway_invasion` | renamed from `main.note_entities_llm_airway_invasion` (non-v2) |
| `manuscript_workspace.detail_table_registry_v1` | rows for both canonicals + deprecated loader |

## QA counts (MotherDuck, post-migration)

| Check | Result |
|--------|--------|
| `COUNT(*)` events | **6,054** |
| `COUNT(*)` patient rollup | **2,820** |
| Event rows with `t4a_implication = 'pT4a'` | **196** |
| Distinct patients with any such event (`any_pT4a_direct` / `any_pT4a_final`) | **135** |

**`any_pT4a_final` note:** The prompt sketch (~200–280 TRUE) aligns with **event**-level `pT4a` mentions (196). At **patient** level, multiple events collapse to **135** patients with a direct `pT4a` flag. In this build, `any_pT4a_final` equals `any_pT4a_direct` (**135**): no additional patients were picked up solely from structural fields (`present` on trachea/larynx/cricoid/RLN/esophagus-in-run) without a direct `pT4a` event.

### `any_pT4a_final` (patient rollup)

| `any_pT4a_final` | `COUNT(*)` |
|--------------------|------------|
| `false` | **2,685** |
| `true` | **135** |

### `worst_tracheal_depth` (patient rollup)

| `worst_tracheal_depth` | `COUNT(*)` |
|------------------------|------------|
| `NULL` | **2,802** |
| `full_thickness` | **7** |
| `adventitia` | **5** |
| `cartilage` | **5** |
| `mucosal` | **1** |

### `cricoid_invasion` (events)

| Value | Events |
|-------|--------|
| `unknown` | 5,404 |
| `absent` | 635 |
| `present` | 13 |
| `NULL` | 2 |

Patients with `any_cricoid_invasion` on rollup: **12**.

## Reconciliation: tracheal vs `canonical_invasion_patient_rollup_v1`

**Comparison:** `canonical_airway_invasion_patient_rollup_v1.any_tracheal_involvement` (tracheal `present` **or** `shaved`) vs `canonical_invasion_patient_rollup_v1.any_tracheal_anywhere`.

| Metric | Value |
|--------|--------|
| Patients in airway rollup | 2,820 |
| `any_tracheal_anywhere` NULL on invasion rollup | 0 |
| Agree | **2,729** |
| Disagree | **91** |
| Agreement | **96.77%** (above the >90% bar) |

Disagreements are treated as LLM-enriched or definition drift vs the coarser union in `canonical_invasion_patient_rollup_v1` until that rollup is rebuilt from this table.

## Path AJCC8 `T4a` cross-check

Using `views_readable.path_malignant_events_VIEW_v1` with `t_stage_ajcc8 = 'T4a'` (patient distinct):

| Metric | Value |
|--------|--------|
| Distinct patients with path AJCC8 T4a in this view | **6** |
| Airway rollup `any_pT4a_final` | **135** patients |
| Patients in both with `any_pT4a_final` | **4** |
| Path T4a but airway `any_pT4a_final` false | **2** |
| Airway `any_pT4a_final` but not in path T4a set | **131** |

The path view is a **narrow** malignant-event slice; most airway `pT4a` signal is note/LLM-based and will not one-to-one match path rows. Use this as a sanity crosswalk, not a completeness test.

## Esophagus overlap

- **Keep separate:** `canonical_esophageal_invasion_*_v1` remains the dedicated esophageal tier-2.  
- **This table:** `esophageal_invasion` reflects the **airway v2** extraction context; dual documentation is intentional where both fire.

## Deprecated objects

| Object | Action |
|--------|--------|
| `main.note_entities_llm_airway_invasion` | **Renamed** to `main._deprecated_note_entities_llm_airway_invasion` |
| `manuscript_workspace.detail_table_registry_v1` | Single row for `_deprecated_note_entities_llm_airway_invasion`; prior name removed on replace |

**Downstream:** Consumers still pointing at `main.note_entities_llm_airway_invasion` (e.g. `scripts/363_invasion_canonical.py` loaders) need a follow-up repoint to v2 and/or `_deprecated_*` for historical runs.

## Carry-forward (not in this migration)

- Rebuild `main.canonical_invasion_patient_rollup_v1` `any_airway_*` / `any_tracheal_*` from `canonical_airway_invasion_patient_rollup_v1` plus structured feeder (separate migration / Script 363 revision).

## RLN / imaging sanity

Not run as an automated gate here; spot-check `any_rln_invasion` against preop imaging vocal-cord narratives when validating edge cases.
