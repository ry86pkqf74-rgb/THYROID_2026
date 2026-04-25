# Migration 56 (project) — Vascular / lymphatic / perineural invasion v2 — close-out

**Date:** 2026-04-24  
**Database:** `thyroid_canonical_publication_v1_0`  
**Repo SHA (migration applied):** `f7378044b8d9dc2e50d827af1c1b386aae1563fe` (re-run `git rev-parse HEAD` after your commit)  
**`build_script` / provenance tag:** `mig_56_vascular_invasion_v2_20260424`

## Repo artifacts

| Artifact | Role |
|----------|------|
| `qc_framework_v1/migrations/57_vascular_invasion_v2_canonical_tier2_v1.sql` | Core `CREATE OR REPLACE` for events + patient rollup, `COMMENT`, `detail_table_registry_v1` for the two canonicals (sequential migration number **57**; project id remains **mig_56** per prompt) |
| `scripts/apply_mig_57_vascular_invasion_v2_extras.py` | Idempotent: rename legacy `note_entities_llm_vascular_invasion` → `_deprecated_note_entities_llm_vascular_invasion`, register deprecated row in the registry |

**Note:** `56_t4b_invasion_canonical_tier2_v1.sql` was already in the tree for T4b tier-2; this vascular work is filed as **57** in `qc_framework_v1/migrations/` to avoid filename collision.

## Objects delivered

| Object | Grain |
|--------|--------|
| `main.canonical_vascular_invasion_events_v1` | one row per mention (`vi_event_id` = `note_row_id`) from `error = 0` on `main.note_entities_llm_vascular_invasion_v2` |
| `main.canonical_vascular_invasion_patient_rollup_v1` | one row per `research_id` |
| `main._deprecated_note_entities_llm_vascular_invasion` | renamed from `main.note_entities_llm_vascular_invasion` (non-v2) |
| `manuscript_workspace.detail_table_registry_v1` | rows for both canonicals + deprecated loader |

## QA counts (MotherDuck, post-migration)

| Check | Result |
|--------|--------|
| `COUNT(*)` events | **3,861** |
| `COUNT(*)` patient rollup | **3,745** |
| `COUNT(DISTINCT research_id)` events | **3,745** |

## `worst_extent` (patient rollup)

| `worst_extent` | `COUNT(*)` |
|----------------|------------|
| `NULL` | **3,288** |
| `focal` | **211** |
| `extensive` | **174** |
| `minimal` | **58** |
| `widely_invasive` | **14** |
| **Non-null total** | **457** (prompt sketched ~459) |

## Path-synoptic / Script 363 rollup reconciliation

**Column:** `main.canonical_invasion_patient_rollup_v1.any_vascular_microscopic_in_op_or_path`

| Metric | Value |
|--------|--------|
| Patients in both rollups (inner join on `research_id`) | 3,745 |
| Comparable patients (both VI booleans non-null) | **3,745** |
| Agree (`vi_llm` = `vi_path`) | **3,381** |
| Disagree | **364** |

Agreement rate on the comparable set: **90.28%** (3,381 / 3,745) — **above the >85% bar**.

## LVI contract check

Expected: `any_lvi_collapsed` ≡ (`any_vascular_invasion` OR `any_lymphatic_invasion`) at patient level when interpreted as a strict “lymphovascular = VI ∨ LI” contract.

| Result | Count |
|--------|--------|
| Mismatches | **9** |

All **9** are the pattern: `any_vascular_invasion = false`, `any_lymphatic_invasion = false`, `any_lvi_collapsed = true` (sample `research_id`: 5312, 5298, 6214, 1839, 5221, 8902, 9387, 1774, 7786). Likely `lvi_collapsed` carrying composite / residual semantics (e.g. mention phrasing) not decomposed into separate VI/LI fields — **worth a short event-level pass**, not a migration blocker.

## Rollup booleans (patient-level)

| Field | `SUM` (truthy) |
|-------|----------------|
| `any_vascular_invasion` | 730 |
| `any_lymphatic_invasion` | 873 |
| `any_perineural_invasion` | 103 |
| `any_lvi_collapsed` | 1,173 |

## Deprecated objects

| Object | Action |
|--------|--------|
| `main.note_entities_llm_vascular_invasion` | **Renamed** to `main._deprecated_note_entities_llm_vascular_invasion` |
| `manuscript_workspace.detail_table_registry_v1` | Row for the deprecated table name; prior name `note_entities_llm_vascular_invasion` removed on replace |

**Downstream consumers** that still referenced `main.note_entities_llm_vascular_invasion` (e.g. `scripts/363_invasion_canonical.py`) need a follow-up repoint to v2 and/or the `_deprecated_` name.

## Carry-forward (not in this migration)

- Rebuild `main.canonical_invasion_patient_rollup_v1` mention-layer `any_vascular_*` / `any_lymphatic_*` / `any_perineural_*` from `canonical_vascular_invasion_patient_rollup_v1` plus path-synoptic feeder (separate migration / Script 363 revision).
