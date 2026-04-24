# LLM Extraction — To-Do List (QC framework v1)

Tracking structured fields that cannot be derived from current canonical columns
and require a new LLM extraction pass over narrative notes (op note, gross
description, microscopic, addenda).

Format: one item per structured column we need; each includes (a) the rule
that needs it, (b) source notes to parse, (c) expected vocabulary, (d) priority.

---

## Done — Tier-2 extractions (closed)

### T4b — invasion of prevertebral fascia, carotid, or mediastinal vessels (CLOSED 2026-04-24)

- **Delivered**: `main.canonical_t4b_invasion_events_v1`, `main.canonical_t4b_invasion_patient_rollup_v1`, loader flattener `main.v_note_entities_llm_t4b_invasion_v1`; crosswalk columns on `main.canonical_ete_subgrade_patient_rollup_v1` (`any_pT4b_from_t4b_invasion`, `pT4b_ete_vs_t4b_invasion_discordant`).
- **Migration**: `qc_framework_v1/migrations/56_t4b_invasion_canonical_tier2_v1.sql` (`build_script` = `mig_55_t4b_invasion_20260424`).
- **Close-out**: `project_mig_55_t4b_invasion_closeout.md`.
- **Source**: `main.note_entities_llm_t4b_invasion_v1` (gpt-oss-120b tier-2 pass).

---

<!-- Add further items below as downstream prompts surface new LLM needs. -->
