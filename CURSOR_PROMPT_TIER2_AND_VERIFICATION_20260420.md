# Cursor Prompt — Tier 2 Completion + Side-by-Side Verification Layer

**Date:** 2026-04-20
**Author:** handed off from Cowork (Logan + Claude) to Opus 4.7 in Cursor
**Follows:** `CURSOR_PROMPT_COMPREHENSIVE_V1_0_CLEANUP_20260420.md` (Scripts 288–303). Execute this prompt AFTER that one completes.

## Goal (one sentence)

For every clinical domain in `thyroid_canonical_publication_v1_0.main`, produce (1) a typed per-event **Tier 2** table (JSON entities → typed columns) for the 12 domains still in JSON-only state, and (2) a **verification table** that sits every canonical field next to its original Excel value, the LLM-extracted value, and the free-text evidence span — so Logan can verify each datapoint by direct comparison. Archive every confirmed duplicate table.

---

## Background — the two-tier pattern you are finishing

CPM has 118 `nlp_<domain>_*` Tier 1 summary columns covering every LLM domain. For ~10 domains, Tier 2 (typed per-event tables) exists. For ~12 domains it's missing — entities still live only in `note_entities_llm_<domain>.result_json`. The ENTITY SHAPE is identical across all LLM tables:

```json
{"entities":[
  {"entity_type":"...","entity_value":"...","entity_date":"...","date_confidence":0.9,
   "date_source_keyword":"...","present_or_negated":"present|negated|uncertain",
   "confidence":0.8,"evidence_text":"... span from note ...","source_line":123}
]}
```

**Confirmed column mapping (audited 2026-04-21 across all 23 note_entities_llm_* + 7 older note_entities_* tables):**

| Concept | Actual column name |
|---|---|
| Timestamp | `extracted_at` (NOT `extraction_timestamp` — the prior version of this prompt was wrong) |
| Note identifier (row-level) | `note_row_id` (UUID string — not a numeric/BIGINT, and NOT the natural join key) |
| Note identifier (ordinal) | `note_index` |
| Natural join key to `clinical_notes_long` | **`(research_id, note_index)`** — NOT `note_id` (no such column) and NOT `note_row_id` (UUID collisions possible across workbook ingestions) |
| Patient key | `research_id` (VARCHAR) |
| Note date/type | `note_date`, `note_type` (both present on every table) |

So any time this prompt says `note_id`, read it as `(research_id, note_index)` compound key and look up `clinical_notes_long` on that composite. Save the compound as a derived column named `source_note_ref` = `research_id || ':' || note_index` for readability in verify tables.

Parse pattern (reuse in every script — now with correct column names):
```sql
WITH ent AS (
  SELECT research_id, note_row_id, note_index, note_date, note_type, extracted_at,
         UNNEST(CAST(json_extract(result_json, '$.entities') AS VARCHAR[])) AS ent_json
    FROM main.note_entities_llm_<domain>
   WHERE result_json IS NOT NULL
)
SELECT research_id,
       note_row_id,                                                                   -- kept for row-level traceability
       note_index,                                                                    -- part of join key
       research_id || ':' || CAST(note_index AS VARCHAR) AS source_note_ref,          -- canonical compound id
       note_date, note_type, extracted_at,
       json_extract_string(ent_json, '$.entity_type')         AS entity_type,
       json_extract_string(ent_json, '$.entity_value')        AS entity_value,
       json_extract_string(ent_json, '$.entity_date')         AS entity_date,
       CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE)        AS confidence,
       json_extract_string(ent_json, '$.present_or_negated')  AS present_or_negated,
       json_extract_string(ent_json, '$.evidence_text')       AS evidence_text,
       CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT)       AS source_line
  FROM ent;
```

For every place this prompt says `_first_note_id` / `_source_note_id`, use `_first_source_note_ref` / `_source_note_ref` (the `research_id:note_index` compound). The `clinical_notes_long` lookup is always `JOIN clinical_notes_long cnl ON cnl.research_id = x.research_id AND cnl.note_index = x.note_index`.

Each domain then adds a small domain-specific step — typically a CASE on `entity_type` that pivots into proper typed columns (`<domain>_site`, `<domain>_result`, `<domain>_extent`, etc.).

## Operating constraints (same as prior prompt)

1. **PHI safety**: no patient-identifying text in logs. `research_id` only. `evidence_text` is permitted inside canonical tables (that's the point) but do NOT `print()` it in script stdout.
2. **Never overwrite non-NULL**: verification tables are new objects; they don't mutate existing tables. When backfilling CPM, only fill where NULL.
3. **Archive before drop/replace**: use the `archive_pub_v1_0."<name>_preNNN_<UTCZ>"` naming convention and log to `manuscript_workspace.archive_move_log_v1`.
4. **Reference-safety check** before archiving any object: enumerate views/tables that reference it; abort if non-archive references found.
5. **One script = one table/domain**: do NOT combine domains into a single large script. Each script commits individually.
6. **Env**: `scripts/_md_connect.py::connect_locked()`. Cross-DB via `"Thyroid 2026 UPdated".archive_pub_v1_0.<x>`.
7. **Every aggregate boolean is dated and source-linked — NO `_anytime` fields.** Any summary flag on a patient-wide table (`had_*`, `any_*`, `persistent_*`, `first_*`, `last_*`) MUST be accompanied by:
   - `<flag>_first_date` — earliest documentation date
   - `<flag>_last_date` — most recent documentation date
   - `<flag>_first_source_note_ref` — compound `research_id:note_index` link to `clinical_notes_long` for the first-documentation note
   - `<flag>_first_evidence_text` — exact free-text span from that note
   - `<flag>_n_notes_documenting` — count of distinct notes documenting this flag
   - (optional) `<flag>_last_source_note_ref` and `<flag>_last_evidence_text` when longitudinal context matters (recurrence, persistent symptoms, ongoing findings)
   A bare boolean without these companion columns is NOT acceptable. The word "anytime" is banned from column names — use `_first_date`/`_last_date` instead. This rule applies to every `*_patient_wide_v1` table produced in Phase A AND to any new summary column added to CPM or any canonical table. Note: the prior version of this prompt used `_first_note_id` / `_source_note_id` — substitute those globally with `_first_source_note_ref` / `_source_note_ref` (the compound `research_id:note_index` key), since `note_id` does not exist on any `note_entities_*` table.

8. **Timestamp column is `extracted_at`, not `extraction_timestamp`.** Every reference in this prompt to `extraction_timestamp` should be read as `extracted_at`. This was confirmed by a full schema audit on 2026-04-21 across all 30 `note_entities_*` tables in `main`.

---

## Phase A — Finish Tier 2 parses (Scripts 304–313)

### Script 304 — `main.frozen_section_event_v1`

**Source**: `note_entities_llm_frozen_section_detail` (8,640 entities in 4,618 notes).

**Target grain**: one row per (research_id, note_id, entity_index).

**Columns**:
- Keys: `research_id`, `note_id`, `note_date`, `note_type`, `event_index` (row_number within note, ordered by source_line)
- Typed: `frozen_section_site` (maps from entity_type='site' or entity_value matching site patterns: 'left lobe', 'right lobe', 'isthmus', 'central neck', 'level VI', etc.), `frozen_section_result` ∈ {benign, malignant, indeterminate, deferred, PTC, follicular_lesion, atypia, NULL}, `frozen_section_indication`, `frozen_section_date`, `was_deferred_flag`, `was_final_diagnosis_flag`.
- Raw: `entity_type`, `entity_value`, `present_or_negated`, `confidence`, `evidence_text`, `source_line`.

**Also create patient-wide pivot** `main.frozen_section_patient_wide_v1`:
- One row per (research_id, linked_surgery_episode_id if resolvable else research_id+note_date)
- Per-event slot columns (cap at 6, overflow → `frozen_events_overflow_json`): for slot `k ∈ 1..6`: `frozen_{k}_site`, `frozen_{k}_result`, `frozen_{k}_date`, `frozen_{k}_note_id`, `frozen_{k}_evidence_text`.
- Summary flags (each follows Constraint 7 — date + note_id + evidence):
  - `n_frozen_events`
  - `any_frozen_performed_flag` + `first_frozen_performed_date` + `first_frozen_performed_note_id` + `first_frozen_performed_evidence_text` + `last_frozen_performed_date` + `n_notes_documenting_frozen_performed`
  - `any_frozen_malignant_result_flag` + `first_malignant_frozen_date` + `first_malignant_frozen_note_id` + `first_malignant_frozen_evidence_text` + `n_notes_documenting_malignant_frozen`
  - `any_frozen_deferred_flag` + `first_deferred_frozen_date` + `first_deferred_frozen_note_id` + `first_deferred_frozen_evidence_text`

**Backfill CPM** (conservative, only where NULL):
- `nlp_frozensec_n_events` := `n_frozen_events` from wide.
- `syn_frozen_section` stays — do not overwrite (it came from synoptic Excel).

Commit as `304_frozen_section_tier2.py`.

---

### Script 305 — `main.vascular_invasion_event_v1`

**Source**: `note_entities_llm_vascular_invasion` (22,800 entities).

**Target grain**: one row per (research_id, note_id, entity_index).

**Columns**:
- Keys: standard
- Typed: `vi_present` ∈ {Y, N, uncertain} (from `present_or_negated`), `vi_extent` ∈ {focal, extensive, angioinvasion, lymphovascular, vascular_only, lymphatic_only, NULL}, `vi_vessel_type` ∈ {artery, vein, lymphatic, capsular, extrathyroidal, NULL}, `vi_count_vessels` (if reported).
- Raw fields + evidence.

**Patient-wide** `main.vascular_invasion_patient_wide_v1` (every flag dated + source-linked per Constraint 7):
- `vi_any_positive_flag` + `vi_first_positive_date` + `vi_first_positive_note_id` + `vi_first_positive_evidence_text` + `vi_last_positive_date` + `vi_n_notes_positive`
- `vi_any_negative_flag` + `vi_first_negative_date` + `vi_first_negative_note_id` + `vi_first_negative_evidence_text`
- `vi_extensive_flag` + `vi_first_extensive_date` + `vi_first_extensive_note_id` + `vi_first_extensive_evidence_text`
- `vi_max_extent` (with `vi_max_extent_date` + `vi_max_extent_note_id` + `vi_max_extent_evidence_text`)
- `vi_all_events_json` (LIST of {date, note_id, extent, vessel_type, evidence_text} for full provenance trail)

**Backfill CPM** where NULL: `vi_any_llm`, `vi_extensive_llm`.

Commit as `305_vascular_invasion_tier2.py`.

---

### Script 306 — `main.airway_invasion_event_v1`

**Source**: `note_entities_llm_airway_invasion` (11,601 entities).

**Columns**:
- Typed: `ai_present` ∈ {Y, N, uncertain}, `ai_extent` ∈ {abutting, invading_no_full_thickness, full_thickness, NULL}, `ai_structure` ∈ {trachea, esophagus, RLN, strap_muscle, prevertebral, carotid, IJV, NULL}, `ai_length_cm` (if measured).
- Raw fields + evidence.

**Patient-wide** `main.airway_invasion_patient_wide_v1` (every flag dated + source-linked per Constraint 7):
- `ai_any_positive_flag` + `ai_first_positive_date` + `ai_first_positive_note_id` + `ai_first_positive_evidence_text` + `ai_last_positive_date` + `ai_n_notes_positive`
- `ai_max_extent` + `ai_max_extent_date` + `ai_max_extent_note_id` + `ai_max_extent_evidence_text`
- `ai_structures_involved_json` (LIST of {structure, first_date, first_note_id, first_evidence_text} per distinct structure)
- `ai_all_events_json` (LIST of {date, note_id, extent, structure, evidence_text} for full provenance trail)

**Backfill CPM** where NULL: `ai_any_llm`, `ai_max_extent_llm`.

Commit as `306_airway_invasion_tier2.py`.

---

### Script 307 — `main.parathyroid_detail_event_v1`

**Source**: `note_entities_llm_parathyroid_detail` (10,130 entities).

**Columns**:
- Typed: `pt_action` ∈ {identified, preserved, autotransplanted, inadvertently_removed, ischemic, biopsied, NULL}, `pt_gland_position` ∈ {LU, LL, RU, RL, unknown}, `pt_count` (integer if reported), `pt_implant_site` (SCM, forearm, other), `pt_was_identified_flag`, `pt_was_preserved_flag`, `pt_was_autotransplanted_flag`, `pt_was_removed_flag`.
- Raw fields + evidence.

**Patient-wide** `main.parathyroid_patient_wide_v1` (every flag dated + source-linked per Constraint 7):
- Counts: `n_pt_identified`, `n_pt_preserved`, `n_pt_autotransplanted`, `n_pt_removed` (each with a companion `_first_surgery_episode_id` + `_first_note_id` + `_first_evidence_text` identifying the first occurrence).
- `any_ischemic_change_flag` + `first_ischemic_date` + `first_ischemic_note_id` + `first_ischemic_evidence_text`
- `autotransplant_site_primary` + `autotransplant_site_first_date` + `autotransplant_site_first_note_id` + `autotransplant_site_first_evidence_text`
- `pt_all_events_json` (LIST of {date, note_id, surgery_episode_id, action, gland_position, evidence_text} for full provenance trail)

Commit as `307_parathyroid_detail_tier2.py`.

---

### Script 308 — `main.past_surgical_hx_event_v1`

**Source**: `note_entities_llm_past_surgical_hx` (3,919 entities).

**Columns**: `psh_procedure`, `psh_anatomic_site`, `psh_date_raw`, `psh_date_parsed` (DATE, best-effort), `psh_was_thyroid_related_flag`, `psh_was_neck_related_flag`, `psh_indication_raw`.

**Patient-wide** `main.past_surgical_hx_patient_wide_v1` (every flag dated + source-linked per Constraint 7):
- `had_prior_thyroid_surgery_flag` + `prior_thyroid_surgery_first_date` + `prior_thyroid_surgery_first_documentation_note_id` + `prior_thyroid_surgery_first_evidence_text` + `prior_thyroid_surgery_n_notes_documenting`
- `prior_thyroid_surgery_type_primary` + `prior_thyroid_surgery_type_primary_source_note_id` + `prior_thyroid_surgery_type_primary_evidence_text`
- `had_prior_neck_surgery_flag` + companion first_date + first_note_id + first_evidence_text
- `prior_neck_surgeries_list_json` (LIST of {procedure, anatomic_site, psh_date_parsed, source_note_id, evidence_text} — this is the full provenance trail, one object per distinct prior surgery).

Commit as `308_past_surgical_hx_tier2.py`.

---

### Script 309 — `main.past_medical_hx_event_v1`

**Source**: `note_entities_llm_past_medical_hx` (865 entities — low volume).

**Columns**: `pmh_condition`, `pmh_onset_date_raw`, `pmh_status` ∈ {active, resolved, chronic, historical, NULL}, `pmh_is_thyroid_related_flag`, `pmh_is_radiation_exposure_flag`, `pmh_is_cancer_history_flag`, `pmh_is_autoimmune_flag`.

**Patient-wide** `main.past_medical_hx_patient_wide_v1` (every flag dated + source-linked per Constraint 7):
- `had_prior_neck_radiation_flag` + `prior_neck_radiation_first_date` + `prior_neck_radiation_first_note_id` + `prior_neck_radiation_first_evidence_text` + `prior_neck_radiation_n_notes_documenting`
- `had_prior_cancer_flag` + same companion set (first_date + first_note_id + first_evidence_text + n_notes)
- `has_autoimmune_thyroid_disease_flag` + same companion set
- `pmh_conditions_list_json` (LIST of {condition, pmh_status, pmh_onset_date_raw, source_note_id, evidence_text} — full provenance trail).

Commit as `309_past_medical_hx_tier2.py`.

---

### Script 310 — `main.functional_outcomes_event_v1`

**Source**: `note_entities_llm_functional_outcomes` (3,322 entities).

**Columns**: `fo_domain` ∈ {voice, swallowing, scar, qol, hypocalcemia_sx, shoulder_mobility, NULL}, `fo_severity` ∈ {none, mild, moderate, severe, NULL}, `fo_resolution` ∈ {resolved, improving, persistent, NULL}, `fo_timepoint_days_postop` (if computable), `fo_qol_score` (numeric).

**Patient-wide** `main.functional_outcomes_patient_wide_v1` (every flag dated + source-linked per Constraint 7):
- `voice_symptom_postop_flag` + `voice_symptom_postop_first_date` + `voice_symptom_postop_first_note_id` + `voice_symptom_postop_first_evidence_text` + `voice_symptom_postop_last_date` + `voice_symptom_postop_last_note_id` + `voice_symptom_postop_last_evidence_text` + `voice_symptom_postop_n_notes_documenting`
- `swallowing_symptom_postop_flag` + same companion set (first_date/note_id/evidence + last_date/note_id/evidence + n_notes)
- `persistent_voice_change_flag` (defined: ≥1 voice symptom documented >6 months postop) + `persistent_voice_change_last_documented_date` + `persistent_voice_change_last_note_id` + `persistent_voice_change_last_evidence_text` + `persistent_voice_change_n_notes_beyond_6mo`
- `persistent_hypocalcemia_sx_flag` + same companion set as persistent_voice_change
- `fo_all_events_json` (LIST of {fo_domain, fo_severity, fo_resolution, fo_timepoint_days_postop, note_id, evidence_text} for full provenance trail).

**Note on "persistent" fields**: "persistent" is permitted ONLY when defined by an explicit cut-point (e.g., "≥1 documentation at >6 months postop" or "documented at last follow-up note"). The cut-point must be stated in the column comment and implemented in the script. "Persistent at last follow-up" must cite the actual `last_fu_note_id` and `last_fu_note_date` — never an implicit "anytime".

Commit as `310_functional_outcomes_tier2.py`.

---

### Script 311 — `main.physical_exam_event_v1`

**Source**: `note_entities_llm_physical_exam` (2,025 entities).

**Columns**: `pe_finding_category` ∈ {thyroid_mass, neck_ln, voice, scar, other, NULL}, `pe_laterality`, `pe_size_cm` (if measured), `pe_tenderness_flag`, `pe_fixed_flag`, `pe_mobile_flag`.

**Patient-wide** `main.physical_exam_patient_wide_v1` (every flag dated + source-linked per Constraint 7 — NO `_anytime` columns):
- `pe_palpable_thyroid_mass_flag` + `pe_palpable_thyroid_mass_first_date` + `pe_palpable_thyroid_mass_first_note_id` + `pe_palpable_thyroid_mass_first_evidence_text` + `pe_palpable_thyroid_mass_last_date` + `pe_palpable_thyroid_mass_last_note_id` + `pe_palpable_thyroid_mass_n_notes_documenting`
- `pe_palpable_ln_flag` + same companion set
- `pe_documented_voice_abnormality_flag` + same companion set
- `pe_all_events_json` (LIST of {pe_finding_category, pe_laterality, pe_size_cm, note_id, note_date, evidence_text} for full provenance trail)

Commit as `311_physical_exam_tier2.py`.

---

### Script 312 — `main.presenting_symptoms_event_v1`

**Source**: `note_entities_llm_presenting_symptoms` (280 entities — low volume).

**Columns**: `ps_symptom`, `ps_onset_duration_raw`, `ps_severity`, `ps_was_trigger_for_workup_flag`.

**Patient-wide** `main.presenting_symptoms_patient_wide_v1` (per Constraint 7):
- `presenting_symptom_primary` + `presenting_symptom_primary_source_note_id` + `presenting_symptom_primary_note_date` + `presenting_symptom_primary_evidence_text`
- `was_symptomatic_at_presentation_flag` + `symptomatic_first_date` + `symptomatic_first_note_id` + `symptomatic_first_evidence_text`
- `presenting_symptoms_list_json` (LIST of {ps_symptom, ps_severity, ps_onset_duration_raw, source_note_id, note_date, evidence_text}).

Commit as `312_presenting_symptoms_tier2.py`.

---

### Script 313 — Remaining small-volume domains (rad_treatment + patient_decision_adherence + dynamic_risk_response)

These are tiny (580 / 641 / 53 entities). Produce three separate per-event tables following the same pattern:

- `main.rad_treatment_event_v1`: `rt_modality` ∈ {EBRT, proton, other}, `rt_total_dose_gy`, `rt_fractions`, `rt_anatomic_target`, `rt_date_range_raw`, evidence.
- `main.patient_decision_adherence_event_v1`: `pda_category` ∈ {declined_surgery, declined_RAI, declined_followup, refused_biopsy, lost_to_followup, compliance_positive, NULL}, `pda_date_raw`, `pda_clinician_concern_flag`, evidence.
- `main.dynamic_risk_response_event_v1`: `drr_category` ∈ {excellent, indeterminate, biochemical_incomplete, structural_incomplete, NULL}, `drr_criteria_basis_raw`, `drr_date_raw`, evidence.

**Also produce three patient-wide pivots** (Constraint 7 applies — no bare booleans, everything dated + source-linked):

- `main.rad_treatment_patient_wide_v1`:
  - `had_external_beam_radiation_flag` + `ebrt_first_treatment_date` + `ebrt_first_treatment_note_id` + `ebrt_first_treatment_evidence_text` + `ebrt_last_treatment_date` + `ebrt_n_notes_documenting`
  - `rt_total_dose_gy_max` + `rt_total_dose_max_note_id` + `rt_total_dose_max_evidence_text`
  - `rt_modality_primary` + `rt_modality_primary_first_date` + `rt_modality_primary_source_note_id` + `rt_modality_primary_evidence_text`
  - `rt_all_events_json` (LIST of {date, note_id, modality, dose_gy, anatomic_target, evidence_text})

- `main.patient_decision_adherence_patient_wide_v1`:
  - `any_declined_surgery_flag` + `declined_surgery_first_date` + `declined_surgery_first_note_id` + `declined_surgery_first_evidence_text` + `declined_surgery_n_notes`
  - `any_declined_rai_flag` + same companion set
  - `any_lost_to_followup_concern_flag` + same companion set (`last_lost_to_followup_concern_date` also required since this is longitudinal)
  - `pda_all_events_json` (LIST of {date, note_id, pda_category, pda_clinician_concern_flag, evidence_text})

- `main.dynamic_risk_response_patient_wide_v1`:
  - `drr_latest_category` + `drr_latest_note_id` + `drr_latest_note_date` + `drr_latest_evidence_text`
  - `drr_worst_category_ever` + `drr_worst_category_date` + `drr_worst_category_note_id` + `drr_worst_category_evidence_text`
  - `drr_category_trajectory_json` (LIST of {date, note_id, drr_category, evidence_text} ordered by date — full trajectory for Logan to scan)

Commit as `313_small_volume_domains_tier2.py` (one script, six tables — this is the one exception to "one table per script" because volumes are too small to justify six commits).

---

### Script 313b — Tier 2 completeness invariant

Write `manuscript_workspace.tier2_completeness_v1`:
```sql
SELECT t.table_name AS llm_source,
       EXISTS (
         SELECT 1 FROM duckdb_tables()
          WHERE database_name='thyroid_canonical_publication_v1_0' AND schema_name='main'
            AND table_name LIKE REPLACE(t.table_name, 'note_entities_llm_', '') || '_event_v1'
       ) AS has_tier2_event_table
  FROM duckdb_tables() t
 WHERE database_name='thyroid_canonical_publication_v1_0'
   AND schema_name='main'
   AND table_name LIKE 'note_entities_llm_%'
   AND table_name NOT LIKE '%__march2026_broken';
```
Invariant: every row `has_tier2_event_table = TRUE`. If not, script errors out loudly listing the gap.

Commit as `313b_tier2_completeness_invariant.py`.

---

## Phase B — Side-by-side verification tables (Scripts 314–324)

**The verification pattern** (identical for every domain). For each canonical field `<fld>`, the verification table has SIX columns (Constraint 7 applies here too — every LLM value must be dated to its source note, not just linked by id):

| Column | Source |
|---|---|
| `<fld>_excel` | Original Excel-sourced value from the pre-LLM ingest table (path_synoptics, ultrasound_reports, molecular_results, etc.) |
| `<fld>_llm` | The Tier 2 parsed value |
| `<fld>_source_text` | `evidence_text` from the LLM entity that populated `<fld>_llm` (or the free-text span from `clinical_notes_long` if upstream) |
| `<fld>_source_note_id` | note_id that produced the evidence (link to `clinical_notes_long`) |
| `<fld>_source_note_date` | `note_date` of the source note — lets Logan time-order discrepancies without re-joining `clinical_notes_long` |
| `<fld>_concordance` | ∈ {'agree', 'disagree', 'excel_only', 'llm_only', 'both_null'} |

When the Excel value carries its own provenance (e.g. `path_synoptics.source_workbook` + ingest date), add a 7th column `<fld>_excel_source_workbook` so the Excel side is equally auditable. If the field spans multiple notes (longitudinal labs, symptom trajectories), pick the *latest* LLM-concordant note for `<fld>_source_note_id`/`_source_note_date` and add `<fld>_llm_n_notes_agreeing` for trend context.

Plus domain-level columns: `research_id`, grain-specific keys, `n_fields_concordant`, `n_fields_discordant`, `pct_concordance`.

**The view lives in `main.verify_<domain>_v1`** (table, not view — DuckDB views on MotherDuck can be slow; prefer CTAS).

Each script creates ONE verify table for ONE domain. Detail:

---

### Script 314 — `main.verify_pathology_synoptics_v1`

**Grain**: one row per (research_id, path_synoptic_episode_id).

**Excel source**: `path_synoptics` (311 cols, carries `source_workbook`).

**LLM source**: `synoptic_tumor_long_v1` (already parsed) + `note_entities_llm_synoptic_pathology_enrichment` (parsed — if already in synoptic_tumor_long_v1, use that; otherwise parse via the standard pattern first).

**Free text**: `clinical_notes_long` joined on note_id.

**Fields (minimum — verify each both-excel-and-llm)**:
- diagnosis_histology (papillary / follicular / anaplastic / medullary / etc.)
- tumor_size_cm_max
- multifocal_flag
- bilateral_flag
- lymphovascular_invasion
- perineural_invasion
- margin_status (negative / positive / close)
- extrathyroidal_extension
- ETE_grade (minimal / gross)
- T_stage
- N_stage
- M_stage
- AJCC_version
- lymph_node_total_examined
- lymph_node_positive
- grade
- tall_cell_percent
- BRAF_IHC_result

Produce concordance summary: `main.verify_pathology_synoptics_summary_v1` with per-field `n_excel_nonnull`, `n_llm_nonnull`, `n_agree`, `n_disagree`, `n_excel_only`, `n_llm_only`, `pct_agree`.

Commit as `314_verify_pathology_synoptics.py`.

---

### Script 315 — `main.verify_us_nodule_v1`

**Grain**: per nodule (use keys from `canonical_us_nodule_master_v1` built in Script 299).

**Excel source**: `ultrasound_reports` wide (nodule_1..nodule_14 × {composition, echogenicity, shape, margins, foci, dims, ti_rads, location, volume}) — UNPIVOT to long grain for the join.

**LLM source**: `tirads_granular_parsed_v1` + `us_nodule_dynamics_parsed_v1` (both produced by Script 299).

**Free text**: `clinical_notes_long` on note_id.

**Fields**: size_cm_max, composition, echogenicity, shape, margin, echogenic_foci, tirads_points_total, tirads_category, extrathyroidal_extension_on_us.

Commit as `315_verify_us_nodule.py`.

---

### Script 316 — `main.verify_genetics_per_test_v1`

**Grain**: per (research_id, molecular_episode_id).

**Excel source**: `molecular_results.raw_payload_json` + `molecular_testing` (original vendor report fields).

**LLM source**: `genetics_per_test_master_v1` (built in Script 302) — the `variants_json` array and per-gene flags.

**Free text**: `clinical_notes_long` on note_id tied to test order/result note.

**Fields**: platform (vendor-stated vs LLM-inferred), bethesda_category, BRAF_status, RAS_status, TERT_status, TP53_status, fusion_present, CNA_flag, n_variants, primary_variant_gene, primary_variant_hgvs, molecular_risk_tier.

Commit as `316_verify_genetics_per_test.py`.

---

### Script 317 — `main.verify_operative_v1`

**Grain**: per (research_id, surgery_episode_id) from `operative_episode_detail_v2`.

**Excel source**: `operative_episode_detail_v2` columns that were populated from the Excel ingest (pre-LLM) — these are the v1 siblings or the base fields. If a clean Excel-only table exists in archive, join that instead.

**LLM source**: `note_entities_operative_detail` (the parsed 47-col table) + `note_entities_procedures`.

**Free text**: `clinical_notes_long` filtered to operative/procedure notes.

**Fields**: surgery_type (total thyroidectomy / lobectomy / completion / revision), surgical_approach (open / transoral / robotic), central_neck_dissection_flag, lateral_neck_dissection_flag, RLN_identified_flag, PT_identified_flag, PT_autotransplanted_flag, frozen_section_performed_flag, estimated_blood_loss_ml, operative_time_min, complications_any.

Commit as `317_verify_operative.py`.

---

### Script 318 — `main.verify_rai_v1`

**Grain**: per (research_id, rai_episode_id).

**Excel source**: `rai_treatment_episode_v2` columns that came from the nuclear-med Excel ingest.

**LLM source**: the parsed output from `note_entities_llm_rai_detailed` (Script 293 handled the initial integration — re-use that).

**Free text**: `clinical_notes_long` filtered to nuclear-med note types.

**Fields**: rai_dose_mci, rai_administration_date, rai_indication, wb_scan_findings, thyroid_remnant_uptake, focal_uptake_sites, stunning_concern_flag, prep_method (thyrogen vs withdrawal).

Commit as `318_verify_rai.py`.

---

### Script 319 — `main.verify_recurrence_v1`

**Grain**: per (research_id, recurrence_event_id).

**Excel source**: limited — `canonical_recurrence_v1` legacy columns if any came from Excel; otherwise mark `*_excel` NULL explicitly.

**LLM source**: parsed `note_entities_llm_recurrence` → `canonical_recurrence_v1`.

**Fields**: recurrence_flag, recurrence_date, recurrence_site, recurrence_bethesda, recurrence_treatment, time_to_recurrence_months.

Commit as `319_verify_recurrence.py`.

---

### Script 320 — `main.verify_frozen_section_v1`

**Grain**: per event from `frozen_section_event_v1` (Script 304).

**Excel source**: `path_synoptics.fs_pathology_frozen_section`, `path_synoptics.frozen_section_obtained` — join on research_id + path_episode_date.

**LLM source**: `frozen_section_event_v1`.

**Free text**: `clinical_notes_long` on note_id (operative or path note).

**Fields**: frozen_section_performed, frozen_section_result, frozen_section_site.

Commit as `320_verify_frozen_section.py`.

---

### Script 321 — `main.verify_vascular_airway_invasion_v1`

Two separate tables (follow the one-table-per-script convention strictly here): `main.verify_vascular_invasion_v1` and `main.verify_airway_invasion_v1`.

**Excel source** (for VI): `path_synoptics.lymphovascular_invasion` and other `*_invasion` columns. **Excel source** (for AI): `path_synoptics.extrathyroidal_extension` + any `*airway*`/`*trachea*` columns.

**LLM source**: `vascular_invasion_event_v1` (305) / `airway_invasion_event_v1` (306).

Commit as `321_verify_vascular_invasion.py` and a separate `321b_verify_airway_invasion.py`.

---

### Script 322 — `main.verify_parathyroid_v1`

**Grain**: per (research_id, surgery_episode_id).

**Excel source**: `operative_episode_detail_v2` PT-related columns.

**LLM source**: `parathyroid_patient_wide_v1` (Script 307) joined on surgery episode.

**Free text**: `clinical_notes_long`.

**Fields**: n_parathyroids_identified, n_preserved, n_autotransplanted, n_inadvertently_removed, autotransplant_site_primary, any_ischemic_concern.

Commit as `322_verify_parathyroid.py`.

---

### Script 323 — `main.verify_ln_v1`

**Grain**: per (research_id, surgery_episode_id).

**Excel source**: `path_synoptics` LN columns (n_ln_examined, n_ln_positive, level VI/II/III/IV/V counts).

**LLM source**: `ln_master_rollup_v1` + parsed `note_entities_llm_cervical_ln_detail`.

**Fields**: n_ln_total, n_ln_positive, largest_positive_ln_size_cm, ENE_present, ln_levels_involved_list.

Commit as `323_verify_ln.py`.

---

### Script 324 — `main.verify_labs_v1`

**Grain**: per (research_id, lab_episode_id).

**Excel source**: `longitudinal_lab_canonical_v1` flowsheet-derived rows (pre-LLM).

**LLM source**: parsed `note_entities_llm_labs` → lab entities (Script 291 does initial integration).

**Fields**: TSH_value, TSH_units, TSH_date, Tg_value, Tg_units, Tg_date, TgAb_value, calcium_value, PTH_value.

Commit as `324_verify_labs.py`.

---

## Phase C — Archive confirmed duplicates (Script 325)

**Candidates (confirmed by prior investigation 2026-04-20)**:

| Source object | Reason | Archive name |
|---|---|---|
| `main.us_nodules_tirads` | Legacy wide format superseded by `canonical_us_nodule_master_v1` (Script 299) | `us_nodules_tirads_pre325_<UTCZ>` |
| `main.imaging_nodule_master_v1` | Redundant with `canonical_us_nodule_characteristics_v1` (confirmed by Script 299's schema diff — verify before archive) | `imaging_nodule_master_v1_pre325_<UTCZ>` |
| `main.canonical_molecular_tested_v1` | Superseded by `genetics_per_patient_master_v1` (Script 303) | `canonical_molecular_tested_v1_pre325_<UTCZ>` |
| `main.note_entities_llm_synoptic_pathology_enrichment__march2026_broken` | Explicit `_broken` suffix | `note_entities_llm_synoptic_pathology_enrichment__march2026_broken_pre325_<UTCZ>` |
| `main.tumor_pathology` (253 cols) | Check for references — if superseded by `path_synoptics`+`synoptic_tumor_long_v1`, archive | `tumor_pathology_pre325_<UTCZ>` |
| `main.data_dictionary_v279` | If a newer data dictionary has been produced in Phase B (dictionary_v325), archive the old one | conditional |
| `main.path_size_adjudication_v241` | Check references; likely frozen adjudication artifact | conditional |
| `main.ret_note_entity_adjudication_v226` | Versioned adjudication — check references | conditional |
| `main.ret_patient_adjudicated_v226` | Versioned — check references | conditional |
| `main.us_nodules_tirads_vs_inm_v1_discordance_v1` | Discordance queue — keep if non-empty and Logan hasn't adjudicated; archive once resolved | conditional |
| `main.tirads_reextraction_queue_v1` | If empty or fully processed, archive | conditional |
| `main.tirads_v2_reports_raw` | If `tirads_v2_nodules_raw` is the current truth, archive reports_raw | check usage first |
| `main.tirads_llm_validation_v2` | If Phase B verify tables supersede this, archive | conditional |
| `main.clinical_notes_long` | **DO NOT ARCHIVE** — this is the free-text source of truth for every verify table. Flag permanently. |

**Archive procedure (per object)** — identical to Script 297:
1. Copy to `archive_pub_v1_0.<new_name>` (CTAS)
2. Verify `src rowcount == dest rowcount`
3. Enumerate views/tables referencing the source via `duckdb_views()` and `duckdb_tables()` scans — abort if any non-archive reference exists
4. Log to `manuscript_workspace.archive_move_log_v1`
5. `DROP TABLE` from `main`

Commit as `325_archive_duplicates_round2.py`.

---

## Phase D — Final verification (Script 326, supersedes 298)

After Scripts 304–325 complete:

1. Re-run all four CPM invariants (rows=10871, distinct_rid=10871, null_fna=0, column count unchanged unless explicitly logged).
2. Run Script 313b's `tier2_completeness_invariant` — every `note_entities_llm_*` must have its `<domain>_event_v1`.
3. For every Phase B verify table, print the concordance summary:
   ```sql
   SELECT domain, pct_agree, n_excel_only, n_llm_only, n_disagree
     FROM (
       SELECT 'pathology_synoptics' AS domain, * FROM main.verify_pathology_synoptics_summary_v1
       UNION ALL SELECT 'us_nodule', * FROM main.verify_us_nodule_summary_v1
       ... (one row per verify table)
     );
   ```
4. Flag any `pct_agree < 0.80` for Logan's review (write to `manuscript_workspace.verification_low_concordance_v1`).
5. Confirm every table in `main` is either (a) canonical current data, (b) a `verify_*_v1` table, (c) a `_event_v1` or `_patient_wide_v1` Tier 2 parse, or (d) a `note_entities_*` source table. Anything else → queue for review.
6. Write `scripts/output/326_postcleanup_audit.md` with:
   - Row counts for every new Tier 2 table
   - Concordance summary per verify table
   - Archive log diff vs Phase-B-start
   - Domain → verify table mapping
7. Print `git log --oneline scripts/30[4-9]*.py scripts/31*.py scripts/32*.py` to confirm all commits landed.

Commit as `326_postcleanup_verification_v2.py`.

---

## Why this doesn't duplicate prior work

- Scripts 288–303 did the conservative CPM backfills, rebuilt operative_episode_detail_v2, closed the TSH/RAI/path_stage/VC-tiering gaps, and built the US + genetics canonical masters.
- This prompt adds the missing Tier 2 parses (12 domains) and the verification layer that makes every datapoint auditable by direct Excel-vs-LLM-vs-source-text comparison. Nothing here rebuilds what 288–303 already produced.

---

## Git discipline

Identical to prior prompt. Per-script:
```bash
cd "/Users/ros/THyroid 2026"
git add scripts/<N>_*.py
python -m pyflakes scripts/<N>_*.py
git commit -m "Script <N>: <summary>"
git push origin main
```

---

## Definition of done

1. Every `note_entities_llm_*` table has a corresponding `<domain>_event_v1` typed table in `main`. Tier 2 completeness invariant holds.
2. Every clinical domain has a `verify_<domain>_v1` table in `main` with Excel + LLM + source_text side-by-side and a concordance summary.
3. Every confirmed duplicate/legacy table has been archived to `archive_pub_v1_0` with reference-safety check passed and archive_move_log_v1 updated.
4. Logan can open any verify table and see, for every field, all three sources in the same row. Low-concordance fields are flagged for manual review.
5. `scripts/output/326_postcleanup_audit.md` committed and pushed.
6. Every script committed individually with descriptive message and pushed to origin/main.
