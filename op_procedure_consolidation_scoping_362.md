# Operative / Complication / Invasion / History Consolidation — Revised Scoping

**Status:** Revised 2026-04-21 after Logan's Q1–Q6 answers
**Follow-on to:** Script 361 (operative pathology consolidation)
**Database:** `thyroid_canonical_publication_v1_0`

## What changed after Logan's answers

- **Q1 (unsure):** recommend SPLIT into 4 scripts (see proposal below)
- **Q2 (cleanest+accurate):** include `canonical_operative_procedure_codes_v1` — one row per procedure mention with source note linkage
- **Q3 (cross-modal provenance):** ETE / tracheal / esophageal invasion flags must trace back independently across op note vs path vs US vs CT vs PET-CT vs MRI → **new dedicated cross-modal invasion canonical**
- **Q4 (per patient w/ date + source):** complication events = event-grain with `detection_date` + `source_modality`
- **Q5 (separate canonicals for PSH/PMH/Meds):** 3 new history/meds canonicals
- **Q6 (RLN injury in complications, parsed by source, multiple rows):** fold `extracted_rln_injury_refined_v2` into complication events; event-grain with source attribution

## Proposed script split

| Script | Scope | Scale |
|---|---|---|
| **362** | Operative procedure canonicalization (narrow) | 4 tables touched, 1 deprecation |
| **363** | Cross-modal invasion findings (NEW domain) | 6 sources, 1–2 new canonicals |
| **364** | Complication consolidation (event-grain, source-attributed) | 5 tables touched, 3 deprecations |
| **365** | PSH / PMH / Medications canonicalization | 4 sources, 3–4 new canonicals |

These are sequenced — 363 (invasion) depends on 362 (operative) to exist as a source; 364 (complications) is independent; 365 (history) is independent.

---

## Inventory found in MotherDuck

### Invasion / ETE (for Script 363)

| Table | Rows | Pts | Role |
|---|---|---|---|
| `note_entities_llm_airway_invasion` | 48,169 | 10,856 | Raw LLM airway/tracheal invasion by source note |
| `note_entities_llm_vascular_invasion` | 39,210 | 10,868 | Raw LLM vascular invasion by source note |
| `ete_adjudication_v1` | 45 | 45 | Manual ETE adjudication layer — tiny; fold in |
| `operative_episode_detail_v2` | 11,773 | 10,871 | Has `gross_ete_flag`, `tracheal_involvement_flag`, `esophageal_involvement_flag`, `strap_muscle_involvement_flag`, `local_invasion_flag` — surgeon's-eye view |
| `path_synoptics` | 11,688 | 10,871 | Has pathology-derived ETE/invasion columns |
| `canonical_us_thyroid_gland_v2` / `canonical_us_nodule_v2` | — | — | Ultrasound ETE suspicion findings |
| `ct_imaging` | 7,701 | 3,086 | CT narrative — need NLP extraction if not already |
| `mri_imaging` | 715 | 462 | MRI narrative — very sparse |
| `nuclear_med` | 2,220 | 1,148 | PET-CT / nuclear medicine narrative |

**Key insight:** `note_entities_llm_airway_invasion` (48k rows) and `note_entities_llm_vascular_invasion` (39k rows) already have `note_type` and `source_column` fields, so they are *pre-split by modality*. The cross-modal invasion canonical is mostly a materialization, not a re-extraction.

### Complications (for Script 364)

| Table | Rows | Pts | Role |
|---|---|---|---|
| `complication_phenotype_v1` | 5,978 | 2,938 | Grain: (research_id, complication_entity) — ~2 per patient. Has 29 cols including `note_mention_flag`, `suspected_flag`, `confirmed_flag`, `transient_flag`, `permanent_flag`, `timing_days_post_surgery`, `biochemical_low_ca/pth`, `pth_nadir`, `ca_nadir`, `voice_resolution_noted`, `evidence_tier`, `detection_date`, `first_surgery_date`, `status_v2`. **This is the natural base for `canonical_complication_events_v1`** — but it lacks `source_modality`, so we enrich from `note_entities_complications` |
| `complication_patient_summary_v1` | 2,938 | 2,938 | Pure patient-grain rollup |
| `extracted_complications_refined_v5` | 358 | 287 | Narrow refinement; fold into events |
| `extracted_rln_injury_refined_v2` | 92 | 92 | RLN-specific subset; fold into complication events with source_modality (per Q6) |
| `note_entities_complications` | 9,359 | 2,840 | Raw NLP feeder — provides `source_modality` via note_type |

### Operative procedure (for Script 362)

| Table | Rows | Pts | Role |
|---|---|---|---|
| `operative_episode_detail_v2` | 11,773 | 10,871 | De facto canonical — rename in place |
| `note_entities_procedures` | 21,942 | 4,723 | Raw procedure mentions (100% `entity_type='procedure'`) — base for procedure_codes table |
| `note_entities_operative_detail` | 12,151 | 4,032 | Raw op-detail (nerve_monitoring, EBL, etc.) — feeds events enrichment |

### History & Meds (for Script 365)

| Table | Rows | Pts | Role |
|---|---|---|---|
| `note_entities_llm_past_surgical_hx` | 11,037 | 5,641 | Past surgical history LLM extractions |
| `note_entities_llm_past_medical_hx` | 11,037 | 5,641 | Past medical history LLM extractions |
| `note_entities_medications` | 7,501 | 2,070 | Medication LLM extractions |
| `note_entities_problem_list` | 11,579 | 4,037 | Problem list NLP extractions (optional 4th canonical) |

---

## Script 362 — Operative procedure canonicalization (NARROW)

Per Q3, pathology-overlap flags (`gross_ete_flag`, `tracheal_involvement_flag`, `esophageal_involvement_flag`, `local_invasion_flag`) come off `operative_events` — they live in Script 363's cross-modal invasion table. `strap_muscle_involvement_flag` stays (op-note/synoptic only per Q3).

**New tables:**
- `canonical_operative_events_v1` (rename `operative_episode_detail_v2` → 49 cols minus 4 invasion flags = 45 cols)
- `canonical_operative_patient_rollup_v1` (NEW)
- `canonical_operative_procedure_codes_v1` (NEW, per Q2 — one row per procedure mention, ~21,942 rows from `note_entities_procedures`, linked by note_row_id → surgery_episode_id)

**Deprecations:** `operative_episode_detail_v2` only (renamed).

**Keep:** `note_entities_procedures`, `note_entities_operative_detail`.

**Dependencies:** None — can run immediately after 361.

---

## Script 363 — Cross-modal invasion findings (NEW DOMAIN)

**New canonicals:**
- `canonical_invasion_findings_events_v1` — grain: one row per (research_id, finding_type, source_modality, source_date). Columns: `research_id`, `surgery_episode_id` (nullable, joined by temporal proximity), `finding_type` ENUM, `source_modality` ENUM, `source_date`, `source_note_id`, `present_or_negated`, `confidence`, `evidence_span`, `adjudicated_grade` (from ete_adjudication_v1, nullable)
- `canonical_invasion_findings_patient_rollup_v1` — one row per patient, per-finding-type rollups: `any_gross_ete`, `gross_ete_sources` (ARRAY), `any_tracheal_invasion`, `tracheal_invasion_sources`, `any_esophageal_invasion`, `esophageal_invasion_sources`, `any_vascular_invasion`, `any_airway_invasion`, etc.

**finding_type values** (per Q3):
- `gross_ete`, `microscopic_ete`, `tracheal_invasion`, `esophageal_invasion`, `strap_muscle_invasion` (op/path only per Q3), `rln_invasion`, `vascular_invasion`, `airway_invasion`, `carotid_invasion`, `jugular_invasion`, `laryngeal_invasion`, `prevertebral_invasion`

**source_modality values** (per Q3):
- `op_note`, `synoptic_path`, `narrative_path`, `ultrasound`, `ct`, `mri`, `pet_ct`, `nucmed`, `frozen_section`

**Source tables merged in:**
- `note_entities_llm_airway_invasion` (48,169 rows → airway/tracheal)
- `note_entities_llm_vascular_invasion` (39,210 rows → vascular)
- `operative_episode_detail_v2` invasion flags (gross_ete, tracheal, esophageal, strap_muscle, local_invasion) → source_modality='op_note'
- `path_synoptics` ETE/invasion columns → source_modality='synoptic_path'
- `canonical_us_thyroid_gland_v2` / `canonical_us_nodule_v2` ETE columns → source_modality='ultrasound'
- `ct_imaging` narrative (may need NLP pass if not already extracted) → source_modality='ct'
- `mri_imaging` narrative → source_modality='mri'
- `nuclear_med` narrative → source_modality='pet_ct' or 'nucmed'
- `ete_adjudication_v1` → enrichment column `adjudicated_grade`

**Deprecations:** `ete_adjudication_v1` (folded in as column); invasion flags come off `operative_events` and `path_synoptics` as materialized sources of the new canonical (path_synoptics itself stays, but its ETE columns are marked "prefer canonical_invasion_findings_events_v1" in docs).

**Dependencies:** Requires 362 complete (for operative linkage).

---

## Script 364 — Complication consolidation

**New canonicals:**
- `canonical_complication_events_v1` — per Q4+Q6: grain = (research_id, complication_type, detection_date, source_modality). Base = `complication_phenotype_v1` joined with `note_entities_complications` for source attribution. Each confirmed complication gets one row per source that documented it (multiple rows allowed per Q6 for RLN).
- `canonical_complication_patient_rollup_v1` — rename of `complication_patient_summary_v1`, extended with derived fields from events.

**complication_type values:** hypocalcemia, hypoparathyroidism, rln_injury, vocal_cord_paralysis, vocal_cord_paresis, hematoma, seroma, chyle_leak, wound_infection, tracheotomy.

**source_modality values:** op_note, path_note, clinic_note, laryngoscopy, emg, lab_panel, discharge_summary.

**Deprecations:**
- `complication_phenotype_v1` (folded into events)
- `complication_patient_summary_v1` (renamed)
- `extracted_complications_refined_v5` (folded into events)
- `extracted_rln_injury_refined_v2` (folded into events as rln_injury rows per Q6)

**Keep:** `note_entities_complications`.

**Dependencies:** None — can run in parallel with 363.

---

## Script 365 — History & Medications canonicalization

**New canonicals** (per Q5):
- `canonical_past_surgical_history_v1` — from `note_entities_llm_past_surgical_hx` (11,037 rows / 5,641 pts). Grain: one row per (research_id, procedure_name, approximate_date). Columns: procedure_normalized, procedure_raw, laterality, year, source_note_id, source_date, confidence.
- `canonical_past_medical_history_v1` — from `note_entities_llm_past_medical_hx` (11,037 rows / 5,641 pts). Grain: one row per (research_id, condition_name). Columns: condition_normalized, icd10_if_mappable, onset_year, resolved_flag, source_note_id.
- `canonical_medications_v1` — from `note_entities_medications` (7,501 rows / 2,070 pts). Grain: one row per (research_id, medication_name, first_mention_date). Columns: drug_name_normalized, rxnorm_if_mappable, dose_text, route, frequency, indication_class (thyroid_specific / cardiac / psych / other), start_date_approx, stop_date_approx, active_flag.
- *(Optional 4th)* `canonical_problem_list_v1` — from `note_entities_problem_list` (11,579 rows / 4,037 pts). Similar to PMH but problem-list-specific. Maybe redundant with PMH — see Q8 below.

**Patient rollups:** one rollup per canonical (3 or 4 rollups).

**Deprecations:** None — these are all net-new canonicals. The `note_entities_*` upstream feeders stay.

**Dependencies:** None — can run anytime.

---

## Q7–Q10 answers (2026-04-21)

- **Q7 Sequential** — run 362 → 363 → 364 → 365 one at a time; wait for each to complete + QA before starting next.
- **Q8 Problem list combined with PMH** — single `canonical_past_medical_history_v1` fed from `note_entities_llm_past_medical_hx` + `note_entities_problem_list` with a `source_source` discriminator column. No separate problem-list canonical.
- **Q9 Single source of truth for invasion** — `canonical_invasion_findings_events_v1` is the ONLY home for invasion findings. Script 363 will drop invasion columns from `canonical_path_malignant_events_v1` (built in 361) and `canonical_operative_events_v1` (built in 362) so they do not carry redundant columns.
- **Q10 Verify imaging NLP coverage** — before writing the 363 Cursor prompt, verify `note_entities_llm_airway_invasion` + `note_entities_llm_vascular_invasion` already cover `ct_imaging` / `mri_imaging` / `nuclear_med` narratives; if gaps, flag them for an NLP extraction pass before the consolidation.

## Cascade implications

- Script 361 proceeds as-written. Invasion/ETE columns on `canonical_path_malignant_events_v1` are preserved for now; Script 363 will strip them.
- Script 362 keeps `gross_ete_flag`, `tracheal_involvement_flag`, `esophageal_involvement_flag`, `local_invasion_flag` on `canonical_operative_events_v1` for now; Script 363 will strip them.
- `strap_muscle_involvement_flag` stays permanently on operative (per Q3 — operative note / synoptic only).

## Original sign-off questions (resolved)

**Q7. Script sequencing.** Run in order 362 → 363 → 364 → 365? Or parallelize (362 first, then 363/364/365 in parallel)? Preference for tight sequential to simplify QA?

**Q8. Problem list as 4th history canonical?** `note_entities_problem_list` is 11,579 rows / 4,037 pts. It's often redundant with PMH but sometimes has acute/active-only conditions. Build `canonical_problem_list_v1` as a separate table, or fold problem-list entries into `canonical_past_medical_history_v1` with a `source_source='problem_list'` column?

**Q9. Invasion canonical — keep pathology-side columns on `canonical_path_malignant_events_v1` (from 361) too, or rely solely on `canonical_invasion_findings_events_v1`?** Cleanest (my recommendation): keep the path-side columns on `canonical_path_malignant_events_v1` as the authoritative pathology-derived ETE/invasion read, AND have them mirrored as `source_modality='synoptic_path'` rows in `canonical_invasion_findings_events_v1`. Double-storage but zero-drift by construction.

**Q10. Imaging NLP for CT/MRI/nucmed invasion.** `ct_imaging` / `mri_imaging` / `nuclear_med` are narrative text. `note_entities_llm_airway_invasion` and `note_entities_llm_vascular_invasion` already extract from imaging narratives (based on note_type). Do you want me to verify coverage before 363 is written, or defer — assume existing LLM extractions cover imaging and build 363 from them?

---

## Recommendation

Confirm Q7–Q10, and I'll write the Script 362 Cursor prompt first (since it's the narrowest and has no upstream dependencies beyond 361). Then 363/364/365 can be written iteratively.
