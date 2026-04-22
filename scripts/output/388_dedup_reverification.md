# Script 388 — dedup re-verification vs 387 baseline

Generated: 2026-04-22T18:07:35.146366+00:00

Source: `manuscript_workspace.script_387_dedup_probe_v1` (now augmented with `probe_key_override_388`).

| canonical | 387 severity | 387 collapse | 387 null_key | 387 key | 388 override key |
|---|---|---:|---:|---|---|
| `canonical_complications_events_v1` | flag_event | 15 | 4,420 | `(research_id, evidence_span_hash)` | — |
| `canonical_complications_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_fna_events_v1` | ok | 0 | 0 | `(fna_event_id)` | — |
| `canonical_fna_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_frozen_section_events_v1` | ok | 0 | 0 | `(entity_id_hash)` | — |
| `canonical_frozen_section_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_invasion_events_v1` | flag_event | 7,578 | 3,778 | `(invasion_event_id)` | — |
| `canonical_invasion_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_labs_calcium_v1` | ok | 0 | 0 | `(research_id, lab_datetime, value_raw, source)` | — |
| `canonical_labs_pth_v1` | ok | 0 | 0 | `(research_id, lab_datetime, value_raw, source)` | — |
| `canonical_labs_thyroglobulin_v1` | ok | 0 | 0 | `(research_id, lab_datetime, analyte, value_raw, source)` | — |
| `canonical_labs_tsh_v1` | ok | 0 | 0 | `(research_id, lab_datetime, value_raw, source)` | — |
| `canonical_labs_vitamin_d_v1` | ok | 0 | 0 | `(research_id, lab_datetime, value_raw, source)` | — |
| `canonical_medications_events_v1` | flag_event | 2,512 | 0 | `(research_id, evidence_span_hash)` | — |
| `canonical_medications_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_molecular_genetics_from_notes_v2` | ok | 0 | 0 | `(research_id, note_row_id, entity_type, evidence_start)` | — |
| `canonical_molecular_genetics_v2` | flag_event | 856 | 525 | `(molecular_episode_id)` | — |
| `canonical_operative_events_v1` | ok | 0 | 0 | `(surgery_episode_id)` | — |
| `canonical_operative_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_operative_procedure_codes_v1` | ok | 0 | 0 | `(procedure_mention_id)` | — |
| `canonical_path_benign_events_v1` | all_null_key | 0 | 11,688 | `(research_id, surgery_episode_id, synoptic_row_ix, specimen_id)` | `(research_id, source_table, source_report_id, synoptic_row_ord)` |
| `canonical_path_benign_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_path_gland_events_v1` | all_null_key | 0 | 28,724 | `(research_id, surgery_episode_id, synoptic_row_ix, specimen_id, gland_position)` | `(research_id, surgery_episode_id, specimen_id, gland_position, synoptic_row_ord)` |
| `canonical_path_gland_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_path_malignant_events_v1` | flag_event | 442 | 3,026 | `(specimen_focus_id)` | — |
| `canonical_path_malignant_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_patient_master` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_pmh_events_v1` | flag_event | 816 | 0 | `(research_id, evidence_span_hash)` | — |
| `canonical_pmh_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_psh_events_v1` | flag_event | 233 | 0 | `(research_id, evidence_span_hash)` | — |
| `canonical_psh_patient_rollup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_recurrence_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_survival_followup_v1` | ok | 0 | 0 | `(research_id)` | — |
| `canonical_us_lymph_node_v2` | ok | 0 | 0 | `(research_id, us_exam_id, us_ln_id)` | — |
| `canonical_us_nodule_v2` | ok | 0 | 0 | `(research_id, us_exam_id, nodule_id)` | — |
| `canonical_us_thyroid_gland_v2` | ok | 0 | 0 | `(research_id, us_exam_id)` | — |

## Notes

* The 7 `flag_event` collapses (complications 15, invasion 7,578, medications 2,512, molecular_genetics_v2 856, path_malignant 442, pmh 816, psh 233) remain at the 387 baseline — they require upstream-builder fixes (Script 388 carry-forward, NOT addressed in this run).
* The 2 `all_null_key` sham keys on path_benign / path_gland have an override key pinned by 388 (see `scripts/output/388_shamkey_probe.json` for the candidate-key evidence).  The underlying canonical event tables are unchanged; the partition-contract fix remains carry-forward to upstream builders.
