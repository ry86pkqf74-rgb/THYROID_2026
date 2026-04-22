# Script 387 — Within-Canonical Dedup Probe Report

Generated: 2026-04-22T12:58:15.385615+00:00
Tables probed: 36  ·  ok: 27  ·  event-flag: 7  ·  rollup-fail: 0  ·  all_null_key: 2  ·  no_key/missing: 0

Severity legend:
* `ok` — collapse_count == 0 on a populated partition key
* `flag_event` — collapse_count > 0 on an `*_events_v1` (richer key may be needed; manual review)
* `fail_rollup` — collapse_count > 0 on a `*_patient_rollup_v1` or `canonical_patient_master` (HARD FAIL; rollup invariant violated)
* `all_null_key` — every row has NULL in at least one column of the chosen key (the picker found a sham key; needs an alternate partition key)
* `no_key` / `missing` — could not choose a partition key (table missing or no candidate columns matched)

## Summary table

Note: `collapse` is computed as `(total_rows - null_key_rows) - distinct_keys`, so a non-zero `null_key` column is what to investigate when collapse looks suspicious — `COUNT(DISTINCT)` discards NULLs and would otherwise overstate the collapse.

| canonical | partition key | rows | distinct | null_key | collapse | severity |
|---|---|---:|---:|---:|---:|---|
| `canonical_complications_events_v1` | `(research_id, evidence_span_hash)` | 10,954 | 6,519 | 4,420 | 15 | flag_event |
| `canonical_complications_patient_rollup_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_fna_events_v1` | `(fna_event_id)` | 8,119 | 8,119 | 0 | 0 | ok |
| `canonical_fna_patient_rollup_v1` | `(research_id)` | 5,266 | 5,266 | 0 | 0 | ok |
| `canonical_frozen_section_events_v1` | `(entity_id_hash)` | 7,081 | 7,081 | 0 | 0 | ok |
| `canonical_frozen_section_patient_rollup_v1` | `(research_id)` | 4,116 | 4,116 | 0 | 0 | ok |
| `canonical_invasion_events_v1` | `(invasion_event_id)` | 51,773 | 40,417 | 3,778 | 7,578 | flag_event |
| `canonical_invasion_patient_rollup_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_labs_calcium_v1` | `(research_id, lab_datetime, value_raw, source)` | 187 | 187 | 0 | 0 | ok |
| `canonical_labs_pth_v1` | `(research_id, lab_datetime, value_raw, source)` | 200 | 200 | 0 | 0 | ok |
| `canonical_labs_thyroglobulin_v1` | `(research_id, lab_datetime, analyte, value_raw, source)` | 53,006 | 53,006 | 0 | 0 | ok |
| `canonical_labs_tsh_v1` | `(research_id, lab_datetime, value_raw, source)` | 556 | 556 | 0 | 0 | ok |
| `canonical_labs_vitamin_d_v1` | `(research_id, lab_datetime, value_raw, source)` | 86 | 86 | 0 | 0 | ok |
| `canonical_medications_events_v1` | `(research_id, evidence_span_hash)` | 7,501 | 4,989 | 0 | 2,512 | flag_event |
| `canonical_medications_patient_rollup_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_molecular_genetics_v2` | `(molecular_episode_id)` | 1,384 | 3 | 525 | 856 | flag_event |
| `canonical_molecular_genetics_from_notes_v2` | `(research_id, note_row_id, entity_type, evidence_start)` | 1,738 | 1,738 | 0 | 0 | ok |
| `canonical_operative_events_v1` | `(surgery_episode_id)` | 11,773 | 11,773 | 0 | 0 | ok |
| `canonical_operative_patient_rollup_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_operative_procedure_codes_v1` | `(procedure_mention_id)` | 21,691 | 21,691 | 0 | 0 | ok |
| `canonical_path_benign_events_v1` | `(research_id, surgery_episode_id, synoptic_row_ix, specimen_id)` | 11,688 | 11,661 | 11,688 | 0 | all_null_key |
| `canonical_path_benign_patient_rollup_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_path_gland_events_v1` | `(research_id, surgery_episode_id, synoptic_row_ix, specimen_id, gland_position)` | 28,724 | 28,714 | 28,724 | 0 | all_null_key |
| `canonical_path_gland_patient_rollup_v1` | `(research_id)` | 10,731 | 10,731 | 0 | 0 | ok |
| `canonical_path_malignant_events_v1` | `(specimen_focus_id)` | 6,689 | 3,221 | 3,026 | 442 | flag_event |
| `canonical_path_malignant_patient_rollup_v1` | `(research_id)` | 4,137 | 4,137 | 0 | 0 | ok |
| `canonical_patient_master` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_pmh_events_v1` | `(research_id, evidence_span_hash)` | 12,444 | 11,628 | 0 | 816 | flag_event |
| `canonical_pmh_patient_rollup_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_psh_events_v1` | `(research_id, evidence_span_hash)` | 3,919 | 3,686 | 0 | 233 | flag_event |
| `canonical_psh_patient_rollup_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_recurrence_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_survival_followup_v1` | `(research_id)` | 10,871 | 10,871 | 0 | 0 | ok |
| `canonical_us_lymph_node_v2` | `(research_id, us_exam_id, us_ln_id)` | 6,801 | 6,801 | 0 | 0 | ok |
| `canonical_us_nodule_v2` | `(research_id, us_exam_id, nodule_id)` | 37,579 | 37,579 | 0 | 0 | ok |
| `canonical_us_thyroid_gland_v2` | `(research_id, us_exam_id)` | 13,578 | 13,578 | 0 | 0 | ok |

## Event-table collapse flags (review-only)
* `canonical_complications_events_v1` — key `(research_id, evidence_span_hash)`, 15 collapses out of 10,954 rows · 4,420 rows have NULL in a key column (likely sparse-key artefact, not a true collapse)
* `canonical_invasion_events_v1` — key `(invasion_event_id)`, 7,578 collapses out of 51,773 rows · 3,778 rows have NULL in a key column (likely sparse-key artefact, not a true collapse)
* `canonical_medications_events_v1` — key `(research_id, evidence_span_hash)`, 2,512 collapses out of 7,501 rows
* `canonical_molecular_genetics_v2` — key `(molecular_episode_id)`, 856 collapses out of 1,384 rows · 525 rows have NULL in a key column (likely sparse-key artefact, not a true collapse)
* `canonical_path_malignant_events_v1` — key `(specimen_focus_id)`, 442 collapses out of 6,689 rows · 3,026 rows have NULL in a key column (likely sparse-key artefact, not a true collapse)
* `canonical_pmh_events_v1` — key `(research_id, evidence_span_hash)`, 816 collapses out of 12,444 rows
* `canonical_psh_events_v1` — key `(research_id, evidence_span_hash)`, 233 collapses out of 3,919 rows

## Sham keys (key column entirely NULL)
* `canonical_path_benign_events_v1` — chosen key `(research_id, surgery_episode_id, synoptic_row_ix, specimen_id)` is NULL on all 11,688 rows; pick another key for this table
* `canonical_path_gland_events_v1` — chosen key `(research_id, surgery_episode_id, synoptic_row_ix, specimen_id, gland_position)` is NULL on all 28,724 rows; pick another key for this table
