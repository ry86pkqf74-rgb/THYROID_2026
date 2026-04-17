# Phase 7 verification (canonical cleanup 20260417)

_Generated 2026-04-17T10:44:11.807059+00:00; database `thyroid_canonical_publication_v1_0`._

## Replay queries (zero-mismatch assertions)

| Replay | Value | Pass |
|---|---:|---|
| `PROMPT18_2_1_unconfirmed_vc_s236` | 0 | PASS (expected 0) |
| `PROMPT18_3_1_oed_TRUE_cpm_not_TRUE` | 0 | PASS (expected 0) |
| `PROMPT18_6_unfixed_duration_unknown` | 0 | PASS (expected 0) |
| `PROMPT18_6_queued_contradictions` | 4 | PASS (expected 4) |
| `PART2_1_1_tirads_unsynced` | 0 | PASS (expected 0) |
| `PART2_2_1_fna_count_diff` | 0 | PASS (expected 0) |
| `PART2_3_1_rai_dose_unsynced` | 0 | PASS (expected 0) |
| `PART2_3_3_tg_count_diff` | 0 | PASS (expected 0) |
| `PART2_3_3_tgab_count_diff` | 0 | PASS (expected 0) |
| `PART2_3_4_tg_peak_diff` | 0 | PASS (expected 0) |
| `PART2_3_4_tg_nadir_diff` | 0 | PASS (expected 0) |
| `PART2_5_3_aggregate_diff` | 0 | PASS (expected 0) |

## n_fna_episodes distribution at n in (11,12)

| n | n_patients |
|---:|---:|
| 11 | 2 |
| 12 | 3 |

## Multifocal post-state

- multifocal_flag_path = TRUE: 1440
- NLP-corroborated TRUE: 559

## Canonical state (Phase 7.2)

| Metric | Value |
|---|---:|
| `cpm_rows` | 10871 |
| `cpm_distinct_research_id` | 10871 |
| `main_object_count` | 115 |
| `manuscript_workspace_view_count` | 67 |
| `archive_pub_v1_0_table_count` | 182 |

## New CPM columns added by this run

- `cpm_built_at`: present
- `comp_hypopara_permanent_source`: present
- `lateral_neck_dissected_structured_or_nlp`: present

## Held for adjudication

- `phase_2_2_contradictions_queue_count`: 4
- `phase_3_1_lab_orphans`: 403
- `phase_3_1_likely_non_cancer`: 403
- `phase_3_1_likely_dropped_from_CPM`: 0
- `phase_3_2_us_placeholders`: ['2332', '2445', '7744']
- `phase_4_4_path_size_violators`: 80
- `phase_4_6_views_with_bare_ajcc8_t_stage`: 9


**Overall replay status: ALL PASS**

