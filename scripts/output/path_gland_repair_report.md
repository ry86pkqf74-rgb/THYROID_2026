# canonical_path_gland_events_v1 Repair Report

- Mode: `apply`
- Batch: `path_gland_repair_20260428`
- Generated: `2026-04-28T23:59:43.554221+00:00`
- Snapshot: `"Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_path_gland_events_v1_pre_repair_20260428T235938Z"`
- Position audit CSV: `scripts/output/path_gland_position_audit.csv`

## Investigation summary

- Original builder found: `scripts/361_op_path_consolidation.py` Step 3; `scripts/396_specimen_master_repair.py` rebuilt the table after specimen-master repair and left `synoptic_row_ix` NULL by design.
- Script 108 defines `synoptic_row_ix` as pandas load-order index from the path_synoptics parquet; live MotherDuck `path_synoptics` does not carry that column. This run used the parquet path listed in the stage summary.
- Width/depth source fields are not separate columns; they are the 2nd/3rd numeric dimensions embedded in gland size strings such as `rl_size_cm`, `ll_size_cm`, `total_thyroid_size`, and `parag_<N>_size`.

## Stage summary

- `path_synoptics_parquet_source`: scripts/output/parquet_backup/path_synoptics.parquet
- `processed_path_synoptics_rows`: 11688
- `stage_rows`: 140256
- `audit_distinct_source_locations`: 538
- `audit_unparseable_distinct_locations`: 190
- `md_path_synoptics_rows`: 11688
- `stage_rows_loaded`: 140256
- `matched_gland_rows`: 28724
- `exact_unique_episode_backfill_rows`: 6120

## Pre/post metrics

| metric | pre | post |
|---|---:|---:|
| `row_count` | 28724 | 28724 |
| `patients` | 10731 | 10731 |
| `null_synoptic_row_ix` | 28724 | 0 |
| `null_width` | 28724 | 8784 |
| `null_depth` | 28724 | 8924 |
| `null_surgery_episode_id` | 6172 | 52 |
| `parathyroid_bad_position_rows` | 4025 | 0 |
| `parathyroid_null_position_rows` | 0 | 294 |
| `parathyroid_rows` | 4025 | 4025 |
| `max_gland_length_cm` | 85.6 | 85.6 |
| `max_gland_weight_g` | 1207.0 | 1207.0 |
| `full_rows` | 22552 | 22552 |
| `specimen_only_rows` | 0 | 6120 |
| `synoptic_only_rows` | 40 | 40 |
| `unlinked_rows` | 6132 | 12 |

## Range outlier review

| research_id | gland_type | gland_position | gland_length_cm | gland_weight_g | review_disposition |
|---:|---|---|---:|---:|---|
| 9588 | thyroid_lobe | right | 85.6 | 18.0 | source_size_string_contains_extreme_first_dimension; retained for manual review |
| 2471 | thyroid_lobe | total | 18.0 | 1007.1 | large_goiter_weight_from_structured_weight_field; retained for manual review |
| 6395 | thyroid_lobe | left | 15.8 | 1207.0 | large_goiter_weight_from_structured_weight_field; retained for manual review |
| 6395 | thyroid_lobe | right | 15.5 | 1113.0 | large_goiter_weight_from_structured_weight_field; retained for manual review |

## Registry/signoff

- `n_columns_total`: 20
- `n_verified`: 15
- `n_not_started`: 0
- `n_failed`: 0
- `n_na`: 5
- `table_status`: verified
- `signoff_migration`: path_gland_repair_20260428
- `registry_not_started`: 0
- `registry_failed`: 0

## Acceptance gates

- `synoptic_row_ix_nonnull_rate`: 100.0000%
- `parathyroid_bad_position_rows`: 0
- `surgery_episode_id_null_rows`: 52
- `registry_table_status`: verified
