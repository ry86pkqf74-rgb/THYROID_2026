# path_gland_repair_20260428

## Scope

Repair verification blockers for `main.canonical_path_gland_events_v1` in MotherDuck database `thyroid_canonical_publication_v1_0`.

## Root cause

- Original builder: `scripts/361_op_path_consolidation.py` Step 3.
- Later rebuild: `scripts/396_specimen_master_repair.py`, which set `build_script='396'` but was not the original builder.
- Both builders intentionally left `synoptic_row_ix` NULL because the Script 108 global row index is pandas load-order based, not SQL-reproducible from `main.path_synoptics` alone.
- Width/depth had not been parsed from multidimensional gland-size strings.
- Missing `surgery_episode_id` rows were mostly exact-date operative matches with no specimen ID.
- Parathyroid `gland_position` stored ordinal slot values (`1`-`6`) rather than the ratified taxonomy.

## Repair implementation

Script: `scripts/397_path_gland_repair.py`

- Dry-run/apply CLI with locked MotherDuck connection through `scripts._md_connect.connect_locked()`.
- Backfilled `synoptic_row_ix` from path_synoptics parquet load order per `scripts/108_synoptic_tumor_long_v1.py`.
  - Local DVC working copy was absent; used `scripts/output/parquet_backup/path_synoptics.parquet`, preserving the pandas load-order anchor.
- Parsed `gland_width_cm` and `gland_depth_cm` from the second/third numeric dimensions of source size strings.
- Backfilled `surgery_episode_id` only for unambiguous exact-date matches to `main.canonical_operative_events_v1`, excluding `date_status='opnote_clustered'`.
- Normalized parathyroid positions to:
  - `right_superior`
  - `right_inferior`
  - `left_superior`
  - `left_inferior`
  - `intrathyroidal_right`
  - `intrathyroidal_left`
  - `extrathyroidal_other`
  - NULL for empty/unparseable source location.
- Refreshed `views_readable.path_gland_events_VIEW_v1`.
- Updated column/table comments, `canonical_column_verification_registry_v1`, `canonical_table_signoff_registry_v1`, and `manuscript_workspace.cpm_reconciliation_provenance_v1`.

## Archive snapshot

Pre-repair snapshot created at:

`"Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_path_gland_events_v1_pre_repair_20260428T235938Z"`

A prior failed attempt created an archive snapshot before hitting the MotherDuck single-database transaction rule. Script 397 was patched so archive snapshot creation happens outside the publication-DB transaction.

## Output artifacts

- `scripts/output/path_gland_position_audit.csv`
- `scripts/output/path_gland_repair_report.md`

## Verification results

| Metric | Pre | Post |
|---|---:|---:|
| Rows | 28,724 | 28,724 |
| `synoptic_row_ix` NULL | 28,724 | 0 |
| `gland_width_cm` NULL | 28,724 | 8,784 |
| `gland_depth_cm` NULL | 28,724 | 8,924 |
| `surgery_episode_id` NULL | 6,172 | 52 |
| Bad parathyroid positions | 4,025 | 0 |
| Parathyroid NULL positions | 0 | 294 |
| `linkage_quality='unlinked'` | 6,132 | 12 |

Registry/signoff:

- `table_status`: `verified`
- `n_columns_total`: 20
- `n_verified`: 15
- `n_not_started`: 0
- `n_failed`: 0
- `n_na`: 5
- `signoff_migration`: `path_gland_repair_20260428`

## Residuals / caveats

- 52 rows remain without `surgery_episode_id`: 40 `synoptic_only` rows plus 12 truly unlinked after exact-date operative matching.
- 294 parathyroid source locations were empty or unparseable and were intentionally left NULL rather than inventing taxonomy values.
- Range outliers were retained as source-faithful values and documented in the report for manual review:
  - `research_id=9588`, right thyroid lobe, length 85.6 cm.
  - `research_id=2471`, total thyroid, weight 1007.1 g.
  - `research_id=6395`, left/right thyroid lobes, weights 1207.0 g and 1113.0 g.

## Commands run

- `python3 -m py_compile scripts/397_path_gland_repair.py`
- `.venv/bin/python scripts/397_path_gland_repair.py --dry-run`
- `.venv/bin/python scripts/397_path_gland_repair.py --apply`
- Independent post-apply MotherDuck verification query for null rates, registry, and provenance.
