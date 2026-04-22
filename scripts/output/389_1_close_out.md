# Script 389.1 — `detail_table_registry_v1` schema migration close-out

**Date:** 2026-04-22 21:30:19Z
**Script:** `scripts/389_1_registry_schema_migration.py`
**Prompt:** `cursor_prompts/CURSOR_PROMPT_REGISTRY_SCHEMA_MIGRATION_20260422_SCRIPT_389_1.md`
**Registry:** `thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1`
**Snapshot:** `"thyroid_canonical_publication_v1_0"."archive_pub_v1_0"."detail_table_registry_v1_pre389_1_20260422T212806Z"`

## Outcome

* Registry rows before/after: **144** (unchanged)
* Snapshot rows: **144** (parity OK)
* Rows updated by backfill: **17**
* Archive snapshot tables scanned: **4**
* Registry rows with archive match (renamed_by_script set): **3**
* Rows with non-NULL `superseded_by`: **0** (deterministic mapping not in scope; backfill leaves NULL)

## New columns

| column | type | semantics |
|---|---|---|
| `superseded_by` | VARCHAR | canonical that replaced this row (NULL = still live or unknown replacement) |
| `renamed_by_script` | VARCHAR | script number that retired/renamed (NULL = never renamed) |

## `__readme` provenance row

```
Script 389.1: detail_table_registry_v1 schema migration — added superseded_by + renamed_by_script columns; backfilled 17 rows from archive_pub_v1_0 snapshot names and archive_move_log_v1. Snapshot: archive_pub_v1_0.detail_table_registry_v1_pre389_1_20260422T212806Z.
```

## Backfill detail

| detail_table_name | renamed_by_script | superseded_by |
|---|---|---|
| `_molecular_patient_rollup_v227` | `297` | NULL |
| `extracted_braf_recovery_v1` | `346` | NULL |
| `extracted_ete_subgraded_v1` | `346` | NULL |
| `extracted_fna_bethesda_v1` | `346` | NULL |
| `extracted_postop_labs_expanded_v1` | `346` | NULL |
| `extracted_ras_patient_summary_v1` | `346` | NULL |
| `frozen_section_event_v1` | `387` | NULL |
| `ln_extract_noncohort_orphan_v279` | `297` | NULL |
| `path_size_adjudication_v241` | `325` | NULL |
| `patient_tier2_master_v1` | `387` | NULL |
| `patient_tumor_rollup_v1` | `348` | NULL |
| `ret_note_entity_adjudication_v226` | `325` | NULL |
| `ret_patient_adjudicated_v226` | `325` | NULL |
| `tirads_llm_validation_v2` | `325` | NULL |
| `tirads_v2_reports_raw` | `325` | NULL |
| `tumor_pathology` | `325` | NULL |
| `vc_paralysis_recalibration_v236` | `297` | NULL |

## Idempotency

Re-running `--phase 0` after this commit will exit cleanly; re-running `--apply` will detect both new columns + `__readme` row and exit 0 with a NO-OP message.

## Carry-forwards

- **CF-1 — superseded_by backfill deferred.** All 17 migrated rows
  have `superseded_by = NULL`. The archive-name heuristic gives
  `renamed_by_script` deterministically but cannot identify the
  replacement table; that requires parsing the close-out markdowns
  (`scripts/output/{297,325,339,346,348,387}*_close_out.md`) for
  explicit retire→replace statements. Follow-up standalone pass.

- **CF-2 — 41-row orphan cohort.** Registry entries for tables that
  no longer exist in `main.*` and have no archive snapshot or
  `move_log` entry. Primarily `manuscript_workspace` audit/queue
  tables and `note_entities_llm_*` shells that were in-flight
  workspace artifacts never formally archived. Examples:
  `cpm_ajcc_dominant_concordance_v1`, `cpm_ete_self_contradiction_queue_v1`,
  `note_entities_llm_labs`, `molecular_*` family, `ultrasound_reports`,
  `us_nodules_tirads`, `survival_cohort_enriched`. Options for a
  cleanup pass: (a) hard-delete from registry; (b) mark with a
  sentinel like `renamed_by_script='manual_review_orphan'`; (c) add a
  third column `status` = `'live'` | `'retired'` | `'orphan'`. Not
  389.1's job — queued for triage.

