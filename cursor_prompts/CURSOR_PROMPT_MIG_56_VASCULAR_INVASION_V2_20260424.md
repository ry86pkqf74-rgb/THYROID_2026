# Cursor Prompt — Migration 56 — Vascular / Lymphatic / Perineural Invasion v2 Tier-2 canonical

**Date:** 2026-04-24
**DB:** `thyroid_canonical_publication_v1_0`
**Source:** `main.note_entities_llm_vascular_invasion_v2` (3,861 rows / 3,745 patients, err=0, build `9b82651`)
**Deliverables:**
1. `main.canonical_vascular_invasion_events_v1` — one row per (research_id, note_row_id)
2. `main.canonical_vascular_invasion_patient_rollup_v1` — one row per research_id
3. Deprecate the legacy `note_entities_llm_vascular_invasion` (non-v2) table — demote with `_deprecated_` prefix when ready.

## Why

The `v2` run separates vascular, lymphatic, perineural into distinct fields plus a `vascular_invasion_extent` (focal / extensive / minimal / widely_invasive). That distinction matters for ATA risk stratification and disease-free survival modeling — collapsing LVI into one flag (`lvi_collapsed`) discards real prognostic information.

This table supersedes the `any_vascular_microscopic_*`, `any_lymphatic_microscopic_*`, `any_perineural_*` columns in `canonical_invasion_patient_rollup_v1` for the mentions-derived layer. (Path synoptic-sourced flags stay; the combined roll-up should be rebuilt to source from this table + the path-synoptic feeder.)

## Source shape

| field | non-null dist (3,861 rows) |
|---|---|
| `vascular_invasion` | absent=2985, present=742, unknown=134 |
| `vascular_invasion_extent` | null=3402, focal=212, extensive=175, minimal=58, widely_invasive=14 |
| `lymphatic_invasion` | absent=2408, present=889, unknown=564 |
| `perineural_invasion` | unknown=2398, absent=1360, present=103 |
| `lvi_collapsed` | absent=2560, present=1193, unknown=108 |
| `tumor_type_context` | e.g. `papillary` / `follicular` |
| `vessel_count` | integer when quoted, else null |
| `confidence`, `evidence_quote`, `reasoning` | per-event |

## Events table

```sql
CREATE OR REPLACE TABLE main.canonical_vascular_invasion_events_v1 AS
SELECT
  note_row_id                                                     AS vi_event_id,
  research_id::VARCHAR                                            AS research_id,
  note_type, note_index,
  source_workbook, source_sheet, source_column,
  json_extract_string(parsed_json, '$.vascular_invasion')          AS vascular_invasion,
  json_extract_string(parsed_json, '$.vascular_invasion_extent')   AS vascular_invasion_extent,
  TRY_CAST(json_extract_string(parsed_json, '$.vessel_count') AS INTEGER)
                                                                  AS vessel_count,
  json_extract_string(parsed_json, '$.lymphatic_invasion')         AS lymphatic_invasion,
  json_extract_string(parsed_json, '$.perineural_invasion')        AS perineural_invasion,
  json_extract_string(parsed_json, '$.lvi_collapsed')              AS lvi_collapsed,
  json_extract_string(parsed_json, '$.tumor_type_context')         AS tumor_type_context,
  json_extract_string(parsed_json, '$.confidence')                 AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')             AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                  AS reasoning,
  llm_model, extracted_at, build_ts AS llm_build_ts,
  'mig_56_vascular_invasion_v2_20260424'                          AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                            AS build_ts
FROM main.note_entities_llm_vascular_invasion_v2
WHERE error = 0;
```

**Expected counts:** 3,861 events; distinct research_id ≈ 3,745 (near 1:1 — most patients have one eligible note).

## Patient rollup

```sql
CREATE OR REPLACE TABLE main.canonical_vascular_invasion_patient_rollup_v1 AS
WITH agg AS (
  SELECT
    research_id,
    COUNT(*)                                                              AS n_events,
    BOOL_OR(vascular_invasion='present')                                  AS any_vascular_invasion,
    BOOL_OR(lymphatic_invasion='present')                                 AS any_lymphatic_invasion,
    BOOL_OR(perineural_invasion='present')                                AS any_perineural_invasion,
    BOOL_OR(lvi_collapsed='present')                                      AS any_lvi_collapsed,
    -- extent tiers: widely_invasive > extensive > focal > minimal > none
    MAX(CASE vascular_invasion_extent
          WHEN 'widely_invasive' THEN 4
          WHEN 'extensive'       THEN 3
          WHEN 'focal'           THEN 2
          WHEN 'minimal'         THEN 1
          ELSE 0 END)                                                     AS max_extent_rank,
    SUM(CASE WHEN vascular_invasion='present'   THEN 1 ELSE 0 END)        AS n_vi_events,
    SUM(CASE WHEN lymphatic_invasion='present'  THEN 1 ELSE 0 END)        AS n_li_events,
    SUM(CASE WHEN perineural_invasion='present' THEN 1 ELSE 0 END)        AS n_pni_events,
    MAX(vessel_count)                                                     AS max_vessel_count,
    STRING_AGG(DISTINCT tumor_type_context, ';' ORDER BY tumor_type_context)
                                                                          AS tumor_type_contexts
  FROM main.canonical_vascular_invasion_events_v1
  GROUP BY research_id
)
SELECT
  research_id,
  n_events,
  any_vascular_invasion, any_lymphatic_invasion, any_perineural_invasion, any_lvi_collapsed,
  CASE max_extent_rank
    WHEN 4 THEN 'widely_invasive'
    WHEN 3 THEN 'extensive'
    WHEN 2 THEN 'focal'
    WHEN 1 THEN 'minimal'
    ELSE NULL
  END                                                                     AS worst_extent,
  n_vi_events, n_li_events, n_pni_events,
  max_vessel_count,
  tumor_type_contexts,
  'mig_56_vascular_invasion_v2_20260424'                                  AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                    AS build_ts
FROM agg;
```

**Expected counts:** 3,745 patients.
- `any_vascular_invasion` ≈ 700+ (742 events, mild collapse for multi-event patients).
- `any_lymphatic_invasion` ≈ 800+.
- `worst_extent` non-null ≈ 459 (212+175+58+14).

## QA

1. Row counts: events=3,861; rollup=3,745.
2. `SELECT worst_extent, COUNT(*) FROM main.canonical_vascular_invasion_patient_rollup_v1 GROUP BY 1;` — expect ~3,286 NULL + ~459 with an extent.
3. Reconcile with path-synoptic-sourced VI flags in `canonical_invasion_patient_rollup_v1.any_vascular_microscopic_in_op_or_path`: agreement rate should be >85%; disagreements are worth surfacing (they're mostly "LLM found it in free-text comment, structured column was blank").
4. `any_lvi_collapsed` ≠ (`any_vascular_invasion` OR `any_lymphatic_invasion`) is a contract violation; count mismatches and investigate.

## Deprecations / carry-forward

- Demote `main.note_entities_llm_vascular_invasion` (non-v2) — prefix `_deprecated_` or move to `ms_workspace.legacy_`.
- Update `canonical_invasion_patient_rollup_v1.any_vascular_microscopic_*` build to source from this table (follow-up migration, not this one).
- Register both new tables in `detail_table_registry_v1`.

## Memory hooks

Close-out as `project_mig_56_vascular_invasion_v2_closeout.md` with:
- final event / patient counts
- extent-tier distribution
- disagreement rate vs path-synoptic flags
- list of deprecated objects
