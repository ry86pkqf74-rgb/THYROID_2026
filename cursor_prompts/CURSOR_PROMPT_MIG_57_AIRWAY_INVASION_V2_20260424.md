# Cursor Prompt — Migration 57 — Airway Invasion v2 Tier-2 canonical

**Date:** 2026-04-24
**DB:** `thyroid_canonical_publication_v1_0`
**Source:** `main.note_entities_llm_airway_invasion_v2` (6,054 rows / 2,820 patients, err=0, build `9b82651`)
**Deliverables:**
1. `main.canonical_airway_invasion_events_v1` — one row per (research_id, note_row_id)
2. `main.canonical_airway_invasion_patient_rollup_v1` — one row per research_id
3. Deprecate legacy `note_entities_llm_airway_invasion` (non-v2) table.

## Why

The v2 run decomposes airway involvement into anatomically distinct layers (tracheal, laryngeal, cricoid, RLN, esophageal) plus a tracheal-invasion depth ladder (mucosal → cartilage → full_thickness). This matters for pT4a staging (AJCC8 `T4a` = gross ETE into a specific neighbor organ) and surgical planning (shave vs window vs resection).

`canonical_invasion_patient_rollup_v1.any_airway_anywhere` and `any_tracheal_*` exist but are coarse unions. This table refines them. `canonical_esophageal_invasion_events_v1` / `_patient_rollup_v1` already exist (188 events / 60 pts) — consider whether the esophageal portion of this table merges into that one or stays separate (recommended: keep separate; document overlap).

## Source shape

| field | non-null dist (6,054 rows) |
|---|---|
| `tracheal_invasion` | unknown=4263, absent=1676, present=67, shaved=46 |
| `tracheal_invasion_depth` | null=6035, full_thickness=7, cartilage=6, adventitia=5, mucosal=1 |
| `laryngeal_invasion` | unknown=4625, absent=1379, present=48 |
| `cricoid_invasion` | (probe — not in top 50 sample, but field is present) |
| `rln_invasion` | unknown=3936, absent=1995, present=121 |
| `rln_paralysis_preop` | usually `unknown` |
| `esophageal_invasion` | mostly `unknown` |
| `t4a_implication` | unable_to_determine=5202, not_pT4a=654, pT4a=196 |
| `confidence`, `evidence_quote`, `reasoning` | per-event |

Note the `shaved` value on tracheal_invasion — that's an intentional third category for "partial-thickness shave resection" cases where the tracheal wall was not full-thickness involved but surgical technique had to accommodate.

## Events table

```sql
CREATE OR REPLACE TABLE main.canonical_airway_invasion_events_v1 AS
SELECT
  note_row_id                                                     AS airway_event_id,
  research_id::VARCHAR                                            AS research_id,
  note_type, note_index,
  source_workbook, source_sheet, source_column,
  json_extract_string(parsed_json, '$.tracheal_invasion')          AS tracheal_invasion,
  json_extract_string(parsed_json, '$.tracheal_invasion_depth')    AS tracheal_invasion_depth,
  json_extract_string(parsed_json, '$.laryngeal_invasion')         AS laryngeal_invasion,
  json_extract_string(parsed_json, '$.cricoid_invasion')           AS cricoid_invasion,
  json_extract_string(parsed_json, '$.rln_invasion')               AS rln_invasion,
  json_extract_string(parsed_json, '$.rln_paralysis_preop')        AS rln_paralysis_preop,
  json_extract_string(parsed_json, '$.esophageal_invasion')        AS esophageal_invasion,
  json_extract_string(parsed_json, '$.t4a_implication')            AS t4a_implication,
  json_extract_string(parsed_json, '$.confidence')                 AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')             AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                  AS reasoning,
  llm_model, extracted_at, build_ts AS llm_build_ts,
  'mig_57_airway_invasion_v2_20260424'                            AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                            AS build_ts
FROM main.note_entities_llm_airway_invasion_v2
WHERE error = 0;
```

## Patient rollup

```sql
CREATE OR REPLACE TABLE main.canonical_airway_invasion_patient_rollup_v1 AS
WITH agg AS (
  SELECT
    research_id,
    COUNT(*)                                                              AS n_events,
    BOOL_OR(tracheal_invasion   IN ('present','shaved'))                  AS any_tracheal_involvement,
    BOOL_OR(tracheal_invasion   =  'present')                             AS any_tracheal_invasion_present,
    BOOL_OR(tracheal_invasion   =  'shaved')                              AS any_tracheal_shave,
    BOOL_OR(laryngeal_invasion  =  'present')                             AS any_laryngeal_invasion,
    BOOL_OR(cricoid_invasion    =  'present')                             AS any_cricoid_invasion,
    BOOL_OR(rln_invasion        =  'present')                             AS any_rln_invasion,
    BOOL_OR(rln_paralysis_preop =  'present')                             AS any_rln_paralysis_preop,
    BOOL_OR(esophageal_invasion =  'present')                             AS any_esophageal_invasion,
    BOOL_OR(t4a_implication     =  'pT4a')                                AS any_pT4a_direct,
    -- depth: full_thickness > cartilage > adventitia > mucosal > null
    MAX(CASE tracheal_invasion_depth
          WHEN 'full_thickness' THEN 4
          WHEN 'cartilage'      THEN 3
          WHEN 'adventitia'     THEN 2
          WHEN 'mucosal'        THEN 1
          ELSE 0 END)                                                     AS max_tracheal_depth_rank,
    SUM(CASE WHEN tracheal_invasion='present'  THEN 1 ELSE 0 END)         AS n_tracheal_present,
    SUM(CASE WHEN tracheal_invasion='shaved'   THEN 1 ELSE 0 END)         AS n_tracheal_shaved,
    SUM(CASE WHEN laryngeal_invasion='present' THEN 1 ELSE 0 END)         AS n_laryngeal_present,
    SUM(CASE WHEN rln_invasion='present'       THEN 1 ELSE 0 END)         AS n_rln_present,
    SUM(CASE WHEN t4a_implication='pT4a'       THEN 1 ELSE 0 END)         AS n_pT4a_events
  FROM main.canonical_airway_invasion_events_v1
  GROUP BY research_id
)
SELECT
  research_id, n_events,
  any_tracheal_involvement, any_tracheal_invasion_present, any_tracheal_shave,
  any_laryngeal_invasion, any_cricoid_invasion, any_rln_invasion,
  any_rln_paralysis_preop, any_esophageal_invasion,
  any_pT4a_direct,
  (any_tracheal_invasion_present OR any_laryngeal_invasion OR any_cricoid_invasion
   OR any_rln_invasion OR any_esophageal_invasion OR any_pT4a_direct)     AS any_pT4a_final,
  CASE max_tracheal_depth_rank
    WHEN 4 THEN 'full_thickness'
    WHEN 3 THEN 'cartilage'
    WHEN 2 THEN 'adventitia'
    WHEN 1 THEN 'mucosal'
    ELSE NULL
  END                                                                     AS worst_tracheal_depth,
  n_tracheal_present, n_tracheal_shaved, n_laryngeal_present, n_rln_present, n_pT4a_events,
  'mig_57_airway_invasion_v2_20260424'                                    AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                    AS build_ts
FROM agg;
```

**Expected counts:** 2,820 patients. `any_pT4a_final` should be ≥196 (196 direct calls + a handful of component-based) but well below ~300.

## QA

1. Row counts: events=6,054; rollup=2,820.
2. `SELECT any_pT4a_final, COUNT(*) FROM main.canonical_airway_invasion_patient_rollup_v1 GROUP BY 1;` — TRUE ≈ 200–280.
3. `SELECT worst_tracheal_depth, COUNT(*) FROM ... GROUP BY 1;` — most NULL; full_thickness / cartilage should each be <20 patients.
4. Reconcile with `canonical_invasion_patient_rollup_v1.any_tracheal_anywhere` — agreement > 90%; disagreements = LLM-enriched hits.
5. Cross-check `any_rln_invasion=TRUE` against any preop imaging that flagged vocal-cord paralysis (sanity).

## Deprecations / carry-forward

- Demote `main.note_entities_llm_airway_invasion` (non-v2).
- Follow-up migration should rebuild `canonical_invasion_patient_rollup_v1.any_airway_*` / `any_tracheal_*` from this table + the structured feeder.
- Register in `detail_table_registry_v1`.

## Memory hooks

Close-out as `project_mig_57_airway_invasion_v2_closeout.md`:
- final event / patient counts
- depth-ladder distribution
- pT4a_final reconciliation with path_malignant.t_stage='T4a'
- list of deprecated objects
