# Cursor Prompt — Migration 54 — ETE Subgrade Tier-2 canonical

**Date:** 2026-04-24
**DB:** `thyroid_canonical_publication_v1_0`
**Source:** `main.note_entities_llm_ete_subgrade_v1` (287 rows / 151 patients, err=0, build `9b82651`)
**Deliverables:**
1. `main.canonical_ete_subgrade_events_v1` — one row per (research_id, note_row_id)
2. `main.canonical_ete_subgrade_patient_rollup_v1` — one row per research_id
3. (optional) `_VIEW_v1` flattener over the loader table if downstream needs column access

## Goal

Turn the 287-row gpt-oss-120b run into the canonical ETE-subgrade master layer that supersedes the unspec/mix of ETE signals in `canonical_invasion_events_v1 WHERE invasion_type='ete_*'`. This feeds the ETE manuscript analytic cohort (381 PTC analytic-eligible, 290 PTC patients — see memory `project_ete_documentation_rate`).

## Source shape

`parsed_json` (already `TO_JSON`'d in loader) carries:

| field | non-null dist (287 rows) |
|---|---|
| `ete_grade` | gross=142, unable_to_determine=92, microscopic=40, absent=13 |
| `ajcc8_implication` | null=138, pT3b=89, pT4a=51, pT3a_size_only=6, pT4b=3 |
| `confidence` | high=217, medium=65, low=5 |
| `evidence_quote` | short verbatim span from note |
| `reasoning` | short free-text rationale |

## Events table

```sql
CREATE OR REPLACE TABLE main.canonical_ete_subgrade_events_v1 AS
WITH parsed AS (
  SELECT
    note_row_id,
    research_id::VARCHAR                                          AS research_id,
    note_type,
    note_index,
    source_workbook,
    source_sheet,
    source_column,
    json_extract_string(parsed_json, '$.ete_grade')               AS ete_grade,
    json_extract_string(parsed_json, '$.ajcc8_implication')       AS ajcc8_implication,
    json_extract_string(parsed_json, '$.confidence')              AS confidence,
    json_extract_string(parsed_json, '$.evidence_quote')          AS evidence_quote,
    json_extract_string(parsed_json, '$.reasoning')               AS reasoning,
    llm_model,
    extracted_at,
    build_ts                                                      AS llm_build_ts
  FROM main.note_entities_llm_ete_subgrade_v1
  WHERE error = 0
)
SELECT
  note_row_id                                                     AS ete_event_id,
  research_id,
  note_type,
  note_index,
  source_workbook, source_sheet, source_column,
  CASE ete_grade
    WHEN 'gross'                 THEN 'gross'
    WHEN 'microscopic'           THEN 'microscopic'
    WHEN 'absent'                THEN 'absent'
    WHEN 'unable_to_determine'   THEN 'unknown'
    ELSE 'unknown'
  END                                                             AS ete_grade,
  ajcc8_implication,
  confidence,
  evidence_quote,
  reasoning,
  llm_model,
  extracted_at,
  llm_build_ts,
  'mig_54_ete_subgrade_20260424'                                  AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                            AS build_ts
FROM parsed;
```

**Expected counts:** 287 events / 151 patients; gross=142, microscopic=40, absent=13, unknown=92.

## Patient rollup

```sql
CREATE OR REPLACE TABLE main.canonical_ete_subgrade_patient_rollup_v1 AS
WITH ranked AS (
  SELECT
    research_id,
    ete_grade,
    ajcc8_implication,
    confidence,
    -- priority: gross > microscopic > absent > unknown
    CASE ete_grade
      WHEN 'gross'       THEN 3
      WHEN 'microscopic' THEN 2
      WHEN 'absent'      THEN 1
      ELSE 0
    END AS grade_rank,
    -- AJCC8 priority: pT4b > pT4a > pT3b > pT3a_size_only > null
    CASE ajcc8_implication
      WHEN 'pT4b'           THEN 4
      WHEN 'pT4a'           THEN 3
      WHEN 'pT3b'           THEN 2
      WHEN 'pT3a_size_only' THEN 1
      ELSE 0
    END AS ajcc_rank,
    ROW_NUMBER() OVER (
      PARTITION BY research_id
      ORDER BY
        CASE ete_grade WHEN 'gross' THEN 3 WHEN 'microscopic' THEN 2 WHEN 'absent' THEN 1 ELSE 0 END DESC,
        CASE ajcc8_implication
          WHEN 'pT4b' THEN 4 WHEN 'pT4a' THEN 3 WHEN 'pT3b' THEN 2 WHEN 'pT3a_size_only' THEN 1 ELSE 0 END DESC,
        CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM main.canonical_ete_subgrade_events_v1
),
worst AS (
  SELECT research_id, ete_grade AS worst_ete_grade, ajcc8_implication AS worst_ajcc8_implication, confidence AS worst_confidence
  FROM ranked WHERE rn = 1
),
agg AS (
  SELECT
    research_id,
    COUNT(*)                                                         AS n_events,
    SUM(CASE WHEN ete_grade='gross'       THEN 1 ELSE 0 END)         AS n_gross,
    SUM(CASE WHEN ete_grade='microscopic' THEN 1 ELSE 0 END)         AS n_microscopic,
    SUM(CASE WHEN ete_grade='absent'      THEN 1 ELSE 0 END)         AS n_absent,
    SUM(CASE WHEN ete_grade='unknown'     THEN 1 ELSE 0 END)         AS n_unknown,
    BOOL_OR(ete_grade='gross')                                       AS any_gross_ete,
    BOOL_OR(ete_grade='microscopic')                                 AS any_microscopic_ete,
    BOOL_OR(ajcc8_implication='pT4b')                                AS any_pT4b,
    BOOL_OR(ajcc8_implication='pT4a')                                AS any_pT4a,
    BOOL_OR(ajcc8_implication='pT3b')                                AS any_pT3b
  FROM main.canonical_ete_subgrade_events_v1
  GROUP BY research_id
)
SELECT
  a.research_id,
  w.worst_ete_grade,
  w.worst_ajcc8_implication,
  w.worst_confidence,
  a.n_events, a.n_gross, a.n_microscopic, a.n_absent, a.n_unknown,
  a.any_gross_ete, a.any_microscopic_ete,
  a.any_pT4b, a.any_pT4a, a.any_pT3b,
  'mig_54_ete_subgrade_20260424'                                      AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                AS build_ts
FROM agg a
LEFT JOIN worst w USING (research_id);
```

**Expected counts:** 151 patients; `any_gross_ete` ≈ the patients contributing the 142 gross events (some patients have multiple; probably ~90–110 pts).

## QA

1. Row counts: `SELECT COUNT(*) FROM main.canonical_ete_subgrade_events_v1` → 287; rollup → 151.
2. `SELECT worst_ete_grade, COUNT(*) FROM main.canonical_ete_subgrade_patient_rollup_v1 GROUP BY 1;` — gross should be largest bucket, unknown nonzero but not dominant.
3. `SELECT worst_ajcc8_implication, COUNT(*) FROM ... GROUP BY 1;` — pT3b > pT4a > pT4b order.
4. Join against the 167 PTC unspec_remaining cohort (the original migration 53 target) — how many now have `worst_ete_grade IN ('gross','microscopic','absent')`?

## Deprecations / carry-forward

- The row for this domain in `detail_table_registry_v1` should add `canonical_ete_subgrade_events_v1` and `canonical_ete_subgrade_patient_rollup_v1`. Filter column is `detail_table_name` (per memory `reference_detail_table_registry_schema`).
- `ete_adjudication_v1` (45 rows / 45 pts) is the prior adjudication layer; leave in place but flag as superseded for ETE grade.
- Downstream `canonical_invasion_patient_rollup_v1.any_gross_ete_*` / `any_microscopic_ete_*` columns should be re-sourced from this table in a future migration (Script 363+).

## Memory hooks

Log close-out as `project_mig_54_ete_subgrade_closeout.md` with:
- final commit SHA
- final event/patient counts
- cohort overlap with 167-PTC unspec_remaining
- carry-forward: retire `ete_adjudication_v1`, update `canonical_invasion_patient_rollup_v1` sourcing
