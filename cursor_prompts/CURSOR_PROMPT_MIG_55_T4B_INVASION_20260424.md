# Cursor Prompt — Migration 55 — T4b Invasion Tier-2 canonical

**Date:** 2026-04-24
**DB:** `thyroid_canonical_publication_v1_0`
**Source:** `main.note_entities_llm_t4b_invasion_v1` (944 rows / 434 patients, err=0, build `9b82651`)
**Deliverables:**
1. `main.canonical_t4b_invasion_events_v1` — one row per (research_id, note_row_id)
2. `main.canonical_t4b_invasion_patient_rollup_v1` — one row per research_id
3. Downstream: patch `canonical_ete_subgrade_patient_rollup_v1.any_pT4b` to cross-check against this.

## Why

Closes `qc_framework_v1/LLM_TODO.md` T4b parsing task (per carry-forward from Task #7). The 411-script Tier-1 lookup cannot distinguish prevertebral fascia from carotid from mediastinal-vessel invasion — gpt-oss-120b can. Without this, AJCC8 pT4b staging is fundamentally unattributable.

## Source shape

`parsed_json` carries three component fields + one roll-up and supporting context:

| field | non-null dist (944 rows) |
|---|---|
| `prevertebral_fascia_invasion` | unknown=937, absent=7 |
| `carotid_encasement` | unknown=908, absent=20, present=16 |
| `mediastinal_vessel_invasion` | unknown=930, absent=8, present=6 |
| `t4b_implication` | unable_to_determine=926, pT4b=18 |
| `confidence` | short label |
| `evidence_quote` / `reasoning` | verbatim + rationale |

Note the sparsity: only ~18 pT4b calls in 944 rows. This is expected — pT4b is a rare disease in this cohort — and the tier-2 rollup should preserve that rarity, not inflate it.

## Events table

```sql
CREATE OR REPLACE TABLE main.canonical_t4b_invasion_events_v1 AS
SELECT
  note_row_id                                                     AS t4b_event_id,
  research_id::VARCHAR                                            AS research_id,
  note_type, note_index,
  source_workbook, source_sheet, source_column,
  json_extract_string(parsed_json, '$.prevertebral_fascia_invasion')  AS prevertebral_fascia_invasion,
  json_extract_string(parsed_json, '$.carotid_encasement')             AS carotid_encasement,
  json_extract_string(parsed_json, '$.mediastinal_vessel_invasion')    AS mediastinal_vessel_invasion,
  json_extract_string(parsed_json, '$.t4b_implication')                AS t4b_implication,
  json_extract_string(parsed_json, '$.confidence')                     AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')                 AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                      AS reasoning,
  llm_model, extracted_at, build_ts AS llm_build_ts,
  'mig_55_t4b_invasion_20260424'                                      AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                AS build_ts
FROM main.note_entities_llm_t4b_invasion_v1
WHERE error = 0;
```

## Patient rollup

Any `present` on any component = that component positive for the patient. `pT4b_final` is positive if ANY `t4b_implication='pT4b'` event OR any component=`present`.

```sql
CREATE OR REPLACE TABLE main.canonical_t4b_invasion_patient_rollup_v1 AS
WITH agg AS (
  SELECT
    research_id,
    COUNT(*)                                                                                  AS n_events,
    BOOL_OR(prevertebral_fascia_invasion='present')                                           AS any_prevertebral_fascia,
    BOOL_OR(carotid_encasement='present')                                                     AS any_carotid_encasement,
    BOOL_OR(mediastinal_vessel_invasion='present')                                            AS any_mediastinal_vessel,
    BOOL_OR(t4b_implication='pT4b')                                                           AS any_pT4b_direct,
    SUM(CASE WHEN t4b_implication='pT4b' THEN 1 ELSE 0 END)                                   AS n_pT4b_events,
    SUM(CASE WHEN prevertebral_fascia_invasion='present' THEN 1 ELSE 0 END)                   AS n_prevertebral_events,
    SUM(CASE WHEN carotid_encasement='present'           THEN 1 ELSE 0 END)                   AS n_carotid_events,
    SUM(CASE WHEN mediastinal_vessel_invasion='present'  THEN 1 ELSE 0 END)                   AS n_mediastinal_events
  FROM main.canonical_t4b_invasion_events_v1
  GROUP BY research_id
)
SELECT
  research_id,
  n_events,
  any_prevertebral_fascia,
  any_carotid_encasement,
  any_mediastinal_vessel,
  any_pT4b_direct,
  (any_prevertebral_fascia OR any_carotid_encasement OR any_mediastinal_vessel OR any_pT4b_direct)
                                                                                              AS any_pT4b_final,
  n_pT4b_events, n_prevertebral_events, n_carotid_events, n_mediastinal_events,
  'mig_55_t4b_invasion_20260424'                                                              AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                                        AS build_ts
FROM agg;
```

**Expected counts:** 434 patients; `any_pT4b_final` should be ~30–40 (18 direct + a handful more from component-based inference; guard against inflation).

## QA

1. `SELECT COUNT(*) FROM main.canonical_t4b_invasion_events_v1` → 944.
2. `SELECT COUNT(*), SUM(any_pT4b_final::INT) FROM main.canonical_t4b_invasion_patient_rollup_v1;` → 434 patients, ~30–40 pT4b.
3. Reconcile with `canonical_ete_subgrade_patient_rollup_v1.any_pT4b`: patients with ETE=pT4b should mostly also be in T4b rollup's `any_pT4b_final` — cases where they disagree are worth flagging.
4. Hits should cluster in pre-existing path_malignant.t_stage IN ('T4a','T4b') or gross_ete=1; any patient outside that pool with `any_pT4b_final=TRUE` deserves manual review.

## Deprecations / carry-forward

- Retire the 411 Tier-1 `nlp_*_t4b_*` flags on CPM once this table is verified (pattern from `project_cpm_llm_parse_architecture`).
- `qc_framework_v1/LLM_TODO.md` T4b entry moves from TODO → DONE; add close-out link.
- Register in `detail_table_registry_v1`.

## Memory hooks

Close-out as `project_mig_55_t4b_invasion_closeout.md`:
- final SHA
- pT4b_final counts + overlap with ETE pT4b
- which component drove each non-obvious pT4b call (free-text audit)
