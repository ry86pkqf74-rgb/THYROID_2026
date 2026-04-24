# Cursor Prompt — Migration 58 — Parathyroid Detail Tier-2 canonical

**Date:** 2026-04-24
**DB:** `thyroid_canonical_publication_v1_0`
**Source:** `main.note_entities_llm_parathyroid_detail_v1`
  - Expected ~8,697 candidate rows across ~5,386 patients (per manifest `expected_rows_est`).
  - Runner still in-flight on RunPod pod `fs3y285hljn82q` at time of drafting (2026-04-24).
  - **Do not run this migration until the loader table lands on MotherDuck with err=0 (verify with `scripts/llm_batch/_verify_loaded.py`).**

**Deliverables:**
1. `main.canonical_parathyroid_events_v1` — one row per (research_id, note_row_id)
2. `main.canonical_parathyroid_patient_rollup_v1` — one row per research_id
3. Registry entry in `detail_table_registry_v1`

## Why

Nothing else in the lakehouse captures gland-count accounting (identified / preserved / autotransplanted / inadvertently removed), autotransplant location, incidental-parathyroidectomy detection, or postop hypocalcemia / permanent-hypoparathyroidism adjudication. This is the foundational table for any downstream parathyroid-complication analysis.

## Source schema (from `scripts/llm_batch/prompts/parathyroid_detail.txt`)

| field | type | values |
|---|---|---|
| `glands_identified_count`       | int 0–4 or null | |
| `glands_preserved_in_situ`      | int 0–4 or null | |
| `glands_autotransplanted`       | int 0–4 or null | |
| `glands_inadvertently_removed`  | int 0–4 or null | |
| `autotransplant_location`       | `SCM` / `forearm` / `other` / null | |
| `parathyroid_pathology`         | `normal` / `hyperplasia` / `adenoma` / `parathyromatosis` / `not_assessed` / null | |
| `incidental_parathyroidectomy`  | `present` / `absent` / `unknown` | |
| `hypocalcemia_postop`           | `present` / `absent` / `unknown` | |
| `hypoparathyroidism_permanent`  | `present` / `absent` / `unknown` | |
| `intact_pth_value_ngL`          | float or null | |
| `confidence`                    | `high` / `medium` / `low` | |
| `evidence_quote`                | short quote | |
| `reasoning`                     | one sentence | |

## Pre-flight (run first)

```sql
-- Sanity
SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS pts, SUM(error) AS err, MAX(build_ts) AS latest
FROM main.note_entities_llm_parathyroid_detail_v1;

-- Parsed-JSON key coverage (expect all 13 keys in most rows)
WITH k AS (
  SELECT unnest(json_keys(parsed_json)) AS key
  FROM main.note_entities_llm_parathyroid_detail_v1
  WHERE parsed_json IS NOT NULL
)
SELECT key, COUNT(*) AS n FROM k GROUP BY 1 ORDER BY n DESC;
```

If row count ≠ ~8,697 or err > 0, stop and triage before building the canonical tables.

## Events table

```sql
CREATE OR REPLACE TABLE main.canonical_parathyroid_events_v1 AS
SELECT
  note_row_id                                                         AS parathyroid_event_id,
  research_id::VARCHAR                                                AS research_id,
  note_type, note_index,
  source_workbook, source_sheet, source_column,
  TRY_CAST(json_extract_string(parsed_json, '$.glands_identified_count')      AS INTEGER) AS glands_identified_count,
  TRY_CAST(json_extract_string(parsed_json, '$.glands_preserved_in_situ')     AS INTEGER) AS glands_preserved_in_situ,
  TRY_CAST(json_extract_string(parsed_json, '$.glands_autotransplanted')      AS INTEGER) AS glands_autotransplanted,
  TRY_CAST(json_extract_string(parsed_json, '$.glands_inadvertently_removed') AS INTEGER) AS glands_inadvertently_removed,
  json_extract_string(parsed_json, '$.autotransplant_location')               AS autotransplant_location,
  json_extract_string(parsed_json, '$.parathyroid_pathology')                 AS parathyroid_pathology,
  json_extract_string(parsed_json, '$.incidental_parathyroidectomy')          AS incidental_parathyroidectomy,
  json_extract_string(parsed_json, '$.hypocalcemia_postop')                   AS hypocalcemia_postop,
  json_extract_string(parsed_json, '$.hypoparathyroidism_permanent')          AS hypoparathyroidism_permanent,
  TRY_CAST(json_extract_string(parsed_json, '$.intact_pth_value_ngL')  AS DOUBLE) AS intact_pth_value_ngL,
  json_extract_string(parsed_json, '$.confidence')                            AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')                        AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                             AS reasoning,
  llm_model, extracted_at, build_ts AS llm_build_ts,
  'mig_58_parathyroid_detail_20260424'                                        AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                        AS build_ts
FROM main.note_entities_llm_parathyroid_detail_v1
WHERE error = 0;
```

## Patient rollup

The tricky piece is that gland counts can appear on many notes per patient; pick the **maximum across all notes** as the "ever documented" count. Incidental-parathyroidectomy and hypocalcemia use `BOOL_OR(=present)`.

```sql
CREATE OR REPLACE TABLE main.canonical_parathyroid_patient_rollup_v1 AS
WITH agg AS (
  SELECT
    research_id,
    COUNT(*)                                                                       AS n_events,
    MAX(glands_identified_count)                                                   AS max_glands_identified,
    MAX(glands_preserved_in_situ)                                                  AS max_glands_preserved,
    MAX(glands_autotransplanted)                                                   AS max_glands_autotransplanted,
    MAX(glands_inadvertently_removed)                                              AS max_glands_inadvertently_removed,
    STRING_AGG(DISTINCT autotransplant_location, ';')
      FILTER (WHERE autotransplant_location IS NOT NULL)                           AS autotransplant_locations,
    STRING_AGG(DISTINCT parathyroid_pathology, ';')
      FILTER (WHERE parathyroid_pathology IS NOT NULL)                             AS parathyroid_pathologies,
    BOOL_OR(incidental_parathyroidectomy = 'present')                              AS any_incidental_parathyroidectomy,
    BOOL_OR(hypocalcemia_postop          = 'present')                              AS any_hypocalcemia_postop,
    BOOL_OR(hypoparathyroidism_permanent = 'present')                              AS any_permanent_hypoparathyroidism,
    BOOL_OR(glands_autotransplanted >= 1)                                          AS any_autotransplant,
    MIN(intact_pth_value_ngL)                                                      AS min_intact_pth_value_ngL,
    MAX(intact_pth_value_ngL)                                                      AS max_intact_pth_value_ngL
  FROM main.canonical_parathyroid_events_v1
  GROUP BY research_id
)
SELECT
  research_id, n_events,
  max_glands_identified, max_glands_preserved,
  max_glands_autotransplanted, max_glands_inadvertently_removed,
  autotransplant_locations, parathyroid_pathologies,
  any_autotransplant,
  any_incidental_parathyroidectomy,
  any_hypocalcemia_postop,
  any_permanent_hypoparathyroidism,
  min_intact_pth_value_ngL, max_intact_pth_value_ngL,
  'mig_58_parathyroid_detail_20260424'                                              AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                              AS build_ts
FROM agg;
```

**Expected counts:** ~5,386 patients.
- `any_autotransplant` is the sentinel metric — published rates vary 5–15%; expect ~300–800 pts.
- `any_permanent_hypoparathyroidism` should be <5% of total thyroidectomy patients.

## QA

1. Row counts: events ≈ rows-in-loader-table-with-err=0; rollup ≈ 5,386.
2. `SELECT max_glands_identified, COUNT(*) FROM main.canonical_parathyroid_patient_rollup_v1 GROUP BY 1;` — expect 4 > 3 > null distribution; patients with >4 should be zero (contract violation).
3. `SELECT any_autotransplant, any_hypocalcemia_postop, COUNT(*) FROM ... GROUP BY 1,2;` — both-TRUE is a meaningful risk subgroup.
4. iPTH sanity: `SELECT COUNT(*), MIN(min_intact_pth_value_ngL), MAX(max_intact_pth_value_ngL) FROM main.canonical_parathyroid_patient_rollup_v1 WHERE max_intact_pth_value_ngL IS NOT NULL;` — values should be in 0–500 range; anything beyond 2000 likely a unit error from the LLM.
5. Reconcile with path-synoptic parathyroid columns (the ~5,386-pt cohort was defined to include them): patients with structured parathyroid tissue hits should also have `any_incidental_parathyroidectomy=TRUE` or at least gland counts populated.

## Deprecations / carry-forward

- Retire any Tier-1 `nlp_*_parathyroid_*` flags once this table is verified.
- Register both new tables in `detail_table_registry_v1`.
- This table unblocks a follow-on "complications" canonical that joins on surgery_episode_id.

## Memory hooks

Close-out as `project_mig_58_parathyroid_detail_closeout.md` with:
- final event / patient counts
- autotransplant rate + distribution by location
- hypocalcemia / permanent hypoparathyroidism rates
- gland-count histogram
- suspected LLM-unit-error iPTH values (for cleanup pass)
- carry-forwards (which nlp_* flags retired, which Script 36x build should source from this)
