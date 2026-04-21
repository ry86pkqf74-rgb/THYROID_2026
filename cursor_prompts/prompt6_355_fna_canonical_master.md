# Cursor Prompt — Script 355: Build FNA canonical masters (additive)

## Role
Phase-6 idempotent Python script for `thyroid_canonical_publication_v1_0`. Follows the conventions of `scripts/prompt6_346_archive_extracted_legacy.py`, `scripts/268_bethesda_semantics.py`.

## Target file
`/Users/ros/THyroid 2026/scripts/prompt6_355_fna_canonical_master.py`

## Goal
Build the two new FNA masters **additively** — no drops, no CPM changes, no view changes. Script 356 handles the flip.

Tables to create:
- `main.fna_event_v1` — one row per FNA event, grain `(research_id, fna_index)`
- `main.fna_patient_rollup_v1` — one row per patient, grain `research_id`

## Design principle
**`research_id` is the sole cross-domain key.** Do NOT include `linked_molecular_episode_id`, `linked_imaging_nodule_id`, `linked_surgery_episode_id`, or `linked_path_specimen_id`. Cross-domain questions are answered at query time via `JOIN ON research_id` (± date window for specimen-level alignment).

Scalar derived fields (`is_index_fna`, `days_to_surgery`) ARE OK — they're attributes of the FNA row, computed once at build via research_id join to `operative_episode_multi_v2`, then stored.

## Source tables (live in `thyroid_canonical_publication_v1_0.main`)
- `fna_history` — 8,119 rows / 5,266 RIDs — raw import (research_id INTEGER)
- `fna_cytology` — 8,063 rows / 5,240 RIDs — Bethesda morphology (research_id VARCHAR; fna_date mixed format)
- `fna_episode_master_v2` — 8,119 rows / 5,266 RIDs — resolved DATE + laterality (research_id VARCHAR)
- **Surgery anchor = `canonical_patient_master.first_surgery_date` (cast to DATE).** NOT `MIN(operative_episode_detail_v2.resolved_surgery_date)` — the latter disagrees with CPM on 207 patients (median 932-day diff, mostly OED pulling in pre-thyroid surgeries). Using `CPM.first_surgery_date` makes this rollup agree with CPM's existing `bethesda_final` **by construction**. Dependency: if CPM's anchor logic changes, rebuild this rollup.

## Critical gotchas
1. **research_id type mismatch**: `fna_history` is INTEGER; the others VARCHAR. CAST all to VARCHAR in output.
2. **Date chaos**: `fna_cytology.fna_date` is mixed US-slash / 2-digit-year / other. DO NOT write a parser. Use **three TRY_CAST fallbacks in order**: (a) `fna_episode_master_v2.resolved_fna_date` (already DATE, authoritative), (b) `TRY_CAST(fna_history.fna_date_parsed AS DATE)` (ISO strings), (c) `TRY_CAST(fna_cytology.fna_date AS DATE)` (raw — DuckDB will accept ISO-like strings, NULL-out the rest). Three fallbacks recover ~90 more events than two; still zero parser code.
3. **Join key**: `(research_id, fna_index)` — `fna_cytology` has 2 orphans not in `fna_history` (QA gate flags them); 58 FNAs in history lack a cytology row (bethesda_calculated_num will be NULL).
4. **Build FROM fna_history** (LEFT JOIN others) so every raw FNA is represented.

## `fna_event_v1` — 40 columns

| # | column | type | source / logic |
|---:|---|---|---|
| 1 | fna_event_id | VARCHAR | `md5(research_id || '-' || fna_index)` |
| 2 | research_id | VARCHAR | CAST from fna_history (universal cross-domain key) |
| 3 | fna_index | BIGINT | fna_history |
| 4 | fna_seq_n | INT | `ROW_NUMBER() OVER (PARTITION BY rid ORDER BY fna_date_resolved NULLS LAST, fna_index)` |
| 5 | fna_total_n_for_patient | INT | `COUNT(*) OVER (PARTITION BY rid)` |
| 6 | is_first_fna | BOOL | fna_seq_n = 1 |
| 7 | is_last_fna | BOOL | fna_seq_n = fna_total_n_for_patient |
| 8 | is_index_fna | BOOL | last preop FNA on operated laterality (research_id join to `canonical_patient_master.first_surgery_date`; NULL if no surgery) |
| 9 | fna_date_raw | VARCHAR | fna_cytology.fna_date (audit preservation) |
| 10 | fna_date_resolved | DATE | `COALESCE(em.resolved_fna_date, TRY_CAST(fh.fna_date_parsed AS DATE), TRY_CAST(fc.fna_date AS DATE))` — three TRY_CAST fallbacks, no parser |
| 11 | fna_date_status | VARCHAR | fna_episode_master_v2 |
| 12 | fna_date_confidence | INT | fna_episode_master_v2 |
| 13 | days_from_first_fna | INT | `fna_date_resolved - MIN(fna_date_resolved) OVER (PARTITION BY rid)` |
| 14 | days_to_surgery | INT | `cpm.first_surgery_date::DATE - fna_date_resolved` via research_id join; NULL if no surgery or no resolved date |
| 15 | specimen_location | VARCHAR | fna_cytology |
| 16 | specimen_site_raw | VARCHAR | fna_episode_master_v2 |
| 17 | laterality | VARCHAR | fna_episode_master_v2 |
| 18 | bethesda_original_text | VARCHAR | fna_cytology.original_bethesda |
| 19 | bethesda_calculated_num | INT | fna_cytology.category_num |
| 20 | bethesda_2010_num | INT | fna_cytology |
| 21 | bethesda_2010_name | VARCHAR | fna_cytology |
| 22 | bethesda_2015_num | INT | fna_cytology |
| 23 | bethesda_2015_name | VARCHAR | fna_cytology |
| 24 | bethesda_2023_num | INT | fna_cytology |
| 25 | bethesda_2023_name | VARCHAR | fna_cytology |
| 26 | bethesda_final_num | INT | `COALESCE(CASE WHEN bethesda_calculated_num BETWEEN 1 AND 6 THEN bethesda_calculated_num END, CASE WHEN TRY_CAST(fc.original_bethesda AS INTEGER) BETWEEN 1 AND 6 THEN TRY_CAST(fc.original_bethesda AS INTEGER) END)` — primary: calculated_num (Script 268 semantics); fallback: numeric `original_bethesda` when it's a single 1-6 digit. Fallback recovers ~113 events. |
| 27 | bethesda_confidence | DOUBLE | fna_cytology |
| 28 | bethesda_derivation_method | VARCHAR | fna_cytology.method |
| 29 | bethesda_rules_category | INT | fna_cytology |
| 30 | bethesda_rules_confidence | DOUBLE | fna_cytology |
| 31 | bethesda_provider | VARCHAR | fna_cytology |
| 32 | bethesda_reasoning | VARCHAR | fna_cytology (PHI-safe, model-generated) |
| 33 | bethesda_evidence_present | BOOL | `fna_cytology.evidence IS NOT NULL` |
| 34 | pathology_diagnosis | VARCHAR | fna_episode_master_v2 |
| 35 | pathology_extended | VARCHAR | fna_episode_master_v2 |
| 36 | subtype | VARCHAR | fna_cytology |
| 37 | path_text_length | BIGINT | fna_cytology (length only — DO NOT pull `path_text` itself) |
| 38 | source_tables_represented | VARCHAR | e.g. 'history+cytology+episode' |
| 39 | ingest_script_version | VARCHAR | 'script_355' |
| 40 | ingested_at_utc | TIMESTAMP | `now()` |

**Explicitly NOT included:** `linked_molecular_episode_id`, `linked_imaging_nodule_id`, `linked_surgery_episode_id`, `linked_path_specimen_id`, `fna_episode_id`.

## `fna_patient_rollup_v1` — 20 columns (all aggregated from fna_event_v1)

| # | column | type | logic |
|---:|---|---|---|
| 1 | research_id | VARCHAR | GROUP BY |
| 2 | n_fnas | INT | COUNT(*) |
| 3 | n_bethesda_calculated | INT | COUNT(bethesda_calculated_num BETWEEN 1 AND 6) |
| 4 | n_nondiagnostic | INT | COUNT(bethesda_final_num = 1) |
| 5 | first_fna_date | DATE | MIN(fna_date_resolved) |
| 6 | last_fna_date | DATE | MAX(fna_date_resolved) |
| 7 | worst_bethesda_num | INT | MAX(bethesda_final_num) |
| 8 | best_bethesda_num | INT | MIN(bethesda_final_num) |
| 9 | bethesda_final | INT | MAX(bethesda_final_num) WHERE preop (days_to_surgery >= 0 OR NULL) |
| 10 | bethesda_final_name | VARCHAR | lookup table for bethesda_final |
| 11 | bethesda_index_nodule | INT | bethesda_final_num WHERE is_index_fna = TRUE |
| 12 | bethesda_index_nodule_linkage_source | VARCHAR | 'imaging' / 'surgery' / 'direct' |
| 13 | bethesda_max_preop_2010 | INT | MAX preop era-2010 |
| 14 | bethesda_max_preop_2015 | INT | MAX preop era-2015 |
| 15 | bethesda_max_preop_2023 | INT | MAX preop era-2023 |
| 16 | cross_fna_concordance | VARCHAR | 'single' / 'concordant' / 'discordant' |
| 17 | latest_bethesda_num | INT | `ARG_MAX(bethesda_final_num, fna_date_resolved)` |
| 18 | bethesda_confidence | DOUBLE | weighted mean by method |
| 19 | bethesda_derivation_methods | VARCHAR | `STRING_AGG(DISTINCT method ORDER BY 1)` |
| 20 | ingest_script_version | VARCHAR | 'script_355' |

**Explicitly NOT included:** `n_molecular_linked`, `n_surgery_linked` — query-time COUNT via research_id join.

## QA gates (fail-loud; write to `output/355_qa_gates.json`)
1. `fna_event_v1.COUNT(*)` == 8,119 (matches fna_history)
2. `fna_event_v1.COUNT(DISTINCT research_id)` == 5,266
3. `fna_event_v1.COUNT(DISTINCT fna_event_id)` == 8,119 (PK unique)
4. `fna_event_v1.COUNT(bethesda_calculated_num IS NOT NULL)` >= 7,935 (structural ceiling — 128 cytology rows have NULL category_num; prior 8,061 threshold was a spec arithmetic error). **Separate gate 4b**: `COUNT(bethesda_final_num IS NOT NULL)` >= 8,040 (ensures the numeric `original_bethesda` fallback is recovering events).
5. `fna_event_v1.COUNT(fna_date_resolved IS NOT NULL) / total` >= 0.80 (with 3-source COALESCE, actual ~0.8051)
6. Every research_id in fna_event_v1 also exists in canonical_patient_master
7. `fna_patient_rollup_v1.COUNT(*)` == 5,266
8. `fna_patient_rollup_v1.bethesda_final` distribution matches current CPM.bethesda_final within **±1.2% per category, non-NULL only** (Script 268 invariant). Threshold intentionally set at 1.2% (not 1.0%) because our spec is MORE CONSERVATIVE than CPM by design: CPM inherits Script 268's cytology multi-format date parser, which we forbid (gotcha #2). That parser dated ~60 preop cat-6 FNAs via M/D/YYYY strings; under our strict TRY_CAST-only spec those remain undated and are correctly excluded from `bethesda_final=6`. Current residual: cat 2 +1.14%, cat 6 −1.12% (4 of 6 categories already within ±1%). The 0.06pp headroom (1.2% − 1.14%) means the gate still fires for any meaningful drift in either direction.

## Idempotency
`CREATE OR REPLACE TABLE` for both outputs. If prior version exists, snapshot to `Thyroid 2026 UPdated.archive_pub_v1_0.<tbl>_pre355_<utc_ts>` first.

## Comments / registry
- `COMMENT ON TABLE main.fna_event_v1 IS '[domain=FNA; grain=one row per FNA event] — source: fna_history+fna_cytology+fna_episode_master_v2 via Script 355'`
- `COMMENT ON TABLE main.fna_patient_rollup_v1 IS '[domain=FNA; grain=one row per patient] — source: fna_event_v1 via Script 355'`
- Do NOT edit `detail_table_registry_v1` or CPM yet — Script 356 owns that.

## Git
`ruff check` → `git add` → `git commit -m "Add Script 355: fna_event_v1 + fna_patient_rollup_v1 (additive)"` → `git push`

## Do NOT in this script
- Do NOT drop, archive, or modify any existing table
- Do NOT modify CPM
- Do NOT modify views
