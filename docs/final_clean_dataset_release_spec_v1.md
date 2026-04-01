# Final clean dataset release spec v1

## Purpose

Define what is included in **THYROID_2026 clean dataset v1** for analysis that depends on note-derived facts with full provenance, episode linkage, and explicit quarantine of ambiguous rows.

## Included tables (artefacts)

| Artefact | Location | Description |
|----------|----------|-------------|
| Clean facts | `processed/canonical_extracted_fact_long_v1.parquet` | Union of all `note_entities_*` domains merged with `clinical_notes_long` provenance, contract columns, episode inference; excludes quarantined rows |
| Quarantine | `processed/canonical_fact_quarantine_v1.parquet` | Same schema as clean plus `quarantine_reason`, `quarantine_date` |
| Run log | `processed/note_extraction_runs.parquet` | One row per `notes_extraction/run_extraction.py` run (`success`, `failure_stage`, counts, LLM telemetry JSON) |
| Notes source | `processed/clinical_notes_long.parquet` | Long-form notes with workbook / row provenance |

Downstream DuckDB objects mirror these names when registered via `scripts/02b_register_notes_entities.py` or `scripts/103_fact_lineage_materialize.py`.

## Inclusion criteria (clean canonical)

- Row appears in `note_entities_<domain>.parquet` outputs for a completed extraction run with stamped `extraction_run_id`.
- Row survives **conservative quarantine** in `scripts/103_fact_lineage_materialize.py` (see below).
- Episode linkage (`inferred_surgery_episode_id`, `ep_distance_days`, `linkage_confidence`) is computed when `operative_episode_detail_v2` is available; linkage strength is recorded, not imputed beyond stated rules.

## Quarantine policy (summary)

Rows are moved to `canonical_fact_quarantine_v1` when any of the following holds:

1. **Multi-surgery episode ambiguity:** Patient has more than one row in `operative_episode_detail_v2` and nearest-episode match is missing or farther than **90 days** from the inferred reference date.
2. **Low-confidence LLM dates:** Extraction method is LLM-derived, `entity_date` is present, and `date_confidence` &lt; **0.35**.
3. **Temporal conflict:** `entity_date` and `inferred_surgery_date` both present and more than **730 days** apart.

Constants are documented in `scripts/103_fact_lineage_materialize.py` (`MULTI_SURGERY_EP_DIST_THRESH_DAYS`, etc.).

## Release metrics

After `scripts/29_validation_engine.py`, inspect `val_fact_release_metrics_v1` for:

- Counts: `n_clean_facts`, `n_quarantine_facts`, `pct_quarantine_of_all_facts`
- Fill rates: `pct_source_file_id`, `pct_episode_linkage`, `pct_extraction_run_id`, `pct_prompt_version_non_regex`
- LLM: `pct_llm_model_name_among_llm`, `pct_llm_back_checked`

Row-level LLM QA remains in `val_fact_provenance_v1`.

## Known limitations

- RegEx-only domains use `prompt_version = 'regex_only'` and null model fields.
- LLM disabled runs are **not** failures: `note_extraction_runs.failure_stage = 'llm_disabled'` with `success = true` distinguishes missing API config from pipeline errors.
- Quarantine is **conservative**: some valid rows may be sidelined when episode or date context is weak; review `canonical_fact_quarantine_v1` for secondary use.

## Build command sequence

See `docs/dataset_hardening_execution_plan_20260401.md`.
