# LLM Extraction Validation Workspace

This workspace is the dedicated home for LLM extraction artifacts before any
integration into canonical or MotherDuck layers.

Purpose:

- keep raw LLM extraction outputs in a predictable repo location
- preserve note-level lineage for every extracted value
- place LLM-derived values next to pre-existing repo values for validation
- support iterative refinement before deciding whether a value is safe to integrate

Primary script:

- `scripts/111_llm_extraction_validation.py`

Expected input:

- `processed/note_entities_llm.parquet` (or `--input` to another parquet with full `ENTITY_SCHEMA_COLUMNS`)

Default output layout (under `studies/llm_extraction_validation/runs/<run-label>/`):

- `llm_lineage.parquet` / `.csv`
- `llm_side_by_side.parquet` / `.csv` — includes `original_source_link`, `extraction_ts`, `original_value_vs_llm_diff`, `algorithm_comparison_status`, `verification_status`, `verified_by`; pipeline fields renamed to `llm_verification_status` / `llm_verification_step`
- `llm_manual_review_queue.csv` — discordant rows (optional `source_limited` via flag); fill `verification_status` for re-run
- `llm_manual_review_queue_pending.csv` — written when the queue file already contains manual `verification_status` edits (preserves reviewer work on re-run)
- `gold_llm_verified_facts.parquet` — rows approved for gold per policy below
- `val_llm_concordance_summary.parquet` / `.csv` — counts by `comparison_domain` × `verification_status` (for MotherDuck / BI)
- `motherduck_setup.sql` — copy/paste ATTACH + `CREATE TABLE` template using local Parquet paths
- `llm_validation_summary.csv`, `manifest.json`, `report.md`

What the validator does:

1. Joins every extracted row back to `clinical_notes_long` using `note_row_id`
2. Adds `original_source_link` (~200 chars around `evidence_span`) and `extraction_ts`
3. Preserves source-note lineage fields like `source_sheet`, `source_column`, and `note_index`
4. Checks whether the stored `evidence_span` still resolves in the source note
5. Routes each extracted value into a comparison domain/token
6. Compares it against local structured DuckDB tables and `note_entities_*` baseline
7. Sets `algorithm_comparison_status`: `concordant_existing`, `discordant_existing`, `concordant_existing_extraction_only`, `existing_missing_fill_candidate`, `source_limited`
8. Maps defaults into `verification_status`: `concordant`, `existing_missing_fill_candidate`, `pending_review`, `source_limited`

Gold policy (`gold_llm_verified_facts.parquet`):

- Only rows with `present_or_negated` = `present`
- `verification_status` must be `concordant` or `existing_missing_fill_candidate`
- **Concordant**: included with `verified_by=auto` unless you pass `--gold-require-manual`
- **Fill candidate** (`existing_missing_fill_candidate`): requires `verified_by=manual` (after merging a filled review CSV) unless you pass `--trust-fill-candidate-auto` (not recommended)

Two-pass workflow:

1. Run validation (writes an empty review queue):

   ```bash
   .venv/bin/python scripts/111_llm_extraction_validation.py --run-label my_run
   ```

2. Edit `llm_manual_review_queue.csv`: set `verification_status` for discordant rows (`concordant`, `existing_missing_fill_candidate`, `reject`, `needs_followup`, …).

3. Re-run with merge (auto-detects the same `llm_manual_review_queue.csv` if any `verification_status` cell is non-empty, or pass `--review-csv PATH`):

   ```bash
   .venv/bin/python scripts/111_llm_extraction_validation.py --run-label my_run
   ```

MotherDuck:

- Open `motherduck_setup.sql` from the run directory; replace `YOUR_DATABASE` and attach with your token per DuckDB docs.
- After attach: `SELECT * FROM md.val_llm_concordance_summary;` (once the table is created from the generated Parquet).
- Optional: `--motherduck-attach` with `MOTHERDUCK_TOKEN` and `MOTHERDUCK_DATABASE` set creates/replaces `val_llm_concordance_summary` remotely.

Flags (see `scripts/111_llm_extraction_validation.py --help`):

- `--review-include-source-limited` — add `source_limited` rows to the manual queue
- `--gold-require-manual` — concordant gold rows must be manually confirmed in CSV
- `--trust-fill-candidate-auto` — allow fill-candidate gold without manual (unsafe default off)

Notes:

- Default workflow does not upload data; `motherduck_setup.sql` is for manual or scripted load.
- Rows marked `existing_missing_fill_candidate` should be reviewed before treating as gold unless policy explicitly allows automation.
