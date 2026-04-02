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

- `processed/note_entities_llm.parquet`

Default output layout:

- `studies/llm_extraction_validation/runs/<timestamp>/llm_lineage.parquet`
- `studies/llm_extraction_validation/runs/<timestamp>/llm_lineage.csv`
- `studies/llm_extraction_validation/runs/<timestamp>/llm_side_by_side.parquet`
- `studies/llm_extraction_validation/runs/<timestamp>/llm_side_by_side.csv`
- `studies/llm_extraction_validation/runs/<timestamp>/llm_validation_summary.csv`
- `studies/llm_extraction_validation/runs/<timestamp>/manifest.json`
- `studies/llm_extraction_validation/runs/<timestamp>/report.md`

What the validator does:

1. Joins every extracted row back to `clinical_notes_long` using `note_row_id`
2. Preserves source-note lineage fields like `source_sheet`, `source_column`, and `note_index`
3. Checks whether the stored `evidence_span` still resolves in the source note
4. Routes each extracted value into a comparison domain/token
5. Compares it against:
   - local canonical/structured DuckDB tables where available
   - existing `note_entities_*` tables as a pre-existing extraction baseline
6. Flags rows as:
   - `concordant_existing`
   - `discordant_existing`
   - `concordant_existing_extraction_only`
   - `existing_missing_fill_candidate`
   - `source_limited`

Recommended run:

```bash
.venv/bin/python scripts/111_llm_extraction_validation.py
```

Optional explicit run label:

```bash
.venv/bin/python scripts/111_llm_extraction_validation.py --run-label first_llm_pass
```

Notes:

- This first-pass workflow is local only. It does not write to MotherDuck.
- It is designed to support iterative LLM extraction refinement before any fill or merge step.
- Rows marked `existing_missing_fill_candidate` should still be reviewed before integration.