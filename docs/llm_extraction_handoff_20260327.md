# LLM Extraction Handoff — 2026-03-27

## Goal
Run the full note-extraction pipeline with LLM assistance across `processed/clinical_notes_long.parquet`, then upload the standard entity tables to local DuckDB.

## What changed in code

### 1. GitHub Models support
File: `llm_extraction/extract_llm.py`
- `LLMExtractor` now prefers `GITHUB_TOKEN` first, then falls back to `OPENAI_API_KEY`.
- GitHub Models endpoint:
  - base URL: `https://models.github.ai/inference/`
  - current default model: `openai/gpt-4o-mini`
- If neither token is present, LLM extraction is disabled.

### 2. Explicit rate-limit handling
File: `llm_extraction/extract_llm.py`
- OpenAI SDK auto-retries were disabled with `max_retries=0`.
- Custom backoff was added for 429/rate-limit responses.
- Current backoff schedule per request:
  - attempt 1: 5s
  - attempt 2: 10s
  - attempt 3: 20s
  - attempt 4: 40s
  - attempt 5: fail that note and continue
- One API client is cached per worker thread for lower overhead.

### 3. Parallel note-level extraction
File: `llm_extraction/run_extraction.py`
- Added `--workers` argument.
- Extraction can now run with note-level thread parallelism.
- Current safe setting found in this session: `--workers 3`
- Auto-default when LLM is enabled is now `3` workers.

## Current recommended run command
From repo root:

```bash
export GITHUB_TOKEN="<your github models token>"
/Users/loganglosser/THYROID_2026/.venv/bin/python -u llm_extraction/run_extraction.py --workers 3
```

On another computer, after cloning the repo, use the local repo path instead of the absolute path above, e.g.:

```bash
cd THYROID_2026
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install openai
export GITHUB_TOKEN="<your github models token>"
python -u llm_extraction/run_extraction.py --workers 3
```

## Environment/setup required
- Python virtual environment available at `.venv/`
- `openai` package installed
- one of:
  - `GITHUB_TOKEN` (preferred)
  - `OPENAI_API_KEY` (fallback)

Do **not** commit tokens into the repo.

## Model checks done in this session
Quick GitHub Models smoke tests succeeded for:
- `openai/gpt-4.1-nano`
- `openai/gpt-4.1-mini`
- `openai/gpt-4o-mini`

Current extractor default remains `openai/gpt-4o-mini` because that was the model actively wired and tested in the extraction flow.

## Operational notes
- With `--workers 6`, the pipeline hit too many 429 responses and became unstable.
- With `--workers 3`, the pipeline ran substantially better and continued through transient 429 periods.
- 429 bursts are expected on GitHub Models free tier; the pipeline should now pause and continue instead of immediately failing.
- Full extraction is large (`11,037` notes), so expect a long-running process.

## Input dataset used
- Source parquet: `processed/clinical_notes_long.parquet`
- Session-observed volume:
  - total notes: `11,037`
  - unique patients: `5,641`
  - op notes: `4,680`

## Existing / expected outputs
Standard extracted parquet outputs in `processed/`:
- `note_entities_staging.parquet`
- `note_entities_genetics.parquet`
- `note_entities_procedures.parquet`
- `note_entities_operative_detail.parquet`
- `note_entities_complications.parquet`
- `note_entities_medications.parquet`
- `note_entities_problem_list.parquet`

LLM-local output:
- `note_entities_llm.parquet`

Note: `note_entities_llm.parquet` is local output and is **not** part of the `09b` upload list.

## Provenance / linkage expectations
The extraction flow is designed to preserve note-level provenance, including fields such as:
- `research_id`
- `note_row_id`
- `note_type`
- `entity_date`
- `note_date`
- `extraction_method`
- `evidence_span`
- `evidence_start`
- `evidence_end`
- `extracted_at`

These come from the shared entity schema and should be checked after the run completes.

## Upload step after extraction completes
Run:

```bash
/Users/loganglosser/THYROID_2026/.venv/bin/python scripts/09b_local DuckDB_upload_notes_entities.py --confirm
```

This uploads the standard notes/entity tables only:
- `clinical_notes_long`
- `note_entities_staging`
- `note_entities_genetics`
- `note_entities_procedures`
- `note_entities_operative_detail`
- `note_entities_complications`
- `note_entities_medications`
- `note_entities_problem_list`

## Suggested validation after extraction
1. Confirm output parquets were rewritten and are readable.
2. Confirm provenance/date columns are present in each output.
3. Run `09b` upload.
4. Spot-check row counts and patient counts before/after upload.
5. Stage, commit, and push only the extraction-related changes.

## Files changed in this session
- `llm_extraction/extract_llm.py`
- `llm_extraction/run_extraction.py`
- `docs/llm_extraction_handoff_20260327.md`

## Resume point
If picking this up on another machine, the next step is:
1. pull latest `main`
2. recreate `.venv` and install dependencies
3. set `GITHUB_TOKEN`
4. run the full extraction with `--workers 3`
5. run `09b` upload
6. verify provenance columns and counts
7. commit/push any resulting extraction artifacts you want tracked
