# THYROID_2026 — Local v2 note extraction runbook

This runbook describes how to run **registry-driven v2 LLM extraction** locally, verify **per-domain parquet outputs** (no silent collapse into a single generic `llm` table), and validate results with script **111**.

## Session norms

- **Repo root**: project directory containing `llm_extraction/`, `processed/`, `config/extraction_domain_registry.yaml`.
- **Canonical runner**: `llm_extraction/run_extraction.py` (not a repo-root `run_extraction.py`).
- **Inputs**: `processed/clinical_notes_long.parquet` (build via `scripts/build_clinical_notes_long.py` if missing).
- **LLM enablement**: set `GITHUB_TOKEN` (GitHub Models) or `OPENAI_API_KEY`. Without either, v2 domains produce empty outputs; registry validation still runs.

## What the runner does (v2)

- For each **v2** registry domain whose extractors include `llm`, the runner calls `LLMExtractor.extract(..., domain=<domain_name>)`, applies `note_scope` filtering, and writes **`processed/<parquet_stem>.parquet`** from the registry (for example `note_entities_llm_imaging.parquet`).
- The legacy merged audit file **`processed/note_entities_llm.parquet`** is written **only** when you pass **`--merge-audit`** (optional; for backwards compatibility / debugging).
- **v1** regex domains behave as before (including the optional broad `llm` bucket from the shared `LLMExtractor` in the regex pass when LLM is enabled).

## Common commands

Registry check only (no extraction):

```bash
.venv/bin/python llm_extraction/run_extraction.py --validate-only
```

Full run (v1 regex + all v2 LLM domains, then optional merged audit):

```bash
.venv/bin/python llm_extraction/run_extraction.py --workers 3
.venv/bin/python llm_extraction/run_extraction.py --merge-audit --workers 3   # adds note_entities_llm.parquet
```

Targeted v2 domain + patient filter (merge-in semantics for listed `research_id`s):

```bash
printf '%s\n' 1001 1002 > /tmp/ids.txt
.venv/bin/python llm_extraction/run_extraction.py --target imaging --research-ids /tmp/ids.txt --workers 1
```

## Validation (script 111)

Outputs are written under **`studies/llm_extraction_validation/runs/<label>/`** by default.

Single domain (resolves parquet path from the registry):

```bash
.venv/bin/python scripts/111_llm_extraction_validation.py --domain imaging --run-label local_smoke
```

Custom input path (legacy or ad hoc parquet):

```bash
.venv/bin/python scripts/111_llm_extraction_validation.py \
  --input processed/note_entities_llm_labs.parquet \
  --output-dir studies/llm_extraction_validation/runs/local_smoke_labs
```

All registry domains that list the `llm` extractor (skips domains with missing parquets; see aggregate CSV):

```bash
.venv/bin/python scripts/111_llm_extraction_validation.py --all-llm-domains --run-label batch_001
```

Flags **`--domain`**, **`--input`**, and **`--all-llm-domains`** are mutually exclusive.

### MotherDuck / concordance summary (optional)

If **`MOTHERDUCK_TOKEN`** is available (see project `.env` / secrets policy), you can push the concordance summary table only:

```bash
export MOTHERDUCK_TOKEN="…"   # or load from your configured secrets file
.venv/bin/python scripts/111_llm_extraction_validation.py \
  --domain imaging \
  --motherduck-attach \
  --run-label md_sync_imaging
```

Generated **`motherduck_setup.sql`** in the run folder is the offline template when you prefer manual loading.

## Manual review workflow

- Each run directory can contain **`llm_manual_review_queue.csv`**. If the file already has non-empty `verification_status` values, a fresh queue is written to **`llm_manual_review_queue_pending.csv`** so manual edits are not overwritten.
- Re-run script 111 with **`--review-csv`** pointing at your completed decisions to merge statuses into **`llm_side_by_side`** / gold eligibility logic.

## Quick verification checklist

1. After a v2 run, confirm multiple files exist: `processed/note_entities_llm_<domain>.parquet` for several registry domains, not only `note_entities_llm.parquet` unless you passed `--merge-audit`.
2. Open one parquet and confirm **`entity_domain`** matches the registry domain name for v2 rows (not the string `"llm"` for those domains).
3. Run **`pytest tests/test_v2_domain_fanout_and_validation.py`** for automated fan-out / validator CLI regressions.

## Related tests

```bash
.venv/bin/pytest tests/test_v2_domain_fanout_and_validation.py tests/test_registry_and_md_connect.py::TestRunExtractionRegistryIntegration -q
```
