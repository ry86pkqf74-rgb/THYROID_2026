# H200 Retune And Staging Validation 2026-04-01

## H200 F investigation

- Host inspected live at `ssh -p 19816 -o StrictHostKeyChecking=no root@ssh9.vast.ai`.
- GPU was healthy before intervention: `NVIDIA H200 NVL`, ~92% utilization, ~436 W draw, stable SM clock around 1785 MHz.
- Slow throughput was not caused by CPU fallback or partial GPU offload. Ollama was returning successful responses while the host stayed GPU-bound.
- The main issue was under-utilization of the H200 lane for a heavy domain. `survival_followup` was still running with `EXTRACTION_CONCURRENCY=3`, and Ollama had not been explicitly started with the higher parallel profile used on H200 G.

## H200 F action taken

- Safely stopped the existing supervisor and worker, preserving the checkpoint at `note_entities_llm_survival_followup.ckpt.jsonl`.
- Restarted Ollama with:
  - `OLLAMA_NUM_PARALLEL=6`
  - `OLLAMA_MAX_LOADED_MODELS=1`
  - `OLLAMA_KEEP_ALIVE=24h`
  - `OLLAMA_FLASH_ATTENTION=1`
- Relaunched the same non-overlapping queue under the supervisor with `EXTRACTION_CONCURRENCY=6`.
- Verified full GPU offload after restart:
  - `offloaded 65/65 layers to GPU`
  - total resident memory about `79.6 GiB`
  - GPU utilization remained about 93% to 94%

## H200 F outcome

- Checkpoint continued advancing after the retune.
- Observed `survival_followup` checkpoint counts:
  - before retune snapshot: `2366`
  - after retune resume check: `2390`
  - later follow-up check: `2417`
- Ollama latency remains highly variable because `survival_followup` appears intrinsically heavier than domains like `recurrence_detailed`, but the host itself is healthy and now running at the intended high-parallel configuration.
- Decision: do not transfer this queue to a new H200 at this point. The live evidence supports keeping the current H200 F host and monitoring under the corrected runtime profile rather than churning checkpoints onto another instance.

## Staging artifact review

### Initial finding

- Archived file `processed/output/v2_parquets/note_entities_llm_staging.parquet` was an older-format artifact.
- It had `11,037` rows but only these fields:
  - `note_row_id`
  - `research_id`
  - `note_type`
  - `note_date`
  - `domain`
  - `llm_model`
  - `llm_base_url`
  - `extracted_at`
  - `result_json`
- It did not include current provenance fields such as `linkage_date`, `source_workbook`, `source_sheet`, or `source_column`, so it was not suitable to certify as-is.

### Repair performed

- Rebuilt the completed `staging` parquet from the local checkpoint `processed/output/v2_checkpoints/note_entities_llm_staging.ckpt.jsonl` using the current `scripts/vastai/run_extraction_concurrent.py` backfill path.
- The extractor rewrote all `11,037` checkpoint rows with the newer provenance payload and rebuilt the parquet without re-running model extraction.

### Validation results for rebuilt staging parquet

- Row count: `11,037`
- Unique `note_row_id`: `11,037`
- Duplicate `note_row_id`: `0`
- Source join on `note_row_id`: `0` unmatched rows
- Source mismatch counts after join:
  - `research_id`: `0`
  - `note_date`: `0`
  - `source_workbook`: `0`
  - `source_sheet`: `0`
  - `source_column`: `0`
- Rebuilt parquet includes:
  - `linkage_date`
  - `source_workbook`
  - `source_sheet`
  - `source_column`
  - `note_index`
  - `preprocess_batch_id`
  - `preprocessed_at_utc`
  - `preprocess_script_version`
- `note_date` is blank on `5,316` rows, but `linkage_date` is populated on all rows through the extractor fallback logic.
- `result_json` parsed successfully on all rows.
- Anomalous payloads are limited to three rows total:
  - `1` row with an explicit model error payload indicating no matching entities
  - `2` rows with `parse_error: true`

### Model mix in staging parquet

- `qwen3:32b`: `9,229` rows
- `qwen3:14b`: `1,808` rows

This mixed-model history reflects prior completed checkpoint content rather than the rebuild itself. The rebuild preserved existing extraction outputs and repaired provenance/linkage fields.

## Publish decision

- The rebuilt `staging` parquet is structurally valid and linked correctly back to the source note parquet.
- The repaired archive should replace the older staging parquet in `processed/output/v2_parquets/`.
- The three anomalous rows are small enough to treat as a targeted follow-up review item, not a blocker for publishing the repaired artifact.