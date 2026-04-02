# VAST Stale Artifact Reconciliation 2026-04-02

This folder captures two reconciled comparison-only artifacts recovered from stale outputs on the sixth VAST extraction host.

Included artifacts:
- `note_entities_llm_staging_partial_clean_for_comparison.parquet`
- `note_entities_llm_staging_partial_clean_for_comparison.jsonl`
- `note_entities_llm_complications_repaired_for_comparison.parquet`
- `note_entities_llm_complications_repaired_for_comparison.jsonl`
- `manifest.json`

Summary:
- `staging` stale checkpoint was a clean old-schema partial run: 1,885 rows, zero parse errors, exact subset of the validated full staging run.
- `complications` stale checkpoint was not safe to use directly: 11,775 parseable rows, 2,668 duplicate rows over 9,107 unique notes, one malformed JSON line, and no source lineage columns.
- `complications` stale parquet was salvageable: 13,705 rows over 11,037 unique note IDs. After deduplicating to one row per `note_row_id`, normalizing empty payloads, and backfilling `source_workbook`, `source_sheet`, and `source_column` from the extraction-time source parquet, the repaired comparison artifact covered all 11,037 notes with zero normalized content conflicts.

Use:
- These files are for comparison/audit only. They are not canonical replacement outputs for the validated production domains.
- The canonical validated complete domains remain: `staging`, `recurrence_detailed`, `operative_v2_enrichment`, `parathyroid_per_gland`, and `tirads_granular`.

Reproduction:
- Run [scripts/vastai/reconcile_stale_artifacts.py](scripts/vastai/reconcile_stale_artifacts.py) with the extraction-time source parquet, the stale staging checkpoint, the stale complications checkpoint, and the stale complications parquet.