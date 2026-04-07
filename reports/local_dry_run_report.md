# THYROID_2026 — local dry run report

**Session:** 2026-04-07  
**Scope:** deterministic local execution only. **No MotherDuck reads or writes** for this prompt.  
**Machine:** repo root `THYROID_2026` with `.venv`, `processed/clinical_notes_long.parquet`, and `thyroid_master.duckdb`.

## Executive readiness

| Gate | Verdict |
|------|---------|
| Registry + offline tests | **PASS** |
| Extraction runner + `note_row_id` | **PASS** (after fix) |
| Script 111 per-domain validation | **PASS** (3 domains) |
| Script 103 fact lineage (dry-run) | **PASS** (counts + clinical note merge after fix) |
| Script 02b registration | **PASS** (after `v_entity_type_normalized` repair) |
| Script 123 presentation views | **PASS** |
| Script 129 imaging→FNA linkage | **HOLD** (local DB missing `fna_episode_master_v2`, `imaging_nodule_master_v1`) |
| Script 128 multimodal contract | **HOLD** (local DB missing `operative_episode_detail_v2` and other upstream cores; bootstrap does not stub OED) |

**Overall local readiness:** **HOLD** — core multimodal / imaging–FNA paths require a fuller local DuckDB (or MotherDuck) episode layer; extraction and governance scripts are otherwise healthy on this checkout.

---

## 1. Commands and flags used (existing CLIs)

| Step | Command |
|------|---------|
| Registry | `.venv/bin/python llm_extraction/run_extraction.py --validate-only` |
| Genetics subset (regex-only) | `env -u OPENAI_API_KEY -u GITHUB_TOKEN .venv/bin/python llm_extraction/run_extraction.py --target genetics --research-ids reports/_dry_run_research_ids.txt --workers 1` |
| Validation | `.venv/bin/python scripts/111_llm_extraction_validation.py --domain <name> --run-label <label>` |
| Fact lineage (no parquet/DB write) | `.venv/bin/python scripts/103_fact_lineage_materialize.py --dry-run` |
| Registration | `.venv/bin/python scripts/02b_register_notes_entities.py` |
| Presentation views | `.venv/bin/python scripts/123_presentation_views.py` |
| Imaging→FNA | `.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py` (local file DB) |
| Multimodal contract | `.venv/bin/python scripts/128_multimodal_contract_mm_v1.py` and `--allow-bootstrap-dev` |

Small-input option used: **`--research-ids`** (two `research_id`s with rich genetics content: **7449**, **7493**).

---

## 2. Extraction registry

- `validate_registry`: **0 issues** (prompt files on disk, no duplicate stems, sub-prompt parents valid).
- **Orphan `note_entities*.parquet` files** (stems not in `all_known_stems()`): **0**.
- **Canonical domains with no `processed/<stem>.parquet` at repo root:** many v2 domains are present only under `processed/output/v2_parquets/` on full installs; this workspace’s `processed/` tree is **partial** (see `reports/local_output_inventory.csv`). **Not a registry defect** — expected when only a subset of parquets is checked in or synced.

---

## 3. Extraction subset

| Track | Domain | Mode | Outcome |
|-------|--------|------|---------|
| Legacy v1 | `genetics` | Regex only (tokens unset) | **91** new rows merged for 2 patients; file `note_entities_genetics.parquet` now **1,738** entity rows total (merge-into-existing semantics). |
| v2 (validation only) | `imaging` | Existing parquet | **8,428** entity rows after fleet JSONL expansion; script **111** completed. |
| v2 (validation only) | `pathology` | Existing parquet | **10,894** entity rows after expansion; script **111** completed. |

v2 **re-extraction** was not re-run with LLM in this session (non-deterministic; not required to validate the offline path). Existing v2 parquets were used for script 111.

---

## 4. Per-domain validation (script 111)

| Run label | Domain | Extracted rows (input) | Review queue CSV rows | Notes |
|-----------|--------|------------------------|----------------------|-------|
| `local_dryrun_genetics` | genetics | 1,738 | 0 | Summary: concordant_existing_extraction_only + fill_candidate rows per `llm_validation_summary.csv` |
| `local_dryrun_imaging` | imaging | 8,428 | 0 | 15 summary lines |
| `local_dryrun_pathology` | pathology | 10,894 | 0 | 14 summary lines |

Artifacts: `studies/llm_extraction_validation/runs/local_dryrun_<domain>/<domain>/` (parquet/csv/md). *Not committed* — reproducible by re-running 111 with the same labels.

**Genetics concordance snapshot** (`llm_validation_summary.csv`): 1,668 rows concordant_existing_extraction_only; 70 existing_missing_fill_candidate (structured fill candidates).

---

## 5. Fact lineage (script 103, `--dry-run`)

Row counts from last run:

| Output | Rows |
|--------|------|
| `canonical_extracted_fact_long_v1` (clean) | 68,077 |
| `canonical_fact_quarantine_v1` | 0 |
| `canonical_extracted_fact_long_v2` (clean) | 123,577 |
| `canonical_fact_quarantine_v2` | **199** (reason: `low_confidence_llm_date`) |

Clinical note provenance merge: **enabled** after synthesizing `note_row_id` on `clinical_notes_long` when the column is absent (same hashing rule as `build_clinical_notes_long.py`).

---

## 6. Registration / materialization (local)

- **02b_register_notes_entities.py**: completed successfully after fixing **`v_entity_type_normalized`** (see fixes below).
- **103** full materialize (write parquets + DuckDB): **not** run (would overwrite large artifacts); **dry-run only**.
- **128 / 129**: blocked on missing upstream tables in **local** `thyroid_master.duckdb` (see executive table).

---

## 7. Issues found and fixed (this session)

1. **`run_extraction.py`**: `clinical_notes_long.parquet` had **no `note_row_id`** → `KeyError`. **Fix:** synthesize `note_row_id` from `(research_id, source_sheet, source_column)` before extraction (aligned with script 111 / `build_clinical_notes_long.py`). Suppressed pandas `FutureWarning` on `concat` in `_merge_into_existing`.
2. **`103_fact_lineage_materialize.py`**: skipped `clin_*` merge when `note_row_id` missing. **Fix:** same synthesis path before merging notes into the unified fact frame.
3. **`123_presentation_views.py`**: `v_entity_type_normalized` referenced **`entity_type_raw`**, which is absent unless script 120 normalization has been applied. **Fix:** resolve `original_entity_type` expression from `PRAGMA table_info('canonical_extracted_fact_long_v2')` (use `COALESCE(entity_type_raw, entity_type)` when the column exists, else `entity_type`).

---

## 8. Automated tests

```text
pytest tests/test_registry_and_md_connect.py tests/test_fleet_registry_parity.py tests/test_v2_domain_fanout_and_validation.py -q
```

**61 passed** (≈0.66s).

---

## 9. Blockers for “full” local multimodal

Local `thyroid_master.duckdb` on this machine **does not** define (non-exhaustive):

- `fna_episode_master_v2`, `imaging_nodule_master_v1` → **129** cannot run.
- `operative_episode_detail_v2` → **128** cannot run even with `--allow-bootstrap-dev`.

**Next step (operator):** materialize canonical episode tables (e.g. script 22 chain + imaging master) into local DuckDB, or attach MotherDuck read-only for verification only (still no writes).

**Investigation prompt (MotherDuck vs local):** [`reports/prompt_investigate_multimodal_holds.md`](prompt_investigate_multimodal_holds.md).

---

## 10. Deliverables

| File | Description |
|------|-------------|
| `reports/local_dry_run_report.md` | This report |
| `reports/local_output_inventory.csv` | Row/size inventory for `note_entities*.parquet` (root + `processed/output/v2_parquets` when present) |

**Console verdict:** **HOLD** (multimodal/imaging–FNA on local DB); **PASS** on registry-driven extraction, 111 validation, 103 dry-run lineage, and 02b registration after view fix.
