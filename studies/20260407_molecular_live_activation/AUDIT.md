# Audit: empty `main.molecular_results` (live path)

## Root cause

1. **No governed load:** The normalized layer DDL (`131`) could be present while **`molecular_results` / `molecular_variant_long` remained empty**—nothing in the pipeline automatically runs `41` / `42` on MotherDuck.
2. **Validation behavior:** With zero rows, `119_md_formalization_validate.py` reported molecular checks as **skipped** (informational PASS), so contract logic was not stress-tested.
3. **Multi-catalog metadata (fixed):** `information_schema` could list `main.*` / `qa.*` objects from **other** attached databases; existence checks that relied only on `information_schema` misclassified prerequisites. `119` now uses `SELECT 1 … LIMIT 1` probes so “exists” means **resolvable on the current connection’s default `main` / `qa`**.

## ThyroSeq / Afirma “latest approved” inputs in-repo

- **Full institution workbook** (`Thyroseq Data Complete.xlsx`) is **not** stored in git (PHI / size). The repo’s **last committed smoke** was recorded under `exports/thyroseq_integration_20260407_0158/` (manifest only in-tree; source workbook path was external).
- **Afirma:** Prior governed runs used `tests/fixtures/afirma/with_xa_variants.csv` pattern; activation used a **cohort-keyed** CSV under `inputs/` for MotherDuck dev (see README).

## Code changes

- **`41_ingest_thyroseq_excel.py`:** Optional workbook columns `Research ID number` / `research_id` / aliases apply **`source_research_id`** matching (parity with `42_ingest_afirma`), so MotherDuck runs do not depend solely on local `raw/` crosswalk Excel when an explicit cohort id is supplied on the row.
- **`119_md_formalization_validate.py`:** Probe-based `_main_object_exists` / `_qa_object_exists` for MotherDuck.

## Provenance (row-level)

Governed envelopes: `ingestion_run_id`, `ingestion_ts`, `lineage_id`, `source_table`, `source_row_fingerprint`, `payload_checksum`, and JSON payloads (`raw_payload_json`) on `main.molecular_results` / contract views. Unified lineage: `main.molecular_fact_long_v` exposes `source_stream`, `fact_provenance_category`, `record_role`, `included_in_primary_analytics`, `precedence_rationale`, assay–note gaps when note rows participate.
