# Molecular ingest refactor (2026-04-08)

## Goal

Consolidate duplicate logic between `scripts/41_ingest_thyroseq_excel.py` and `scripts/42_ingest_afirma.py` into a **mapping-driven shared layer** without changing matching rules, CLI behavior, or governed output shapes.

## What moved

| Concern | Location |
|--------|----------|
| Governed column lists (`molecular_results`, `molecular_variant_long`) | `utils/molecular_ingest_common.py` → `GOVERNED_MOLECULAR_RESULT_COLUMNS`, `GOVERNED_MOLECULAR_VARIANT_LONG_COLUMNS` |
| Header snake_case + alias rename | `canonicalize_columns_from_map`, `normalize_header_snake`; Afirma maps from YAML |
| Row fingerprint (keyed SHA-256, 24-char) | `compute_keyed_row_hash` + `get_afirma_row_hash_keys` / `get_thyroseq_row_hash_fields` |
| Payload JSON checksum | `checksum_sorted_json_payload` |
| Scalar coercion for JSON payloads | `json_friendly_scalar` |
| `molecular_result_id` (32-char) | `molecular_result_id_from_parts` |
| Variant row ids | `molecular_variant_id_thyroseq` vs `molecular_variant_id_afirma` (**different key tuples**: Afirma appends `cdna_hgvs`; ThyroSeq does not — preserved) |
| Afirma `test_date` parsing | `parse_test_date_iso_and_native` (re-exported as `parse_test_date` from `utils/afirma_helpers`) |
| ThyroSeq workbook test date cell | `parse_thyroseq_workbook_test_date_cell` |
| Provenance stamping | `stamp_thyroseq_ingestion_metadata`, `stamp_afirma_ingestion_metadata` |

## Config

- **`config/molecular_ingest_aliases.yaml`** — Afirma column aliases, Afirma row-hash key order, ThyroSeq row-hash field names.
- **`utils/molecular_ingest_common.load_molecular_ingest_config`** — reads YAML when present; otherwise uses embedded defaults identical to the pre-refactor dictionaries (no behavior change when PyYAML or the file is absent).

## Scripts 41 / 42

Remain orchestration layers: DB connect, crosswalk, patient match, staging exports, and domain-specific parsing (ThyroSeq mutations/fusions/CNA vs Afirma GEC/GSC/XA). **Normalized layer builders stay in-place** to avoid risky merges of divergent QC rules.

## Regression

- Golden JSON under `tests/fixtures/molecular_ingest_golden/` — built with `ingestion_run_id=golden_batch01` and fixed `ingestion_ts` after stamping.
- **`tests/test_molecular_ingest_regression.py`** — compares Afirma CSV fixtures and a synthetic ThyroSeq row to those goldens.
- Empty `molecular_variant_long` exports as `[]`; tests accept `[]` vs zero-row framed output.

## Non-goals

- No new fuzzy patient or variant matching.
- No MotherDuck / local DuckDB writes as part of this change set.
- No changes to `molecular_code_crosswalk` semantics or Afirma XA expansion logic beyond import paths.
