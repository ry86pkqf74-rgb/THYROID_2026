# Afirma structured ingest

This pipeline loads **vendor-neutral, tabular** Afirma extracts into the governed molecular layer (`molecular_results`, `molecular_variant_long`), mirroring the ThyroSeq workbook flow (`scripts/41_ingest_thyroseq_excel.py`).

## Repo context (Afirma references)

| Location | Role |
|----------|------|
| `docs/MOLECULAR_FACT_LINEAGE.md` | Unified `main.molecular_fact_long_v` (and `molecular_results_unified_v`): note genetics + structured `molecular_results` with assay-first precedence. |
| `llm_extraction/prompts/molecular_thyroseq_afirma_extraction_v1.txt` | LLM entity schema (`afirma_result`, `afirma_version`, `afirma_xpression_atlas`, Bethesda, FNA) |
| `data_dictionary.md` | Legacy wide table `molecular_testing` from `THYROSEQ_AFIRMA_12_5.xlsx` (`thyroseq_afirma`, `result`, `mutation`, …) |
| `scripts/sql/131_molecular_results_layer_ddl.sql` | `molecular_assay_dictionary` + `molecular_code_crosswalk` seeds for Afirma assay keys and classifier buckets |

## Prerequisites

1. Apply the molecular layer DDL (once per database):

   ```bash
   .venv/bin/python scripts/131_molecular_results_layer.py --execute
   .venv/bin/python scripts/131_molecular_results_layer.py --execute --md --md-env dev
   ```

2. Install project deps (pandas, openpyxl for XLSX).

## Usage

```bash
.venv/bin/python scripts/42_ingest_afirma.py --input /path/to/file.csv [--md] [--md-env dev] [--local] [--dry-run]
```

- **`--dry-run`**: Writes export bundle under `exports/afirma_integration_<timestamp>/`, does **not** persist staging or molecular tables.
- **`--md` / `--local`**: Same MotherDuck vs local file semantics as script 41 (`utils/md_connect.connect_md_or_file`).
- **Replaces** all rows where `molecular_results.source_table = '42_afirma_structured_file'` before inserting the new batch (idempotent per source tag).

## Supported file formats

| Format | Detection |
|--------|-----------|
| CSV | `.csv` |
| Excel | `.xlsx`, `.xls` (first sheet) |
| JSON | `.json` — either a JSON **array** of row objects or `{"rows": [ ... ]}` |

## Column mapping (aliases → canonical)

After headers are normalized to lower snake case, the following aliases are accepted (see `utils/afirma_helpers.HEADER_ALIASES`):

| Canonical | Accepted aliases (examples) |
|-----------|----------------------------|
| `research_id` | `rid`, `study_id` |
| `mrn` | `patient_mrn`, `euh_mrn`, `pt_mrn` |
| `dob` | `date_of_birth`, `birth_date` |
| `patient_name` **or** `last_name` + `first_name` | `patient_full_name`, `last_nm`, … |
| `specimen_id` | `sample_id`, `specimen_key` |
| `accession` | `accession_number`, `case_accession` |
| `test_date` | `result_date`, `collection_date` |
| `bethesda` | `bethesda_category`, `fna_bethesda` |
| `fna_cytology` | `cytology`, `cytology_summary` |
| `gec_call` | `gec_result`, `afirma_gec`, `gene_expression_call` |
| `gsc_call` | `gsc_result`, `afirma_gsc`, `genomic_sequencing_call` |
| `panel_type` | `assay_panel`, `afirma_panel`, `assay_version` |
| `xpression_variants` | `xa_variants`, `variant_findings_json` (string JSON) |

**Patient linkage**

- If `research_id` is populated with a valid integer, it **wins** (`match_method = source_research_id`, confidence 1.0).
- Otherwise the script reuses `build_crosswalk` + `match_patients` from `41_ingest_thyroseq_excel` (MRN / DOB / name — **exact** institutional crosswalk, no fuzzy patient matching).

## Xpression Atlas variants

`xpression_variants` may be:

1. A JSON **string** of an array of objects, or
2. In JSON input files, a native array.

Each object may include:

`gene_symbol` (or `gene`), `partner_gene_symbol`, `fusion_partner`, `variant_class`, `transcript_id`, `genomic_hgvs`, `cdna_hgvs`, `protein_hgvs`, `canonical_hgvs`, `allele_fraction`, `interpretation_text`, `raw_variant_token`.

`variant_class` is normalized only through **`molecular_code_crosswalk` domain `variant_class`** (exact `source_code` match). Unknown classes become `OTHER` with QC flag `xa_variant_class_unmapped`.

## Panel-only (expression / classifier only)

When there are **no** XA variants, the loader still writes **one** `molecular_results` row per source line:

- `canonical_hgvs` left null.
- `raw_payload_json` retains structured provenance plus harmonized vs raw classifier fields.
- Optional QC flag `expression_only_no_classifier_call` when GEC, GSC, and XA are all absent (truly empty classifier row).

## Crosswalk and assay metadata

- **Embedded** defaults in `utils/afirma_helpers` match the SQL seeds so dry-runs work without a seeded DB.
- At runtime, **`molecular_code_crosswalk` overlays** embedded maps (DB wins on duplicate `source_code`).
- Domains used: `afirma_call`, `afirma_risk_call`, `afirma_assay_key`, `variant_class`.
- Assay display names / `panel_version` / `platform` / `vendor` / `loinc_code` resolve from `molecular_assay_dictionary` by `assay_key` (with the same embedded fallback).

**No fuzzy matching**: classifier strings must match a crosswalk `source_code` exactly (after trim). Unmapped values set `unmapped_gec_call` / `unmapped_gsc_call` QC flags and `normalization_status = pending_review`, and add `afirma_review_queue` / export rows with `issue_type = crosswalk_mapping` when appropriate.

`loinc_code` is **null** in seeds until institution-specific LOINC is curated into `molecular_assay_dictionary`.

## Output tables (non–dry-run)

| Table | Purpose |
|-------|---------|
| `stg_afirma_structured_raw` | Canonical-column staging + hashes |
| `stg_afirma_match_results` | Patient match audit |
| `afirma_review_queue` | Match, crosswalk, and XA QC routing |
| `molecular_results` | `source_table = 42_afirma_structured_file` |
| `molecular_variant_long` | Long variants (XA rows only when provided) |
| `molecular_ingestion_runs` | Batch metadata |

Contract views (`molecular_results_contract_v1`, `molecular_variant_long_contract_v1`, `molecular_results_enriched_v1`, `molecular_normalization_review_v1`) apply once DDL is installed.

## Tests

Synthetic fixtures (no PHI) live under `tests/fixtures/afirma/`. Run:

```bash
.venv/bin/python -m pytest tests/test_afirma_ingest.py -v
```
