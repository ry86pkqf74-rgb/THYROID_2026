# DICOM flattened-header ingest — operator runbook

**Script:** `scripts/150_ingest_dicom_headers.py`  
**Helpers:** `utils/dicom_header_helpers.py`  
**Aliases:** `config/dicom_header_aliases.yml`  
**DDL:** `scripts/sql/150_dicom_header_layer_ddl.sql`

## Purpose

Ingest **flattened** DICOM header exports (CSV, XLSX, JSON array, or Parquet) **or raw DICOM files (`.dcm`)** into governed, repo-native tables with **deterministic** linkage to existing imaging/specimen/FNA structures. The **`.dcm` path is metadata-only**: the pipeline uses `pydicom.dcmread(..., stop_before_pixels=True)` and never decodes pixel data or builds image pipelines. Canonical outputs (`dicom_study_header_v1`, `dicom_series_header_v1`, provenance, exact-link tables, review queue) are the same as for flattened exports.

This layer is **additive**: it does not change `imaging_nodule_master_v1`, `scripts/128_multimodal_contract_mm_v1.py`, or `scripts/129_imaging_fna_linkage_mm_v1.py`. Optional `optional_attach_dicom_to_imaging_nodule_frame()` is a **no-op** unless `dicom_study_header_v1` exists.

## Default: export-only (no database writes)

```bash
cd /path/to/THYROID_2026
python3 scripts/150_ingest_dicom_headers.py \
  --input /path/to/headers.csv

# Multiple files, explicit format
python3 scripts/150_ingest_dicom_headers.py \
  --input a.csv --input b.json --format csv

# One or more raw DICOM files (same export tables as flattened ingest)
python3 scripts/150_ingest_dicom_headers.py \
  --input /path/to/study_image.dcm --input /path/to/second_series.dcm --format auto
```

**Outputs** (under `exports/dicom_header_ingest_<UTC_ts>/`, gitignored):

| Artifact | Description |
|----------|-------------|
| `dicom_header_ingestion_provenance_v1.parquet` | Row-level provenance + raw JSON + fingerprint |
| `dicom_study_header_v1.parquet` | One row per `StudyInstanceUID` |
| `dicom_series_header_v1.parquet` | One row per `SeriesInstanceUID` |
| `dicom_imaging_link_exact_v1.parquet` | Exact links only |
| `dicom_link_review_queue_v1.parquet` | Ambiguous / discordant / missing matches |
| `manifest.json` | Row counts, git SHA, run metadata |

### Dry-run expectations

- `manifest.json` → `write_db: false`, non-zero provenance/study counts when input is valid.
- `dicom_link_review_queue_v1` may be non-empty when accession is missing or cannot be matched.

## Optional: read-only candidate pull (MotherDuck or local DuckDB)

Uses the same **accession normalization** as multimodal imaging↔FNA (`[^a-z0-9]` stripped after lowercasing). Candidates are a **UNION** of:

- `imaging_nodule_master_v1` (dynamic accession/exam/nodule column pick, mirroring script 129’s candidate list)
- `fna_history` (specimen / accession-like columns when present)
- `specimen_master_v1.accession_or_source_id`

```bash
# MotherDuck (RW token); read-only SELECTs unless --write-db
python3 scripts/150_ingest_dicom_headers.py --input headers.csv \
  --md --link-candidates-from-db

# Local file DB
python3 scripts/150_ingest_dicom_headers.py --input headers.csv \
  --db-path ./thyroid_master.duckdb --link-candidates-from-db
```

**Offline linkage:** pass a pre-built candidate frame:

```bash
python3 scripts/150_ingest_dicom_headers.py --input headers.csv \
  --candidate-parquet ./my_candidates.parquet
```

Required columns: `research_id`, `accession_norm`, `imaging_exam_id`, `imaging_nodule_id`, `specimen_id`, `source_table`, `exam_date_yyyymmdd`.

## Database materialization (requires explicit confirmation)

**Do not** use `--write-db` on production MotherDuck without operator sign-off. The script applies DDL then **merges** into:

- `dicom_header_ingestion_provenance_v1`
- `dicom_study_header_v1`
- `dicom_series_header_v1`
- `dicom_imaging_link_exact_v1`
- `dicom_link_review_queue_v1`

Token resolution follows `motherduck_client` / `motherduck.local.toml` (see `motherduck.local.toml.example`).

```bash
python3 scripts/150_ingest_dicom_headers.py --input headers.csv \
  --md --link-candidates-from-db --write-db
```

## Validation (CI / local)

```bash
python3 -m py_compile scripts/150_ingest_dicom_headers.py utils/dicom_header_helpers.py tests/test_dicom_header_ingest.py
python3 -m mypy --ignore-missing-imports scripts/150_ingest_dicom_headers.py utils/dicom_header_helpers.py tests/test_dicom_header_ingest.py
python3 -m pyflakes scripts/150_ingest_dicom_headers.py utils/dicom_header_helpers.py tests/test_dicom_header_ingest.py
ruff check scripts/150_ingest_dicom_headers.py utils/dicom_header_helpers.py tests/test_dicom_header_ingest.py
python3 -m pytest tests/test_dicom_header_ingest.py -q
```

## Tests and fixtures

Synthetic-only fixtures live under `tests/fixtures/dicom_headers/` (flattened formats). They contain no real PHI. **Raw `.dcm` coverage** uses programmatic minimal DICOM files created during tests (not committed).

**Dependency:** `pydicom` (see `requirements.txt`) is required only when ingesting `.dcm` files.

## Related documentation

- Study design memo: `studies/20260408_dicom_header_ingest/design_memo.md`
- MotherDuck contract: `docs/motherduck_database_contract_v1.md` (add DICOM tables to local manifests when promoted)
