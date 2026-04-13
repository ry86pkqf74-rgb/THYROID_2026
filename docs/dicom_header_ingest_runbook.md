# DICOM flattened-header ingest — operator runbook

**Script:** `scripts/150_ingest_dicom_headers.py`  
**Helpers:** `utils/dicom_header_helpers.py`  
**Aliases:** `config/dicom_header_aliases.yml`  
**DDL:** `scripts/sql/150_dicom_header_layer_ddl.sql`

**Status vs canonical contract:** The ingest layer is **fully implemented in this repository** (script, DDL, tests, export path). **Canonical-live presence in MotherDuck `main`** is **not** implied by this runbook; treating `dicom_*_v1` as part of the promoted contract requires an explicit operator materialization/promotion step and verification. Default runs remain **export-only** (no DB writes). See `docs/motherduck_database_contract_v1.md` (DICOM subsection) and `studies/20260413_dicom_promotion_reconciliation/report.md`.

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

### Exact-accession ambiguity (specimen vs imaging)

After accession matches exactly one `research_id` and imaging-exam ambiguity is ruled out (`≤1` distinct non-blank `imaging_exam_id` on imaging candidate rows), the resolver counts **distinct non-blank** `specimen_id` values across **all** candidate rows for that accession.

- **`AMBIGUOUS_ACCESSION_MULTI_SPECIMEN`** — More than one distinct non-blank `specimen_id` for the same normalized accession under a single `research_id`. No auto-link; `candidate_specimen_ids_json` lists all distinct IDs (sorted). Previously, the first specimen row could be taken silently; that behavior is removed.
- **Precedence with `AMBIGUOUS_ACCESSION_MULTI_IMAGING_EXAM`** — If there are `>1` distinct imaging exam IDs, the review reason remains **`AMBIGUOUS_ACCESSION_MULTI_IMAGING_EXAM`** (unchanged). The review row still includes `candidate_specimen_ids_json` when specimen IDs exist; if there are multiple distinct specimen IDs as well, `conflict_note` also notes specimen multiplicity.
- **Non-ambiguous** — `0` distinct specimen IDs → `specimen_id` on the link stays null. Exactly `1` distinct specimen ID (including when many rows repeat the same ID) → that ID may be attached to the `exact_accession` link.

Blank / whitespace-only `imaging_exam_id` and `specimen_id` values are ignored when forming distinct-ID sets.

### Malformed raw `.dcm` files (QC vs linkage)

If `pydicom` cannot read a file (or `StudyInstanceUID` is missing after read), the row is recorded in **`dicom_header_ingestion_provenance_v1`** with `parse_status` **`error`** and QC flags such as **`MISSING_STUDY_INSTANCE_UID`**. No **`dicom_study_header_v1`** row is emitted for that study. **`resolve_exact_links`** only iterates study-level rows, so **no** `dicom_link_review_queue_v1` row is produced for that failure mode when the study frame is empty for that case. (Study-level QC blockers such as malformed UIDs on rows that *do* produce a study row still route to review via **`STUDY_HEADER_QC_BLOCKER`**.)

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
