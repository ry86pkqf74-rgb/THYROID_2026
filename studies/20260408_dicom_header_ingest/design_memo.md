# DICOM header ingest layer (v1) — design memo

**Date:** 2026-04-08  
**Script:** `scripts/150_ingest_dicom_headers.py`  
**DDL:** `scripts/sql/150_dicom_header_layer_ddl.sql`

## Objective

**Initial design focus (v1 memo draft):** flattened radiology PACS/RIS **header exports** aligned to existing **canonical imaging / specimen / FNA** structures.

**Shipped scope evolution:** **Raw `.dcm` ingestion was added after the first pass** — same logical columns as flattened exports, using `pydicom` with **`stop_before_pixels=True`** so **pixel data are never decoded** and no image pipeline is introduced. The layer remains **additive** and **deterministic** (explicit `research_id` or exact normalized accession against the candidate spine; no fuzzy matching).

**Non-goals (unchanged):**

- Rebuilding molecular ingestion (`41`, `42`, `131`), multimodal contract (`128`), imaging↔FNA linkage (`129`), or specimen/FHIR (`138`–`140`).
- Using fuzzy patient matching, MRN+date fallbacks, or auto-merge of ambiguous identities.

## Schema (v1)

| Object | Grain | Role |
|--------|-------|------|
| `dicom_header_ingestion_provenance_v1` | Source row | `source_file`, `source_row_number`, `raw_payload_json`, `row_fingerprint_sha256`, `ingestion_ts`, `ingestion_run_id`, `parse_status`, `qc_flags_json`, UID columns |
| `dicom_study_header_v1` | `StudyInstanceUID` | Normalized + raw accession/dates/descriptions/IDs; modality and body-part summaries; explicit `research_id` if present in file |
| `dicom_series_header_v1` | `SeriesInstanceUID` | Series-level attributes; QC if a series UID maps to multiple studies |
| `dicom_imaging_link_exact_v1` | Deterministic link | Only **explicit_research_id** or **exact_accession** after candidate filter |
| `dicom_link_review_queue_v1` | Review item | Reason code + candidate JSON + context |

Raw source strings are **preserved** in `*_raw` / `raw_payload_json`; normalized fields are additive.

## Alias mapping

Canonical keys and inbound header aliases are defined in `config/dicom_header_aliases.yml` (versioned `version: 1`). Ingest performs **case-insensitive** column name matching after strip.

Minimum canonicalized fields when present:

- `StudyInstanceUID`, `SeriesInstanceUID`, `SOPInstanceUID`
- Accession (`AccessionNumber` / `accession` / …)
- `StudyDate`, `SeriesDate`, `Modality`, `BodyPartExamined`
- `StudyDescription`, `SeriesDescription`
- `PatientID` / `MRN`
- `InstitutionName`
- Optional cohort key: `research_id` (only if the **file** supplies it; never inferred)

## Linkage precedence

1. **explicit_research_id** — Source column maps to `research_id` and parses as integer. **Does not** consult the candidate spine first. Conflicting explicit IDs on the same study produce QC / review (`DISCORDANT_EXPLICIT_RESEARCH_ID`, `INVALID_EXPLICIT_RESEARCH_ID`).
2. **exact_accession** — Normalize accession identically to imaging↔FNA helpers (`LOWER` + strip non-alphanumeric). Match only against the **candidate UNION** (imaging nodule master, FNA history, specimen master).  
   - **Date concordance:** if both DICOM `study_date` and candidate `exam_date_yyyymmdd` exist, `|delta| > date_skew_days_max` (default **14**) sends the row to review (`DATE_DISCORDANT_ACCESSION_MATCH`). **Date alone never creates a link.**
3. No MRN, name, or “close enough” accession logic exists in code paths.

## Review queue reason codes (non-exhaustive)

| Code | Meaning |
|------|---------|
| `STUDY_HEADER_QC_BLOCKER` | Missing/malformed study UID or invalid/discordant explicit `research_id` |
| `MISSING_ACCESSION_NO_RESEARCH_ID` | No accession and no explicit `research_id` |
| `ACCESSION_NO_DB_MATCH` | Normalized accession absent from candidate spine |
| `AMBIGUOUS_ACCESSION_MULTI_RESEARCH_ID` | Same accession maps to >1 `research_id` |
| `AMBIGUOUS_ACCESSION_MULTI_IMAGING_EXAM` | Same accession and one `research_id` but >1 imaging `exam_id` |
| `DATE_DISCORDANT_ACCESSION_MATCH` | Accession matched but study vs exam date skew over threshold |

Review rows include JSON arrays of candidate IDs, modalities, and a free-text `conflict_note`.

## Relation to `imaging_nodule_master_v1`, 128, and 129

- **`imaging_nodule_master_v1`** remains the SSOT for multimodal nodule/exam grain; this layer only **references** it when building accession candidates or when future optional joins are enabled.
- **Script 128** builds the multimodal **contract** star schema; DICOM v1 tables are **parallel**, not a replacement fact table inside `mm_contract_dev` unless you explicitly integrate them in a later script.
- **Script 129** runs imaging↔FNA linkage rules unchanged. `utils/dicom_header_helpers.optional_attach_dicom_to_imaging_nodule_frame()` is explicitly a **no-op** unless `dicom_study_header_v1` exists, so downstream jobs stay safe.

## Dry-run commands

```bash
python3 scripts/150_ingest_dicom_headers.py \
  --input tests/fixtures/dicom_headers/study_series_synthetic.csv
```

**Expected:** `manifest.json` with `study: 1`, `series: 2`, `provenance: 2`, `link_*` depending on `--link-candidates-from-db`.

## Promotion notes

- Treat `exports/dicom_header_ingest_*` as **reproducible artifacts** (gitignored pattern `exports/dicom_header_ingest_*/`).
- **MotherDuck `main` contract:** `dicom_*_v1` tables are **repo-defined** and become **canonical-live** only after operator `--write-db` materialization and promotion; see `docs/motherduck_database_contract_v1.md` (DICOM subsection) and `studies/20260413_dicom_promotion_reconciliation/report.md`.
