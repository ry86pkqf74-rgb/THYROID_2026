# One-page manuscript safety memo — THYROID_2026 final master (2026-04-07)

## What is signed off

- **Structured thyroglobulin lab feed** ingested from `Thyroid_Thyroglobulin_Lab_20251120.csv` via `113_tg_lab_ingestion.py` with PII stripped, deterministic keys, and QC in `processed/tg_lab_ingestion_qc_v1.json`.
- **Canonical note-derived facts** in `main.canonical_extracted_fact_long_v2` with **100% non-null `extraction_run_id`** (release validation).
- **Frozen tables** under **`release_20260407_final2`** including labs and `master_*_verified_v1` materializations suitable for analyst extracts.

## What is explicitly *not* claimed

- **No raw note text** in MotherDuck or curated parquet bundles.
- **`reviewer_status` NULL** on most presentation-layer facts: extraction is traceable, but **not** individually adjudicated unless a queue row applies to that patient-domain pair.
- **`SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`** (5,620 queue rows): governance automation — **do not** describe as clinician manual review.

## Numbers to cite

Use **`release_20260407_final2`** row counts from `qa.release_manifest` / `RELEASE_MANIFEST.md` so citations align to tag **`20260407_final2`**. The live `main.master_*` views may show a different `release_tag` due to manifest ordering by numeric tag; prefer the release schema for locked totals.

## Residual uncertainty

- **199** quarantine facts — excluded from primary long canonical.
- **Lab** unmatched `research_id` tail and interpretation limits documented in QC JSON.
- **Patient rollup `pct_reviewed` ~14% mean** — reflects queue overlap, not chart completion.

## Recovery / backup posture

MotherDuck reports **DUCKLAKE** for this database; **native snapshot / PIT restore features may be unavailable** (see preflight error on `md_information_schema.snapshots`). **Recovery path:** retain immutable **`release_*` schemas**, `qa.release_manifest`, and **`exports/parquet_release_20260407_final2`**.

## Governance note

Schema **`release_20260407_final`** from the same calendar window was superseded by **`20260407_final2`** after queue reconciliation; do not use the former for publication extracts.
