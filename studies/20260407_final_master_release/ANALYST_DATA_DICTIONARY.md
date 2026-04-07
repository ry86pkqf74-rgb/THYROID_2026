# Analyst-safe data dictionary — final master release

**Scope:** MotherDuck schema objects used for manuscript-ready analysis. **No raw note text** appears in listed tables or exports.

## Core fact tables

### `main.canonical_extracted_fact_long_v2`

Long-format extracted clinical entities (wide note-level promotion). Key columns:

| Column | Description |
|--------|-------------|
| `research_id` | Patient research identifier |
| `fact_id` | Deterministic fact id |
| `fact_domain` / `entity_type` | Domain and entity (registry-normalized) |
| `entity_value_norm`, `entity_value_raw` | Normalized and raw values (raw is not full note) |
| `note_row_id` | Stable id linking to note **metadata** (not note body in MD) |
| `extraction_run_id` | FK to `note_extraction_runs` (non-null per release contract) |
| `extracted_at`, `extraction_method`, `confidence` | Telemetry |

### `main.canonical_fact_quarantine_v2`

Downgraded / invalid facts excluded from primary analytic long table. Use for audit only unless explicitly analysing extraction failures.

### `main.note_extraction_runs`

One row per extraction batch run: `run_id`, timestamps, git/build metadata.

## Lab canon

### `main.thyroglobulin_lab_canonical_v1`

Script-113 structured Tg/TgAb canonical rows (assay, numeric/qualifier, temporal window). See `scripts/113_tg_lab_ingestion.py` for column semantics.

### `main.longitudinal_lab_canonical_v1`

Long-format lab timeline (multiple sources; script-113 wave idempotent on `source_script`). Join to patients on `research_id`.

### View `main.longitudinal_lab_deduped_v`

Deterministic dedupe surface over longitudinal labs (see `117` DDL).

## Presentation layer (analyst-facing)

### `main.master_fact_long_verified_v1` (view)

Adds **`reviewer_status`** and reviewer metadata by joining `qa.manual_review_queue` on `(research_id, domain)` (latest row). **`release_tag`** column follows “latest manifest” rule — for pinned tag use release schema.

### `main.master_patient_rollup_verified_v1` (view)

Per-patient aggregates (fact counts by linkage family, `pct_reviewed`, etc.).

### `main.master_source_lineage_v1` (view)

Joins fact → extraction run → reviewer → release tag; exposes `source_object_id` (= `note_row_id`).

## Frozen release copy

**Use for citation-stable extracts:** `release_20260407_final2.<table>` — same logical tables as `main` plus explicit `release_tag`.

## QA (non-PHI operational tables)

- `qa.manual_review_queue` — discordance / fill-candidate work queue (truncated exports in bundle omit `review_reason` where triage script applies).
- `qa.release_manifest` — immutable registry of release snapshots.
