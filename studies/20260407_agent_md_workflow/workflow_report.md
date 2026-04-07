# MotherDuck agent workflow — 2026-04-07

MotherDuck MCP was not enabled in the Cursor workspace; operations used `duckdb` + `scripts/130_md_env_bootstrap.py`, `scripts/131_molecular_results_layer.py`, and ad-hoc Python in this folder (token via `motherduck_client` / `.streamlit/secrets.toml`).

## Databases

| Role | Name | Notes |
|------|------|--------|
| Production | `Thyroid 2026` | Type `DUCKLAKE` |
| Dev (refreshed) | `Thyroid 2026 Molecular Dev 20260407` | Zero-copy from prod, then molecular layer DDL |
| QA (existing) | `Thyroid 2026 Molecular QA 20260407` | Listed; not modified |
| PrePromote backup | `Thyroid 2026 Molecular PrePromote agent_20260407_workflow` | Zero-copy rollback handle (not prod data mutation) |

## Snapshot note (prod)

Named `CREATE SNAPSHOT name OF "Thyroid 2026"` is **not supported** for `DUCKLAKE` (script skips; see `docs/motherduck_sandbox_clone_runbook.md`). Latest automatic snapshot row on prod at run time was `snapshot_id=b72a425d-0a47-4446-bf89-6cb098b8922d` (`MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS`).

## Executed SQL (mutations)

```sql
CREATE OR REPLACE DATABASE "Thyroid 2026 Molecular PrePromote agent_20260407_workflow" FROM "Thyroid 2026";
```

```sql
DROP DATABASE IF EXISTS "Thyroid 2026 Molecular Dev 20260407";
CREATE DATABASE "Thyroid 2026 Molecular Dev 20260407" FROM "Thyroid 2026";
```

Plus full `scripts/sql/131_molecular_results_layer_ddl.sql` (applied via `131_molecular_results_layer.py --execute --md --md-env dev`).

## Prod inspection (read-only)

In **`Thyroid 2026` / `main`** at inspection time:

- `molecular_testing`, `thyroseq_molecular_enrichment`, `thyroseq_followup_labs`, `thyroseq_followup_events`: **not present**
- `note_entities_genetics`: **present** (~1,738 rows); columns include `research_id`, `note_row_id`, `entity_type`, `entity_value_norm`, `verification_status`, etc.
- `molecular_results`, `molecular_variant_long`: tables exist; **0 rows** in prod snapshot inspected
- `molecular_assay_dictionary`: **0 rows** in prod before dev DDL refresh path
- `molecular_code_crosswalk`: **17 rows** in prod
- `molecular_results_contract_v`, `molecular_fact_long_v`: **not in main**
- `canonical_extracted_fact_long_v2`: **present**

ThyroSeq/Afirma structured staging tables (`thyroseq_*`) were not found in `information_schema` for this catalog; `v2_stage` includes `note_entities_llm_molecular_thyroseq_afirma`.

## Dev validation summary (after 131)

| Slice | Rows |
|-------|------|
| `note_entities_genetics` | 1,738 |
| `molecular_results` | 0 |
| `molecular_variant_long` | 0 |
| `molecular_assay_dictionary` | 4 (Afirma seed) |
| `molecular_code_crosswalk` | 44 (131 seeds + overlays) |

- All `note_entities_genetics.verification_status` = `unverified` (1,738) — QC proxy for “pending verification”
- Assay dictionary: **4** rows, all `platform = Afirma`
- Structured governed layer QC flags: **0** rows (no ingest yet)
- Distinct `assay_name` not in dictionary: **0** (empty `molecular_results`)

Top gene mentions from notes: BRAF, NRAS, HRAS, RET, RAS, TERT, KRAS, ALK (same order as `run_validation.py`).

## Dive

| Field | Value |
|-------|--------|
| ID | `bdf800d0-7065-4fe9-b9b5-5d0eb32a68fb` |
| Title | THYROID 2026 Molecular Dev — molecular QC (2026-04-07) |
| URL | https://app.motherduck.com/dives/bdf800d0-7065-4fe9-b9b5-5d0eb32a68fb |

Created with `MD_CREATE_DIVE` (`create_dive.py`).

## Reproduce

```bash
cd THYROID_2026
.venv/bin/python studies/20260407_agent_md_workflow/run_validation.py
```
