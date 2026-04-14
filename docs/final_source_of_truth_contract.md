# Final source of truth — repo and MotherDuck contract

This document is the **single canonical contract** for what counts as live truth versus historical or adjunct material. Read this before citing row counts, release tags, or “verified” status in manuscripts or dashboards.

## Canonical live source

- **Catalog:** MotherDuck database **`main`** holds promoted analytic tables and analyst-facing views.
- **Governance:** Schema **`qa`** holds append-only release history and manual review queues (`qa.release_manifest`, `qa.manual_review_queue`, etc.). It is part of the **live** operational surface, not a separate “staging only” fiction.

Local file `thyroid_master.duckdb` (when used) is a **developer artifact** and may diverge from cloud promotion state — do not cite it as production SSOT without explicit reconciliation.

## Canonical release ledger

- **Table:** `qa.release_manifest`
- **Role:** Ordering and provenance for release tags (e.g. `release_tag`, `git_sha`, `created_at`). **Live MotherDuck is authoritative** for promotion history.
- **Not interchangeable with:** checked-in JSON under `exports/release_manifests/` (see [Historical only](#historical-only-not-live-ssot)).

## Canonical analyst surfaces

These **views** on `main` are the agreed analyst presentation layer (deployed by `scripts/125_master_verified_views.py`):

| Object | Role |
|--------|------|
| `main.master_fact_long_verified_v1` | One row per extracted fact with traceability columns and release tag |
| `main.master_patient_rollup_verified_v1` | Per-patient aggregates over the long view |
| `main.master_source_lineage_v1` | Provenance-oriented projection of the long view |

Upstream promoted facts: `main.canonical_extracted_fact_long_v2`; quarantine: `main.canonical_fact_quarantine_v2`.

## Reviewer status grain (critical)

**`reviewer_status` and related reviewer columns in `master_*_verified_v1` are not per-fact human validation by default.** They are joined from `qa.manual_review_queue` at **`(research_id, domain)`** grain (one queue row per patient per domain after deduplication in the view). The views expose explicit columns:

- `review_grain` — always `research_id_domain` for the long and lineage views
- `review_status_source` — `qa.manual_review_queue`
- `review_join_key` — deterministic key matching the join (e.g. `research_id|fact_domain`)

Row-level or fact-level human review requires evidence in queue or decision tables at that grain — **do not** infer it from propagated columns alone.

## Canonical current-state artifact (repo)

- **Path:** `studies/CURRENT_MOTHERDUCK_REPO_STATE.md`
- **Generator:** `scripts/144_md_repo_current_state_summary.py` (use `--md` with credentials via `motherduck_client.get_token()` / `motherduck.local.toml`; never log tokens)

This file reconciles **optional live introspection** with **checked-in** pointers. It is **machine-generated reconciliation**, not a substitute for a fresh `scripts/119_md_formalization_validate.py --md --release-mode` report for release verdicts.

## Historical only (not live SSOT)

Treat these as **point-in-time or archival** unless regenerated and explicitly promoted:

- Checked-in files under `exports/release_manifests/*.json` (including `LATEST_MANIFEST.json` unless refreshed from live — see `exports/release_manifests/README.md`)
- Dated evidence packs under `studies/` (e.g. per-release `EVIDENCE_PACK.md` files) and duplicate `CURRENT_MOTHERDUCK_REPO_STATE.md` copies nested under `studies/` (not the root `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` default output)
- March 2026 local manuscript freeze, Zenodo bundles, and local DuckDB snapshots referenced in older docs

## Specimen and FHIR layers

**Specimen** and **analytic FHIR** objects on `main` (e.g. `specimen_*`, `fhir_*` bundles) are **validated adjunct** layers for interoperability and QA. They are **gated** by release validation (e.g. `119` Check 13) but are **not** the same slice as manuscript-analytic `release_*` schemas or `118 --final-master` Parquet bundles. See `docs/specimen_fhir_contract_review.md` and `tests/test_release_final_master_surface.py`.

## Duplicate natural keys on facts

Multiple fact rows may share the same natural key `(research_id, source_domain, source_object_id, entity_type, entity_value_norm, entity_date)` when extraction grain or lab semantics allow (e.g. multi-analyte rows). **`119` release-mode** may WARN on duplicate-key groups; investigate before treating as a promotion bug.

## Related documents

- **This file:** `docs/final_source_of_truth_contract.md` (SSOT narrative anchor for CI phrase checks)

- `docs/motherduck_database_contract_v1.md` — broader DB contract
- `docs/specimen_fhir_contract_review.md` — specimen/FHIR scope
- `studies/ssot_cleanup_audit_20260414T013901Z/report.md` — read-only audit motivating this contract
