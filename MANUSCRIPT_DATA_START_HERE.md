# Manuscript data — start here

> **Last verified:** 2026-04-14 (canonical lakehouse finalization pass).
> **Full contract:** [`docs/final_source_of_truth_contract.md`](docs/final_source_of_truth_contract.md).

## Live source of truth

Live MotherDuck database **`Thyroid 2026`** (`main` + `qa` schemas) is canonical. Local `thyroid_master.duckdb` is a developer artifact and must not be cited as production SSOT.

## Analyst-facing tables and views

Use these three views for manuscript analyses. All are deployed by `scripts/125_master_verified_views.py`.

| View | Role | Grain |
|------|------|-------|
| `main.master_fact_long_verified_v1` | One row per extracted fact with release tag and reviewer status | fact |
| `main.master_patient_rollup_verified_v1` | Per-patient aggregates (fact counts, review coverage) | patient |
| `main.master_source_lineage_v1` | Full provenance chain (extraction run, reviewer, release tag) | fact |

### Supporting canonical tables

| Table | Role |
|-------|------|
| `main.canonical_extracted_fact_long_v2` | Upstream promoted facts (55,500 rows) |
| `main.canonical_fact_quarantine_v2` | Quarantined facts excluded from canonical (199 rows) |
| `main.longitudinal_lab_canonical_v1` | Structured lab results (Tg, TgAb, PTH, calcium) |
| `main.longitudinal_lab_deduped_v` | Deduplicated lab view |

### Specimen/FHIR (validated adjunct — not core manuscript SSOT)

| Table | Role |
|-------|------|
| `main.specimen_master_v1` | Per-specimen metadata (10,139 rows) |
| `main.specimen_tumor_focus_v1` | Tumor focus details (11,103 rows) |
| `main.specimen_genomic_assay_v1` | Genomic assay linkage (10,370 rows) |
| `main.fhir_bundle_specimen_export_v1` | FHIR bundle export (10,139 rows) |

## Row count citation rule

**Always cite from live MotherDuck.** Never cite row counts from checked-in snapshots, evidence packs, or `.json` manifests unless they were regenerated from live data and timestamped. Run `scripts/144_md_repo_current_state_summary.py --md` to get current counts.

## Reviewer status caveat

Reviewer status in verified views is joined at **(research_id, domain)** grain from `qa.manual_review_queue` — it is **not** per-fact human adjudication. The `review_grain` column on each view makes this explicit. As of 2026-04-14, all 5,622 MRQ rows are automation-tier (`auto_accepted_*`); **zero** are true human-reviewed.

## Can we start manuscripts now?

**Technically yes, with a governance caveat.** The technical SSOT is complete: all 55,500 canonical facts are linked, deduplicated at the fact_id level, and pass `119 --release-mode` structural validation. However, manuscript-grade human validation (named reviewer sign-off on MRQ rows) has not been performed. If your publication policy requires human chart review evidence, that remains an open blocker — see `docs/publication_governance_gate.md`.

## Files to ignore (historical only)

These are point-in-time snapshots. Do not cite them as current truth:

- `exports/release_manifests/LATEST_MANIFEST.json` — labeled `role: historical_checkpoint`; live SSOT is `qa.release_manifest`
- `studies/20260407_*/` — superseded by later releases
- `studies/20260408_*/` — superseded
- `studies/20260409_final_master_release/` — superseded by `20260411` pack
- Any `EVIDENCE_PACK.md` under `studies/` — point-in-time; prefer live SQL

## Key scripts

| Script | Purpose | Mode |
|--------|---------|------|
| `scripts/144_md_repo_current_state_summary.py --md` | Regenerate `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` | read-only |
| `scripts/119_md_formalization_validate.py --md --release-mode` | Full structural + release validation | read-only |
| `scripts/125_master_verified_views.py --md` | Deploy/refresh analyst views | writes views |
| `scripts/120_review_queue_triage.py --md` | Export MRQ triage worklists | read-only |
| `scripts/126_final_master_release.py --md` | Full release orchestration | writes (requires inputs) |
