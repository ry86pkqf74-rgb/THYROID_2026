# Methodology — TI-RADS / FNA / molecular / pathology linkage study

## Objective

Produce an auditable, **deterministic** patient-nodule longitudinal trail from structured ultrasound nodule rows through FNA (Bethesda), molecular testing, and definitive surgical pathology, reusing repository linkage surfaces (`imaging_fna_linkage_mm_v1`, `fna_molecular_linkage_v3`, `preop_surgery_linkage_v3`, `surgery_pathology_linkage_v3`).

## PHI boundary

- Outputs contain **no clinical note body text**; contexts are structured IDs, dates, categories, and capped JSON.
- Full-text fields from imaging/FNA are limited to existing short specimen/site descriptors present in structured tables.

## Execution

1. **Inventory** — `information_schema` + row counts for candidate inputs → `schema_inventory.md`, `source_profile.csv`.
2. **Linkage SQL** — `utils/canonical_nodule_linkage.py` builds a single `WITH` tree; `scripts/149_md_canonical_nodule_linkage_study.py` executes read-only and exports Parquet/CSV.
3. **QA** — `linkage_qc_summary` reproduces upstream yields; `discordance_summary` tracks clinically meaningful disagreements; `validation_report.{md,json}` records counts.

## MotherDuck safety

- Default operator path: **SELECT-only** exports under `studies/`; no base-table writes.
- **VIEW materialization** (optional): `149 --materialize-view` issues `CREATE OR REPLACE VIEW main.canonical_nodule_linkage_study_v1` only.
  - **Prod:** requires `--md-env prod` **and** `--confirm-prod-view` (explicit opt-in).
  - **Dev/QA:** use `--md-env dev|qa` when the clone has full linkage DDL (often after `130` refresh); otherwise the script fails fast on missing v3 tables.
- Follow `docs/motherduck_database_contract_v1.md` for promotion and immutable `release_*` snapshots.

## Makefile release tag

`make md-live-release-dryrun` / `md-live-release-final` pass `--tag $(MD_RELEASE_TAG)` (default: UTC date).
If `release_${MD_RELEASE_TAG}` already exists, set e.g. `export MD_RELEASE_TAG=20260410` before `make`.

## Sensitivity analyses

- **Exclude NIFTP** from primary malignancy numerators using `niftp_flag = FALSE` on `canonical_nodule_linkage`.
- Include all rows in supplementary sets and cite `discordance_summary.niftp_rows` for prevalence context.

## Limits

- Imaging spine is **per exam nodule** (`nodule_id` embeds exam); serial longitudinal identity across exams is **not** re-derived here (see script 50 / imaging patient summaries for future enhancement).
- When `imaging_fna_linkage_mm_v1` is absent, the study fails fast — operator must run script 129 first.
