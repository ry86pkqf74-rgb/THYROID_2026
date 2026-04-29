# MEMORY.md — repo memory index

This file indexes the structured memory store under `memory/`. Each entry
links to a single memory file (`project_*.md`, `reference_*.md`, or
`feedback_*.md`) with a one-line summary kept under 150 characters.

The repo's broader project / preference notes also live in `AGENTS.md` at
the repo root; this index supplements those for narrow, traceable
project + feedback + reference memories that benefit from a separate file
per topic (citations, cross-references, future audit).

## Index

- [clinical_date_retype_20260428](../qc_framework_v1/migrations/clinical_date_retype_20260428.md) — Script 413; six cols VARCHAR/TIMESTAMP→DATE + archive snapshots + view DDL refresh on MotherDuck
- [Script 365b close-out](project_script_365b_close_out.md) — 2026-04-22 SHA 0a2ec27; 6 canonicals (CHANGES A-N) + Option-C cleanup; 5 patterns
- [Frozen CPM source DB](reference_thyroid_ete_fix_20260413_namespace.md) — holds 78 frozen CPM cols via Scripts 212/215 lineage; do not delete
- [Dry-run before build](feedback_dryrun_signoff_before_build.md) — Tier-2 builds need dry-run + QA sign-off before any main.* CREATE OR REPLACE
- [CPM frozen at publication](feedback_cpm_frozen_at_publication.md) — Never repoint frozen CPM cols in cleanup; frozen-at-publication is correct
- [Mig 60 invasion rollup v2 refresh](../project_mig_60_invasion_rollup_refresh_closeout.md) — Closed 2026-04-24 SHA 5454cf5; v2 additive OR rollup; glands=5 patched
