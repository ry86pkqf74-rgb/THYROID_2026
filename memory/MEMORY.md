# MEMORY.md — repo memory index

This file indexes the structured memory store under `memory/`. Each entry
links to a single memory file (`project_*.md`, `reference_*.md`, or
`feedback_*.md`) with a one-line summary kept under 150 characters.

The repo's broader project / preference notes also live in `AGENTS.md` at
the repo root; this index supplements those for narrow, traceable
project + feedback + reference memories that benefit from a separate file
per topic (citations, cross-references, future audit).

## Index

- [Molecular genetics v2 mig_116 close-out](project_molecular_genetics_v2_mig_116_closeout.md) — 2026-04-29: canonical_molecular_genetics_v2 verified; 69 cols closed, 5 na
- [Labs family mig_115 close-out](project_labs_family_mig_115_closeout.md) — 2026-04-29: 5 lab canonicals verified via Script-347 normalizer replay; 0 drift
- [PMH rollup mig_114 close-out](project_pmh_rollup_mig_114_closeout.md) — 2026-04-29: stale PMH rollup rebuilt; 77 derived cols, 0 post-rebuild drift
- [Complications rollup mig_108 close-out](project_complications_rollup_mig_108_closeout.md) — 2026-04-29: verify-only patient rollup signoff; 49 derived cols, 0 drift
- [PMH events mig_107 close-out](project_pmh_events_mig_107_closeout.md) — 2026-04-28: 4-source PMH verification; legacy+LLM exact, synthetic verify-as-injected
- [Multi-source canonical verification](feedback_multisource_canonical_verification.md) — Pattern: stratify by source, exact rederive deterministic rows, verify synthetic rows as injected
- [clinical_date_retype_20260428](../qc_framework_v1/migrations/clinical_date_retype_20260428.md) — Script 413; six cols VARCHAR/TIMESTAMP→DATE + archive snapshots + view DDL refresh on MotherDuck
- [mig_103 medications classifier](../qc_framework_v1/migrations/103_mig_medications_apply.md) — 2026-04-29: REAL/TEMPLATE note classifier; 7501→6473 meds rows; +6 PMH; table verified
- [Script 365b close-out](project_script_365b_close_out.md) — 2026-04-22 SHA 0a2ec27; 6 canonicals (CHANGES A-N) + Option-C cleanup; 5 patterns
- [Frozen CPM source DB](reference_thyroid_ete_fix_20260413_namespace.md) — holds 78 frozen CPM cols via Scripts 212/215 lineage; do not delete
- [Dry-run before build](feedback_dryrun_signoff_before_build.md) — Tier-2 builds need dry-run + QA sign-off before any main.* CREATE OR REPLACE
- [CPM frozen at publication](feedback_cpm_frozen_at_publication.md) — Never repoint frozen CPM cols in cleanup; frozen-at-publication is correct
- [Mig 60 invasion rollup v2 refresh](../project_mig_60_invasion_rollup_refresh_closeout.md) — Closed 2026-04-24 SHA 5454cf5; v2 additive OR rollup; glands=5 patched
