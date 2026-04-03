# MotherDuck Connection Hardening — 2026-04-03

## Summary

Centralized all MotherDuck (MD) connection logic through the canonical shared layer
`utils/md_connect.py :: connect_md_or_file()`. Prior to this change, many scripts
that accepted `--md` resolved a token but then silently connected to a local
`thyroid_master.duckdb` file, meaning `--md` had no real effect.

## Shared Connection Layer

```
utils/md_connect.py :: connect_md_or_file(db_path, *, md, env)
```

- When `md=True`: resolves token via `motherduck_client.get_token()`, connects
  through `MotherDuckClient.for_env()`, prints connection target.
- When `md=False` or no token available: falls back to local DuckDB file at `db_path`.
- Single source of truth — no script should resolve tokens or call
  `MotherDuckClient().connect_rw()` directly.

## Fixed Files (by category)

### Group A — Direct MotherDuckClient inline blocks (17 files)

| Script | Pattern replaced |
|--------|-----------------|
| `15_date_association_audit.py` | `MotherDuckClient().connect_rw()` + ATTACH |
| `16_reconciliation_v2.py` | `MotherDuckClient().connect_rw()` |
| `17_semantic_cleanup_v3.py` | `MotherDuckClient().connect_rw()` |
| `18_adjudication_framework.py` | `MotherDuckClient().connect_rw()` |
| `19_reviewer_persistence.py` | `MotherDuckClient().connect_rw()` |
| `20_manuscript_exports.py` | `MotherDuckClient().connect_rw()` |
| `21_validation_tests.py` | `MotherDuckClient().connect_rw()` |
| `27_fix_legacy_episode_compatibility.py` | `MotherDuckClient(cfg).connect_rw()` |
| `28_manual_review_export.py` | `MotherDuckClient(cfg).connect_rw()` + try/except |
| `31_analytic_models.py` | `MotherDuckClient(cfg).connect_rw()` |
| `36_statistical_analysis_examples.py` | `MotherDuckClient(cfg).connect_rw()` |
| `37_publication_export.py` | `_get_con()` with `MotherDuckClient()` |
| `38_advanced_survival_analysis.py` | `MotherDuckClient(cfg).connect_rw()` |
| `39_gap_remediation.py` | `MotherDuckClient().connect_rw()` |
| `40_benign_classification.py` | `MotherDuckClient().connect_rw()` |
| `100_canonical_metrics_registry.py` | `MotherDuckClient.for_env().connect_rw()` |
| `107_global_completion_oed_path_linkage.py` | Unconditional `connect_rw()` |
| `108_synoptic_tumor_long_v1.py` | `materialize_motherduck()` with inline token |
| `109_synoptic_encounter_qc.py` | `get_token()` + `MotherDuckClient(cfg)` |
| `110_operative_notes_full_history_scan.py` | `MotherDuckClient().connect_rw()` |
| `_fix_missing_v2_tables.py` | `MotherDuckClient(cfg).connect_rw()` |

### Group B — Custom connection functions with broken MD paths (10 files)

| Script | Function replaced |
|--------|------------------|
| `03_research_views.py` | `_connect(use_md)` |
| `04_publication_exports.py` | `_connect(use_md)` |
| `38_mixture_cure_models.py` | `_get_con(use_md, use_local)` |
| `39_promotion_time_cure_models.py` | `_get_con(use_md, use_local)` |
| `40_predictive_analytics_batch.py` | `_get_con(use_md, use_local)` |
| `95_episode_linkage_repair.py` | `get_connection(args)` |
| `96_episode_downstream_repair.py` | `get_connection(args)` |
| `98_multi_surgery_artifact_linkage_audit.py` | `get_connection()` |
| `100_episode_linkage_v2_hardening.py` | `get_connection(args)` |
| `106_ct_imaging_date_recovery.py` | `get_connection(args)` |

### Group C — Inline `if args.md:` blocks in main() (4 files)

| Script | Block replaced |
|--------|---------------|
| `67_database_hardening_validation.py` | Inline token resolution |
| `68_lab_ingestion_scaffold.py` | Inline token resolution |
| `69_manuscript_reconciliation.py` | Inline token resolution |
| `70_canonical_backfill.py` | Inline token resolution |

### Group D — Automated patcher fixes (48 files)

Scripts fixed by `_patch_md_connections.py` (9 files) and
`_patch_md_connections_v2.py` (39 files) — all replaced `connect_md()` /
`get_connection()` / `connect()` function bodies with `connect_md_or_file`.

### Group E — Dead code cleanup (10 files)

| Script | Cleanup |
|--------|---------|
| `46_provenance_audit.py` | Removed dead `_get_token()` |
| `48_build_analysis_resolved_layer.py` | Removed dead `_get_token()` |
| `49_enhanced_linkage_v3.py` | Removed dead `_get_token()` |
| `50_multinodule_imaging.py` | Removed dead `_get_token()` |
| `51_thyroid_scoring_systems.py` | Removed dead `_get_token()` |
| `52_complication_phenotyping_v2.py` | Removed dead `_get_token()` |
| `53_longitudinal_lab_hardening.py` | Removed dead `_get_token()` |
| `55_analysis_validation_suite.py` | Removed dead `_get_token()` |
| `90_manuscript_freeze_rebuild.py` | Removed dead code lines |
| `105_manuscript_freeze_v1.py` | Fixed `get_rw_connection()` |
| `113_tg_lab_ingestion.py` | Replaced `_get_token()` + `connect_duckdb()` |

### Already Compliant (no changes needed)

| Script | Status |
|--------|--------|
| `77_lab_canonical_layer.py` | Already using `connect_md_or_file` |
| `112_ata_primary_source_scan_and_scoring.py` | Orchestrator — delegates `--md` to subprocess |

## Smoke Test

```bash
# Local connection
.venv/bin/python scripts/smoke_test_md_connection.py

# MotherDuck connection (requires MOTHERDUCK_TOKEN or LOCAL_DB_PATH)
.venv/bin/python scripts/smoke_test_md_connection.py --md
```

## What Changed

- **Before**: ~80 scripts resolved LOCAL_DB_PATH tokens but then called
  `duckdb.connect("thyroid_master.duckdb")` — connecting to a local file regardless
  of `--md`.
- **After**: All scripts delegate to `utils.md_connect.connect_md_or_file()`, which
  properly uses `MotherDuckClient` when a token is available and `--md` is set.
- **No schema changes**: Only connection plumbing was modified.
- **Local fallback preserved**: If no MD token is found, all scripts fall back to
  the local DuckDB file.
