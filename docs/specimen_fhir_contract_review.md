# Specimen + analytic FHIR — reviewer and release contract

**Scope:** `main.specimen_*`, `main.fhir_*_v1`, `main.fhir_bundle_specimen_export_v1`, genomics binding (`scripts/140_md_specimen_genomics_binding.py`), and **`qa.v_diag_*`** diagnostic views (`scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`).

## Operational user-agents (MotherDuck query history)

Default RW attribution: **`specimen_fhir_ref_integrity_v2`** with stable session hint (see `utils/md_pipeline_attribution.specimen_fhir_release_writer_attribution`). `MOTHERDUCK_CUSTOM_USER_AGENT` / `MOTHERDUCK_SESSION_HINT` override when set. Historical runs may show **`specimen_fhir_release_truth_v2`** or **`specimen_fhir_release_truth_v1`** in query logs.

| Step | `custom_user_agent` |
|------|---------------------|
| Identity + FHIR orchestration + QA DDL deploy (`--md`) | `specimen_fhir_ref_integrity_v2` (`scripts/138_md_specimen_fhir_layer.py`, second connection share same UA) |
| Standalone QA diagnostic deploy | `specimen_fhir_ref_integrity_v2` (`scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py`) |
| Identity-only / genomics standalone (`139` / `140`) | `specimen_fhir_ref_integrity_v2` |
| Local FHIR NDJSON export — RW attach (`--md`) | `specimen_fhir_export_restore_v1` (`scripts/141_fhir_specimen_json_export.py`; override via `MOTHERDUCK_CUSTOM_USER_AGENT`) |
| Local FHIR NDJSON export — read-scaling (`--read-scaling`) | `specimen_fhir_export_restore_v1` (same; use after `REFRESH DATABASE` on the reader). Legacy logs: `specimen_fhir_export_v1`. |

Do **not** use `MD_READ_SCALING_TOKEN` for **138** / **139** / **140** / **143** writers — those require RW tokens. Read scaling is valid for **141** when you want least-privilege export after a writer snapshot boundary.

## CI (offline)

GitHub Actions `multimodal-tests` job runs pytest (blocking, no secrets) for:

- `tests/test_specimen_identity_layer.py`
- `tests/test_specimen_fhir_layer.py`
- `tests/test_specimen_fhir_qa_diagnostics.py`
- `tests/test_specimen_genomics_binding.py`
- `tests/test_specimen_fhir_release_gate.py` (119 Check 13 / gate orchestration helpers)
- `tests/test_specimen_fhir_scripts_offline.py` — temp-DuckDB coverage for **`141`** export, **`143`** + `142` diagnostic DDL, **`144`** (`--introspect-local`), and **`138 --dry-run`** orchestrator smoke

No MotherDuck token is required for this job.

## Validation surfaces

1. **Row-level contract table** — `qa.val_specimen_contract_v1` (checks embedded in `138_md_specimen_fhir_layer.run_validation`).
2. **Genomics binding** — `qa.val_specimen_genomic_binding_v1` (script 140).
3. **Formalization gate** — `scripts/119_md_formalization_validate.py` Check 13: table presence, uniqueness, FAIL rows in both `val_*` tables, **`qa.v_diag_*`** plus **`qa.t_diag_specimen_focus_qa_metrics_v1`** (duplicates at master and focus grain, orphans, broken FHIR refs, provenance gaps), and specimen-adjacent review burden.

## Diagnostic views (`142`)

| View / table | Purpose |
|--------------|---------|
| `qa.v_diag_specimen_duplicate_master_fp_v1` | Master fingerprints with row_count &gt; 1 |
| `qa.v_diag_specimen_duplicate_focus_fp_v1` | Focus fingerprints with row_count &gt; 1 (deterministic aggregate on `specimen_tumor_focus_v1`) |
| `qa.v_diag_specimen_orphan_focus_master_v1` | Focus rows whose `specimen_id` is missing from `specimen_master_v1` |
| `qa.v_diag_specimen_orphan_genomic_master_v1` | Genomic rows referencing missing `specimen_master_v1` |
| `qa.v_diag_specimen_orphan_genomic_focus_v1` | Genomic rows whose `specimen_focus_id` is set but missing from `specimen_tumor_focus_v1` |
| `qa.v_diag_specimen_fhir_broken_refs_v1` | Subject / Procedure / Encounter / Episode reference mismatches (optional refs only when JSON path present) |
| `qa.v_diag_specimen_provenance_master_v1` | `identity_build_run_id` gaps on `specimen_master_v1` |
| `qa.v_diag_specimen_provenance_focus_v1` | Scalar rollup: `identity_build_run_id` blank/null gaps on `specimen_tumor_focus_v1` |
| `qa.v_diag_specimen_provenance_focus_gaps_v1` | Row list for the same predicate as the scalar `n_missing_identity_run` (deterministic COUNT for Check 13 vs `t_diag`) |
| `qa.v_diag_specimen_provenance_genomic_v1` | High-tier genomics rows with null `specimen_id` |
| `qa.v_diag_specimen_review_burden_v1` | Row counts by status for **genomic link** review queue |
| `qa.t_diag_specimen_focus_qa_metrics_v1` | **Table** (full rebuild per deploy): scalar rollup of focus duplicate groups, orphan focus→master, genomic→focus orphans, and focus provenance gaps — used alongside list views so `scripts/119_md_formalization_validate.py` Check 13 does not depend on ad hoc Python-issued scans of `main.specimen_tumor_focus_v1`. If this deploy step fails on a catalog, treat it as a **blocking QA DDL failure** (fix or document the engine limitation); do not silently downgrade to WARN. |

**Check 13 (`119`):** when **all** `SPECIMEN_FHIR_OBJECTS` tables exist (layer complete), focus duplicate / orphan / provenance signals are read **only** from the `qa.v_diag_*` focus surfaces and `qa.t_diag_specimen_focus_qa_metrics_v1`, matching `142` — **FAIL** on any non-zero defect counts, `val_*` FAIL rows, master fingerprint collision, missing 142 objects, metrics/view disagreement (including `SUM(row_count)` on duplicate-focus groups vs `n_rows_in_duplicate_fp_groups`, and provenance gap row count vs `n_missing_focus_provenance`), or query error when evaluating diagnostics. When the synoptic anchor exists but some specimen/FHIR tables are not yet materialized, Check 13 stays **WARN**-oriented (graceful partial deploy). No Python full-table scans of `main.specimen_tumor_focus_v1` in the validator; stability comes from SQL views + the per-deploy metrics table built in `142`.

**Pipeline note:** `scripts/138_md_specimen_fhir_layer.py` deploys `142` **before** `qa.val_specimen_contract_v1` materialization so specimen contract rows for focus fingerprint / orphan / provenance use the same predicates as the diagnostic views (single source of truth in SQL).

**PK / UNIQUE on catalogs:** `139` DDL declares `PRIMARY KEY` / `UNIQUE` on identity tables where supported; DuckLake-backed or legacy catalogs may not enforce them. **Release uniqueness** remains governed by these QA surfaces (and `val_specimen_*`), not by assuming engine-enforced constraints.

Deploy with **`138 --md`** (recommended) or **`143_md_specimen_fhir_qa_diagnostics_deploy.py --md`** if tables already exist and only views need refresh.

## Release / snapshot

Before promoting specimen/FHIR changes, **`138`** attempts `CREATE SNAPSHOT <name> OF "<database>"` (see script; DuckLake may skip). For scratch validation from a snapshot, use org-specific clone/runbook steps in [`motherduck_sandbox_clone_runbook.md`](motherduck_sandbox_clone_runbook.md) when your token role permits.

### Scope vs `115` / `118` final-master artifacts

[`scripts/115_release_snapshot.py`](115_release_snapshot.py) with `--final-master` copies **`FINAL_MASTER_TABLES`** into `release_<tag>`: canonical cores, `longitudinal_lab_canonical_v1`, and the three `master_*_verified_v1` presentation tables. It does **not** copy `specimen_*`, `fhir_*`, genomic binding tables, or `qa.v_diag_*` diagnostics — those remain authoritative in **`main`** / **`qa`** on the live catalog.

[`scripts/118_parquet_release_bundle.py`](118_parquet_release_bundle.py) with `--final-master` exports the same **manuscript analytic** subset via **`FINAL_MASTER_MAIN`** + **`FINAL_MASTER_QA`**. Consumers needing specimen, FHIR bundles, or genomics binding for interoperability should read **`main`** on MotherDuck or run [`scripts/138_md_specimen_fhir_layer.py`](138_md_specimen_fhir_layer.py) / [`scripts/140_md_specimen_genomics_binding.py`](140_md_specimen_genomics_binding.py) as documented; **Check 13** in [`scripts/119_md_formalization_validate.py`](119_md_formalization_validate.py) is the release gate for that surface, not the `115`/`118` table lists.

[`scripts/126_final_master_release.py`](126_final_master_release.py) (default `--release-mode`) calls the same [`utils/specimen_fhir_release_gate.py`](utils/specimen_fhir_release_gate.py) pathway as **124** before **`115 --final-master`** / **`118 --final-master`** — use **`--materialize-specimen-fhir`** to run **138** or **143** when the gate would otherwise block; snapshot/parquet steps remain manuscript-only by design.

## Read scaling for reviewers

After a release snapshot on the writer catalog, readers using **`MD_READ_SCALING_TOKEN`** should:

1. Export `MD_READ_SCALING_TOKEN` (and optionally **`MD_READ_SCALING_SESSION_HINT`**) — same keys supported in repo-root **`motherduck.local.toml`** or `.streamlit/secrets.toml` (see [`motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md) §8).
2. Connect via `MotherDuckClient.for_env(...).connect_read_scaling()` (or RO share when configured). For a **restricted manual hidden** share, attach with the **reviewer** or read-scaling identity only; grant **READ** on the reviewer-facing database or share — never commit share URLs that embed secrets.
3. Run **`REFRESH DATABASE`** (or `scripts/136_md_read_scaling_snapshot_refresh.py reader`, or `utils/md_read_scaling_refresh.py`) **on the read-scaling connection** after the operator’s writer snapshot so replicas honor the export/review snapshot boundary.
4. Optional NDJSON export for offline review (no PHI in bundle payloads — analytic de-identified resources only):

   ```bash
   # Operator (RW token from env, motherduck.local.toml, or .streamlit/secrets.toml)
   .venv/bin/python scripts/141_fhir_specimen_json_export.py --md

   # Reviewer (read-scaling token only; refresh first)
   MD_READ_SCALING_TOKEN=… MD_READ_SCALING_SESSION_HINT=thy_review_01 \
     .venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod
   MD_READ_SCALING_TOKEN=… MD_READ_SCALING_SESSION_HINT=thy_review_01 \
     .venv/bin/python scripts/141_fhir_specimen_json_export.py --read-scaling
   ```

   Output directory pattern: `exports/fhir_specimen_<UTC_timestamp>/` with `specimen_bundles.ndjson`, `manifest.json`, and `README.md`. That tree is **gitignored**; keep manifests or study notes under `studies/` if you need provenance in git.

### Export `manifest.json` (script 141)

Machine-readable provenance for each run:

| Field | Meaning |
|-------|---------|
| `git_sha` | `git rev-parse HEAD` (or `unknown`) |
| `timestamp` / `build_timestamp_utc` | UTC build time |
| `source_catalog` | Resolved catalog name (`current_database()`), or `MOTHERDUCK_DATABASE` / `MOTHERDUCK_DB` when set |
| `source_views` | Objects read for bundle rows: `[main.fhir_bundle_specimen_export_v1]` **or** the four reconstructed `main.fhir_*_v1` resource tables |
| `from_prebuilt_bundle_view` | `true` when rows came from `fhir_bundle_specimen_export_v1`; `false` when reconstructed |
| `export_route` | `bundle_table` \| `reconstructed_from_resources` |
| `custom_user_agent` | Default `specimen_fhir_export_restore_v1` |
| `export_source_row_count` | Rows returned by the SQL path before dropping blank JSON |
| `bundle_row_count` | Lines written to `specimen_bundles.ndjson` |
| `source_tables_main` | Row counts (or `missing`) for the FHIR-related `main.*` tables enumerated by the exporter |

Offline tests: `pytest tests/test_specimen_fhir_scripts_offline.py -k 141` (temp DuckDB; no secrets).

### Service account / org admin (reviewer identity)

This repo does **not** issue MotherDuck tokens or call Admin REST APIs. **Build operators** should use **`MD_SA_TOKEN`** or **`MOTHERDUCK_TOKEN`** (RW) from a secret manager, **`motherduck.local.toml`** (gitignored), or `.streamlit/secrets.toml`. **Reviewers** should receive a **read-scaling** token or an invitation to a **restricted** share with read-only access. Typical MotherDuck UI paths (wording may vary by product version):

- **Service account:** Organization settings → Service accounts → create → copy token once → store in reviewer-bound secret channel; scope to read-only / target share.
- **Share:** Shares → Create share → visibility **Restricted**, update policy **Manual** if you must pin snapshots; grant **Read** to the reviewer’s user or service account only.

## Query history / telemetry

Operational connection strings set `custom_user_agent` (e.g. `specimen_fhir_export_restore_v1`, `specimen_fhir_ref_integrity_v2`) for governance. On catalogs where `md_information_schema.recent_queries` is available, the `user_agent` column may still show the DuckDB client string (e.g. `duckdb/v1.4.x …`) rather than the custom UA — filter by **`query_text`** (e.g. `main.fhir_bundle_specimen_export_v1`) or use org-level MotherDuck query logs if your plan exposes custom UA there. If `RECENT_QUERIES` / `QUERY_HISTORY` are blocked by role or tier, say so in the reviewer ops report.

Details: [`motherduck_read_scaling_dashboard.md`](motherduck_read_scaling_dashboard.md).

## Machine-generated repo state

[`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../studies/CURRENT_MOTHERDUCK_REPO_STATE.md) is produced by [`scripts/144_md_repo_current_state_summary.py`](../scripts/144_md_repo_current_state_summary.py) (`--md` fills live DB sections).
