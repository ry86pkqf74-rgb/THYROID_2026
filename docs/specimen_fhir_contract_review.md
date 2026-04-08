# Specimen + analytic FHIR — reviewer and release contract

**Scope:** `main.specimen_*`, `main.fhir_*_v1`, `main.fhir_bundle_specimen_export_v1`, genomics binding (`scripts/140_md_specimen_genomics_binding.py`), and **`qa.v_diag_*`** diagnostic views (`scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`).

## Operational user-agents (MotherDuck query history)

Default RW attribution: **`specimen_fhir_release_truth_v1`** with stable session hint (see `utils/md_pipeline_attribution.specimen_fhir_release_writer_attribution`). `MOTHERDUCK_CUSTOM_USER_AGENT` / `MOTHERDUCK_SESSION_HINT` override when set.

| Step | `custom_user_agent` |
|------|---------------------|
| Identity + FHIR orchestration + QA DDL deploy (`--md`) | `specimen_fhir_release_truth_v1` (`scripts/138_md_specimen_fhir_layer.py`, second connection share same UA) |
| Standalone QA diagnostic deploy | `specimen_fhir_release_truth_v1` (`scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py`) |
| Identity-only / genomics standalone (`139` / `140`) | `specimen_fhir_release_truth_v1` |
| Local FHIR NDJSON export reader (`--md`) | `specimen_fhir_export_v1` (`scripts/141_fhir_specimen_json_export.py` — read-mostly export helper) |

Do **not** use `MD_READ_SCALING_TOKEN` for these writers — read scaling is attach-only for reviewer dashboards.

## CI (offline)

GitHub Actions `multimodal-tests` job runs pytest for identity (`tests/test_specimen_identity_layer.py`), FHIR layer/QA/genomics contracts, and temp-DuckDB coverage for **`141`** export (`--local-duckdb`), **`143`** + `142` diagnostic views, and **`144`** (`--introspect-local`) — no MotherDuck token required.

## Validation surfaces

1. **Row-level contract table** — `qa.val_specimen_contract_v1` (checks embedded in `138_md_specimen_fhir_layer.run_validation`).
2. **Genomics binding** — `qa.val_specimen_genomic_binding_v1` (script 140).
3. **Formalization gate** — `scripts/119_md_formalization_validate.py` Check 13: table presence, uniqueness, FAIL rows in both `val_*` tables, **`qa.v_diag_*`** aggregates (duplicates, orphans, broken FHIR refs, provenance gaps), and specimen-adjacent review burden.

## Diagnostic views (`142`)

| View | Purpose |
|------|---------|
| `qa.v_diag_specimen_duplicate_master_fp_v1` | Master fingerprints with row_count &gt; 1 |
| `qa.v_diag_specimen_orphan_genomic_master_v1` | Genomic rows referencing missing `specimen_master_v1` |
| `qa.v_diag_specimen_fhir_broken_refs_v1` | Subject / Procedure / Encounter / Episode reference mismatches (optional refs only when JSON path present) |
| `qa.v_diag_specimen_provenance_master_v1` | `identity_build_run_id` gaps on `specimen_master_v1` |
| `qa.v_diag_specimen_provenance_genomic_v1` | High-tier genomics rows with null `specimen_id` |
| `qa.v_diag_specimen_review_burden_v1` | Row counts by status for **genomic link** review queue |

**Also (script 119, best-effort):** duplicate / orphan **focus** fingerprints, genomic→focus orphans, and focus provenance gaps — SQL issued directly against `main.specimen_tumor_focus_v1` when the catalog allows full scans (some MotherDuck builds error on aggregates over that table; validator then **WARN**s with `focus-table scans unavailable`).

Deploy with **`138 --md`** (recommended) or **`143_md_specimen_fhir_qa_diagnostics_deploy.py --md`** if tables already exist and only views need refresh.

## Release / snapshot

Before promoting specimen/FHIR changes, **`138`** attempts `CREATE SNAPSHOT <name> OF "<database>"` (see script; DuckLake may skip). For scratch validation from a snapshot, use org-specific clone/runbook steps in [`motherduck_sandbox_clone_runbook.md`](motherduck_sandbox_clone_runbook.md) when your token role permits.

### Scope vs `115` / `118` final-master artifacts

[`scripts/115_release_snapshot.py`](115_release_snapshot.py) with `--final-master` copies **`FINAL_MASTER_TABLES`** into `release_<tag>`: canonical cores, `longitudinal_lab_canonical_v1`, and the three `master_*_verified_v1` presentation tables. It does **not** copy `specimen_*`, `fhir_*`, genomic binding tables, or `qa.v_diag_*` diagnostics — those remain authoritative in **`main`** / **`qa`** on the live catalog.

[`scripts/118_parquet_release_bundle.py`](118_parquet_release_bundle.py) with `--final-master` exports the same **manuscript analytic** subset via **`FINAL_MASTER_MAIN`** + **`FINAL_MASTER_QA`**. Consumers needing specimen, FHIR bundles, or genomics binding for interoperability should read **`main`** on MotherDuck or run [`scripts/138_md_specimen_fhir_layer.py`](138_md_specimen_fhir_layer.py) / [`scripts/140_md_specimen_genomics_binding.py`](140_md_specimen_genomics_binding.py) as documented; **Check 13** in [`scripts/119_md_formalization_validate.py`](119_md_formalization_validate.py) is the release gate for that surface, not the `115`/`118` table lists.

## Read scaling for reviewers

After a release snapshot on the writer catalog, readers using **`MD_READ_SCALING_TOKEN`** should:

1. Connect via `MotherDuckClient.for_env(...).connect_read_scaling()` (or RO share when configured).
2. Set **`MD_READ_SCALING_SESSION_HINT`** (or equivalent `session_hint`) for stable routing.
3. Run **`REFRESH DATABASE`** (or `utils/md_read_scaling_refresh.py` helpers) so read replicas observe the new snapshot boundary.

Details: [`motherduck_read_scaling_dashboard.md`](motherduck_read_scaling_dashboard.md).

## Machine-generated repo state

[`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../studies/CURRENT_MOTHERDUCK_REPO_STATE.md) is produced by [`scripts/144_md_repo_current_state_summary.py`](../scripts/144_md_repo_current_state_summary.py) (`--md` fills live DB sections).
