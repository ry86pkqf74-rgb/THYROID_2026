# Specimen + analytic FHIR — reviewer and release contract

**Scope:** `main.specimen_*`, `main.fhir_*_v1`, `main.fhir_bundle_specimen_export_v1`, genomics binding (`scripts/140_md_specimen_genomics_binding.py`), and **`qa.v_diag_*`** diagnostic views (`scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`).

## Operational user-agents (MotherDuck query history)

| Step | `custom_user_agent` |
|------|---------------------|
| Full identity + FHIR + genomics pipeline | `specimen_fhir_export_v1` (`scripts/138_md_specimen_fhir_layer.py`) |
| QA diagnostic view deploy (second connection on `--md`) | `specimen_fhir_release_ops_v1` (same script + `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py`) |
| Genomics binding body | `specimen_genomics_binding_v1` (`140_md_specimen_genomics_binding.py`) |

Do **not** use `MD_READ_SCALING_TOKEN` for these writers — read scaling is attach-only for reviewer dashboards.

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

## Read scaling for reviewers

After a release snapshot on the writer catalog, readers using **`MD_READ_SCALING_TOKEN`** should:

1. Connect via `MotherDuckClient.for_env(...).connect_read_scaling()` (or RO share when configured).
2. Set **`MD_READ_SCALING_SESSION_HINT`** (or equivalent `session_hint`) for stable routing.
3. Run **`REFRESH DATABASE`** (or `utils/md_read_scaling_refresh.py` helpers) so read replicas observe the new snapshot boundary.

Details: [`motherduck_read_scaling_dashboard.md`](motherduck_read_scaling_dashboard.md).

## Machine-generated repo state

[`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../studies/CURRENT_MOTHERDUCK_REPO_STATE.md) is produced by [`scripts/144_md_repo_current_state_summary.py`](../scripts/144_md_repo_current_state_summary.py) (`--md` fills live DB sections).
