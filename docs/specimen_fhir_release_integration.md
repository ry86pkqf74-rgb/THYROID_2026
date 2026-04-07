# Specimen / FHIR layer and release orchestration

## Scope

Check **13** in `scripts/119_md_formalization_validate.py` enforces the specimen identity + analytic FHIR surface **when** `main.synoptic_tumor_long_v1` exists. That surface is materialized by:

- **`scripts/138_md_specimen_fhir_layer.py`** — identity DDL (139), FHIR tail (138 SQL), genomics binding (140), validation rows, and deploy of **`scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`**
- **`scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py`** — **142** only (when main tables already exist but `qa.v_diag_*` views are missing)

Manuscript / final-master **parquet** bundles intentionally **exclude** specimen/FHIR tables (see `docs/specimen_fhir_contract_review.md`); Check 13 still applies to the **live** `main` / `qa` catalog.

## Fail-closed preflight (124 / 126 / 137)

Shared logic lives in `utils/specimen_fhir_release_gate.py`.

| Condition | Behavior |
|-----------|----------|
| `synoptic_tumor_long_v1` **absent** | Gate **PASS** (Check 13 N/A); validator skips specimen section. |
| Anchor **present**, tables or `v_diag_*` **incomplete** | **124** with `--final-release` **exits before** step 5 unless you pass **`--materialize-specimen-fhir`** or pre-build the layer. |
| Same, **126** with default `--release-mode` | Same preflight **after** `125`, **before** `115` / `118`. |
| **`--skip-specimen-fhir-gate`** | No early exit; **119** `--release-mode` may still **FAIL** on Check 13. |
| **`--materialize-specimen-fhir`** | Runs **138** if any core `main.*` specimen/FHIR table is missing; else **143** if only diagnostic views are missing. Subprocess gets `--md` only when the parent orchestrator uses MotherDuck (**124** `--md`, **126** always `--md`). |

**137** forwards the two flags to **124** `prod-audit` (place them **before** the subcommand, next to `--execute` / `--md-sa`).

## Operator commands

Full layer (typical):

```bash
cd THYROID_2026
.venv/bin/python scripts/138_md_specimen_fhir_layer.py --md
```

Diagnostics only (142):

```bash
.venv/bin/python scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md
```

Prod audit with auto-materialize:

```bash
.venv/bin/python scripts/124_md_live_release_audit.py --md --md-env prod --final-release \
  --materialize-specimen-fhir --tag 20260407
```

Workflow promote:

```bash
.venv/bin/python scripts/137_md_molecular_release_workflow.py --execute --md-sa promote --tag 20260407 \
  --materialize-specimen-fhir
```

Final master (MotherDuck):

```bash
.venv/bin/python scripts/126_final_master_release.py --md --md-sa --release-date 20260407 \
  --materialize-specimen-fhir \
  --hydrate-mrq-from studies/… --decisions-csv studies/…
```

## MotherDuck snapshot preflight (124)

**124** preflight tries snapshot metadata queries in order:

1. `md_information_schema.database_snapshots`
2. `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS`
3. `md_information_schema.snapshots`

Evidence is written to `preflight_db_list.json` and `snapshot_metadata.json` with a `source` field indicating which query succeeded.
