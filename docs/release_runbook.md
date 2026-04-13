# THYROID_2026 — Molecular release runbook (MotherDuck)

This document is the **production-safe molecular promotion path** for the `Thyroid 2026` MotherDuck catalog. It complements [`motherduck_release_runbook_v2.md`](motherduck_release_runbook_v2.md) (pipeline internals) and [`motherduck_sandbox_clone_runbook.md`](motherduck_sandbox_clone_runbook.md) (zero-copy sandboxes).

## Environment model

| Plane | MotherDuck database | Use |
|--------|---------------------|-----|
| **dev** | `Thyroid 2026 Molecular Dev …` (see [`config/motherduck_environments.yml`](../config/motherduck_environments.yml)) | Zero-copy clone; daily development. Refresh from prod when schema drifts. |
| **qa** | `Thyroid 2026 Molecular QA …` | Pre-prod validation; run **119 `--release-mode`** here before touching prod. |
| **prod** | `Thyroid 2026` | Canonical analyst surface; promotion + `release_*` schemas. |

Set `MOTHERDUCK_ENV=dev|qa|prod` (or `MOTHERDUCK_DATABASE`) so scripts attach the intended catalog. Staging loaders (**116**) use the default MotherDuck client environment (`MOTHERDUCK_ENV` / prod if unset) — **set `MOTHERDUCK_ENV=dev` in the shell when loading to the dev clone**.

## Prerequisites

- Read/write token: `MOTHERDUCK_TOKEN` or `MD_SA_TOKEN` (never use read-scaling-only tokens for promotion).
- Formal validation and live audit: **119**, **124** as documented in the v2 runbook.
- Sandbox bootstrap: **130** (`inspect`, `clone`, `refresh-dev`, `prepromote-backup`).

## 1. Development (zero-copy dev clone)

Refresh dev from current prod (DuckLake-safe: latest state, no snapshot selector):

```bash
cd THYROID_2026
.venv/bin/python scripts/130_md_env_bootstrap.py --execute refresh-dev --latest --md-sa
```

Work against dev:

```bash
export MOTHERDUCK_ENV=dev
.venv/bin/python scripts/116_md_stage_loader.py --md
# … other dev-local pipeline steps as needed
```

## 2. QA validation (clone or dedicated QA DB)

Ensure QA matches the candidate lineage (re-clone from prod or promote from dev per your policy), then run **strict** formalization validation on **qa**:

```bash
.venv/bin/python scripts/137_md_molecular_release_workflow.py qa-validate --tag 20260409
# or
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env qa --release-mode \
  --output-dir studies/20260409_molecular_qa_release_mode
```

Resolve **119** failures before prod promotion. Release-mode requires presentation views, molecular contract views, release schema/manifest expectations, and an empty pending slice of `qa.manual_review_queue` where applicable — see 119 docstring.

**Governance:** Release-mode 119 also rejects **synthetic** `verification_status` placeholders and requires **`decision_batch_id`** on non-empty `qa.promotion_review_decisions`. Rehearsal vs publication modes are documented in [`publication_governance_gate.md`](publication_governance_gate.md).

## 3. Before prod promotion (safety net + audits)

### 3.1 Named snapshot / rollback handle

**DuckLake (current prod):** MotherDuck may reject **named** `CREATE SNAPSHOT name OF "Thyroid 2026"`. Use:

1. **Automatic history** — query `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` for audit IDs and timestamps.
2. **Deterministic rollback clone** — `130 prepromote-backup` creates `Thyroid 2026 Molecular PrePromote <label>` as a **zero-copy** copy of prod at promotion time (reversible baseline).

```bash
# SQL only (review)
.venv/bin/python scripts/130_md_env_bootstrap.py prepromote-backup --label 20260409_1545

# Apply
.venv/bin/python scripts/130_md_env_bootstrap.py --execute prepromote-backup --label 20260409_1545 --md-sa
```

**Native storage (if prod is ever native):** named snapshots and `CREATE DATABASE … FROM … (SNAPSHOT_ID|SNAPSHOT_NAME|SNAPSHOT_TIME)` work — see **130** `snapshot` and `clone` subcommands and the sandbox runbook.

### 3.2 Writer snapshot (read-scaling / share freshness)

Read-scaling and share-backed dashboards consume **snapshot** state. Before large writes, optional **unnamed** `CREATE SNAPSHOT OF` on the writer pins visibility for readers (see **136** and [`motherduck_read_scaling_dashboard.md`](motherduck_read_scaling_dashboard.md)):

```bash
# Script 136 uses --prefer-sa (service-account token preference), not --md-sa
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod --prefer-sa
```

### 3.3 Formal validation + live release audit

After QA is green, re-confirm on the **target** you will promote (typically prod **after** backup):

- **Live release audit (full chain):** **124** with `--md-env prod` and `--final-release` for signed release.
- **Formal validation:** **119** `--release-mode` is already invoked at the end of **124**; for a standalone prod check, run 119 with `--md-env prod --release-mode`.
- **Specimen / FHIR (Check 13):** When `main.synoptic_tumor_long_v1` exists, **124** `--final-release` fails *before* presentation views unless the specimen/FHIR layer and `qa.v_diag_*` views are present — unless you pass **`--skip-specimen-fhir-gate`** or **`--materialize-specimen-fhir`** (runs **138** or **143**). **126** (default `--release-mode`) enforces the same gate **before** **`115 --final-master`** / **`118 --final-master`**; those steps still copy/export only the manuscript analytic lists in **115**/**118**, not `specimen_*` / `fhir_*`. See [`specimen_fhir_release_integration.md`](specimen_fhir_release_integration.md) and [`specimen_fhir_contract_review.md`](specimen_fhir_contract_review.md) §*Scope vs `115` / `118`*.
- **Snapshot preflight:** **124** records MotherDuck snapshots using `md_information_schema.database_snapshots` when available, with fallbacks documented in that integration note.

## 4. Deterministic promotion strategies

### A. Clone / swap (full-catalog rollback)

1. `130 prepromote-backup --label <id>` → rollback database `Thyroid 2026 Molecular PrePromote <id>`.
2. Run **124** (or manual promotion SQL from **112**) against **prod**.
3. If prod must revert: coordinate with MotherDuck ops to **swap primary catalog** to the PrePromote clone or recreate `Thyroid 2026` from that clone (policy-specific; capture exact DDL in the change ticket).

### B. Schema-scoped publish + snapshot fallback

1. Same **prepromote-backup** (catalog-level safety net).
2. **115** `release_YYYYMMDD` schemas + **qa.release_manifest** provide **immutable, schema-scoped** publication inside prod (see v2 runbook section 6).
3. Rollback for analyst consumption: **point consumers at the previous `release_*` schema** documented in `qa.release_manifest`, or restore catalog from PrePromote clone if `main` corruption requires it.

## 5. Share-backed dashboard freshness

After **prod** mutation (especially **115** / **124** completing):

1. **Writer:** `CREATE SNAPSHOT OF "Thyroid 2026"` (unnamed or named if supported) — **136** `writer`.
2. **Readers:** `REFRESH DATABASE` (or `REFRESH DATABASES`) with the **read-scaling** token — **136** `reader`.

```bash
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod
MD_READ_SCALING_TOKEN=… .venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod
```

Streamlit / `dashboard.py` paths are documented in [`motherduck_read_scaling_dashboard.md`](motherduck_read_scaling_dashboard.md).

### 5.1 Specimen / FHIR reviewer NDJSON export (optional)

After **138** / **143** have populated `main.fhir_*_v1` and **`main.fhir_bundle_specimen_export_v1`** on the target catalog, operators can export analytic (de-identified) bundle collections for external reviewers:

```bash
# RW token — fail-closed attach; see motherduck_client.get_token()
.venv/bin/python scripts/141_fhir_specimen_json_export.py --md --output-root exports
```

Reviewers with **only** `MD_READ_SCALING_TOKEN` should run **`136` `reader`** (or `REFRESH DATABASE`) first, then:

```bash
.venv/bin/python scripts/141_fhir_specimen_json_export.py --read-scaling --output-root exports
```

Artifacts land in **gitignored** `exports/fhir_specimen_<UTC>/` (`specimen_bundles.ndjson`, `manifest.json`, `README.md`). Record row counts and git SHA in a dated `studies/` note if you need repository provenance without committing large NDJSON. Full reviewer contract: [`specimen_fhir_contract_review.md`](specimen_fhir_contract_review.md).

## 6. Orchestrator (137)

**`scripts/137_md_molecular_release_workflow.py`** chains: backup → optional named snapshot attempt → writer snapshot → **119** (qa, release-mode) → **124** (prod) → reader refresh.

**Rehearsal (no MotherDuck writes in 124/136):**

```bash
.venv/bin/python scripts/137_md_molecular_release_workflow.py promote --tag 20260409
```

**Production promotion (mutating backup + snapshots + full 124):**

Global flags such as ``--execute`` and ``--md-sa`` must appear **before** the ``promote`` subcommand:

```bash
.venv/bin/python scripts/137_md_molecular_release_workflow.py --execute promote --tag 20260409 --md-sa
```

Individual steps:

```bash
.venv/bin/python scripts/137_md_molecular_release_workflow.py backup-prod --label rel_20260409
.venv/bin/python scripts/137_md_molecular_release_workflow.py --execute backup-prod --label rel_20260409 --md-sa
.venv/bin/python scripts/137_md_molecular_release_workflow.py try-named-snapshot --snapshot-name pre_promote_20260409
.venv/bin/python scripts/137_md_molecular_release_workflow.py writer-snapshot --md-sa
.venv/bin/python scripts/137_md_molecular_release_workflow.py prod-audit --tag 20260409
.venv/bin/python scripts/137_md_molecular_release_workflow.py refresh-readers
```

Manifest output: `studies/<tag>_molecular_release_workflow/workflow_manifest.json`.

## 7. Rollback procedure

1. **Stop** further writes to prod (`Thyroid 2026`).
2. **Identify** the rollback artifact:
   - **PrePromote clone:** `Thyroid 2026 Molecular PrePromote <label>` from section 3.1, or
   - **Snapshot ID / time:** rows in `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` (native sources support `SNAPSHOT_ID` / `SNAPSHOT_TIME` on `CREATE DATABASE … FROM` — see **130**), or
   - **Schema-scoped:** previous `release_YYYYMMDD` + manifest row in `qa.release_manifest`.
3. **Restore path (policy):**
   - *Catalog swap:* repoint applications to the PrePromote database temporarily, or `DROP` + `CREATE DATABASE "Thyroid 2026" FROM` the backup clone (coordinate with MotherDuck — destructive).
   - *Analyst-only:* downgrade consumer queries to the prior `release_*` schema documented at promotion time.
4. **Readers:** run **136** `reader` after the writer snapshot reflects the restored state.
5. **Record** remediation in a dated folder under `studies/` (incident + validation re-run).

## 8. Reference matrix

| Step | Script | Notes |
|------|--------|--------|
| Stage load | **116** | Set `MOTHERDUCK_ENV=dev` for dev clone |
| Promotion gate + SQL | **112** | Invoked inside **124** |
| Contract / molecular views | **117**, **132** | In **124** |
| Release snapshot | **115** | `release_*` schema |
| Formal validate | **119** | `--release-mode` for sign-off |
| Live audit | **124** | `--md-env prod --final-release`; optional `--materialize-specimen-fhir` |
| Specimen/FHIR materialize | **138**, **143** | See [`specimen_fhir_release_integration.md`](specimen_fhir_release_integration.md) |
| Specimen/FHIR NDJSON export | **141** | `--md` (RW) or `--read-scaling` after refresh; see [`specimen_fhir_contract_review.md`](specimen_fhir_contract_review.md) |
| Reader freshness | **136** | writer + reader |
| Sandbox / backup DDL | **130** | `prepromote-backup`, `clone`, `snapshot` |
| Workflow orchestration | **137** | `promote` and single-step wrappers; forwards specimen/FHIR flags to **124** |

---

**See also:** [`motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md) (tokens, catalogs), CI jobs in `.github/workflows/ci.yml` (116 / 112 / 119 / 124 patterns).
