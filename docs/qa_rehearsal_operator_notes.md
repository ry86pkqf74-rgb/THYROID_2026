# QA MotherDuck release rehearsal — operator notes

## Preflight (tokens)

Resolution order matches `motherduck_client.get_token()`: `MD_SA_TOKEN` → `MOTHERDUCK_TOKEN` → `motherduck_token` (env) → JWT-like `LOCAL_DB_PATH` → `.streamlit/secrets.toml` same keys.

Log **SET/MISSING and length only**; never print token strings.

## Standard rehearsal environment variables

```bash
cd /path/to/THYROID_2026
export MOTHERDUCK_ENV=qa
export MOTHERDUCK_CUSTOM_USER_AGENT='THYROID_2026_release_rehearsal'
export MOTHERDUCK_SESSION_HINT='qa_release_rehearsal_YYYYMMDD'
```

Optional: `export MOTHERDUCK_DATABASE='...'` overrides catalog from `config/motherduck_environments.yml`.

## Narrow safe path (no MotherDuck writes)

Full orchestrator dry-run (structural `119` at end — **not** `--release-mode` unless you pass `--final-release` to `124`):

```bash
.venv/bin/python scripts/124_md_live_release_audit.py \
  --md --dry-run --md-env qa \
  --output-dir studies/YYYYMMDD_release_rehearsal_qa/md_live_audit_dryrun \
  --tag YYYYMMDD
```

Formalization only:

```bash
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env qa \
  --output-dir studies/YYYYMMDD_release_rehearsal_qa/validation_only
```

Strict publication gate (expect additional failures until QA matches prod lineage):

```bash
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env qa --release-mode \
  --output-dir studies/YYYYMMDD_release_rehearsal_qa/validation_release_mode
```

## Molecular lineage views

- Deploy: `.venv/bin/python scripts/132_molecular_fact_lineage_views.py --execute --md --md-env qa`
- Validate without DDL: `--validate-only` (now **SKIP**s cleanly if views are absent on the connected catalog).

## Specimen / FHIR

If `main.synoptic_tumor_long_v1` exists, align with **`138_md_specimen_fhir_layer.py`** / **`143_md_specimen_fhir_qa_diagnostics_deploy.py`** per `utils/specimen_fhir_release_gate.py` and `docs/specimen_fhir_release_integration.md`.

## Evidence layout

Store rehearsal summaries under `studies/<YYYYMMDD>_release_rehearsal_qa/` (report + CSV + optional full `md_live_audit_dryrun/` log bundle).

## 2026-04-07 rehearsal index

See `studies/20260407_release_rehearsal_qa/rehearsal_report.md` and `rehearsal_metrics.csv`.

## 2026-04-07 follow-up execution (next steps)

Executed on **QA** after the structural dry-run:

| Step | Command | Outcome |
|------|---------|--------|
| Molecular lineage deploy | `132_molecular_fact_lineage_views.py --execute --md --md-env qa` | **FAIL** — `main.molecular_results` missing on QA |
| Release-mode validation | `119_md_formalization_validate.py --md --md-env qa --release-mode` | **FAIL** — 5b governance (MRQ synthetic placeholders + `decision_batch_id`) + Check **12b** (`molecular_testing` spine missing) |

Evidence: `studies/20260407_release_rehearsal_qa/next_steps/` (`EXECUTION_SUMMARY.md`, logs, `119_release_mode/validation_report.md`).
