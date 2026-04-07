# THYROID_2026 — MotherDuck staging operator runbook

Use this runbook to repeat **v2 parquet → `v2_stage`** loads and **staging-only** canonical fact materialization without promoting to `main`.

## Prerequisites

- Repo root: `THYROID_2026/` with `.venv` and local parquets under `processed/output/v2_parquets/`.
- **Read/write** MotherDuck token available via **`MOTHERDUCK_TOKEN`**, **`MD_SA_TOKEN`**, or **`.streamlit/secrets.toml`** (see `motherduck_client.get_token()`). Never print token values; logs should only show SET/MISSING/length for env preflight.
- Choose environment explicitly: **`MOTHERDUCK_ENV=dev|qa|prod`**. Default in code is `prod` if unset — for sandbox work, **always set `dev` or `qa`**.
- Optional attribution (recommended):  
  `MOTHERDUCK_SESSION_HINT=THYROID_2026`  
  `MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_molecular/<script>;kind=<ingest|materialize|validate>`

## Schema isolation

- **Staging plane:** `v2_stage` — all tables from `116` and, when using `--md-schema v2_stage`, canonical outputs from `103`.
- **Canonical plane:** `main` — **not** written by this runbook path.
- **Config:** `config/motherduck_environments.yml` maps dev/qa/prod to **different** MotherDuck database names; do not rely on cross-environment attachments.

## Step 1 — Load domain parquets into `v2_stage`

```bash
cd /path/to/THYROID_2026
export MOTHERDUCK_ENV=dev
export MOTHERDUCK_SESSION_HINT=THYROID_2026
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular/116_stage_loader;kind=ingest"

.venv/bin/python scripts/116_md_stage_loader.py --md
```

Dry-run (no writes):

```bash
.venv/bin/python scripts/116_md_stage_loader.py --md --dry-run
```

## Step 2 — Materialize canonical facts to **staging** (not `main`)

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular/103_fact_lineage;kind=materialize"
.venv/bin/python scripts/103_fact_lineage_materialize.py --md --md-schema v2_stage
```

- Omit `--md-schema` or pass `--md-schema main` only when intentionally writing promoted `main` tables.
- Script always writes local parquets under `processed/`; MotherDuck writes follow `--md-schema`.

## Step 3 — QC export

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular/142_staging_qc;kind=validate"
.venv/bin/python scripts/142_md_staging_qc.py --md
```

Output: `reports/motherduck_stage_counts.csv`.

## What not to do in this path

- Do **not** run `motherduck_promote.sql` or `112_v2_domain_promotion_gate.py` unless executing a **promotion** task with all gates green.
- Do **not** use read-scaling-only tokens (`MD_READ_SCALING_TOKEN`) for loaders or `103 --md`.
- Do **not** set `MOTHERDUCK_DATABASE` unless you intend a deliberate cross-catalog override (document the reason).

## Multimodal (128 / 129 / 130)

These scripts assume **canonical / episode** data on **`main`** and are part of the **release** path, not the **v2_stage-only** staging path. Run them only when the task explicitly includes multimodal deployment after `main` prerequisites exist.

## Related docs

- `docs/motherduck_database_contract_v1.md` — schema contract.  
- `docs/motherduck_v2_staging_runbook.md` — broader staging narrative.  
- `reports/motherduck_stage_report.md` — latest staged run summary (when generated).
