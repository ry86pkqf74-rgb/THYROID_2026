# Operator summary — cursor prod release (2026-04-07)

## Release outcome

- **MotherDuck mutations:** The live `124` pipeline run for tag **`20260410`** completed **stage → gate → promote → canonical → qa → contract → lineage → presentation → release snapshot `release_20260410` → parquet bundle**. Initial **`137 promote --execute`** manifest recorded **`prod-audit` return code 1** because **119** failed on **Review queue** (duplicate MRQ batch with NULL `verification_status`, `run_label = promotion_gate`).
- **Remediation:** Implemented **MRQ verification backfill** in `scripts/114_qa_schema_setup.py` and executed it on prod for `promotion_gate`. **Re-run:** `119 --md --md-env prod --release-mode` → **PASS WITH WARN** (26 PASS / 1 WARN / 0 FAIL) — see `postfix_119_prod_release_mode/validation_report.md`. WARN: specimen-adjacent review burden (genomic link queue / merge queue) per existing governance.
- **Verdict:** Treat as **operator-complete** for automation: prod has **`release_20260410`**, manifest updated, MRQ counts consistent; **manuscript / analyst sign-off** remains subject to README governance (synthetic MRQ posture, lab waves, etc.).

## CLI correction

- **`137`:** Global flags must precede the subcommand, e.g.  
  `.venv/bin/python scripts/137_md_molecular_release_workflow.py --execute promote --tag YYYYMMDD [--md-sa]`  
  (documented in `docs/release_runbook.md`).

## Preflight facts

- **RW token mode:** `secrets.toml:MOTHERDUCK_TOKEN` (no `MD_SA_TOKEN` in use for this session).
- **Read-scaling token:** **none** (`read_scaling_token_mode=none`).
- **Prod type:** **DUCKLAKE**; unnamed automatic snapshots listed in `DATABASE_SNAPSHOTS`; **no** named snapshot for rollback.

## Tagging note

- **`124` dry-run** used **`--tag 20260407`**; `release_20260407` already existed. **`115`** was adjusted so **dry-run** skips with a warning when the schema exists (rehearsal only). **Live promotion** used **`--tag 20260410`** for a new immutable `release_*` slice.

## Rollback handles

See **`rollback.md`** (PrePromote DB names, `release_20260410` / prior `release_*`, DuckLake limits).

## Read-scaling verification

- **Writer snapshot:** `CREATE SNAPSHOT OF "Thyroid 2026"` re-run after MRQ fix logged in `post_writer_snapshot.log`.
- **Reader refresh / query-history on read-scaling ducklings:** **Not run** — no `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` in environment or secrets resolution path used here (`136 reader` had exited with **120** during `137` for the same reason).

## Artifacts

| Path | Purpose |
|------|---------|
| `qa_release_mode/` | Pre-prod **119** on QA |
| `preflight_dryrun/` | **124** prod dry-run after **115** fix |
| `step3_prepromote_backup.log` | PrePromote clone `20260407_cursor_prod_release` |
| `step5_promote_20260410_execute.log` | Full **137** stdout (includes failed-then-fixed **119**) |
| `workflow_manifest.json` | Step return codes (`overall_returncode` 120 = reader step) |
| `postfix_119_prod_release_mode/` | Post-fix **119** prod release-mode |
| `../20260410_motherduck_live_release_audit/` | Full **124** deliverables for tag **20260410** |

## Remaining blockers (governance)

- **Genomic / specimen review burden** WARN in **119** (open queues).
- **README** publication posture: synthetic automation vs manuscript-reviewed adjudication; non-Tg lab wave; Zenodo/GitHub drift — unchanged by this release slice.
