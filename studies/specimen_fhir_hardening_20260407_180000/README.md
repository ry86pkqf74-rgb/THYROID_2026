# Specimen + FHIR hardening (2026-04-07)

Machine-generated audit memos and telemetry from `scripts/138_md_specimen_fhir_layer.py` land here when run with `--md` (or a custom `--study-dir`).

Checked-in artifacts in this folder:

- **README.md** (this file) — how to run
- **implementation_report.md** — template filled by the script with commit SHA and policy summary

After a MotherDuck run you should also have:

- `audit_memo.md` — README vs sign-off reconciliation boilerplate + validation rows
- `query_history_telemetry.md` — `QUERY_HISTORY` snippet for `custom_user_agent=specimen_fhir_hardening_v1` (if permitted)

## Run (RW token required)

```bash
cd THYROID_2026
export MOTHERDUCK_TOKEN=...   # or MD_SA_TOKEN
# optional: export MOTHERDUCK_DATABASE="Thyroid 2026"
.venv/bin/python scripts/138_md_specimen_fhir_layer.py --md
```

Dry plan only:

```bash
.venv/bin/python scripts/138_md_specimen_fhir_layer.py --md --dry-run
```

## Scratch / reviewer attach

Create a zero-copy clone from latest (or a named snapshot when not on DuckLake) per `docs/motherduck_sandbox_clone_runbook.md`, then share read-only with reviewers. Do not attach PHI or raw note text.
