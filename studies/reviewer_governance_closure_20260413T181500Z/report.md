# Reviewer / governance closure — 2026-04-13

## Git revision

- **Closure commit:** `4dcf67b90df72e6659e6a28f65e0234e008a6daa` (short: `4dcf67b`).

## Scope (files touched)

- `utils/md_connect.py` — `connect_read_scaling_fail_closed()` for least-privilege MotherDuck readers.
- `scripts/120_review_queue_triage.py` — `--read-scaling`, `--md-env`, `--session-hint`.
- `scripts/151_specimen_genomic_review_queue_export.py` — same flags as 120.
- `docs/motherduck_database_contract_v1.md` — default specimen/FHIR writer UA = `specimen_fhir_ref_integrity_v2`; legacy UA names called out.
- `docs/specimen_fhir_contract_review.md` — CI workflow split + read-scaling examples for 120/151.
- `docs/review_queue_triage_export.md` — read-scaling command examples.
- `docs/publication_governance_gate.md` — explicit external blockers (`true_human_reviewed`, specimen–genomic burden / waiver).
- `.github/workflows/ci.yml` — `tests/test_151_specimen_genomic_review_export.py` added to `llm-extraction-gold`.
- `tests/test_120_review_queue_triage.py` — read-scaling fail-closed + CLI monkeypatch tests.
- `tests/test_151_specimen_genomic_review_export.py` — same.

## Read-scaling reviewer ops — execution vs blocked

**Blocked in this workspace:** `get_read_scaling_token()` resolved **MISSING** (no `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` in env or readable gitignored TOML in CI context). Read/write token **was** present; read-scaling token **was not**. No fake runs.

**Would have run (in order), after org issues a read-scaling credential:**

1. `.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod --prefer-sa`
2. `.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod`
3. `.venv/bin/python scripts/141_fhir_specimen_json_export.py --read-scaling --output-root exports`
4. `.venv/bin/python scripts/120_review_queue_triage.py --read-scaling --output-root exports`
5. `.venv/bin/python scripts/151_specimen_genomic_review_queue_export.py --read-scaling --output-root exports`

**Missing / required for the least-privilege path**

- `MD_READ_SCALING_TOKEN` (alias `MOTHERDUCK_READ_SCALING_TOKEN`) — **required** for `--read-scaling` on 120/151/141.
- Optional: `MD_READ_SCALING_SESSION_HINT` / `MOTHERDUCK_READ_SCALING_SESSION_HINT`, `MOTHERDUCK_SESSION_HINT`, `MOTHERDUCK_DATABASE` / `MOTHERDUCK_DB` if catalog name differs.
- RW path remains `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN` / `motherduck.local.toml` per `motherduck_client.get_token()` — **not** interchangeable with read-scaling.

**Fallback when read-scaling token is unavailable**

- A **restricted read-only share** attach with reviewer-scoped identity is an acceptable org pattern (see `docs/specimen_fhir_contract_review.md` “Read scaling for reviewers” and `motherduck_client.connect_ro_share`). That path still requires MotherDuck policy support for the share ACL; it is **not** a substitute for committing secrets into the repo.

## Live MotherDuck mutation

- **None** for this task: no `UPDATE`/`INSERT` against production `qa.*` for reviewer hydration; no replay of structural scripts **138/143**.

## Real reviewer decision hydration

- **Not performed.** The repository contains many historical `studies/**/manual_review_queue.csv` artifacts from prior gates and rehearsals; **additive import into MotherDuck** is an explicit operator action (`114` / `126`) and requires **RW** credentials plus org authorization (see `AGENTS.md`).
- **Before/after MRQ counts (live `qa.manual_review_queue`):** not queried in this run.
- **Before/after specimen–genomic review burden:** not queried in this run.
- **Governance documentation** was updated in `docs/publication_governance_gate.md` to state the external blocker explicitly (non-empty `true_human_reviewed` or waiver; specimen–genomic open-queue burn-down or waiver).

## Optional live state refresh (`144`)

- **Skipped:** no successful MotherDuck-affecting step in this task, so `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` was **not** refreshed (remains point-in-time per operator policy).

## Verification commands (executed locally)

```bash
.venv/bin/python -m py_compile scripts/120_review_queue_triage.py scripts/151_specimen_genomic_review_queue_export.py utils/md_connect.py
.venv/bin/ruff check scripts/120_review_queue_triage.py scripts/151_specimen_genomic_review_queue_export.py utils/md_connect.py tests/test_120_review_queue_triage.py tests/test_151_specimen_genomic_review_export.py --select F
.venv/bin/mypy utils/md_connect.py
.venv/bin/python -m pytest \
  tests/test_120_review_queue_triage.py \
  tests/test_151_specimen_genomic_review_export.py \
  tests/test_specimen_identity_layer.py \
  tests/test_specimen_fhir_layer.py \
  tests/test_specimen_fhir_qa_diagnostics.py \
  tests/test_specimen_genomics_binding.py \
  tests/test_specimen_fhir_scripts_offline.py \
  -v --tb=short
```

All listed tests **passed** (51 items).

## README.md

- **No change:** top-level status text unchanged.
