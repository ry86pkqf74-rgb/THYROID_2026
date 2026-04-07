# THYROID_2026 — current MotherDuck vs repo state

> **Not automation SSOT.** Prefer a fresh `119_md_formalization_validate.py --md --release-mode` output (e.g. under `studies/`) for release verdicts. This file reconciles **checked-in** repo artifacts with optional live introspection.

> **Stale guard:** If **`Commit SHA`** below ≠ `git rev-parse HEAD` on your machine, treat **Live MotherDuck** bullets as **historical** until you re-run this generator with `--md`.

**Machine-generated:** 2026-04-07T18:26:10.637479+00:00
**Commit SHA:** `01485882f352947969ff0a8b04f38c85a73abab7`

> Regenerate after promotion or specimen/FHIR deploy: `python scripts/144_md_repo_current_state_summary.py --md`

## Read scaling (reviewers)

For dashboards or ad hoc read-after-snapshot review, use **only** `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` with `MotherDuckClient.connect_read_scaling()` — never for writes. Set `MD_READ_SCALING_SESSION_HINT` (or per-connection `session_hint`) for stable duckling affinity; after a writer creates a named snapshot, readers should run `REFRESH DATABASE` / helpers in `utils/md_read_scaling_refresh.py` (see [`docs/motherduck_read_scaling_dashboard.md`](../docs/motherduck_read_scaling_dashboard.md)).

## Live MotherDuck status (`--md` runs only)

- _(This run did **not** pass `--md` — no live MotherDuck session.)_
- _(Any **previously committed** row counts in this file are not refreshed here; operators must not treat them as live.)_


## Checked-in release manifest (exports/)

- **manifest_id:** `release_8c18892_20260315_170027`
- **overall_status:** RELEASE_READY
- **git_sha (at generation):** `8c18892`

## Stale checked-in validation artifacts

_Validation reports under `studies/` older than **14** days (by local mtime — regenerate with `119_md_formalization_validate.py --md`):_

- _(none matched staleness rule or no reports found)_

## Specimen / FHIR QA

- Diagnostic views: `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`
- Deploy-only: `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md`
- Orchestrated build: `scripts/138_md_specimen_fhir_layer.py --md`

## Query-history telemetry (MotherDuck)

(run with `--md` to populate)

## Reviewer RO share (manual)

Restricted read-only shares are created in the MotherDuck UI/org console. Attach with org-issue token; document grant + manual refresh policy in your release ticket — do not commit tokens.
