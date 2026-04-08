# THYROID_2026 — current MotherDuck vs repo state

> **Not automation SSOT.** Prefer a fresh `119_md_formalization_validate.py --md --release-mode` output (e.g. under `studies/`) for release verdicts. This file reconciles **checked-in** repo artifacts with optional live introspection.

> **Stale guard:** If **`Commit SHA`** below ≠ `git rev-parse HEAD` on your machine, treat **Live MotherDuck** bullets as **historical** until you re-run this generator with `--md`.

> **Repo posture (sync with README):** Latest **live** `119 --release-mode` + specimen/FHIR truth baselines live under `studies/specimen_fhir_release_truth_*` (regenerate with this script + `119_md_formalization_validate.py --md --release-mode`). **Governance:** operator `119` may **PASS WITH WARN** while **manuscript** sign-off still requires **human-reviewed** MRQ/promotion paths (not automation-only verification). **Institutional non-Tg lab wave** (`final_institutional_20260407`) is **ingested**; residual lab gaps are **source-limited**, not a missing-wave blocker. Operator evidence pack: `studies/20260411_final_master_release/EVIDENCE_PACK.md`.

**Machine-generated:** 2026-04-08T06:54:46.657696+00:00
**Commit SHA:** `c6ef439583320aebbff811d1865e4b38a0075aa4`

> Regenerate after promotion or specimen/FHIR deploy: `python scripts/144_md_repo_current_state_summary.py --md`

## Read scaling (reviewers)

For dashboards or ad hoc read-after-snapshot review, use **only** `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` with `MotherDuckClient.connect_read_scaling()` — never for writes. Set `MD_READ_SCALING_SESSION_HINT` (or per-connection `session_hint`) for stable duckling affinity; after a writer creates a named snapshot, readers should run `REFRESH DATABASE` / helpers in `utils/md_read_scaling_refresh.py` (see [`docs/motherduck_read_scaling_dashboard.md`](../docs/motherduck_read_scaling_dashboard.md)).

## Live MotherDuck status (`--md` runs only)

- **current_database():** `Thyroid 2026`
- **specimen_master_v1:** 10,139 rows
- **specimen_tumor_focus_v1:** 11,103 rows
- **specimen_genomic_assay_v1:** 10,862 rows
- **fhir_bundle_specimen_export_v1:** 10,139 rows
- **qa.release_manifest (latest 3):**
  - tag `20260408r3` | sha `a593544` | 2026-04-08 05:20:40.752314
  - tag `20260408r2` | sha `a593544` | 2026-04-08 05:18:04.189662
  - tag `20260411` | sha `de13c33` | 2026-04-07 19:15:39.106720
- **qa.manual_review_queue (NULL verification_status):** 0

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

| user_agent | approx_queries |
|---|---:|


## Reviewer RO share (manual)

Restricted read-only shares are created in the MotherDuck UI/org console. Attach with org-issue token; document grant + manual refresh policy in your release ticket — do not commit tokens.
