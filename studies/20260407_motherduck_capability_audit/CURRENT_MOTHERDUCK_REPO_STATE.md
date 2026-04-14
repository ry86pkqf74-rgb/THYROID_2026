# THYROID_2026 — current MotherDuck vs repo state

> **Superseded copy:** This path is a **frozen study export**. For the default generator output, use [`../CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md) and [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md).

> **Not automation SSOT.** Prefer a fresh `119_md_formalization_validate.py --md --release-mode` output (e.g. under `studies/`) for release verdicts. This file reconciles **checked-in** repo artifacts with optional live introspection.

> **Stale guard:** If **`Commit SHA`** below ≠ `git rev-parse HEAD` on your machine, treat **Live MotherDuck** bullets as **historical** until you re-run this generator with `--md`.

> **April 2026 repo posture (human-maintained, sync with README / `docs/REPO_STATUS.md`):** **Technically passing but blocked by synthetic MRQ** — i.e. latest committed `119 --release-mode` is **PASS WITH WARN** (see `studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`), while **manuscript** sign-off is not complete until MRQ/promotion reflects **human-reviewed** governance (not automation-only / `--synthetic-fill-mrq-verification` posture). The **final institutional non-Tg lab wave** (`final_institutional_20260407`) is **ingested**; residual lab issues are **source-limited enrichment**, not a missing-wave blocker. Operator snapshot: `studies/20260411_final_master_release/EVIDENCE_PACK.md`.

**Machine-generated:** 2026-04-08T02:48:13.790278+00:00
**Commit SHA:** `70b76a24322368683ced22953a6334b5bf9152e4`

> Regenerate after promotion or specimen/FHIR deploy: `python scripts/144_md_repo_current_state_summary.py --md`

## Read scaling (reviewers)

For dashboards or ad hoc read-after-snapshot review, use **only** `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` with `MotherDuckClient.connect_read_scaling()` — never for writes. Set `MD_READ_SCALING_SESSION_HINT` (or per-connection `session_hint`) for stable duckling affinity; after a writer creates a named snapshot, readers should run `REFRESH DATABASE` / helpers in `utils/md_read_scaling_refresh.py` (see [`docs/motherduck_read_scaling_dashboard.md`](../docs/motherduck_read_scaling_dashboard.md)).

## Live MotherDuck status (`--md` runs only)

- **current_database():** `Thyroid 2026`
- **specimen_master_v1:** 10,139 rows
- **fhir_bundle_export:** 10,139 rows
- **specimen_genomic_assay_v1:** 10,862 rows
- **qa.release_manifest (latest 3):**
  - tag `20260411` | sha `de13c33` | 2026-04-07 19:15:39.106720
  - tag `20260410` | sha `618086b` | 2026-04-07 16:22:53.465299
  - tag `20260407_tier` | sha `7793059` | 2026-04-07 15:25:17.363482
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
