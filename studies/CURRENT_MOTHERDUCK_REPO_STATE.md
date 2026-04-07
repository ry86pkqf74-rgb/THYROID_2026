# THYROID_2026 — current MotherDuck vs repo state

> **Point-in-time snapshot — not auto-refreshed on every commit.** Regenerate after promotions or specimen/FHIR deploys: `python scripts/144_md_repo_current_state_summary.py --md`. If the **Commit SHA** below differs from `git rev-parse HEAD`, treat live sections as **historical** until regenerated.

**Machine-generated:** 2026-04-07T07:50:34.618080+00:00
**Commit SHA:** `6b74741b2f5e4402ee2c4a936e12e68250824927`

> Regenerate after promotion or specimen/FHIR deploy: `python scripts/144_md_repo_current_state_summary.py --md`

## Read scaling (reviewers)

For dashboards or ad hoc read-after-snapshot review, use **only** `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` with `MotherDuckClient.connect_read_scaling()` — never for writes. Set `MD_READ_SCALING_SESSION_HINT` (or per-connection `session_hint`) for stable duckling affinity; after a writer creates a named snapshot, readers should run `REFRESH DATABASE` / helpers in `utils/md_read_scaling_refresh.py` (see [`docs/motherduck_read_scaling_dashboard.md`](../docs/motherduck_read_scaling_dashboard.md)).

## Live MotherDuck status (`--md` runs only)

- **current_database():** `Thyroid 2026`
- **specimen_master_v1:** 10,139 rows
- **fhir_bundle_export:** 10,139 rows
- **specimen_genomic_assay_v1:** 10,126 rows
- **qa.release_manifest (latest 3):**
  - tag `20260407_final2` | sha `4ad9052` | 2026-04-07 05:11:41.171561
  - tag `20260407_final` | sha `4ad9052` | 2026-04-07 05:08:12.328508
  - tag `20260406` | sha `4b2d076` | 2026-04-07 04:07:52.519215
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
