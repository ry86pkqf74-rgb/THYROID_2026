# THYROID_2026 — current MotherDuck vs repo state

> **Canonical contract:** [`docs/final_source_of_truth_contract.md`](../docs/final_source_of_truth_contract.md) defines live SSOT (`main` / `qa` on MotherDuck), analyst surfaces, and what is historical only.

> **Naming:** This file is the default **output path** for this script. It is **not** guaranteed fresh unless **`Commit SHA`** matches `git rev-parse HEAD` **and** you trust the timestamp.

> **Live catalog:** MotherDuck **`main`** (analytics) and **`qa`** (governance) — not local `thyroid_master.duckdb` unless you explicitly reconcile.

> **Publication narratives:** Signoff context and superseding validation pointers: [`20260407_publication_signoff_live/README.md`](20260407_publication_signoff_live/README.md).

> **Not automation SSOT.** Prefer a fresh `119_md_formalization_validate.py --md --release-mode` output (e.g. under `studies/`) for release verdicts. This file reconciles **checked-in** repo artifacts with optional live introspection.

> **Stale guard:** If **`Commit SHA`** below ≠ `git rev-parse HEAD`, treat **Live MotherDuck** bullets as **point-in-time** until you re-run: `.venv/bin/python scripts/144_md_repo_current_state_summary.py --md` (RW token via `motherduck_client.get_token()` / `motherduck.local.toml` or env — do not log secrets).

> **Repo posture (sync with README / `truth_sync_summary.md`):** Technical validation (`119 --release-mode`) can be green while **governance** (human-reviewed MRQ / promotion where policy requires) remains a separate concern — do not conflate them. Specimen/FHIR baselines: `studies/specimen_fhir_release_truth_*`. **Institutional non-Tg lab wave** (`final_institutional_20260407`) is **ingested**; residual lab gaps are **source-limited**. Evidence pack (may lag live row counts): `studies/20260411_final_master_release/EVIDENCE_PACK.md`.

**Machine-generated:** 2026-04-14T02:23:05.739568+00:00
**Commit SHA:** `ac8642e833c7b24327fcef46c338af8e0b88a9d9`

> Regenerate after promotion or specimen/FHIR deploy: `python scripts/144_md_repo_current_state_summary.py --md`

## Read scaling (reviewers)

For dashboards or ad hoc read-after-snapshot review, use **only** `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` with `MotherDuckClient.connect_read_scaling()` — never for writes. Set `MD_READ_SCALING_SESSION_HINT` (or per-connection `session_hint`) for stable duckling affinity; after a writer creates a named snapshot, readers should run `REFRESH DATABASE` / helpers in `utils/md_read_scaling_refresh.py` (see [`docs/motherduck_read_scaling_dashboard.md`](../docs/motherduck_read_scaling_dashboard.md)).

## Live MotherDuck status (`--md` runs only)

### Catalog probe (read-only)
- **current_database():** `Thyroid 2026`
- **md_information_schema.databases.type:** `DUCKLAKE`
- **Named CREATE SNAPSHOT (policy):** DUCKLAKE — do not assume native named snapshot semantics; use dev/qa/prepromote-backup runbook patterns.
### Core analytic surfaces (row counts)
- **canonical_extracted_fact_long_v2:** 55,500 rows
- **canonical_fact_quarantine_v2:** 199 rows
- **master_fact_long_verified_v1:** 55,500 rows
- **master_patient_rollup_verified_v1:** 5,141 rows
- **master_source_lineage_v1:** 55,500 rows
- **longitudinal_lab_canonical_v1:** 77,960 rows
- **longitudinal_lab_deduped_v:** 56,198 rows
### Specimen / FHIR layer row counts
- **specimen_master_v1:** 10,139 rows
- **specimen_tumor_focus_v1:** 11,103 rows
- **specimen_genomic_assay_v1:** 10,370 rows
- **fhir_bundle_specimen_export_v1:** 10,139 rows
- **qa.release_manifest (latest tag; ordering aligned with script 125):** `20260411` | sha `de13c33` | 2026-04-07 19:15:39.106720
- **qa.release_manifest (latest 3 by created_at):**
  - tag `20260408r4` | sha `d9b9dc9` | 2026-04-08 08:56:49.086697
  - tag `20260408r3` | sha `a593544` | 2026-04-08 05:20:40.752314
  - tag `20260408r2` | sha `a593544` | 2026-04-08 05:18:04.189662
- **qa.manual_review_queue (total rows):** 5,622
- **qa.manual_review_queue (pending NULL verification_status):** 0

## Checked-in release manifest (exports/)

- **manifest_id:** `release_8c18892_20260315_170027`
- **overall_status:** RELEASE_READY
- **git_sha (at generation):** `8c18892`
- **generated_at (checked-in):** `2026-03-15T17:00:27.540527` — file mtime ~0d old
- **role:** historical_checkpoint
- **WARNING (historical vs live):**
  - checked-in `git_sha` (`8c18892`) ≠ live latest manifest sha (`de13c33`) — **treat checked-in JSON as historical**; live SSOT is `qa.release_manifest`.
  - live tag `20260411` may not match checked-in manifest_id era (`release_8c18892_20260315_170027`) — see `exports/release_manifests/README.md`.

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
