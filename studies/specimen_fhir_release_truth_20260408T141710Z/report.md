# Specimen / FHIR release truth — live MotherDuck rebaseline

**Generated (UTC):** 2026-04-08T14:17–14:18  
**Repo commit SHA:** `git log -1 --format=%H -- studies/specimen_fhir_release_truth_20260408T141710Z/report.md`

**Reconciliation bundle commit (this push, 40-char):** `b80ace799288ccfc166a7839134b4e36134843a7` *(tip-of-`main` after a follow-up doc commit may differ; prefer the* `git log` *line above for the blob you have checked out).*

**MotherDuck `current_database()`:** `Thyroid 2026`  
**Catalog type (preflight):** `DUCKLAKE` — native named `CREATE SNAPSHOT` semantics are not assumed; promotion/backups follow org runbook (`docs/motherduck_database_contract_v1.md`, `docs/specimen_fhir_contract_review.md`).

## Connection / attribution

- **fail_closed:** `connect_md_or_file(..., fail_closed=True)` (scripts 119, 144).
- **custom_user_agent:** `specimen_fhir_release_truth_v2` via `MOTHERDUCK_CUSTOM_USER_AGENT` (release-truth operator run; not read-scaling token).
- **Token:** RW credentials from gitignored repo-root `motherduck.local.toml` (pattern: `motherduck.local.toml.example`). Read-scaling token was **not** used for any write.

## Commands run (exact)

```bash
cd "/Users/ros/THyroid 2026"
export MOTHERDUCK_CUSTOM_USER_AGENT=specimen_fhir_release_truth_v2

# 144 — repo/live reconciliation markdown (read-only queries on MD)
/opt/homebrew/bin/python3 scripts/144_md_repo_current_state_summary.py --md

# 119 — formalization release gate (includes Check 13 specimen/FHIR)
/opt/homebrew/bin/python3 scripts/119_md_formalization_validate.py --md --release-mode \
  --output-dir studies/specimen_fhir_release_truth_20260408T141710Z/119_release_validation_prod
```

**Not run:** `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md`, `scripts/138_md_specimen_fhir_layer.py --md` — **119 Check 13 PASS** with QA diagnostics clean; no stale/missing views signal.

## Live row counts (specimen / FHIR core)

From `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` after this session’s `144` (`2026-04-08T14:16:55Z`):

| Object | Rows |
|--------|-----:|
| `main.specimen_master_v1` | 10,139 |
| `main.specimen_tumor_focus_v1` | 11,103 |
| `main.specimen_genomic_assay_v1` | 10,370 |
| `main.fhir_bundle_specimen_export_v1` | 10,139 |

**`qa.release_manifest` (latest):** tag `20260408r4`, sha `d9b9dc9`, timestamp `2026-04-08 08:56:49.086697`.

## Check 13 outcome (`119 --release-mode`)

| Item | Status |
|------|--------|
| Specimen/FHIR tables present | **PASS** (10 objects) |
| Master fingerprint uniqueness | **PASS** |
| `qa.val_specimen_contract_v1` | **PASS** (no FAIL rows) |
| `qa.val_specimen_genomic_binding_v1` | **PASS** (no FAIL rows) |
| QA diagnostics (142 + `t_diag_specimen_focus_qa_metrics_v1`) | **PASS** — clean |

Full machine report: [`119_release_validation_prod/validation_report.md`](119_release_validation_prod/validation_report.md).

## Overall `119` verdict

**PASS WITH WARNINGS** — **33 PASS / 6 WARN / 0 FAIL**.

Warnings include: local canonical parquet export parity not checked (operator machine), molecular assay/panel dictionary pairing (expected for some panels), and **specimen-adjacent review burden** (non-blocking for Check 13): `genomic_link_review` open/pending **10,155**; `specimen_merge_review` open/pending **1**.

## Remaining blockers (-release framing)

- **Automation:** No FAIL on release-mode gate; specimen/FHIR structural gate satisfied.
- **Governance / manuscript:** Human-reviewed MRQ and promotion posture remains a **policy** concern separate from Check 13; see [`studies/20260407_publication_signoff_live/final_verdict_memo.md`](../20260407_publication_signoff_live/final_verdict_memo.md).
- **Checked-in vs cloud:** `exports/release_manifests/LATEST_MANIFEST.json` remains a **point-in-time** Git artifact (March 2026-era); authoritative promotion history is **`qa.release_manifest`** on the live catalog.

## Artifact lineage (this folder)

| Path | Role |
|------|------|
| `report.md` | **Current** operator release-truth summary (this file) |
| `119_release_validation_prod/validation_report.md` | **Current** full `119 --release-mode` capture |

**Superseded** for “latest live specimen/FHIR + `119`” pointer: `studies/specimen_fhir_release_truth_20260408T122117Z/` (and earlier `065318Z`, `121042Z` folders listed in README).

## Repo docs reviewed (context)

- `AGENTS.md`, `README.md`, `docs/motherduck_database_contract_v1.md`, `docs/specimen_fhir_contract_review.md`
- `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` (regenerated)
- `studies/20260409_final_master_release/` — **historical** final-master evidence tree; operator truth for master evidence is **`studies/20260411_final_master_release/EVIDENCE_PACK.md`** (**current** per README)
