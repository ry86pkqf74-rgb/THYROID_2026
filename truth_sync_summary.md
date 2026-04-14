# Truth sync summary — April 2026 (MotherDuck SSOT)

## Single headline (used verbatim across README, RELEASE_NOTES, `docs/REPO_STATUS.md`, `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` generator)

**Live MotherDuck `main` / `qa` are canonical; `119 --release-mode` can pass while governance (human-reviewed MRQ where policy requires) remains a separate concern.**

Full contract: [`docs/final_source_of_truth_contract.md`](docs/final_source_of_truth_contract.md). Analyst surfaces include **`main.master_fact_long_verified_v1`**, **`main.master_patient_rollup_verified_v1`**, **`main.master_source_lineage_v1`** (see `scripts/125_master_verified_views.py`).

## Three layers (do not collapse)

1. **Automation / validation** — Fresh `119 --release-mode` on live MotherDuck; cite a **timestamped** `validation_report.md` under `studies/`, not ad hoc row guesses.
2. **Governance / human review** — Manuscript sign-off may require named reviewers and substantive decisions; `auto_accepted_*` / queue structure alone are **not** row-level human validation.
3. **Source-limited enrichment backlog** — Operative NLP materialization, recurrence sparsity, RAI ceiling, residual non-Tg lab gaps. The **final institutional non-Tg lab wave** (`final_institutional_20260407`) is **ingested** — **not** “missing wave” (see `studies/20260411_final_master_release/EVIDENCE_PACK.md` — row counts may lag live prod).

## Stale / superseded pointers (preserved, not deleted)

- `exports/release_manifests/LATEST_MANIFEST.json` — may be historical; live SSOT is `qa.release_manifest` (see `exports/release_manifests/README.md`).
- `studies/20260409_final_master_release/EVIDENCE_PACK.md` — historical row snapshot; prefer **20260411** pack or live SQL for current counts.
- `studies/20260407_formalization_validation_release_mode/` — **20-check** era PASS; **not** current `119`.
- `docs/REPO_STATUS.md` — March 13 tables retained under **Historical snapshot**; cloud narrative is **above** them.

## Ops / verification

- Live MotherDuck dashboard: `python scripts/144_md_repo_current_state_summary.py --md` (token via `motherduck_client.get_token()` / `motherduck.local.toml` / env — do not log secrets).
- Optional: `python scripts/145_export_release_manifest_pointer.py --md` to refresh checked-in manifest pointer.
- Doc path integrity: `python scripts/check_doc_paths.py`.
