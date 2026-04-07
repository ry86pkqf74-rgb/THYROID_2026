# Truth sync summary — April 2026 (post–live MotherDuck rerun)

## Single headline (used verbatim across README, RELEASE_NOTES, `docs/REPO_STATUS.md`, `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` generator)

**Technically passing but blocked by synthetic MRQ.**

## Three layers (do not collapse)

1. **Automation / validation** — Latest committed `119 --release-mode`: **PASS WITH WARN** (`studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`). Earlier same-day snapshot can show **BLOCKED** on specimen/FHIR — always cite the **timestamped** artifact.
2. **Governance / human review** — Manuscript sign-off **not** complete until MRQ + promotion reflect **human-reviewed** paths; automation-only / historical synthetic-fill postures **do not** count.
3. **Source-limited enrichment backlog** — Operative NLP materialization, recurrence sparsity, RAI ceiling, **residual** non-Tg lab gaps. The **final institutional non-Tg lab wave** (`final_institutional_20260407`) is **ingested** — **not** “missing wave” (see `studies/20260411_final_master_release/EVIDENCE_PACK.md`).

## Stale / superseded pointers (preserved, not deleted)

- `studies/20260409_final_master_release/EVIDENCE_PACK.md` — historical row snapshot; prefer **20260411** pack for current MotherDuck counts.
- `studies/20260407_formalization_validation_release_mode/` — **20-check** era PASS; **not** current `119`.
- `docs/REPO_STATUS.md` — March 13 tables retained under **Historical snapshot**; April MotherDuck narrative is **above** them.

## Ops / verification

- Live MotherDuck dashboard: `python scripts/144_md_repo_current_state_summary.py --md` (token via `.streamlit/secrets.toml` / env).
- Doc path integrity: `python scripts/check_doc_paths.py`.
