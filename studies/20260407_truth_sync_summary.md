# Truth-sync summary — 2026-04-07 (checkpoint)

> **Superseded narrative:** See repo-root [`truth_sync_summary.md`](../truth_sync_summary.md) for the **April 2026** single-headline reconciliation (**Technically passing but blocked by synthetic MRQ**) and the automation / governance / source-limited split.

- **Live SSOT (automation + catalog lineage):** [`studies/20260407_live_truth_and_lineage_contract_audit/`](20260407_live_truth_and_lineage_contract_audit/) — latest committed **27-check** `119` under `119_release_validation/`.
- **Governance / MRQ / lab memos:** [`studies/20260407_publication_signoff_live/`](20260407_publication_signoff_live/README.md) — dual-timestamp `119` notes (early BLOCKED vs later PASS+WARN).
- **README:** Single narrative — synthetic MRQ dominates; non-Tg lab wave missing; **`119 --release-mode`** evidence is **timestamped** (see lineage + signoff folders; do not assume a single PASS/FAIL forever).
- **Formalization folder:** [`studies/20260407_formalization_validation_release_mode/`](20260407_formalization_validation_release_mode/) — **04:47Z**, **20-check** **PASS** snapshot; **historical** vs current validator.
- **Signoff memo:** Supersession banner added; body retained.
- **Ops:** Makefile final-master + lab dry-run targets; CI `workflow_dispatch` job for `126 --dry-run` with required path inputs; `scripts/check_doc_paths.py` in static CI; MotherDuck contract expanded (tokens, `MD_INFORMATION_SCHEMA`, DuckLake).
