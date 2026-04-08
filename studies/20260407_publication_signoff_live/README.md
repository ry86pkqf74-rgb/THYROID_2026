# Live publication signoff audit — 2026-04-07

**Repo delta (2026-04-08):** Consolidated live-vs-checked-in reconciliation is in [`../20260407_repo_delta_gap_audit/`](../20260407_repo_delta_gap_audit/) (fresh `119 --release-mode`, MRQ/lab/stale-doc matrix). Prefer that folder when deciding what still blocks publication **today**.

**Supersession (2026-04-07, later UTC session):** The checked-in [`validation_report.md`](validation_report.md) in this folder is a **point-in-time snapshot** (generated `2026-04-07T10:33:51Z`) showing **BLOCKED** with **`broken_fhir_refs=10139`**. A **subsequent** live `119 --release-mode` run recorded **PASS WITH WARNINGS** (`broken_fhir_refs=0`; still WARN on genomic review burden). Use **[`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md)** and **[`../20260407_live_truth_and_lineage_contract_audit/live_truth_audit.md`](../20260407_live_truth_and_lineage_contract_audit/live_truth_audit.md)** as the **current** automation + catalog truth for catalog state **after** that rerun. Memos below (`mrq_*`, `lab_*`) may be **stale** vs MotherDuck — see the repo-delta study for histogram updates.

**Single folder for Prompt 1 outputs** (live MotherDuck, fail-closed).

| Artifact | Purpose |
|----------|---------|
| [`final_verdict_memo.md`](final_verdict_memo.md) | One-line verdict + footnotes |
| [`live_audit_memo.md`](live_audit_memo.md) | DB inventory, counts, `119` summary |
| [`mrq_reconciliation_memo.md`](mrq_reconciliation_memo.md) | Checked-in doc contradictions vs live |
| [`lab_coverage_memo.md`](lab_coverage_memo.md) | Waves + non-Tg gaps |
| [`validation_report.md`](validation_report.md) | **First** `119 --release-mode` snapshot (BLOCKED); **current** automation report: [`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md) |
| [`../20260407_live_truth_and_lineage_contract_audit/live_truth_audit.md`](../20260407_live_truth_and_lineage_contract_audit/live_truth_audit.md) | Live catalog + lineage verdict (same-day follow-on) |
| [`md_introspection_snapshot.md`](md_introspection_snapshot.md) | `MD_INFORMATION_SCHEMA` samples |
| [`commands_run.log`](commands_run.log) | Exact commands |

Triage export: `exports/review_queue_triage_20260407_103411/`.
