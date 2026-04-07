# Artifact reconciliation — status documents vs reality

This section compares **checked-in** README/signoff/validation artifacts and **live** MotherDuck evidence from the audit run (`motherduck_audit_evidence.md`). Labels follow the user request: **current**, **stale**, or **superseded-without-cleanup**.

## Summary table

| Artifact | Role | Assessment | Notes |
|----------|------|------------|-------|
| [`README.md`](../../README.md) | Repo entry/status narrative | **Current** (with nuance) | Describes 2026-04-06/07 formalization; points to **live truth audit** for latest committed `119` and **formalization_validation_release_mode** only as 20-check **history**. March-13 local-freeze paragraphs are **context**, not MotherDuck SSOT. |
| [`studies/20260407_signoff_memo/signoff_memo.md`](../20260407_signoff_memo/signoff_memo.md) | Architecture sign-off | **Superseded-without-cleanup** for MotherDuck operational truth | Dated 2026-04-07; verdict “NOT READY” and blockers (e.g. v2_stage missing, load_inventory missing) **conflict** with the **same-dated** release-mode validation report and with **live** MotherDuck (v2 parity + specimen tables populated). **Retain** as historical audit of a point-in-time review; do not treat as current go/no-go without re-validation. |
| [`studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md) | Latest committed `119 --release-mode` (27 checks) | **Current** as *checked-in snapshot* for automation bar | PASS WITH WARNINGS; specimen/FHIR diagnostics **WARN** only (`broken_fhir_refs=0` in this run). |
| [`studies/20260407_formalization_validation_release_mode/validation_report.md`](../20260407_formalization_validation_release_mode/validation_report.md) | Early formalization PASS | **Historical** | Timestamp **2026-04-07T04:47:39Z**; **20 checks** only; MRQ count **16,866** — predates full 27-check validator + later MRQ hydrates. |
| [`docs/motherduck_database_contract_v1.md`](../../docs/motherduck_database_contract_v1.md) | Schema / process contract | **Current** | Includes specimen + analytic FHIR v1, script 138 references, Check 13 in validator. |
| [`docs/motherduck_v2_staging_runbook.md`](../../docs/motherduck_v2_staging_runbook.md) | Operator runbook | **Current** | Staging vs main, tokens, formalization CI; consistent with contract. |
| [`docs/analysis_resolved_layer.md`](../../docs/analysis_resolved_layer.md) | Local analysis-resolved layer | **Current** for *local* `md_*` analysis tables; **stale** where it implies only local deployment | MotherDuck formalization now carries episode/linkage v3 names in `main` via script 117; wording still skews to “local DuckDB” in places. |
| [`docs/THYROSEQ_INTEGRATION_REPORT.md`](../../docs/THYROSEQ_INTEGRATION_REPORT.md) | ThyroSeq ingest report | **Stale metrics / current process** | Regenerated per ingest; committed snapshot shows **toy** counts (1 source row). **Do not** use for cohort-scale metrics; use governed tables on the target DB + fresh report. |

## README vs signoff vs validation report (explicit)

1. **README** states: prefer **`studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`** for the latest **committed** 27-check `119`; early 20-check PASS lives under `studies/20260407_formalization_validation_release_mode/`; release gate includes non-null `verification_status` on **`qa.manual_review_queue`** in strict mode (not missing manuscript adjudication); specimen/FHIR via scripts **138** + DDL.
2. **Signoff memo** asserts hard blockers for a **point-in-time** review. That picture is **inconsistent** with later validation snapshots and **live** MotherDuck counts (v2 parity, release schemas, specimen/FHIR populated). Compare **timestamps** when reconciling.
3. **Resolution for operators:** Treat **`119_md_formalization_validate.py --md --release-mode`** plus a **fresh** attach check as the promotion health bar. Treat the **signoff memo** as **retained historical review**. **Cleanup** (delete or archive signoff) was **not** performed in this audit task.

## Entity-type quality (orthogonal but linked)

The validation report **PASS** does **not** negate signoff concern **B3** in spirit: `canonical_extracted_fact_long_v2` still lists **594** distinct `entity_type` values in the checked-in report, including known garbage rows (e.g. truncated JSON / LLM failure strings). That is a **manuscript analytics** risk **separate** from specimen keying, but it affects any analysis that groups by raw `entity_type`.
