# Artifact reconciliation — status documents vs reality

This section compares **checked-in** README/signoff/validation artifacts and **live** MotherDuck evidence from the audit run (`motherduck_audit_evidence.md`). Labels follow the user request: **current**, **stale**, or **superseded-without-cleanup**.

## Summary table

| Artifact | Role | Assessment | Notes |
|----------|------|------------|-------|
| [`README.md`](../../README.md) | Repo entry/status narrative | **Current** (with nuance) | Describes 2026-04-06/07 formalization, release gate on `qa.manual_review_queue`, points to latest validation under `studies/20260407_formalization_validation_release_mode/`. Still contains historical March-13 local-freeze paragraphs; those are **context**, not the MotherDuck formalization SSOT. |
| [`studies/20260407_signoff_memo/signoff_memo.md`](../20260407_signoff_memo/signoff_memo.md) | Architecture sign-off | **Superseded-without-cleanup** for MotherDuck operational truth | Dated 2026-04-07; verdict “NOT READY” and blockers (e.g. v2_stage missing, load_inventory missing) **conflict** with the **same-dated** release-mode validation report and with **live** MotherDuck (v2 parity + specimen tables populated). **Retain** as historical audit of a point-in-time review; do not treat as current go/no-go without re-validation. |
| [`studies/20260407_formalization_validation_release_mode/validation_report.md`](../20260407_formalization_validation_release_mode/validation_report.md) | Formalization PASS evidence | **Current** as *checked-in snapshot* of one successful `--release-mode` run | Timestamp **2026-04-07T04:47:39Z**; PASS all 20 checks; v2_stage/main parity; review queue all reviewed; release schemas through **20260409**; canonical fact distribution still shows **594** `entity_type` values (data-quality topic separate from specimen design). |
| [`docs/motherduck_database_contract_v1.md`](../../docs/motherduck_database_contract_v1.md) | Schema / process contract | **Current** | Includes specimen + analytic FHIR v1, script 138 references, Check 13 in validator. |
| [`docs/motherduck_v2_staging_runbook.md`](../../docs/motherduck_v2_staging_runbook.md) | Operator runbook | **Current** | Staging vs main, tokens, formalization CI; consistent with contract. |
| [`docs/analysis_resolved_layer.md`](../../docs/analysis_resolved_layer.md) | Local analysis-resolved layer | **Current** for *local* `md_*` analysis tables; **stale** where it implies only local deployment | MotherDuck formalization now carries episode/linkage v3 names in `main` via script 117; wording still skews to “local DuckDB” in places. |
| [`docs/THYROSEQ_INTEGRATION_REPORT.md`](../../docs/THYROSEQ_INTEGRATION_REPORT.md) | ThyroSeq ingest report | **Stale metrics / current process** | Regenerated per ingest; committed snapshot shows **toy** counts (1 source row). **Do not** use for cohort-scale metrics; use governed tables on the target DB + fresh report. |

## README vs signoff vs validation report (explicit)

1. **README** states: latest MotherDuck validation artifact is `studies/20260407_formalization_validation_release_mode/validation_report.md`; release gate includes empty pending manual review on **`qa.manual_review_queue`** in strict mode; specimen/FHIR materialization via scripts **138** + DDL.
2. **Signoff memo** asserts hard blockers (empty v2_stage, missing load_inventory, no release schemas, thousands of pending reviews). That picture is **inconsistent** with the **checked-in validation_report.md** (PASS; load_inventory 150; 4 release schemas; 16,866 reviewed rows) and with **live** MotherDuck counts showing populated specimen/FHIR tables.
3. **Resolution for operators:** Treat **`119_md_formalization_validate.py --md --release-mode`** plus a **fresh** attach check as the promotion health bar. Treat the **signoff memo** as a **retained historical review** that failed to converge with subsequent remediation **or** reflected a different database state than `validation_report.md`. **Cleanup** (delete or archive signoff) was **not** performed in this audit task.

## Entity-type quality (orthogonal but linked)

The validation report **PASS** does **not** negate signoff concern **B3** in spirit: `canonical_extracted_fact_long_v2` still lists **594** distinct `entity_type` values in the checked-in report, including known garbage rows (e.g. truncated JSON / LLM failure strings). That is a **manuscript analytics** risk **separate** from specimen keying, but it affects any analysis that groups by raw `entity_type`.
