# Release-mode next steps — executed 2026-04-07

**Environment:** `MOTHERDUCK_ENV=qa`, catalog `Thyroid 2026 Molecular QA 20260407`.

## 1. `132_molecular_fact_lineage_views.py --execute --md --md-env qa`

**Result: FAIL (exit 1)** — `main.molecular_results` does not exist on the QA catalog. DuckDB suggests the table exists on dev / PrePromote / prod attachments, not on QA `main`.

**Remediation:** Materialize the molecular results layer on QA (e.g. `scripts/131_molecular_results_layer.py --execute --md --md-env qa` per repo runbooks) and ingest sources (`41`/`42`) as required, **or** refresh/reclone QA from a catalog that already has `molecular_results`, then re-run **132**.

**Log:** `132_execute_qa.log`

## 2. `119_md_formalization_validate.py --md --md-env qa --release-mode`

**Result: BLOCKED (exit 1)** — **22 PASS / 0 WARN / 3 FAIL**.

| Check | Status | Detail |
|-------|--------|--------|
| 5b | FAIL | **5,620** MRQ rows with synthetic placeholder `verification_status` (`SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`). Requires human-reviewed hydrate or `128_mrq_tier_policy_gate_build.py` per `publication_governance_gate.md`. |
| 5b | FAIL | **2 / 2** rows in `qa.promotion_review_decisions` missing `decision_batch_id`. |
| 12b | FAIL | **`main.molecular_testing` missing** while `molecular_test_episode_v2` has **10,126** rows — upstream spine gap; validator message points to loading `molecular_testing`, re-running **22**, **49**, **140** on MD. |

**Logs:** `119_release_mode_console.log`, `119_release_mode/validation_report.md`

## 3. Specimen / FHIR (`138` / `143`)

**Not run** — `main.synoptic_tumor_long_v1` is **absent** on QA `main` (same as structural rehearsal). No Check 13 surface to deploy.

## Verdict

- **132 on QA:** blocked until **`molecular_results`** (and dependencies) exist on QA `main`.
- **119 release-mode:** **FAIL** until governance (MRQ + `decision_batch_id`) and **12b** `molecular_testing` spine are remediated on QA (or validation is run against a catalog that already satisfies those gates).
