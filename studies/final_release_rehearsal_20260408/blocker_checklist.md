# Blocker checklist — QA catalog `Thyroid 2026 Molecular QA 20260407`

**Captured:** 2026-04-08 (rehearsal). Source: `119_md_formalization_validate.py --release-mode`  
**Study logs:** `119_qa_release_mode.log`, `qa_release_mode/validation_report.md`

| # | Check | Status | Notes |
|---|--------|--------|--------|
| 1 | Canonical row parity (local vs MD) | **FAIL** | `canonical_extracted_fact_long_v2`, `canonical_fact_quarantine_v2` — stale/mismatched local attach vs cloud |
| 2 | MRQ synthetic placeholders | **FAIL** | 5,620 rows `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` in `qa.manual_review_queue` |
| 3 | Promotion decision `decision_batch_id` | **FAIL** | 2 / 2 rows NULL/blank in `qa.promotion_review_decisions` |
| 4 | Molecular spine | **FAIL** | `main.molecular_testing` missing; `molecular_test_episode_v2` present — Check 12b |
| 5 | Specimen / FHIR gate | **PASS** (skipped) | `synoptic_tumor_long_v1` absent — Check 13 skipped |

**Gate inputs validated for 126 dry-run (repo files, not QA DB state):**

- `studies/20260407_tier_policy_review_gate/manual_review_queue.csv` — non-blank verification, no synthetic placeholders per `publication_governance.py`.
- `studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv` — batch summary row(s); live 126 sets `decision_batch_id` from `--release-date` / `--decision-batch-id`.

**Institutional lab wave:** no `exports/incoming/final_lab_*.csv` found; `--lab-csv` omitted from 126 dry-run.
