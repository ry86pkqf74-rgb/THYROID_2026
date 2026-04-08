# Specimen / FHIR focus QA — deterministic diagnostics & Check 13 hardening

**Date:** 2026-04-08  
**Git commit (validator + tests + docs + this report, `main`):** `bd4370de063010db7fb7a374a7efc3c9aa6f04c0`

## Objective

Remove WARN-only treatment of focus-grain specimen QA when the full specimen/FHIR layer is deployed. Deterministic surfaces from `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` were already present; the validator (`scripts/119_md_formalization_validate.py` Check 13) still downgraded defects to WARN whenever `strict=False`.

## Before / after — diagnostic surfaces

| Surface | Before | After |
|--------|--------|-------|
| `qa.v_diag_specimen_duplicate_focus_fp_v1` | Defined in `142`; list duplicate focus fingerprints | Unchanged |
| `qa.v_diag_specimen_orphan_focus_master_v1` | Orphan focus → master | Unchanged |
| `qa.v_diag_specimen_orphan_genomic_focus_v1` | Genomics → missing focus id | Unchanged |
| `qa.v_diag_specimen_provenance_focus_v1` | Aggregate missing `identity_build_run_id` on focus | Unchanged |
| `qa.t_diag_specimen_focus_qa_metrics_v1` | Per-deploy scalar rollup for Check 13 | Unchanged |
| Check 13 when layer complete + defect | Often **WARN** if `strict=False` | **FAIL** (integrity + missing 142 + metrics mismatch + read errors) |
| Check 13 partial deploy (missing some `SPECIMEN_FHIR_OBJECTS`) | WARN | Still WARN-oriented |
| Review burden (open merge/genomic queues) | WARN when non-zero | Still WARN (operational, not structural integrity) |

## MotherDuck / engine probe — full-scan instability

**Not reproduced in this change.** CI and local tests rebuild `142` on an in-memory DuckDB; focus checks use SQL `GROUP BY` / `LEFT JOIN` on `main.specimen_tumor_focus_v1` inside views and a single `CREATE TABLE AS` metrics rollup. No evidence in-repo that list views are unstable while metrics are stable; if a catalog ever fails **only** the `t_diag` build, treat that as a blocking DDL failure (per `specimen_fhir_contract_review.md`).

## Validator changes (exact)

**File:** `scripts/119_md_formalization_validate.py` (`check_specimen_fhir_layer`)

- After resolving `missing` `SPECIMEN_FHIR_OBJECTS`, set `layer_complete = not bool(missing)` and `status_integrity = "FAIL" if layer_complete else status_skip`.
- Use `status_integrity` for: master fingerprint uniqueness failures; `qa.val_specimen_contract_v1` / `qa.val_specimen_genomic_binding_v1` with FAIL rows; missing `142` views/tables when base tables exist; all `142` diagnostic aggregate failures; **metrics vs list-view mismatch** (promoted from WARN to FAIL when layer complete); exceptions in the diagnostics `try` block).

## Deploy path

- `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` — no structural change (already contained focus views + metrics table).
- `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py` — unchanged (full `142` apply).

## Tests

- `tests/test_specimen_fhir_qa_diagnostics.py`: `test_check_13_fails_on_focus_diagnostic_defect_when_layer_complete` asserts FAIL for both `strict=False` and `strict=True`; `test_check_13_fails_on_metrics_mismatch_when_layer_complete` forces stale `n_orphan_genomic_focus` in `t_diag` and expects FAIL.

## Documentation

- `docs/specimen_fhir_contract_review.md` — Check 13 behavior clarified (FAIL when layer complete).
- `docs/motherduck_database_contract_v1.md` — Formalization bullet updated accordingly.

## Validation run (local)

- `python3 -m py_compile scripts/119_md_formalization_validate.py`
- `python3 -m pytest tests/test_specimen_fhir_qa_diagnostics.py -q` — 9 passed
- `ruff` / `mypy`: pre-existing issues in `119` (E402 module import order; tuple indexability) unchanged by this task
