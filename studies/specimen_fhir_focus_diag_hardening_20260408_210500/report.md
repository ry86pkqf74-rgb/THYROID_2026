# Specimen FHIR focus diagnostic hardening — 2026-04-08

## Summary

Focus-level specimen QA is **deterministic** and **authoritative** for Check 13 when the full specimen/FHIR layer is materialized: all signals come from `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` surfaces and `qa.t_diag_specimen_focus_qa_metrics_v1`, not from ad hoc Python scans of `main.specimen_tumor_focus_v1`.

## Changes

1. **142 DDL** — Added `qa.v_diag_specimen_provenance_focus_gaps_v1` (row-level listing for blank/missing `identity_build_run_id` on focus rows). Scalar `qa.v_diag_specimen_provenance_focus_v1` remains for rollup KPIs.

2. **Check 13 (`119`)** — Extended cross-checks when diagnostics exist and the layer is complete:
   - `n_rows_in_duplicate_fp_groups` must equal `SUM(row_count)` from `qa.v_diag_specimen_duplicate_focus_fp_v1`.
   - `n_missing_focus_provenance` must equal `COUNT(*)` from `qa.v_diag_specimen_provenance_focus_gaps_v1`.
   - Exception path now reports the same check name as the success path (`142 surfaces + focus metrics`).

3. **138 orchestration** — `deploy_specimen_fhir_qa_diagnostics` (142) runs **before** `run_validation` / `qa.val_specimen_contract_v1` persistence. `run_validation` now uses `NOT EXISTS (SELECT 1 FROM qa.v_diag_…)` for focus fingerprint, orphan focus→master, so contract rows align with the diagnostic views.

4. **139** — Documented that standalone identity runs keep inline SQL for focus checks because full **142** requires genomic/FHIR tables not present on identity-only deploys.

5. **143** — Docstring states full 142 apply path including focus surfaces and metrics table (no code path change; still runs `142_specimen_fhir_qa_diagnostics_ddl.sql`).

6. **Release gate** — `utils/specimen_fhir_release_gate.py` `SPECIMEN_FHIR_DIAG_VIEWS` includes `v_diag_specimen_provenance_focus_gaps_v1`.

7. **Tests** — `tests/test_specimen_fhir_qa_diagnostics.py`: provenance gaps vs metrics; focus metrics vs `SUM(row_count)` and gap COUNT; Check 13 failures for orphan genomic metrics mismatch, duplicate row-sum mismatch, and provenance gap mismatch. `tests/test_specimen_fhir_scripts_offline.py`: `EXPECTED_QA_DIAG_VIEWS` aligned with Check 13.

8. **Docs** — `docs/specimen_fhir_contract_review.md` updated for the gaps view and cross-check rules.

## Verification (local)

- `python3 -m py_compile` on touched Python modules  
- `ruff check` on edited modules (119 has pre-existing E402 at module import site)  
- `pytest tests/test_specimen_fhir_qa_diagnostics.py`, `tests/test_specimen_fhir_scripts_offline.py`, `tests/test_specimen_fhir_release_gate.py` (targeted) — pass  

## Remaining engine limits

None identified for these surfaces on DuckDB/MotherDuck beyond existing notes in the contract doc (e.g. merge-queue `COUNT` may be unavailable on some catalogs — already WARN-only in Check 13).
