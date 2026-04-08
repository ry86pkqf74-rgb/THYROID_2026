# Specimen FHIR focus-level diagnostic hardening

**UTC stamp:** 2026-04-08T16:05:00Z (report folder timestamp)  
**Repo:** THYROID_2026

## Summary

Focus-level release checks for the specimen/FHIR layer are now **first-class** in `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`. `scripts/119_md_formalization_validate.py` Check 13 reads **only** `qa` diagnostic views/tables for focus duplicates, orphan focus rows, genomic→focus orphans, and focus provenance gaps — no best-effort Python SQL against `main.specimen_tumor_focus_v1`.

## Before / after

| Aspect | Before | After |
|--------|--------|--------|
| Focus duplicate FP | Ad hoc `SELECT ... GROUP BY focus_fingerprint_sha256 HAVING COUNT(*) > 1` from 119; opaque failures returned `None` → **WARN** “focus-table scans unavailable” | `qa.v_diag_specimen_duplicate_focus_fp_v1` + `qa.t_diag_specimen_focus_qa_metrics_v1.n_duplicate_fp_groups` |
| Orphan focus ↔ master | Ad hoc LEFT JOIN from 119 | `qa.v_diag_specimen_orphan_focus_master_v1` |
| Genomic → missing focus | Ad hoc JOIN from 119 | `qa.v_diag_specimen_orphan_genomic_focus_v1` (distinct from master orphan view) |
| Focus provenance | Ad hoc `COUNT(*) FILTER` on focus table from 119 | `qa.v_diag_specimen_provenance_focus_v1` |
| Check 13 authority | Partially best-effort when scans “failed” | When layer + `142` are present, counts are **authoritative**; metrics vs view mismatch → **WARN** with rerun-143 hint |
| Release gate (`utils/specimen_fhir_release_gate.py`) | Listed 6 `v_diag_*` names | Lists all 10 `v_diag_*` names + `t_diag_specimen_focus_qa_metrics_v1` |

## Live MotherDuck

**Not touched in this change set.** Validation was offline: in-memory DuckDB via pytest, same as CI. Deploying `142` to a live catalog still requires **`138 --md`** or **`143 --md`** with RW token (e.g. from `.streamlit/secrets.toml` / MotherDuck SA per project convention).

## Optional: PK / UNIQUE capability (local probe)

On a throwaway in-process DuckDB (`:memory:`), `CREATE TABLE t(a INT PRIMARY KEY)` and `CREATE TABLE u(b INT UNIQUE)` **succeed**. This does **not** prove MotherDuck/DuckLake behavior. **Design:** keep **QA-enforced uniqueness** (`v_diag_*`, `val_specimen_*`) as the release contract; do not rely on `PRIMARY KEY`/`ON CONFLICT` unless a future catalog probe on the **target** database confirms support and benefit.

## Tests added/updated

- `tests/test_specimen_fhir_qa_diagnostics.py`: happy-path metrics parity, defect injection (duplicate FP, orphan focus, orphan genomic focus, blank provenance), **Check 13 strict FAIL** on orphan genomic focus, name alignment with `utils/specimen_fhir_release_gate`.

## Docs

- `docs/specimen_fhir_contract_review.md` — diagnostic table + Check 13 description updated.  
- `docs/motherduck_database_contract_v1.md` — `142`/Check 13/`qa` catalog rows updated.
