# Unresolved blockers (RC audit)

Auditor: fail-closed checks against **live** MotherDuck only. No local DuckDB fallback.

## 1. `extraction_run_id` completeness (contract + analyst traceability)

**Observation**

| Surface | Rows with NULL or blank `extraction_run_id` | Total rows | Share |
|---------|---------------------------------------------|-----------:|------:|
| `main.canonical_extracted_fact_long_v2` | 55,500 | 123,577 | 44.9% |
| `main.master_fact_long_verified_v1` | 24,421 | 123,577 | 19.8% |

The presentation view partially backfills run id via join / temporal fallback to `main.note_extraction_runs` (`scripts/125_master_verified_views.py`), but **~19.8%** of analyst-facing fact rows still lack a resolvable `extraction_run_id`.

**Contract**

`docs/motherduck_database_contract_v1.md` §3 lists `extraction_run_id` as **required** for extracted-entity data in `main`.

**Gate coverage**

`scripts/119_md_formalization_validate.py` does **not** assert non-null `extraction_run_id` rate on `canonical_extracted_fact_long_v2`; release-mode PASS therefore does **not** clear this provenance gap.

**Status:** **Unresolved** for strict RC sign-off unless governance explicitly waives 100% run-id coverage on historical or blended facts.

## 2. Scalar `release_tag` vs newest `release_YYYYMMDD` schema suffix

**Observation**

- Newest snapshot schema by date suffix: **`release_20260409`** (tables present; includes canonical, labs, master views).
- Scalar `release_tag` propagated to `main.master_*_verified_v1`: **`20260406`** (driven by `qa.release_manifest` latest `created_at`).

**Status:** **Documented inconsistency**, not a load defect. Operators must align manifest append order / view definition if analysts are expected to show tag `20260409` while pointing at current `main`.

## Non-blockers (closed)

- **Uniform 11,037 domain counts:** note-level staging/promotion; `COUNT(*) = COUNT(DISTINCT note_row_id)` on all 23 stems in `v2_stage` and `main` — see `grain_note_row_id.md`.
- **Sparse `reviewer_status`:** consistent with MRQ coverage; zero join defects (NULL status with MRQ row for same patient/domain).
