# Release surface decision — specimen/FHIR vs final-master snapshots

## Decision

**Keep manuscript-only `release_*` / parquet final-master lists in 115 and 118.** Specimen, FHIR bundle/export, genomics binding, and `qa.v_diag_*` diagnostics remain authoritative in **`main`** / **`qa`** on the live catalog and are gated by **119** Check 13 — not duplicated into **`FINAL_MASTER_*`** table lists.

## Rationale

- `docs/specimen_fhir_contract_review.md` already states this explicitly (§*Scope vs `115` / `118*`).
- Widening **115**/**118** would inflate immutable `release_*` schemas and parquet bundles with interoperability tables operators may not want in every manuscript export.
- **124** and **126** already enforce (or optionally materialize via **138**/**143**) the specimen/FHIR layer *before* presentation/release steps; **126** runs that gate *before* **`115 --final-master`** and **`118 --final-master`** in source order.

## Enforcement

- `tests/test_motherduck_release_surface_invariants.py` — substring guard on final-master lists.
- `tests/test_release_final_master_surface.py` — prefix guard for `specimen_` / `fhir_`.
- `tests/test_126_final_master_release_contract.py` — orchestration orders gate before 115.

## Doc narrowing vs code widening

**Chosen: doc narrowing/clarification only** for the ambiguity between “release surface” (validation + live `main`) vs “final-master snapshot/parquet surface” (manuscript analytic subset). No change to **FINAL_MASTER_TABLES** / **FINAL_MASTER_MAIN** / **FINAL_MASTER_QA**.
