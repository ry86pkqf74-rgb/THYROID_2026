# RO share smoke — decision (2026-04-13)

## Failure inspected

GitHub Actions run **24356791824** (workflow run associated with public CI #343), job **Syntax / Lint**, step **Smoke test — MotherDuck RO share**.

Root error:

```text
_duckdb.CatalogException: Catalog Error: Table with name master_cohort does not exist!
LINE 1: SELECT COUNT(DISTINCT research_id) FROM master_cohort
```

## Classification

- **Not** a token or connectivity failure: the share attached successfully; the query targeted a table that is **not published** on the read-only share `thyroid_research_ro_v2`.
- **Not** a client bug in `connect_ro_share()`: attachment works; catalog contents differ from full prod `Thyroid 2026`.
- **Repo-owned deterministic fix:** the smoke query must use a table that **exists on the RO share**. Internal docs (`docs/motherduck_canonical_upload_20260402.md`) and inventory notes document `manuscript_cohort_v1` on this share (~10,871 rows).

## Decision

**Fix in place** (smallest surface): keep the blocking live-read RO smoke in **Syntax / Lint**, but change the probe from `master_cohort` to `manuscript_cohort_v1` with the same distinct-patient threshold (`> 10000`).

**Not chosen:** moving the RO smoke to a separate scheduled/manual job — unnecessary once the query matches the share catalog; the remaining steps in the same job still use `connect_rw()` against prod for canonical checks.

## Follow-up

If MotherDuck changes share publication again, prefer updating the documented table list in `docs/motherduck_canonical_upload_20260402.md` and the CI smoke query together.
