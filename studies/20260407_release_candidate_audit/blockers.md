# Release-candidate blockers and residual risk

**Generated:** 2026-04-07 (audit run)

## Resolved on live MotherDuck (this audit)

- **Manual review queue:** `qa.manual_review_queue` shows **0 pending** for run `mrq_hydrate_gate` (5,622 rows reviewed). Strict `119 --release-mode` **PASS** on review gate.
- **Master verified views:** `125_master_verified_views.py` rebuilt successfully; row counts 123,577 / 5,574 / 123,577.

## Active / residual

1. **Schema completeness (WARN):** Promoted v2 domain tables on MD lack physical `entity_type` / `entity_value_*` columns (wide JSON note-level shape). Long-form analytic truth remains **`main.canonical_extracted_fact_long_v2`** and presentation views. Treat WARN as documentation-only unless analysts query raw `main.note_entities_llm_*` directly without expansion.

2. **Uniform 11,037 rows per v2 parquet and MD tables:** Current **local** parquets under `processed/output/v2_parquets/` each have **11,037** rows, matching `v2_stage` and `main`. This is **not** a parity bug: earlier gate studies with heterogeneous per-domain counts reflected **different** on-disk extractions. *Investigation suggestion:* compare `COUNT(DISTINCT note_row_id)` per stem vs row count to confirm note-level vs entity-level grain.

3. **Named `CREATE SNAPSHOT` (MotherDuck):** `CREATE SNAPSHOT rc_thyroid_2026_20260407 OF "Thyroid 2026"` failed: *Database is not a native duckdb database so it does not have snapshots* (DuckLake-typed DB). **Mitigation:** rely on `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` automatic history, immutable `release_YYYYMMDD` schemas, and `qa.release_manifest` (latest tag **20260409** at audit time).

4. **Snapshot retention:** `MD_INFORMATION_SCHEMA.DATABASES` shows `historical_snapshot_retention` = **7 days** for `Thyroid 2026`. For longer RC/legal hold, adjust via MotherDuck Business / support per account capabilities (outside repo).

5. **Final analyst lab pull:** Still pending (~5–6 days per release plan). Cohort / lab deltas may require re-staging and a **new** release tag; do not overwrite `release_20260407`–`20260409`.

6. **Hydration domain column:** `114_qa_schema_setup.py` now maps `source_domain` → `domain` **before** `comparison_domain` so future hydrations align QA rows with registry domain keys. **Re-hydrate** if you rely on `domain = source_domain` for older runs.

## Verdict

**RC READY pending final labs** — release-mode validation passes with schema WARN only; blockers above are governance/operational follow-ups, not hard fails.
