# Safe to start stats / manuscripts

Release tag **20260409** (git `b77b4be8a3a4f194d0e2556828073afdf7dda962`) passed scripted gates when this memo was generated.

** Preconditions verified by automation:**

- `qa.manual_review_queue` has zero pending `verification_status` when `--release-mode` validation completed.
- `main.master_fact_long_verified_v1` exposes `research_id`, `source_object_id` (note row id), and extraction run id per fact.
- Curated parquet under `exports/final_master_release_<tag>/` excludes raw note bodies.

** Still source-limited (do not over-interpret gaps as QC failure):**

- Operative NLP enrichment (berry ligament, frozen section, EBL) may remain sparse by design.
- Recurrence dates: large unresolved fraction is documented as source-limited.
- RAI dose recovery ceiling unless nuclear medicine notes / structured feeds improve.
- Non-Tg lab panel (TSH, PTH, Ca, vit D) depends on institutional lab extract coverage.

Use `docs/final_master_database_contract.md` for analyst-facing column semantics.
