# mig_086 — Legacy promotion sweep (pub_canonical view facades)

**Date:** 2026-05-06  
**Migration:** `bq_migrations/mig_086_legacy_promotion_sweep_views.sql`  
**Governance:** Airtable Data Feedback Log `DFL-20260506-T7` (record `recngpdfGKKZF0HMu`); `pub_signoff.bq_migration_log_v1` row `mig_086_legacy_promotion_sweep_views`.

## Scope

- **Views created:** **55** (`CREATE OR REPLACE VIEW … AS SELECT * FROM pub_legacy_source_20260416.<same_name>`).
- **Discovery:** Dynamic anti-join on `INFORMATION_SCHEMA` (legacy BASE TABLE vs `pub_canonical` + `pub_workspace` tables/views), minus the skip list documented in the migration header.
- **Legacy dataset:** **Not modified** (read-only reference).

## Domain breakdown (grouping in SQL file)

| Domain | Count |
|--------|-------|
| synoptic | 1 |
| tumor_ | 2 |
| path_ | 1 |
| extracted_ | 8 |
| molecular_ / thyroseq | 5 |
| note_entities_llm_ | 13 |
| note_entities (non-LLM) | 5 |
| operative_ | 1 |
| fna_ | 3 |
| imaging_ | 2 |
| ultrasound_ / us_ / tirads_llm_ | 4 |
| complication_ | 2 |
| lab / longitudinal | 2 |
| canonical_* (legacy-named) | 4 |
| outcomes / scoring | 2 |
| **Total** | **55** |

## Skip list (not promoted; remain legacy-only)

Same 10 names as in the migration header — dictionaries, ingest logs, md_* mirrors, lab dedup map, analysis subset:

`data_dictionary_v2`, `data_dictionary_v221`, `data_dictionary_parquet_v221`, `molecular_ingestion_runs`, `molecular_assay_dictionary`, `molecular_code_crosswalk`, `md_synoptic_tumor_long_v1`, `md_extracted_fna_bethesda_v1`, `lab_cross_wave_dedup_map_v1`, `analysis_molecular_subset_v1`.

## Verification (executed)

| Check | Result |
|-------|--------|
| `COUNT(*)`, `COUNT(DISTINCT research_id)` on `pub_canonical.synoptic_tumor_long_v1` | **n = 11,103**, **pts = 8,422** |
| Procedural loop: for all 55 names, `COUNT(*)` canonical facade = legacy table | **PASS** (`mig_086_all_55_facade_counts_match_legacy`; script used `--location=us-central1` because default job location differed from dataset) |
| `CALL pub_signoff.run_qc_assertions()` | Completed **exit 0** |
| Latest `qc_violations_v1` slice (`MAX(run_id)`) | **failing_assertions = 0** |
| `COUNT(*)` on `pub_canonical.survival_cohort_enriched` | **61,134** |

## Follow-ups

- **Cox PH (Cursor prompt 4):** Comment posted on Linear **THY-11** — `survival_cohort_enriched` is now visible under `pub_canonical`; re-fit as needed.
- **Cowork:** Confirm `bq_migration_log_v1` and refresh manuscript `bq_cohort_n` where drift was from missing joins.

## Rollback

Per migration header: `DROP VIEW IF EXISTS` each facade in `pub_canonical`; legacy snapshot unchanged.
