# mig_214 investigation — `canonical_molecular_genetics_v2` NULL `molecular_episode_id`

**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)  
**Date:** 2026-04-30  
**Trigger:** Lane D — ChatGPT review follow-up; confirm no recoverable per-test anchor before patient-level-only flag.

## Build context (`scripts/269_backfill_molecular_episodes.py`)

Script 269 backfills rows into **legacy** `molecular_test_episode_v2` from:

1. `thyroseq_molecular_enrichment` (patient-level pick, first row per patient),
2. `extracted_braf_recovery_v1` (NGS-BRAF positive),
3. `ret_patient_adjudicated_v226` (RET true positive).

Inserts are tagged `ingestion_source = 'script_269_backfill'`. Publication `main` does not host `molecular_test_episode_v2` (live table is under `"Thyroid 2026 UPdated".molecular_legacy_20260421`); genetics builder still emits `report_text_ref = 'molecular_test_episode_v2#None'` when no episode key is wired.

## Live probes (post-`git pull`, connect_locked)

| Probe | Result |
|--------|--------|
| Rows with `molecular_episode_id IS NULL` | **525** rows, **520** distinct `research_id` |
| All NULL-episode `ingestion_source` | **`script_269_backfill`** (525/525) |
| `test_date_native` non-null | **0** / 525 |
| `resolved_test_date` non-null | **0** / 525 |
| `linked_fna_episode_id` non-empty | **0** / 525 |
| `linked_surgery_episode_id` non-null | **0** / 525 |
| `report_text_ref` present | **525** / 525 (pattern `molecular_test_episode_v2#None`) |

Random sample of 20 NULL-episode rows: mixed `report_source_table` (`thyroseq_molecular_enrichment`, `extracted_braf_recovery_v1`, `ret_patient_adjudicated_v226`); no hidden dates in sampled scalar fields.

## Upstream ThyroSeq enrichment

- Publication `main` has **no** `thyroseq_molecular_enrichment` table.
- Legacy: `"Thyroid 2026 UPdated".molecular_legacy_20260421.thyroseq_molecular_enrichment` (~10,861 rows).
- Columns include `imported_at` (ingest stamp) and raw/pathology fields; **no** clinical specimen/test date column suitable for episode recovery.

## Conclusion

The 525 rows **lack recoverable per-test / per-date anchors** in structured fields or legacy enrichment metadata sufficient to assign `molecular_episode_id` without new NLP or institutional feeds.

**Decision:** Proceed with **mig_214** — add `is_patient_level_only_evidence BOOLEAN`, set `TRUE` where `molecular_episode_id IS NULL` (525 rows). Per-test analyses should filter `WHERE is_patient_level_only_evidence = FALSE` (or equivalently `WHERE molecular_episode_id IS NOT NULL` while invariant holds).
