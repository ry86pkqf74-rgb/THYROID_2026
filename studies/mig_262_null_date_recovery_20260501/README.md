# mig_262 — NULL `exam_date` recovery (CF-mig262-NULL-DATE-RECOVERY)

**Status:** **Blocked / no automatic fix** in `thyroid_canonical_publication_v1_0` as of 2026-05-01 (post `mig_262_imaging` typo repair).

## Counts (MotherDuck publication `main`)

| Object | `exam_date` NULL | Total rows |
|--------|------------------|------------|
| `imaging_exam_master_v1` | **2,050** | 13,347 |
| `canonical_us_nodule_v2` (non-aggregate) | 2,231 | 37,579 |
| `canonical_us_thyroid_gland_v2` | 6,785 | 13,578 |
| `canonical_us_lymph_node_v2` | 0 | 6,801 |

Distinct keys among NULL `imaging_exam_master_v1` rows: **2,050** `(research_id, exam_id)` pairs (one row each).

## Blockers

1. **`main.raw_imaging_12_slots_v1` is absent** in the publication database — the Round-6 recovery path (archive + optional regex on `raw_text_excerpt`) cannot run here until that table is re-materialized or an equivalent ingest exists.
2. **Legacy join does not recover dates:** `LEFT JOIN` of NULL-date `imaging_exam_master_v1` rows to  
   `"Thyroid 2026 UPdated".us_legacy_20260421.imaging_nodule_master_v1` on `(research_id, exam_id)` with non-null `exam_date` → **0** matches.
3. **Weak same-patient signal:** only **6** NULL-exam rows have *any* non-null `exam_date` on another legacy nodule row for the same `research_id` (ambiguous which date belongs to which exam — not safe for bulk UPDATE).
4. **`canonical_us_nodule_v2.tirads_reported_in_text`** is **`INTEGER`** in publication (not narrative text); regex scan for embedded dates on NULL-exam nodule rows → **0** hits.

## What already shipped

- `mig_262_imaging` fixed the **two** century/OCR typos on `imaging_exam_master_v1` and `canonical_us_nodule_v2` (rids 12048, 10511).
- LN suspicious rollup + `signoff_migration.mig_262` addressed **CF-mig260g** (separate lane).

## Next steps (require product / Logan)

- Re-promote **`raw_imaging_12_slots_v1`** (or merged raw US slot ingest) into publication and re-run §3c of the Composer dispatch (`cursor_prompts/CURSOR_PROMPT_MIG_262_IMAGING_DATE_CLEANUP_20260501.md`) for regex/ratified recovery — **or** accept NULL as ground truth for the 2,050 exams and close the carry-forward explicitly.
- Optional: rerun this probe after any ingest with `.venv/bin/python scripts/mig_262_null_date_probe.py --md`.
