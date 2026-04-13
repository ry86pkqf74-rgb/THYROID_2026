# Source inventory summary (local `raw/` + parsers)

Counts are **recomputed** in this run via the same ingest helpers as Phase 12 / April 13 audit (`ingest_complete_us_excel`, `ingest_tirads_scored_excel`, `parse_imaging_12_exam_slots`). FNA workbook parsed with the wide-FNA parser from `studies/20260413_source_truth_completeness_audit/run_source_truth_audit.py` logic (not re-imported here; FNA counts taken from MotherDuck tables for this reaudit).

## Ultrasound corpora

| Corpus | File | Nodule-level rows / keys (this parse) | Notes |
|--------|------|----------------------------------------|--------|
| COMPLETE structured | `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` | 19,891 nodule rows → 19,891 deterministic keys `research_id\|YYYY-MM-DD\|nodule_number` | Same key scheme as `imaging_nodule_master_v1` join. |
| Scored TIRADS | `US Nodules TIRADS 12_1_25.xlsx` | 19,367 nodule rows (non-empty TR cells) → 19,367 keys | Keys use same `rid\|date\|nodule_number` after `norm_date_str`. |
| Imaging_12 inferred | `Imaging_12_1_25.xlsx` | 20,910 inferred nodule rows (`parse_imaging_12_exam_slots`) | Keys = `deterministic_key` from `utils/imaging_12_slots.py`. |

**MotherDuck raw staging row counts (read-only):** `raw_us_tirads_excel_v1` = 19,891; `raw_us_tirads_scored_v1` = 19,549; `raw_imaging_12_slots_v1` = 21,079. Discrepancies vs local key counts reflect different grain (e.g. MD ingest timing) but **deterministic key parity vs `imaging_nodule_master_v1`** is reported in `us_nodule_coverage_audit.csv`.

## FNA corpus

| Source | Evidence |
|--------|----------|
| Wide Excel `FNAs 12_5_2025.xlsx` | Present on disk; episode-level long parse not duplicated in this reaudit script. |
| Canonical episodes | `fna_episode_master_v2` = **8,119** rows (MotherDuck). |
| Cytology rows | `fna_cytology` = **8,063** rows (MotherDuck). |

## Lymph-node–related (source narrative)

Exam-level COMPLETE wide sheet includes `Lymph_Node_Assessment` (used in April US LN audit). This reaudit does **not** re-parse every exam line; see `us_lymph_node_audit_expanded.csv` and April `studies/20260413_us_lymph_node_audit/verdict.md` for the heuristic PASS scope.
