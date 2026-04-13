# Ultrasound lymph-node extraction audit — 20260413

## Scope

- **Primary sources:** `raw_us_tirads_excel_v1` + `raw_us_tirads_scored_v1` (COMPLETE + scored workbooks ingested to MotherDuck), `raw_imaging_12_slots_v1` (Imaging_12_1_25.xlsx ingest).
- **Structured target:** `ultrasound_reports` (`lymph_node_assessment` plus `clinical_impression`, `source_us_impression`, `recommendation` combined for classification).
- **Not used as proof of US completeness:** `cervical_ln_detail` / pathology-linked NLP (per audit brief).
- **serial_imaging_us:** table not present on connected database — if your environment materializes it, re-run after ingest.
- **Local Excel:** optional pass-through when `raw/COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` and/or `raw/Imaging_12_1_25.xlsx` exist (gitignored in many setups).

## Counts (deduped exams from source inventory layers)

| Metric | Count |
|--------|------:|
| Total US exams (rows in source inventory, all layers) | 34553 |
| Exams with **any** LN-related narrative (state ≠ `no_ln_content`, deduped by exam key) | 7137 |
| Exams with **explicit negative** LN statements (best state per exam) | 29 |
| Exams with **positive/suspicious** LN findings (best state per exam) | 4769 |
| **fully_captured** (row-level audit) | 21905 |
| **partially_captured** | 3834 |
| **text_only_not_structured** | 688 |
| **absent_but_should_exist** | 0 |
| **source_ambiguous** | 8126 |

**Note:** `source_ambiguous` rows are usually cross-source tension (e.g. Imaging_12 slot text has no LN keywords while `ultrasound_reports` narrative for the same key mentions lymph nodes) or empty Imaging_12 excerpts paired with richer structured rows. Count **physical lines ≠ row count** in CSVs when fields contain embedded newlines; use pandas `len(read_csv(...))` for exact row counts.

## Strict criteria result

- **Positive/suspicious misses (rows):** 0 — see `positive_ln_misses.csv`
- **Negative preservation gaps (rows):** 0 — see `negative_ln_capture_gaps.csv`

## Miss lists (identifiers)

### Positive / suspicious not fully represented in structured fields
- (none)

### Explicit negative not preserved in structured combined text
- (none)

## Verdict

**PASS (heuristic):** No source-derived positive/suspicious LN statements lacked structured representation; no explicit-negative gap detected in `ultrasound_reports` combined text vs source layers.

---
Generated UTC: 2026-04-13T18:48:07.349366+00:00
Database: `MotherDuck (md:Thyroid 2026)`
