# US nodule / TI-RADS / linkage audit verdict

**Generated (UTC):** 2026-04-13T18:25:19.953804+00:00
**MotherDuck token:** `motherduck.local.toml:MOTHERDUCK_TOKEN` (secret not printed)
**Connection:** `MotherDuck fail-closed`

## Claims (evidence in CSVs in this folder)

1. **Every source ultrasound nodule was extracted into canonical tables**  
   - Deterministic + heuristic match rates are in `nodule_match_matrix.csv`.  
   - `unmatched_source_nodules.csv` row count: **0** (must be 0 for “complete”).

2. **Every source nodule with enough detail received TI-RADS (explicit or ACR-recomputable)**  
   - Source-side explicit TR: **39121** rows with `tirads_reported` not null.  
   - Source-side recomputable (≥5 ACR fields or recalc present): see `source_nodule_inventory.csv`.  
   - Gaps: `missing_canonical_tirads_despite_sufficient_source` count: **0**.

3. **Provenance + downstream linkage**  
   - `provenance_status` is exact/heuristic/missing per matched row.  
   - Downstream states: linked_to_fna **13298**, no_eligible_fna **13399**, unresolved **33822** (source-aligned rows in match matrix).

## Per-source extraction vs `imaging_nodule_master_v1`

| source_system | n_rows | exact | heuristic | missing |
|---------------|-------:|------:|----------:|--------:|
| COMPLETE_MULTI_SHEET | 19891 | 19891 | 0 | 0 |
| IMAGING_12_1_25 | 21079 | 20452 | 627 | 0 |
| US_NODULES_TIRADS_SCORED | 19549 | 19017 | 532 | 0 |

**Interpretation:** `imaging_nodule_master_v1` is built from `raw_us_tirads_excel_v1` unpivot, then supplemented from `raw_us_tirads_scored_v1` and `Imaging_12_1_25.xlsx` (``utils/imaging_12_slots.py``) via ``scripts/50_multinodule_imaging.py`` (±30d dedup vs existing rows).  
- **COMPLETE_MULTI_SHEET** missing count **0** — if 0, claim (1) holds for the structured COMPLETE corpus.  
- Remaining gaps are usually dates beyond ±30d vs any canonical row with the same `research_id` + `nodule_number`, or Excel/audit parser drift.

## Recomputed source counts (this run)

| Metric | Value |
|--------|------:|
| COMPLETE workbook exam rows (All_Ultrasound_Reports) | 6793 |
| COMPLETE structured nodules (ingest rows) | 19891 |
| US Nodules TIRADS scored nodules | 19549 |
| Imaging_12_1_25 inferred nodule rows (exam slots / measurement splits) | 21079 |
| Imaging_12 unique exam groups (rid+slot+date) | 8816 |
| **Total source_nodule_inventory rows** | **60519** |
| serial_imaging_us rows (if queried) | N/A |
| ultrasound_reports rows | 6793 |
| raw_us_tirads_excel_v1 rows | 19891 |
| imaging_nodule_master_v1 rows | 36957 |
| imaging_nodule_long_v2 rows | 19891 |
| extracted_tirads_validated_v1 rows (patient-level) | 3439 |
| imaging_fna_linkage_mm_v1 rows | 7305 |

## Verdict counts (required)

| Metric | Count |
|--------|------:|
| Total source nodules | 60519 |
| Exact extracted | 59360 |
| Heuristic extracted | 1159 |
| Missing | 0 |
| Explicit TI-RADS in source (non-null tirads_reported) | 39121 |
| Recomputable TI-RADS from source (≥5 criteria or source recalc) | 19891 |
| Canonical TI-RADS present (master: reported OR ACR not null) | 28163 |
| Missing canonical TI-RADS despite sufficient source detail | 0 |
| Nodules with exact provenance (match matrix) | 59360 |
| Nodules with downstream linked_to_fna (match matrix) | 13298 |
| Nodules with no_eligible_fna | 13399 |
| Nodules unresolved | 33822 |

## Overall completeness rule

- `unmatched_source_nodules.csv` empty: **True**
- `canonical_without_source.csv` rows: **0** (must be 0 or documented duplicates)
- Missing TI-RADS despite sufficient source: **0**

**Overall:** `COMPLETE`

### If NOT_COMPLETE

See per-row notes in `nodule_match_matrix.csv` and unmatched/orphan CSVs. Imaging_12 rows are **inferred** from exam-slot text (measurement regex split); they are not duplicate COMPLETE rows.

## Methods

1. Parsed `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` (All_Ultrasound_Reports) with excel row index + nodule slot.  
2. Parsed `US Nodules TIRADS 12_1_25.xlsx` per sheet with row index.  
3. Parsed `Imaging_12_1_25.xlsx` per US-1..14 slot when date present; nodule rows inferred from measurement counts in nodule text (min 1).  
4. Canonical keys (same grain as `imaging_nodule_master_v1` / script 50): `research_id|YYYY-MM-DD|nodule_number` — **no** US report number in key.  
5. Heuristic: ±1 calendar day; then for `US_NODULES_TIRADS_SCORED` and `IMAGING_12_1_25` only, closest canonical row within **±30 days** with same `research_id` + `nodule_number`.  
6. Linkage: primary `imaging_fna_linkage_mm_v1`; pathology via `imaging_pathology_concordance_review_v2.nodule_id` when present; no FNA episodes ⇒ `no_eligible_fna`.

Per-source extraction parity: see `source_system_summary.csv`.

---
`run_us_nodule_tirads_linkage_audit.py`
