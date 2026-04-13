# FNA Bethesda completeness verdict

**Run:** 2026-04-13T18:59:54.858609+00:00Z  
**Database:** `MotherDuck (Thyroid 2026)`  
**Token mode:** `motherduck.local.toml:MOTHERDUCK_TOKEN`

## Method

- **Source spine:** `fna_history` (one row per FNA episode; **8119** rows). This table is the ETL long melt of
  `FNAs 12_5_2025.xlsx` produced by `scripts/build_fna_history_from_fnas_detailed.py` / `01_ingest_all_files.py`.
  `fna_index` is chronological within patient (not Excel slot order).
- Raw workbook required on disk for provenance; classification uses `bethesda`, `path`, and `path_extended` from `fna_history`.
- Join keys for structured tables: `(research_id, fna_index)`.

## Summary counts

| Metric | Value |
|--------|------:|
| Total source FNA episodes (`fna_history`) | 8119 |
| Rows aligned with structured `fna_history` (identity) | 8119 |
| Episodes with explicit Bethesda in `fna_history.bethesda` column | 7659 |
| Episodes with conservative text-inferred Bethesda | 4 |
| Episodes not scorable from source (with justification) | 456 |
| Episodes **missing_unexplained** (target 0) | 0 |
| Cross-table numeric Bethesda conflicts | 1899 |

## Strict criteria

- **missing_unexplained:** 0
- **not_scorable_from_source:** each row documents why in `status_justification`.
- **Conflicts:** `fna_bethesda_conflicts.csv` — `unresolved_numeric_mismatch` until adjudicated.

## Artifacts

- `source_fna_inventory.csv` — one row per `fna_history` episode with Bethesda classification
- `structured_fna_inventory.csv` — `fna_episode_master_v2` ⋈ `fna_cytology`
- `bethesda_crosswalk_audit.csv` — source ↔ structured Bethesda fields
- `fna_missing_bethesda.csv` — no numeric 1–6 in episode / history / cytology
- `fna_bethesda_conflicts.csv` — numeric mismatches across tables

## Notes

- `extracted_fna_bethesda_v1` is not deployed on this MotherDuck database; audit uses `fna_episode_master_v2`,
  `fna_history`, `fna_cytology`. See view `v_fna_episode_bethesda_resolved_v1` for episode–cytology resolution.

## Conflict list (exact)

- Full machine-readable list: `fna_bethesda_conflicts.csv` (**1899** rows).
- Each row has `research_id`, `fna_index`, and the three normalized Bethesda numbers when at least two disagree.
