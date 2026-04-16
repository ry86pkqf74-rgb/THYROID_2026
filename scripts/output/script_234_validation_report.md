# Script 234 — RAI/Tg Data Quality Resolution + DB Hygiene
## 2026-04-16 15:54 UTC

## Phase 1 — RAI/Tg data-quality fixes (8 new columns)
| # | Issue | Flagged | New column(s) |
|---|---|---|---|
| 1 | RAI flag/episode discordance | 279 | rai_received_reconciled, rai_flag_discordant |
| 2 | Benign + RAI + NULL histology | 100 | benign_rai_suspect_malignant |
| 3 | Tg availability documentation | n/a | tg_data_available, tg_limitation_note |
| 4 | Tg nadir outliers (>100, no recurrence) | 74 | tg_nadir_suspect_preablation |
| 5 | RAI dose availability + extreme cum dose | dose_avail=214 / extreme=21 | rai_dose_data_available, rai_cumulative_dose_extreme |

Recovery table created: `rai_benign_histology_recovery_v234` (0 rows).

## Phase 2 — detail_table_registry_v1 pointer integrity
- Registry entries verified       : 109
- Missing tables                  : 0
- Row-count mismatches            : 0
- Tables with orphan research_ids : 47

## Phase 3 — Dictionary updates
- `__readme`              : +1 row(s)
- `data_dictionary_v221`  : +8 inserted, 0 updated

## Phase 4 — Working-DB cleanup
- Candidate tables        : 45
- Working DB BEFORE       : 176 tables
- Working DB AFTER        : 131 tables
- Dropped                 : 45
- Archive manifest        : scripts/output/working_db_archive_manifest_v234.json

## Phase 5 — Invariants
- canonical_patient_master: 10,871 rows × 1479 columns
- 0 NULL research_ids, 0 NULL fna_path_outcome
- Publication DB main tables : 112
- Working DB main tables     : 131
