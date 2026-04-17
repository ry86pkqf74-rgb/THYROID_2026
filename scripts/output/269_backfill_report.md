# Script 269 - Molecular Episode Backfill Report
_Generated 2026-04-17T05:01:24.125642+00:00_

## Backfill summary
- mte_v2 rows: 10125 -> 10650 (delta +525)
- All inserts tagged `ingestion_source='script_269_backfill'`

## Backfilled rows by source_table
| source_table | n_inserted |
|---|---:|
| thyroseq_molecular_enrichment | 443 |
| extracted_braf_recovery_v1 | 46 |
| ret_patient_adjudicated_v226 | 36 |

## Top 20 source_table values (overall, post-backfill)
| source_table | n_rows |
|---|---:|
| molecular_testing | 10125 |
| thyroseq_molecular_enrichment | 443 |
| extracted_braf_recovery_v1 | 46 |
| ret_patient_adjudicated_v226 | 36 |

## Concordance check (informational)
- Patients with pinned mol_n_tests value: 10025
- Match (pinned == derived from episodes): 9506
- Mismatch (pinned != derived): 519
- Pinned NULL but has episodes: 1
- Match rate: 94.82%

_NOTE: pinned CPM feeders remain authoritative per Scripts 252-265 architecture._
_Episode table is a drill-down surface; backfill closes F1 audit gap without_
_changing the patient-level rollup that CPM uses._

## CRITICAL NON-ACTION (NOT performed by this script)
- `mol_has_thyroseq`, `mol_has_afirma`, `mol_platform`, `molecular_tested_confirmed` remain pinned to `canonical_molecular_tested_v1`
- `mol_n_tests` remains pinned to `_molecular_patient_rollup_v227`
- These pinned feeders are intentionally NOT re-derived from the expanded episode table