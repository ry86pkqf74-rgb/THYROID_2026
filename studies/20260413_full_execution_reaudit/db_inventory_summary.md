# Database inventory summary (MotherDuck `Thyroid 2026`, read-only SELECT)

Generated with `connect_md_fail_closed` + `studies/20260413_full_execution_reaudit/run_full_execution_reaudit_readonly.py` and ad hoc verification queries.

## Core tables

| Object | Row count | Notes |
|--------|----------:|--------|
| `imaging_nodule_master_v1` | 37,016 | Canonical nodule long table. |
| `imaging_nodule_long_v2` | 19,891 | Same count as COMPLETE raw nodules — aligns with COMPLETE-only grain for this legacy v2 table. |
| `raw_us_tirads_excel_v1` | 19,891 | COMPLETE ingest. |
| `raw_us_tirads_scored_v1` | 19,549 | Scored workbook ingest. |
| `raw_imaging_12_slots_v1` | 21,079 | Imaging_12 inferred slots ingest. |
| `ultrasound_reports` | 6,793 | Exam-level structured US table. |
| `imaging_fna_linkage_mm_v1` | 7,305 | Multimodal imaging–FNA linkage. |
| `imaging_fna_linkage_v3` | 9,911 | V3 linkage (v2 name **does not exist** on this database). |
| `fna_episode_master_v2` | 8,119 | FNA episodes. |
| `fna_cytology` | 8,063 | Cytology rows. |
| `extracted_tirads_validated_v1` | 3,439 | Per-patient reconciled TIRADS (not nodule-grain). |

## Missing / absent objects (catalog errors on COUNT)

| Name | Status |
|------|--------|
| `serial_imaging_us` | **Does not exist** on connected MotherDuck (`Catalog Error` on `COUNT(*)`). |
| `imaging_fna_linkage_v2` | **Does not exist**; `imaging_fna_linkage_v3` is present. |

## Views verified (exist; row counts)

| View | Rows |
|------|-----:|
| `v_imaging_nodule_linkage_classification_v1` | 37,016 |
| `v_fna_episode_bethesda_resolved_v1` | 8,119 |
| `val_imaging_fna_linkage_audit_v1` | (see prior audits; not re-printed here) |

## Canonical nodule rows by `source_table`

| source_table | n |
|--------------|--:|
| `raw_us_tirads_excel_v1` | 19,891 |
| `raw_imaging_12_slots_v1` | 8,794 |
| `raw_us_tirads_scored_v1` | 8,331 |

Sum = 37,016 ✓
