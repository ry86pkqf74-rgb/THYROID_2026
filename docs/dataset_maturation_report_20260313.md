# Dataset Maturation Report (2026-03-13)

Generated: 2026-03-13T10:30:17.154346

## Summary

This report documents the final dataset maturation pass that transitions
THYROID_2026 from **manuscript-ready** to **dataset-mature** status.

## Fixes Applied

### Provenance System
- Added unified provenance columns to 4 analysis tables
- Columns: source_table, source_script, provenance_note, resolved_layer_version

### Chronology Anomalies
- Total classified: N/A

### Health Dashboard Tables
- `val_dataset_integrity_summary_v1`
- `val_provenance_completeness_v2`
- `val_episode_linkage_completeness_v1`

### MotherDuck Optimization
- ANALYZE TABLE run on all large canonical tables

## Remaining Structural Limitations

1. **imaging_nodule_long_v2** remains a schema stub (deprecated); `imaging_nodule_master_v1` is canonical
2. **Non-Tg lab dates** (TSH/PTH/Ca/vitD) at 0% — requires institutional data extract
3. **Nuclear medicine notes** — zero in corpus; cannot improve RAI NLP further
4. **Vascular invasion grading** — 87% remain present_ungraded (synoptic template limitation)
5. **ETE sub-grading** — 49 remain present_ungraded after Phase 9 rules

## Validation Outputs

See `exports/dataset_maturation_*/` for CSV exports of all monitoring tables.