# Important data/linkage table audit — 2026-04-14T03:30Z

**Source:** Live MotherDuck `Thyroid 2026`

## Canonical objects (manuscript SSOT)

| Object | Rows | Analyst use? |
|--------|-----:|:---:|
| `main.canonical_extracted_fact_long_v2` | 55,500 | upstream only |
| `main.canonical_fact_quarantine_v2` | 199 | no (excluded) |
| `main.note_extraction_runs` | 3 | no (orchestration) |
| `main.master_fact_long_verified_v1` | 55,500 | **YES** |
| `main.master_patient_rollup_verified_v1` | 5,141 | **YES** |
| `main.master_source_lineage_v1` | 55,500 | **YES** |
| `main.longitudinal_lab_canonical_v1` | 77,960 | **YES** |
| `main.longitudinal_lab_deduped_v` | 56,198 | **YES** |

## Validated adjunct (not core manuscript SSOT)

| Object | Rows | Role |
|--------|-----:|------|
| `main.imaging_nodule_master_v1` | 37,016 | multi-nodule imaging |
| `main.fna_episode_master_v2` | 8,119 | FNA episode master |
| `main.v_fna_episode_bethesda_resolved_v1` | 8,119 | resolved Bethesda |
| `main.v_imaging_nodule_linkage_classification_v1` | 37,016 | imaging-FNA linkage |
| `main.specimen_master_v1` | 10,139 | specimen metadata |
| `main.specimen_tumor_focus_v1` | 11,103 | tumor focus |
| `main.specimen_genomic_assay_v1` | 10,370 | genomic assay linkage |
| `main.fhir_bundle_specimen_export_v1` | 10,139 | FHIR export |

## Governance objects

| Object | Rows | Role |
|--------|-----:|------|
| `qa.release_manifest` | 12 | release ledger (latest tag: 20260408r4) |
| `qa.manual_review_queue` | 5,622 | manual review (0 pending, 0 human-reviewed) |
| `qa.promotion_review_decisions` | 6 | batch promotion decisions (5 batches) |

## Validation status summary

- All canonical objects are populated and internally consistent
- Lineage completeness: 100% (55,500/55,500 facts linked)
- Parity: canonical = master = lineage (55,500 each)
- No duplicate fact_ids
- 0 broken FHIR references
- Export to GitHub: canonical counts CSV/JSON in this study folder
