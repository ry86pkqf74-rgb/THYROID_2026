# Data Dictionary — Multimodal Prediction Dataset

Generated: 20260318_0604

| Variable | Description | Type |
|----------|-------------|------|
| research_id | Unique anonymized patient identifier (integer) | categorical |
| age_at_surgery | Age at first thyroid surgery (years) | categorical |
| sex | Patient sex (Female/Male) | categorical |
| race | Self-reported race (normalized groups) | categorical |
| histology_final | Final histology type (PTC, FTC, MTC, etc.) | categorical |
| t_stage | AJCC pathologic T stage | categorical |
| n_stage | AJCC pathologic N stage | categorical |
| m_stage | AJCC pathologic M stage | categorical |
| ete_grade | Extrathyroidal extension grade (none/microscopic/gross) | categorical |
| multifocal_flag | Multifocal disease flag | flag |
| tumor_size_cm | Largest tumor dimension (cm) | numeric |
| ln_positive_count | Number of lymph nodes positive | numeric |
| ln_examined_count | Number of lymph nodes examined | numeric |
| margin_status | Surgical margin status (positive/negative/close) | categorical |
| vascular_invasion | Vascular invasion status | categorical |
| braf_positive | BRAF V600E mutation positive flag | categorical |
| ras_positive | RAS mutation positive flag (any subtype) | categorical |
| tert_positive | TERT promoter mutation positive flag | categorical |
| molecular_platform | Molecular testing platform (ThyroSeq/Afirma/Other) | categorical |
| ajcc8_stage | AJCC 8th Edition stage group | categorical |
| ata_risk | ATA 2015 initial risk stratification | categorical |
| macis_score | MACIS prognostic score | numeric |
| has_tirads_data | Patient has ACR TI-RADS scoring data | flag |
| tirads_worst | Worst TI-RADS score across nodules | categorical |
| tirads_worst_category | Worst TI-RADS category (TR1-TR5) | categorical |
| n_nodules_imaged | Number of imaged nodules | categorical |
| has_nodule_master | Patient has per-nodule imaging master record | flag |
| has_molecular_data | Patient has molecular testing episode(s) | flag |
| n_molecular_tests | Count of molecular testing episodes | categorical |
| has_rai_data | Patient has RAI treatment episode(s) | flag |
| n_rai_episodes | Count of RAI treatment episodes | categorical |
| has_lab_data | Patient has canonical longitudinal lab records | flag |
| n_lab_values | Count of lab measurements | categorical |
| n_analyte_groups | Count of distinct analyte groups in labs | categorical |
| has_fna_data | Patient has FNA Bethesda cytology data | flag |
| bethesda_worst | Worst (highest) Bethesda category | categorical |
| recurrence_flag | Any recurrence (structural or biochemical) | flag |
| first_recurrence_date | Earliest recurrence date (sparse — see caveats) | categorical |
| has_complication_record | Patient has refined complication phenotype record | flag |
| modality_group | Multimodal data availability classification | categorical |

## Caveats

- `first_recurrence_date`: Only 2.7% exact-source dates; 88.8% unresolved. Not suitable for precise time-to-event analysis without further adjudication.
- Boolean flags from local DuckDB may arrive as text 'true'/'false'; coerced to Python bool in dataset.
- `imaging_nodule_long_v2` is deprecated; `imaging_nodule_master_v1` and `imaging_patient_summary_v1` used instead.
- No PHI or full note text is included in any deliverable.