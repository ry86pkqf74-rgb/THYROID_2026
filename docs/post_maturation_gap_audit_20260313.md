# Post-Maturation Gap Audit — 2026-03-13

## Summary

Audit of canonical table propagation gaps after script 75 dataset maturation.
Profiled against live MotherDuck `thyroid_research_2026` database.

---

## 1. Operative Episode Detail (`operative_episode_detail_v2`)

| Metric | Value |
|--------|-------|
| Total rows | 9,371 |
| Distinct patients | 9,368 |

### Boolean flag fill rates (all 100% non-NULL after script 75)

| Column | Non-NULL % | TRUE % |
|--------|-----------|--------|
| `central_neck_dissection_flag` | 100.0 | 26.6 |
| `lateral_neck_dissection_flag` | 100.0 | 2.6 |
| `rln_monitoring_flag` | 100.0 | (boolean) |
| `gross_ete_flag` | 100.0 | (boolean) |
| `drain_flag` | 100.0 | (boolean) |
| `reoperative_field_flag` | 100.0 | (boolean) |
| `parathyroid_autograft_flag` | 100.0 | (boolean) |
| `note_date_resolved` | 99.9 | — |

### Sparse/unpropagated columns

| Column | Fill % | Root cause |
|--------|--------|------------|
| `rln_finding_raw` | 4.0 | NLP extractor output sparsely populated |
| `operative_findings_raw` | 6.3 | STRING_AGG of present findings — most ops have no NLP hits |
| `ebl_ml` | 1.3 | Structured `operative_details.ebl` only 122/9368 rows |
| `parathyroid_identified_count` | — | Column does NOT exist; NLP `parathyroid_management` domain not propagated |
| `parathyroid_resection_flag` | — | Column does NOT exist; `parathyroid_removed` entity not wired |
| `frozen_section_flag` | — | Column does NOT exist; `specimen_detail` domain not propagated |
| `berry_ligament_flag` | — | Column does NOT exist; `berry_ligament` domain not propagated |
| `ebl_ml_nlp` | — | Column does NOT exist; NLP-extracted EBL not propagated |

### Linkage columns: NONE exist on this table

---

## 2. RAI Treatment Episode (`rai_treatment_episode_v2`)

| Metric | Value |
|--------|-------|
| Total rows | 1,857 |
| Distinct patients | 862 |

| Column | Fill % | Notes |
|--------|--------|-------|
| `dose_mci` | 20.0 | 371/1,857 rows; 276 unique patients via `extracted_rai_dose_refined_v1` |
| `resolved_rai_date` | 68.5 | |
| `rai_assertion_status` | 100.0 | |
| `linked_surgery_episode_id` | 1.0 | Only 19 rows from script 70 backfill |
| `dose_source` | — | Column does NOT exist |
| `dose_confidence` | — | Column does NOT exist |
| `surgery_link_score_v3` | — | Column does NOT exist |

### Root cause for dose sparsity
- Zero nuclear medicine notes in `clinical_notes_long` corpus
- `extracted_rai_dose_refined_v1`: 307 rows, 276 patients, avg 141.8 mCi
- Remaining 1,486 RAI episodes have no dose information in any source

---

## 3. Molecular Test Episode (`molecular_test_episode_v2`)

| Metric | Value |
|--------|-------|
| Total rows | 10,126 |
| Distinct patients | 10,026 |

| Column | Fill % | Absolute | Notes |
|--------|--------|----------|-------|
| `ras_flag` TRUE | 3.2 | 325 | From script 70 backfill |
| `ras_subtype` | 1.9 | 191 | 134 rows with ras_flag=TRUE but no subtype |
| `braf_flag` TRUE | 2.7 | ~274 | |
| `linked_fna_episode_id` | 6.3 | ~638 | From script 70 backfill |
| `linked_surgery_episode_id` | 0.0 | 0 | NOT propagated |
| `fna_link_score_v3` | — | — | Column does NOT exist |

### Available for propagation
- `extracted_ras_patient_summary_v1`: 321 patients (NRAS=169, HRAS=92, KRAS=31, unspecified=29)
- 134 rows with ras_flag=TRUE but NULL ras_subtype can be filled

---

## 4. Recurrence (`extracted_recurrence_refined_v1`)

| Detection category | Count | % |
|--------------------|-------|---|
| no_recurrence | 8,885 | 81.7 |
| structural_date_unknown | 1,764 | 16.2 |
| biochemical_only | 168 | 1.5 |
| structural_confirmed | 54 | 0.5 |

### Gap: 1,764 patients with structural recurrence but no date
- No `recurrence_date_best` or `recurrence_date_status` columns exist
- Recurrence flag is separated from date quality tier

---

## 5. Linkage V3 Coverage

| Linkage table | Rows | Patients |
|--------------|------|----------|
| `imaging_fna_linkage_v3` | 0 | 0 |
| `fna_molecular_linkage_v3` | 708 | 637 |
| `preop_surgery_linkage_v3` | 3,591 | 3,176 |
| `surgery_pathology_linkage_v3` | 9,409 | 8,733 |
| `pathology_rai_linkage_v3` | 23 | 19 |

### Propagation status to canonical tables

| Target table | Has linkage columns? | V3 score columns? | Gap |
|-------------|---------------------|-------------------|-----|
| `tumor_episode_master_v2` | NO | NO | Zero linkage columns in schema |
| `operative_episode_detail_v2` | NO | NO | Zero linkage columns in schema |
| `imaging_nodule_master_v1` | NO | NO | Zero linkage columns in schema |
| `molecular_test_episode_v2` | `linked_fna_episode_id` (6.3%) | NO | Missing score, surgery link |
| `fna_episode_master_v2` | `linked_molecular_episode_id` (1.2%) | NO | Missing surgery link, score |
| `rai_treatment_episode_v2` | `linked_surgery_episode_id` (1.0%) | NO | Missing score |

---

## 6. Lab Coverage

### Source tables

| Table | Rows | Patients |
|-------|------|----------|
| `thyroglobulin_labs` | 30,245 | 2,569 |
| `anti_thyroglobulin_labs` | 14,314 | 2,127 |
| `extracted_postop_labs_expanded_v1` | 1,395 | 1,051 |
| `longitudinal_lab_clean_v1` | 38,699 | 2,673 |

### `longitudinal_lab_clean_v1` by analyte

| Lab type | Rows | Patients | Completeness tier |
|----------|------|----------|-------------------|
| thyroglobulin | 24,261 | 2,569 | current_structured |
| anti_tg | 14,302 | 2,126 | current_structured |
| pth | 136 | 126 | current_nlp_partial |
| tsh | 0 | 0 | future_institutional_required |
| calcium | 0 | 0 | future_institutional_required |
| ionized_calcium | 0 | 0 | future_institutional_required |
| vitamin_d | 0 | 0 | future_institutional_required |
| albumin | 0 | 0 | future_institutional_required |

### Root causes for lab sparsity
- TSH/calcium/ionized_ca exist in `extracted_postop_labs_expanded_v1` (1,395 rows) but are NOT present in `longitudinal_lab_clean_v1`
- Script 53 only ingested Tg, anti-Tg, and PTH into the clean table
- No institutional lab feed exists yet

---

## 7. Health Monitoring Tables

| Table | Rows | Status |
|-------|------|--------|
| `val_dataset_integrity_summary_v1` | 30 | Available, NOT used by dashboard |
| `val_provenance_completeness_v2` | 23 | Available, NOT used by dashboard |
| `val_episode_linkage_completeness_v1` | 5 | Available, NOT used by dashboard |
| `val_temporal_anomaly_resolution_v1` | 626 | Available, NOT used by dashboard |

---

## 8. Gap Classification

### Fixable now (with current corpus)
1. Operative NLP enrichment: 5 new columns from extractor domains not yet wired
2. Molecular RAS subtype: 134 rows can get subtype filled from `extracted_ras_patient_summary_v1`
3. Linkage ID propagation: 6 canonical tables missing v3 linkage columns/scores
4. Recurrence date tiers: add quality classification to existing recurrence data
5. RAI dose provenance: add source/confidence columns to already-filled doses
6. Lab canonical layer: unify existing sources into forward-compatible schema

### Source-limited (future institutional extract required)
1. RAI dose: 80% of episodes have no dose in any source (no nuclear med notes)
2. TSH/calcium/vitamin D/albumin labs: zero structured lab data
3. Recurrence dates: 1,764 structural_date_unknown cases have no date source
4. Imaging-FNA linkage: 0 rows (imaging nodule data not linked to FNA)
5. Molecular surgery linkage: 0% filled, `preop_surgery_linkage_v3` maps FNA->surgery not molecular->surgery directly
