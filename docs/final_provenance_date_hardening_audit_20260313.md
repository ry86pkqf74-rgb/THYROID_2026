# Provenance & Date-Linkage Hardening Audit

**Date:** 2026-03-13
**Scope:** Manuscript-critical and patient-facing resolved layers

---

## Provenance Coverage Matrix

### Resolved Layer Tables

| Table | source_table | source_script | provenance_note | resolved_layer_version | Date Fields |
|-------|:-----------:|:------------:|:--------------:|:--------------------:|:-----------:|
| patient_analysis_resolved_v1 | ✓ | ✓ | ✓ | ✓ | surg_first_date |
| episode_analysis_resolved_v1_dedup | ✓ | ✓ | ✓ | ✓ | resolved_surgery_date |
| lesion_analysis_resolved_v1 | ✓ | ✓ | ✓ | ✓ | tumor_date |
| survival_cohort_enriched | ✓ | ✓ | ✓ | ✓ | time_days |

### Episode Tables

| Table | date_status | date_confidence | source_tables | event_date |
|-------|:----------:|:--------------:|:------------:|:----------:|
| tumor_episode_master_v2 | ✓ | ✓ | ✓ | resolved_date |
| molecular_test_episode_v2 | ✓ | ✓ | ✓ | resolved_date |
| rai_treatment_episode_v2 | ✓ | ✓ (dose_confidence) | ✓ | resolved_rai_date |
| operative_episode_detail_v2 | ✓ | ✓ (op_confidence) | ✓ | resolved_surgery_date |
| fna_episode_master_v2 | ✓ | ✓ | ✓ | resolved_fna_date |
| imaging_nodule_master_v1 | — | — | ✓ (tirads_source) | exam_date |

### Recurrence & Labs

| Table | date_status | confidence | source_link |
|-------|:----------:|:----------:|:----------:|
| extracted_recurrence_refined_v1 | recurrence_date_status (4 tiers) | recurrence_date_confidence | detection_category |
| longitudinal_lab_canonical_v1 | lab_date_status | — | source_table, source_script |

---

## Graded Provenance Matrix

| Domain | Grade | Detail |
|--------|-------|--------|
| Pathology (path_synoptics) | FULLY_SOURCE_AND_DATE_LINKED | Structured synoptic with surg_date |
| Tumor pathology | FULLY_SOURCE_AND_DATE_LINKED | Joins to path_synoptics |
| Molecular testing | SOURCE_LINKED_DATE_PARTIAL | 3,178 day-level dates, 1,327 non-placeholder |
| RAI treatment | SOURCE_LINKED_DATE_PARTIAL | 41% have dose; all have date_status |
| Recurrence events | DERIVED_WITH_UPSTREAM_PROVENANCE | 54 exact + 168 biochemical + 1,764 flag-only |
| Thyroglobulin labs | FULLY_SOURCE_AND_DATE_LINKED | 99.5% specimen_collect_dt accuracy |
| Non-Tg labs (TSH/PTH/Ca) | RAW_TEXT_ONLY | NLP-extracted, no structured lab date |
| Operative NLP fields | SOURCE_LIMITED | Extractor exists; boolean defaults block enrichment |
| Imaging TIRADS | SOURCE_LINKED_DATE_PARTIAL | From Excel; 80.1% ACR concordance |
| Nuclear medicine | SOURCE_LIMITED | Zero notes in clinical_notes_long corpus |

---

## Date Status Distribution (Provenance Events)

From `provenance_enriched_events_v1` (50,297 events):

| Status | Count | Meaning |
|--------|-------|---------|
| LAB_DATE_USED | ~2,500 | Structured specimen collection date |
| ENTITY_DATE_USED | ~15,000 | Entity-level date from extraction |
| ENTITY_DATE_EQUALS_NOTE_DATE | ~8,000 | Entity date matches note date |
| NOTE_DATE_FALLBACK | ~18,000 | Only note-level date available |
| NO_DATE | ~6,800 | No date in any source (mostly non-Tg labs) |

---

## Acceptance Criteria

- [x] Manuscript-critical tables have explicit documented provenance/date coverage rates
- [x] No blanket "every data point traceable" claim
- [x] Exceptions listed explicitly (non-Tg labs, operative NLP, nuclear med)
