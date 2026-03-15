# Dataset Truth Snapshot — 20260314

Generated: 2026-03-14T23:57:22.850888
Source: MotherDuck `thyroid_research_2026`

## 1. Core Dataset Metrics

| Metric | Value |
|--------|-------|
| Total Patients Patient Analysis | 10871 |
| Total Patients Path Synoptics | 10871 |
| Surgical Cohort Manuscript | 10871 |
| Analysis Cancer Cohort | 4136 |
| Episode Dedup Rows | 9368 |
| Scoring Table Rows | 10871 |
| Survival Cohort Enriched | 61134 |
| Recurrence Total Flagged | 1986 |
| Recurrence Date Exact | 54 |
| Recurrence Date Biochem | 168 |
| Recurrence Date Unresolved | 1764 |
| Rai Episodes Total | 1857 |
| Rai With Dose | 761 |
| Molecular Tested Patients | 10025 |
| Braf Positive Final | 546 |
| Ras Positive Final | 292 |
| Tert Positive | 96 |
| Tirads Patients | 3474 |
| Imaging Nodule Master Rows | 19891 |
| Imaging Fna Linkage Rows | 9024 |
| Lab Canonical Rows | 39961 |
| Lab Canonical Patients | 3349 |
| Tg Lab Patients | 2569 |
| Adjudication Decisions Rows | 0 |
| Complications Refined Rows | 358 |
| Complications Patients | 287 |
| Master Clinical V12 Rows | 12886 |
| Demographics Harmonized Rows | 11673 |
| Motherduck Table Count | 605 |

## 2. Recurrence Resolution Tiers

| Tier | Count |
|------|-------|
| Total flagged | 1986 |
| Exact source date | 54 |
| Biochemical inferred | 168 |
| Unresolved | 1764 |

Recurrence review packets exported: 1986 cases → `exports/recurrence_review_packets/`

## 3. RAI Dose Coverage

- Total RAI episodes: 1857
- With dose: 761
- Coverage: **41.0%**

## 4. Operative NLP Field Coverage

| Field | Upstream | Upstream % | Downstream | Downstream % | Status |
|-------|----------|-----------|------------|-------------|--------|
| rln_monitoring_flag | 1702 | 18.2% | 0 | 0.0% | PIPELINE_LIMITED: present upstream but not materialized in analytic table |
| rln_finding_raw | 371 | 4.0% | 0 | 0.0% | PIPELINE_LIMITED: present upstream but not materialized in analytic table |
| parathyroid_autograft_flag | 40 | 0.4% | 40 | 0.4% | OK |
| gross_ete_flag | 22 | 0.2% | 0 | 0.0% | PIPELINE_LIMITED: present upstream but not materialized in analytic table |
| local_invasion_flag | 25 | 0.3% | 25 | 0.3% | OK |
| tracheal_involvement_flag | 9 | 0.1% | 9 | 0.1% | OK |
| esophageal_involvement_flag | 0 | 0.0% | 0 | 0.0% | SOURCE_LIMITED: 0% upstream — extractor output never materialized or entity type not in NLP vocab |
| strap_muscle_involvement_flag | 186 | 2.0% | 186 | 1.9% | OK |
| reoperative_field_flag | 46 | 0.5% | 49 | 0.5% | OK |
| drain_flag | 169 | 1.8% | 0 | 0.0% | PIPELINE_LIMITED: present upstream but not materialized in analytic table |
| operative_findings_raw | 588 | 6.3% | 594 | 6.2% | OK |
| parathyroid_identified_count | 0 | 0.0% | N/A | N/A% | NOT_PROPAGATED: no downstream analytic table target |
| frozen_section_flag | 0 | 0.0% | N/A | N/A% | NOT_PROPAGATED: no downstream analytic table target |
| berry_ligament_flag | 0 | 0.0% | N/A | N/A% | NOT_PROPAGATED: no downstream analytic table target |
| ebl_ml_nlp | 0 | 0.0% | N/A | N/A% | NOT_PROPAGATED: no downstream analytic table target |
| op_rln_monitoring_any | 1701 | 15.6% | N/A | N/A% | patient_agg |
| op_drain_placed_any | 169 | 1.6% | N/A | N/A% | patient_agg |
| op_strap_muscle_any | 186 | 1.7% | N/A | N/A% | patient_agg |
| op_reoperative_any | 46 | 0.4% | N/A | N/A% | patient_agg |
| op_parathyroid_autograft_any | 40 | 0.4% | N/A | N/A% | patient_agg |
| op_local_invasion_any | 25 | 0.2% | N/A | N/A% | patient_agg |
| op_tracheal_inv_any | 9 | 0.1% | N/A | N/A% | patient_agg |
| op_esophageal_inv_any | 0 | 0.0% | N/A | N/A% | patient_agg |
| op_intraop_gross_ete_any | 22 | 0.2% | N/A | N/A% | patient_agg |
| op_n_surgeries_with_findings | 8733 | 80.3% | N/A | N/A% | patient_agg |
| op_findings_summary | 587 | 5.4% | N/A | N/A% | patient_agg |

## 5. Lab Coverage

- Canonical lab rows: 39961
- Canonical lab patients: 3349
- Tg lab patients: 2569

## 6. Imaging & Linkage

- Imaging nodule master rows: 19891
- TIRADS patients: 3474
- Imaging-FNA linkage rows: 9024

## 7. Adjudication & Review

- Adjudication decisions: 0
- Complications refined rows: 358
- Patients w/ any complication: 287

## 8. Documentation Reconciliation

| Document | Metric | Documented | Live | Action |
|----------|--------|-----------|------|--------|
| README.md | MotherDuck tables | 578 | 605 | **Updated** |
| MANUSCRIPT_CAVEATS | BRAF positive | 376 | 546 | Stale — pre-NLP-recovery number |
| source_limited_field_registry | BRAF positive | 441 | 546 | Stale — pre-FP-correction number |
| reviewer_gap_FAQ | BRAF positive | 546 | 546 | ✓ Correct |
| statistical_analysis_plan | BRAF positive | 376 | 546 | Stale — same as MANUSCRIPT_CAVEATS |

### BRAF Count Provenance

| Source Table | Count | Description |
|---|---|---|
| `patient_refined_master_clinical_v12.braf_positive_final` | **546** | **CANONICAL** — structured + confirmed NLP |
| `extracted_braf_recovery_v1` WHERE positive | 730 | Multi-source rows (duplicates per patient) |
| `extracted_molecular_panel_v1.braf_positive` | 266 | Structured panel-only |
| `extracted_molecular_refined_v1.braf_positive_refined` | 266 | Same structured set |

**Action:** Updated `MANUSCRIPT_CAVEATS_20260313.md` and `statistical_analysis_plan_thyroid_manuscript.md` BRAF references from 376 → 546.

## 9. Source-Limited Fields (Cannot Improve Without New Data)

| Field | Current Coverage | Limitation |
|-------|-----------------|------------|
| Non-Tg lab dates (TSH/PTH/Ca) | 0% | Requires institutional lab extract |
| Nuclear medicine notes | 0 notes | Not in clinical_notes_long corpus |
| Vascular invasion grading | 87% ungraded | Synoptic template uses 'x' only |
| Recurrence dates | 88.8% unresolved | Requires manual chart review |
| Pre-2019 operative notes | absent | Institutional data limitation |

## 10. Pipeline-Limited Fields (Fixable With Engineering)

| Field | Current Coverage | Limitation |
|-------|-----------------|------------|
| parathyroid_identified_count | >0 upstream | Not propagated to episode table |
| frozen_section_flag | 0% upstream | Entity type not in NLP vocabulary |
| berry_ligament_flag | 0% upstream | Entity type not in NLP vocabulary |
| ebl_ml_nlp | 0% upstream | Entity type not in NLP vocabulary |
| esophageal_involvement_flag | 0% upstream | 0 entities in NLP corpus |

---
*Snapshot generated by `scripts/94_dataset_truth_snapshot.py` on 2026-03-14T23:57:22.850888*