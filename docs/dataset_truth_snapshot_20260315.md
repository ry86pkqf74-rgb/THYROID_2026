# Dataset Truth Snapshot — 20260315

Generated: 2026-03-15T02:02:08.682542
Source: MotherDuck `thyroid_research_2026` (prod)
Script: `scripts/99_comprehensive_final_verification.py`

## 1. Core Dataset Metrics

| Metric | Value |
|--------|-------|
| Total Patients | 10871 |
| Surgical Cohort | 10871 |
| Analysis Cancer Cohort | 4136 |
| Manuscript Cohort | 10871 |
| Episode Dedup | 9368 |
| Scoring Rows | 10871 |
| Survival Cohort Enriched | 61134 |
| Multi Surg Patients | 761 |
| Multi Surg Episodes | 1576 |
| Recurrence Flagged | 1986 |
| Recurrence Exact | 54 |
| Recurrence Biochem | 168 |
| Recurrence Unresolved | 1764 |
| Rai Episodes | 1857 |
| Rai With Dose | 761 |
| Molecular Tested | 10025 |
| Braf Positive | 546 |
| Ras Positive | 292 |
| Tert Positive | 96 |
| Tirads Patients | 3474 |
| Imaging Nodule Rows | 19891 |
| Imaging Fna Linkage | 9024 |
| Lab Canonical Rows | 39961 |
| Lab Canonical Patients | 3349 |
| Tg Patients | 2569 |
| Adjudication Decisions | 0 |
| Adjudication Progress | 0 |
| Complications Refined | 358 |
| Complications Patients | 287 |
| Master V12 Rows | 12886 |
| Demographics Rows | 11673 |
| Md Table Count | 629 |
| Op Rln Monitoring | 1702 |
| Op Drain | 169 |
| Op Findings Raw | 588 |
| Op Episodes Total | 9371 |
| Ep Note Linkage | 8323 |
| Ep Lab Linkage | 12057 |
| Ep Chain Linkage | 5088 |
| Ep Pathrai Linkage | 2465 |
| Ep Ambiguity Registry | 2339 |

## 2. Multi-Surgery Episode Integrity

- Multi-surgery patients: **761**
- Multi-surgery episodes: **1576**
- High-risk review queue: **334** patients (HIGH=0, MEDIUM=334)

### Surgery Count Distribution

| Surgeries | Patients |
|-----------|----------|
| 2 | 719 |
| 3 | 33 |
| 4 | 7 |
| 5 | 1 |
| 6 | 1 |

### Artifact Linkage by Domain

| Domain | Table | MS Artifacts | Uniquely Linked | Ambiguous | Distant | Unlinked | No Date |
|--------|-------|-------------|----------------|-----------|---------|----------|---------|
| pathology | path_synoptics | 1576 | 1449 | 126 | 0 | 0 | 1 |
| operative | operative_episode_detail_v2 | 624 | 561 | 59 | 1 | 0 | 2 |
| fna | fna_episode_master_v2 | 5353 | 92 | 43 | 325 | 95 | 4781 |
| molecular | molecular_test_episode_v2 | 715 | 6 | 9 | 71 | 8 | 621 |
| rai | rai_treatment_episode_v2 | 436 | 65 | 12 | 64 | 64 | 119 |
| imaging_us | raw_us_tirads_excel_v1 | 1348 | 0 | 0 | 0 | 0 | 0 |
| lab_tg | thyroglobulin_labs | 7973 | 221 | 506 | 1444 | 3939 | 0 |
| lab_canonical | longitudinal_lab_canonical_v1 | 10304 | 227 | 506 | 1447 | 3946 | 93 |
| notes | clinical_notes_long | 1185 | 420 | 28 | 34 | 119 | 485 |

### Episode Key Propagation

| Table | Column | MS Rows | Distinct IDs | Status |
|-------|--------|---------|-------------|--------|
| operative_episode_detail_v2 | surgery_episode_id | 624 | 3 | CORRECT |
| episode_analysis_resolved_v1_dedup | surgery_episode_id | 622 | 3 | CORRECT |
| tumor_episode_master_v2 | surgery_episode_id | 1577 | 6 | CORRECT |
| episode_note_linkage_repair_v1 | surgery_episode_id | N/A | N/A | COLUMN_MISSING |
| episode_lab_linkage_repair_v1 | surgery_episode_id | N/A | N/A | COLUMN_MISSING |
| episode_chain_linkage_repair_v1 | surgery_episode_id | N/A | N/A | COLUMN_MISSING |
| episode_pathrai_linkage_repair_v1 | surgery_episode_id | N/A | N/A | COLUMN_MISSING |

### V3 Linkage Table Health

| Table | Total | Weak | Has Score |
|-------|-------|------|-----------|
| surgery_pathology_linkage_v3 | 9409 | 566 | True |
| pathology_rai_linkage_v3 | 23 | 0 | True |
| fna_molecular_linkage_v3 | 708 | 29 | True |
| preop_surgery_linkage_v3 | 3591 | 2045 | True |

## 3. Recurrence Resolution Tiers

| Tier | Count | % |
|------|-------|---|
| unresolved_date | 2557 | 84.3% |
| biochemical_inflection_inferred | 303 | 10.0% |
| exact_source_date | 172 | 5.7% |

Multi-surgery recurrence patients: 1362
Recurrence review packets: 3032 cases → `exports/recurrence_review_packets/`

## 4. RAI Dose Coverage

- Episodes: 1857
- With dose: 761
- Coverage: **41.0%**
- Source limitation: nuclear medicine notes absent from clinical_notes_long

## 5. Operative NLP Field Coverage

| Field | Upstream | Upstream % | Status |
|-------|----------|-----------|--------|
| rln_monitoring_flag | 1702 | 18.2% | PIPELINE_GAP |
| rln_finding_raw | 371 | 4.0% | PIPELINE_GAP |
| parathyroid_autograft_flag | 40 | 0.4% | OK |
| gross_ete_flag | 22 | 0.2% | PIPELINE_GAP |
| local_invasion_flag | 25 | 0.3% | OK |
| tracheal_involvement_flag | 9 | 0.1% | OK |
| esophageal_involvement_flag | 0 | 0.0% | SOURCE_LIMITED |
| strap_muscle_involvement_flag | 186 | 2.0% | OK |
| reoperative_field_flag | 46 | 0.5% | OK |
| drain_flag | 169 | 1.8% | PIPELINE_GAP |
| operative_findings_raw | 588 | 6.3% | OK |
| parathyroid_identified_count | 0 | 0.0% | SOURCE_LIMITED |
| frozen_section_flag | 0 | 0.0% | SOURCE_LIMITED |
| berry_ligament_flag | 0 | 0.0% | SOURCE_LIMITED |
| ebl_ml_nlp | 0 | 0.0% | SOURCE_LIMITED |

### Patient-Level Aggregates

| Field | Count | % |
|-------|-------|---|
| op_rln_monitoring_any | 1701 | 15.6% |
| op_drain_placed_any | 169 | 1.6% |
| op_strap_muscle_any | 186 | 1.7% |
| op_reoperative_any | 46 | 0.4% |
| op_parathyroid_autograft_any | 40 | 0.4% |
| op_local_invasion_any | 25 | 0.2% |
| op_tracheal_inv_any | 9 | 0.1% |
| op_esophageal_inv_any | 0 | 0.0% |
| op_intraop_gross_ete_any | 22 | 0.2% |
| op_n_surgeries_with_findings | 8733 | 80.3% |
| op_findings_summary | 587 | 5.4% |

## 6. Lab Coverage

- Canonical lab rows: 39961
- Canonical lab patients: 3349
- Tg lab patients: 2569

## 7. Imaging & Linkage

- Imaging nodule rows: 19891
- TIRADS patients: 3474
- Imaging-FNA linkage: 9024

## 8. Adjudication & Complications

- Adjudication decisions: 0
- Adjudication progress entries: 0
- Complications refined: 358
- Complication patients: 287

## 9. Episode Linkage Repair Tables

- Notes linkage: 8323 rows
- Lab linkage: 12057 rows
- Chain linkage: 5088 rows
- Pathology/RAI linkage: 2465 rows
- Ambiguity registry: 2339 rows

## 10. Documentation Reconciliation

| Doc | Metric | Documented | Live |
|-----|--------|-----------|------|
| docs/MANUSCRIPT_CAVEATS_20260313.md | recurrence unresolved | 88.8% | 89% |

## 11. Source-Limited Fields

| Field | Coverage | Limitation |
|-------|---------|------------|
| Non-Tg lab dates (TSH/PTH/Ca) | 0% | Institutional lab extract needed |
| Nuclear medicine notes | 0 | Not in clinical_notes_long |
| Vascular invasion grading | 87% ungraded | Synoptic 'x' placeholder |
| Recurrence dates | ~89% unresolved | Manual chart review or registry needed |
| Esophageal involvement | 0% | No NLP entities extracted |
| Frozen section / Berry ligament | 0% | Entity type not in NLP vocab |
| Imaging-FNA size matching | 0% | imaging_nodule_long_v2 size columns empty |

---
*Generated by `scripts/99_comprehensive_final_verification.py` on 2026-03-15T02:02:08.682542*