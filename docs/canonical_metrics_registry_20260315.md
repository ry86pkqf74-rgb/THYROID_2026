# Canonical Metrics Registry — 20260315

**Generated:** 2026-03-15T06:32:57.688065+00:00  
**Git SHA:** `e68c7f1`  
**Environment:** prod  
**Total metrics:** 36

## Governance Rules

1. **All manuscript-facing counts** must reference this registry by `metric_id`.
2. **Stale metrics** (>7 days since `last_verified_at`) trigger warnings in release gates.
3. **Unregistered metrics** in manuscript docs trigger CI warnings.
4. **Use tiers** control where a metric may appear:
   - `primary` — manuscript text, abstract, tables
   - `descriptive` — supplement, methods, internal dashboards
   - `sensitivity` — sensitivity analyses only (conditional use)
   - `prohibited` — known-bad value, must not be cited

## Metrics

### Cohort

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `total_surgical_patients` | Total Surgical Patients | **10,871** | primary | `path_synoptics` | — |
| `master_cohort_rows` | Master Cohort Rows | **11,673** | descriptive | `master_cohort` | — |
| `manuscript_cohort_size` | Manuscript Cohort Size | **10,871** | primary | `manuscript_cohort_v1` | — |
| `cancer_cohort_size` | Analysis-Eligible Cancer Cohort | **4,136** | primary | `analysis_cancer_cohort_v1` | — |
| `dedup_episodes` | Deduplicated Episodes | **9,368** | primary | `episode_analysis_resolved_v1_dedup` | — |
| `multi_surgery_patients` | Multi-Surgery Patients | **763** | primary | `tumor_episode_master_v2` | — |
| `multi_surgery_episodes` | Multi-Surgery Episodes | **819** | descriptive | `tumor_episode_master_v2` | — |
| `survival_cohort_enriched` | Survival Cohort (Enriched) | **61,134** | descriptive | `survival_cohort_enriched` | — |
| `master_clinical_v12_rows` | Master Clinical V12 Rows | **12,886** | descriptive | `patient_refined_master_clinical_v12` | — |

### Molecular

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `braf_positive` | BRAF Positive Patients | **546** | primary | `patient_refined_master_clinical_v12` | NLP false-positive correction applied (659→546); see reviewe… |
| `ras_positive` | RAS Positive Patients | **337** | primary | `patient_refined_master_clinical_v12` | Includes Phase 11 recovery + Phase 13 subtype resolution |
| `tert_positive` | TERT Positive Patients | **108** | primary | `patient_refined_master_clinical_v12` | — |
| `molecular_tested_patients` | Molecular-Tested Patients | **5,249** | descriptive | `extracted_fna_bethesda_v1` | — |

### Recurrence

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `recurrence_flagged` | Recurrence Any Flagged | **1,986** | primary | `extracted_recurrence_refined_v1` | — |
| `recurrence_exact_date` | Recurrence Exact-Date Tier | **54** | primary | `extracted_recurrence_refined_v1` | Only 2-3% of recurrence cases; rest unresolved — source limi… |
| `recurrence_biochem_inferred` | Recurrence Biochemical-Inferred | **168** | sensitivity | `extracted_recurrence_refined_v1` | Rising Tg > 1.0 and > 2x nadir; conditional for TTE analysis |
| `recurrence_unresolved` | Recurrence Unresolved Date | **1,764** | descriptive | `extracted_recurrence_refined_v1` | 88.8% — requires manual chart review; prioritized queue depl… |

### Rai

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `rai_episodes` | RAI Treatment Episodes | **1,857** | primary | `rai_treatment_episode_v2` | — |
| `rai_patients` | RAI Patients | **862** | primary | `rai_treatment_episode_v2` | — |
| `rai_with_dose` | RAI Episodes With Dose | **761** | primary | `rai_treatment_episode_v2` | 41% coverage — capped by absence of nuclear medicine notes (… |

### Complications

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `complication_any_patients` | Patients With Any Refined Complication | **287** | primary | `extracted_complications_refined_v5` | — |
| `rln_injury_total` | RLN Injury Total (Incl Suspected) | **92** | primary | `extracted_rln_injury_refined_v2` | — |

### Imaging

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `tirads_patients` | TIRADS-Scored Patients | **3,474** | primary | `extracted_tirads_validated_v1` | 32.5% fill rate; improved from 4.2% via Phase 12 |

### Linkage

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `imaging_fna_linkage_rows` | Imaging-FNA Linkage Rows | **9,024** | descriptive | `imaging_fna_linkage_v3` | — |
| `surgery_pathology_linkage` | Surgery→Pathology Linkage | **9,409** | descriptive | `surgery_pathology_linkage_v3` | — |
| `fna_molecular_linkage` | FNA→Molecular Linkage | **708** | descriptive | `fna_molecular_linkage_v3` | — |

### Labs

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `lab_canonical_rows` | Canonical Lab Rows | **39,961** | primary | `longitudinal_lab_canonical_v1` | — |
| `lab_canonical_patients` | Canonical Lab Patients | **3,349** | primary | `longitudinal_lab_canonical_v1` | — |

### Operative

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `operative_episodes` | Operative Episodes | **9,371** | descriptive | `operative_episode_detail_v2` | — |
| `operative_rln_monitoring` | Operative RLN Monitoring Flag | **1,702** | descriptive | `operative_episode_detail_v2` | FALSE = NOT_PARSED, not confirmed-negative |

### Scoring

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `ajcc8_calculable` | AJCC8 Calculable Patients | **4,083** | primary | `thyroid_scoring_py_v1` | 37.6% full-cohort; ~96.6% among cancer-eligible |
| `ata_calculable` | ATA 2015 Calculable Patients | **3,144** | primary | `thyroid_scoring_py_v1` | 28.9% full-cohort; ~76.0% among cancer-eligible |

### Review

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `adjudication_decisions` | Adjudication Decisions | **0** | descriptive | `adjudication_decisions` | — |

### Infrastructure

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `motherduck_table_count` | MotherDuck Table Count | **629** | descriptive | `information_schema.tables` | — |

### Demographics

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `demographics_age_coverage_pct` | Demographics Age Coverage % | **99.3** | descriptive | `demographics_harmonized_v2` | — |

### Provenance

| ID | Name | Value | Use Tier | Source Table | Limitation |
|---|---|---|---|---|---|
| `provenance_events` | Provenance Events | **50,297** | descriptive | `provenance_enriched_events_v1` | — |

## Cross-Source Discrepancy Resolution

| Metric | Old Registry (v1.csv) | Canonical (this registry) | Root Cause |
|---|---|---|---|
| BRAF+ | 376 (extracted_braf_recovery_v1) | **Uses patient_refined_master_clinical_v12** | NLP FP correction + multi-source recovery |
| RAS+ | 292 (extracted_molecular_refined_v1) | **Uses patient_refined_master_clinical_v12** | Phase 11+13 subtype resolution |
| TERT+ | 96 (extracted_molecular_refined_v1) | **Uses patient_refined_master_clinical_v12** | Subtype propagation from mol_test_episode_v2 |

## Staleness Policy

- Registry must be re-verified within **7 days** of any manuscript submission.
- Release manifests auto-check registry freshness; stale registry → WARN gate.
- Dashboard surfaces link to this registry; they must not hard-code counts.

*Registry hash: `882ff55ae7c7`*