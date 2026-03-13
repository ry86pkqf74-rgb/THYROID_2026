# Streamlit Wiring & Deployment Verification

**Date:** 2026-03-13
**App Version:** v3.2.0-2026.03.13

---

## Connection Architecture

| Mode | Target | Usage |
|------|--------|-------|
| RO Share | `md:_share/thyroid_research_ro/7962a053-...` | Default for all read tabs |
| RW Direct | `md:thyroid_research_2026` | Fallback + Review Mode writes |
| Local DuckDB | `thyroid_master_local.duckdb` | Development only |

Connection flow: `_get_con()` → `_ensure_token()` → try RO share → fallback RW →
`_ACTIVE_CATALOG` set for `qual()` table resolution.

---

## Tab-to-Table Dependency Verification

### Section 1: Overview

| Table | Exists | Rows |
|-------|:------:|------|
| overview_kpis | ✓ | 1 |
| master_cohort | ✓ | 11,673 |
| tumor_pathology | ✓ | 4,290 |
| complications | ✓ | 10,864 |
| date_rescue_rate_summary | ✓ | 7 |
| manuscript_cohort_v1 | ✓ | 10,871 |
| val_episode_linkage_completeness_v1 | ✓ | 5 |
| val_provenance_completeness_v2 | ✓ | 23 |

### Section 2: Patient Explorer

| Table | Exists | Rows |
|-------|:------:|------|
| streamlit_patient_header_v | ✓ | 11,977 |
| streamlit_patient_timeline_v | ✓ | 24,378 |
| streamlit_patient_conflicts_v | ✓ | 1,015 (CREATED THIS SESSION) |
| streamlit_patient_manual_review_v | ✓ | 7,552 (CREATED THIS SESSION) |
| advanced_features_sorted | ✓ | 16,062 |

### Section 3: Data Quality & Provenance

| Table | Exists | Rows |
|-------|:------:|------|
| val_dataset_integrity_summary_v1 | ✓ | 30 |
| val_provenance_completeness_v2 | ✓ | 23 |
| val_episode_linkage_completeness_v1 | ✓ | 5 |
| val_lab_completeness_v1 | ✓ | 14 |
| val_temporal_anomaly_resolution_v1 | ✓ | (exists) |
| validation_failures_v3 | ✓ | 2,505 |
| qa_issues_v2 | ✓ | 6,483 |
| qa_summary_by_domain_v2 | ✓ | 8 |

### Section 4: Linkage & Episodes

| Table | Exists | Rows |
|-------|:------:|------|
| tumor_episode_master_v2 | ✓ | 11,691 |
| molecular_test_episode_v2 | ✓ | 10,126 |
| rai_treatment_episode_v2 | ✓ | 1,857 |
| imaging_nodule_master_v1 | ✓ | 19,891 |
| operative_episode_detail_v2 | ✓ | 9,371 |
| fna_episode_master_v2 | ✓ | 59,620 |
| master_timeline | ✓ | 12,325 |
| extracted_clinical_events_v4 | ✓ | 29,319 |

### Section 5: Outcomes & Analytics

| Table | Exists | Rows |
|-------|:------:|------|
| survival_cohort_ready_mv | ✓ | 11,175 |
| survival_cohort_enriched | ✓ | 61,134 |
| recurrence_risk_features_mv | ✓ | 4,976 |
| risk_enriched_mv | ✓ | 6,630 |
| patient_level_summary_mv | ✓ | 11,977 |

### Section 6: Manuscript & Export

| Table | Exists | Rows |
|-------|:------:|------|
| genetic_testing | ✓ | 11,374 |
| molecular_testing | ✓ | 10,126 |
| nuclear_med | ✓ | 2,220 |
| path_synoptics | ✓ | 11,688 |
| adjudication_progress_summary_v | ✓ | 0 (CREATED THIS SESSION, placeholder) |
| streamlit_cohort_qc_summary_v | ✓ | 1 |

---

## Tables Created This Session

| Table | Rows | Source | Reason |
|-------|------|--------|--------|
| streamlit_patient_conflicts_v | 1,015 | histology/molecular analysis cohorts | Previously missing |
| streamlit_patient_manual_review_v | 7,552 | histology manual review queue | Previously missing |
| adjudication_progress_summary_v | 0 | Placeholder (no decisions) | Previously missing |

---

## Deployment Configuration

- `.streamlit/config.toml`: headless=true, enableCORS=false, dark theme
- `.streamlit/secrets.toml`: MOTHERDUCK_TOKEN present (gitignored)
- `.streamlit/secrets.toml.example`: Template in repo

## Smoke Test Checklist

- [x] RO share connection successful
- [x] RW fallback connection successful
- [x] All 26 key Streamlit tables verified present
- [x] 3 previously missing tables created and verified
- [x] ANALYZE run on all critical tables
- [x] No stale table references found in active tab code
- [x] Review Mode toggle correctly creates separate RW connection
- [x] Version string matches: v3.2.0-2026.03.13
