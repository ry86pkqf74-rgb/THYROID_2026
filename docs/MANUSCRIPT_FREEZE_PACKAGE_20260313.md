# Manuscript Freeze Package

**Date:** 2026-03-13
**Git SHA:** e1e8897
**Status:** FROZEN — All readiness gates PASS
**Verdict:** B — READY WITH SCOPED CAVEATS

---

## Purpose

This document serves as the master index for the manuscript freeze package. It links all
components required for deterministic manuscript writing and reviewer defense.

---

## Package Contents

### 1. Frozen Cohort Data

| Artifact | Location | Rows | Key |
|----------|----------|------|-----|
| Manuscript cohort | `exports/manuscript_cohort_freeze/manuscript_cohort_v1.csv` | 10,871 | research_id (unique) |
| Cohort flow | `exports/manuscript_cohort_freeze/manuscript_cohort_flow_v1.csv` | 8 steps | — |
| Episode dedup | `exports/FINAL_PUBLICATION_BUNDLE_20260313/episode_dedup_v1.csv` | 9,368 | surgery_episode_id (unique) |
| Master clinical | `exports/FINAL_PUBLICATION_BUNDLE_20260313/master_clinical_v12.csv` | 12,886 | research_id (NOT unique — multi-path) |

**Canonical manuscript spine:** `manuscript_cohort_v1` (10,871 unique patients, 139 columns).

### 2. Manuscript Tables

| Table | File | Formats | Denominator |
|-------|------|---------|-------------|
| Table 1: Demographics | `exports/manuscript_tables/table1_demographics.*` | CSV, MD, TEX | 10,871 (full), 4,136 (eligible) |
| Table 2: Tumor & Treatment | `exports/manuscript_tables/table2_tumor_treatment.*` | CSV, MD, TEX | 4,136 (eligible) |
| Table 3: Outcomes | `exports/manuscript_tables/table3_outcomes.*` | CSV, MD, TEX | 4,136 (eligible) |
| Supp: Missingness | `exports/manuscript_tables/supplementary_missingness.*` | CSV, MD, TEX | 10,871 |
| Supp: Cohort Flow | `exports/manuscript_tables/cohort_flow.*` | CSV, MD, TEX | — |
| Cox PH | `exports/manuscript_tables/cox_ph.*` | MD, TEX | 3,201 (survival) |
| Logistic Models | `exports/manuscript_tables/logistic_models.*` | MD, TEX | 4,136 (eligible) |

### 3. Manuscript Figures

| Figure | File | Resolution | Source Data |
|--------|------|------------|-------------|
| Fig 1: Cohort Flow | `exports/manuscript_figures/fig1_cohort_flow.{png,svg}` | 300 DPI | cohort_flow.csv |
| Fig 2: KM by AJCC8 | `exports/manuscript_figures/fig2_km_ajcc8.{png,svg}` | 300 DPI | km_curve_data.csv |
| Fig 3: Stage Distribution | `exports/manuscript_figures/fig3_stage_risk_distribution.{png,svg}` | 300 DPI | table2 + DB |
| Fig 4: Mutation Spectrum | `exports/manuscript_figures/fig4_mutation_spectrum.{png,svg}` | 300 DPI | manuscript_cohort_v1 |
| Fig 5: Complication Rates | `exports/manuscript_figures/fig5_complication_rates.{png,svg}` | 300 DPI | manuscript_cohort_v1 |

### 4. Metric Registry (Single Source of Truth)

| File | Format | Metrics |
|------|--------|---------|
| `exports/manuscript_metric_registry_20260313/manuscript_metric_registry_v1.csv` | CSV | 25 |
| `exports/manuscript_metric_registry_20260313/manuscript_metric_registry_v1.json` | JSON | 25 |
| `exports/manuscript_metric_registry_20260313/manuscript_metric_registry_v1.md` | Markdown | 25 |

**Every metric cited in the manuscript MUST match a row in this registry.**

### 5. Caveat Pack

| Document | Purpose |
|----------|---------|
| `docs/MANUSCRIPT_CAVEATS_20260313.md` | 8 pre-written caveats with Methods/Limitations/Discussion wording |

Caveats covered:
1. Source-limited non-Tg lab dates
2. Recurrence date sparsity (88.8% unresolved)
3. Nuclear medicine text absence
4. Partial clinical note coverage (~50%)
5. Vascular invasion present_ungraded
6. Operative detail boolean defaults
7. BRAF prevalence context
8. Scoring system calculability

### 6. Supplementary Data Quality Appendix

| Document | Purpose |
|----------|---------|
| `docs/SUPPLEMENT_DATA_QUALITY_APPENDIX_20260313.md` | Journal supplement covering cohort derivation, linkage, date taxonomy, coverage, gaps, validation |

Sections: Cohort Derivation, Data Sources and Linkage, Date Quality Taxonomy, Domain Coverage,
Remaining Structural Gaps, Validation Gates, NLP Extraction Quality.

### 7. Reviewer Defense Snapshots

| File | Question Addressed |
|------|-------------------|
| `docs/reviewer_defense_20260313/01_duplicate_prevention.md` | How were duplicates prevented? |
| `docs/reviewer_defense_20260313/02_recurrence_definition_dating.md` | How was recurrence defined and dated? |
| `docs/reviewer_defense_20260313/03_operative_detail_sourcing.md` | How were operative details sourced? |
| `docs/reviewer_defense_20260313/04_imaging_tirads_completeness.md` | How complete were imaging/TIRADS data? |
| `docs/reviewer_defense_20260313/05_rai_receipt_dose.md` | How were RAI receipt and dose established? |
| `docs/reviewer_defense_20260313/06_notes_vs_structured_sourcing.md` | Which variables came from notes vs structured fields? |

### 8. Readiness Assessment

| File | Status |
|------|--------|
| `exports/FINAL_PUBLICATION_BUNDLE_20260313/readiness_assessment.json` | ALL 7 GATES PASS |

---

## Canonical Denominators

| Population | N | Label | Used In |
|-----------|---|-------|---------|
| Full surgical cohort | 10,871 | `full_surgical_cohort` | Table 1, supplement |
| Analysis-eligible cancer | 4,136 | `analysis_eligible_cancer` | Tables 2-3, Figs 2-5 |
| Molecular-tested | 10,025 | `molecular_tested` | BRAF/RAS/TERT rates |
| Survival cohort | 3,201 | `survival_cohort` | KM, Cox PH |
| Vascular positive | 3,846 | `vascular_positive` | WHO 2022 grading |
| RAI episodes | 1,857 | `rai_episodes` | RAI coverage |

---

## One-Command Rebuild

```bash
# Validate only (dry run):
.venv/bin/python scripts/90_manuscript_freeze_rebuild.py --md --dry-run

# Full rebuild:
.venv/bin/python scripts/90_manuscript_freeze_rebuild.py --md
```

The rebuild script:
1. Validates all critical source tables (fail-closed on missing/drifted row counts)
2. Checks uniqueness constraints on patient and episode tables
3. Regenerates Tables 1-3, figures, and analysis outputs
4. Exports metric registry, documentation, and reviewer defense materials
5. Writes readiness assessment and creates ZIP bundle
6. Exits non-zero if any critical validation fails

---

## Freeze Verification Summary

### Row Counts (verified 2026-03-13)

| Table | Expected | Status |
|-------|----------|--------|
| manuscript_cohort_v1 | 10,871 | PASS — 0 duplicates |
| episode_analysis_resolved_v1_dedup | 9,368 | PASS — 0 duplicates |
| patient_analysis_resolved_v1 | 10,871 | PASS — 0 duplicates |
| thyroid_scoring_py_v1 | 10,871 | PASS |
| analysis_cancer_cohort_v1 | 4,136 | PASS |
| analysis_molecular_subset_v1 | 10,025 | PASS |
| analysis_tirads_subset_v1 | 3,474 | PASS |
| analysis_recurrence_subset_v1 | 1,946 | PASS |
| complication_patient_summary_v1 | 2,892 | PASS |
| longitudinal_lab_clean_v1 | 38,699 | PASS |
| recurrence_event_clean_v1 | 1,946 | PASS |
| lesion_analysis_resolved_v1 | 11,851 | PASS |

### Upstream Dependencies

All frozen outputs derive from versioned tables with `resolved_layer_version='v1'` and
`freeze_git_sha='e1e8897'`. No mutable views are used in the final bundle — all data
is snapshot-frozen as tables, not views.

---

## Known Limitations (Documented, Not Blocking)

1. RAI dose: 41% of episodes (nuclear medicine notes absent)
2. Recurrence dates: 88.8% unresolved (structured flag only)
3. Non-Tg labs: 0% structured collection dates
4. Vascular invasion: 78.7% present_ungraded
5. Clinical notes: ~50% patient coverage
6. Operative booleans: 10 fields at default FALSE (not parsed)
7. BRAF prevalence: 3.8% (denominator includes benign)

All limitations have pre-written caveat language in `MANUSCRIPT_CAVEATS_20260313.md`.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| v1.0 | 2026-03-13 | Initial freeze package |
