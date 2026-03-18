# Analysis Lineage — THYROID_2026 ETE Staging Manuscript
## Generated: 2026-03-18

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     RAW DATA SOURCES                            │
│                                                                 │
│  Excel files → scripts/01_ingest.py                            │
│    • All Diagnoses & synoptic 12_1_2025.xlsx                   │
│    • Notes 12_1_25.xlsx                                         │
│    • Thyroid OP Sheet data.xlsx                                  │
│    • THYROSEQ_AFIRMA_12_5.xlsx                                 │
│    • Imaging_12_1_25.xlsx                                       │
│    • Nuclear_Med_final.xlsx                                     │
│    • US Nodules TIRADS 12_1_25.xlsx                             │
│                                                                 │
│  Output → DuckDB tables (local + MotherDuck):                  │
│    path_synoptics, tumor_pathology, clinical_notes_long,        │
│    operative_details, molecular_testing, serial_imaging_us,     │
│    benign_pathology, fna_cytology, genetic_testing              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               CANONICAL VIEWS (scripts/03_research_views.py)    │
│                                                                 │
│  ptc_cohort (VIEW):                                            │
│    SELECT FROM path_synoptics ps                                │
│    JOIN tumor_pathology tp ON ps.research_id = tp.research_id   │
│    WHERE tp.histology_1_type = 'PTC'                           │
│                                                                 │
│  recurrence_risk_cohort (VIEW):                                │
│    SELECT FROM ptc_cohort                                       │
│    + risk band derivation + staging                             │
│                                                                 │
│  patient_level_summary_mv (TABLE):                             │
│    Materialized demographics + first surgery + histology        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│    MATERIALIZED FEATURES (scripts/10_materialized_features.py)  │
│                                                                 │
│  recurrence_risk_features_mv (TABLE):                          │
│    Per-patient risk features: recurrence_flag, tg labs,         │
│    ln_positive, ln_examined, braf_positive, ras_positive,       │
│    recurrence_risk_band                                         │
│    N ≈ 4,976 rows (up to 25 rows/patient before dedup)         │
│                                                                 │
│  survival_cohort_ready_mv (TABLE):                             │
│    time_to_event_days, event_occurred, age_at_diagnosis,        │
│    sex, overall_stage_ajcc8, histology                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  ENRICHED VIEW (scripts/13_performance_optimizations_pack.py)   │
│                                                                 │
│  risk_enriched_mv (TABLE):                                     │
│    SELECT *                                                     │
│    FROM recurrence_risk_features_mv r                           │
│    LEFT JOIN survival_cohort_ready_mv s                         │
│      ON r.research_id = s.research_id                          │
│    -- Unified table: demographics + tumor + outcomes + survival  │
│    -- N ≈ 5,794                                                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────┼────────────────┐
              │            │                │
              ▼            ▼                ▼
┌──────────────────┐┌──────────────────┐┌──────────────────────────┐
│ CSV EXPORT       ││ CSV EXPORT       ││ DIRECT SQL QUERY         │
│ (primary)        ││ (expanded)       ││ (Cox PH / KM)            │
│                  ││                  ││                          │
│ ptc_full.csv     ││ ptc_full.csv     ││ proposal2_cox_           │
│ recurrence_      ││ + recurrence_    ││   regression.py          │
│   full.csv       ││   full.csv       ││ Queries risk_enriched_mv │
│ imaging_         ││ + imaging_       ││ directly via DuckDB      │
│   correlation.csv││   corr.csv       ││ N ≈ 5,794                │
│                  ││                  ││                          │
│ → load_data()    ││ → load_all_ptc() ││                          │
│ → Filter classic ││ → All PTC        ││                          │
│                  ││                  ││                          │
│ analytic_        ││ analytic_cohort_ ││                          │
│ cohort.csv       ││ expanded.csv     ││                          │
│ N=596            ││ N=3,278          ││                          │
└────────┬─────────┘└────────┬─────────┘└─────────┬────────────────┘
         │                   │                    │
         ▼                   ▼                    ▼
┌──────────────────────────────────────────────────────────────────┐
│                   ANALYSIS SCRIPTS                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ proposal2_ete_analysis.py (PRIMARY, N=596)              │    │
│  │ → Table 1: Demographics by ETE group                    │    │
│  │ → Table 3: Stage migration (McNemar)                    │    │
│  │ → Table 4: Ordinal regression (CC)                      │    │
│  │ → Figure 2-4: Stage distribution, waterfall             │    │
│  │ → AUC cross-validation (5-fold)                         │    │
│  │ → Classic variant subgroup                              │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ proposal2_endpoint_psm_strata.py (PSM, N=2,460→711)    │    │
│  │ → PSM: mETE vs NoETE (exclude Gross)                   │    │
│  │ → Table 6: PSM effect + balance                         │    │
│  │ → Table 7: Size-stratified logistic                     │    │
│  │ → Table 8: Interaction tests (×size, ×age, ×nodal)     │    │
│  │ → Figure 10: Matched DFS KM curves                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ proposal2_cox_regression.py (Cox, N≈5,794)             │    │
│  │ → Table 3 addendum: Cox PH coefficients                 │    │
│  │ → Figure 10: KM by risk band                            │    │
│  │ → Figure 11: KM by ETE status                           │    │
│  │ → Concordance index                                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ proposal2_expanded_cohort.py (Expanded, N=3,278)       │    │
│  │ → Cohort A/B/C/D ordinal regressions                    │    │
│  │ → Multiple imputation (m=20, PMM, Rubin's rules)       │    │
│  │ → Aggressive variant safety analysis                     │    │
│  │ → Stage migration (expanded)                             │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ proposal2_recommendations.py (Sensitivity, N=596)      │    │
│  │ → Table 5: Sensitivity analysis grid                     │    │
│  │ → Figure 6: Forest plot                                  │    │
│  │ → Figure 7: KM by ETE                                    │    │
│  │ → MI ordinal (primary cohort)                            │    │
│  │ → Subgroup: age ≥55, age <55, tumor ≤4cm                │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ audit_reproduce.py (Independent Audit, all cohorts)    │    │
│  │ → Re-runs all major analyses independently              │    │
│  │ → Corrects T3b→T4a mapping to T3b→T3 (346 patients)   │    │
│  │ → Validates PSM, ordinal, stage migration results       │    │
│  └─────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                  MANUSCRIPT OUTPUT                                │
│                                                                  │
│  studies/proposal2_ete_staging/tables/                           │
│    table1_demographics.csv                                       │
│    table2_ete_distribution.csv                                   │
│    table3_stage_migration.csv                                    │
│    table4_ordinal_regression.csv                                 │
│    table5_sensitivity.csv                                        │
│                                                                  │
│  studies/proposal2_ete_staging/audit_tables/                    │
│    table6_propensity_matching_effect.csv                         │
│    table6_propensity_matching_balance.csv                        │
│    table7_stratified_models.csv                                  │
│    table8_interaction_tests.csv                                  │
│    table9_structural_endpoint.csv                                │
│                                                                  │
│  studies/proposal2_ete_staging/figures/                          │
│    fig1-fig11 (stage distribution, waterfall, ROC, forest, KM)  │
│                                                                  │
│  studies/manuscript_draft/manuscript_v1.md                       │
│    → Final manuscript text (Abstract–References)                 │
└──────────────────────────────────────────────────────────────────┘
```

---

## Research ID Linkage Path

```
path_synoptics.research_id (INTEGER, primary key across all tables)
  → ptc_cohort.research_id
    → recurrence_risk_features_mv.research_id
      → risk_enriched_mv.research_id
        → CSV exports (research_id column preserved)
          → analytic_cohort.csv / analytic_cohort_expanded.csv
            → Final manuscript analyses (all keyed by research_id)
```

**Linkage is 1:1 at the patient level.** No crosswalk table is needed — `research_id` is the native primary key throughout the pipeline.

**Exception:** `recurrence_risk_features_mv` can have up to 25 rows per research_id. The expanded cohort construction deduplicates via `GROUP BY research_id` with `BOOL_OR(recurrence_flag)` and `MAX(tg_max)`.

---

## Authoritative File Inventory

| Artifact | Path | Role | Authoritative |
|----------|------|------|---------------|
| analytic_cohort.csv | studies/proposal2_ete_staging/tables/ | Primary classic PTC (N=596) | **YES** |
| analytic_cohort_expanded.csv | studies/proposal2_ete_staging/audit_tables/ | Expanded all PTC (N=3,278) | **YES** |
| analysis_metadata.yaml | studies/proposal2_ete_staging/ | Full run provenance | **YES** |
| proposal2_ete_analysis.py | studies/proposal2_ete_staging/ | Primary ordinal + staging | **YES** |
| proposal2_endpoint_psm_strata.py | studies/proposal2_ete_staging/ | PSM + interactions | **YES** |
| proposal2_cox_regression.py | studies/proposal2_ete_staging/ | Cox PH + KM | **YES** |
| proposal2_expanded_cohort.py | studies/proposal2_ete_staging/ | Expanded + MI + safety | **YES** |
| proposal2_recommendations.py | studies/proposal2_ete_staging/ | Sensitivity + recommendations | **YES** |
| audit_reproduce.py | studies/proposal2_ete_staging/ | Independent audit | **YES** |
| risk_enriched_mv | MotherDuck: thyroid_research_2026 | Upstream source table | **YES** |
| manuscript_v1.md | studies/manuscript_draft/ | Manuscript text | **YES** |
| ptc_full.csv | studies/proposal2_ete_staging/tables/ | Intermediate (PTC demographics) | intermediate |
| recurrence_full.csv | studies/proposal2_ete_staging/tables/ | Intermediate (risk features) | intermediate |
| imaging_correlation.csv | studies/proposal2_ete_staging/tables/ | Intermediate (imaging data) | intermediate |
| manuscript_cohort_v1 | MotherDuck table | Repo-level cohort (broader) | NO (different analysis) |
| scripts/62-66_*.py | scripts/ | Repo-level Tables 1-3 | NO (different analysis) |

---

## Stale / Non-Authoritative Pipelines

| Artifact | Why NOT Authoritative |
|----------|----------------------|
| manuscript_cohort_v1 (10,871 rows) | Broader repo-level cohort — not the ETE-specific PTC study |
| scripts/63_run_primary_models.py | Generates different models for the broader thyroid cohort |
| patient_analysis_resolved_v1 | Resolved layer for all surgical patients, not ETE-specific |
| exports/manuscript_freeze_v1/ | Freezes the broad 10,871-patient cohort, not the 596/3,278 ETE cohort |
| studies/hypothesis1_cln_lobectomy/ | Different hypothesis (CLN in lobectomy) |
| studies/hypothesis2_goiter_sdoh/ | Different hypothesis (Goiter SDOH) |
