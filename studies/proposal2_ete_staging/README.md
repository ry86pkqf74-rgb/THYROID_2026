# Proposal 2: ETE Staging & Recurrence Risk

Extrathyroidal Extension (ETE) impact on AJCC 8th edition staging and recurrence risk
in papillary thyroid carcinoma (PTC).

## Data Source

- **Analytic cohort:** `risk_enriched_mv` (MotherDuck `thyroid_research_2026`)
- **Fallback:** `tables/analytic_cohort.csv` (exported snapshot)
- **Cohort size:** PTC patients with complete staging, ETE, and follow-up data

## Analyses

| Script | Description |
|--------|-------------|
| `proposal2_ete_analysis.py` | Primary analysis: ETE distribution, stage migration, ordinal regression, ROC |
| `proposal2_endpoint_psm_strata.py` | Propensity score matching + stratified recurrence endpoints |
| `proposal2_expanded_cohort.py` | Expanded cohort with relaxed inclusion criteria |
| `proposal2_recommendations.py` | Clinical recommendation synthesis |
| `audit_reproduce.py` | Reproducibility audit with independent verification |

## Key Outputs

- **Tables 1-7:** Demographics, stage migration, regression, sensitivity, expanded results, comparison
- **Figures 1-10:** ETE distribution, stage migration, risk by ETE, ROC, Tg trajectory, forest plots, KM survival, cohort flow, matched DFS
- `analysis_report.md` — Full narrative report
- `audit_report.md` — Reproducibility audit results
- `recommendations.md` — Clinical recommendations
- `analysis_metadata.yaml` — Run provenance (seeds, versions, timestamps)
