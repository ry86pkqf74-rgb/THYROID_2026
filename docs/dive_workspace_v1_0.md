# Dive Workspace Reference — v1.0

**Database:** `thyroid_canonical_publication_v1_0`
**MotherDuck Dives:** 31 total (19 dedicated, 12 thematic)
**Manuscript coverage:** 63 / 63
**Date:** 2026-04-16

---

## How to Use Dives

Dives are interactive React data apps hosted on MotherDuck that query live data from `manuscript_workspace` views. Each Dive uses the `useSQLQuery` hook for live queries and Recharts for visualization.

### For co-authors

1. Open MotherDuck and navigate to the Dives tab
2. Search by manuscript number (e.g., "M048") or theme (e.g., "Molecular")
3. The Dive will show pre-built panels with filters — no SQL required
4. Use `manuscript_dive_map_v1` to find which Dive covers your manuscript:
   ```sql
   SELECT dive_title, cohort_view_name 
   FROM manuscript_workspace.manuscript_dive_map_v1 
   WHERE manuscript_id = 48;
   ```

### For analysts

Each Dive's underlying data comes from a cohort view in `manuscript_workspace`. You can query these views directly:
```sql
SELECT * FROM manuscript_workspace.cohort_m048_tnm_multifocal_v1 LIMIT 100;
```

The consolidated full-cohort view has ~130 columns for ad-hoc exploration:
```sql
SELECT * FROM manuscript_workspace.cohort_descriptive_full_cohort_v1 LIMIT 100;
```

## Thematic Dive Descriptions

### T1 — Whole-Cohort Pathology Descriptives
Serves M48-M54, M58-M60. Panels: histology distribution, tumor size distribution, AJCC staging breakdown. Filters: ATA risk, surgery extent. Source: full cohort (N=10,871).

### T2 — Frozen Section Series
Serves M62-M65. Panels: concordance matrix, false-negative rates by nodule features, frozen section outcomes by surgery type. Extends Sprint A M47 theme. Source: full cohort.

### T3 — Graves/Hashimoto/Thyroiditis
Serves M4, M16, M61, M69, M78. Panels: carcinoma rate by autoimmune status, demographic comparison, recurrence by thyroiditis subtype. Source: dedicated cohort views with autoimmune/thyroiditis filters.

### T4 — Molecular Testing Applications
Serves M6, M18, M23, M68, M72, M80. Panels: surgical decision by molecular result, outcome comparison by platform, Afirma vs ThyroSeq usage. Source: molecular-tested cohort views.

### T5 — Post-op Surveillance & Tg Kinetics
Serves M67, M73, M76. Panels: Tg trajectory by surgery type, surveillance density, detection curves. v1.1 upgrade flagged for improved Tg date coverage. Source: Tg/surveillance cohort views.

### T6 — RAI Treatment Outcomes
Serves M19, M55, M81. Panels: RAI dose distribution, response by genetic profile, resistance patterns. Source: RAI-treated cohort views.

### T7 — Parathyroid Intraop & Pathology
Serves M9, M17, M66, M79, M82. Panels: intraop-vs-final parathyroid counts, outcomes, PTH/Ca correlation. v1.1 upgrade flagged for M17/M79 PTH/Ca longitudinal data. Source: parathyroid cohort views.

### T8 — TIRADS Decision Support
Serves M11, M75. Panels: malignancy by TIRADS category, nodule count histograms, FNA yield by TIRADS level. Source: TIRADS-scored cohort views.

### T9 — Risk Stratification & Reclassification
Serves M7, M57. Panels: ATA risk distribution, reclassification table, recurrence by risk tier. Extends Sprint A M36 ATA comparison. Source: ATA risk-categorized cohort views.

### T10 — Age & Epidemiology
Serves M56. Panels: age histograms, age x demographic crosstabs, trend over time. Source: age/epidemiology cohort view.

### T11 — Indeterminate Nodule Outcomes
Serves M1. Panels: genetics-tested vs untested outcomes for Bethesda III/IV, time-to-surgery, final pathology breakdown. Extends Sprint A M28 theme. Source: indeterminate nodule cohort view.

### T12 — Hereditary & Immunologic
Serves M70, M71. Panels: hereditary condition prevalence, immunologic medication patterns. Lighter-weight Dive. Source: hereditary/immunologic cohort views.

## Top 3 Most Useful Thematic Dives for Co-Authors

1. **T4 — Molecular Testing Applications** (6 manuscripts): Highest manuscript density, covers the most active research theme across the group. Molecular decision-making is central to multiple ongoing studies.

2. **T1 — Whole-Cohort Pathology Descriptives** (10 manuscripts): The workhorse Dive for any manuscript needing full-cohort descriptive statistics — Table 1 generation, staging breakdowns, and pathology distributions.

3. **T7 — Parathyroid Intraop & Pathology** (5 manuscripts): Bridges intraoperative and pathologic parathyroid findings across multiple complementary manuscripts, with v1.1 upgrade path for longitudinal PTH/Ca data.
