# Cursor Prompt: M083 BRAF Dual-Platform Discordance Analysis

**Agent:** Sonnet 4.6 (Composer 2.0) — focused concordance/discordance analysis with clear diagnostic metrics; Sonnet handles this efficiently  
**Estimated time:** 1.5–2 hours  
**Date:** 2026-05-04

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, analytic view `manuscript_workspace.m083_dual_platform_analytic_v1` (N=167). This cohort includes patients who received BOTH ThyroSeq AND Afirma molecular testing.

### Cohort Summary:
- Total: 167 patients
- BRAF+: 132 (79.0%)
- RAS+: 36 (21.6%)
- Fusion+: data available via `mol_has_fusion`
- Malignant: 111 (66.5%)
- Recurred: 20 (12.0%)

### Critical Finding:
**99 of 160 evaluable patients (61.9%) have discordant BRAF results between ThyroSeq and Afirma.** This is a manuscript-worthy finding — dual-platform BRAF discordance at this rate has not been well-characterized in the literature.

### Available Columns (33):
`research_id`, `age_at_surgery`, `sex`, `race`, `histology_final`, `tumor_size_cm_dominant`, `is_malignant`, `ajcc8_stage_group`, `ajcc8_t_stage`, `ajcc8_n_stage`, `braf_positive_final`, `braf_variant`, `braf_detection_method_v11`, `ras_positive_final`, `ras_subtype`, `mol_has_fusion`, `molecular_risk_tier`, `mol_n_distinct_genes`, `mol_genes_list`, `mol_platform_original`, `mol_platform_resolved`, `mol_platform_evidence`, `mol_platform_confidence`, `n_episodes_used`, `any_recurrence_flag`, `any_confirmed_complication_flag`, `rai_received_reconciled`, `ete_grade_clean`, `vascular_invasion_final`, `ln_positive_final`, `ln_rollup_total_examined`, `ln_rollup_total_positive`, `surg_procedure_type`

### Data Architecture Note:
The per-episode molecular data is in `canonical_molecular_genetics_v2` (1,384 rows). Each dual-platform patient has 2+ episodes. The CPM-level `braf_positive_final` is the RECONCILED value (TRUE if ANY episode detected BRAF). To assess per-platform BRAF results, you MUST join back to `canonical_molecular_genetics_v2` on `research_id`.

## Task

### 1. Per-Platform BRAF Results

Join `m083_dual_platform_analytic_v1` to `canonical_molecular_genetics_v2` to extract:
- ThyroSeq BRAF result for each patient
- Afirma BRAF result for each patient
- Create a 2×2 concordance table:

| | Afirma BRAF+ | Afirma BRAF− |
|---|---|---|
| ThyroSeq BRAF+ | Concordant+ | Discordant (TS+/Af−) |
| ThyroSeq BRAF− | Discordant (TS−/Af+) | Concordant− |

**NOTE:** `research_id` is VARCHAR in CPM but may be BIGINT in `canonical_molecular_genetics_v2` — cast appropriately for the join.

### 2. Discordance Characterization

For the 99 discordant patients:
- Direction of discordance: ThyroSeq+/Afirma− vs ThyroSeq−/Afirma+
- Was the discordance clinically relevant? (i.e., did the BRAF+ result on one platform change management?)
- Histology distribution: are discordant cases enriched for FVPTC or other borderline histologies?
- Tumor size in discordant vs concordant cases
- Malignancy rate in discordant vs concordant cases

### 3. Which Platform Was "Right"?

Using final surgical pathology as gold standard:
- For BRAF+ on final path (`braf_positive_final IS TRUE`): which platform detected it?
- False negative rate by platform (BRAF+ on final but negative on that platform's test)
- If IHC data is available (`braf_detection_method_v11`), use it as an independent arbiter

### 4. Possible Explanations for Discordance

Investigate:
- **Timing difference:** Was one test done before and one after surgery? (check `mol_first_test_date` and episode dates in CMG_v2)
- **Sample difference:** Different nodules biopsied? (FNA from different sites)
- **Platform sensitivity:** ThyroSeq NGS detects low-allele-fraction BRAF that Afirma's expression-based assay may miss (or vice versa)
- **Version effects:** GEC vs GSC, v2 vs v3 — do version transitions correlate with discordance?

### 5. RAS Discordance

Repeat the concordance analysis for RAS mutations:
- Per-platform RAS result (from CMG_v2 episodes)
- Concordance table
- Note: Only 2 RAS-discordant patients were identified in the molecular audit — confirm this

### 6. Clinical Impact of Discordance

Compare discordant vs concordant patients:
- Surgical procedure type (total vs hemi) — did discordance lead to more aggressive surgery?
- RAI receipt rate
- Recurrence rate
- ATA risk category distribution
- AJCC stage distribution

### 7. Output

Save to `studies/m083_braf_discordance/`:
- `braf_concordance_2x2.csv` — per-platform BRAF concordance table
- `discordance_characterization.csv` — demographics, histology, tumor size of discordant cases
- `platform_accuracy.csv` — which platform was correct vs final path
- `ras_concordance.csv` — RAS concordance table
- `clinical_impact.csv` — outcomes in discordant vs concordant
- `discordance_explanations.csv` — timing, version, possible mechanism data
- `braf_discordance_summary.tex` — LaTeX tables

### 8. Upload to MotherDuck

Create `manuscript_workspace.m083_discordance_analysis_v1` with patient-level per-platform results and concordance flags.

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR in CPM/views but may be BIGINT in `canonical_molecular_genetics_v2` — CAST appropriately
- Boolean columns: use `IS TRUE` / `IS NOT TRUE`
- The 99/160 discordance rate was computed during the molecular audit — verify by independently extracting per-episode BRAF from CMG_v2
- 7 patients had missing secondary episodes in CMG_v2 — exclude from concordance table, report separately
- `braf_positive_final` in CPM is the RECONCILED value (any-positive wins) — do NOT use this for per-platform analysis
- Afirma's BRAF assessment is indirect (expression-based classifier) vs ThyroSeq's direct NGS sequencing — this biological difference may explain much of the discordance
- Sex: lowercase `female`/`male`
- This is a NEW manuscript (M083) — no prior cohort view existed before this session
