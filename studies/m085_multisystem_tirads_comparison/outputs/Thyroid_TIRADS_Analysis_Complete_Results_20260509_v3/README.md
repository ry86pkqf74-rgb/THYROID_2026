# M085 — Multi-system TIRADS Comparison: Complete Results (v3)

**Generated:** 2026-05-09 (v3 re-pack — canonical_operative_patient_rollup_v1_1 promotion)  
**Cohort:** 6,750 surgical thyroid patients (1994-2025), dominant nodule per patient  
**Outcome:** Final-pathology malignancy (43% prevalence)  
**Manuscript:** M085 (Airtable rec `recotdCiIuU8UQbLs`)

## What changed in v3 vs v2

A canonical-layer defect fix was promoted to `pub_canonical.canonical_operative_patient_rollup_v1_1`
on 2026-05-09. The v3 re-pack refreshes the surgery-type column using the corrected table.

**Surgery-type counts (v2 → v3):**

| Type | v2 (rollup_v1) | v3 (rollup_v1_1) |
|------|-------------:|----------------:|
| Total thyroidectomy | 2,801 | **3,907** |
| Hemithyroidectomy | 1,747 | **2,288** |
| Completion thyroidectomy | 284 | **191** |
| Unknown | **1,918 (28.4%)** | **137 (2.1%)** |

Root cause: v1 relied exclusively on `canonical_operative_events_v1.procedure_normalized` (NULL for 99.8%
of affected patients). v1_1 adds two higher-fidelity sources:
- `canonical_operative_procedure_codes_v1` (note-level CPT extraction, 90% coverage)
- `canonical_path_gland_patient_rollup_v1` lobe-dimension laterality (94% coverage)

A path-gland override rule resolves 343 single-surgery cases where OPC over-attributed hemi notes as
total thyroidectomy. Cascade-defensible agreement: **98.25% (8,685/8,840 multi-source patients)**.
23 residual low-confidence patients staged to `pub_workspace.qc_v1_1_residual_review_v1`.

All primary analytical results (AUC, ROM by TIRADS category, sensitivity/specificity, kappa) are
**unchanged** — surgery type is a descriptive covariate, not an analytical endpoint.

**Figure 5 (surgery type by ACR category) reflects v1_1 counts.** Table 1 surgery-type row updated.

## Contents

### Primary deliverables (PDFs)

- `all_figures_compiled.pdf` — All 10 figures (Figure 5 updated with v1_1 surgery types)
- `all_tables.xlsx` — 9 sheets; T1 cohort descriptives updated
- `statistical_methods.pdf` — Methods document; surgery-type ascertainment limitation updated
- `draft_manuscript.pdf` — Full manuscript draft with v1_1 surgery counts
- `plain_language_summary.pdf` — Lay summary

### Supporting artifacts

- `figures/` — 10 PNG figures
- `cohort_descriptives.json` — v1_1 surgery type counts
- All other data CSVs — **unchanged** (surgical analysis not affected)

## Top-line findings (unchanged from v2)

1. AUC range 0.530 (Horvath) → 0.684 (ACR-strict). ACR TI-RADS leads modestly.
2. ACR-imputed loses negligible accuracy vs ACR-strict while gaining 2.4× coverage.
3. Inter-system agreement bimodal: ACR/EU/K-TIRADS/ATA cluster at κ ≥ 0.85.
4. Park 2009 underperforms; ~3 of 12 features severely under-documented.
5. Histologic distribution: 80.2% PTC, 10.8% follicular, 2.9% MTC.

## Reproducibility

- Analyses ran against `pub_canonical.canonical_operative_patient_rollup_v1_1` (promoted 2026-05-09).
- Predecessor table: `pub_canonical.canonical_operative_patient_rollup_v1` (deprecated).
- v1 snapshot preserved at `pub_workspace.canonical_operative_patient_rollup_v1_pre_v1_1_promotion_20260509_snapshot`.
- DFL: `DFL-2026-05-09-v1-1-canonical-promotion-execute` (Airtable appJYOnUb7KrHKwpV)

## Audit trail

- MFL: rec63EZqsKUiaSgiG (M085, base appJYOnUb7KrHKwpV)
- DFL promotion: recEF0fpaciZjta41
- Linear: THY-56
- Cascade-defensible agreement: 98.25% (8,685/8,840)
- Residual review queue: `pub_workspace.qc_v1_1_residual_review_v1` (23 patients)
