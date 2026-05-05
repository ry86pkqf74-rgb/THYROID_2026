# M025 Submission Package v2.0 — NODULE-LEVEL pivot

**Status:** scaffold (Cowork-drafted header). Build scripts to be authored by Cursor under mig_307.
**Predecessor:** `M025_submission_package_v1_0/` (patient-level, frozen as sister analysis).
**Driving table:** `thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_m025_nodule_level_v1` (mig_306, signed 2026-05-04).
**Release tag:** to be `pub_v1_2_<date>` after mig_307 + Cursor verification.

## Title (working)

**Patient-level versus nodule-level TI-RADS calibration in a 25-year operative thyroid cohort: attribution error explains most apparent operative-cohort risk inflation**

## Rationale for the v1.0 → v2.0 pivot

The v1.0 paper analyzed TI-RADS at the **patient grain** (one row per research_id, MAX_TIRADS_CATEGORY_EVER as the test variable, IS_MALIGNANT as outcome). This collapsed multinodular patients to a single TR category and produced per-TR ROM that substantially exceeded ACR-published expected ranges at every category — a finding originally interpreted as "operative-cohort selection bias."

When the same cohort is re-analyzed at the **nodule grain** — using `canonical_us_nodule_v2` per-nodule ACR2017 categorization, bridging to FNA via `imaging_fna_linkage_v3` (rid + laterality + ±30d), and matching to malignancy via same-side path tumor within 365d post-US — TR4 and TR5 risk-of-malignancy land **inside** the ACR-expected ranges:

| TIRADS | Patient-level ROM (v1.0) | Nodule-level ROM (v2.0) | ACR-expected | Inflation (pp) |
|---|---:|---:|---|---:|
| TR2 | 32.1% | 12.9% | <2% | +19.2 |
| TR3 | 27.6% | 9.1% | <5% | +18.5 |
| **TR4** | 47.4% | **18.7%** | **5–20% ✓** | +28.7 |
| **TR5** | 58.7% | **26.1%** | **>20% ✓** | +32.6 |

This implies **50–70% of apparent operative-cohort ROM inflation is attribution error from multinodular patients**, not selection bias. Properly per-nodule TI-RADS recovers ACR-expected calibration at the clinically actionable thresholds.

## Primary research question (Q1 v2.0)

> In a 25-year single-institution operative thyroid cohort, what is the per-nodule diagnostic performance of ACR TI-RADS 2017, and how much of the apparent operative-cohort ROM inflation observed in patient-level analyses is attributable to multinodular attribution error versus true selection bias?

## Primary endpoints

1. Per-nodule sensitivity, specificity, PPV, NPV, AUC at TR≥TR3, TR4, and TR5 thresholds.
2. Direct quantitative comparison of patient-level vs nodule-level per-TR ROM in the same cohort, with attribution-error attribution.
3. Calibration plots: observed nodule-level ROM vs ACR-expected ROM per TR.
4. Bethesda × TI-RADS cross-stratification at nodule grain.

## Cohort definition

- Source: `manuscript_workspace.cohort_m025_nodule_level_v1` (mig_306).
- Total nodules: 37,438 across 6,523 patients.
- **Primary analytic cohort: `analytic_eligible_strict_acr_pernodule = TRUE`** = 3,687 nodules. Filters: ACR2017 feature points complete, ACR2017 category populated, laterality known, not size-outlier, not multi-nodule-attribution-unresolved.
- **With FNA Bethesda available (post-bridge):** 2,216 nodules.
- **Path-confirmed malignant nodules:** 3,973 across 1,230 patients (any-eligibility); restricted to strict cohort = TBD by build script.

## Companion / sister analyses

- `M025_submission_package_v1_0/` — patient-level paper, retained as sister analysis. Will be cited in v2.0 Discussion as the comparator that motivated the methodological investigation.
- `cohort_m075_tirads_multi_nodule_v1` — overlapping multi-nodule analysis (n=3,282); v2.0 Methods will distinguish scope.

## Open carry-forwards that affect v2.0 numbers

- **CF-FNA-SIZE-CM-NULL** — `imaging_fna_linkage_v3.fna_size_cm` is NULL by design in v1_0; size_score is a flat 0.5 prior. v1_1 NLP extraction will upgrade FNA-link recall (currently bridges via rid + laterality + ±30d, ~70% recovery vs total FNA episodes). Sensitivity arm in v2.0 should report nodule-level performance with and without size-aware FNA-linkage scoring.
- **CF-mig_264-BETHESDA2-LINKAGE-MISMAP** — 360 residual Bethesda-2 + malignant patients pending disposition. If mig_264 reclassifies any subset as linkage mismaps, v2.0 numbers will shift; the per-nodule view explicitly carries `bethesda_final_num` per nodule so the audit can be more granular.

## Build provenance

- Per-nodule view built and signed: `mig_306` (2026-05-04) — see `qc_framework_v1/migrations/306_nodule_level_spine_20260504.sql` and `cursor_prompts/CURSOR_PROMPT_MIG_306_NODULE_LEVEL_SPINE_20260504.md`.
- Build scripts (to be authored by Cursor under mig_307): `08_analysis_code/build_m025_v2_tables.py`, `build_m025_v2_figures.py`, `build_m025_v2_manuscript_md.py`.

## Closeout checklist (mirrors v1.0)

- [ ] Replace this scaffold with Cursor-built tables, figures, manuscript.docx, supplement.docx after mig_307.
- [ ] Cross-check per-nodule numbers vs MotherDuck source (live SQL in mig_306 §5).
- [ ] Cite v1.0 patient-level analysis as sister.
- [ ] Sign off mig_307 in `signoff_migration`.
