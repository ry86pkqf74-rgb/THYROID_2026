# Methods snippet — ACR 2017 TI-RADS completeness (manuscript paste-up)

**Context:** Per-nodule US data live in `main.canonical_us_nodule_v2` (publication DB `thyroid_canonical_publication_v1_0`). Lane E Round 2 (`mig_219`–`mig_221`, 2026-04-30) added cohort views and clarified completeness semantics.

**Suggested Methods text (adapt to journal style):**

> **ACR TI-RADS 2017 component completeness.** We classified nodules as having **complete ACR 2017 feature inputs** when `acr2017_feature_points_complete` is TRUE. This flag reflects historical completeness of the five required **sonographic descriptor** fields on the upstream characteristics table (composition, echogenicity, shape, margins, calcifications) at the time of initial QA (`tirads_score_component_complete` in the consolidation pipeline), **not** mere non-nullness of the five per-feature point columns after downstream imputation (Script 376 can populate `*_pts` from normalized labels without changing this flag). Therefore the **primary strict ACR 2017 analytic subset** additionally requires non-null `acr2017_tirads_points` and `acr2017_tirads_category` and excludes aggregate, shell, and NLP-pending rows via `manuscript_workspace.canonical_us_nodule_v2_filtered`, materialized as `manuscript_workspace.vw_us_nodule_tirads_strict_acr2017_VIEW_v1`. Sensitivity analyses that prioritize **any** radiologist-reported or institutional category use `vw_us_nodule_tirads_any_reported_VIEW_v1` and are reported separately so denominators are not mixed.

**Related artifacts:** `memory/feedback_acr2017_feature_points_complete_semantic.md`, `memory/feedback_tirads_category_canonical.md`.
