# M037 — Lymph node predictors: canonical methods conventions (mig_266)

**Purpose:** Methods-section declarations for manuscript M037 (LN predictors). Transclude or paste into full draft Methods. **Lane:** mig_266 bulk footnote update (post round-6 migrations).

---

## LN status source (F1 — mig_258 / CF-mig258-MANUSCRIPT-FILTER-UPDATE)

Lymph-node positivity in analyses that require numeric concordance between AJCC N-stage and structured metastatic counts is sourced from `ln_status_source ∈ {'staging','count','both',NULL}` on `canonical_patient_master`, where `staging` indicates AJCC 8 N-stage assertion only, `count` indicates `ln_total_positive > 0`, and `both` indicates concordance. For regressions treating LN positivity as a count-based burden (e.g., Table 2 sensitivity refits), restrict to malignant patients with `ln_status_source = 'both'` (**n=2,628**; **1,126** LN-positive, 42.8%) versus the full malignant analytic spine (**n=4,137**; 1,126 LN-positive, 27.2%) (`snowflake_trial/reports/m037_sensitivity_ln_both.md`), to avoid mixing N-stage-only positivity with unevaluated LN counts.

## AJCC stage rollup (F2 — mig_263 Option B)

`canonical_patient_master.ajcc8_stage_group` collapses {IVA, IVB, IVC} to `IVB` at patient-level rollup (mig_266b overlay family); M1 distant disease is uniformly `IVB`. Full published labels remain in `ajcc8_stage_group_resolved`.

## Ultrasound suspicious LN flag (F4 — mig_262)

`any_suspicious_us_ln_ever` was rebuilt from `canonical_us_thyroid_gland_v2` per-nodule data; post-rebuild prevalence is ~1,733 patients among ultrasound-eligible subjects vs 8 under the legacy threshold. Cite mig_262 when reporting US-LN predictor rates.

## Bethesda category II ROM (F5 — mig_264 / mig_264b)

Bethesda-2 risk of malignancy in this operative cohort was 18.9% (385 of 2,033 with `bethesda_final = 2`) before mig_264b reclassification. After mig_264b (24 NIFTP/follicular adenoma non-malignant reclassifications; 19 postoperative-FNA mismatches repointed to preoperative Bethesda), residual ROM is ~342 patients (~16.8%). Elevation vs 0–3% screening cohorts reflects (1) tertiary surgical referral enrichment and (2) cytology limits distinguishing follicular adenoma from carcinoma. Stratify Bethesda analyses accordingly.

## NLP and structured-path discordance (F6 — mig_265 / CF-mig260b,c,d / mig_261c,d,e)

- Vascular NLP (`nlp_path_vasc_inv_mentioned`) misses patients positive on `canonical_invasion_events_v1` (mig_177/179 CAP recovery).
- LN NLP/PMH flags disagree with `canonical_path_malignant_events_v1.ln_involved` in 1,105 patients.
- Smoking (~0.25% extracted), family hx thyroid (30 pts), family hx any cancer (16 pts) are under-extracted vs clinical expectation.
- Primary-outcome models using smoking, family history, or vascular NLP as exposure should cross-validate to `canonical_invasion_events_v1` / `canonical_path_malignant_events_v1.ln_involved`.

**Open scope:** `CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE`.
