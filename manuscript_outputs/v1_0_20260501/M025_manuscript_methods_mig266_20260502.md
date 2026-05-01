# M025 — ACR TI-RADS performance: canonical methods conventions (mig_266)

**Purpose:** Methods-section declarations for manuscript M025 (cytology / TIRADS performance). Transclude into full draft Methods when available. **Lane:** mig_266.

---

## Bethesda category II ROM (F5 — mig_264 / mig_264b)

Bethesda-2 risk of malignancy in this operative cohort is **18.9%** (385 of 2,033 patients with `bethesda_final = 2`), far above the **0–3%** range typical of screening populations. After **mig_264b** reclassification (24 NIFTP + follicular adenoma cases as non-malignant; 19 patients with postoperative-FNA mismapping repointed to preoperative Bethesda), the **residual** Bethesda-2 malignant count is **~342 patients (~16.8% ROM)**. Remaining elevation reflects operative-cohort selection and inherent cytology limits (follicular adenoma vs carcinoma). All TR-category–ROM and Bethesda-stratified performance metrics should be interpreted in this context.

## NLP and structured-path discordance (F6 — mig_265 / CF-mig260b,c,d / mig_261c,d,e)

Subgroup analyses depending on **smoking**, **family history**, or **NLP vascular invasion** as primary predictors are limited: NLP vascular mentions under-cover `canonical_invasion_events_v1`; LN NLP/PMH flags disagree with `canonical_path_malignant_events_v1.ln_involved` in 1,105 patients; smoking and family-history NLP coverage is sub-percent to low single-digit percent vs expected clinical prevalence. Cross-validate primary claims against structured pathology tables where applicable.

**Open scope:** `CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE`.
