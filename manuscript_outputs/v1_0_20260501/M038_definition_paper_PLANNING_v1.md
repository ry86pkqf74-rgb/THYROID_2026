# M038 — Definition of "Massive" Goiter and Perioperative Complication Risk
**Type:** Planning document (pre-draft)
**Author:** Logan Glosser (Cowork lane)
**Date:** 2026-05-01
**Cohort source:** `manuscript_workspace.cohort_m038_massive_goiter_v1` (canonical pub v1.0; **extended via mig_251 — 24 → ~95 columns**)
**Status:** ⏸ **PAUSED — blocked on mig_252 (comp rollup fix) + mig_253 (surg procedure-type fill).** RQ locked, primary outcome locked, cohort extended via mig_251. Drafting resumes once Cursor Composer lands the two upstream fixes.

---

## Reframe

The master-list subtitle "Massive Goiter Surgery (ECMO support)" is **dropped**. The lakehouse contains zero ECMO columns and zero ECMO/cardiopulmonary-bypass tokens in NLP airway findings or operative-findings free text. ECMO-supported thyroidectomy is a case-report-level event in the broader literature; this dataset cannot study it directly. The paper is reframed as a **definition study**: weight-based vs. anatomic-compression-based operationalizations of "massive" goiter, head-to-head as predictors of perioperative complications.

## Research question

> Among patients undergoing thyroidectomy at Emory (1999–2024), how do three operationalizations of "massive" goiter — gland weight ≥200 g, substernal extension, and CT-detected airway compromise — compare as predictors of perioperative complications?

**Hypothesis (data-driven, not pre-registered):** Anatomic-compression definitions outperform pure-weight definitions. In exploratory cross-tabs, weight-only "massive" (≥200 g without substernal extension or airway compromise) had a **lower** complication rate (11%) than non-massive substernal disease (~48%), and the highest-risk cell is the intersection of all three (54%).

This thesis is publishable because the existing literature is cluttered with inconsistent thresholds (Hedayati ≥200 g, Newman ≥150 g, others by volume) and rarely benchmarks weight against anatomic compression directly.

## Cohort

| Subset | n | Notes |
|---|---:|---|
| Full eligible cohort | 10,871 | Source view: `cohort_m038_massive_goiter_v1` |
| With `gland_weight_final_g` | 9,130 | 84% coverage |
| Weight ≥100 g | 1,429 | Sensitivity-analysis cutoff |
| Weight ≥150 g | 805 | Sensitivity-analysis cutoff |
| **Weight ≥200 g (primary)** | **475** | ~p95 of weight distribution; matches Hedayati et al. |
| Weight ≥250 g | 293 | Sensitivity-analysis cutoff |
| Weight ≥300 g | 181 | Sensitivity-analysis cutoff |

**Weight distribution (g):** min 1, p25 15, p50 28.7, p75 65.7, p90 139, p95 204, p99 403, max 2,320.

**Anatomic-compression flags (denominator = full cohort 10,871):**
- CT substernal extension: 921 (8.5%)
- MRI substernal extension: 160
- CT tracheal deviation: 1,398
- CT tracheal narrowing: 1,262
- CT airway compromise: 1,269
- CT goiter present: 1,750

## Exposures (the three definitions to compare)

| Code | Definition | n | Source field(s) |
|---|---|---:|---|
| E1 | Weight-based: `gland_weight_final_g` ≥ 200 g | 475 | `gland_weight_final_g` (path module) |
| E2 | Substernal: `ct_substernal_extension_any` OR `mri_substernal_any` | ~1,000 (deduped from 921 CT + 160 MRI) | CT/MRI imaging modules |
| E3 | Airway compromise: `ct_airway_compromise_any` OR `ct_tracheal_narrowing_any` | ~1,500 (deduped) | CT imaging module |
| E1∩E2∩E3 | All three | 179 | Intersection — highest-risk cell |
| E1 only (no E2, no E3) | Pure weight | 251 | Lowest-risk cell within "massive" |

## Primary outcome

**Strict definition (post-mig_252):** event-level filter on `canonical_complications_events_v1` — `finding_status = 'present'` AND `evidence_strength IN ('definitive','probable')`, OR'd across the 10 family complications (seroma, hematoma, RLN injury, chyle leak, VC paresis/paralysis, hypocalcemia, hypoparathyroidism, airway complication, pneumothorax).

The original `any_confirmed_complication_flag` on `canonical_patient_master` is **broken** — it counts `finding_status='absent'` rows (negation evidence) as confirmation events. Cowork audit on 2026-05-01 found 95% of `comp_seroma_confirmed=TRUE` patients have zero `finding_status='present'` events. mig_252 dispatched to Cursor Composer to fix the rollup cohort-wide.

| Subset | n | Buggy rollup rate (DO NOT USE) | **Strict-definition rate (use this)** |
|---|---:|---:|---:|
| Full cohort | 10,871 | 23% (2,490) | **3.6% (388)** |
| ≥200 g | 475 | 30.7% (146) | **2.1% (10)** |
| <200 g | 8,655 | 21.3% (1,844) | **3.4% (296)** |
| Weight NULL | 1,741 | 28.7% (500) | **4.7% (82)** |

Strict cross-tab (E1 = weight ≥200 g, E2 = any substernal, E3 = any airway compromise):

| E1 | E2 | E3 | n | Strict events | Rate |
|:-:|:-:|:-:|---:|---:|---:|
| Y | Y | Y | 179 | 7 | 3.9% |
| Y | Y | N | 8 | 0 | 0% |
| Y | N | Y | 37 | 1 | 2.7% |
| Y | N | N | 251 | 2 | 0.8% |
| N | Y | Y | 493 | 32 | 6.5% |
| N | Y | N | 144 | 10 | 6.9% |
| N | N | Y | 371 | 25 | 6.7% |
| N | N | N | 7,647 | 229 | 3.0% |

**Implications for the analysis plan:**

- The "weight alone is a poor predictor" thesis still holds — pure-weight ≥200 g without compression has the lowest rate (0.8%) of any cell.
- Anatomic compression still elevates risk, but at clinically realistic absolute rates (6–7%), not the 47–54% the buggy rollup suggested.
- With only **10 events in the ≥200 g focal cohort**, focal-cohort-restricted regressions are severely underpowered. The full-cohort interaction model (n≈10,871, 388 events) supports 3–4 predictors with interactions and is the better analytic frame.

## Secondary outcomes (descriptive only — small absolute numbers)

| Outcome | n events in ≥200 g subset | Use |
|---|---:|---|
| `comp_hematoma_confirmed` | 1 | Descriptive table |
| `comp_rln_injury_confirmed` | 0 | Descriptive table — NLP-confirmation conservatism caveat |
| `comp_vc_paralysis_confirmed` | 2 | Descriptive table |
| `comp_seroma_confirmed` | **39** | Descriptive table — most-frequent confirmed component |
| `comp_airway_complication_definitive` | 0 | Descriptive table — NLP-confirmation conservatism caveat |
| `comp_pneumothorax_definitive` | 0 | Descriptive table |
| Tracheostomy (`proc_nlp_tracheostomy`) | 14 | Descriptive secondary outcome |
| `nsqip_unplanned_intubation` | 2 | Descriptive |
| `nsqip_transfusion` (any) | reportable in n=68 with NSQIP data | Descriptive |
| `nsqip_operative_duration_min` | n=68 with data, median to be computed | Continuous descriptive — operative-complexity proxy |
| `nsqip_length_of_stay_days` | n=42 with data, median 1 day | Descriptive (low variability) |
| `nsqip_readmission_30d_flag` | 1 | Descriptive |
| `is_malignant` | 67 (14.1%) | Incidental-cancer rate — secondary descriptive finding |
| Death (`death_occurred`) | 16 | Descriptive only. Median follow-up = 0 yr, p75 = 1.48 yr. Cannot support survival analysis. |

## Analysis plan

1. **Table 1 — cohort characterization.** Stratified by E1 (weight ≥200 g vs <200 g), with the n=475 column being the focal "massive" arm. Demographics (age, sex, race, BMI), comorbidity panel (ASA class, NSQIP-derived diabetes/HTN/COPD/CHF, NLP-derived diabetes/HTN/CAD/CKD/COPD/autoimmune-thyroid hx, Graves, Hashimoto, anticoagulation, prior thyroidectomy, prior neck surgery), surgical procedure type, surgical era (1999–2009 / 2010–2019 / 2020–2024), surgeon, malignancy status, follow-up coverage. Coverage caveats reported per row.
2. **Table 2 — three-definition cross-tab.** 2×2×2 cells of (E1, E2, E3) showing n and complication rates per cell. This is the centerpiece table; it reveals weight-only "massive" has lower complications than anatomic compression alone.
3. **Multivariable model.** Logistic regression, outcome = composite any-complication. Compare three nested-model specifications:
   - M-A: E1 alone (weight ≥200 g)
   - M-B: E2 alone (any substernal extension)
   - M-C: E1 + E2 + E3 + interactions
   Compare AUC, AIC, and net reclassification across specifications. Adjust minimally (age, sex, total-vs-hemi). Avoid overfitting given event count.
4. **Sensitivity analyses.** Re-run primary at weight ≥150 g and ≥250 g cutoffs. Re-run excluding cases with NULL `surg_procedure_type` (n=121 in the ≥200 g subset).
5. **Reporting outcome.** Effect estimates as adjusted odds ratios with 95% CI. Discrimination metric: AUC. No survival modeling.

## Known data caveats

- **No ECMO data.** Title reframed.
- **Median follow-up = 0 yr.** Perioperative-only paper. No survival, recurrence, or long-term morbidity analyses.
- **Sparse confirmed RLN/hematoma counts.** 0 confirmed RLN and 1 hematoma in the ≥200 g subset reflect NLP-confirmation conservatism (the canonical pipeline only counts confirmed events). The composite `any_confirmed_complication_flag` (146/475) is the analyzable endpoint; component breakdowns go in a descriptive table with a caveat.
- **NSQIP fields are sparsely populated** in the ≥200 g subset: `nsqip_operative_approach` 16/475 (all "Open (planned)" — no variability), `nsqip_length_of_stay_days` 42/475 (median 1 day, p95 4 days), `nsqip_transfusion` 68/475, `ops_difficult_airway` 8/475. NSQIP-derived variables are **not** analyzable as primary or secondary outcomes; mention only as period-specific descriptive footnotes.
- **`surg_procedure_type` has 121 NULLs in the ≥200 g subset** (out of 475). Either impute conservatively or run as sensitivity exclusion.
- **`sex` is coded as `'female'`/`'male'`**, not `'F'`/`'M'`. Trivial — flag for the drafting query templates.
- **`op_findings_summary` is only nonnull for 587/10,871 patients** total. Cannot rely on it as a structured exposure or outcome source.

## Cohort extension — DONE (mig_251)

`mig_251` (applied 2026-05-01) extended the cohort view from 24 → ~95 columns. The extension was driven by Logan's standing rule (every manuscript must include demographics + a systematic full-dataset column review; see `memory/feedback_manuscript_demographics_and_full_column_review.md`).

**Categories added:**
- **Demographics:** `bmi_combined`, `bmi_source`, `nsqip_bmi`, `nsqip_height_in`, `nsqip_weight_lbs`, `nsqip_smoker`, `nsqip_tobacco_use`, `pmhx_nlp_smoking_status`, `bmi_missingness_reason`
- **Comorbidities & PMHX:** `nsqip_asa_class`, `nsqip_diabetes`, `nsqip_hypertension`, `nsqip_copd`, `nsqip_heart_failure`, `nsqip_bleeding_disorder`, `nsqip_disseminated_cancer`, `nsqip_functional_status`, `pmhx_nlp_diabetes/hypertension/cad/ckd/copd`, `pmhx_nlp_n_comorbidities`, `pmhx_nlp_autoimmune_thyroid_hx`, `syn_graves`, `syn_hashimoto`, `ops_anticoagulation_meds`, `pshx_nlp_prior_thyroidectomy`, `pshx_nlp_prior_neck_surgery`
- **Surgical context:** `surg_total_thyroidectomy`, `surg_hemithyroidectomy`, `surg_n_procedures`, `nsqip_central_neck_dissection`, `nsqip_lateral_neck_dissection`, `nsqip_operative_approach`, `nsqip_operative_duration_min`, `nsqip_drain_usage`, `nsqip_vessel_sealant`, `nsqip_rln_monitoring`, `ops_difficult_airway`, `ops_surgeon`, `ops_surg_date`, `nsqip_inpatient_outpatient`, `nsqip_same_day_discharge_flag`, `nsqip_primary_indication`
- **LOS & disposition:** `nsqip_hospital_los_days`, `nsqip_length_of_stay_days`, `nsqip_surgical_los_days`, `nsqip_admission_date`, `nsqip_discharge_date`, `nsqip_discharge_destination`
- **Anatomy:** `syn_isthmus_height_cm`, `syn_left_lobe_height_cm`, `syn_right_lobe_height_cm`
- **Pathology:** `bilateral_disease_flag`, `bilateral_path_flag`, `closest_margin_mm`
- **Confirmed-complication panel (expanded):** `comp_airway_complication_definitive`, `comp_pneumothorax_definitive`, `comp_vc_paresis_confirmed/permanent`, `comp_vc_paralysis_confirmed/permanent`, `comp_hypocalcemia_confirmed/permanent`, `comp_hypoparathyroidism_confirmed/permanent`, `comp_chyle_leak_confirmed`, `comp_seroma_confirmed`, `comp_mortality_definitive`
- **NSQIP perioperative outcomes:** `nsqip_transfusion`, `nsqip_neck_hematoma`, `nsqip_hematoma_flag`, `nsqip_rln_injury_flag`, `nsqip_hypocalcemia_flag`, `nsqip_unplanned_intubation`, `nsqip_unplanned_return_or`, `nsqip_readmission_30d_flag`, `nsqip_readmission_count`, `nsqip_death_30d`, `nsqip_pneumonia`, `nsqip_dvt`, `nsqip_pe`, `nsqip_sepsis`, `nsqip_superficial_ssi`, `nsqip_deep_ssi`, `nsqip_organ_space_ssi`
- **Tracheostomy:** `proc_nlp_tracheostomy`, `proc_nlp_tracheostomy_date`, `proc_nlp_tracheostomy_days_from_surg`, `proc_nlp_tracheostomy_n_mentions`
- **Recurrence:** `any_recurrence_flag`, `biochemical_recurrence_flag` (descriptive only — short follow-up)

**Verification confirmed:** row count = 10,871; primary outcome unchanged at 146/475 in ≥200 g; gate1 = 218 (no registry impact); cohort_parity TRUE.

## Tables and figures planned

- **Table 1.** Demographics & surgical characteristics, stratified by weight ≥200 g.
- **Table 2.** Three-definition exposure cross-tab with composite-complication rates (the centerpiece).
- **Table 3.** Adjusted ORs from M-A, M-B, M-C model specifications.
- **Table 4.** Sensitivity analyses across weight cutoffs (150 / 200 / 250 g).
- **Figure 1.** Forest plot of adjusted ORs by exposure.
- **Figure 2.** ROC curves comparing M-A, M-B, M-C.

## Target journal candidates

- *Surgery* (high-volume cohort + definitional contribution fits well)
- *World Journal of Surgery* (clinical-surgical readership; publishes thyroid cohorts)
- *Thyroid* (clinical section)
- *Annals of Surgical Oncology* (if cancer-yield secondary findings are amplified)

## Related work to cite (priors only — not exhaustive)

- Hedayati N, et al. — ≥200 g definition, anchor paper.
- Newman et al. (2019) — ≥150 g definition.
- Weiss et al. — substernal goiter operative outcomes.
- Coskun et al. — airway compromise as predictor.
- Logan to fill in the rest at drafting time.

## Carry-forwards from this planning doc

| ID | Action | Trigger to close |
|---|---|---|
| ~~CF-M038-COHORT-EXTEND~~ | ~~Optional `mig_25X` to add tracheostomy / LOS / transfusion / difficult-airway columns~~ | **CLOSED — applied as mig_251 on 2026-05-01 with substantially expanded scope (71 new columns covering demographics, comorbidities, surgical context, LOS, expanded complications, NSQIP outcomes, tracheostomy, anatomy, recurrence)** |
| **CF-COMP-CONFIRMED-ROLLUP-FIX** | Cohort-wide repair of `comp_*_confirmed` and `any_confirmed_complication_flag` on `canonical_patient_master` — current rollup counts `finding_status='absent'` rows as confirmations. Dispatched to Cursor Composer as **mig_252**. | mig_252 lands |
| **CF-SURG-PROC-TYPE-FILL** | Cohort-wide fill of `surg_procedure_type` / `surg_total_thyroidectomy` / `surg_hemithyroidectomy` for the 2,138 NULL cases (121 in ≥200g subset). Source data exists in `canonical_operative_events_v1` and `nsqip_cpt_code/description`. Dispatched as **mig_253**. | mig_253 lands |
| **CF-M038-PAUSED** | M038 first-draft paused until both mig_252 and mig_253 land. Once landed, regression power must be recalculated against the strict-definition event count. | Both mig_252 + mig_253 land |
| **CF-M032-COMPLICATIONS-REBUILD** | M032 25-yr descriptive paper draft references the buggy 23% complication rate. Needs complications-section rebuild after mig_252. | mig_252 lands |
| **CF-M038-SEX-CODING** | Sex coded `'female'`/`'male'` not `'F'`/`'M'` — minor query-template note | Caught at first draft |
| ~~CF-M038-SURG-TYPE-NULL~~ | ~~121/475 in ≥200 g subset have NULL `surg_procedure_type` — sensitivity-exclude or impute~~ | **SUPERSEDED — escalated to cohort-wide CF-SURG-PROC-TYPE-FILL / mig_253** |
| **CF-M038-DRAFT-V1** | First draft following M032 pattern, using strict-definition primary outcome and full-cohort interaction model | After mig_252 + mig_253 land |

---

**Bottom line:** M038 is reframed from a phantom-ECMO outcomes paper into a **definition paper** with a clean, novel thesis: anatomic compression (substernal + airway) drives perioperative complication risk while weight alone — even at ≥200 g — does not. Strict-definition complication rate is ~2% in the ≥200 g focal cohort (not the 30% the buggy rollup suggested). Drafting paused pending mig_252 (comp rollup fix) and mig_253 (surg procedure-type fill). Once both land, the analysis frame shifts to a full-cohort interaction-terms logistic model (n≈10,871, ~388 strict events) rather than focal-cohort nested models.
