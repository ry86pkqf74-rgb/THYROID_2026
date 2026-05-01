# Snowflake Cortex Validation — Prompt 11: Comorbidity / PMH / PSH Coverage
**Generated:** 2026-05-01 (post-handoff)
**Source:** MD-direct via MCP (`thyroid_canonical_publication_v1_0`); equivalent script `snowflake_trial/scripts/17_prompt11_comorbidity.py` will produce the same numbers from Snowflake once Logan re-exports.
**Tables probed:** `canonical_pmh_patient_rollup_v1` (10,871 × 79), `canonical_pmh_events_v1` (12,696 × 19), `canonical_psh_events_v1` (3,919 × 19), `manuscript_workspace.cohort_descriptive_full_cohort_v1`.

---

## Summary

PMH/PSH coverage is **two-tiered**: cardiovascular, endocrine, and prior-cancer signals are present at plausibly-extracted prevalences (15–20%); social history, family history, and bone-health signals are dramatically under-extracted (<1%). M004 (autoimmune carcinoma) cohort selection is methodologically sound — it uses synoptic-derived `syn_graves`/`syn_hashimoto` (574 + 248 patients cohort-wide; 57 + 94 in malignant), not the much-thinner PMHx-NLP signal (80 patients).

Net new findings:
- **CF-mig261b-PMH-DEFINITIVE-COL-DEAD** — 8 PMH conditions (autoimmune_thyroid_hx, family_hx_*, osteoporosis, smoking, coagulopathy, men_syndrome, radiation_exposure) have `_definitive=0` for all rows despite `_any_evidence > 0`. Either the rollup builder never assigns "definitive" tier for these, or the evidence threshold is too strict.
- **CF-mig261c-SMOKING-COVERAGE-GAP** — only 27 patients (0.25%) have any smoking-status extraction; a thyroid surgery cohort at intake should have ~7,000+ documented smoking statuses. Manuscript impact for any subgroup analysis depending on smoking.
- **CF-mig261d-FAMILY-HX-COVERAGE-GAP** — family_hx_thyroid (30 pts) + family_hx_cancer (16 pts); both should be 5–15% in a cancer-referral cohort. Likely upstream NLP miss.
- **CF-mig261e-HYPERTENSION-UNDERCOUNT** — 16.4% prevalence vs ~47% US adult expected. CV disease signal likely under-extracted in non-PMHx note sections.

---

## 1. PMH rollup prevalences (any-evidence vs definitive)

Cohort denominator: 10,871 CPM rows.

| Condition | any_n | def_n | any% | def% |
| --- | --- | --- | --- | --- |
| hypothyroidism | 1,963 | 1,962 | 18.1% | 18.0% |
| hypertension | 1,781 | 1,775 | 16.4% | 16.3% |
| diabetes | 1,483 | 1,466 | 13.6% | 13.5% |
| hyperthyroidism | 1,163 | 1,163 | 10.7% | 10.7% |
| prior_cancer_hx | 676 | 530 | 6.2% | 4.9% |
| obesity | 542 | 523 | 5.0% | 4.8% |
| gerd | 478 | 478 | 4.4% | 4.4% |
| asthma | 475 | 475 | 4.4% | 4.4% |
| breast_cancer | 427 | 425 | 3.9% | 3.9% |
| depression | 399 | 399 | 3.7% | 3.7% |
| cad | 247 | 224 | 2.3% | 2.1% |
| ckd | 221 | 221 | 2.0% | 2.0% |
| afib | 175 | 174 | 1.6% | 1.6% |
| lung_cancer | 156 | 156 | 1.4% | 1.4% |
| copd | 107 | 107 | 1.0% | 1.0% |
| **autoimmune_thyroid_hx** | **78** | **0** | **0.7%** | **0.0%** |
| **radiation_exposure** | **33** | **0** | **0.3%** | **0.0%** |
| **family_hx_thyroid** | **30** | **0** | **0.3%** | **0.0%** |
| **osteoporosis** | **23** | **0** | **0.2%** | **0.0%** |
| **family_hx_cancer** | **16** | **0** | **0.1%** | **0.0%** |
| **smoking_current** | **14** | **0** | **0.1%** | **0.0%** |
| **coagulopathy** | **13** | **0** | **0.1%** | **0.0%** |
| smoking_never | 9 | 0 | 0.1% | 0.0% |
| smoking_former | 6 | 0 | 0.1% | 0.0% |
| **men_syndrome** | **6** | **0** | **0.1%** | **0.0%** |

Two systemic patterns:
1. **Definitive-tier blackout** for 9 conditions: `_definitive=0` for every row, despite `_any_evidence > 0`. Either the evidence-strength promotion rule never fires for these conditions or there's a definitional gap. **CF-mig261b**.
2. **Social-history under-extraction**: smoking (29 total), family history (46 total combined), osteoporosis (23) — all an order of magnitude below expected community-cohort rates. Likely an NLP extractor that doesn't run on PMHx/SocHx prose subsections. **CF-mig261c, CF-mig261d**.

---

## 2. PMH events table — smoking under-coverage probe

| finding_value_norm | finding_status | n_events | n_pts |
| --- | --- | --- | --- |
| smoking_never | present | 9 | 9 |
| smoking_status | present | 8 | 5 |
| smoking_never | absent | 7 | 7 |
| smoking_former | present | 5 | 5 |
| smoking_current | present | 1 | 1 |

Only 30 events / 27 patients across the entire `canonical_pmh_events_v1` table. The rollup is faithfully aggregating an essentially empty upstream — this is a real upstream coverage gap, not a build bug.

---

## 3. M004 (autoimmune carcinoma) cross-validation

| Signal | Cohort-wide n | Malignant cohort n |
| --- | --- | --- |
| `syn_graves` (synoptic-derived) | 574 | **57** |
| `syn_hashimoto` (synoptic-derived) | 248 | **94** |
| `syn_chronic_thyroiditis` (synoptic-derived) | 1,096 | 566 |
| `pmhx_nlp_autoimmune_thyroid_hx` (PMHx NLP) | 80 | 26 |
| `pmhx_nlp_hyperthyroidism` (PMHx NLP) | 1,163 | 435 |
| `pmhx_nlp_hypothyroidism` (PMHx NLP) | 1,962 | 981 |

**M004 cohort numbers reconcile**: session summary's 57 Graves + 94 Hashimoto comes directly from synoptic-derived `syn_graves` / `syn_hashimoto` (path-report findings), not PMHx NLP. The methodology is sound — synoptic findings come from histologic examination of the resected gland and are more authoritative than chart documentation of pre-surgery autoimmune disease.

The PMHx NLP signal (80 patients with autoimmune-thyroid-hx) is supplementary and would *add* — it captures patients whose pre-surgery chart noted autoimmune thyroid hx but whose path report didn't show synoptic features (treated/inactive disease). For full pre-existing autoimmune background, M004 may want to UNION both.

---

## 4. PSH (prior surgical history) — distribution

Top categories from `canonical_psh_events_v1` (3,919 events / 1,878 patients).

| finding_value_norm | finding_status | n_events | n_pts |
| --- | --- | --- | --- |
| other_surgery | present | 953 | 489 |
| prior_fna | present | 904 | 754 |
| prior_thyroidectomy | present | 626 | 555 |
| prior_rai | present | 269 | 237 |
| total_thyroidectomy | present | 185 | 181 |
| prior_neck_surgery | present | 156 | 123 |
| prior_neck_dissection | present | 142 | 118 |
| hysterectomy | present | 62 | 61 |
| prior_parathyroidectomy | present | 60 | 56 |
| cholecystectomy | present | 56 | 56 |
| right_hemithyroidectomy | present | 55 | 49 |
| left_hemithyroidectomy | present | 54 | 52 |
| prior_core_biopsy | present | 52 | 48 |
| tonsillectomy | present | 44 | 43 |
| appendectomy | present | 44 | 44 |
| thyroidectomy_unspecified | present | 44 | 44 |
| cesarean_section | present | 31 | 24 |
| partial_thyroidectomy | present | 27 | 23 |
| thyroid_biopsy | present | 26 | 25 |
| completion_thyroidectomy | present | 22 | 21 |

Aggregating prior-thyroid-surgery types (prior_thyroidectomy + total_thy + right_hemi + left_hemi + completion + revision + partial + thyroidectomy_unspecified):
- **~881 patients (8.1%) had prior thyroid surgery** before the indexed surgery in this cohort, consistent with mig_104's PSH events scope.
- 754 patients (6.9%) had a prior FNA — all malignant cohort patients should have one; the 754 captures only those with explicit prior-FNA mentions in operative notes.

---

## 5. Reusable patterns

- **Definitive-tier blackout probe** (`_any_evidence > 0 AND _definitive = 0` for ≥99% of patients with that condition) flags conditions where the rollup builder either skipped the strength-promotion rule or set the threshold too high. Cheap one-shot SQL across all `pmh_*_definitive` flags.
- **Population-rate sanity check**: comparing extracted prevalences (HTN 16%, smoking 0.25%) against published US-adult community rates (HTN ~47%, smoking ~14%) is a fast bullshit-detector for the extraction pipeline.
- **Synoptic-derived vs NLP-derived autoimmune signals** disagree in size by 10×; cohort selection methodology should declare which signal is being used and why (synoptic = histologic, NLP-PMHx = chart documentation).

---

## 6. Carry-forwards (new)

| CF | Description | Severity | Action |
| --- | --- | --- | --- |
| CF-mig261b-PMH-DEFINITIVE-COL-DEAD | 9 conditions have `_any_evidence>0 AND _definitive=0` for 100% of patients | LOW | Audit rollup builder strength-promotion rule |
| CF-mig261c-SMOKING-COVERAGE-GAP | Only 27 patients have any smoking-status extraction (~0.25% vs expected ~70%+) | MED | Upstream NLP refresh on social-history sections; manuscript impact for smoking-stratified analyses |
| CF-mig261d-FAMILY-HX-COVERAGE-GAP | family_hx_thyroid 30 pts + family_hx_cancer 16 pts; expected 5–15% in cancer cohort | MED | Upstream NLP refresh; impacts familial PTC subgroup analyses |
| CF-mig261e-HYPERTENSION-UNDERCOUNT | 16.4% extracted vs ~47% US adult expected | LOW | Cardiovascular-section NLP coverage check |

No CFs raised on M004 — the cohort numbers reconcile cleanly via synoptic-derived flags.
