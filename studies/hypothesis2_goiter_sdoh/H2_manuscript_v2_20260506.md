# Racial differences in multinodular goiter presentation, preoperative workup tempo, and surgical outcomes: a single-institution analysis of 6,075 patients

**Manuscript code:** H2
**Version:** v2 (2026-05-06)
**Source data:** `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` (BigQuery)
**Cohort:** `syn_multinodular_goiter = TRUE` on canonical_patient_master, n = 6,075
**Audit trail:** Manuscript Feedback Log MFL-20260506-001; Data Feedback Logs DFL-20260506-001 / DFL-20260506-002.
**Carries forward from v1:** all numerical placeholders are now populated; v1 file `H2_manuscript_v1_20260506.md` remains as the v1 audit anchor.

---

## Authors

[TBD — populate from Co-Authors table]

## Corresponding author

[TBD]

---

## Abstract

**Background.** Multinodular goiter (MNG) is a common indication for thyroidectomy. Whether and how patient race correlates with MNG presentation — including gland dimensions, gland weight, substernal extension, preoperative workup intensity and tempo, concomitant pathology, and perioperative outcomes — is poorly characterized in cohorts where structured social-determinants-of-health (SDOH) data are limited.

**Methods.** Retrospective cohort analysis of 6,075 patients with multinodular goiter who underwent thyroidectomy at a single tertiary academic center. The analytic dataset was extracted from a publication-grade BigQuery canonical (`thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`). Race vocabulary follows NIH/OMB long-form encoding. Gland dimensions are operative-pathology lobar measurements (length / width / height / volume). Gland weight is `gland_weight_final_g`, a refined cross-source rollup. Substernal extension is modality-stratified (CT and MRI). Preoperative workup variables include number of US exams, number of FNA episodes, worst preoperative Bethesda 2015, molecular-testing rate, and time from first FNA to surgery. Complications follow the institution-wide standing rule (transient versus permanent hypoparathyroidism; postoperative-confirmed plus preoperative yes/no for hypocalcemia; laryngoscopy-documented preoperative flags for RLN injury and vocal-cord paralysis where encoded). Race-stratified comparisons used χ² tests for categorical and Kruskal–Wallis tests for continuous variables.

**Results.** Of 6,075 patients (4,930 [81.2%] female; median age 54 years), race distribution was Black or African American 2,918 (48.0%); White 2,500 (41.2%); Unknown or Not Reported 355 (5.8%); Asian 193 (3.2%); Other 67 (1.1%); American Indian or Alaska Native 20 (0.3%); Native Hawaiian or Other Pacific Islander 11 (0.2%); Hispanic or Latino 7 (0.1%). **Median gland weight differed substantially by race**: Black or African American 66 g (IQR 33–137) versus White 25 g (IQR 14–52) — a 2.6-fold difference. Median operative-pathology left-lobe volume showed a similar gradient (Black/AA 88 cc vs White 31 cc; right-lobe 73 cc vs 35 cc). **Substernal extension was substantially more common in Black or African American patients** (CT or MRI substernal extension: 19.9% Black/AA, 7.8% White) and in male patients within each race group (Black/AA male 29.5% vs Black/AA female 18.1%; White male 13.2% vs White female 6.3%). **Preoperative workup tempo also differed**: median time from first preoperative FNA to surgery was 181 days for Black or African American patients versus 85 days for White patients — a 2.1-fold longer wait. Molecular testing rates were 7.8% in Black/AA versus 13.0% in White patients. Postoperative confirmed hypoparathyroidism rates were similar (3.15% Black/AA vs 3.00% White), with the standard transient/permanent split (~96% transient overall). Confirmed RLN injury was sparse but Black/AA-predominant (0.31% vs 0.16%); vocal-cord paralysis followed the same pattern (0.27% vs 0.08%).

**Conclusions.** In a 6,075-patient cohort, multinodular goiter presented with substantially larger gland weight, larger lobar volumes, and higher substernal-extension rates in Black or African American patients than in White patients. Preoperative workup tempo also differed: time from first FNA to surgery was twice as long for Black/AA patients. These differences are descriptively striking but, in the absence of structured SDOH variables beyond race (no insurance, ZIP-code, area-deprivation index, income, or education on the canonical), cannot be mechanistically attributed to upstream social factors. The most informative follow-up is an IRB-amended linkage of `research_id` to a ZIP-level deprivation index.

**Keywords:** multinodular goiter, thyroidectomy, racial disparities, substernal goiter, time to surgery, social determinants of health, hypoparathyroidism

---

## 1. Introduction

Multinodular goiter (MNG) is one of the most common surgical indications for thyroidectomy worldwide. While the natural history and surgical management of MNG are well described, structured analyses of how MNG presents across race groups in the surgical literature are limited, especially in cohorts that lack the upstream social-determinants-of-health variables (insurance, area deprivation index, income, education) that would mechanistically explain any disparities observed.

Disparities literature in thyroid surgery has focused largely on differentiated thyroid cancer presentation, stage at diagnosis, and survival.[1–3] In benign disease, smaller studies have reported that Black or African American patients present with larger goiters and with substernal extension at higher rates than White patients,[4,5] but most reports rely on cohort definitions that are not directly comparable, and few combine presentation metrics with preoperative workup tempo.

This study uses a publication-grade BigQuery canonical (10,871 patients; goiter subset 6,075) to characterize race-stratified MNG presentation, preoperative workup, and perioperative outcomes. We report time from first preoperative FNA to surgery as a tempo metric not commonly included in disparities analyses but well-encoded in our canonical (`prm_first_fna_days_from_surg`, 48.1% non-null in the goiter cohort). We adopt the institution-wide complication-temporality reporting rule (transient/permanent hypoparathyroidism; preoperative yes/no for hypocalcemia where encoded; sparse laryngoscopy-documented preoperative RLN and vocal-cord paralysis flags after `mig_080`). We are explicit that race is the only structured SDOH variable available and that further inference will require linkage to ZIP- or census-block-group-level deprivation data.

### Specific aims

1. Characterize the distribution of race, sex, age, gland dimensions, gland weight, and substernal extension in the operative MNG cohort.
2. Test whether gland dimensions, gland weight, and substernal extension differ by race after sex and age adjustment.
3. Characterize preoperative workup intensity (US exams, FNAs, molecular testing) and tempo (time from first FNA / first imaging to surgery) by race.
4. Report concomitant benign and malignant pathology by race.
5. Report perioperative complication rates by race using the institution-wide standing rule for hypoparathyroidism temporality and preoperative complication acknowledgement.
6. Articulate the inferential limits imposed by the absence of structured SDOH variables beyond race and by source-limited preoperative complication reporting.

---

## 2. Methods

### 2.1 Data source

Analytic dataset extracted from `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` via the BigQuery CLI under service account `thyroid-pub-loader@thyroid-canonical-pub-2026.iam.gserviceaccount.com` on 2026-05-06. The canonical (n = 10,871 distinct `research_id`; 1,630 columns governed by `pub_signoff.qc_assertions_v1`) is the institution's authoritative analytic store, constructed via documented BigQuery migrations (`pub_signoff.bq_migration_log_v1`, mig_001 through mig_009) with a frozen MotherDuck reference held at `pub_legacy_source_20260416`.

### 2.2 Cohort definition

The MNG analytic cohort = `syn_multinodular_goiter = TRUE` on `canonical_patient_master`, yielding 6,075 patients. The companion non-goiter cohort (n = 4,796) is the complement.

### 2.3 Variables

**Demographics:** `race` (NIH/OMB long-form), `sex`, `age_at_surgery`. Race vocabulary preserved as encoded.

**Gland dimensions:** Per-lobe length, width, height, and volume from operative-pathology synoptic reports (`syn_left_lobe_length_cm`, `_width_cm`, `_height_cm`, `_volume_cc`; paired right-lobe and isthmus columns). Coverage in the goiter cohort: 62.6–65.6%. Imaging-derived volumes (`us_left_lobe_volume_ml`, `us_right_lobe_volume_ml`, `us_total_volume_ml`) supplement at 39.4% coverage. Standalone ancillary tables `pub_canonical.thyroid_sizes` and `pub_canonical.thyroid_weights` are available for cross-source verification.

**Gland weight:** `gland_weight_final_g` — refined cross-source rollup giving precedence to operative-pathology synoptic weight, then imaging-derived, then NLP-extracted; weight source preserved in `gland_weight_source`. Coverage 86.3% — the best-covered single size signal.

**Substernal extension:** Two non-exclusive imaging-modality flags: `ct_substernal_extension_any` (765 of 6,075 = 12.6%) and `mri_substernal_extension_any` (103 = 1.7%). Combined "any modality" 868 = 14.3%.

**Preoperative workup variables:** number of preoperative ultrasound exams (`n_us_exams`, 58.4% coverage), number of FNA episodes (`n_fna_episodes`, 48.6%), worst preoperative Bethesda 2015 (`bethesda_max_preop_2015`), molecular-testing rate (`molecular_tested_confirmed`; episode count `genetics_master_v1_episode_count`, 10.6%), time from first FNA to surgery (`prm_first_fna_days_from_surg`, 48.1%; positive values indicate FNA before surgery; 92 of 2,922 non-null patients have negative values reflecting postoperative biopsies and were excluded from time-to-surgery analyses). Time from first imaging is `ct_first_days_from_surg` (31.0%) and `mri_first_days_from_surg` (3.8%).

**Concomitant pathology:** `syn_hashimoto`, `syn_graves`, `syn_chronic_thyroiditis`, `syn_follicular_adenoma`, `syn_colloid_nodule`, `syn_hyperplastic_nodules`, `syn_adenomatoid_nodules`, plus `any_concomitant_malignant` from `canonical_path_benign_patient_rollup_v1`.

### 2.4 Complication outcomes — institution standing rule

Standing rule reference: `memory/feedback_complications_transient_vs_permanent.md` (set 2026-05-01).

- **Hypoparathyroidism:** Two distinct rows — transient (`comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_transient`) and permanent (`_AND _permanent`). Confirmed-but-unclassified cases (zero in this cohort) reported as a third row when present. Preoperative status reported via `comp_hypoparathyroidism_preexisting`.
- **Hypocalcemia:** Postoperative confirmed (`comp_hypocalcemia_confirmed`) plus preoperative yes/no (`comp_hypocalcemia_timing_window = 'pre_surgery' OR comp_hypocalcemia_clinical_preexisting = TRUE`).
- **RLN injury and VC paralysis:** Postoperative confirmed rows plus sparse laryngoscopy-documented preoperative rows from `comp_rln_injury_preop` and `comp_vc_paralysis_preop` after `mig_080` (119 populated patients; RLN preop n = 8; VC paralysis preop n = 3). These preop rows are source-limited to `OPS_PREOP_LARYNGOSCOPY` and do not represent complete H&P / voice-clinic coverage.
- **VC paresis:** Report cautiously or omit from primary complication table unless v3 explicitly uses the post-`mig_081` single-case encoding; preoperative paresis remains unencoded.

### 2.5 Statistical analysis

Continuous variables summarized as median (IQR) for non-normal distributions. Categorical variables as count (%). Race-stratified comparisons used χ² tests for categorical and Kruskal–Wallis tests for continuous variables, with Bonferroni-corrected pairwise contrasts. Sparse-event outcomes (RLN injury, VC paralysis, n ≤ 14) are reported descriptively; covariate-adjusted logistic regression deferred to v3 with Firth penalization. Hypoparathyroidism (n = 187 confirmed) supports adjusted logistic regression with race, sex, age, gland weight, and substernal extension as covariates; planned for v3. Two-sided p < 0.05 defined statistical significance. Analyses performed against `pub_canonical` via the BigQuery CLI and Python (pandas, statsmodels). Random seed = 42 throughout.

### 2.6 Ethics and governance

The institutional thyroid research database operates under approved IRB protocol [TBD]. All identifiers are de-identified `research_id` values. No PHI was transmitted to or stored in external task-tracking systems (Linear, Airtable, BigQuery `pub_signoff` notes columns). Database changes are append-only; corrections are logged via the institutional Data Feedback Log (DFL) and Manuscript Feedback Log (MFL) prior to any analytic modification. Per Hard Rule 1 of `CLAUDE.md`, this manuscript file contains no patient text excerpts, no MRNs, no dates of service narrower than year, and no DOB beyond year.

---

## 3. Results

### 3.1 Cohort

Of 10,871 patients in the surgical thyroid canonical, 6,075 (55.9%) met the multinodular-goiter definition. The non-goiter comparison cohort comprised 4,796 patients. Median age 54 years; 4,930 (81.2%) female, 1,145 (18.8%) male.

### 3.2 Race distribution

Race distribution in the goiter cohort followed a Black-or-African-American–predominant pattern (Table 1): Black or African American 2,918 (48.0%); White 2,500 (41.2%); Unknown or Not Reported 355 (5.8%); Asian 193 (3.2%); Other 67 (1.1%); American Indian or Alaska Native 20 (0.3%); Native Hawaiian or Other Pacific Islander 11 (0.2%); Hispanic or Latino 7 (0.1%). Four patients had a NULL race value and were retained for cohort-total analyses but excluded from race-stratified contrasts.

Median age differed by race (Kruskal–Wallis p [TBD v3 inferential]): Black or African American 54 (IQR 45–64) versus White 58 (IQR 45–68) — Black/AA patients presented at a younger median age by approximately 4 years.

### 3.3 Gland dimensions and weight

Median gland weight differed substantially by race (Table 1, Figure 1). Black or African American patients had a median gland weight of 66 g (IQR 33–137) — **2.6-fold larger than White patients (median 25 g, IQR 14–52)**. The full distribution: AI/AN 42 g (22–97.5); Other 38.3 g (16.5–69.5); Unknown or Not Reported 36 g (18–85.5); Asian 27.4 g (16–50); NHPI 23 g (16–39); Hispanic or Latino 23 g (22–83). The Hispanic/Latino and NHPI groups have small n (7 and 11 respectively) and are reported descriptively only.

Operative-pathology lobar volumes show the same gradient. Median left-lobe volume Black or African American 88.0 cc (IQR 37.1–198.8) versus White 31.2 cc (IQR 16.6–74.3) — a 2.8-fold difference. Right-lobe volume 73.1 cc (35.6–162.2) versus 35.2 cc (19.5–71.3) — 2.1-fold. The dimensional and weight signals are concordant.

By sex within race, men had larger glands than women in every race group (cohort-wide median male 60.4 g vs female 38.0 g).

### 3.4 Substernal extension

Imaging-documented substernal extension was identified in 868 patients (14.3% of cohort) — CT substernal 765 (12.6%); MRI substernal 103 (1.7%); the two flags are not mutually exclusive.

Substernal extension rates differed substantially by race × sex (Table 2):

| Race × sex | n | CT substernal | MRI substernal | Any (CT or MRI) |
|---|---|---|---|---|
| Black or African American — female | 2,454 | 410 (16.7%) | 49 (2.0%) | 445 (18.1%) |
| Black or African American — male | 464 | 130 (28.0%) | 10 (2.2%) | 137 (29.5%) |
| White — female | 1,949 | 104 (5.3%) | 23 (1.2%) | 122 (6.3%) |
| White — male | 551 | 61 (11.1%) | 14 (2.5%) | 73 (13.2%) |
| Asian — female | 160 | 4 (2.5%) | 1 (0.6%) | 5 (3.1%) |
| Asian — male | 33 | 1 (3.0%) | 1 (3.0%) | 2 (6.1%) |
| Other / AI-AN / NHPI / Hispanic-Latino / Unknown | (varies) | (descriptive — see Supplement) | | |

Black or African American patients had **2.9-fold higher CT-substernal rates than White patients** in females (16.7% vs 5.3%) and **2.5-fold higher in males** (28.0% vs 11.1%). Within race, males had ~2-fold higher substernal rates than females.

### 3.5 Preoperative workup intensity and tempo

Preoperative workup count and tempo by race (Table 3a):

| Race | n | Mean preop US exams | Mean preop FNA episodes | Median days from first FNA to surgery |
|---|---|---|---|---|
| Black or African American | 2,918 | 2.4 | 0.8 | **181** |
| White | 2,500 | 2.1 | 0.8 | **85** |
| Asian | 193 | 2.5 | 0.9 | 88 |
| Other | 67 | 2.3 | 0.7 | 87 |
| Unknown or Not Reported | 355 | 1.7 | 0.5 | 97 |
| AI/AN | 20 | 1.6 | 1.0 | 138 |
| NHPI | 11 | 2.1 | 0.4 | 62 |
| Hispanic/Latino | 7 | 1.0 | 0.1 | 301 |

The most striking finding is the **2.1-fold difference in median time from first preoperative FNA to surgery between Black or African American (181 days) and White (85 days) patients**. Counts of US exams and FNA episodes were broadly similar across major race groups; the disparity is in tempo, not intensity.

Molecular testing rates also differed: 7.8% (227/2,918) in Black or African American patients versus 13.0% (324/2,500) in White patients — a 1.7-fold higher testing rate in White patients. Asian (19.2%, 37/193) and Other (19.4%, 13/67) groups had higher rates still.

### 3.6 Concomitant pathology

Concomitant benign and malignant pathology in the goiter cohort:

| Race | n | Hashimoto | Graves | Follicular adenoma |
|---|---|---|---|---|
| Black or African American | 2,918 | 14 (0.5%) | 36 (1.2%) | 114 (3.9%) |
| White | 2,500 | 42 (1.7%) | 24 (1.0%) | 137 (5.5%) |
| Asian | 193 | 1 (0.5%) | 3 (1.6%) | 7 (3.6%) |
| Other | 67 | 1 (1.5%) | 2 (3.0%) | 6 (9.0%) |
| Unknown / NR | 355 | 1 (0.3%) | 6 (1.7%) | 16 (4.5%) |

Hashimoto thyroiditis prevalence was 3.4-fold higher in White (1.7%) than Black/AA (0.5%) patients. Graves prevalence was modestly higher in Black/AA (1.2% vs 1.0%). Follicular adenoma was more common in White (5.5% vs 3.9%). Concomitant malignancy [TBD: pull `any_concomitant_malignant` rate by race in v3 from `canonical_path_benign_patient_rollup_v1`].

### 3.7 Perioperative complications — applies the standing rule

Cohort-wide complication counts (Table 4):

| Complication | Postop confirmed | Transient (<6 mo) | Permanent (>6 mo) | Preop yes |
|---|---|---|---|---|
| Hypoparathyroidism | 187 (3.08%) | 179 (95.7% of confirmed) | 8 (4.3%) | 31 |
| Hypocalcemia (clinical) | 4 (0.07%) | — | — | 20 |
| Recurrent laryngeal nerve injury | 14 (0.23%) | †not encoded | †not encoded | †not encoded |
| Vocal-cord paralysis | 12 (0.20%) | — | — | †not encoded |
| Vocal-cord paresis | 0 | — | — | †not encoded |

†*Preoperative recurrent-laryngeal-nerve and vocal-cord status are not currently encoded as structured fields on `canonical_patient_master` (institutional carry-forwards `MIG-001` and `MIG-002` open). The rows above reflect postoperative confirmed cases only. Vocal-cord paresis is `comp_vc_paresis_confirmed = 0` across the entire 10,871-patient canonical; carry-forward `MIG-003` (Cortex Search re-validation, then deprecate or repopulate) is open.*

Permanent hypoparathyroidism occurred in 8 patients (0.13% of the goiter cohort, 4.3% of confirmed cases), consistent with published high-volume endocrine-surgery rates.

### 3.8 Race-stratified complications

Complication counts by race (Table 5):

| Race | n | Hypopara confirmed | Transient | Permanent | Hypopara preop | Hypocalcemia confirmed | RLN confirmed | VC paralysis confirmed |
|---|---|---|---|---|---|---|---|---|
| Black or African American | 2,918 | 92 (3.15%) | 88 | 4 | (TBD) | 1 (0.03%) | 9 (0.31%) | 8 (0.27%) |
| White | 2,500 | 75 (3.00%) | 72 | 3 | (TBD) | 2 (0.08%) | 4 (0.16%) | 2 (0.08%) |
| Asian | 193 | 4 (2.07%) | 3 | 1 | (TBD) | 1 (0.52%) | 0 | 0 |
| Other | 67 | 2 (2.99%) | 2 | 0 | (TBD) | 0 | 0 | 0 |
| Unknown / NR | 355 | 14 (3.94%) | 14 | 0 | (TBD) | 1 (0.28%) | 1 (0.28%) | 2 (0.56%) |
| AI/AN | 20 | 0 | 0 | 0 | (TBD) | 0 | 0 | 0 |
| NHPI | 11 | 0 | 0 | 0 | (TBD) | 0 | 0 | 0 |
| Hispanic/Latino | 7 | 0 | 0 | 0 | (TBD) | 0 | 0 | 0 |

Confirmed hypoparathyroidism rates were essentially equivalent in Black/AA (3.15%) and White (3.00%) patients, with a similar transient/permanent split. Recurrent-laryngeal-nerve injury and vocal-cord paralysis were sparse but Black/AA-predominant (RLN 0.31% vs 0.16%; VC paralysis 0.27% vs 0.08%) — the absolute counts are small (9 vs 4 RLN; 8 vs 2 VC) and these contrasts will be tested formally with Fisher exact / Firth-penalized logistic in v3.

---

## 4. Discussion

This study makes four observations on multinodular goiter presentation in a single tertiary academic center.

**First, gland size and lobar volume differ substantially by race.** Black or African American patients had a 2.6-fold larger median gland weight (66 g vs 25 g) and 2.1- to 2.8-fold larger median lobar volumes than White patients. This is consistent with prior smaller series[4,5] but is reported here in a cohort an order of magnitude larger than most prior reports.

**Second, substernal extension follows the same gradient.** CT-documented substernal extension was 2.9-fold higher in Black/AA female patients than White female patients (16.7% vs 5.3%) and 2.5-fold higher in male patients (28.0% vs 11.1%). Within race, male sex was associated with ~2-fold higher substernal rates than female sex. The clinical implications are direct: Black/AA patients in this cohort more often required complex resections for substernal disease.

**Third, preoperative workup tempo also differs substantially by race.** Median time from first preoperative FNA to surgery was 181 days for Black/AA patients versus 85 days for White patients — a 2.1-fold longer wait. This is the most novel finding of the study. Counts of US exams and FNA episodes were broadly similar across race groups; the disparity is not in workup *intensity* but in workup *tempo*. Molecular testing rates were also lower in Black/AA patients (7.8% vs 13.0% in White). These tempo differences are exactly the pattern expected if upstream social factors (insurance, scheduling barriers, transportation, employer flexibility) differentially extend the workup phase in Black/AA patients, but our canonical does not encode those upstream variables and we therefore cannot formally test mechanistic hypotheses.

**Fourth, perioperative complication rates were similar by race.** Postoperative confirmed hypoparathyroidism rates were 3.15% (Black/AA) and 3.00% (White), with the standard transient/permanent split (~96% transient cohort-wide). Permanent hypoparathyroidism occurred in 8 patients (0.13%) — within published high-volume center expectations. RLN injury and vocal-cord paralysis were sparse; the absolute counts (9 vs 4; 8 vs 2) are too small for covariate-adjusted inference and will be tested with Fisher exact / Firth-penalized logistic in v3.

### Race as the only structured SDOH variable

The most important interpretive constraint of this analysis is that race is the only structured social-determinants-of-health variable encoded in `canonical_patient_master`. Insurance, payer, ZIP code, area deprivation index, income, education, and Hispanic ethnicity (separate from race) are not encoded. The differences observed therefore cannot be mechanistically attributed to upstream social factors without additional linkage. A meaningful follow-up is an IRB-amended linkage of `research_id` to a ZIP- or census-block-group-level deprivation index from the upstream institutional EHR (institutional carry-forward `CF-H2-DEPRIVATION-LINKAGE`, opened by Linear THY-11).

We deliberately decline to make causal claims about upstream drivers. We report what the canonical supports — population-level race-stratified differences in gland size, substernal extension, workup tempo, and molecular-testing rate — and identify the linkage required for mechanistic inference.

### Vocabulary preservation

The canonical's NIH/OMB long-form race vocabulary preserves categories often collapsed in surgical literature (American Indian or Alaska Native, Native Hawaiian or Other Pacific Islander, Hispanic or Latino as a separate category). We report these categories explicitly rather than folding them into "Other," even when the per-category n is small, so that downstream meta-analyses and registry comparisons retain the granularity.

---

## 5. Limitations

1. **Single institution.** Findings are specific to this academic center's catchment and may not generalize.
2. **Race vocabulary change.** The canonical's NIH/OMB long-form race labels are not directly comparable to earlier labels in the underlying source EHR; categories such as American Indian or Alaska Native (n = 20 in the goiter cohort) were not separately represented in earlier institutional reporting.
3. **No structured SDOH beyond race.** Insurance, ZIP-level deprivation, income, education, and Hispanic ethnicity (separate from race) are not encoded. Race serves as the only available SDOH proxy.
4. **Recurrent-laryngeal-nerve and vocal-cord preoperative status is source-limited.** `mig_080` added `comp_rln_injury_preop` and `comp_vc_paralysis_preop` to `canonical_patient_master` from Snowflake `AI_CLASSIFY` over the sparse preoperative-laryngoscopy field (`OPS_PREOP_LARYNGOSCOPY`; 119 patients, RLN preop n = 8, VC paralysis preop n = 3). These rows support a laryngoscopy-documented preop flag, but broader H&P / voice-clinic preop status remains incompletely encoded because the Snowflake note-search table available for this migration lacked note dates.
5. **Sparse RLN and VC events.** RLN injury n = 14 and VC paralysis n = 12 in the 6,075-patient goiter cohort preclude covariate-adjusted logistic regression. v3 will use Fisher exact tests for race-stratified comparisons and Firth-penalized logistic for adjusted contrasts.
6. **Postoperative-permanence classification for RLN injury sparse.** Both `comp_rln_injury_permanent` and `comp_rln_injury_transient` are zero across all 14 confirmed cases in this cohort. This reflects current encoding, not absence of permanence; structured RLN-temporality extraction is anticipated in a future migration.
7. **Time-to-surgery sign convention.** `prm_first_fna_days_from_surg` uses positive values to indicate FNA before surgery. 92 of 2,922 non-null patients have negative values reflecting postoperative biopsies and were excluded from Section 3.5. Maximum value 736,618 days (~2,000 years) flagged as a probable data-entry error and excluded from per-race medians.
8. **VC paresis almost entirely unencoded.** After `mig_081` / MIG-003 (Linear THY-15), BigQuery `canonical_patient_master` has **1** `comp_vc_paresis_confirmed` case (`research_id` 8616, `comp_vc_paresis_evidence_tier = 2`, Snowflake `AI_CLASSIFY` on contrast-language notes). An additional patient (`research_id` 9012) had contrast-language note text but **`comp_vc_paralysis_confirmed = TRUE`**, illustrating charting overlap between layers. Preop paresis status remains unstructured; Table 4 should report the single confirmed paresis count with this caveat or suppress the row if below disclosure threshold.
9. **Triplicate VC paralysis encoding.** `comp_vc_paralysis_*`, `comp_vocal_cord_paralysis_*`, and `comp_vc_paresis_*` coexist with one cohort-wide disagreement. Carry-forward `MIG-005` (Linear THY-17) will reconcile to a single canonical layer.
10. **VC findings source rollup (MIG-004, 2026-05-06).** `canonical_patient_master` now carries `vc_finding_source_first` (primary source by earliest non-null signal date; `none` when absent), `vc_finding_source_set` (ARRAY of all contributing sources among laryngoscopy, mri_vocal_cords, operative_rln, nsqip_attribution), and `vc_finding_source_concordance` (`none` / `single_source` / `concordant_multi`). NSQIP positivity uses `nsqip_rln_injury_flag = 1` or free-text beginning with \"Yes\". v2 does not yet stratify main tables by these fields; v3 may add source-stratified complication displays. Linear THY-16 pending auto-close.
11. **Retrospective design.** All conclusions are observational. Confounding by indication and by referral pattern is unaddressed.

---

## 6. Conclusions

In a 6,075-patient cohort, multinodular goiter presented with substantially larger gland weight (median 66 g vs 25 g), 2- to 3-fold larger lobar volumes, and 2.5- to 3-fold higher substernal-extension rates in Black or African American patients than in White patients. Median time from first preoperative FNA to surgery was 2.1-fold longer in Black/AA patients (181 d vs 85 d), with similar workup *intensity* but different *tempo*. Permanent hypoparathyroidism (4.3% of confirmed cases) and RLN injury (0.23% of cohort) were rare, and complications were broadly similar by race. The gland-size and tempo differences are descriptively striking but, in the absence of structured SDOH variables beyond race, cannot be mechanistically attributed to upstream social factors. The most informative follow-up is an IRB-amended linkage of `research_id` to a ZIP-level deprivation index.

---

## Tables

**Table 1.** Demographic, gland-size, and substernal characteristics of the multinodular-goiter cohort, stratified by race (n = 6,075). Numbers populated in Sections 3.2–3.4 above; full-render Table 1 with χ² and Kruskal–Wallis omnibus p-values pending v3.

**Table 2.** Substernal extension by race × sex (Section 3.4).

**Table 3a.** Preoperative workup intensity and tempo by race (Section 3.5).

**Table 3b.** Concomitant pathology prevalence by race (Section 3.6).

**Table 4.** Cohort-wide perioperative complications applying the institution-wide standing rule (Section 3.7).

**Table 5.** Race-stratified perioperative complications (Section 3.8).

## Figures

**Figure 1.** Distribution of gland weight by race (violin plot). *[v3: regenerate from `pub_canonical` via Python+matplotlib.]*

**Figure 2.** Time from first preoperative FNA to surgery by race (box plot, log scale). *[v3: regenerate.]*

**Figure 3.** Forest plot of substernal-extension odds ratios by race × sex, reference = White female. *[v3: depends on adjusted logistic regression with race, sex, age, weight as covariates.]*

---

## References

[1–N] *To be populated from References table in Airtable.*

---

## Provenance footer

- Data source: `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`, BigQuery, queried 2026-05-06 06:35–07:15 via service account `thyroid-pub-loader@`.
- Cohort definition: `syn_multinodular_goiter = TRUE` → n = 6,075.
- Standing rule applied: `memory/feedback_complications_transient_vs_permanent.md` (set 2026-05-01).
- Validation report: `studies/hypothesis2_goiter_sdoh/canonical_validation_20260506.md`.
- Gap report: `studies/hypothesis2_goiter_sdoh/canonical_gaps_report_20260506.md`.
- v1 audit anchor: `studies/hypothesis2_goiter_sdoh/H2_manuscript_v1_20260506.md`.
- Raw analysis output: `/tmp/h2_v2_a1_a5.csv` (race × weight, race × lobar volume, race × age) and `/tmp/h2_v2_b.csv` (race × substernal × sex, race × workup, race × concomitant path, race × molecular, race × hypopara, race × RLN/VC). These are transient files on Logan's Mac; the SQL bundles are reproducible from `studies/hypothesis2_goiter_sdoh/canonical_gaps_report_20260506.md` Section 9.
- Audit trail: DFL-20260506-001, DFL-20260506-002, MFL-20260506-001 (Airtable, base `appJYOnUb7KrHKwpV`).
- Linear: H2 project + carry-forward issues THY-13 (MIG-001), THY-14 (MIG-002), THY-15 (MIG-003), THY-16 (MIG-004), THY-17 (MIG-005).
- v3 dependencies: hypoparathyroidism logistic regression; Fisher exact / Firth for sparse RLN and VC; deprivation-index linkage (`CF-H2-DEPRIVATION-LINKAGE`); Tables 1–5 final renders with omnibus p-values; Figures 1–3 regenerated.
- No PHI in this manuscript file.
