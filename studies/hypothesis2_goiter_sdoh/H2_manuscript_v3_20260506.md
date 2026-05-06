# Racial differences in multinodular goiter presentation, preoperative workup tempo, and surgical outcomes: a single-institution analysis of 6,075 patients

**Manuscript code:** H2  
**Version:** v3 (2026-05-06)  
**Source data:** `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` (BigQuery)  
**Cohort:** `syn_multinodular_goiter = TRUE` on canonical_patient_master, n = 6,075  
**Audit trail:** Manuscript Feedback Log MFL-20260506-003 (pre-write); carries MFL-20260506-001; Data Feedback Logs DFL-20260506-001 / DFL-20260506-002.  
**Carries forward from v2:** `H2_manuscript_v2_20260506.md` — v3 replaces inferential [TBD] placeholders with live `pub_canonical` statistics (`studies/hypothesis2_goiter_sdoh/build_h2_v3.py`, `h2_v3_stats.json`).

---

## Authors

[TBD — populate from Co-Authors table]

## Corresponding author

[TBD]

---

## Abstract

**Background.** Multinodular goiter (MNG) is a common indication for thyroidectomy. Whether and how patient race correlates with MNG presentation — including gland dimensions, gland weight, substernal extension, preoperative workup intensity and tempo, concomitant pathology, and perioperative outcomes — is poorly characterized in cohorts where structured social-determinants-of-health (SDOH) data are limited.

**Methods.** Retrospective cohort analysis of 6,075 patients with multinodular goiter who underwent thyroidectomy at a single tertiary academic center. The analytic dataset was extracted from a publication-grade BigQuery canonical (`thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`). Race vocabulary follows NIH/OMB long-form encoding. Gland dimensions are operative-pathology lobar measurements (length / width / height / volume). Gland weight is `gland_weight_final_g`, a refined cross-source rollup. Substernal extension is modality-stratified (CT and MRI). Preoperative workup variables include number of US exams, number of FNA episodes, worst preoperative Bethesda 2015, molecular-testing rate, and time from first FNA to surgery. Complications follow the institution-wide standing rule (transient versus permanent hypoparathyroidism; postoperative-confirmed plus preoperative yes/no for hypocalcemia; laryngoscopy-documented preoperative flags for RLN injury and vocal-cord paralysis where encoded). Race-stratified comparisons used χ² tests for categorical variables and Kruskal–Wallis tests for continuous variables (complete formulations in §2.5). Pairwise Black–White contrasts for continuous variables used two-sided Mann–Whitney U tests. Sparse RLN injury and vocal-cord paralysis contrasts (Black or African American vs White) used Fisher exact tests with Table2×2 odds ratios and 95% CIs. Hypoparathyroidism (binary confirmed) used multivariable logistic regression adjusting for race (indicator variables; White reference), sex (male vs female reference), age at surgery, gland weight, and CT substernal extension (`ct_substernal_extension_any`).

**Results.** Of 6,075 patients (4,930 [81.2%] female; median age 54 years), race distribution was Black or African American 2,918 (48.0%); White 2,500 (41.2%); Unknown or Not Reported 355 (5.8%); Asian 193 (3.2%); Other 67 (1.1%); American Indian or Alaska Native 20 (0.3%); Native Hawaiian or Other Pacific Islander 11 (0.2%); Hispanic or Latino 7 (0.1%); NULL race 4 (0.1%). **Median gland weight differed substantially by race** (Kruskal–Wallis *H* = 801.0, *p* < 0.001): Black or African American 66 g (IQR 33–137) versus White 25 g (IQR 14–52). **Substernal extension** (CT or MRI) varied by race (χ² = 192.9, *p* < 0.001) with the race × sex pattern summarized in Table 2. **Preoperative workup tempo:** among patients with strictly positive preoperative FNA-to-surgery intervals (FNA before surgery, values > 0 and < 10,000 days; *n* = 2,816), median time from first FNA to surgery was 197 days for Black or African American versus 90 days for White patients (Kruskal–Wallis *H* = 146.8, *p* < 0.001; Mann–Whitney Black vs White *p* < 0.001). **Multivariable hypoparathyroidism:** adjusted odds ratio for Black or African American versus White = 1.13 (95% CI 0.81–1.57; Wald *p* = 0.47), with strong association of CT-documented substernal extension (OR 2.00, 95% CI 1.34–2.98). **RLN injury** Fisher OR (Black or African American vs White) = 1.93 (95% CI 0.59–6.28), *p* = 0.40; **vocal-cord paralysis** OR = 3.43 (95% CI 0.73–16.2), *p* = 0.12.

**Conclusions.** In a 6,075-patient cohort, multinodular goiter presented with substantially larger gland weight, larger lobar volumes, and higher substernal-extension rates in Black or African American patients than in White patients. Preoperative workup tempo also differed: FNA-to-surgery intervals were longer among Black/AA patients in the analyzable-positive-interval subset. After adjustment, Black/AA hypoparathyroidism odds were not higher than White; CT substernal extension remained positively associated with confirmed hypoparathyroidism. These differences cannot be mechanistically attributed to upstream social factors without additional SDOH linkage.

**Keywords:** multinodular goiter, thyroidectomy, racial disparities, substernal goiter, time to surgery, social determinants of health, hypoparathyroidism

---

## 1. Introduction

*(Unchanged from v2 — see `H2_manuscript_v2_20260506.md` §1.)*

Multinodular goiter (MNG) is one of the most common surgical indications for thyroidectomy worldwide. While the natural history and surgical management of MNG are well described, structured analyses of how MNG presents across race groups in the surgical literature are limited, especially in cohorts that lack the upstream social-determinants-of-health variables (insurance, area deprivation index, income, education) that would mechanistically explain any disparities observed.

Disparities literature in thyroid surgery has focused largely on differentiated thyroid cancer presentation, stage at diagnosis, and survival.[1–3] In benign disease, smaller studies have reported that Black or African American patients present with larger goiters and with substernal extension at higher rates than White patients,[4,5] but most reports rely on cohort definitions that are not directly comparable, and few combine presentation metrics with preoperative workup tempo.

This study uses a publication-grade BigQuery canonical (10,871 patients; goiter subset 6,075) to characterize race-stratified MNG presentation, preoperative workup, and perioperative outcomes. We report time from first preoperative FNA to surgery as a tempo metric (`prm_first_fna_days_from_surg`). We adopt the institution-wide complication-temporality reporting rule (transient/permanent hypoparathyroidism; preoperative yes/no for hypocalcemia where encoded; sparse laryngoscopy-documented preoperative RLN and vocal-cord paralysis flags after `mig_080`). We are explicit that race is the only structured SDOH variable available.

### Specific aims

1. Characterize the distribution of race, sex, age, gland dimensions, gland weight, and substernal extension in the operative MNG cohort.  
2. Test whether gland dimensions, gland weight, and substernal extension differ by race (omnibus and stratified χ² / Kruskal–Wallis).  
3. Characterize preoperative workup intensity and tempo by race with omnibus tests.  
4. Report concomitant benign and malignant pathology by race.  
5. Report perioperative complication rates by race using the standing rule, with Fisher exact tests for sparse Black–White contrasts and adjusted logistic regression for hypoparathyroidism.  
6. Articulate inferential limits imposed by missing SDOH beyond race and source-limited preoperative flags.

---

## 2. Methods

### 2.1 Data source

*(As v2.)* Analytic dataset from `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` (service account `thyroid-pub-loader@thyroid-canonical-pub-2026.iam.gserviceaccount.com`). Frozen reference: `pub_legacy_source_20260416`.

### 2.2 Cohort definition

MNG cohort = `syn_multinodular_goiter = TRUE`, *n* = 6,075.

### 2.3 Variables

*(As v2 — gland dimensions, weight, substernal CT/MRI, workup, pathology.)* Concomitant malignancy: `any_concomitant_malignant` on `canonical_path_benign_patient_rollup_v1` joined by `research_id`.

### 2.4 Complication outcomes — institution standing rule

*(As v2 / `memory/feedback_complications_transient_vs_permanent.md`.)* In the goiter cohort, preoperative laryngoscopy-encoding positives: `comp_rln_injury_preop` *n* = 5; `comp_vc_paralysis_preop` *n* = 1 (sparse; source-limited).

### 2.5 Statistical analysis (v3 complete)

Analyses used Python 3.14 with `pandas`, `scipy.stats`, and `statsmodels` against a cohort extract from BigQuery (reproducible SQL in `studies/hypothesis2_goiter_sdoh/build_h2_v3.py`). Random seed 42.

- **Categorical × race:** Pearson χ² tests of independence on multi-row × multi-column contingency tables (race × sex; race × binary indicator).  
- **Continuous × race:** Kruskal–Wallis *H* across all non-missing race categories (patients with NULL race excluded from race-stratified inferential summaries; *n* = 6,071 for race-stratified models).  
- **Pairwise Black–White (continuous):** two-sided Mann–Whitney U (exploratory; uncorrected *p*-values reported in `h2_v3_stats.json`).  
- **FNA timing:** Positive-interval subset: `prm_first_fna_days_from_surg` > 0, < 10,000 days (excludes postoperative biopsies and extreme outliers).  
- **Hypoparathyroidism (confirmed):** `statsmodels.discrete.discrete_model.Logit`; maximum likelihood via L-BFGS; reference race White; reference sex female; median imputation for `age_at_surgery` and `gland_weight_final_g` only; `ct_substernal_extension_any` coerced to {0,1}; *n* = 6,071, 187 events.  
- **Sparse RLN and VC paralysis:** 2×2 Fisher exact (Black or African American vs White); odds ratios and 95% CIs via `statsmodels.stats.contingency_tables.Table2x2`.  
- **Substernal race × sex (Figure 3):** separate logistic model with indicator for each race × sex combination with cell size ≥ 30; reference White female. Two-sided *α* = 0.05.

### 2.6 Ethics and governance

*(As v2.)* IRB [TBD]. No PHI in external trackers; append-only logs; MFL-20260506-003 logged before v3 write.

---

## 3. Results

### 3.1 Cohort

6,075 multinodular-goiter patients; 4,930 (81.2%) female; median age 54 years.

### 3.2 Race distribution

Race counts as in Abstract. Median age differed by race (Kruskal–Wallis *H* = 76.45, *p* < 0.001): Black or African American median 54 (IQR 45–64) vs White 58 (IQR 45–68).

### 3.3 Gland dimensions and weight

Median gland weight differed by race (Kruskal–Wallis *p* < 0.001); Black or African American 66 g (IQR 33–137) vs White 25 g (IQR 14–52). Operative-pathology lobar volumes also differed (left lobe *H* = 496.5, *p* < 0.001; right lobe *H* = 378.8, *p* < 0.001). **Table 1** summarizes demographics and gland metrics with omnibus *p*-values footnoted.

### 3.4 Substernal extension

Race × sex cell proportions unchanged from v2 (Table 2). **Among females:** χ² = 163.8, *p* < 0.001 (*n* = 4,926). **Among males:** χ² = 47.0, *p* < 0.001 (*n* = 1,145).

### 3.5 Preoperative workup intensity and tempo

Mean US exams and FNA episodes differed modestly by race (Kruskal–Wallis *p* = 4.3×10⁻⁴ and *p* = 1.3×10⁻⁵, respectively). Molecular-testing rate differed (χ² = 63.4, *p* < 0.001). **FNA timing:** in the positive-interval analytic subset (*n* = 2,816), Kruskal–Wallis *H* = 146.8, *p* < 0.001; Table 3a reports medians. Figure 2 visualizes the positive-interval distribution.

### 3.6 Concomitant pathology

Race-stratified prevalences and χ² *p*-values in **Table 3b**. Concomitant malignancy (`any_concomitant_malignant`) varied by race (χ² = 267.9, *p* < 0.001).

### 3.7 Perioperative complications — standing rule

**Table 4** (cohort-wide). Hypoparathyroidism confirmed *n* = 187 (3.08%): transient 179 (95.7% of confirmed), permanent 8 (4.3%). Preoperative hypoparathyroidism flag (`comp_hypoparathyroidism_preexisting`) *n* = 31. Hypocalcemia postop confirmed *n* = 4; preop hypocalcemia-related encoding *n* = 20. RLN injury confirmed *n* = 14; VC paralysis *n* = 12; **VC paresis confirmed *n* = 1** (White cohort cell in race table). Preop RLN / VC paralysis (*n* = 5 / 1) remain source-limited to laryngoscopy encoding.

### 3.8 Race-stratified complications

**Table 5.** Omnibus χ² for hypoparathyroidism confirmed × race: χ² = 2.86, *p* = 0.90. **Fisher exact (Black or African American vs White):** RLN injury OR = 1.93 (95% CI 0.59–6.28), *p* = 0.40; VC paralysis OR = 3.43 (95% CI 0.73–16.2), *p* = 0.12.

### 3.9 Adjusted hypoparathyroidism model

Multivariable logistic regression (*n* = 6,071): **Black or African American vs White OR = 1.13 (95% CI 0.81–1.57; Wald *p* = 0.47).** CT substernal extension OR = 2.00 (95% CI 1.34–2.98). Male sex OR = 1.07 (95% CI 0.73–1.56). Per-year age OR = 1.005 (95% CI 0.995–1.016). Per-gram gland weight OR = 0.996 (95% CI 0.994–0.999). Full coefficient table in `h2_v3_stats.json`.

---

## 4. Discussion

Core interpretive points match v2: large gland and substernal gradients by race; **longer FNA-to-surgery intervals in the positive-interval subset** for Black/AA versus White; **no elevation in adjusted hypoparathyroidism odds** for Black/AA versus White despite univariate similarity in event rates; sparse RLN/VC cells limit precision (wide Fisher CIs). Race remains the only structured SDOH variable (`CF-H2-DEPRIVATION-LINKAGE`).

---

## 5. Limitations

*(Largely as v2; numeric updates.)* (1) Single institution. (2) Race vocabulary shift vs historic EHR. (3) No structured SDOH beyond race. (4) Preop RLN/VC flags sparse (*n* = 5 / 1 preop positives in goiter cohort on BQ at v3 pull). (5) RLN/VC events rare — Fisher CIs wide. (6) RLN trans/perm encoding sparse. (7) FNA-interval sign convention; extreme outliers excluded. (8) VC paresis *n* = 1. (9–11) Triplicate VC encodings / MIG-005; `vc_finding_source_*`; retrospective design.

---

## 6. Conclusions

Black or African American patients presented with larger glands and more substernal extension; preoperative FNA-to-surgery intervals were longer in the analyzable positive subset. Adjusted hypoparathyroidism did not differ by race versus White; CT substernal extension was associated with higher hypoparathyroidism odds. SDOH linkage remains the priority follow-up.

---

## Tables

### Table 1. Demographics, gland size, and substernal extension by race (goiter cohort)

| Race | n | Female n (%) | Age, median (IQR), y | Gland wt, g median (IQR) | CT substernal n (%) | MRI substernal n (%) | Any CT/MRI n (%) |
|------|---|--------------|----------------------|----------------------------|----------------------|----------------------|------------------|
| Black or African American | 2,918 | 2,454 (84.1%) | 54 (45–64) | 66 (33–137) | 540 (18.5%) | 59 (2.0%) | 582 (19.9%) |
| White | 2,500 | 1,949 (78.0%) | 58 (45–68) | 25 (14–52) | 165 (6.6%) | 37 (1.5%) | 195 (7.8%) |
| Unknown or Not Reported | 355 | 271 (76.3%) | 51 (41–61) | 36 (18–82) | 42 (11.8%) | 4 (1.1%) | 44 (12.4%) |
| Asian | 193 | 160 (82.9%) | 49 (40–60) | 27 (16–50) | 5 (2.6%) | 2 (1.0%) | 7 (3.6%) |
| Other | 67 | 59 (88.1%) | 56 (45–68) | 38 (17–70) | 12 (17.9%) | 1 (1.5%) | 13 (19.4%) |
| American Indian or Alaska Native | 20 | 18 (90.0%) | 52 (41–62) | 42 (22–98) | 0 | 0 | 0 |
| Native Hawaiian or Other Pacific Islander | 11 | 9 (81.8%) | 51 (44–58) | 23 (16–39) | 0 | 0 | 0 |
| Hispanic or Latino | 7 | 6 (85.7%) | 39 (30–57) | 23 (22–83) | 0 | 0 | 0 |
| NULL race | 4 | 4 (100%) | 40 (32–47) | 6 (5–8) | 1 (25.0%) | 0 | 1 (25.0%) |

**Omnibus tests** (non-NULL race, *n* = 6,071): sex × race χ² = 42.17, df = 7, *p* = 4.81×10⁻⁷; age Kruskal–Wallis *H* = 76.45, *p* < 0.001; gland weight *H* = 801.0, *p* < 0.001; left lobe volume *H* = 496.5, *p* < 0.001; right lobe volume *H* = 378.8, *p* < 0.001; any substernal × race χ² = 192.9, *p* < 0.001; CT substernal × race χ² = 199.3, *p* < 0.001.

---

### Table 2. Substernal extension by race × sex *(v2 cell counts; inferential tests §3.4)*

| Race × sex | n | CT substernal | MRI substernal | Any (CT or MRI) |
|------------|---|---------------|----------------|-----------------|
| Black or African American — female | 2,454 | 410 (16.7%) | 49 (2.0%) | 445 (18.1%) |
| Black or African American — male | 464 | 130 (28.0%) | 10 (2.2%) | 137 (29.5%) |
| White — female | 1,949 | 104 (5.3%) | 23 (1.2%) | 122 (6.3%) |
| White — male | 551 | 61 (11.1%) | 14 (2.5%) | 73 (13.2%) |
| Asian — female | 160 | 4 (2.5%) | 1 (0.6%) | 5 (3.1%) |
| Asian — male | 33 | 1 (3.0%) | 1 (3.0%) | 2 (6.1%) |
| Other / smaller groups | — | *(Supplement)* | | |

**Stratified χ² (race × binary any substernal):** females χ² = 163.76, *p* < 0.001; males χ² = 46.99, *p* < 0.001.

---

### Table 3a. Preoperative workup by race

| Race | n | Mean US exams | Mean FNA episodes | Median days first FNA→sx* | Molecular tested % |
|------|---|---------------|-------------------|---------------------------|-------------------|
| Black or African American | 2,918 | 2.37 | 0.85 | **197** | 7.8% |
| White | 2,500 | 2.14 | 0.80 | **90** | 13.0% |
| Asian | 193 | 2.51 | 0.87 | 88 | 19.2% |
| Other | 67 | 2.34 | 0.75 | 87 | 19.4% |
| Unknown or Not Reported | 355 | 1.72 | 0.54 | 105 | 12.4% |
| American Indian or Alaska Native | 20 | 1.62 | 1.05 | 209 | 5.0% |
| NHPI | 11 | 2.09 | 0.36 | 564 | 0% |
| Hispanic or Latino | 7 | 1.0 | 0.14 | 301 | 0% |

\*Positive-interval subset: FNA before surgery, 0 < days < 10,000; medians computed on patients with non-missing values in that subset.

**Omnibus:** US exam count *H* = 26.38, *p* = 4.31×10⁻⁴; FNA episode count *H* = 34.61, *p* = 1.32×10⁻⁵; molecular tested χ² = 63.41, *p* < 0.001; FNA interval (positive subset *n* = 2,816): *H* = 146.78, *p* < 0.001.

---

### Table 3b. Concomitant pathology by race

| Race | n | Hashimoto % | Graves % | Follicular adenoma % | Concomitant malignant % |
|------|---|------------|----------|----------------------|-------------------------|
| Black or African American | 2,918 | 0.48% | 1.23% | 3.91% | 17.3% |
| White | 2,500 | 1.68% | 0.96% | 5.48% | 34.4% |
| Asian | 193 | 0.52% | 1.55% | 3.63% | 50.3% |
| Other | 67 | 1.49% | 2.99% | 8.96% | 34.3% |
| Unknown / NR | 355 | 0.28% | 1.69% | 4.51% | 27.0% |
| AI/AN | 20 | 5.0% | 0 | 0 | 15.0% |
| NHPI | 11 | 0 | 0 | 9.1% | 27.3% |
| Hispanic/Latino | 7 | 0 | 0 | 0 | 14.3% |

**χ² race × flag:** Hashimoto χ² = 25.83, *p* = 5.41×10⁻⁴; Graves χ² = 4.49, *p* = 0.72; follicular adenoma χ² = 12.65, *p* = 0.081; concomitant malignant χ² = 267.85, *p* < 0.001.

---

### Table 4. Cohort-wide complications (standing rule)

| Outcome | Postop confirmed | Transient | Permanent | Preop / pre-existing flag |
|---------|------------------|-----------|-----------|---------------------------|
| Hypoparathyroidism | 187 (3.08%) | 179 | 8 | 31 (preexisting rollup) |
| Hypocalcemia (clinical) | 4 (0.07%) | — | — | 20 (preop encoding) |
| RLN injury | 14 (0.23%) | — | — | 5 (preop laryngoscopy-encoded) |
| Vocal-cord paralysis | 12 (0.20%) | — | — | 1 (preop) |
| Vocal-cord paresis | 1 (0.02%) | — | — | not structured |

---

### Table 5. Race-stratified complications

| Race | n | Hypopara conf. | Transient | Permanent | Hypopara preop | Hypocalc. conf. | RLN | VC paralysis | VC paresis |
|------|---|----------------|-----------|---------|----------------|-----------------|-----|--------------|------------|
| Black or African American | 2,918 | 92 (3.15%) | 88 | 4 | 13 | 1 (0.03%) | 9 | 8 | 0 |
| White | 2,500 | 75 (3.00%) | 72 | 3 | 13 | 2 (0.08%) | 4 | 2 | 1 |
| Asian | 193 | 4 (2.07%) | 3 | 1 | 2 | 1 (0.52%) | 0 | 0 | 0 |
| Other | 67 | 2 (3.0%) | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| Unknown / NR | 355 | 14 (3.94%) | 14 | 0 | 3 | 1 (0.28%) | 1 | 2 | 0 |
| AI/AN | 20 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| NHPI | 11 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Hispanic/Latino | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| NULL race | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

**Omnibus χ²** hypoparathyroidism × race: χ² = 2.86, *p* = 0.898. **Fisher (Black/AA vs White):** RLN OR = 1.93, *p* = 0.405; VC paralysis OR = 3.43, *p* = 0.120.

---

## Figures

- **Figure 1.** `figures_v3/figure_1_gland_weight_by_race.png` (and `.svg`) — violin plot, log *y*; race groups with *n* ≥ 11.  
- **Figure 2.** `figures_v3/figure_2_fna_days_by_race.png` (and `.svg`) — box plot, positive FNA intervals only, log *y*; median annotations.  
- **Figure 3.** `figures_v3/figure_3_substernal_forest_race_sex.png` (and `.svg`) — forest of substernal ORs vs **White female**; cells with combined race × sex *n* ≥ 30.

---

## References

[1–N] *To be populated from References table in Airtable.*

---

## Provenance footer

- **Build:** `studies/hypothesis2_goiter_sdoh/build_h2_v3.py` → `h2_v3_stats.json`, `figures_v3/`.  
- **Data:** `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` + `canonical_path_benign_patient_rollup_v1`, queried 2026-05-06 (v3 session).  
- **Cohort:** `syn_multinodular_goiter = TRUE`, *n* = 6,075.  
- **Standing rule:** `memory/feedback_complications_transient_vs_permanent.md`.  
- **v2 anchor:** `H2_manuscript_v2_20260506.md`.  
- **MFL:** MFL-20260506-003 (`recMVyzUnachPjxzI`).  
- **No PHI** in this manuscript file.  
