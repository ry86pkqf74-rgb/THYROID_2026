---
manuscript_id: M038-DEF-WT200
title_scaffold: >-
  Massive goiter defined by final pathology gland weight ≥200 g —
  demographic and clinical correlates in a 25-year thyroid surgery cohort
lane: mig_276
status: >-
  Scaffold v1 (Methods + Results only). Introduction, Discussion, Abstract,
  and References deferred to LG / companion edits.
cohort_spine: main.canonical_patient_master (n = 10,871 distinct research_id)
analysis_slice: >-
  Patients with non-null final pathology gland weight (n = 9,130);
  weight strata per mig_273 / Snowflake COHORT_M038_MASSIVE_GOITER lineage.
snowflake_report: snowflake_trial/reports/m038_table1_massive_goiter.md
generated_at: 2026-05-03
table1_generated_at_report: 2026-05-01 18:34:46
---

# M038 — Weight-stratified massive goiter (≥200 g): Methods and Results (draft scaffold)

**Authoring split.** This file contains **§2 Methods** and **§3 Results** for the definition-paper arm that operationalizes “massive goiter” as **final gland weight ≥200 g**, with patients stratified by pathology weight (massive ≥200 g; moderate 50–199 g; small \<50 g). **Introduction, Discussion, Abstract, and References are intentionally omitted here** (LG / Cowork companion).

---

## 2. Methods

### 2.1 Study design and population

We analyzed a retrospective cohort of patients undergoing thyroid surgery within the Thyroid 2026 canonical surgical spine (`main.canonical_patient_master`; **N = 10,871** distinct `research_id`, 1999–2025 accrual window per registry conventions). The analytic subset restricted to patients with **non-missing final pathology gland weight** (`gland_weight_final_g IS NOT NULL`), yielding **N = 9,130 (84.0%)**. Patients with missing gland weight (**N = 1,741; 16.0%**) were excluded from the primary tabulations reported here and will be characterized separately (coverage audit; overlap with institutional synoptic completeness).

### 2.2 Exposure and weight strata

Following institutional manuscript conventions ratified for the Snowflake replication pipeline (**mig_273**; view `main.cohort_m038_massive_goiter_v1` on MotherDuck; **`THYROID_VALIDATION.PUBLIC.COHORT_M038_MASSIVE_GOITER`** on Snowflake), patients with documented weight were classified:

| Stratum | Definition | N |
| --- | --- | ---: |
| Massive | ≥200 g | 475 |
| Moderate | 50–199 g | 2,467 |
| Small | \<50 g | 6,188 |

The **≥200 g** threshold aligns with commonly cited surgical definitions of “massive” or “giant” goiter in the literature (typically cited ranges **≥150–200 g**); the conservative **200 g** cut point was selected for primary reporting to reduce false-positive labeling of modest multinodular glands.

### 2.3 Data sources

Structured fields were harmonized in the publication-tier database **`thyroid_canonical_publication_v1_0`** (`main` schema). Gland weight and multinodular disease indicators derive from synoptic pathology feeds; malignancy and AJCC 8th-edition stage group fields reflect the canonical patient master rollup; surgical procedure type reflects operative-spine harmonization (**post–mig_253** procedure-type recovery in the broader manuscript chain); cross-sectional imaging descriptors (e.g., CT goiter present) reflect structured radiology/imaging extraction layers. The descriptive aggregate reported here was generated from **Snowflake** against **`COHORT_M038_MASSIVE_GOITER`** with LN imaging rollup consistent with **post–mig_262** cervical lymph-node suspicious-flag rebuild (see footnote **F4**).

### 2.4 Variables

**Continuous:** gland weight (g), age at surgery (years), tumor size maximum (cm), follow-up duration (years; registry definition per survival/follow-up spine).

**Categorical:** sex; race/ethnicity as recorded in the institutional spine; malignant histology flag (`is_malignant`); pathologic multifocality (`multifocal_flag_path`); synoptic multinodular goiter flag; CT goiter present; AJCC 8 stage group (rollup); surgery type (total thyroidectomy, hemithyroidectomy, other/unknown categories as harmonized); receipt of radioactive iodine (`rai_received_flag`); any recurrence flag (`any_recurrence_flag`).

### 2.5 Outcomes (primary manuscript endpoints — staging)

**Primary:** Cross-stratum comparison of baseline demographic, oncologic, surgical, and recurrence-indicator distributions among weight strata.

**Secondary (pre-specified extensions, not fully tabulated in the archived Snowflake Table 1 export):**

- **Strict-definition perioperative complications** (`finding_status = 'present'` AND `evidence_strength IN ('definitive','probable')` on the canonical complication rollup; **mig_252** lineage). Complication rates **by weight stratum** require the complication × weight stratification query pack (not present in `m038_table1_massive_goiter.md`); manuscript sentences below reserve **[TBD: mig_252 × weight strata]**.
- **Surgical complexity proxies** (operative time, estimated blood loss, length of stay): columns **`cpm_op_time_min`**, **`cpm_ebl_ml`**, **`cpm_los_days`** are present on **`main.cohort_m038_massive_goiter_v1`** post–**mig_273**, with provenance fields **`cpm_*_source`**. Stratified reporting pending **mig_275** validation / QC — Results subsection **3.4** uses an explicit placeholder until those aggregates are signed off.

### 2.6 Statistical analysis

Across the three weight strata, **continuous** variables were compared with the **Kruskal–Wallis** test; **categorical** variables were compared with **Pearson chi-square** tests. All p-values are **two-sided**. Analyses were executed in the Snowflake Cortex reporting pipeline; the archived markdown summary is cited below. Multivariable logistic regression (any strict complication ~ massive ≥200 g, adjusted for age, sex, multifocality, malignancy, surgery type) is **planned** as a companion analytic chunk once complication stratification is frozen — not claimed as executed in this scaffold.

---

## 3. Results

### 3.1 Cohort and weight distribution

Among **9,130** patients with non-missing gland weight, **475 (5.2%)** met the **≥200 g** massive threshold. Observed weights in the massive stratum displayed **median 271.5 g** **[230.0–362.5 interquartile range]** with **mean ± SD 323.0 ± 164.4 g** (range **200–2,320 g** per cohort extremes captured in the extraction spine). The moderate stratum (**50–199 g**) comprised **2,467 (27.0%)** patients (**median 86.5 g** **[64.5–123.0]**; **mean ± SD 97.5 ± 39.4 g**). The small stratum (**\<50 g**) comprised **6,188 (67.8%)** patients (**median 19.0 g** **[11.8–29.3]**; **mean ± SD 21.3 ± 11.9 g**). Gland weight differed across strata (**p \< 0.0001**, Kruskal–Wallis).

### 3.2 Demographics and tumor size

**Age** differed across strata (**p \< 0.0001**): massive **mean ± SD 56.8 ± 13.6 years**, **median 58.0 [47.0–67.0]**; moderate **52.6 ± 14.8**, **median 53.0 [42.0–64.0]**; small **50.5 ± 15.2**, **median 50.0 [39.0–62.0]**.

**Sex** differed (**p \< 0.0001**): in the massive stratum, **191 (40.2%)** were male vs **284 (59.8%)** female; moderate **601 male (24.4%)** vs **1,866 female (75.6%)**; small **1,191 male (19.2%)** vs **4,997 female (80.8%)**.

**Race** distributions differed (**p \< 0.0001**). Black or African American patients comprised **361 (76.0%)** of the massive stratum vs **1,476 (59.8%)** moderate vs **1,652 (26.7%)** small; White patients **86 (18.1%)** vs **745 (30.2%)** vs **3,626 (58.6%)**; Unknown or Not Reported **19 (4.0%)** vs **144 (5.8%)** vs **432 (7.0%)**; Asian **6 (1.3%)** vs **56 (2.3%)** vs **334 (5.4%)**; remaining Census categories each **≤0.4–1.3%** within massive.

**Maximum tumor size (cm)** differed (**p \< 0.0001**): massive **median 1.6 [0.5–9.0]** (**mean ± SD 4.5 ± 4.7**); moderate **median 3.0 [0.7–5.9]** (**3.5 ± 2.9**); small **median 1.6 [0.9–2.8]** (**1.9 ± 1.5**). *Interpretive note for drafting:* distributional overlap and skew—particularly in the massive arm—warrant cautious clinical interpretation of mean tumor size; manuscript polish may emphasize medians or winsorized summaries.

**Follow-up duration (years)** differed (**p \< 0.0001**) with **median 0.0 years [IQR 0.0–0.0]** reported in all three strata in this Snowflake summary (**means:** massive **0.7 ± 2.2**, moderate **1.3 ± 2.9**, small **2.0 ± 3.9**). This pattern reflects **registry censoring / capture heterogeneity** rather than literal absence of longitudinal records for all patients; oncologic time-to-event analyses must join **`canonical_survival_followup_v1`** / recurrence-resolved views under publication-safe cohort filters.

### 3.3 Pathology, imaging, staging, and treatment indicators

**Malignancy** prevalence differed (**p \< 0.0001**): **67 malignant (14.1%)** in massive vs **569 (23.1%)** moderate vs **3,146 (50.8%)** small.

**Pathologic multifocality** coding differed (**p \< 0.0001**): multifocal **TRUE** in **13 (2.7%)** massive vs **188 (7.6%)** moderate vs **1,153 (18.6%)** small; **FALSE** in **255 (53.7%)** vs **1,611 (65.3%)** vs **4,441 (71.8%)**; missing in **207 (43.6%)** vs **668 (27.1%)** vs **594 (9.6%)**.

**Synoptic multinodular goiter** flag differed (**p \< 0.0001**): **427 (89.9%)** massive vs **1,848 (74.9%)** moderate vs **2,968 (48.0%)** small flagged **TRUE**; remainder **missing** within stratum.

**CT goiter present** differed (**p \< 0.0001**): **227 (47.8%) TRUE**, **6 (1.3%) FALSE**, **242 (50.9%) missing** in massive vs **681 / 55 / 1,731** in moderate vs **483 / 427 / 5,278** in small.

**AJCC 8 stage group** distributions differed (**p \< 0.0001**) with sparse advanced-stage cells in the massive stratum (e.g., **IVB 20 (4.2%)**, **II 23 (4.8%)**, **I 24 (5.1%)**, **III 0**)—consistent with **lower malignant prevalence** and benign multinodular dominance; interpret alongside **AJCC IVA/IVC collapse** conventions on the publication spine (footnote **F2**).

**Surgery type** differed (**p \< 0.0001**): **total thyroidectomy 400 (84.2%)**, **hemithyroidectomy 74 (15.6%)**, **other 1 (0.2%)** in massive vs **1,788 / 672 / 6** moderate vs **3,508 / 2,621 / 55** small (plus sparse **unknown/isthmusectomy** rows in smaller strata).

**RAI received** differed (**p \< 0.0001**): **5 (1.1%) TRUE** in massive vs **91 (3.7%)** moderate vs **422 (6.8%)** small.

**Any recurrence flag** differed (**p \< 0.0001**): **6 (1.3%) TRUE** massive vs **69 (2.8%)** moderate vs **390 (6.3%)** small.

### 3.4 Surgical complexity (operative time, EBL, LOS)

**Placeholder — pending mig_275 QC.** MotherDuck view **`main.cohort_m038_massive_goiter_v1`** carries **`cpm_op_time_min`**, **`cpm_ebl_ml`**, and **`cpm_los_days`** with source columns **`cpm_op_time_min_source`**, **`cpm_ebl_ml_source`**, **`cpm_los_days_source`** (**mig_273**). Stratified summaries (median/IQR by weight bucket, non-parametric tests, missingness rates) will replace this paragraph once **mig_275** surgical-complexity scaffolding is signed off and replicated to Snowflake.

### 3.5 Complications (strict definition)

**Placeholder — mig_252 stratification.** Institutionally aligned **strict** complication burden (**present** AND **definitive/probable** evidence tier; **mig_252** rollup repair) **by weight bucket** is **not** enumerated in `snowflake_trial/reports/m038_table1_massive_goiter.md`. The archived note warns that **pre-repair** complication rollups **over-counted** negated NLP/path rows; **do not** paste legacy “any complication %” from early drafts into this arm without re-querying post–**mig_252**.

Planned reporting sentence shell (to fill after query):

> Under the strict complication definition, **any complication** occurred in **[n massive (%)]** vs **[n moderate (%)]** vs **[n small (%)]** (chi-square **p = [value]**). **[Component-specific counts — chyle leak, seroma, hematoma, hypocalcemia, hypoparathyroidism, RLN injury, vocal cord paralysis — TBD.]**

---

## Manuscript footnotes (mig_266 family)

- **F2 — AJCC stage IVA/IVC collapse:** AJCC 8th-edition stage reporting on the canonical spine applies the institutional **IVA/IVC→IVB collapse** rule when promoting **`ajcc8_stage_group`** for malignant patients (**mig_263** lineage). Stage-group comparisons across weight strata must use collapsed labels consistently with **`canonical_patient_master`**.

- **F4 — LN suspicious flag rebuild:** cervical lymph-node imaging rollup feeding downstream **`n_abnormal_us_ln_on_exam`** semantics was widened and reconciled under **mig_262** (US exam master refresh). Table 1 provenance flags **post–mig_262** LN rebuild.

- **F5 — Bethesda 2 enrichment:** if secondary cytology stratification is added to this definition paper, cite **`v_fna_episode_bethesda_resolved_v1`** / **mig_264** audit conventions rather than raw **`fna_episode_master_v2.bethesda_category`** NULLs.

- **F6 — NLP phenotype coverage (smoking / family history):** PMH NLP tiers under-cover lifestyle and family-history mentions relative to chart reality (**mig_265** definitive-tier expansion narrative). Any stratification by smoking or family history requires explicit coverage caveats.

---

## Provenance

| Artifact | Role |
| --- | --- |
| `snowflake_trial/reports/m038_table1_massive_goiter.md` | Primary numeric SSOT for §3 |
| `qc_framework_v1/migrations/273_cohort_m038_view_20260502.sql` | Weight bucket DDL (**mig_273**) |
| `cursor_prompts/CURSOR_PROMPT_MIG_276_M038_MANUSCRIPT_DRAFT_20260502.md` | Composer dispatch spec (**mig_276**) |

---

## Drafting checklist (next passes)

1. Replace **§3.4** placeholder with mig_275-backed medians and missingness.
2. Replace **§3.5** placeholder with mig_252 × weight-stratum complication SQL + chi-square.
3. Add multivariable logistic model if hypothesis-testing arm is activated.
4. LG: Introduction + Discussion bridging **weight-only ≥200 g** definition vs **composite M038** manuscript (`M038_massive_goiter_DRAFT_v2_post_mig_252_253.md`).
5. Insert literature citations for **200 g** threshold choice.
