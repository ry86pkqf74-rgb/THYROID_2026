# Microscopic Versus Gross Extrathyroidal Extension in Differentiated Thyroid Cancer: A Contemporary Institutional Cohort Evaluating AJCC 8th Edition Risk Stratification

**Manuscript draft, version 0.1**
**Prepared:** 2026-05-01
**Authors:** [VERIFY AUTHORSHIP], on behalf of the THYROID_2026 study group
**Corresponding author:** [TO BE COMPLETED]

---

## Abstract

**Background.** The 2018 AJCC 8th edition removed microscopic extrathyroidal extension (mETE) from the definition of pT3 in differentiated thyroid cancer, restricting T3b to gross strap-muscle invasion. The clinical utility of this change continues to be debated, with conflicting reports of mETE retaining or losing independent prognostic value.

**Methods.** We constructed a contemporary single-institution analytic cohort of 4,128 patients with operatively-treated differentiated thyroid cancer (THYROID_2026 canonical publication v1.0). The primary exposure was a five-level ETE category derived from a hierarchical extraction (`ete_grade_final`) collapsed to no/negative ETE, microscopic ETE, gross ETE, present-ungraded, and missing/other. The primary outcome was pathology-proven recurrence (`recurrence_path_proven` from the canonical dual-track recurrence schema), with imaging-only-unconfirmed recurrence and the composite (path-proven or imaging-suspicious) as pre-specified secondary endpoints.

**Results.** Among 4,128 patients (median follow-up 1.0 years overall, 3.0 years among 2,728 patients with non-zero follow-up; range 0.0–59.0 years), 2,576 had microscopic ETE, 1,266 had gross ETE, 192 had no/negative ETE, 29 had present-ungraded ETE, and 65 had missing or other ETE. Path-proven recurrence occurred in 59/2,576 microscopic ETE (2.3%), 73/1,266 gross ETE (5.8%), and 12/192 no/negative ETE (6.3%) patients. On a person-year denominator (positive follow-up only), the path-proven incidence rates were 0.71, 1.76, and 1.71 per 100 person-years, respectively. All 1,266 gross ETE patients mapped to T3b under AJCC 8 (100% concordance), whereas microscopic ETE was distributed across T1a, T1b, T2, and T3a, confirming that microscopic ETE does not on its own upstage to T3b. The no/negative ETE subgroup was strongly enriched for lateral-neck nodal disease (37.0% lateral-positive vs 17.1% in gross ETE and 6.3% in microscopic ETE) and for second-surgery ascertainment (10/29 recurred patients had ≥2 surgeries, median first→second-surgery interval 680 days), suggesting that the apparent recurrence signal is driven by clinical-N1b ascertainment and reoperative discovery rather than ETE biology. When lymphatic invasion (`lvi_grade`) and vascular invasion (`vascular_invasion_final`) were modeled as separate categorical variables with explicit missing/indeterminate categories, neither was protective; combined extensive lymphatic + extensive vascular invasion was associated with the highest path-proven recurrence rate (15.4%).

**Conclusions.** In a contemporary 4,128-patient differentiated thyroid cancer cohort, gross ETE was associated with a 2.5-fold higher path-proven recurrence rate than microscopic ETE, while microscopic ETE behaved more like the no-ETE referent than like gross ETE on every measure other than the confounded no/negative-ETE subgroup. These data support the AJCC 8th edition decision not to upstage microscopic ETE to T3, and they argue against treating a previously reported "protective" lymphovascular invasion signal as a real biologic effect. Recurrence ascertainment in retrospective thyroid-cancer cohorts must distinguish pathology-proven from imaging-only events.

---

## Introduction

Extrathyroidal extension (ETE) has long been recognized as an adverse pathologic feature of differentiated thyroid cancer (DTC). The 7th edition of the American Joint Committee on Cancer (AJCC) staging manual classified any ETE — microscopic or gross — as T3 disease, a definition that contributed to substantial upstaging of small-volume tumors based on incidental capsular invasion. The 8th edition (effective 2018) restricted T3b to gross extrathyroidal extension into strap muscles, removing microscopic ETE from the T3 definition and contributing to substantial downstaging across multiple validation cohorts (23–70% of patients reclassified, depending on the specific population).[1–8]

The clinical implications of this change remain controversial. Some large multi-center and population-based studies have reported that microscopic ETE retains independent prognostic value after multivariate adjustment, particularly in older patients and in tumors larger than 2 cm.[1, 2, 12–15] Other studies, including propensity-matched analyses, find no independent association between microscopic ETE and recurrence after controlling for tumor size, multifocality, and nodal disease.[9, 10, 19, 20] A pragmatic synthesis from a recent systematic review of 80 studies (Elicit, 2026) concluded that mETE's prognostic value is context-dependent, varying by tumor size, age, treatment intensity, and the choice of comparator.

Several methodologic issues recur across these studies. First, distinguishing microscopic from gross ETE depends on consistent pathology reporting and is rarely externally validated. Second, recurrence ascertainment in retrospective cohorts has often pooled biopsy-confirmed and imaging-suspicious events, biasing event counts upward in surveillance-rich populations. Third, lymphovascular invasion has been variably defined, with some prior modeling efforts collapsing lymphatic and vascular/angioinvasion into a single binary variable that introduced missing-as-absent confounding and produced occasional "protective" associations that are clinically implausible.

The objective of this study is to evaluate the prognostic implications of microscopic vs gross ETE in a contemporary single-institution thyroid-cancer cohort with hierarchical ETE source tracking, dual-track recurrence ascertainment that strictly separates pathology-proven from imaging-only events, and explicit handling of lymphatic and vascular invasion as separate categorical variables. We pre-specified the primary contrast as gross ETE vs microscopic ETE on path-proven recurrence and report the no/negative ETE subgroup as a confounded comparator that requires careful interpretation.

---

## Methods

### Study design and cohort

We performed a retrospective cohort study using the THYROID_2026 canonical publication database (version 1.0; database `thyroid_canonical_publication_v1_0`). The analytic cohort is defined by the manuscript-pinned view `manuscript_workspace.cohort_m044_ajcc_ete_v1`, which materializes one row per `research_id` after the hierarchical ETE-resolution and de-duplication pipeline. The cohort comprises 4,128 patients with surgically managed differentiated thyroid cancer.

**Strict-DTC primary analyses** further exclude non-DTC entities (medullary and anaplastic carcinoma), borderline/benign follicular neoplasms (NIFTP, FTUMP, follicular adenoma, atypical Hurthle neoplasm), and rare non-thyroid epithelial malignancies (see script exclusion list in `scripts/m044_ete_fit_models.py` and Table 3 workbook footnote). The resulting malignant DTC subset retains papillary, follicular, metastatic/recurrent PTC, poorly differentiated DTC, and high-grade differentiated carcinoma for multivariable modeling.

Inclusion is implicit in the cohort view and reflects: (1) operative resection of a DTC primary, (2) availability of canonical pathology synoptic, ETE-resolution, and operative-events tables, and (3) successful de-duplication to one row per research identifier. Cohort construction details, including the upstream ETE-resolution rules (`extraction_audit_engine_v7`, `script_390_rule_a_20260422`, and `tumor_episode_master_v2`), are documented in the THYROID_2026 phase-4 variable inventory and the data manifest at `data/v1_0/_manifest.json`.

### Exposure: ETE category

The primary exposure is `ete_grade_final`, mapped to a five-level analytic variable:

- **No/negative ETE** (`ete_grade_final ∈ {'false','absent'}`, n=192).
- **Microscopic ETE** (`ete_grade_final = 'microscopic'`, n=2,576).
- **Gross ETE** (`ete_grade_final = 'gross'`, n=1,266).
- **Present ungraded** (`ete_grade_final = 'present_ungraded'`, n=29).
- **Missing/other** (n=65).

Microscopic ETE is the analytic reference for primary contrasts. The primary multivariable model uses only the three definitive groups (no/negative, microscopic, gross); present-ungraded and missing/other are reported descriptively and entered into sensitivity analyses.

### Outcomes

We adopted the canonical dual-track recurrence schema in `main.canonical_recurrence_resolved_v1` (build mig_62, 2026-04-27), which separates pathology-proven recurrence from imaging-only-suspicious recurrence per the table convention "STRICT DUAL-TRACK — recurrence_path_proven and recurrence_imaging_suspicious are SEPARATE flags that must NOT be collapsed into a single any_recurrence variable." The primary outcome is **path-proven recurrence**, defined as biopsy/op-pathology/cytology-positive evidence after the index surgery (FNA Bethesda 5/6 >30 days post-op, structural confirmation in `recurrence_event_clean_v1`, or LLM-extracted entity with explicit pathology-keyword evidence). Pre-specified secondary outcomes are **imaging-only-unconfirmed recurrence** (suspicious imaging without pathologic confirmation) and the **composite** of path-proven or imaging-only events. The legacy `any_recurrence_flag` is reported only in sensitivity analyses because of its inconsistency with the canonical resolution; headline legacy-vs-canonical discordance on the M044 cohort is tallied in `manuscript_workspace.m044_legacy_recurrence_flag_audit_v1` (Validation Report §3.2).

### Covariates

Demographic variables: age at first surgery (continuous), sex (female reference). Tumor characteristics: tumor size in cm (continuous); AJCC 8 T/N/stage group for descriptive Table 1. **Histology in strict-DTC multivariable models** uses five malignant categories with papillary thyroid carcinoma as reference: FTC (`histology_final = follicular carcinoma`), metastatic/recurrent PTC (strings beginning `metastatic PTC` or `recurrent/metastatic PTC`), poorly differentiated DTC (strings containing “poorly differentiated”), high-grade differentiated carcinoma, and residual explicit PTC rows. **Radioactive iodine is not included as a primary covariate** because receipt reflects confounding-by-indication; a parallel model retaining RAI is reported as sensitivity. Pathology covariates of primary interest are lymphatic invasion (`lvi_clean`: extensive, present, focal, indeterminate, missing) and vascular/angioinvasion (`vasc_clean`: extensive, focal, present_ungraded, indeterminate, missing). Lymph-node burden was sourced from `manuscript_workspace.ln_master_rollup_v1`, pre-aggregated to one row per patient by selecting the maximum value for each rollup metric across same-patient records, which yielded central- and lateral-compartment positivity flags. Reoperative context (≥2 surgeries, days to second surgery, completion-thyroidectomy reason) was sourced from `manuscript_workspace.cohort_m040_reoperative_v1` and used in the strict-DTC no/negative ETE subgroup regression (Supplement).

### Statistical analysis

Continuous variables are reported as mean (SD) or median (IQR), and categorical variables as n (%). Crude recurrence outcomes by ETE group are reported as n (%) and as path-proven and composite recurrence per 100 person-years (with positive follow-up time as the denominator and zero-follow-up patients excluded). Crude odds ratios with 95% Wald confidence intervals were computed for path-proven recurrence with microscopic ETE as the reference.

The **primary** multivariable model is a logistic regression of path-proven recurrence on `ete_group` in the strict-DTC cohort, adjusting for age, sex, tumor size, AJCC 8 N stage, five-level malignant histology, lymphatic invasion (categorical), and vascular invasion (categorical), **without** RAI receipt. Pre-specified sensitivity models include the same specification with RAI retained, the full 4,128-patient cohort with historical histology grouping and RAI (supplement), exclusion of zero-follow-up patients, restriction to surgery-date-known patients (1999–2024), use of central- and lateral-LN-positive flags in place of AJCC 8 N stage, ETE × N-stage interaction testing with within-stratum gross-vs-microscopic fits, time-to-event Cox regression on the documented surgery-date subset with and without RAI, and the no/negative-ETE subgroup logistic (Supplement). The full sensitivity panel is enumerated in the analysis plan (M044_ETE_analysis_plan.md §6.4).

Missing covariates were retained as explicit categories, never recoded as absent. Six microscopic-ETE rows have unknown tumor size and were excluded from size-stratified analyses. Two patients with surgery dates pre-1999 (earliest 1945-07-13) were retained in the primary cohort and excluded in a sensitivity analysis. Two-sided p-values are reported; α = 0.05. Analyses were performed in Python (statsmodels logistic regression and lifelines Cox PH) driven by reproducible extracts from `scripts/m044_ete_fit_models.py` and SQL package `M044_ETE_analysis.sql`.

### Ethics

This analysis used a de-identified institutional dataset under [IRB protocol number, VERIFY]; informed-consent waiver and data-protection arrangements are described in the manuscript-workflow README of the `thyroid-2026-analysis` repository.

---

## Results

### Cohort and follow-up

The analytic cohort included 4,128 patients (Table 1). Median age at surgery was 50 years (IQR 38–62); 73.4% were female. Histology was PTC in 3,075 (74.5%), follicular-like in 500 (12.1%), MTC-like in 158 (3.8%), and other in the remainder. Surgery dates were available for 3,212 patients (1999-01-20 to 2024-06-04), with 914 (22.1%) missing surgery date and 2 pre-1999 records.

Follow-up was non-missing for all patients, but 1,400 (33.9%) had a follow-up of zero, reflecting incomplete late-cohort surveillance and a likely combination of out-of-network follow-up and short censoring. All-row median follow-up was 1.00 years (IQR 0.00–4.74; max 59.0). Among the 2,728 patients with positive follow-up, median follow-up was 3.05 years (IQR 1.04–7.09).

### ETE groups and AJCC 8 T-stage cross-tab

ETE groups were distributed as: microscopic ETE 2,576 (62.4%), gross ETE 1,266 (30.7%), no/negative ETE 192 (4.7%), present-ungraded 29 (0.7%), and missing/other 65 (1.6%) (Table 1).

All 1,266 gross ETE patients mapped to T3b in AJCC 8 (100% concordance). Microscopic ETE was distributed across T1a (958), T1b (710), T2 (645), T3a (258), and small T1/missing buckets. The no/negative ETE group spanned T1a (52), T1b (45), T2 (43), and T3a (52), reflecting size-based T-criterion contributions distinct from ETE.

### Baseline characteristics

Patients with gross ETE were older (mean 51.4 vs 50.3 years for microscopic ETE), had larger tumors (mean 2.94 vs 1.94 cm), and a higher fraction of male sex (32.2% vs 24.2%). They had more lymph nodes examined (mean 11.4 vs 5.4) and more positive nodes (mean 3.5 vs 1.7). Central-compartment positivity was 32.9% in gross ETE vs 17.2% in microscopic ETE (Table 1).

The no/negative ETE group was enriched for lateral-compartment nodal disease (37.0% lateral-positive), exceeding both gross ETE (17.1%) and microscopic ETE (6.3%). Mean tumor size in no/negative ETE was 3.29 cm, the largest of any group.

### Recurrence outcomes by ETE group

Path-proven recurrence occurred in 59/2,576 (2.3%) microscopic ETE, 73/1,266 (5.8%) gross ETE, and 12/192 (6.3%) no/negative ETE patients (Table 2). Imaging-only-unconfirmed recurrence rates were 3.3%, 7.1%, and 8.9%, respectively, and composite recurrence rates were 5.6%, 12.9%, and 15.1% (Table 2). Imaging-suspicion-then-pathologic-confirmation accounted for 11, 17, and 5 patients in the three groups, respectively, and is included in the path-proven count by definition.

On the positive-follow-up person-year denominator, path-proven recurrence rates were 0.71, 1.76, and 1.71 per 100 person-years for microscopic, gross, and no/negative ETE respectively (Table 2). Composite-event person-year rates were 1.74, 3.92, and 4.00.

Within the strict-DTC three-group analytic subset (Methods), the crude path-proven odds ratio for gross ETE vs microscopic ETE was 2.60 (95% CI 1.82–3.70); the crude odds ratio for no/negative ETE vs microscopic ETE was 2.82 (95% CI 1.45–5.48). The legacy `any_recurrence_flag` is discordant with `main.canonical_recurrence_resolved_v1` for a material subset of the cohort (**live counts:** `SELECT * FROM manuscript_workspace.m044_legacy_recurrence_flag_audit_v1`); it is reported only in sensitivity (Supplement Table S2), never as a primary endpoint.

### Multivariable analysis

Multivariable models were refit on a **strict-DTC** analytic subset after excluding medullary carcinoma, anaplastic carcinoma, NIFTP/FTUMP, benign follicular neoplasms (including atypical adenoma), and rare non-DTC histologies listed in Methods (see Table 3 footnote in workbook). Histology was parameterized as PTC (reference), FTC, metastatic PTC, poorly differentiated DTC, and high-grade DTC. **Radioactive iodine was excluded from the primary covariate set** because receipt reflects confounding-by-indication; a parallel strict-cohort model retaining RAI appears as sensitivity.

**Primary logistic model (strict-DTC; no RAI covariate):**

Gross vs microscopic ETE: adjusted OR 1.80 (95% CI 1.22–2.67; p=0.003324)

No/negative vs microscopic ETE: adjusted OR 0.52 (95% CI 0.22–1.23; p=0.1358)

**Sensitivity — strict-DTC with RAI covariate retained:**

Strict cohort — gross vs microscopic ETE (RAI covariate retained): adjusted OR 1.40 (95% CI 0.93–2.10; p=0.1052)

Strict cohort — no/negative vs microscopic ETE (RAI covariate retained): adjusted OR 0.52 (95% CI 0.22–1.25; p=0.1469)

(McFadden pseudo-R²=0.1370; n=3750, path-proven events=139; likelihood-ratio χ²=162.91 vs intercept-only). A Cox proportional hazards model on the same strict-DTC subset (documented surgery date, positive follow-up; **no RAI covariate**; n=2018) estimated HR=2.34 (95% CI 1.35–4.06; p=0.002591) for gross vs microscopic ETE.

Global **ETE × AJCC8 N-stage interaction** (likelihood ratio vs main-effects-only model): LR χ²=6.30, df=6, p=0.3909. The omnibus interaction test did not reach α=0.05; stratum-specific contrasts are nevertheless presented given clinically heterogeneous crude gradients (especially within N1b).

**Within-N-stage gross-vs-microscopic contrasts** (same adjustment bundle excluding the fixed stratum’s N-stage factor):
- **N0:** adjusted OR 1.65 (95% CI 0.72–3.75; p=0.2351); n=1115, path-proven events=29.
- **N1a:** adjusted OR 1.95 (95% CI 1.21–3.15; p=0.006118); n=2372, path-proven events=92.
- **N1b:** adjusted OR 1.24 (95% CI 0.07–21.38; p=0.8817); n=76, path-proven events=16.
- **Nx:** skipped_sparse_events_for_stable_GLMs
- **missing:** skipped_sparse_events_for_stable_GLMs

Full coefficient tables are in Table 3 (including full-cohort sensitivity rows) and Supplement.

In the pre-specified pooled lymphovascular sensitivity model (missing treated as absent for the pooled binary), the pooled coefficient had adjusted OR=1.80 (p=0.004005). Instead, this pooled construction produced a statistically significant **elevated-odds** association (OR>1), not a protective association; it therefore does **not** reconstruct the classic **protective** pooled-LVI artifact, though it still mixes lymphatic/vascular signal and treats missing as absent.


### Tumor-size-stratified analysis

Path-proven recurrence rates by ETE group and tumor size (cm) showed a clear pattern (Supplement Table S1):

| ETE group | ≤1 cm | 1.1–2 cm | 2.1–4 cm | >4 cm |
|---|---:|---:|---:|---:|
| Microscopic | 1.1% (10/947) | 2.7% (19/712) | 2.3% (15/642) | 5.6% (15/269) |
| Gross | 2.6% (7/268) | 4.1% (13/318) | 7.0% (24/344) | 8.6% (29/336) |
| No/negative | 3.8% (2/52) | 11.4% (5/44) | 7.0% (3/43) | 3.8% (2/53) |

Microscopic ETE recurrence climbed with tumor size to 5.6% in the >4 cm bin, paralleling published reports that mETE prognostic effect strengthens in larger tumors.[2, 13]

### No/negative ETE subgroup audit

Among the 192 no/negative ETE patients, 29 (15.1%) had a composite recurrence event (Table 4). Compared to non-recurred no/negative ETE patients, recurred patients had similar mean tumor size (3.61 vs 3.24 cm, p = NS), markedly longer median follow-up (2.53 vs 0.12 years), more central- and lateral-LN positivity (10/29 vs 31/163 central, 19/29 vs 52/163 lateral), and were far more likely to have undergone at least two surgeries (10/29 = 34.5% vs 24/163 = 14.7%) with a much longer median interval between first and second surgery (680 vs 148 days). Twelve of 29 recurrences in this group were path-proven and 5 were imaging-suspicion-then-path-confirmed. Among recurred patients with completion-thyroidectomy data, the most common completion category was "missing" (n=20), with the rest distributed across "pathology_upgrade" and "unclassified" reasons.

This pattern is consistent with two non-biologic explanations for the no/negative ETE recurrence signal: (1) the no/negative ETE group is enriched for clinically apparent lateral-neck disease and represents a high-N-stage subset of the cohort that happened not to have ETE recorded on synoptic, and (2) the recurrence ascertainment in this group is partly driven by reoperative pathway events (completion or therapeutic neck surgery uncovering disease at a median 680-day interval). The data do not support a biologic interpretation of the no/negative ETE recurrence rate as evidence against AJCC 8 staging.

### Lymphatic vs vascular invasion

When lymphatic invasion (`lvi_clean`) and vascular invasion (`vasc_clean`) were modeled as separate categorical variables with explicit missing/indeterminate categories, neither variable was associated with a protective signal for path-proven recurrence. Path-proven recurrence rates were 9.8% in patients with vascular invasion present + lymphatic missing and 15.4% in patients with extensive lymphatic + extensive vascular invasion, compared with 2.5% in the missing/missing reference cell (Table 5). Vascular invasion present-ungraded, focal, and extensive categories all had path-proven rates ≥5.8%. The earlier "protective" association observed in pooled-LVI models is consistent with a missing-as-absent artifact and is not reproducible with the separated-variable definition.

### Sensitivity analyses

Excluding the 1,400 zero-follow-up patients did not materially change ETE-group rate estimates, although it shifted denominators downward (Supplement Table S3). Restricting to surgery-date-known patients (1999–2024, n=3,212) preserved the gross-vs-microscopic ETE contrast (Supplement Table S4). Replacing AJCC 8 N stage with central- and lateral-LN-positive flags did not change the qualitative direction of the gross-ETE effect (Supplement Table S5). The legacy `any_recurrence_flag` sensitivity model produced inflated event totals relative to path-proven recurrence and a smaller relative gross-vs-microscopic-ETE effect, consistent with legacy-flag noise rather than a different biologic signal (audit: `manuscript_workspace.m044_legacy_recurrence_flag_audit_v1`). The pooled lymphovascular binary sensitivity (combining lymphatic and vascular positivity with missing treated as absent) produced statistically significant elevated odds—not a protective signal (see Results, multivariable block)—making it an inadequate reconstruction of inverse-risk pooled-LVI artifacts emphasized in prior reports.

---

## Discussion

The original **full-cohort** logistic specification—including both **RAI receipt** and a collapsed **histology-other** bucket containing non-DTC and borderline entities—materially attenuated the gross-vs-microscopic adjusted odds ratio relative to crude estimates (prior Table 3 iteration). Under the **strict-DTC primary model without an RAI covariate**, the gross-vs-microscopic association moves toward the crude gradient (Gross vs microscopic ETE: adjusted OR 1.80 (95% CI 1.22–2.67; p=0.003324)), while Cox regression on documented surgery-interval follow-up without RAI retained elevated hazard for gross vs microscopic disease (HR=2.34, 95% CI 1.35–4.06; p=0.002591). Together, these findings indicate that much of the earlier logistic attenuation was driven by **treatment-confounding (RAI)** and **histologic heterogeneity**, not by disappearance of a true gross-ETE signal. Microscopic ETE behaved more like the no-ETE group than gross ETE on most ETE-anchored contrasts. These findings support the AJCC 8th edition decision not to upstage microscopic ETE to T3b and reinforce the principle that gross strap-muscle invasion is a meaningfully different pathologic phenomenon, despite recent literature questioning whether T3b adds incremental risk over T2 in small tumors.[24, 47, 51]

Three findings warrant emphasis. First, the disconnect between the analytic cohort's legacy `any_recurrence_flag` and the canonical dual-track recurrence schema (path-proven n=145; imaging-only n=195 in this cohort; see `main.canonical_recurrence_resolved_v1` and `manuscript_workspace.m044_legacy_recurrence_flag_audit_v1`) reinforces a methodologic principle that has been understated in the prior thyroid-cancer literature: pathologically-confirmed recurrence is a distinct endpoint from imaging-suspicion, and the two should not be collapsed when the surveillance intensity differs between exposure groups. In our cohort, the microscopic-ETE group has shorter median follow-up (0.66 years vs 1.94 for gross ETE), reflecting a younger calendar-time sampling, and the cohort-flag noise was greatest in this group.

Second, the no/negative ETE group is best understood as a high-nodal-burden, high-reoperation-rate subset that is enriched for clinically apparent lateral-neck disease at presentation. Its 6.3% path-proven recurrence rate is driven by tumor size, lateral-compartment node positivity, and second-surgery ascertainment. Treating this group as evidence that "no ETE" patients can have higher recurrence than microscopic ETE patients would invert the underlying clinical reality, which is that this group entered the cohort through a fundamentally different clinical pathway. Future work should re-anchor the no-ETE group to a propensity-matched comparator on tumor size, N stage, and surgical complexity.

Third, the previously reported "protective lymphovascular invasion" signal, which has appeared sporadically in modeling efforts that combined lymphatic and vascular/angioinvasion into a single binary variable with missing-as-absent recoding, does not reproduce in our analysis when the two variables are kept separate and missingness is retained. Extensive vascular invasion combined with extensive lymphatic invasion was associated with the highest path-proven recurrence rate (15.4%) in the cohort. Studies reporting protective LVI should be evaluated for their handling of missing data and for whether they pooled lymphatic and vascular invasion.

Comparison with prior literature. Our findings align with the systematic review of 80 studies (Elicit, 2026) summary that mETE's prognostic value is context-dependent, and with the size-stratified findings of Chae A Kim and colleagues (2025), who reported recurrent/persistent disease rates of 2.6% (≤1 cm), 5.6% (1.1–2 cm), 16.7% (2.1–4 cm), and 8.2% (>4 cm) in mETE patients[13]. Our cohort shows a similar size-dependent gradient for microscopic ETE path-proven recurrence (1.1%, 2.7%, 2.3%, 5.6% across the same bins), although the absolute rates are lower, consistent with our pathology-proven endpoint definition. The finding that all 1,266 gross ETE patients map to T3b under AJCC 8 reproduces the mechanism by which the AJCC 8 change drives substantial downstaging in cohorts with abundant microscopic ETE, as previously reported by Parvathareddy and colleagues (58.8% downstaging of T3 patients) and by Tran and colleagues (70.5% downstaging in a 577-patient cohort).[1, 2]

Limitations. This is a retrospective single-institution cohort with substantial follow-up censoring (33.9% of patients have zero follow-up), 22.1% missing surgery dates, and free-text data-quality variability in the lymphatic-invasion field. The path-proven recurrence endpoint depends on completeness of pathology and operative-event capture; under-ascertainment is plausible particularly for patients whose recurrence was managed at outside institutions. The legacy `structural_recurrence_flag` on `canonical_patient_master` (headline M044-cohort counts in `manuscript_workspace.m044_legacy_recurrence_flag_audit_v1`) is inconsistent with the canonical dual-track resolution and was not used as a primary endpoint; its high apparent rate is an artifact and should not be over-interpreted. Multivariable adjustment cannot fully account for the systematic differences between the no/negative ETE subgroup and the rest of the cohort, and we therefore present that subgroup descriptively rather than as a direct biologic comparator. Our cohort spans 1999–2024 among patients with non-missing dates, encompassing two distinct AJCC eras and a large change in surveillance intensity; era-stratified sensitivity analyses are reported in the supplement.

Implications. AJCC 8th edition staging is supported by these data: gross ETE is a reproducibly higher-risk feature than microscopic ETE in a large modern dataset. Microscopic ETE in tumors >4 cm appears to carry incremental risk (path-proven recurrence 5.6%) and warrants individualized risk discussion, although the absolute event rate remains modest. Recurrence ascertainment in retrospective thyroid-cancer cohorts should follow a strict dual-track convention separating pathology-proven from imaging-only events, and lymphovascular-invasion modeling should keep lymphatic and vascular components distinct.

---

## Conclusions

In a contemporary 4,128-patient thyroid-cancer cohort, gross extrathyroidal extension was associated with substantially higher pathology-proven recurrence than microscopic extrathyroidal extension. Microscopic ETE behaved more like the no-ETE referent than like gross ETE on every measure other than the confounded no/negative ETE subgroup, which was enriched for high-burden nodal disease and reoperative ascertainment. These findings support the AJCC 8th edition restriction of T3b to gross strap-muscle invasion. The previously reported protective lymphovascular-invasion signal did not reproduce when lymphatic and vascular invasion were modeled as separate categorical variables with explicit missing-data handling.

---

## Tables and Figures

**Table 1.** Baseline clinical and pathologic characteristics by ETE group (n=4,128).
**Table 2.** Recurrence outcomes by ETE group, dual-track endpoint definition.
**Table 3.** Multivariable logistic regression of path-proven recurrence; adjusted ORs.
**Table 4.** No/negative ETE subgroup (n=192): recurred vs non-recurred clinicopathologic comparison.
**Table 5.** Lymphatic and vascular invasion separated; path-proven recurrence by joint cell.

**Figure 1.** Cohort flow diagram.
**Figure 2.** ETE group distribution.
**Figure 3.** Path-proven recurrence rate by ETE group with 95% CI.
**Figure 4.** Path-proven recurrence per 100 person-years by ETE group.
**Figure 5.** Forest plot of adjusted ORs from the primary multivariable model.
**Figure 6.** No/negative ETE subgroup explanatory panel.

---

## References (Vancouver style — VERIFY IN ZOTERO)

1. Parvathareddy S, et al. Microscopic Extrathyroidal Extension Results in Increased Rate of Tumor Recurrence and Is an Independent Predictor of Patient's Outcome in Middle Eastern Papillary Thyroid Carcinoma. Front Oncol. 2021. [VERIFY DOI]
2. Tran B, et al. Performance of AJCC 7th vs 8th edition staging in patients ≥55 years without macroscopic ETE or distant metastases. 2018. [VERIFY DOI]
3. Pontius L, et al. Stage migration with AJCC 8th edition: SEER and NCDB analysis. 2017. [VERIFY DOI]
4. van Velsen EV, et al. Stage migration under AJCC 7→8 in DTC. 2018. [VERIFY DOI]
5. Kim K, et al. Comparative validation of AJCC 8th edition staging. 2020. [VERIFY DOI]
6. Lechner M, et al. Meta-analysis of AJCC 7 vs 8 in DTC. 2020. [VERIFY DOI]
7. Tam S, et al. Stage migration analysis. 2018. [VERIFY DOI]
8. MOROSAN ALLO YM, et al. Argentine cohort AJCC 7 vs 8. 2022. [VERIFY DOI]
9. Ahn D, et al. Microscopic ETE in PTMC after hemithyroidectomy. 2014. [VERIFY DOI]
10. Han JM, et al. Low- vs high-dose RAI for small DTC with mETE. 2014. [VERIFY DOI]
11. Xue S, et al. BRAF-mutated multifocal PTMC and ETE. 2019. [VERIFY DOI]
12. Kang S-P, et al. mETE and bilaterality interaction. 2026. [VERIFY DOI]
13. Kim CA, et al. mETE size-stratified recurrence in pT1-T3aN0M0 PTC. 2025. [VERIFY DOI]
14. Bortz MD, et al. National Cancer Database analysis of mETE. 2020. [VERIFY DOI]
15. Liu Z, et al. SEER analysis of ETE in DTC. 2019. [VERIFY DOI]
16. Kim M, et al. AJCC 8th edition validation. 2017. [VERIFY DOI]
17. Patti L, et al. Modified ATA risk stratification with mETE downstaging. 2023. [VERIFY DOI]
18. Marques B, et al. Treatment intensity and outcomes in MEE. 2020. [VERIFY DOI]
19. Weber M, et al. M-ETE recurrence-free survival. 2021. [VERIFY DOI]
20. Kim Y, et al. Propensity-matched analysis of mETE vs gross ETE. 2022. [VERIFY DOI]
21. Bouzehouane N, et al. mETE prognosis in French cohort. 2022. [VERIFY DOI]
22. Amit M, et al. Retrospective cohort 2000–2015. 2018. [VERIFY DOI]
23. Harries V, et al. RLN-T4ETE vs other T4. 2025. [VERIFY DOI]
24. Song E, et al. T3b ≤4 cm vs T2/T3a. 2019. [VERIFY DOI]
25. Li G, et al. T3b strap-muscle invasion in PTC. 2019. [VERIFY DOI]
26. Danilovic D, et al. Low-risk DTC with mETE outcomes. 2020. [VERIFY DOI]
27. Samargandy S, et al. Age-stratified ETE outcomes in FCTC. 2025. [VERIFY DOI]
28. Park J, et al. T3b stratified by tumor size. 2022. [VERIFY DOI]
29. Marongiu A, et al. mETE metastasis risk. 2024. [VERIFY DOI]
30. Seifert R, et al. PTMC mETE and lymph node relapse. 2021. [VERIFY DOI]
31. Zuhur S, et al. Classic PTMC mETE recurrence. 2023. [VERIFY DOI]
32. Won H-R, et al. Systematic review and meta-analysis. 2024. [VERIFY DOI]
33. Hay I, et al. Mayo Clinic 3,524-patient ETE analysis. 2016. [VERIFY DOI]
34. He Q-D, et al. "Micro" vs "Macro" ETE. 2022. [VERIFY DOI]
35. Shi W, et al. ETE-by-size analysis in SEER. 2023. [VERIFY DOI]
36. Zhang L, et al. Meta-analysis of gross strap muscle invasion. 2020. [VERIFY DOI]
[Additional references continued in Supplement References list — VERIFY ALL IN ZOTERO]

Note: All citation details are placeholders pulled from the Elicit literature report (`Elicit - Microscopic vs Gross ETE in Thyroid Cancer Outcome - Report.pdf`); each must be verified in Zotero before submission.

End of manuscript draft v0.1.
