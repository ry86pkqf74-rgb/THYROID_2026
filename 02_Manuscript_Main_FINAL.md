# Diagnostic Performance of ACR TI-RADS in a 25-Year Operative Thyroid Cohort: Patient-Level Analysis with Nodule-Level Sister Validation

## Abstract

**Background.** ACR TI-RADS guides fine-needle aspiration (FNA) decisions for thyroid nodules. Operative-cohort validations routinely report risk of malignancy (ROM) above ACR-published bands, an observation usually attributed to surgical selection bias. We tested whether part of this elevation reflects multinodular attribution error — crediting a patient's cancer to the highest-TR nodule even when the malignant focus was elsewhere.

**Methods.** We studied 3,375 thyroidectomy patients from a 1994–2025 single-institution surgical warehouse. The primary analysis used the maximum re-scored ACR 2017 TI-RADS category per patient as the predictor and any WHO 2022 pathology-proven thyroid malignancy as the reference. Diagnostic performance was evaluated at TR≥TR3, TR≥TR4, and TR≥TR5 with Wilson 95% confidence intervals (CIs). A pre-specified nodule-level sister analysis included 3,687 strict ACR feature-complete nodules from 1,668 patients.

**Results.** Of 3,375 patients, 1,479 (43.8%) were malignant. Patient-level discrimination was modest (AUC 0.648, 95% CI 0.630–0.667). TR≥TR4 was Youden-optimal (J=0.271; sensitivity 71.3%, specificity 55.9%). Patient-level ROM was 28.2%, 32.1%, 27.6%, 47.4%, and 58.7% for TR1–TR5; only TR5 fell within ACR-expected bands. Nodule-level analysis restored within-band calibration for TR4 (18.7%, 95% CI 16.3–21.5) and TR5 (26.1%, 95% CI 23.7–28.6); nodule AUC was 0.640. Patient-versus-nodule inflation was +28.6 percentage points (pp) at TR4 and +32.6 pp at TR5.

**Conclusions.** In operative cohorts, patient-level TI-RADS ROM is inflated by multinodular attribution. Future validations should report per-nodule ROM.

**Keywords:** ACR TI-RADS; thyroid nodule; risk of malignancy; operative cohort; multinodular attribution; selection bias; ultrasound; diagnostic performance; Bethesda.

---

## Introduction

Thyroid nodules are common, but only a minority are malignant. Ultrasound risk stratification therefore drives the central clinical decisions: which nodules warrant FNA, which can be observed, and which require surgery or surveillance [1,2]. The American College of Radiology Thyroid Imaging Reporting and Data System (ACR TI-RADS), introduced in 2017, scores five ultrasound feature groups — composition, echogenicity, shape, margin, and echogenic foci — to assign a category from TR1 to TR5 with size-based FNA thresholds [3,4]. ACR-expected ROM bands are TR1 <2%, TR2 <2%, TR3 <5%, TR4 5–20%, and TR5 >20% [3]. The 2015 American Thyroid Association guidelines and the Bethesda System for Reporting Thyroid Cytopathology remain complementary frameworks for management [1,5].

Surgical validation studies routinely report category-specific ROMs above the ACR-expected bands, particularly at lower and intermediate categories [6–13]. This is unsurprising: a thyroidectomy cohort is not a screening cohort. Patients reach surgery via cytology, symptoms, growth, compressive findings, clinician concern, or patient preference. A project-specific evidence review of adult surgical-cohort ACR TI-RADS studies found overall malignancy prevalence ranging from 12.2% to 66.1%, with most reports exceeding screening expectations; only a minority explicitly addressed surgical selection bias [14].

Selection bias is not the only explanation. Another is **multinodular attribution error**: when a patient with several nodules is collapsed to the highest TI-RADS category seen on any preoperative ultrasound, any subsequent malignancy is credited to that maximum category — even when the malignant focus was not the highest-TR nodule. This convention inflates patient-level ROM.

The distinction is methodological but clinically important. The unit of analysis (the "grain") can be the patient or the individual nodule. ACR TI-RADS was designed to guide nodule-level decisions, so calibration should ideally be tested at the nodule grain. Patient-level analyses are useful for surgical risk summaries but do not directly answer the clinical question: *what is the malignancy risk of this nodule?*

Longitudinal thyroid research also faces a second challenge: classification systems evolve. ACR TI-RADS was published in 2017, while this cohort spans 1994–2025. A project-specific evidence review of thyroid classification-system transitions identified retrospective re-classification, parallel application of multiple systems, and pre/post era comparisons as standard strategies [15]. Scoring-based systems such as ACR TI-RADS may also offer better reproducibility than pattern-based systems when applied retrospectively across long time periods [16].

We therefore evaluated ACR TI-RADS diagnostic performance in a 25-year single-institution thyroidectomy cohort. The primary patient-level analysis used the maximum re-scored TI-RADS category per patient. A pre-specified nodule-level sister analysis tested whether per-nodule ROM recovered ACR-expected calibration and quantified the share of patient-level inflation attributable to multinodular attribution.

---

## Methods

### Study design and cohort

We analyzed the institutional canonical thyroid surgical warehouse (`thyroid_canonical_publication_v1_0`, release tag `pub_v1_1`, built 2026-05-05), comprising 10,871 unique research IDs from thyroid surgical care between 1994 and 2025. Patients were eligible for the primary analysis if they had at least one preoperative ultrasound with a documented nodule and definitive operative pathology. The final patient-level analytic cohort included 3,375 patients.

The all-nodule analytic spine included 37,438 ultrasound nodule observations from 6,523 distinct patients. The strict nodule-level sister cohort required complete ACR feature scoring and linkage eligibility, yielding 3,687 strict ACR-eligible nodules from 1,668 patients.

### ACR 2017 TI-RADS re-scoring

Because the cohort spans years before and after publication of ACR TI-RADS, we did not rely on historical report-assigned TI-RADS labels. Each nodule was re-scored under the ACR 2017 system using uniform rules across the full cohort era. Nodule-level ultrasound descriptions were parsed into the five ACR feature groups (composition, echogenicity, shape, margin, echogenic foci); the ACR 2017 point algorithm was then applied programmatically (TR1 = 0, TR2 = 2, TR3 = 3, TR4 = 4–6, TR5 ≥7 points) [3]; the strict analytic subset required complete five-feature scoring.

This approach assigns historical reports a modern ACR TI-RADS score under a single uniformly-applied lexicon — a retrospective re-classification strategy consistent with longitudinal best practice. Of the 3,687 strict-eligible nodules, 3,660 (99.3%) derived their five-feature scores from the structured `imaging_nodule_master_v1` source; 27 (0.7%) used LLM-augmented feature extraction. Reporter-assigned TR labels, when present, were retained as audit fields only and did not define the analytic predictor.

### Patient-level predictor and reference standard

The primary predictor was each patient's maximum re-scored ACR TI-RADS category across all preoperative ultrasound examinations. For example, a patient with documented TR2, TR3, and TR5 nodules was assigned TR5. This convention mirrors common operative-cohort practice and reflects how high-suspicion nodules drive clinical concern.

The reference standard was any pathology-proven thyroid malignancy on the operative specimen, classified per WHO 2022 [17]. Noninvasive follicular thyroid neoplasm with papillary-like nuclear features (NIFTP) was treated as non-malignant.

### Nodule-level sister analysis

The pre-specified sister analysis used the per-nodule analytic spine (`cohort_m025_nodule_level_v1`). The predictor was the nodule's re-scored ACR 2017 category. The strict nodule subset required complete five-feature scoring, known laterality, no size-outlier quarantine, and no unresolved multi-nodule attribution flag. The reference standard was a same-side pathology-proven malignant tumor with surgery date within 365 days after the index ultrasound. This analysis was designed to address the calibration question most relevant to clinical use: whether the ROM of individual nodules falls within ACR-expected bands.

### Time-window sensitivity for nodule-pathology matching

The primary nodule-level pathology linkage used a same-side match within 365 days. This is clinically pragmatic but allows two forms of temporal mismatch — interval growth and multifocal disease ascertainment. To test robustness, we pre-specified tighter ultrasound-to-surgery windows of 180, 90, and 30 days.

### FNA and Bethesda linkage

Patient-level FNA results were attached through `canonical_fna_events_v1`. Nodule-level Bethesda 2023 results were bridged through a reconstructed nodule-FNA linkage table using research ID, normalized laterality, and ultrasound-to-FNA timing within 30 days. When multiple candidate links existed, the best link used the smallest absolute day gap and then the highest linkage score. Among 3,375 patients, 2,380 (70.5%) had a Bethesda result; among 3,687 strict-eligible nodules, 495 (13.4%) had a bridged Bethesda value. The nodule-level Bethesda table therefore has a large "Missing" row, reflecting an FNA-linkage carry-forward limitation rather than absence of cytologic evaluation.

### ACR FNA-eligibility audit

We retrospectively applied ACR 2017 size-based FNA-eligibility rules at the patient grain. This is a rules-based audit: it identifies cases that would or would not have met ACR FNA thresholds based on TR-size combination, and should not be interpreted as proof that every flagged case represented an avoidable real-world biopsy.

### Statistical analysis

Continuous variables are reported as mean (SD) or median; categorical variables as count and percentage. Category-specific ROM was calculated with Wilson 95% CIs [18]. Diagnostic performance was evaluated at three pre-specified thresholds (TR≥TR3, TR≥TR4, TR≥TR5), reporting sensitivity, specificity, positive predictive value (PPV), and negative predictive value (NPV). The Youden index (sensitivity + specificity − 1) identified the optimal threshold [19]. Discrimination was summarized by AUC using the closed-form rank Mann–Whitney equivalent [20]. Patient-versus-nodule ROM inflation was reported in percentage points. Analyses were performed in DuckDB SQL on the MotherDuck publication database. De-identified summary tables and reproduction code are provided as supplementary material.

---

## Results

### Cohort assembly and baseline characteristics

Of 10,871 unique research IDs in the institutional thyroid surgical warehouse, 3,375 patients met criteria for the primary analysis (Figure 1). The cohort comprised 2,691 women (79.7%) and 684 men (20.3%). Race distribution was 1,535 Black or African American (45.5%), 1,382 White (40.9%), 204 Asian (6.0%), 165 Unknown or Not Reported (4.9%), 66 Other (2.0%), 11 Native Hawaiian or Other Pacific Islander (0.3%), 10 American Indian or Alaska Native (0.3%), and 2 with missing race. Median age at surgery was 54 years; median surgery year was 2021 (range 1994–2025).

Overall, 1,479 patients (43.8%) had pathology-proven thyroid malignancy. Distribution by maximum re-scored TI-RADS category was TR1 in 340 (10.07%), TR2 in 299 (8.86%), TR3 in 845 (25.04%), TR4 in 492 (14.58%), and TR5 in 1,399 (41.45%). Bethesda cytology was available in 2,380 patients (70.5%); a malignant or borderline histology category was available in 1,538 (45.6%).

*[Insert Table 1 here]*
*[Insert Figure 1 here]*

### Patient-level diagnostic performance

Patient-level discrimination was modest, with AUC 0.648 (95% CI 0.630–0.667).

At TR≥TR3, sensitivity was high but specificity was low: sensitivity 87.0% (95% CI 85.2–88.6), specificity 23.6% (21.7–25.5), PPV 47.0%, NPV 70.0%.

At TR≥TR4 — the Youden-optimal threshold (J=0.271) — sensitivity was 71.3% (68.9–73.5), specificity 55.9% (53.6–58.1), PPV 55.7%, NPV 71.4%.

At TR≥TR5, sensitivity decreased and specificity increased: sensitivity 55.5% (53.0–58.0), specificity 69.5% (67.4–71.6), PPV 58.7%, NPV 66.7%.

These results reflect the expected clinical tradeoff. Lower thresholds capture more cancers but overcall many benign nodules; higher thresholds are more specific but miss more cancers.

*[Insert Table 2 here]*
*[Insert Figure 2 here]*

### Patient-level ROM by TI-RADS category

Patient-level ROM substantially exceeded ACR-expected bands at TR1 through TR4: 28.2% for TR1, 32.1% for TR2, 27.6% for TR3, 47.4% for TR4, and 58.7% for TR5. Only TR5 fell within its ACR-expected band (>20%).

The directional gradient from TR3 to TR4 to TR5 was preserved, but absolute ROM was inflated. This is the pattern commonly seen in surgical cohorts: TI-RADS still discriminates, but patient-level category-specific ROM is not calibrated to outpatient or screening expectations.

*[Insert Table 3 here]*
*[Insert Figure 3 here]*

### Nodule-level sister analysis

The strict nodule-level sister cohort included 3,687 nodules from 1,668 patients, of which 631 were same-side pathology-matched malignant nodules within the primary 365-day window. Nodule-level discrimination was similar to patient-level discrimination (AUC 0.640).

Nodule-level ROM was 12.9% for TR2, 9.1% for TR3, 18.7% for TR4, and 26.1% for TR5. TR4 and TR5 fell within ACR-expected bands: TR4 18.7% (95% CI 16.3–21.5) within the expected 5–20% range, and TR5 26.1% (95% CI 23.7–28.6) within the expected >20% range. TR3 remained above the ACR-expected <5% band but was much closer to expectation than the patient-level TR3 estimate.

The divergence between patient and nodule grain quantifies multinodular attribution. Patient-versus-nodule inflation was +18.4 pp at TR3, +28.6 pp at TR4, and +32.6 pp at TR5. In practical terms, collapsing multinodular patients to a single maximum TI-RADS category inflated intermediate- and high-category ROMs by roughly 19–33 pp.

*[Insert Figure 3b here]*

### Bethesda × TI-RADS cross-stratification

At the patient level, Bethesda distribution among the 2,380 patients with cytology was: I (nondiagnostic) n=88 (ROM 29.5%), II (benign) n=897 (16.2%), III (AUS) n=403 (48.4%), IV (FN/SFN) n=275 (51.6%), V (suspicious) n=129 (89.9%), and VI (malignant) n=588 (83.7%); 995 patients had no Bethesda result on file.

At the strict nodule level, only 495 of 3,687 nodules (13.4%) had a bridged Bethesda value. The Bethesda × TI-RADS heatmap therefore primarily documents linkage coverage and cytology distribution among linked nodules rather than complete cytologic ascertainment.

*[Insert Table 4 here]*
*[Insert Supplementary Figure S1 here]*

### ACR FNA-eligibility audit

Retrospective application of ACR 2017 size-based FNA-eligibility rules flagged 1,553 patients as below the ACR FNA threshold — 46.0% of the 3,375-patient cohort. The audit identified 1,007 malignant patients above the ACR threshold and 472 malignant patients below it. The 472 cancers below threshold represent 31.9% of malignant patients (472/1,479) and 14.0% of the total cohort (472/3,375).

*[Insert Figure 4 here]*

### Era and match-window sensitivity

The era split used 2017-05-01 (the ACR TI-RADS publication boundary). At the patient grain, 422 patients were pre-2017 and 2,953 were post-2017; overall ROM was 40.0% pre-2017 and 44.4% post-2017. Patient-level ROM remained elevated across eras.

At the strict nodule grain, 381 nodules were pre-2017 and 3,306 were post-2017. Post-2017 nodule-level ROM preserved the main calibration result: TR4 ROM was 18.0% and TR5 ROM was 24.4%, both within ACR-expected bands. Pre-2017 strict-nodule ROM was higher (TR4 24.7%; TR5 41.6%).

Tightening the ultrasound-to-surgery window also supported the main finding. At 180 days, nodule-level ROM was 7.4% for TR3, 15.7% for TR4, and 22.2% for TR5 — TR4 and TR5 remained within ACR-expected bands. At 90 and 30 days, ROMs decreased further, as expected when fewer later surgeries were counted.

### Subgroup analyses

Subgroup AUCs by sex, age band, and surgery era preserved modest discrimination. AUC was 0.646 in women and 0.647 in men. By age band, AUC was 0.694 for patients <40, 0.663 for 40–54, 0.639 for 55–69, and 0.599 for ≥70. By era, AUC was 0.670 pre-2017 and 0.643 post-2017. ACR TI-RADS retains modest discrimination across demographic strata, but patient-level ROM remains a poor substitute for nodule-level calibration.

*[Insert Figure 5 here]*

---

## Discussion

### Principal findings

In this 25-year operative thyroid cohort of 3,375 patients, ACR TI-RADS provided modest patient-level discrimination for thyroid malignancy (AUC 0.648, 95% CI 0.630–0.667). TR≥TR4 was the Youden-optimal threshold, with sensitivity 71.3% and specificity 55.9% — values clinically plausible for an operative cohort: TI-RADS helps separate higher-risk from lower-risk patients but is not a stand-alone diagnostic test.

The key finding is calibration. Patient-level ROM was markedly higher than ACR-expected bands at TR1–TR4. When the same data were reanalyzed at the nodule grain, TR4 and TR5 ROM returned to the ACR-expected ranges (TR4 18.7%; TR5 26.1%). Patient-versus-nodule inflation was +28.6 pp at TR4 and +32.6 pp at TR5. Operative-cohort ROM elevation therefore reflects more than surgical selection bias; a substantial portion is multinodular attribution error introduced by collapsing multinodular patients to a single maximum TI-RADS category.

### Comparison with prior literature

Our patient-level results align with prior surgical validations of ACR TI-RADS. Published operative cohorts have reported wide malignancy prevalence, variable thresholds, and higher category-specific ROM than the original ACR bands [6–13]. A project-specific evidence review of 25 surgical-cohort studies found overall malignancy prevalence from 12.2% to 66.1%, with TR5 ROM often far above outpatient expectations [14]. This heterogeneity is expected because surgical cohorts are enriched for patients with cytologic, symptomatic, growth-related, or clinician-selected indications for operation.

The present study adds a denominator-level explanation. Prior studies generally asked, "How well does TI-RADS predict malignancy among patients or nodules that reached surgery?" Our paired analysis asks a narrower calibration question: "Does the apparent ROM inflation persist when the unit of analysis matches the clinical unit for TI-RADS decision-making?" For TR4 and TR5, the answer was no — the nodule-grain analysis recovered ACR-expected calibration.

### Multinodular attribution error

Multinodular attribution error is easy to miss because patient-level analysis feels clinically intuitive. Surgeons and endocrinologists often think in terms of patients, not rows of nodules. But TI-RADS assigns risk to nodules. When a patient has multiple nodules, a patient-level maximum TR score can misattribute the pathology outcome.

For example, a patient may have a TR5 benign nodule and a smaller ipsilateral TR3 cancer. At the patient level, the cancer is credited to TR5. Across thousands of patients, this convention raises apparent ROM in the maximum-TR categories. The same issue can occur in reverse for lower categories when small incidental cancers are discovered during surgery for otherwise benign multinodular disease.

The magnitude in this cohort was large: +28.6 pp at TR4 and +32.6 pp at TR5 — enough to make a calibrated nodule-level system appear poorly calibrated when evaluated only at the patient level.

### Clinical implications

For everyday practice, the findings support continued use of ACR TI-RADS as a nodule-level decision tool. The system showed modest discrimination at the patient level and recovered expected calibration at TR4 and TR5 when applied to strict, feature-complete nodules.

The results also caution against overinterpreting operative-cohort patient-level ROM. A patient-level TR4 ROM of 47.4% should not be quoted to a patient as the expected malignancy risk of an individual TR4 nodule in routine practice; the corresponding strict nodule-level ROM was 18.7%, within the ACR-expected 5–20% range.

The FNA-eligibility audit also requires careful interpretation. ACR rules flagged 1,553 patients (46.0% of the 3,375-patient cohort) as below the FNA threshold, and 472 malignant patients (31.9% of all malignancies; 14.0% of the cohort) fell below the rule. This does not mean ACR TI-RADS "missed" all clinically meaningful cancers, nor does it prove that all 1,553 flagged biopsies were unnecessary in real time. Surgical cohorts include incidental cancers, multifocal disease, compressive symptoms, patient preference, and non-imaging indications. The audit is best read as a size-threshold stress test, not a direct clinical-outcome trial.

### Methodological implications for future validation studies

Future operative-cohort TI-RADS studies should report nodule-level ROM whenever possible. Patient-level performance remains useful for surgical counseling and cohort description but should not be the sole calibration benchmark against ACR-published nodule-level risk bands.

A practical reporting standard would include: patient-level AUC and threshold metrics; nodule-level per-category ROM; explicit description of nodule–pathology matching; handling of multifocal and bilateral disease; and sensitivity analyses using shorter ultrasound-to-surgery windows. For cohorts spanning classification-system updates, investigators should describe how historical reports were re-scored or harmonized.

### Strengths

This study has several strengths. The cohort spans 25 years and includes 3,375 operative patients with definitive surgical pathology. The cohort is racially diverse, including 45.5% Black or African American patients. ACR TI-RADS was re-scored uniformly across pre- and post-2017 reports rather than relying on historical reporter labels. The nodule-level sister analysis was pre-specified and used a strict feature-complete ACR subset. The main calibration finding was robust in the post-2017 era and at the 180-day ultrasound-to-surgery window. All primary counts, ROMs, AUCs, and threshold metrics are traceable to locked summary tables and QA gates.

### Limitations

The study is limited by its operative design; results should not be generalized to unselected screening populations. The strict nodule-level subset excluded many nodules that lacked complete five-feature ACR scoring, especially in earlier reporting eras; the relaxed nodule cohort partly bounds this effect but does not eliminate completeness-related selection.

Nodule-pathology matching remains imperfect. Same-side matching within 365 days is clinically reasonable but cannot prove that the imaged nodule and the malignant pathology focus are always the same lesion. Tighter time windows support the main result, but anatomic linkage remains a limitation of retrospective data.

Per-nodule FNA size linkage is incomplete; size-aware ACR FNA-compliance analysis was therefore performed at the patient grain rather than fully at the nodule grain. Bethesda coverage at the nodule level was also incomplete: only 13.4% of strict-eligible nodules had a bridged Bethesda value.

The use of structured extraction with limited LLM augmentation is another consideration. However, 99.3% of strict-eligible nodule feature scores came from the structured imaging source, and the ACR algorithm was applied programmatically. Finally, this is a single-institution cohort; external validation is needed before these inflation estimates are treated as universal.

---

## Conclusions

In a 25-year single-institution operative thyroid cohort, ACR TI-RADS showed modest patient-level discrimination for thyroid malignancy (AUC 0.648, 95% CI 0.630–0.667), with TR≥TR4 as the Youden-optimal threshold. Patient-level ROM exceeded ACR-expected bands at TR1–TR4, reproducing the pattern commonly seen in surgical cohorts.

A pre-specified nodule-level sister analysis changed the interpretation. At the nodule grain, TR4 ROM was 18.7% and TR5 ROM was 26.1%, both within ACR-expected bands. Patient-versus-nodule inflation was +28.6 pp at TR4 and +32.6 pp at TR5, demonstrating that multinodular attribution error explains a substantial share of apparent operative-cohort ROM elevation.

Future operative-cohort TI-RADS validations should report per-nodule ROM in addition to patient-level performance.

---

## Acknowledgments

*[TODO: Confirm with senior author.]*

## Funding

*[TODO: Confirm.]* No external funding was received for this work unless otherwise specified by the senior author.

## Conflicts of Interest

The authors declare no conflicts of interest related to this work.

## Ethics / IRB

This retrospective analysis was conducted under Emory University Institutional Review Board protocol [#TBD] with informed-consent waiver. De-identified data only.

## Author Contributions

L.D.G.: Conceptualization, Data Curation, Formal Analysis, Methodology, Software, Visualization, Writing — Original Draft, Writing — Review and Editing.
[Co-author 2]: Data Curation, Investigation, Writing — Review and Editing.
[Senior Author]: Conceptualization, Methodology, Resources, Supervision, Writing — Review and Editing.

## Data and Code Availability

De-identified summary tables and reproduction SQL/Python code are included as Supplementary Material. Patient-level data are subject to institutional sharing rules and are available on reasonable request to the corresponding author.

---

## Tables

### Table 1. Baseline characteristics by maximum TI-RADS category

Patient cohort, n = 3,375.

| Max TR | n (%)         | Age, mean (SD) | Female n (%) | Black n | White n | Median imaging size (cm) | Malignant n (%) |
|--------|---------------|----------------|--------------|---------|---------|--------------------------|-----------------|
| TR1    | 340 (10.07%)  | 51.8 (14.8)    | 282 (82.9%)  | 184     | 120     | 1.06                     | 96 (28.24%)     |
| TR2    | 299 (8.86%)   | 53.3 (14.2)    | 253 (84.6%)  | 158     | 100     | 1.44                     | 96 (32.11%)     |
| TR3    | 845 (25.04%)  | 53.9 (14.9)    | 684 (80.9%)  | 489     | 272     | 1.76                     | 233 (27.57%)    |
| TR4    | 492 (14.58%)  | 54.2 (14.3)    | 397 (80.7%)  | 207     | 215     | 1.99                     | 233 (47.36%)    |
| TR5    | 1,399 (41.45%)| 53.6 (15.3)    | 1,075 (76.8%)| 497     | 675     | 2.29                     | 821 (58.68%)    |

### Table 2. Diagnostic performance of ACR TI-RADS at three pre-specified thresholds

| Grain           | Threshold       | TP    | FP    | FN  | TN    | Sens % (95% CI)    | Spec % (95% CI)    | PPV % | NPV % |
|-----------------|-----------------|-------|-------|-----|-------|--------------------|--------------------|-------|-------|
| Patient         | TR≥TR3          | 1,287 | 1,449 | 192 | 447   | 87.0 (85.2–88.6)   | 23.6 (21.7–25.5)   | 47.0  | 70.0  |
| Patient         | TR≥TR4 (Youden) | 1,054 | 837   | 425 | 1,059 | 71.3 (68.9–73.5)   | 55.9 (53.6–58.1)   | 55.7  | 71.4  |
| Patient         | TR≥TR5          | 821   | 578   | 658 | 1,318 | 55.5 (53.0–58.0)   | 69.5 (67.4–71.6)   | 58.7  | 66.7  |
| Nodule (strict) | TR≥TR3          | 627   | 3,029 | 4   | 27    | 99.4 (98.4–99.8)   | 0.9 (0.6–1.3)      | 17.2  | 87.1  |
| Nodule (strict) | TR≥TR4          | 485   | 1,616 | 146 | 1,440 | 76.9 (73.4–80.0)   | 47.1 (45.4–48.9)   | 23.1  | 90.8  |
| Nodule (strict) | TR≥TR5          | 324   | 917   | 307 | 2,139 | 51.4 (47.5–55.2)   | 70.0 (68.3–71.6)   | 26.1  | 87.5  |

Wilson 95% CIs. Patient AUC = 0.648 (95% CI 0.630–0.667). Nodule AUC = 0.640.

### Table 3. Patient-level versus nodule-level ROM by TI-RADS category with ACR-expected bands

| TR  | Patient n | Patient malignant | Patient ROM % (95% CI) | Nodule n | Nodule malignant | Nodule ROM % (95% CI) | ACR band | Nodule in band? | Inflation pp |
|-----|-----------|-------------------|------------------------|----------|------------------|-----------------------|----------|------------------|--------------|
| TR1 | 340       | 96                | 28.24 (23.71–33.24)    | —        | —                | —                     | <2%      | —                | —            |
| TR2 | 299       | 96                | 32.11 (27.07–37.60)    | 31       | 4                | 12.90 (5.13–28.85)    | <2%      | no               | +19.21       |
| TR3 | 845       | 233               | 27.57 (24.67–30.68)    | 1,555    | 142              | 9.13 (7.80–10.67)     | <5%      | no               | +18.44       |
| TR4 | 492       | 233               | 47.36 (42.98–51.77)    | 860      | 161              | 18.72 (16.26–21.47)   | 5–20%    | YES              | +28.64       |
| TR5 | 1,399     | 821               | 58.68 (56.08–61.24)    | 1,241    | 324              | 26.11 (23.74–28.62)   | >20%     | YES              | +32.57       |

Inflation = patient ROM − nodule ROM, in percentage points.

### Table 4. Bethesda × TI-RADS contingency at the strict-ACR-eligible nodule level

n = 3,687 nodules.

| Bethesda                 | TR2 | TR3   | TR4 | TR5   |
|--------------------------|-----|-------|-----|-------|
| I (nondiagnostic)        | 1   | 9     | 6   | 10    |
| II (benign)              | 0   | 84    | 29  | 45    |
| III (AUS)                | 2   | 31    | 25  | 26    |
| IV (FN/SFN)              | 0   | 23    | 14  | 19    |
| V (suspicious)           | 0   | 6     | 6   | 23    |
| VI (malignant)           | 3   | 23    | 33  | 77    |
| Missing (no FNA bridge)  | 25  | 1,379 | 747 | 1,041 |

495 (13.4%) of strict-eligible nodules had a bridged Bethesda value.

---

## Figure Legends

**Figure 1. Cohort flow diagram.** From 10,871 unique research IDs in the institutional thyroid surgical warehouse, 3,375 patients met inclusion criteria for the patient-level primary analysis. The pre-specified strict-ACR-eligible nodule subset included 3,687 nodules from 1,668 patients and was used for the nodule-level sister analysis. Exclusion arms quantify dropouts at each gate.

**Figure 2. Patient-level receiver-operating-characteristic curve.** ROC for ACR TI-RADS in the 3,375-patient operative cohort. AUC 0.648 (95% CI 0.630–0.667). Three pre-specified threshold operating points (TR≥TR3, TR≥TR4, TR≥TR5) are annotated. The Youden-optimal threshold, TR≥TR4 (J=0.271; sensitivity 71.3%, specificity 55.9%), is circled.

**Figure 3. Patient-level per-category risk of malignancy.** Patient-level ROM by ACR TI-RADS category with Wilson 95% CIs and overlay of ACR 2017 expected ranges (TR1 <2%, TR2 <2%, TR3 <5%, TR4 5–20%, TR5 >20%). Patient-level ROM exceeds expected bands at TR1–TR4; only TR5 falls within the expected band.

**Figure 3b. Patient- vs nodule-grain ROM with ACR-expected bands.** Paired patient-level and nodule-level per-category ROM with Wilson 95% CIs and ACR 2017 expected bands. Per-nodule TR4 (18.7%) and TR5 (26.1%) recover within-band calibration. Patient-versus-nodule inflation at TR4 and TR5 is +28.6 and +32.6 percentage points, quantifying multinodular attribution error.

**Figure 4. Diagnostic confusion at TR≥TR4 and ACR FNA-eligibility audit.** Patient-level confusion matrix at the Youden-optimal TR≥TR4 threshold (TP 1,054; FP 837; FN 425; TN 1,059) and ACR 2017 FNA-eligibility stacked chart. Retrospective rule application flagged 1,553 below-threshold cases (46.0% of the 3,375-patient cohort) and 472 malignant patients below the ACR FNA threshold (31.9% of 1,479 malignancies; 14.0% of the cohort).

**Figure 5. Subgroup forest plot — AUC by demographic stratum.** AUC stratified by sex, age band (<40, 40–54, 55–69, ≥70 years), and surgery era (pre-2017 / post-2017). Discrimination remains modest but is preserved across all strata. The overall cohort AUC of 0.648 is shown as a reference.

**Supplementary Figure S1. Bethesda × TI-RADS heatmap at the strict-eligible nodule level.** Heatmap of the Bethesda × TI-RADS contingency at the strict-eligible nodule level (n=3,687). 495 (13.4%) of strict-eligible nodules had a bridged Bethesda value. The dominant "Missing" row reflects the FNA-linkage carry-forward limitation.

---

## References

1. Haugen BR, Alexander EK, Bible KC, et al. 2015 American Thyroid Association management guidelines for adult patients with thyroid nodules and differentiated thyroid cancer. *Thyroid*. 2016;26(1):1–133.
2. Durante C, Grani G, Lamartina L, Filetti S, Mandel SJ, Cooper DS. The diagnosis and management of thyroid nodules: a review. *JAMA*. 2018;319(9):914–924.
3. Tessler FN, Middleton WD, Grant EG, et al. ACR Thyroid Imaging, Reporting and Data System (TI-RADS): white paper of the ACR TI-RADS Committee. *J Am Coll Radiol*. 2017;14(5):587–595.
4. Middleton WD, Teefey SA, Reading CC, et al. Multiinstitutional analysis of thyroid nodule risk stratification using the ACR TI-RADS. *AJR Am J Roentgenol*. 2017;208(6):1331–1341.
5. Ali SZ, Baloch ZW, Cochand-Priollet B, Schmitt FC, Vielh P, VanderLaan PA. The 2023 Bethesda System for Reporting Thyroid Cytopathology. *Thyroid*. 2023;33(9):1039–1044.
6. Gao L, Xi X, Jiang Y, et al. Comparison among TIRADS (ACR TI-RADS and KWAK-TI-RADS) and 2015 ATA Guidelines in the diagnostic efficiency of thyroid nodules. *Endocrine*. 2019;64(1):90–96.
7. Hoang JK, Middleton WD, Farjat AE, et al. Reduction in thyroid nodule biopsies and improved accuracy with American College of Radiology Thyroid Imaging Reporting and Data System. *Radiology*. 2018;287(1):185–193.
8. Middleton WD, Teefey SA, Reading CC, et al. Comparison of performance characteristics of ACR TI-RADS, Korean Society of Thyroid Radiology TIRADS, and ATA guidelines. *AJR Am J Roentgenol*. 2018;210(5):1148–1154.
9. Grani G, Lamartina L, Ascoli V, et al. Reducing the number of unnecessary thyroid biopsies while improving diagnostic accuracy: toward the "right" TIRADS. *J Clin Endocrinol Metab*. 2019;104(1):95–102.
10. Ha EJ, Na DG, Baek JH, Sung JY, Kim JH, Kang SY. US fine-needle aspiration biopsy for thyroid malignancy: diagnostic performance of seven society guidelines applied to 2000 thyroid nodules. *Radiology*. 2018;287(3):893–900.
11. Castellana M, Castellana C, Treglia G, et al. Performance of five ultrasound risk stratification systems in selecting thyroid nodules for FNA: a meta-analysis. *J Clin Endocrinol Metab*. 2020;105(5):dgz170.
12. Sahli ZT, Karipineni F, Hang JF, et al. The association between the Ultrasonography TIRADS classification system and surgical pathology among indeterminate thyroid nodules. *Surgery*. 2019;165(1):69–74.
13. Wright KL, Ramonell KM, Sutton W, et al. Critical evaluation of the ACR TI-RADS at a single academic center. *Surgery*. 2022;172(6):1571–1578.
14. Elicit. ACR TI-RADS malignancy prevalence in surgical cohorts. Evidence report. 2026. Unpublished evidence report supplied as source material.
15. Elicit. Standardizing diagnostic criteria in thyroid research when Bethesda and TI-RADS classification systems evolve. Evidence report. 2026. Unpublished evidence report supplied as source material.
16. Tappouni RR, Itri JN, McQueen TS, Lalwani N, Ou JJ. ACR TI-RADS: pitfalls, solutions, and future directions. *Radiographics*. 2019;39(7):2040–2052.
17. Baloch ZW, Asa SL, Barletta JA, et al. Overview of the 2022 WHO classification of thyroid neoplasms. *Endocr Pathol*. 2022;33(1):27–63.
18. Wilson EB. Probable inference, the law of succession, and statistical inference. *J Am Stat Assoc*. 1927;22(158):209–212.
19. Youden WJ. Index for rating diagnostic tests. *Cancer*. 1950;3(1):32–35.
20. Hanley JA, McNeil BJ. The meaning and use of the area under a receiver operating characteristic curve. *Radiology*. 1982;143(1):29–36.

---

## Final changes made

- **Harmonized FNA-audit denominator language.** The 1,553 below-threshold cases are now consistently expressed as 46.0% of the 3,375-patient cohort, and the 472 cancers below threshold are reported as 31.9% of the 1,479 malignant patients (and 14.0% of the cohort). Removed ChatGPT's meta-comment about the prior "15.0% of malignancies" wording so the manuscript reads cleanly. Figure 4 legend updated to match.
- **Standardized inflation rounding.** TR4 inflation reported as +28.6 pp throughout (matching exact 28.64); TR5 as +32.6 pp (matching 32.57). The original draft's stray "+28.7 pp" instances are eliminated.
- **Verified median age = 54 years** against `M025_master_data.xlsx` patient_master sheet (n=3,375; median 54.0; mean 53.57). Retained ChatGPT's correction.
- **Verified all primary statistics** (AUC 0.648, nodule AUC 0.640, sensitivity/specificity at all three thresholds, per-category ROM, Wilson CIs, era and match-window numbers) against `M025_tables_and_summary.xlsx` — every value matches the locked summary tables exactly.
- **Tightened prose** throughout: shortened wordy method sentences, reduced redundancy in Results paragraphs, and converted bullet-style restatements in Discussion into cleaner paragraphs while preserving every clinical claim.
- **Preserved all tables, figure references, and statistical results** verbatim from the approved package.
- **Kept all TODO markers** for senior-author input (acknowledgments, funding, IRB number, author list).
