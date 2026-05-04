# M038 — Detailed Manuscript Outline (with cross-checked numbers)

**Title:** Massive Goiter at a Tertiary Referral Center: A Composite-Definition Descriptive Cohort of 2,501 Patients

**Cohort anchors (verified against Cohort Overview + Supp S1):**
- Total cohort: 10,871
- Massive: 2,501 (23.0%)
- Non-massive: 8,370 (77.0%)
- Components (cohort-wide): Weight ≥100 g = 1,429 (13.1%); Substernal CT/MRI = 1,047 (9.6%); Airway CT = 1,440 (13.2%)

---

## 1. ABSTRACT (≤350 words; structured)

- Background: massive goiter is heterogeneously defined; weight, mediastinal extension, and airway compromise are the three major axes; no single threshold captures all clinically demanding cases.
- Methods: single-institution retrospective cohort, 1999–2025; 10,871 adult thyroid surgeries; composite-massive flag = (gland_weight_final_g ≥100 g) OR substernal extension on CT/MRI OR airway compromise on CT (deviation/narrowing/compromise). Strict-definition complication rollup (post-mig_252) with hypoparathyroidism transient/permanent split per standing rule.
- Results: 2,501 (23.0%) met composite-massive criteria. 3-bucket era trend: 12.4% pre-2015 → 25.0% (2015–2019) → 32.3% (2020–2025); chi-squared = 449.9, df=2, p<0.001. Components by era show component documentation rates (substernal, airway) explain most of the rise. Massive enriched in males (30.3% vs 20.9%), Black patients (37.3% vs 13.6% in White), older age, higher BMI, higher comorbidity burden. Median gland weight 124.2 g (IQR 67.0–189.0) vs 22.0 g (13.0–40.0). Lower malignancy in massive (24.8% vs 40.6%; p<0.001) but more bilateral disease on clinical/imaging assessment (29.9% vs 16.6%); pathology-only bilateral disease was lower (5.8% vs 9.2%). More total thyroidectomy (66.9% vs 51.7%), longer operative time, more drain placement, higher tracheostomy rate (4.8% vs 3.1%). Strict-definition any-complication 5.3% vs 3.2% (RR 1.65, 95% CI 1.34–2.02), RLN injury 0.6% vs 0.1% (RR 6.69, 95% CI 2.70–16.57), VC paralysis 0.8% vs 0.05% (RR 15.90, 95% CI 5.41–46.68), transient hypoparathyroidism 3.3% vs 2.4% (RR 1.41, 95% CI 1.10–1.82).
- Conclusions: composite definition captures a clinically meaningful, increasingly prevalent phenotype with elevated technical risk despite lower malignancy. Era trend is driven by rising imaging documentation; demographic disparities and complication risk inform preoperative planning and counselling.

## 2. INTRODUCTION (3 paragraphs)

P1 — Clinical relevance:
- Goiter is one of the oldest indications for thyroidectomy.
- Preoperative weight or volume thresholds have been used (e.g., ≥100 g, ≥200 g), but capture only one axis.
- Substernal extension and airway compromise drive technical complexity independent of weight.

P2 — Definitional challenges:
- Heterogeneous definitions in the literature: weight ≥80–500 g; volume ≥100 mL; ≥50% retrosternal; clinical "giant," etc.
- Imaging-era artifact: substernal/airway compromise is documented only when cross-sectional imaging is obtained, biasing temporal trends.
- Need for a composite definition combining mass, mediastinal extension, and airway compromise.

P3 — Study rationale and aims:
- Aim 1: Quantify prevalence and demographic pattern of composite-massive phenotype across 27 years at a tertiary academic center.
- Aim 2: Characterize era-level trends and component contributions.
- Aim 3: Compare pathology, surgical context, and strict-definition complication outcomes between massive and non-massive arms.
- Hypotheses: composite prevalence is rising, driven by rising imaging documentation; massive enriched in males, Black patients, older, obese, more comorbid; lower malignancy but higher technical/complication burden.

## 3. METHODS (adapt eMethods)

- Design: retrospective cohort at Emory University Hospital and affiliates, 1999-01-01 to 2025-12-31.
- Source: THYROID_2026 canonical lakehouse (MotherDuck thyroid_canonical_publication_v1_0; release pub_v1_1_20260504; manuscript_workspace.cohort_m038_massive_goiter_v1 post mig_255).
- Cohort: 10,871 adults with ≥1 thyroid surgery and ≥1 resolved histologic record; no exclusions for age/prior surgery/indication/follow-up.
- Composite massive flag (3 components, OR-disjunction):
  - Weight (W): gland_weight_final_g ≥100 g.
  - Substernal (S): ct_substernal_extension_any OR mri_substernal_any.
  - Airway (A): ct_tracheal_deviation_any OR ct_tracheal_narrowing_any OR ct_airway_compromise_any.
- Variables (4.1–4.5): demographics (age, sex, race 9-bucket, BMI bmi_combined NSQIP-first vitals fallback), comorbidities (NSQIP + pmhx_nlp_*), surgical context (procedure type, NSQIP duration/LOS/drains/sealant/RLN monitoring/neck dissection), pathology (histology_final, is_malignant, bilateral flags, closest_margin_mm), strict complication rollup (suffix _confirmed/_definitive; mig_252) with hypoparathyroidism standing-rule split per memory/feedback_complications_transient_vs_permanent.md (transient <6 mo, permanent >6 mo, preexisting, new postop).
- Era binning: 5-yr (1999-2004, 2005-2009, 2010-2014, 2015-2019, 2020-2025); 3-bucket (pre-2015, 2015-2019, 2020-2025).
- Statistical methods: medians (IQR), means±SD; Mann-Whitney U; chi-squared (Fisher when expected<5); RR with Wald 95% CI on ln(RR); Wilson 95% CI for arm prevalence; Haldane–Anscombe 0.5 continuity correction in zero-cell scenarios; chi-squared trend test for era; two-sided p<0.05; no multiplicity correction (descriptive intent).
- Software: Python 3.9, pandas 2.3, scipy, openpyxl, python-docx; duckdb 1.4 client.
- Validation: 156-cell audit, 153 PASS / 3 patched / 0 FAIL on 2026-05-01.

## 4. RESULTS

### 4.1 Cohort and composite-flag composition
- 10,871 surgeries; 2,501 (23.0%) met composite criterion.
- Component contributions (cohort-wide): W = 1,429 (13.1%), S = 1,047 (9.6%), A = 1,440 (13.2%).
- Mutually exclusive Venn regions (Fig 1): W only 898 (35.9% of massive); S only 145 (5.8%); A only 429 (17.2%); W∧S (no A) 18 (0.7%); W∧A (no S) 127 (5.1%); S∧A (no W) 498 (19.9%); all three 386 (15.4%).

### 4.2 Demographics and comorbidities (Table 1)
- Age: median 56.0 (IQR 45.0–66.0) massive vs 50.0 (39.0–62.0) non-massive; mean 55.4±14.5 vs 50.5±15.2; p<0.001.
- Sex: 70.8% female in massive vs 79.9%; 29.2% male vs 20.1%; p<0.001.
- Race (massive vs non-massive within arm): White 28.5% vs 54.4%; Black 62.2% vs 31.2%; Asian 2.3% vs 5.0%; Hispanic 0.0% vs 0.3%. Stratum-prevalence: Black 1,555/4,168 = 37.3% massive; White 714/5,266 = 13.6% massive (p<0.001 across race).
- BMI: 32.1 (27.7–37.5) vs 28.5 (24.4–33.6); n=417 vs 1,668; p<0.001. Obese-III stratum 34.2% massive prevalence vs 10.3% normal (p<0.001).
- Comorbidities (NLP-extracted): hypertension 27.8% vs 12.9%; diabetes 20.0% vs 11.5%; CAD 3.4% vs 1.7%; CKD 3.4% vs 1.6%; COPD 1.9% vs 0.7%; mean # 2.78 vs 2.38 (all p<0.001 except no-events). Comorbidity burden 4+ → 40.0% massive prevalence (p<0.001 across burden).
- Thyroid history: Graves 4.3% vs 5.6% (p=0.014); Hashimoto 1.6% vs 2.5% (p=0.006); prior thyroidectomy 8.4% vs 7.8% (p=0.337).
- ASA III–V on NSQIP-linked subset (n=246/1,164): 0 documented (data-quality limitation; see 4.6).
- Smoking: NSQIP smoker 1.5% vs 1.6%; no association.

### 4.3 Size & weight metrics
- Median gland_weight_final_g: 124.2 (67.0–189.0) massive vs 22.0 (13.0–40.0) non-massive; n=2,158 vs 6,972.
- Cohort-wide gland weight by sex (final synoptic): Female median 27.0 (14.0–60.6) vs Male 35.4 (18.5–90.0); %≥100 g 11.7% Female vs 18.3% Male; %≥200 g 3.4% vs 7.9%; %≥500 g 0.3% vs 0.7% (p<0.001).
- By age: median rises from 23.5 g (<30 y) to 32.5 g (60–69) and 31.3 g (80+); %≥100 g rises from 6.7% (<30) to 16.4% (80+); p<0.001.
- By race: Black 54.0 (26.0–113.0) g, %≥100 g 23.8% vs White 20.0 (12.0–39.0) g, %≥100 g 6.0%; p<0.001.
- By BMI: obese III 48.8 g median, %≥100 g 17.3% vs normal 21.0 g median, %≥100 g 6.5%; p<0.001.
- By era (3-bucket): pre-2015 25.5 g median, %≥100 g 11.7%; 2015-2019 29.7 g, 13.6%; 2020-2025 32.0 g, 14.3%; p<0.001 — modest rise in actual gland weight is much smaller than rise in composite prevalence.
- Weight-bin distribution (cohort-wide): 100–<150 g n=624; 150–<200 g n=330; 200–<300 g n=294; 300–<500 g n=137; ≥500 g n=44.

### 4.4 Symptoms by demographics
- Substernal CT 8.5% female vs 11.9% male; airway-any 12.0% vs 17.7% (p<0.001).
- Tracheal deviation rises with age (4.7% <30 → 21.0% 80+); same pattern for narrowing/compromise.
- Race: Black patients have substernal-any 15.6% vs White 6.0%; airway-any 21.0% vs 8.1% (p<0.001).
- Era: substernal-any 0.4% pre-2015 → 11.3% 2015-2019 → 17.7% 2020-2025; airway-any 0.7% → 15.5% → 24.2% (p<0.001).
- Tracheostomy peri/postop 0.0% pre-2015, 0.4% 2015-2019, 9.5% 2020-2025 (note ascertainment shift).
- Difficult airway at intubation: 0.1% → 0.3% → 0.5% (p=0.005).
- Symptom co-occurrence (within massive): airway-CT components co-occur (deviation+narrowing 1,251; deviation+compromise 1,229; narrowing+compromise 1,156); substernal-CT overlaps with airway components (deviation 853, narrowing 794, compromise 781); 60 of 121 perioperative tracheostomies overlap substernal-CT (49.6%).

### 4.5 Era stratification (Table 5, Fig 2, Fig 4)
- 5-year buckets: 12.2% / 12.1% / 12.8% / 25.0% / 32.3%.
- 3-bucket: 12.4% / 25.0% / 32.3%; chi-squared 449.9, df=2, p<0.001.
- Component coverage cohort-wide (Fig 4): %weight ≥100 g rose modestly (12.2% → 14.3%); %substernal rose dramatically (0% → 17.7%); %airway rose dramatically (0.1% → 24.2%). Documentation effect is principal driver.

### 4.6 Pathology (Table 2)
- Malignant any: 24.8% massive vs 40.6% non-massive; p<0.001.
- Bilateral disease (clinical/imaging): 29.9% vs 16.6%; p<0.001.
- Bilateral disease (pathology only): 5.8% vs 9.2%; p<0.001 — direction-flip vs clinical/imaging.
- Histology among reported cases: PTC 16.7% vs 31.8% (p<0.001); follicular 3.9% vs 4.6% (NS); MTC 1.3% vs 1.4% (NS); poorly diff 0.9% vs 0.2% (p<0.001); anaplastic 0.5% vs 0.1% (p<0.001); diff high-grade 0.2% vs 0.0% (p=0.013); metastatic-PTC 0.4% vs 1.6% (p<0.001).
- Histology missingness: 74.2% massive vs 58.3% non-massive (Table 2).
- Closest margin (malignant only): 1.0 (0.4–1.0) mm vs 1.0 (0.3–1.0) mm; p=0.592.
- Subgroup malignancy within massive (Exploratory): weight-only 12.8%; substernal-only 42.8%; airway-only 46.2%; all-three 18.7% — airway-only and substernal-only carry highest malignancy.

### 4.7 Surgical context (Table 3)
- Total thyroidectomy 66.9% vs 51.7% (p<0.001); hemi 31.7% vs 43.5% (p<0.001).
- Operative duration (NSQIP, n=350 vs 911): 113.5 (90.0–153.0) vs 107.0 (81.5–152.0) min; mean 130.8±63.8 vs 121.3±54.2; p=0.009.
- LOS: median 1.0 (1.0–1.0) both arms; mean 1.3±1.3 vs 1.1±1.3; p=0.001 (analysis workbook) / 0.439 in pub Table 3 — report MWU per workbook.
- Drain placed: 49.4% vs 12.2% (pub Table 3 wording uses NSQIP-linked-with-drain-data subset 173/350 and 111/911 = 49.4% and 12.2%); analysis workbook reports 55.6% and 17.4% over a slightly different drain-data denominator (311/639). Use pub-Table 3 numbers in main text.
- Vessel sealant 91.6% vs 83.4% (p=0.006); RLN monitoring 85.5% vs 73.3% (p<0.001).
- Central neck dissection: 17.7% vs 30.4% (workbook); 15.7% vs 21.2% (pub Table 3) — different denominators; use pub Table 3 in main text. Lateral neck dissection 6.1% vs 3.1% (workbook; pub 5.4% vs 2.2%, p=0.003).
- Same-day discharge 34.0% vs 22.0%; p<0.001.
- Inpatient 17.7% vs 13.8% (NS p=0.083).
- Difficult airway op-note NLP 0.5% vs 0.2%; p=0.012.
- Tracheostomy 4.8% vs 3.1%; p<0.001.

### 4.8 Strict-definition complications (Table 4, Fig 3)
- Any confirmed: 5.3% vs 3.2%; RR 1.65 (1.34–2.02); p<0.001.
- Hematoma 0.9% vs 0.5%; RR 1.71 (1.04–2.82); p=0.033.
- Seroma 0.5% vs 0.3%; RR 1.49 (0.75–2.93); p=0.249.
- Chyle leak 0.1% vs 0.0%; RR 6.69 (0.61–73.79); p=0.134 (Fisher).
- RLN injury 0.6% vs 0.1%; RR 6.69 (2.70–16.57); p<0.001.
- VC paralysis 0.8% vs 0.05%; RR 15.90 (5.41–46.68); p<0.001.
- Hypocalcemia confirmed postop 0.04% vs 0.10% (1 vs 8); RR 0.42 (0.05–3.34); p=0.694; preexisting 0.3% vs 0.5%.
- Hypoparathyroidism transient (<6 mo): 3.3% vs 2.4%; RR 1.41 (1.10–1.82); p=0.008.
- Hypoparathyroidism permanent (>6 mo): 0.16% vs 0.14%; RR 1.12 (0.36–3.46); p=0.772.
- Hypoparathyroidism preexisting: 0.7% vs 0.5%; RR 1.35 (0.77–2.38); p=0.288.
- Hypoparathyroidism new postop: 4.0% vs 3.1%; RR 1.28 (1.02–1.60); p=0.034.
- Tracheostomy 4.8% vs 3.1%; RR 1.54 (1.25–1.90); p<0.001.
- Mortality (definitive) 1 vs 0; survival rollup 2.4% vs 1.6%; RR 1.48 (1.10–2.01); p=0.010 (any-time death).

### 4.9 NSQIP 30-day complications (NSQIP-linked subset)
- Denominators: massive 350, non-massive 911.
- RLN flag 11.7% vs 5.2%; RR 2.27 (1.52–3.39); p<0.001.
- Hypocalcemia flag 6.9% vs 6.4%; p=0.752 (NS).
- Hematoma flag 2.3% vs 0.9%; RR 2.60 (0.98–6.88); p=0.053.
- Unplanned return to OR 1.7% vs 0.5%; RR 3.12 (0.96–10.17); p=0.082.
- 30-day readmission 3.1% vs 2.0%; p=0.216.
- 30-day death 0/350 vs 1/911 (NS).
- Other 30-day events <1.5% in both arms.

### 4.10 Component subgroup outcomes (within massive)
- Weight only (n=898): any-comp 2.2%, hypoPT-transient 1.0%, tracheostomy 2.1%.
- Substernal only (n=145): any-comp 6.2%, hypoPT-transient 6.2%, tracheostomy 2.1%.
- Airway only (n=429): any-comp 7.7%, hypoPT-transient 4.7%, tracheostomy 6.8%.
- W∧S (incl. all-three) (n=404): any-comp 5.2%, hypoPT-transient 3.0%, tracheostomy 4.5%.
- W∧A (incl. all-three) (n=513): any-comp 5.3%, hypoPT-transient 3.1%, tracheostomy 5.1%.
- S∧A (incl. all-three) (n=884): any-comp 7.0%, hypoPT-transient 4.6%, tracheostomy 6.8%.
- All three (n=386): any-comp 5.2%, hypoPT-transient 3.1%, tracheostomy 4.4%.

### 4.11 Sensitivity (Supp S4) — weight ≥200 g
- Weight ≥200 g (n=475) vs Weight 100–<200 g (n=954) vs Non-massive (n=8,370):
  - Any-comp 2.1% vs 4.0% vs 3.2%.
  - HypoPT transient 1.5% vs 1.9% vs 2.4%.
  - HypoPT permanent 0.0% vs 0.2% vs 0.1%.
  - RLN injury 0.4% vs 0.0% vs 0.1%.
  - Tracheostomy 2.9% vs 3.4% vs 3.1%.
  - Mortality 0/475 vs 0/954 vs 0/8,370.

## 5. DISCUSSION (5 paragraphs)

P1 — Headline. Composite captures 23% of cohort vs ~13% by weight alone — confirms the additive value of substernal/airway components.

P2 — Era trend interpretation. The 12.4% → 32.3% rise is largely a documentation phenomenon: cohort-wide substernal CT/MRI rate rose from 0.4% to 17.7% and airway-CT from 0.7% to 24.2%, while %≥100 g rose only 11.7% → 14.3%. True biological prevalence shift is modest; the trend reflects increasing preoperative cross-sectional imaging utilization.

P3 — Demographic disparities. Massive enriched in Black patients (37.3% prevalence) at >2× the rate of White (13.6%); this aligns with prior reports of higher endemic goiter prevalence in African-descent populations and may also reflect longer time-to-presentation. Larger gland in Black patients (median 54 g vs 20 g) is the strongest demographic enrichment in the data. Sex (M>F), age, BMI, and comorbidity burden also enrich.

P4 — Pathology paradox and surgical complexity. Massive arm has lower malignancy (24.8% vs 40.6%) — consistent with the long-standing observation that benign multinodular goiter dominates the massive phenotype, while incidentally-discovered malignancies are typically smaller. Yet massive arm has higher complication rates (RR 1.65 any-comp; ~16× VC paralysis). Despite lower malignancy, technical risk is substantially higher: more total thyroidectomy, longer operative time, more drain placement, higher tracheostomy. Bilateral disease direction flips between clinical/imaging (massive >) and pathology-only (massive <), reflecting that bilateral large goiter is bulkier in clinical/imaging assessment but often histologically unifocal benign, vs. the smaller multifocal malignancies in non-massive.

P5 — Strengths and limitations + future directions:
- Strengths: 27-year horizon; 10,871 patients; tri-source phenotype; strict-definition complications with temporality split; reproducible lakehouse pipeline.
- Limitations: single-center; retrospective; era-effects from imaging documentation rather than disease incidence; histology missingness 74% in massive arm (gland often not formally reported when benign); NSQIP linkage only ~13% of cohort; preop voice/VC status not captured (CF-VC-PARALYSIS-PREOP-FLAG); no multiplicity correction; descriptive design without inferential primary.
- Future directions: prospective imaging-mandated cohort to disentangle documentation from biology; matched cohort analysis controlling for demographic enrichment; preop voice imaging; cost analysis.

## 6. CONCLUSIONS
- Composite-massive captures a clinically meaningful 23% phenotype.
- Era trend largely reflects rising imaging documentation.
- Despite lower malignancy, technical and complication burden is substantially higher.
- Composite definition supports preoperative risk stratification, referral planning, and informed consent.

## 7. REFERENCES (15–25 plausible)
(see manuscript draft)

## 8. TABLES / FIGURES
- Table 1. Demographics and comorbidities.
- Table 2. Pathology.
- Table 3. Surgical & operative context.
- Table 4. Strict-definition complications + RR (95% CI).
- Table 5. Era stratification + components.
- Fig 1. Venn (mutually exclusive 7 regions).
- Fig 2. Era trend (5-year + 3-bucket).
- Fig 3. Forest of complication RRs.
- Fig 4. Component coverage by era.
- Supp S1–S6.

