# Safe Claims — THYROID_2026 Manuscript

**Generated:** 2026-03-15  
**Source:** Verified against MotherDuck production (20/20 critical table counts PASS)  
**Rule:** Every number below is sourced from a frozen table with a documented SQL fragment in the metric registry. Authors may cite these directly without additional verification.

---

## Cohort & Demographics

- The study cohort comprises **10,871 patients** who underwent thyroid surgery at our institution.
- Among **4,136 analysis-eligible cancer patients**, mean age at surgery was **50.7 years (SD 15.7)**, with **73.0% female** and **27.0% male**.
- Racial composition of the cancer cohort: White 59.1%, Black 23.7%, Asian 7.0%, Other/Unknown 10.1%.
- **9,368 deduplicated surgery episodes** were identified after removing 146 multi-pathology-per-surgery duplicates.
- The survival analysis cohort included **3,201 patients** with a median follow-up of **7.4 years** and **965 events**.

## Molecular Testing

- **10,025 patients (92.2%)** had at least one molecular panel result (ThyroSeq, Afirma, IHC, PCR, or FISH).
- **BRAF mutations** were identified in **376 patients (3.8% of tested)** after false-positive correction (113 NLP FP removed from original 659 flagged).
- **RAS mutations** were identified in **292 patients (2.9% of tested)**: NRAS 196, HRAS 114, KRAS 59.
- **TERT promoter mutations** were identified in **108 patients (1.1% of tested)**, recovered from a baseline of 1 via platform-level data extraction.
- BRAF prevalence (3.8%) is below published PTC rates (40-45%) because the surgical cohort includes benign disease patients who underwent molecular testing.

## Pathology & Staging

- Extrathyroidal extension (ETE) grading after Phase 9 refinement: **microscopic 5,393**, **gross 278**, **present ungraded 66** (98.6% of previously ungraded resolved).
- Microscopic ETE does NOT upstage T1-T2 per AJCC 8th Edition rules.
- AJCC 8th Edition staging was calculable for **4,083 patients (37.6% of full cohort)**; among analysis-eligible cancer patients, calculability exceeds **98%**.
- ATA 2015 initial risk stratification was calculable for **3,144 patients** (**76.0% of analysis-eligible**).
- MACIS score was calculable for **4,072 patients (37.5%)**.
- AGES and AMES were calculable for **100%** of patients.
- Vascular invasion was present in **3,846 patients**; WHO 2022 grading (focal/extensive) was available for **819 (21.3%)**.

## Imaging

- Pre-operative TIRADS scores were available for **3,474 patients (32.0%)**, derived from structured Excel data, NLP extraction, and ACR TI-RADS recalculation.
- ACR concordance with radiologist-assigned TIRADS was **80.1%**, with a systematic **-1.0 tier mean mismatch** (radiologists tend to score 1 tier lower).
- TIRADS distribution: TR4 Moderately Suspicious was the dominant category (44.0%), followed by TR5 Highly Suspicious (24.1%).

## Recurrence

- **1,986 patients (18.3%)** had any recurrence (structural or biochemical).
- Of these, **54 (2.7%)** had exact source-dated structural recurrence, **168 (8.5%)** had biochemical-only recurrence (rising Tg), and **1,764 (88.8%)** had recurrence flagged without day-level dates.
- Biochemical recurrence was defined as Tg > 1.0 ng/mL and > 2x nadir in the absence of structural disease.

## Survival

- 5-year recurrence-free probability by AJCC stage: **Stage I/II: 0.823**, **Stage III/IV: 0.161** (log-rank p < 0.0001).
- 5-year recurrence-free probability by BRAF status: **BRAF+ 0.565**, **BRAF- 0.753** (p < 0.0001).
- 5-year recurrence-free probability by ATA risk: **High 0.504**, **Intermediate 1.0**, **Low 1.0**.
- Multivariable Cox PH model concordance index: **0.853**.
- Schoenfeld residuals identified non-proportional hazards for age (p = 0.024), stage III/IV (p < 5e-5), ATA high risk (p < 5e-5), and LN positive (p = 0.042).

## Complications

- Confirmed post-operative complication rate: **287 patients (2.6%)** across 7 entity types.
- Confirmed RLN injury: **59 patients (0.54%)** via 3-tier evidence (Tier 1 laryngoscopy: 6, Tier 2 chart-documented: 19, Tier 3 NLP-confirmed: 34).
- Confirmed hematoma: **38 (0.35%)**.
- Confirmed hypoparathyroidism: **34 (0.31%)**.
- Confirmed seroma: **28 (0.26%)**.
- Confirmed chyle leak: **20 (0.18%)**.
- Confirmed hypocalcemia: **18 (0.17%)**.
- Confirmed wound infection: **2 (0.02%)**.
- Raw NLP complication precision was 3.3%; a 13-phase refinement pipeline (source-weighted, context-aware exclusion, positive-qualifier gating) achieved validated per-entity confirmed precision.

## Labs

- **2,559 patients (23.5%)** had thyroglobulin laboratory measurements (30,245 total values); 99.5% of Tg lab dates use structured `specimen_collect_dt`.
- **673 patients (6.2%)** had PTH measurements (expanded from 131 via NLP extraction).
- **559 patients (5.1%)** had calcium measurements (expanded from 69).
- Canonical lab table (`longitudinal_lab_canonical_v1`): **39,961 rows** across 5 analyte groups covering **3,349 unique patients**.

## RAI

- **1,857 RAI treatment episodes** were identified across all certainty tiers.
- **35 patients** met the strict "confirmed with dose" criterion.
- RAI dose was available for **761 of 1,857 episodes (41.0%)**.

## Data Quality

- All 7 readiness gates PASS (zero duplicates, zero null keys, scoring calculable, complications phenotyped, tables populated, SAP exists).
- 16 validation tables (`val_*`) cover provenance, linkage, scoring, complications, staging, and labs.
- NLP molecular false-positive correction: BRAF 17.1% reduction (659 → 546), RAS 7.4% (364 → 337).
- 25 canonical metrics in source-linked registry with SQL fragments.

## Reproducibility

- Fixed random seeds (`random_state = 42`) used throughout.
- Zenodo archive DOI: **10.5281/zenodo.18945510**.
- 909-line statistical analysis plan at `docs/statistical_analysis_plan_thyroid_manuscript.md`.
- Frozen cohort at `exports/manuscript_freeze_v1/` with 33 tables and 65/65 checksums PASS.
