# Supplementary Appendix: Data Quality and Provenance

**THYROID_2026 — Thyroid Surgery Outcomes Study**
Date: 2026-03-13
Git SHA: `e1e8897`
Pipeline version: v3.2.0-2026.03.13

---

## 1. Cohort Derivation

### 1.1 Study Population

The study cohort was derived from a single-institution thyroid surgery database
encompassing all patients who underwent thyroid surgery with pathology
documentation. Cohort construction proceeded through the following stages:

1. **Starting population.** All patients with at least one record in the
   institutional pathology synoptic database (`path_synoptics`): **N = 10,871
   unique patients** across 11,688 pathology records. Patients with multiple
   pathology records (e.g., staged bilateral procedures) were counted once using
   the unique `research_id` key.

2. **Surgery episode deduplication.** Pathology records were linked to surgery
   episodes. After removing 146 multi-pathology-per-surgery duplicates
   (prioritized by analysis eligibility, T-stage severity, tumor size, and
   linkage score), the cohort contained **9,368 unique surgery episodes**.

3. **Analysis eligibility.** Among the 10,871 patients, **4,136 met analysis
   eligibility criteria**, defined as confirmed thyroid malignancy with
   sufficient data for AJCC 8th Edition staging.

4. **Exclusions.** The remaining 6,735 patients were excluded for the following
   reasons: benign thyroid surgery (multinodular goiter, Graves disease,
   thyroiditis), completion thyroidectomies without primary malignancy data,
   missing histology type, and non-thyroid malignancy.

5. **Domain-specific subsets.** The following subsets are not sequential
   exclusions but cross-sectional availability counts drawn from the full
   10,871-patient surgical population:

   - Molecular-tested: 10,025 (92.2%)
   - RAI-treated (confirmed dose + assertion): 35
   - Thyroglobulin laboratory values available: 2,569
   - TIRADS scoring available: 3,474
   - Survival-eligible (positive follow-up time): 10,870

### 1.2 CONSORT-Style Flow

The full surgical pathology database contained 11,688 records from 10,871
patients. Episode deduplication removed 146 records where a single surgery
produced multiple pathology entries (e.g., separate synoptic reports for
distinct tumor foci resected in one operation), yielding 9,368 episodes.
Analysis eligibility required confirmed thyroid malignancy and at least one
calculable staging variable; 4,136 patients met this threshold. Exclusion
categories were not mutually exclusive: many benign-surgery patients also lacked
histology type classification. Domain-specific subsets (molecular testing,
imaging, laboratory) reflect data availability across the full cohort rather
than sequential filtering, because each analytic question draws on a different
combination of variables.

---

## 2. Data Sources and Linkage

### 2.1 Primary Data Sources

| Source | Records | Patients | Linkage Key | Primary Variables |
|--------|--------:|----------:|-------------|-------------------|
| Pathology synoptics (`path_synoptics`) | 11,688 | 10,871 | `research_id` | Histology, T/N staging, margins, invasion, LN yield |
| Tumor pathology (`tumor_pathology`) | ~4,000 | ~4,000 | `research_id` | Detailed tumor characteristics, molecular markers |
| Operative details (`operative_details`) | 9,371 | ~9,300 | `research_id` | Procedure type, EBL, laterality |
| FNA cytology (`fna_cytology`) | ~5,200 | ~5,200 | `research_id` | Bethesda category |
| Molecular testing (`molecular_testing`) | ~10,000 | ~10,000 | `research_id` | Platform, gene results |
| Clinical notes (`clinical_notes_long`) | 11,037 | ~5,500 | `research_id` | NLP-extracted supplementary variables |
| Thyroglobulin labs (`thyroglobulin_labs`) | 30,245 | 2,569 | `research_id` | Tg, anti-Tg values with specimen dates |
| RAI episodes (`rai_treatment_episode_v2`) | 1,857 | 862 | `research_id` | RAI assertion status, dose |
| Imaging (`raw_us_tirads_excel_v1`) | 19,891 | ~4,000 | `research_id` | TIRADS scoring, nodule dimensions |

All tables are linked via integer `research_id`, a de-identified patient
identifier assigned at data extraction. No protected health information
(MRN, date of birth, patient name) is stored in analytic tables.

### 2.2 Cross-Domain Episode Linkage

Episode linkage connects clinical events across data sources using a numeric
scoring framework:

| Linkage | Episodes | Linkage Rate |
|---------|--------:|--------------|
| Surgery → Pathology | 9,409 | 100% |
| FNA → Molecular | 708 | 100% |
| Preop → Surgery | 3,591 | 100% |
| Pathology → RAI | 23 | 100% |
| Imaging → FNA | 0 | N/A (empty source) |

Linkage scores range from 0.0 to 1.0 and combine three components: temporal
proximity (50% weight), laterality concordance (30% weight), and size
compatibility (20% weight), with an ambiguity penalty applied when multiple
candidate matches exist (0.1 per additional candidate). Confidence tiers are
derived from the numeric score: exact match (≥0.85), high confidence (≥0.65),
plausible (≥0.45), and weak (>0). Multi-candidate linkages are routed to a
dedicated ambiguity review queue.

The imaging-to-FNA linkage pathway has zero episodes because the canonical V2
imaging table (`imaging_nodule_long_v2`) contains no populated nodule-size data;
imaging measurements exist in the Excel-derived `imaging_nodule_master_v1` but
were not propagated to the V2 canonical layer.

---

## 3. Date Quality Taxonomy

### 3.1 Provenance Classification

All event dates in the analytic pipeline are assigned a provenance tier
reflecting the method by which the date was determined:

| Tier | Label | Confidence | Description |
|-----:|-------|:----------:|-------------|
| 1 | `exact_source_date` | 100 | Date directly from a structured field (e.g., `surgery_date`, `specimen_collect_dt`) |
| 2 | `inferred_day_level_date` | 70 | Clinical note date (`note_date`) used as proxy for entity date |
| 3 | `note_text_inferred_date` | 50 | Date extracted from clinical note body text via NLP |
| 4 | `coarse_anchor_date` | 35–60 | Surgery or FNA date used as temporal anchor for undated entities |
| 5 | `unresolved_date` | 0 | No date source available from any pipeline stage |

The fallback chain for date assignment follows the precedence:
`entity_date` → `note_date` → `note_body_date` → `surgery/FNA anchor` → NULL.
A future-date plausibility guard caps any inferred date at the current date.

### 3.2 Date Accuracy by Analyte

| Analyte | Date Source | Accuracy | Patients |
|---------|------------|:--------:|----------:|
| Thyroglobulin | `specimen_collect_dt` (structured lab) | 99.5% | 2,569 |
| Anti-thyroglobulin | `specimen_collect_dt` (structured lab) | 97.7% | 2,127 |
| PTH | NLP-extracted / note-anchored | Limited temporal fidelity | 673 |
| Calcium (total) | NLP-extracted / note-anchored | Limited temporal fidelity | 559 |
| TSH / free T4 / free T3 | No data source | N/A | 0 |

Thyroglobulin and anti-thyroglobulin values benefit from structured laboratory
specimen collection timestamps. PTH and calcium values were recovered through
NLP extraction from clinical notes and carry limited temporal fidelity
sufficient for broad temporal windowing (e.g., 0–30 days vs. 31–180 days
post-surgery) but not for precise postoperative-day analyses.

---

## 4. Domain Coverage

| Domain | Variable | Coverage (N/10,871) | Pct | Source Quality | Notes |
|--------|----------|--------------------:|----:|----------------|-------|
| Demographics | Age | 11,585 / 11,673 | 99.2% | Structured | Cross-source harmonized from 7 tables |
| Demographics | Sex | 10,880 / 11,673 | 93.2% | Structured | |
| Demographics | Race | 10,870 / 11,673 | 93.1% | Structured | 25+ raw categories normalized to 6 analytic groups |
| Pathology | Histology type | 4,137 / 10,871 | 38.1% | Structured | NA for benign-only patients |
| Pathology | Tumor size | 4,130 / 10,871 | 38.0% | Structured | |
| Pathology | AJCC8 T stage | 4,083 / 10,871 | 37.6% | Calculated | Derived from tumor size + ETE |
| Pathology | Vascular invasion | 3,846 / 10,871 | 35.4% | Structured | 87% present but ungraded (synoptic 'x' placeholder) |
| Molecular | Any testing | 10,025 / 10,871 | 92.2% | Structured + NLP | ThyroSeq, Afirma, IHC, PCR, FISH |
| Molecular | BRAF status | 376 / 10,025 | 3.8% | Structured + confirmed NLP | Corrected after false-positive removal |
| Molecular | RAS status | 292 / 10,025 | 2.9% | Structured + confirmed NLP | NRAS 196, HRAS 114, KRAS 59 |
| Molecular | TERT status | 108 / 10,025 | 1.1% | Structured | Recovered from 1 to 96 via platform data |
| RAI | Any episode | 862 / 10,871 | 7.9% | Structured + NLP | 1,857 total episodes |
| RAI | Confirmed with dose | 35 / 10,871 | 0.3% | Structured | Strict: definite received + dose |
| RAI | Dose available | 761 / 1,857 | 41.0% | Structured + NLP | No nuclear medicine notes in corpus |
| Imaging | TIRADS score | 3,474 / 10,871 | 32.0% | Excel + NLP + calculated | ACR concordance 80.1% |
| Labs | Thyroglobulin | 2,569 / 10,871 | 23.6% | Structured lab | 30,245 measurements |
| Labs | PTH | 673 / 10,871 | 6.2% | NLP + structured | Expanded from 131 via lab extraction |
| Labs | Calcium | 559 / 10,871 | 5.1% | NLP + structured | Expanded from 69 via lab extraction |
| Labs | TSH | 0 / 10,871 | 0% | Not available | No institutional lab feed |
| Outcomes | Recurrence flag | 1,986 / 10,871 | 18.3% | Structured | Exact recurrence date available for 54 (2.7%) |
| Complications | Any confirmed | 287 / 10,871 | 2.6% | Refined NLP + structured | 7 entity types |
| Complications | RLN confirmed | 59 / 10,871 | 0.54% | 3-tier refined | Tier 1: 6, Tier 2: 19, Tier 3 confirmed: 34 |
| Surgery | Procedure type | 8,733 / 10,871 | 80.3% | Structured | |
| Surgery | Date | 8,731 / 10,871 | 80.3% | Structured | |

Coverage percentages for pathology and staging variables are calculated against
the full surgical population (N = 10,871), which includes benign-only patients
for whom cancer-specific variables (histology type, AJCC staging) are not
applicable. Among the analysis-eligible cancer cohort (N = 4,136), coverage
rates for pathology variables exceed 95%.

---

## 5. Remaining Structural Gaps

The following gaps represent limitations that cannot be resolved through
additional computational processing and require new data sources or
institutional infrastructure changes:

| Gap | Impact | Classification | Resolution Pathway |
|-----|--------|----------------|-------------------|
| Nuclear medicine notes absent from note corpus | RAI dose coverage capped at 41% | Source-limited | Requires integration with nuclear medicine reporting system |
| Recurrence dates unresolved (88.8%) | Time-to-event analysis restricted to 222 dated events | Source-limited | Requires structured recurrence registry |
| Non-thyroglobulin lab dates (0% structured) | No day-level PTH/calcium/TSH temporal analysis | Source-limited | Requires institutional lab extract feed |
| Vascular invasion ungraded (78.7%) | WHO 2022 grading limited to 819 patients | Template-limited | Requires synoptic template modification to capture vessel count |
| Imaging nodule sizes in V2 canonical table | V2 table empty; data exists in imaging_nodule_master_v1 | Pipeline gap | Propagation from Excel-derived master table |
| Operative NLP fields (10 boolean fields at 0%) | No NLP-sourced operative detail enrichment | Pipeline gap | Requires NULL-default restructuring and V2 extractor materialization |
| Adjudication decisions (0 completed) | No manual review of algorithmically flagged discordances | Process gap | Review framework deployed, awaiting clinical adjudicator sessions |
| IHC BRAF (2 results only) | Minimal VE1 immunohistochemistry data | Source-limited | VE1 addendum reports not present in clinical note corpus |

Gaps are classified as **source-limited** (data does not exist in available
institutional systems), **template-limited** (data exists but is not captured
in current reporting templates), **pipeline-limited** (data exists but has not
been propagated through the computational pipeline), or **process-limited**
(infrastructure exists but human workflow has not been executed).

---

## 6. Validation Gates Summary

Prior to manuscript analysis, the dataset passed a 7-gate readiness assessment:

| Gate | Description | Result | Value |
|-----:|-------------|:------:|-------|
| G1 | Patient-level duplicate check | **PASS** | 0 duplicates in `manuscript_cohort_v1` |
| G2 | Episode-level duplicate check | **PASS** | 0 duplicates after deduplication (146 removed) |
| G3 | Scoring system calculability | **PASS** | AJCC8 37.6%, MACIS 37.5%, AGES 100%, AMES 100% |
| G4 | Complication entity types | **PASS** | 7 confirmed entity types |
| G5 | Supporting table population | **PASS** | All 15 supporting tables non-empty |
| G6 | Data quality audit | **PASS** | 0 null `research_id` values |
| G7 | Statistical analysis plan | **PASS** | 909-line formal SAP |

### 6.1 Additional Validation Infrastructure

- **Validation tables.** Sixteen `val_*` validation tables are maintained across
  the pipeline, covering provenance traceability, episode linkage completeness,
  scoring system calculability, complication refinement, staging recovery, lab
  completeness, TIRADS concordance, and structural gap quantification.

- **Metric registry.** A source-linked metric registry contains 12 canonical
  manuscript metrics, each defined by a SQL fragment traced to its source
  table(s).

- **Cross-source consistency.** Four cross-source consistency checks were
  performed: 3 of 4 returned CONSISTENT; the single mismatch was explained by
  multi-row patients in `recurrence_risk_features_mv` (up to 25 rows per
  `research_id`), resolved by `GROUP BY` aggregation.

---

## 7. NLP Extraction Quality

### 7.1 Complication Entity Precision

| Entity | Raw Precision | Refined Precision | Refinement Method |
|--------|:------------:|:-----------------:|-------------------|
| RLN injury | 3.5% | Tier 1–2: 100%; Tier 3: ~50% | Context-aware SQL exclusion of consent boilerplate |
| Chyle leak | ~3% | ~100% (confirmed) | Valsalva hemostasis phrase exclusion |
| Hypocalcemia | ~3% | ~100% (confirmed) | Consent boilerplate exclusion + medication confirmation |
| All complications (weighted) | 3.3% | Validated per entity | 13-phase extraction pipeline |

### 7.2 Contamination Source

All History and Physical (H&P) notes in the institutional corpus contain a
verbatim surgical risk-listing template (e.g., "risks include scarring,
hypocalcemia, hoarseness, chyle leak, seroma, numbness..."). This template
contaminates every NLP entity extraction for complication terms.
Additionally, operative notes frequently contain the phrase "Valsalva to 20–30
cm H₂O performed to confirm hemostasis and lack of a chyle leak," which
produces false-positive chyle leak mentions despite describing confirmed absence.

### 7.3 Refinement Pipeline

The extraction refinement pipeline comprises 13 phases (engine versions v1
through v11), applying the following strategies:

1. **Source-specific reliability scoring.** Each clinical note type is assigned
   a reliability tier: pathology report (1.0), operative note (0.9), endocrine
   note (0.8), discharge summary (0.7), imaging report (0.7), other (0.5),
   H&P/consent (0.2).

2. **Context-aware exclusion rules.** Risk discussion language, preservation
   documentation, historical references, and same-day H&P boilerplate are
   excluded through SQL-based heuristic rules.

3. **Positive qualifier gating for molecular markers.** NLP-extracted molecular
   entity mentions (e.g., "BRAF") require explicit positive qualifiers
   (positive, detected, V600E, mutation identified) in surrounding note text.
   Bare gene-name mentions are excluded from positivity counts.

4. **Cross-source concordance.** Where multiple sources report on the same
   variable, concordance is tracked and discordant cases are routed to manual
   review queues.

### 7.4 Molecular Marker Correction

An audit of NLP-derived molecular positivity identified systematic
false-positive contamination:

- **BRAF:** 659 initially flagged → 546 after removing 113 false positives
  (17.1% reduction). False positives comprised 34 confirmed negatives, 68
  ambiguous mentions, and 11 conflicting contexts.
- **RAS:** 364 initially flagged → 337 after removing 27 false positives
  (7.4% reduction).
- **TERT:** 108, unchanged (derived exclusively from structured sources).

---

## 8. Scoring Systems

The following validated scoring systems are computed for eligible patients:

| System | Calculable (N) | Calculable (%) | Required Inputs |
|--------|---------------:|:--------------:|-----------------|
| AJCC 8th Edition (T/N/M/Stage) | 4,083 | 37.6% | Tumor size, ETE, LN status, distant metastasis, age |
| ATA 2015 Initial Risk | 3,149 | 76.0%* | Histology, ETE, vascular invasion, LN status, completeness of resection |
| MACIS | 4,072 | 37.5% | Age, tumor size, completeness of resection, local invasion, distant metastasis |
| AGES | 10,871 | 100% | Age, tumor grade, ETE, tumor size |
| AMES | 10,871 | 100% | Age, distant metastasis, ETE, tumor size |

\* ATA calculability reported among analysis-eligible patients (N = 4,136).

Each scoring system includes a `*_calculable_flag` and `*_missing_components`
column; scores are never silently computed when required inputs are missing.

---

## 9. Reproducibility

- All analyses use fixed random seeds (`random_state=42`) for reproducibility.
- The complete analytic pipeline is version-controlled (Git SHA `e1e8897`).
- A formal statistical analysis plan (909 lines) is maintained at
  `docs/statistical_analysis_plan_thyroid_manuscript.md`.
- The Zenodo archive (DOI: 10.5281/zenodo.18945510) contains code, de-identified
  analytic tables, and documentation sufficient to reproduce all reported results.
- MotherDuck cloud database and local DuckDB are synchronized via a 163-entry
  materialization map covering all analytic tables.
