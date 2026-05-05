# Cursor Prompt: M047 Frozen Section Accuracy Analysis

**Agent:** Opus 4.7 (Composer 2.0) — requires NLP interpretation of raw frozen section text + complex concordance logic; Opus's reasoning depth is needed for parsing unstructured pathology text  
**Estimated time:** 3–4 hours  
**Date:** 2026-05-04

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, full cohort (N=10,871). Manuscript M047 evaluates the diagnostic accuracy of intraoperative frozen section in thyroid surgery.

### Frozen Section Distribution:
| Group | N | Malignant | Recurred |
|---|---|---|---|
| Frozen performed | 4,116 | 1,698 (41.3%) | 273 (6.6%) |
| No frozen | 6,755 | 2,321 (34.4%) | 241 (3.6%) |

### Structured Frozen Section Data:
- `frozen_any_performed_flag` — boolean (4,116 TRUE)
- `frozen_n_total` — count of frozen sections
- `frozen_source_hierarchy` — data source
- `syn_frozen_section` — boolean (4,086 TRUE among frozen performed)
- `syn_carcinoma_on_frozen` — boolean (581 TRUE = carcinoma detected on frozen, 3,535 FALSE)
- `syn_frozen_section_result` — **RAW TEXT** containing full pathology frozen section reports (unstructured)

### Critical Data Challenge:
`syn_frozen_section_result` contains raw pathology report text, NOT structured categories. Examples include full frozen section diagnoses like:
- "RL - PTC" (clear malignant)
- "LL - MNG" (clear benign — multinodular goiter)
- "FOLLICULAR LESION, DEFER TO PERMANENTS" (deferred/indeterminate)
- Multi-part reports with parathyroid, lymph node, and thyroid assessments combined

**The primary task is to parse `syn_frozen_section_result` into structured categories before any analysis can proceed.**

## Task

### 1. Parse Frozen Section Results into Structured Categories

Create an NLP/rule-based parser for `syn_frozen_section_result` that classifies each frozen section into:

**Frozen Section Diagnosis Categories:**
- `malignant` — PTC, carcinoma, positive for malignancy explicitly stated
- `suspicious` — "suspicious for," "cannot exclude," "atypical, rule out"
- `follicular_lesion` — "follicular lesion," "follicular neoplasm" (indeterminate)
- `benign` — MNG, colloid nodule, nodular hyperplasia, adenoma, goiter, negative for malignancy
- `deferred` — "defer to permanents," "pending permanent sections"
- `non_thyroid` — parathyroid only, lymph node only, soft tissue
- `unclassifiable` — cannot determine from text

**Parsing rules:**
1. Look for the thyroid-specific portion (ignore parathyroid, lymph node assessments)
2. If multiple thyroid assessments exist, use the one matching the dominant/index nodule
3. Key positive phrases: "PTC", "papillary carcinoma", "carcinoma", "malignant", "positive for"
4. Key negative phrases: "MNG", "multinodular goiter", "colloid nodule", "nodular hyperplasia", "benign", "negative for malignancy", "adenoma"
5. Key indeterminate phrases: "follicular lesion", "follicular neoplasm", "defer", "pending permanent"
6. Use `syn_carcinoma_on_frozen` (581 TRUE) as validation — most of these should map to "malignant"

### 2. Concordance Analysis

Compare frozen section diagnosis vs final surgical pathology (`histology_final` + `is_malignant`):

**2×2 Table:**
| | Final: Malignant | Final: Benign |
|---|---|---|
| Frozen: Malignant | True Positive | False Positive |
| Frozen: Benign | False Negative | True Negative |

Calculate:
- Sensitivity, specificity, PPV, NPV with 95% Wilson CI
- False negative rate (the most clinically relevant metric)
- Accuracy
- Handle "follicular_lesion" and "deferred" as a third category (indeterminate)

### 3. Indeterminate/Deferred Analysis

For frozen sections classified as "follicular_lesion" or "deferred":
- What fraction were malignant on final path?
- What fraction led to completion thyroidectomy?
- Cross-reference with `completion_thyroidectomy_resolved_v1` in `manuscript_workspace`

### 4. Frozen Section Impact on Surgical Decision

Compare patients WITH vs WITHOUT frozen section:
- Demographics (age, sex, BMI)
- Procedure type distribution (total vs hemi) — does frozen section change intraoperative decision?
- Completion thyroidectomy rate
- Complication rate
- Recurrence rate
- Use propensity score matching or logistic regression to control for confounders

### 5. Subgroup Performance

Stratify frozen section accuracy by:
- Tumor size (<1cm, 1–2cm, 2–4cm, >4cm)
- Histology subtype (PTC vs FTC vs other)
- Bethesda category (if available)
- Time period (by decade)
- Surgeon (if identifiable — likely not, but check)

### 6. False Negative Deep Dive

For false negatives (frozen benign/deferred, final malignant):
- What histology types were missed? (expect FTC, FVPTC, NIFTP)
- What was the tumor size?
- Did it lead to completion thyroidectomy?
- Were these clinically significant misses (recurrence, advanced stage)?

### 7. Output

Save to `studies/m047_frozen_section/`:
- `frozen_section_parsed.csv` — patient-level with raw text + parsed category + confidence
- `frozen_concordance_2x2.csv` — concordance tables
- `diagnostic_performance.csv` — sensitivity/specificity/PPV/NPV
- `indeterminate_analysis.csv` — deferred/follicular lesion outcomes
- `frozen_vs_no_frozen_comparison.csv` — comparative analysis
- `false_negative_analysis.csv` — deep dive
- `frozen_section_summary.tex` — LaTeX tables

### 8. Upload to MotherDuck

Create in `manuscript_workspace`:
- `m047_frozen_section_parsed_v1` — parsed frozen section results
- `m047_frozen_section_analysis_v1` — patient-level concordance

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR
- Boolean columns: use `IS TRUE`
- `syn_frozen_section_result` is raw text — expect pathology jargon, abbreviations, multi-part reports
- `syn_carcinoma_on_frozen` (581 TRUE) is a pre-existing NLP extraction that can validate your parser
- Many frozen sections in this cohort are for parathyroid identification, not thyroid diagnosis — filter these
- NIFTP should be classified as "benign" on final path for concordance purposes (it was reclassified from malignant to benign in 2016)
- The 4,116 frozen section patients likely have higher acuity (more total thyroidectomies, higher malignancy rate) — this is expected selection bias
