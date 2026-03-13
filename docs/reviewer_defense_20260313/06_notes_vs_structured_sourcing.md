# Reviewer Defense: Which Variables Came From Notes vs. Structured Fields?

## Critical Principle

**All manuscript-critical metrics come from STRUCTURED sources.** NLP-derived variables provide supplementary enrichment only and are never the sole basis for primary outcome definitions.

## Structured-Primary Variables (Manuscript-Critical)

| Variable | Source Table | Rows | Notes |
|----------|-------------|------|-------|
| Histology type | `path_synoptics` | 10,871 | Normalized to 15 categories (PTC_classic through benign) |
| T/N/M staging | `path_synoptics` + `tumor_pathology` | ~4,100 | AJCC 8th Ed; microscopic ETE does NOT upstage T1–T2 |
| Surgery date | `path_synoptics.surg_date` | 10,871 | VARCHAR type; TRY_CAST for date operations |
| Procedure type | `operative_episode_detail_v2` | 9,371 | Normalized: total/hemi/unknown/other |
| Margins | `path_synoptics` | 3,957 | 'x' = involved; R-classification derived |
| Vascular/LVI invasion | `path_synoptics` | 3,846 / 3,780 | 'x' = present_ungraded; WHO 2022 grading where vessel count available |
| Molecular results | `molecular_testing` + `extracted_molecular_panel_v1` | 10,025 | ThyroSeq, Afirma, IHC, PCR, FISH |
| Thyroglobulin | `thyroglobulin_labs` | 30,245 values | Structured specimen collection dates; 2,569 patients |
| Recurrence flag | `recurrence_risk_features_mv` | 4,976 | Institutional registry boolean; NOT from NLP |
| Demographics | `path_synoptics` + `demographics_harmonized_v2` | ~11,000 | Cross-source harmonized from 7 tables |
| TIRADS | `raw_us_tirads_excel_v1` + `raw_us_tirads_scored_v1` | 3,474 | ACR recalculated + radiologist scored |

## NLP-Supplementary Variables (Enrichment Only)

| Variable | NLP Source | Raw Precision | Refined Precision | Patients | Notes |
|----------|-----------|---------------|-------------------|----------|-------|
| RLN injury detail | `note_entities_complications` | 3.5% | Tier 1–2: 100% | 92 | Context-aware refinement pipeline |
| Complication flags (6 entities) | `note_entities_complications` | 3.3% overall | Per-entity validated | 287 | chyle_leak, hypocalcemia, seroma, hematoma, hypoparathyroidism, wound_infection |
| Lateral neck dissection | Op note NLP | N/A (new) | High (clinical terms) | 78 (+41 structured) | Supplements structured 25→119 |
| Nodule sizes | H&P + op note NLP | N/A | Size guard 0.1–15 cm | 3,051 | Median 3.2 cm |
| BRAF recovery | `note_entities_genetics` | ~70% pre-correction | ~95% post-FP removal | 175 new | Requires explicit positive qualifier |
| RAS subtypes | `note_entities_genetics` + mutation text | Similar | ~95% | 316 positive | Gene-specific subtyping (NRAS/HRAS/KRAS) |
| Lab values (PTH/Ca) | Clinical note NLP | N/A | Plausibility guard per analyte | 673 PTH, 559 Ca | 5.1x and 8.1x increase over structured-only |

## Source Reliability Hierarchy

All NLP variables carry a source reliability score:

| Source Category | Reliability | Rationale |
|-----------------|------------|-----------|
| Path report | 1.0 | Gold standard; synoptic structured data |
| Op note | 0.9 | Surgeon-authored contemporaneous documentation |
| Endocrine note | 0.8 | Specialist follow-up documentation |
| Discharge summary | 0.7 | Summary of hospitalization |
| Imaging report | 0.7 | Radiologist-authored |
| Other note types | 0.5 | Variable quality |
| H&P / consent | 0.2 | Contaminated by boilerplate risk language |

## Consent Boilerplate Contamination

Every H&P note in the corpus contains verbatim risk-listing language:

> "...risks including scarring, hypocalcemia, hoarseness, chyle leak, seroma, numbness, orodental trauma..."

This contaminated **all raw NLP complication extractions** — overall precision was 3.3% before refinement. The refined pipeline applies:

1. **Source-type filtering**: H&P/consent notes excluded from complication entity extraction
2. **Context-aware rules**: risk discussion language, preservation language, historical references, and same-day H&P boilerplate patterns are explicitly excluded
3. **Positive qualifier gating**: for molecular markers, bare gene mentions (e.g., "BRAF") do NOT constitute positive results — explicit qualifiers (positive/detected/V600E/mutation identified) are required

## Molecular Marker False-Positive Correction

NLP entity `present_or_negated='present'` means a non-negated mention, NOT a positive test result. The 2026-03-12 FP audit found:

| Marker | Before Correction | After Correction | FP Removed | Reduction |
|--------|-------------------|------------------|------------|-----------|
| BRAF positive | 659 | 546 | 113 | 17.1% |
| RAS positive | 364 | 337 | 27 | 7.4% |
| TERT positive | 108 | 108 (unchanged) | 0 | 0% (structured-only) |

BRAF FP breakdown: 34 confirmed negatives, 68 ambiguous mentions, 11 conflicting context.

## Key References

- **Structured sources**: `path_synoptics`, `operative_episode_detail_v2`, `molecular_testing`, `thyroglobulin_labs`, `recurrence_risk_features_mv` (all on MotherDuck)
- **NLP refinement pipeline**: `notes_extraction/complications_refined_pipeline.py`, `notes_extraction/rln_refined_pipeline.py`
- **Molecular FP correction**: `notes_extraction/extraction_audit_engine_v9.py` (Phase 11)
- **Complication refinement audit**: `extracted_complications_exclusion_audit_v2` (MotherDuck)
- **Source reliability scores**: `notes_extraction/extraction_audit_engine_v4.py` (Phase 5 source hierarchy)
- **Validation table**: `val_complication_refinement` (MotherDuck, per-entity raw vs refined counts)
- **Overall data quality report**: `notes_extraction/phase8_final_report.md` (96/100 composite score)
