# RLN Injury Refinement Report

**Date:** 2026-03-12
**Pipeline:** `notes_extraction/rln_refined_pipeline.py`
**Evaluator:** `notes_extraction/intrinsic_evaluator.py`

## Executive Summary

The binary `rln_injury` flag was contaminated by NLP false positives from surgical
consent boilerplate and risk discussions, inflating the RLN injury rate to 6.25%
(679/10,871 patients). After context-aware refinement, the confirmed rate drops to
**0.54% (59 patients)** with an additional 33 suspected cases (0.85% total, 92 patients).
This aligns with published benchmarks of 1-3% transient / <1% permanent RLN injury.

## Problem Diagnosis

### Tier 3 Contamination Sources (Intrinsic Evaluation, n=200)

| Source | Prevalence | Example Context |
|--------|------------|-----------------|
| Risk discussion boilerplate | 92.0% | "risks including... recurrent laryngeal nerve injury" |
| Same-day H&P generic mention | 1.0% | Surgery-date H&P with "RLN injury" in consent |
| Nerve preservation language | 0.5% | "RLN identified and preserved" |
| Historical reference | 1.0% | "history of RLN injury at outside hospital" |
| **True injury** | **3.5%** | Post-op endocrine note: "right vocal cord paralysis" |
| Suspected (unclassified) | 3.0% | Same-day mention, ambiguous context |

**Estimated Tier 3 precision before refinement: 3.5%**

### Temporal Distribution (All Tier 3 Mentions)

| Window | Patients | Mentions | Likely True Injury |
|--------|----------|----------|--------------------|
| Same-day (d0) | 636 | 963 | ~5-10 (from complication sections) |
| 1-7 days | 1 | 1 | High probability |
| 8-30 days | 4 | 5 | High probability |
| 31-90 days | 4 | 9 | High probability |
| 91-365 days | 4 | 9 | High probability |
| >365 days | 11 | 13 | Likely permanent or historical |

## Refinement Rules

Eight SQL-based context classification rules, applied in priority order:

1. **Risk discussion detection**: Regex on 300-char context window for consent/risk language
2. **Preservation language detection**: "identified and preserved", "intact", "no injury"
3. **Historical reference**: "history of", "prior", "pre-existing" + injury terms
4. **Same-day H&P exclusion**: Day-0 H&P notes with generic "rln_injury" (not VCP/VCParesis)
5. **True injury language**: "noted to have", "postoperative hoarseness", "scope showed"
6. **Diagnosis section detection**: Mention within Assessment/Diagnosis/Complications headers
7. **Specific entity post-day-0**: vocal_cord_paralysis/paresis in post-op notes (high specificity)
8. **Same-day non-H&P suspected**: Specific entities in op notes (moderate signal)

## Results

### Rate Comparison

| Metric | Before | After |
|--------|--------|-------|
| Total RLN patients | 679 | 92 |
| RLN injury rate | 6.25% | 0.85% |
| Confirmed RLN injury | N/A | 59 (0.54%) |
| Tier 3 patients | 654 | 67 (34 confirmed + 33 suspected) |
| **Tier 3 excluded** | **0** | **587 (89.8%)** |

### Tier Breakdown (Refined)

| Tier | Source | N | Confirmed |
|------|--------|---|-----------|
| 1 | Laryngoscopy-confirmed | 6 | Yes |
| 2 | Chart-documented | 19 | Yes |
| 3 | NLP-confirmed (context-filtered) | 34 | Yes |
| 3 | NLP-suspected (weak evidence) | 33 | No |
| **Total** | | **92** | **59 confirmed** |

### Sensitivity Analysis: CLN-RLN Association

| Definition | N RLN | Rate | OR (CLN vs no-CLN) | p-value |
|------------|-------|------|---------------------|---------|
| Original (unfiltered) | 679 | 6.25% | 1.485 | <0.0001 |
| Tier 1 only | 6 | 0.06% | 0.475 | 0.677 |
| Tier 1+2 | 25 | 0.23% | 1.585 | 0.275 |
| Refined (all) | 92 | 0.85% | 1.679 | 0.016 |
| **Refined (confirmed)** | **59** | **0.54%** | **1.878** | **0.021** |

**Key finding:** Removing false positives *increases* the OR from 1.485 to 1.878.
The true signal was being diluted by noise. CLN is associated with ~88% higher
odds of confirmed RLN injury (p=0.021).

## Recommendations

1. **H1 re-analysis**: Re-run CLN-lobectomy hypothesis using `rln_injury_is_confirmed`
   flag from `extracted_rln_injury_refined_v2`. The PSM CLN-RLN OR of 1.93 should
   be verified with the refined flag — expect it to remain significant or strengthen.

2. **H2 implications**: The goiter-SDOH analysis can now use tiered RLN definitions
   for sensitivity analysis (tier 1+2 only vs refined all).

3. **Dashboard**: Update the Complications tab to show the refined tier breakdown
   with confirmed/suspected labels and a comparison card.

4. **Generalization**: The `refine_extraction(entity_name)` function can be applied
   immediately to hypocalcemia, hematoma, seroma, and chyle_leak using the same
   framework. Hypocalcemia is the recommended next target (2,740 NLP mentions likely
   have similar boilerplate contamination).

## MotherDuck Tables Created

| Table | Rows | Description |
|-------|------|-------------|
| `extracted_rln_injury_refined_v2` | 92 | Per-patient refined RLN injury detail |
| `extracted_rln_injury_refined_summary_v2` | 1 | Summary KPIs |
| `extracted_rln_exclusion_audit_v2` | 1 | Exclusion accounting |

## Files Delivered

| File | Purpose |
|------|---------|
| `notes_extraction/intrinsic_evaluator.py` | Reusable evaluation framework (any entity) |
| `notes_extraction/rln_refined_pipeline.py` | RLN-specific pipeline + deploy function |
| `prompts/rln_injury_v2.txt` | Structured LLM judge prompt template |
| `notes_extraction/rln_intrinsic_eval_report.md` | Intrinsic evaluation report |
| `notebooks/rln_sensitivity_v2.ipynb` | Interactive sensitivity analysis notebook |
| `exports/rln_sensitivity_analysis_v2.csv` | OR results for all tier definitions |
| `exports/rln_improvement_report_2026.md` | This report |
