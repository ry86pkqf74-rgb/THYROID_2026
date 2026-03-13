# Pre-Manuscript Analysis Checklist

**Version:** 1.0  
**Last updated:** 2026-03-13  
**Purpose:** Verify that the analysis pipeline is ready before running statistical analyses or drafting manuscript results sections.

---

## Default Analysis Source

All manuscript analyses should use the **analysis-grade resolved layer** as the primary data source:

| Table | Grain | Use for |
|-------|-------|---------|
| `patient_analysis_resolved_v1` | 1 row per patient | Table 1, logistic regression, patient-level outcomes |
| `episode_analysis_resolved_v1` | 1 row per surgery | Per-surgery complication analysis, episode-level outcomes |
| `lesion_analysis_resolved_v1` | 1 row per tumor | Tumor-level pathology, per-lesion molecular correlation |
| `longitudinal_lab_clean_v1` | 1 row per lab result | Tg trajectory, mixed-effects models, biochemical recurrence |
| `recurrence_event_clean_v1` | 1 row per event | Time-to-event, Kaplan-Meier, Cox PH |

Legacy views (`risk_enriched_mv`, `advanced_features_v3`, `ptc_cohort`) remain available as fallback but are NOT preferred for new manuscript analyses.

The `ThyroidStatisticalAnalyzer` class in `utils/statistical_analysis.py` now defaults to the resolved layer via `_VIEW_PRIORITY`.

---

## Scripts to Run Before Statistics

Execute in this order against MotherDuck (`--md`):

```bash
# 1. Upstream canonical tables (if not already deployed)
.venv/bin/python scripts/22_canonical_episodes.py --md

# 2. Supporting analysis tables (scripts 49-53)
.venv/bin/python scripts/51_thyroid_scoring_systems.py --md
.venv/bin/python scripts/50_multinodule_imaging.py --md
.venv/bin/python scripts/49_enhanced_linkage_v3.py --md
.venv/bin/python scripts/52_complication_phenotyping_v2.py --md
.venv/bin/python scripts/53_longitudinal_lab_hardening.py --md

# 3. Build resolved layer
.venv/bin/python scripts/48_build_analysis_resolved_layer.py --md

# 4. Materialize to MotherDuck
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/26_motherduck_materialize_v2.py --md

# 5. Verify
.venv/bin/python scripts/54_motherduck_verification_reports.py --md
.venv/bin/python scripts/55_analysis_validation_suite.py --md --strict
.venv/bin/python scripts/56_pre_manuscript_audit.py --md
```

---

## Verification Reports to Check

All reports are in `exports/verification_reports/`:

| Report | Must-check items |
|--------|-----------------|
| `analysis_grade_cohort_verification_report.md` | Row counts, 0 duplicates, null audit |
| `linkage_quality_report.md` | Linkage tier distribution, ambiguity counts |
| `scoring_coverage_report.md` | AJCC8/ATA/MACIS calculability %, concordance |
| `complication_definition_report.md` | Raw vs confirmed counts, date precedence |
| `pre_manuscript_audit_report.md` | Clinical face validity, provisional flags |
| `resolved_layer_data_dictionary.csv` | Per-column null rates for all domains |

---

## Provisional / Requires Manual Review

These fields have definitions that need clinician verification before publication:

| Field | Issue | Action Required |
|-------|-------|----------------|
| `ata_response_category` | Uses suppressed Tg nadir as proxy; stimulated Tg unavailable | Document limitation in Methods; consider excluding from primary analysis |
| `biochemical_recurrence_flag` | Simplified "rising Tg > 2× nadir" definition | Verify anti-Tg antibody interference; document assay context |
| `rln_permanent_flag` | Based on note documentation; may underestimate if follow-up sparse | Cross-check patients with `lab_completeness_score < 40` |
| `hypocalcemia_status` / `hypoparathyroidism_status` | PTH/Ca labs have limited coverage (5–8%) | State lab coverage limitation; use confirmed cases only |
| `macis_score` / `ages_score` | Histologic grade sparse (<30%); scores affected by missing grade input | Report calculability % alongside score results |
| Cross-exam nodule identity | Heuristic laterality+size matching for serial US | Verify for patients with >1 exam |

---

## Known Data Limitations

1. **imaging_nodule_long_v2** size/TIRADS columns are empty on MotherDuck. Use `raw_us_tirads_excel_v1` and `extracted_tirads_validated_v1` as the actual imaging data source.

2. **molecular_test_episode_v2 ras_flag bug**: `ras_flag` can be FALSE despite `ras_subtype` being populated (184 patients). Always use `ras_positive_final` from the resolved layer.

3. **patient_refined_master_clinical_v12** has 12,886 rows for 10,871 unique patients (ratio 1.19). Always deduplicate with `GROUP BY research_id` or `QUALIFY ROW_NUMBER()` before patient-level joins.

4. **Histology_final is NULL for ~62% of patients** — these are non-cancer (benign) surgical procedures. Filter on `analysis_eligible_flag = TRUE` for cancer-specific analyses.

5. **Scoring tables (AJCC8, ATA, MACIS) require script 51** to be materialized on MotherDuck before they produce non-NULL values in the resolved layer.

---

## Readiness Gate

The pipeline is **ready for statistical analysis** when ALL of the following are true:

- [ ] `patient_analysis_resolved_v1` has 0 duplicate research_ids
- [ ] `episode_analysis_resolved_v1` has <10 duplicate (research_id, surgery_episode_id) pairs
- [ ] Scoring calculability: AJCC8 > 20%, ATA > 20% (among analysis-eligible)
- [ ] Complication rates are non-zero for at least 3 entity types
- [ ] `val_analysis_resolved_v1` has 0 FAIL results (or all FAILs are documented exceptions)
- [ ] Pre-manuscript audit report reviewed by clinical team
- [ ] All provisional fields documented in manuscript Methods section
