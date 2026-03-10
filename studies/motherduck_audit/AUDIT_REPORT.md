# THYROID_2026 — MotherDuck Audit Report

**Generated:** 2026-03-10  
**Scope:** Full audit of adjudicated table usage, materialization gaps, raw-text normalization, chronology validation, and linkage risks.

---

## 1. Scorecard — MotherDuck Upgrade Pass

| Metric | Before Upgrade | After Upgrade | Status |
|--------|---------------|---------------|--------|
| Adjudicated views deployed (scripts 15–20) | 0 | 47 views/tables | DONE |
| V2 canonical episode tables (script 22) | 0 | 9 tables | DONE |
| V2 linkage tables (script 23) | 0 | 6 tables | DONE |
| V2 reconciliation review views (script 24) | 0 | 5 views | DONE |
| V2 QA tables (script 25) | 0 | 4 tables | DONE |
| MotherDuck materialized tables (script 26) | 0 | 20 md_ tables | DONE |
| Date provenance columns (script 27) | 0 | 4 cols on 6 tables | DONE |
| Streamlit V2 dashboard tabs | 0 | 6 tabs | DONE |
| Streamlit adjudication tabs | 0 | 8 tabs | DONE |
| Manuscript export bundle | 0 | 4 cohorts (CSV+Parquet+manifest) | DONE |
| Post-review overlay views | 0 | 3 (histology/molecular/RAI) | DONE |
| Reviewer persistence tables | 0 | 2 tables + 8 views | DONE |
| Validation test suite (script 21) | 0 | Covers overlays, exports, manuscript | DONE |
| Normalization maps in vocab.py | 0 | 8 maps | PARTIAL |
| V2 extractors built | 0 | 5 extractors | BUILT (not wired) |

### Remaining Gaps

| Gap | Severity |
|-----|----------|
| 6 Streamlit-critical views NOT materialized | HIGH |
| V2 extractors not wired into pipeline | HIGH |
| 6 V2 linkage tables not materialized | MEDIUM |
| ~18 raw-text fields across 5 domains | MEDIUM |
| 6 missing chronology rules | MEDIUM |
| dashboard.py QA tab uses old qa_issues | LOW |
| 2 missing tbl_exists fallbacks | LOW |

---

## 2. Streamlit Table Usage Audit

### Correctly Using Adjudicated Tables (13 modules)

All V2 app modules (`extraction_completeness.py`, `molecular_dashboard.py`, `rai_dashboard.py`, `imaging_nodule_dashboard.py`, `operative_dashboard.py`, `adjudication_summary.py`) correctly use `_resolve_view()` with canonical-then-md_ fallback.

All adjudication modules (`cohort_qc.py`, `patient_audit.py`, `review_histology.py`, `review_molecular.py`, `review_rai.py`, `review_timeline.py`, `review_queue.py`, `diagnostics.py`) correctly reference scripts 18–19 views.

### Issues Found

| File | Issue | Fix |
|------|-------|-----|
| `dashboard.py` (QA tab, ~L1200) | Uses `qa_issues` instead of `qa_issues_v2` | Replace with `qa_issues_v2` + `md_qa_issues_v2` fallback |
| `app/adjudication_summary.py` (L23) | `mrq_view` uses `_resolve_view(con, "md_manual_review_queue_summary_v2", "md_manual_review_queue_summary_v2")` — same name twice | Change to `_resolve_view(con, "manual_review_queue_summary_v2", "md_manual_review_queue_summary_v2")` |
| `app/review_molecular.py` (~L70) | `molecular_episode_v3` used without `tbl_exists` | Wrap in tbl_exists for graceful degradation |
| `app/review_rai.py` (~L38) | `rai_episode_v3` used without `tbl_exists` | Wrap in tbl_exists for graceful degradation |

### Legacy Tabs (Design Decision, Not Bugs)

`dashboard.py` Overview/Visualizations/Genetics tabs use pre-adjudication tables (`master_cohort`, `tumor_pathology`, `nuclear_med`, `genetic_testing`). These serve full-cohort browsing; manuscript/analysis views are analysis-eligible subsets. No change required unless the user wants to unify.

---

## 3. Performance Bottlenecks — Materialization Gaps

### P0 — Must Materialize (Streamlit-critical, expensive)

| View | Source Script | Joins | CTEs | Window Fns | Impact |
|------|-------------|-------|------|------------|--------|
| `streamlit_cohort_qc_summary_v` | 18 | 20+ subqueries | 0 | 0 | HIGH — loads on every Cohort QC tab open |
| `patient_reconciliation_summary_v` | 18 | 7 | 8 | 0 | HIGH — backbone for patient audit |
| `streamlit_patient_header_v` | 18 | 0 (depends on above) | 0 | 0 | HIGH — per-patient query |
| `streamlit_patient_timeline_v` | 18 | 0 (depends on patient_master_timeline_v2) | 0 | 0 | HIGH — per-patient query |
| `streamlit_patient_conflicts_v` | 18 | 3 UNIONs | 0 | 0 | HIGH — per-patient query |
| `streamlit_patient_manual_review_v` | 18 | 4 UNIONs | 0 | 0 | HIGH — per-patient query |

### P1 — Should Materialize (upstream heavy views)

| View | Source Script | Joins | CTEs | Window Fns | Impact |
|------|-------------|-------|------|------------|--------|
| `histology_reconciliation_v2` | 16 | 5+ | 6 | ROW_NUMBER | MEDIUM |
| `validation_failures_v3` | 17 | UNION of 6 enriched + 3 reconciled | 7 | ROW_NUMBER | MEDIUM |
| `molecular_episode_v2` | 16 | 4+ | 6 | ROW_NUMBER | MEDIUM |
| `rai_episode_v2` | 16 | 2 | 3 | ROW_NUMBER | MEDIUM |
| `patient_master_timeline_v2` | 16 | UNION of 3 reconciled | 1 | 0 | MEDIUM |
| `patient_manual_review_summary_v` | 18 | UNION of 4 queues | 1 | 0 | MEDIUM |

### P2 — Nice to Have

| View | Impact |
|------|--------|
| 6 enriched_note_entities_* views | LOW-MEDIUM (batch) |
| `histology_analysis_cohort_v` | LOW |
| `molecular_episode_v3`, `rai_episode_v3` | LOW |
| `adjudication_progress_summary_v` | LOW |

### Cross-Database Join Risk

Enriched views join `note_entities_*` (potentially in `thyroid_share`) with `path_synoptics`, `fna_history`, `molecular_testing` (in `thyroid_research_2026`). When deployed to MotherDuck with share attached, this is a cross-database join. Materializing the enriched views eliminates this.

### Recommended Additions to Script 26

```python
# P0 — Streamlit-critical
("md_streamlit_cohort_qc_summary",    "streamlit_cohort_qc_summary_v"),
("md_patient_recon_summary",          "patient_reconciliation_summary_v"),
("md_streamlit_patient_header",       "streamlit_patient_header_v"),
("md_streamlit_patient_timeline",     "streamlit_patient_timeline_v"),
("md_streamlit_patient_conflicts",    "streamlit_patient_conflicts_v"),
("md_streamlit_patient_manual_review","streamlit_patient_manual_review_v"),

# P1 — Upstream heavy
("md_histology_reconciliation_v2",    "histology_reconciliation_v2"),
("md_validation_failures_v3",         "validation_failures_v3"),
("md_molecular_episode_v2",           "molecular_episode_v2"),
("md_rai_episode_v2",                 "rai_episode_v2"),
("md_patient_master_timeline_v2",     "patient_master_timeline_v2"),
("md_patient_manual_review_summary",  "patient_manual_review_summary_v"),
```

---

## 4. Raw-Text Fields — Normalization Gaps

### HIGH Priority (manuscript-critical)

| Domain | Field | Current State | Action Needed |
|--------|-------|---------------|---------------|
| Pathology | `margin_status` | Raw text in path_synoptics | Apply HISTOLOGY_DETAIL_NORM or add MARGIN_NORM |
| Pathology | `vascular_invasion` | Raw text | Normalize to yes/no/focal/extensive |
| Pathology | `extrathyroidal_extension` | Raw text/boolean mix | Normalize to none/microscopic/gross; ETE manuscript dependency |
| Pathology | `extranodal_extension` | Raw text | Normalize to yes/no |
| Molecular | `mutation` | Raw text in molecular_testing | Consolidate into MOLECULAR_VARIANT_NORM (BRAF_V600E, NRAS_Q61, etc.) |
| Molecular | `detailed_findings_raw` | Full raw text | Structured parsing for key variants |
| RAI | `stimulated_tg`, `stimulated_tsh` | NULL in rai_treatment_episode_v2 | RAIDetailExtractor parses these; wire into script 22 |
| RAI | `rai_assertion_status` | Ad-hoc regex in script 22 | Use RAI_STATUS_NORM from vocab.py |
| Operative | `rln_monitoring_flag` | Hardcoded FALSE | OperativeDetailExtractor parses this; wire in |
| Operative | `rln_finding_raw` | NULL | OperativeDetailExtractor has this; wire in |

### MEDIUM Priority (analysis-useful)

| Domain | Field | Current State | Action Needed |
|--------|-------|---------------|---------------|
| Pathology | `lymphatic_invasion`, `perineural_invasion`, `capsular_invasion` | Raw text | Binary normalization |
| Pathology | `consult_diagnosis` | Free text snippet | Keep raw; optional histology extraction |
| Molecular | `platform_raw` | Partial (ThyroSeq only) | No version (v2/v3); align with MOLECULAR_PLATFORM_NORM |
| Molecular | Gene fusions | Semi-normalized | Add GENE_FUSION_NORM (RET-PTC1/2/3, PAX8-PPARG, NTRK) |
| RAI | `scan_findings_raw` | NULL | Wire RAIDetailExtractor output |
| RAI | `iodine_avidity_flag` | NULL | Wire RAIDetailExtractor output |
| Imaging | `composition`, `echogenicity` | Raw text from ultrasound_reports | Apply COMPOSITION_NORM, ECHOGENICITY_NORM |
| FNA | `pathology_diagnosis` | Free text | Consider Bethesda category extraction |

### Vocab.py Gaps

| Map | Status |
|-----|--------|
| MOLECULAR_PLATFORM_NORM | Defined, not used by extractors |
| MOLECULAR_RESULT_NORM | Defined, not used |
| RAI_INTENT_NORM | Defined, not used (script 22 uses CASE) |
| RAI_STATUS_NORM | Defined, not used |
| COMPOSITION_NORM | Defined, partial overlap with extractor |
| ECHOGENICITY_NORM | Defined, not used |
| OPERATIVE_FINDING_NORM | Defined, not used |
| HISTOLOGY_DETAIL_NORM | Defined, not used by extractor |
| **Missing:** SHAPE_NORM | Need to add |
| **Missing:** MARGINS_NORM | Need to add |
| **Missing:** CALCIFICATION_NORM | Need to add |
| **Missing:** VASCULARITY_NORM | Need to add |
| **Missing:** ETE_DETAIL_NORM | Need to add |
| **Missing:** AGGRESSIVE_VARIANT_NORM | Need to add |
| **Missing:** MOLECULAR_VARIANT_NORM | Need to add |
| **Missing:** GENE_FUSION_NORM | Need to add |

### Root Cause

V2 extractors (`extract_molecular_v2.py`, `extract_rai_v2.py`, `extract_imaging_v2.py`, `extract_operative_v2.py`, `extract_histology_v2.py`) are built but not wired into the extraction pipeline. Script 22 builds episode tables from structured source tables, not from extracted note entities. The V2 extractor outputs would fill the NULL fields above.

---

## 5. Chronology Validation Status

### Domain Coverage

| Domain | Validation Status | Details |
|--------|-------------------|---------|
| **Pathology** | COMPLETE | Uses `path_synoptics.surg_date`; date_status/confidence present |
| **RAI** | COMPLETE | QA: `rai_before_surgery` (error), `rai_implausible_dose` (10–300 mCi); script 24 also checks |
| **Operative** | COMPLETE | Uses `operative_details.surg_date`; joined to pathology on matching surgery_date |
| **Molecular** | PARTIAL | QA: `molecular_chronology` (test after surgery) but NO molecular-before-FNA check |
| **FNA** | PARTIAL | Dates parsed; no validation that molecular result comes after FNA specimen |
| **Imaging** | PARTIAL | Dates present; NO temporal constraint in imaging_pathology_concordance_review_v2 |

### Existing Chronology Rules

| Rule | Script | Severity |
|------|--------|----------|
| RAI before linked surgery | QA 25, script 24 | error |
| RAI implausible dose (10–300 mCi) | QA 25 | error |
| Molecular test after linked surgery | QA 25 | warning |
| Entity vs. note date gap > 365 days | script 17 | warning |
| Imaging-FNA laterality mismatch | QA 25 | warning |

### Missing Chronology Rules (Recommended)

| Rule | Rationale | Priority |
|------|-----------|----------|
| Molecular before FNA | Molecular results cannot precede specimen collection; current weak tier allows negative day_gap | HIGH |
| Imaging before surgery (pre-op) | Pre-op US should precede surgery; no analogous check | MEDIUM |
| FNA before surgery | FNA is pre-op; weak tier `ABS(day_gap) <= 365` allows surgery-before-FNA | MEDIUM |
| Imaging-pathology temporal constraint | `imaging_pathology_concordance_review_v2` joins on research_id only; multi-surgery patients get wrong comparisons | HIGH |
| Operative vs. pathology surgery date mismatch | operative_details.surg_date vs path_synoptics.surg_date discrepancies can break joins | LOW |

### Linkage Risks

| Risk | Linkage Table | Issue |
|------|--------------|-------|
| Molecular before FNA linked as "weak" | `fna_molecular_linkage_v2` | `ABS(day_gap) <= 180` includes negative day_gap; should be `day_gap BETWEEN 0 AND 180` |
| Imaging compared to wrong surgery | `imaging_pathology_concordance_review_v2` | Joins only on research_id; multi-surgery patients get cross-surgery comparisons |
| Preop after surgery in weak tier | `preop_surgery_linkage_v2` | `ABS(day_gap) <= 365` allows surgery before preop; should be `day_gap BETWEEN 0 AND 365` |
| Pathology-RAI: negative interval as weak | `pathology_rai_linkage_v2` | Negative `days_surg_to_rai` with historical/ambiguous → weak; acceptable but should flag |

### Date Rescue Opportunities

| Source | Current | Opportunity |
|--------|---------|-------------|
| `molecular_testing.date` year-only | Script 27 handles; script 22 does NOT | Mirror script 27's year-only handling in script 22 |
| `genetic_testing` DATE_1/2/3_year | Script 27 uses molecular_testing only | Could add genetic_testing year fallback |
| RAI note_entities_medications | entity_date → note_date fallback | No coarse_anchor_date (surgery/FNA) fallback in script 22 |
| Multi-surgery anchor | Uses earliest surg_date | Later surgery might be better anchor for some events |

### Multi-Surgery Patient Gaps

| Area | Issue |
|------|-------|
| Script 27 anchor | Uses earliest surgery date; events tied to later surgeries get wrong anchor |
| Imaging-pathology | Joins on research_id only; can compare imaging from surgery 1 with pathology from surgery 2 |
| event_date_audit_v2 | No episode_id for multi-surgery disambiguation |

---

## 6. Export-Ready Manual Review Tables

### Currently Available Review Queues

| View | Columns | Row Source |
|------|---------|------------|
| `histology_manual_review_queue_v` | queue_row_id, research_id, priority_score, review_domain, unresolved_reason, conflict_summary, source_histology_raw_ps/tp, t_stage_source_path/note, final_histology/t_stage_for_analysis, recommended_reviewer_action, supporting_source_objects, linked_episode_id | histology_analysis_cohort_v WHERE adjudication_needed OR unresolved |
| `molecular_manual_review_queue_v` | queue_row_id, research_id, priority_score, review_domain, unresolved_reason, conflict_summary, specimen_date_raw, platform_normalized, test_name_raw, result_category/summary, recommended_reviewer_action | molecular_episode_v3 WHERE NOT eligible AND NOT placeholder |
| `rai_manual_review_queue_v` | queue_row_id, research_id, priority_score, review_domain, unresolved_reason, conflict_summary, rai_term_normalized, rai_date, linked_surgery_date, days_surgery_to_rai, dose_mci, rai_mention_text_short | rai_episode_v3 WHERE NOT eligible AND NOT negated |
| `timeline_manual_review_queue_v` | queue_row_id, research_id, priority_score, review_domain, unresolved_reason, conflict_summary, detected_value, source_domain | validation_failures_v3 date errors/warnings |
| `unresolved_high_value_cases_v` | Union of high-priority items: histology (≥80), molecular (≥70), RAI (≥90), timeline (≥95) | Four queues filtered by priority |

### Current Unresolved Burdens (from manuscript export manifest)

| Domain | Count |
|--------|-------|
| Histology needing review | 7,629 |
| Molecular low confidence | 4,230 |
| RAI ambiguous | 0 |
| Timeline errors | 1,431 |
| **Total adjudicated decisions** | **0** |

### Export Script: `scripts/28_manual_review_export.py`

See `scripts/28_manual_review_export.py` (created alongside this report) for the export-ready script that produces CSV + Parquet for all five review queues plus the unresolved high-value cases.

---

## 7. Action Items — Prioritized

### MUST DO (before MotherDuck window closes)

| # | Action | File(s) | Effort |
|---|--------|---------|--------|
| 1 | Add 12 views to script 26 MATERIALIZATION_MAP (6 P0 + 6 P1) | `scripts/26_motherduck_materialize_v2.py` | 30 min |
| 2 | Export manual review queues to CSV/Parquet | New `scripts/28_manual_review_export.py` | 20 min |
| 3 | Fix dashboard.py QA tab: qa_issues → qa_issues_v2 | `dashboard.py` ~L1200 | 5 min |
| 4 | Fix adjudication_summary.py mrq_view double-md_ | `app/adjudication_summary.py` L23 | 2 min |

### SHOULD DO (near-term)

| # | Action | File(s) | Effort |
|---|--------|---------|--------|
| 5 | Fix FNA-molecular linkage: enforce FNA before molecular | `scripts/23_*.py` fna_molecular_linkage_v2 | 15 min |
| 6 | Add imaging-pathology temporal constraint | `scripts/24_*.py` imaging_pathology_concordance_review_v2 | 15 min |
| 7 | Fix preop-surgery weak tier: preop before surgery | `scripts/23_*.py` preop_surgery_linkage_v2 | 10 min |
| 8 | Add tbl_exists fallback for molecular_episode_v3, rai_episode_v3 | `app/review_molecular.py`, `app/review_rai.py` | 10 min |
| 9 | Mirror script 27 year-only date handling in script 22 | `scripts/22_*.py` molecular date handling | 10 min |
| 10 | Add missing QA chronology rules (molecular_before_fna, imaging_after_surgery) | `scripts/25_*.py` | 20 min |

### NICE TO HAVE (future sprint)

| # | Action | File(s) | Effort |
|---|--------|---------|--------|
| 11 | Wire V2 extractors into pipeline | `run_extraction.py`, `scripts/22_*.py` | 2–4 hrs |
| 12 | Apply vocab.py normalization maps to extractors | All V2 extractors | 2–3 hrs |
| 13 | Add 8 missing normalization maps to vocab.py | `notes_extraction/vocab.py` | 1–2 hrs |
| 14 | Normalize margin_status, vascular_invasion, ETE in script 22 | `scripts/22_*.py` | 1 hr |
| 15 | Materialize enriched_note_entities_* (batch of 6) | `scripts/26_*.py` | 30 min |
| 16 | Multi-surgery anchor improvement in script 27 | `scripts/27_*.py` | 1 hr |
