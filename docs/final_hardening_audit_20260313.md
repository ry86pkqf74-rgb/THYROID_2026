# Final Hardening Audit — 2026-03-13

## Scope

Targeted audit of post-maturation state after scripts 75 (dataset maturation),
76 (canonical gap closure), 77 (lab canonical layer), and the Streamlit
workflow-first refactor.

## Objects Reviewed

| Object | Type | Purpose |
|--------|------|---------|
| `scripts/75_dataset_maturation.py` | Script | 10-phase maturation pass |
| `scripts/76_canonical_gap_closure.py` | Script | 5-phase canonical gap closure (A-E) |
| `scripts/77_lab_canonical_layer.py` | Script | Canonical lab scaffold |
| `scripts/26_motherduck_materialize_v2.py` | Script | 163-entry materialization map |
| `dashboard.py` | App | 6-section workflow-first layout |
| `app/qa_workbench.py` | Module | Dataset health monitoring |
| `app/manual_review_workbench.py` | Module | Prioritized triage |
| `app/patient_timeline_explorer.py` | Module | Per-patient timeline + rescue KPIs |
| `README.md` | Doc | Repo-facing status |
| `RELEASE_NOTES.md` | Doc | Versioned release history |
| 27 docs in `docs/` | Docs | Audit/design/architecture docs |

---

## 1. Operative V2 Entity Availability

### Current State

Script 71 (`71_operative_nlp_to_motherduck.py`) already pushed 11 NLP boolean
columns to MotherDuck. Before/after:

| Column | Before | After |
|--------|--------|-------|
| rln_monitoring_flag | 0 | 1,702 |
| rln_finding_raw | 0 | 371 |
| drain_flag | 0 | 169 |
| operative_findings_raw | 0 | 588 |
| gross_ete_flag | 0 | 22 |
| strap_muscle_involvement_flag | 0 | 186 |

Script 76 Phase A added 6 further columns via `note_entities_procedures`:
`parathyroid_identified_count`, `parathyroid_resection_flag`,
`frozen_section_flag`, `berry_ligament_flag`, `ebl_ml_nlp`,
`op_enrichment_source`. These may or may not be propagated on MotherDuck
depending on whether script 76 Phase A was run with `--md`.

### Remaining Unwired Fields

The `OperativeDetailExtractor` extracts 13 clinical domains. Of these,
fields NOT wired as individual boolean flags (only captured in
`operative_findings_raw` text):

- `parathyroid_management` (identified/removed/devascularized detail)
- `specimen_detail` (frozen section result, specimen orientation)
- `intraop_complication` individual flags
- `berry_ligament` individual flag (vs Phase A boolean)

**Classification**: Structurally ready, awaiting individual flag extraction.
Not actionable without new regex refinement work.

### Pre-2019 Gap

~6,500 operative episodes have no electronic op notes (institutional data
limitation for surgeries before ~2019). This is a permanent source limitation.

---

## 2. Recurrence Unresolved-Date Distribution

Script 76 Phase E created 4 date tiers on `extracted_recurrence_refined_v1`:

| Tier | Status | Count | Confidence |
|------|--------|-------|------------|
| 1 | `exact_source_date` | 54 | 1.0 |
| 2 | `biochemical_inflection_inferred` | 168 | 0.5 |
| 3 | `unresolved_date` | 1,764 | 0.0 |
| 4 | `not_applicable` | 8,885 | NULL |

The 1,764 `unresolved_date` rows represent patients with structural
recurrence confirmed (from `recurrence_risk_features_mv` recurrence_flag
or NLP clinical events) but no parseable date from any extraction source.

### High-Yield Triage Subset

A bounded subset of manuscript-relevant unresolved recurrence dates can be
created by filtering to:
- Patients in `manuscript_cohort_v1` (analysis-eligible cancer cohort)
- `detection_category IN ('structural_confirmed', 'structural_date_unknown')`
- Patients with multiple evidence sources

This is the most impactful manual review target for recurrence analyses.

**Classification**: Fixable via manual chart review. Automated date recovery
exhausted. Review queue created in this hardening pass.

---

## 3. Imaging-to-FNA Linkage Root Cause

### Definitive Root Cause: Category A (Materialization Missing)

`imaging_fna_linkage_v3` is empty (0 rows) because:

1. `imaging_nodule_long_v2` has 10,866 rows from `serial_imaging_us` but ALL
   size/TIRADS/composition columns are NULL (placeholder schema from unpopulated
   `dominant_nodule_size_on_us` column).
2. `imaging_nodule_master_v1` was **empty (0 rows)** on MotherDuck when
   script 49 ran.
3. The V3 UNION ALL supplements v2 with v1 for patients NOT already in v2.
   Since v2 has 6,123 patients with non-null `exam_date_native`, v1 patients
   overlapping with those 6,123 are excluded.

### Current State

`imaging_nodule_master_v1` now has **19,891 rows** (4,074 patients) from
Phase 12 TIRADS Excel ingestion. These rows have actual measurements
(size, TIRADS, ACR criteria).

### Fix Path

Re-run the `imaging_fna_linkage_v3` SQL with a relaxed UNION that prefers
v1 rows (which have real features) over v2 rows (all-NULL). The dedup guard
should be inverted: include v1 rows for ALL patients, exclude v2 rows where
v1 has coverage (since v2 features are meaningless).

**Classification**: Fixable now. Implemented in this hardening pass.

---

## 4. Molecular-to-Surgery Linkage Semantics

### Confirmed: By Design

Direct molecular-to-surgery linkage does NOT exist as a separate table.
The intended data model is chained linkage:

```
molecular_test → FNA_episode → surgery_episode
```

This is implemented via `preop_surgery_linkage_v3` which UNIONs both FNA
and molecular episodes in its `preop` CTE. Molecular episodes appear with
`preop_type = 'molecular'`.

Current chained linkage coverage:
- `fna_molecular_linkage_v3`: links molecular tests to FNA episodes
- `preop_surgery_linkage_v3`: links FNA/molecular to surgery episodes
- Full chain (molecular → FNA → surgery): available through two-hop join

**Classification**: Not a gap. Documentation improvement needed.

---

## 5. RAI Source-Limitation Audit

### Current State

761 / 1,857 RAI episodes (41.0%) have `dose_mci` populated after script 76
Phase B relaxed backfill. Source breakdown:

| Source | Count | Reliability |
|--------|-------|------------|
| Structured (script 22) | 49 | 0.90 |
| NLP-linked to structured date | 145 | 0.70 |
| NLP-standalone (relaxed match) | 113 | 0.50 |
| Provenance-tagged existing | ~454 | 0.85 |

### Missing-Dose Classification

For the remaining ~1,096 episodes without dose:

| Reason | Estimated Count | Description |
|--------|----------------|-------------|
| `no_source_report_available` | ~900 | Zero nuclear medicine notes in `clinical_notes_long` corpus |
| `linkage_failed` | ~100 | Episode exists but no linkage to dose source |
| `source_present_no_dose_stated` | ~50 | Source note exists but dose not extractable |
| `other_unknown` | ~46 | Ambiguous or multiple conflicting sources |

The dominant category is `no_source_report_available`: the clinical notes
corpus contains **zero nuclear medicine procedure notes**. This is a
fundamental source limitation.

**Classification**: Source-limited. Future improvement requires institutional
nuclear medicine report ingestion. Missingness classification field added
in this hardening pass.

---

## 6. Lab Scaffold Audit

### Current Canonical Lab Contract

`longitudinal_lab_canonical_v1` (45,954 rows, 3,349 patients):

| Analyte | Source | Tier | Patients |
|---------|--------|------|----------|
| thyroglobulin | thyroglobulin_labs | current_structured | 2,569 |
| anti_thyroglobulin | anti_thyroglobulin_labs | current_structured | 2,127 |
| pth | extracted_postop_labs_expanded_v1 | current_nlp_partial | 673 |
| calcium_total | extracted_postop_labs_expanded_v1 | current_nlp_partial | 559 |
| calcium_ionized | extracted_postop_labs_expanded_v1 | current_nlp_partial | ~50 |

9 analytes are future placeholders with 0 measurements:
tsh, free_t4, free_t3, vitamin_d, albumin, phosphorus, magnesium,
calcitonin, cea.

### Contract Gaps Identified

1. **Zero inline validation** in canonical table creation SQL
2. **`lab_validation_rules_v1`** (18 rules) exists in script 68 but is
   never consumed by any script
3. **No `val_*` table** validates canonical lab data
4. **Script 55 lab tests target wrong table** (`longitudinal_lab_clean_v1`
   not `longitudinal_lab_canonical_v1`)
5. **No test files** in `tests/` for lab contract

**Classification**: Fixable now. Contract hardening implemented in this pass.

---

## Summary Classification

| Domain | Status | Action |
|--------|--------|--------|
| Operative V2 NLP | Landed (script 71) | Verify Phase A propagation |
| Operative unwired fields | Structurally ready | Deferred (needs new regex) |
| Recurrence unresolved dates | 1,764 rows | Review queue created |
| Imaging-FNA linkage | Fixable now | Re-run with relaxed UNION |
| Molecular-surgery linkage | By design (chained) | Documentation only |
| RAI dose missingness | Source-limited (no nuc med notes) | Classification field added |
| Lab contract | Scaffold exists, no validation | Contract hardened |
| Dashboard structure | 6-section layout complete | Polish pass applied |
| Documentation | Scripts 76/77 undocumented | Synchronized |
