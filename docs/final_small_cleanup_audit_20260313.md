# Final Small Cleanup Audit — 2026-03-13

## Audit Summary

This audit was performed prior to the final bounded cleanup pass. No broad
rewrites, no new extraction campaigns, no destabilization of manuscript-ready
outputs.

---

## Q1: Operative V2 Field Status

### Columns at 0% fill in `operative_episode_detail_v2` (9,371 rows):

| Column | Fill | Category | Source |
|--------|------|----------|--------|
| `berry_ligament_flag` | 0% | C — raw text only | V2 extractor not materialized |
| `ebl_ml_nlp` | 0% | C — raw text only | V2 extractor not materialized |
| `frozen_section_flag` | 0% | C — raw text only | V2 extractor not materialized |
| `op_confidence` | 0% | C — metadata | Depends on extractor run |
| `op_enrichment_source` | 0% | C — metadata | Depends on extractor run |
| `parathyroid_identified_count` | 0% | C — raw text only | V2 extractor not materialized |
| `parathyroid_autograft_count` | 0% | C — raw text only | V2 extractor not materialized |
| `parathyroid_autograft_site` | 0% | C — raw text only | V2 extractor not materialized |

### Already populated (high-value fields from structured sources):

| Column | Fill | Source |
|--------|------|--------|
| `central_neck_dissection_flag` | 100% | path_synoptics (script 75) |
| `lateral_neck_dissection_flag` | 100% | path_synoptics (script 75) |
| `drain_flag` | 100% | structured |
| `gross_ete_flag` | 100% | structured |
| `rln_monitoring_flag` | 100% | structured |
| `procedure_normalized` | 100% | structured |
| `resolved_surgery_date` | 99.9% | structured |
| `linked_pathology_episode_id` | 93.2% | linkage v3 |

### Verdict

All 8 unfilled columns are **Category C** — the `OperativeDetailExtractor`
(`notes_extraction/extract_operative_v2.py`) exists but has never been run
against MotherDuck. Running it would constitute a new extraction campaign,
which is out of scope for this cleanup pass. The structural columns were
added by script 76 Phase A as stubs awaiting future extractor output.

**Action:** Document status. No safe landing possible without running the
extractor pipeline.

---

## Q2: Lab Duplicate Profile

### `longitudinal_lab_canonical_v1`: 45,954 total rows, 3,349 patients

| Metric | Value |
|--------|-------|
| Total rows | 45,954 |
| Exact duplicate groups | 5,976 |
| Total excess rows | 5,993 |
| Affected analyte | thyroid_tumor_markers (100% of dupes) |

### Duplicate group sizes

| Group size | Count |
|------------|-------|
| 2 | 5,966 |
| 3 | 3 |
| 4 | 7 |

### Root cause

Duplicates share identical (research_id, lab_date, lab_name_standardized,
analyte_group, value_numeric, unit_standardized, source_table, source_script)
but **differ only in `is_censored` flag**. Script 77 ingested threshold values
(e.g., "<0.9") as both a censored row (is_censored=TRUE) and an uncensored
row (is_censored=FALSE) for the same numeric parse.

### Sample

| research_id | date | value | censored | interpretation |
|-------------|------|-------|----------|----------------|
| 5120 | 2016-08-02 | 0.9 | TRUE | ≤0.9 (threshold) |
| 5120 | 2016-08-02 | 0.9 | FALSE | =0.9 (exact) |
| 5978 | 2017-06-19 | 0.5 | FALSE | duplicate |
| 5978 | 2017-06-19 | 0.5 | FALSE | duplicate |
| 6077 | 2017-07-25 | 0.1 | TRUE | duplicate |
| 6077 | 2017-07-25 | 0.1 | TRUE | duplicate |

### Safe dedup rule

Within each (research_id, lab_date, lab_name_standardized, analyte_group,
value_numeric, unit_standardized, source_table) group:

1. Keep exactly **one** row per group
2. Prefer `is_censored = TRUE` when the group contains both censored and
   uncensored rows (conservative: the censored interpretation preserves the
   "<X" threshold semantics)
3. Break remaining ties by `ingestion_wave DESC` (prefer latest wave)
4. Final tiebreak: arbitrary (ROW_NUMBER)

This removes 5,993 excess rows without losing any patient or measurement.

---

## Q3: README / Dashboard / Release Notes Stale Statements

### README.md

| Line(s) | Issue | Fix |
|---------|-------|-----|
| 65-73 | "Remaining Source-Limited Gaps" duplicated | Consolidate |
| 90-92 | "If the app asks you to sign in..." stale guidance | Move to deployment section, reduce prominence |
| 302-313 | "New Dashboard Features (enabled during MotherDuck trial)" | Remove — these are standard features now |
| 302 | "Five new tabs added by scripts/12..." | Replace with current 6-section structure |
| 364-368 | "V3 Dashboard Materialization" references old deploy order | Update to current pipeline chain |
| 438 | "(Current)" tag on v2026.03.10-v3-dashboard in RELEASE_NOTES | Move to v2026.03.13-final-hardening |

### dashboard.py

| Line(s) | Issue | Fix |
|---------|-------|-----|
| 1-17 | Docstring references "v3" and old tab names | Update to reflect current architecture |
| 116 | `_APP_VERSION = "v3.1.0-2026.03.10"` | Update to v3.2.0-2026.03.13 |

### RELEASE_NOTES.md

| Line(s) | Issue | Fix |
|---------|-------|-----|
| 438 | "(Current)" on wrong version | Move to latest |

### App modules

`qa_workbench.py` and `manual_review_workbench.py` are current and accurate.
No changes needed except minor caveat text additions.

---

## MotherDuck Object Status

| Object | Status | Rows |
|--------|--------|------|
| `recurrence_manual_review_queue_v1` | Deployed | 1,764 |
| `imaging_fna_linkage_v3` | Deployed, fixed | 9,024 |
| `vw_rai_dose_missingness_summary` | Deployed | 2 categories |
| `val_lab_canonical_v1` | Deployed, WARN status | 5 analytes |
| `val_lab_completeness_v1` | Deployed | 14 analytes |
| `longitudinal_lab_canonical_v1` | Has 5,993 excess rows | 45,954 |
