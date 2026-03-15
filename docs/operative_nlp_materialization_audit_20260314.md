# Operative NLP Materialization Audit — 2026-03-14

**Script**: `scripts/94_pipeline_gap_closure.py` (Workstream A)  
**Date**: 2026-03-14

---

## Objective

End-to-end audit of operative NLP data availability and field-level coverage across:
- `note_entities_procedures` (raw NLP extractions)
- `operative_episode_detail_v2` (structured V2 canonical episode table)
- `patient_analysis_resolved_v1` / `manuscript_cohort_v1` (patient-level aggregates)

---

## Source Classification: Operative V2 Fields

### RELIABLE (structured, well-populated)

| Field | operative_episode_detail_v2 (true) | Source |
|---|---|---|
| `rln_monitoring_flag` | **1,702** | V2 OperativeDetailExtractor |
| `rln_finding_raw` | 371 (non-null) | V2 OperativeDetailExtractor |
| `strap_muscle_involvement_flag` | **186** | V2 OperativeDetailExtractor |
| `reoperative_field_flag` | **46** | V2 OperativeDetailExtractor |
| `drain_flag` | **169** | V2 OperativeDetailExtractor |
| `gross_ete_flag` | **22** | V2 OperativeDetailExtractor |
| `operative_findings_raw` | **588** (non-null) | V2 OperativeDetailExtractor |
| `parathyroid_autograft_flag` | **40** | V2 OperativeDetailExtractor (+ NLP corroboration) |

### NOT PARSED (hardcoded FALSE, not confirmed-negative)

Per prior audit (script 80, `val_operative_field_semantics_v1`), these fields are 
unreliable because V2 extractor outputs were never materialized via a proper
`COALESCE(extracted_value, FALSE)` update chain — the `FALSE` value means
**UNKNOWN/NOT_PARSED**, not confirmed-negative:

| Field | Count | Semantic |
|---|---|---|
| `local_invasion_flag` | 25 TRUE | Partially parsed |
| `tracheal_involvement_flag` | 9 TRUE | Partially parsed |
| `esophageal_involvement_flag` | 0 TRUE | SOURCE_ABSENT (0 entities in vocabulary) |

### SOURCE_ABSENT (no NLP entities in vocabulary)

| Field | Root Cause |
|---|---|
| `berry_ligament_flag` | Entity type not in `note_entities_procedures` vocabulary |
| `frozen_section_flag` | Entity type not in NLP vocabulary |
| `ebl_ml_nlp` | Entity type not in NLP vocabulary |
| `parathyroid_identified_count` | Domain entity type not materialized |
| `parathyroid_autograft_count` | Not materialized |

---

## NLP Supplement Analysis: Parathyroid Autotransplant

### `note_entities_procedures` counts (parathyroid_autotransplant, present)
- Total patient mentions: **48**
- Already flagged TRUE in `operative_episode_detail_v2`: **16**
- Have an op row but not flagged: **0** (column is native BOOLEAN, not text — all 48 were checked)
- Have NO `operative_episode_detail_v2` row: **32**

### Root Cause for 32 Missing Rows
These 32 patients documented autotransplant in clinical notes but have no corresponding
row in `operative_episode_detail_v2`. Possible explanations:
1. Their procedures were performed before the operative_details structured data capture period
2. Their cases exist in `operative_details` raw table but were not included in the V2
   canonical episode join (e.g., missing `surg_date` match)
3. Procedures at an outpatient or off-site facility not captured in the main dataset

**Recommendation**: These 32 patients could be targeted for manual chart review if
parathyroid autotransplant is a study endpoint.

---

## Patient-Level Aggregate Coverage (patient_analysis_resolved_v1)

Total patients: **10,871**

| op_* column | Count (any=TRUE) | % coverage |
|---|---|---|
| `op_rln_monitoring_any` | 1,701 | 15.6% |
| `op_drain_placed_any` | 169 | 1.6% |
| `op_strap_muscle_any` | 186 | 1.7% |
| `op_reoperative_any` | 46 | 0.4% |
| `op_parathyroid_autograft_any` | 40 | 0.4% |
| `op_local_invasion_any` | 25 | 0.2% |
| `op_tracheal_inv_any` | 9 | 0.1% |
| `op_esophageal_inv_any` | 0 | 0.0% (SOURCE_ABSENT) |
| `op_intraop_gross_ete_any` | 22 | 0.2% |
| `op_n_surgeries_with_findings > 0` | 587 | 5.4% |

**Coverage interpretation**: Low rates reflect structural source limitations (nuclear
medicine text absent, NLP entities not in vocabulary) and should be interpreted as
lower bounds, not prevalences.

---

## Validation Table

`val_operative_coverage_v2` created (MotherDuck) with 2 rows:
1. `operative_episode_detail_v2`: episode-level field counts  
2. `note_entities_procedures`: NLP supplement counts

---

## Recommendations

1. **Do not use `esophageal_involvement_flag`** in any analysis — it is confirmed 0 and
   SOURCE_ABSENT; mark as excluded in data dictionary
2. **Annotate `not_parsed_as_false`** boolean fields in the SAP and manuscript methods
   for `berry_ligament`, `frozen_section`, `ebl` fields
3. **Targeted NLP re-run** for parathyroid autotransplant: run
   `notes_extraction/extract_operative_v2.py` against the 32 identified patients and
   check if they appear in `operative_details` under different surgery dates
4. **RLN monitoring** (1,702, 15.6%) is reliable and usable as a process quality indicator
