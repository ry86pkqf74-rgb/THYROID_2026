# Reviewer Defense: How Were Operative Details Sourced?

## Primary Source

`operative_episode_detail_v2` provides **9,371 surgery episodes** from structured institutional operative records (script 22 canonical build). Supplemented by `path_synoptics` (11,688 records) for broader coverage; both sources are unioned for maximum patient capture when needed.

## Reliably Extracted Variables

| Variable | Source | Coverage | Values |
|----------|--------|----------|--------|
| Procedure type | `procedure_normalized` | 9,371 (100%) | total_thyroidectomy (4,561), hemithyroidectomy (3,810), unknown (644), other (356) |
| Surgery date | `resolved_surgery_date` | 9,371 (100%) | VARCHAR; use TRY_CAST for date operations |
| Central neck dissection | Structured field | ~3,240 identified | Composite flag from 4 criteria |
| Lateral neck dissection | Structured + NLP | 119 patients | 41 structured levels + 78 op note NLP |

Central LND composite flag definition: `central_compartment_dissection IS NOT NULL` OR `tumor_1_level_examined LIKE '%6%'` OR `other_ln_dissection` contains 'central'/'level 6' OR `tumor_1_ln_location` contains perithyroidal/pretracheal/paratracheal/delphian/prelaryngeal.

## Lateral Neck Dissection Detail

Coverage increased **4.76x** (25 to 119 patients) via NLP enrichment:

| Detection Method | N | Examples |
|-----------------|---|---------|
| Structured levels | 41 | Level II-V from pathology synoptic |
| NLP: jugular | 23 | "jugular chain dissection" |
| NLP: level II-V | 19 | "levels II through V dissected" |
| NLP: lateral_neck_dissection | 14 | "lateral neck dissection performed" |
| NLP: selective_neck | 14 | "selective neck dissection" |
| NLP: radical | 4 | "radical neck dissection" |
| NLP: modified_radical | 3 | "modified radical neck dissection" |
| NLP: lateral_compartment | 1 | "lateral compartment dissection" |

## Operative Boolean Fields — Interpretation Caveat

Ten operative boolean fields have **FALSE as a hardcoded default**, not as a confirmed-negative finding:

| Field | Status | Explanation |
|-------|--------|-------------|
| `rln_monitoring_flag` | NOT_PARSED | FALSE = unknown, not "no monitoring" |
| `parathyroid_autograft_flag` | NOT_PARSED | FALSE = unknown, not "no autograft" |
| `gross_ete_flag` | NOT_PARSED | FALSE = unknown, not "no ETE" |
| `drain_flag` | NOT_PARSED | FALSE = unknown, not "no drain" |
| `reoperative_field_flag` | NOT_PARSED | FALSE = unknown, not "primary case" |
| + 5 others | NOT_PARSED | Same pattern |

The V2 NLP OperativeDetailExtractor was run (13,186 entities extracted), but `COALESCE(new, old)` guards in the SQL pipeline prevent overwriting defaults when `old=FALSE`. These fields are **not used as manuscript-critical predictors**.

## Source-Absent Fields

| Field | Status | Reason |
|-------|--------|--------|
| Berry ligament dissection | SOURCE_ABSENT | NLP entity type not in extraction vocabulary |
| Frozen section | SOURCE_ABSENT | Not captured in note extraction |
| Parathyroid count | SOURCE_ABSENT | Not captured in note extraction |
| EBL (NLP) | SOURCE_ABSENT | Not captured in note extraction |
| EBL (structured) | 1.3% coverage | `operative_details.ebl_ml` — not reliable |

## Key References

- **Canonical table**: `operative_episode_detail_v2` (MotherDuck, 9,371 rows)
- **Field semantics audit**: `val_operative_field_semantics_v1` (MotherDuck, 17 rows)
- **Lateral neck table**: `extracted_lateral_neck_v1` (MotherDuck, 119 rows)
- **V2 extractor**: `notes_extraction/extract_operative_v2.py`
- **Operative semantics doc**: `docs/operative_semantics_hardening_20260313.md`
- **Script 22**: `scripts/22_canonical_episode_tables.py`
