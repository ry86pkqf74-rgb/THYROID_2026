# Operative Semantics Hardening Audit — 20260313

## Summary

This document audits the semantic truth of boolean operative fields in
`operative_episode_detail_v2`. The core problem: 10 boolean fields are
hardcoded to FALSE in script 22, but this represents 'NOT_PARSED' rather
than 'confirmed negative'. The V2 OperativeDetailExtractor exists and CAN
extract these fields, but its output was never materialized to the
`note_entities_procedures` table that script 76 reads from.

## Parse Status Taxonomy

| Status | Meaning | Boolean Interpretation |
|--------|---------|----------------------|
| RELIABLE | Derived from structured data (path_synoptics) | TRUE/FALSE are accurate |
| PARTIAL | Structured source covers subset of patients | Non-NULL values are accurate |
| NOT_PARSED | V2 extractor exists but output not materialized | FALSE = UNKNOWN (not confirmed negative) |
| SOURCE_ABSENT | NLP entity type not in vocabulary | NULL = no extraction attempted |

## Per-Field Audit

| Field | TRUE | FALSE | NULL | Parse Status | Source |
|-------|------|-------|------|--------------|--------|
| drain_flag | 169 | 9,202 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| esophageal_involvement_flag | 0 | 9,371 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| gross_ete_flag | 22 | 9,349 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| local_invasion_flag | 25 | 9,346 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| parathyroid_autograft_flag | 40 | 9,331 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| parathyroid_resection_flag | 0 | 9,371 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| reoperative_field_flag | 46 | 9,325 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| rln_monitoring_flag | 1,702 | 7,669 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| strap_muscle_involvement_flag | 186 | 9,185 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| tracheal_involvement_flag | 9 | 9,362 | 0 | NOT_PARSED | V2_EXTRACTOR_NOT_MATERIALIZED |
| ebl_ml (structured) | 124 | 0 | 9,247 | PARTIAL | STRUCTURED_DATA_PARTIAL |
| central_neck_dissection_flag | 2,497 | 6,874 | 0 | RELIABLE | STRUCTURED_DATA_SOURCE |
| lateral_neck_dissection_flag | 241 | 9,130 | 0 | RELIABLE | STRUCTURED_DATA_SOURCE |
| berry_ligament_flag | 0 | 0 | 9,371 | SOURCE_ABSENT | NLP_ENTITY_TYPE_NOT_IN_VOCABULARY |
| ebl_ml_nlp | 0 | 0 | 9,371 | SOURCE_ABSENT | NLP_ENTITY_TYPE_NOT_IN_VOCABULARY |
| frozen_section_flag | 0 | 0 | 9,371 | SOURCE_ABSENT | NLP_ENTITY_TYPE_NOT_IN_VOCABULARY |
| parathyroid_identified_count | 0 | 0 | 9,371 | SOURCE_ABSENT | NLP_ENTITY_TYPE_NOT_IN_VOCABULARY |

## Architecture Gap: V2 Extractor Materialization

The `OperativeDetailExtractor` in `notes_extraction/extract_operative_v2.py`
has 13 domain pattern banks that CAN extract berry_ligament, frozen_section,
EBL, parathyroid management, and more. However:

1. Script 22 creates operative_episode_detail_v2 with hardcoded FALSE
2. Script 22 runs V2 extractors inline but via COALESCE(new, old)
3. Since old = FALSE (non-NULL), COALESCE never overwrites
4. Script 76 adds ALTER TABLE columns (NULL) and tries to UPDATE from
   note_entities_procedures, but the V2 entity types are not in that table

**Remediation**: Run V2 extractors to a staging table, then UPDATE
operative_episode_detail_v2 using IS NULL OR original_value = FALSE guard.
This is a future pipeline improvement, not a current data quality issue.

## Recommendation for Manuscript Use

- **central_neck_dissection_flag**: SAFE to use (structured source)
- **lateral_neck_dissection_flag**: SAFE to use (structured + Phase 10 NLP)
- **ebl_ml**: SAFE where non-NULL (structured operative_details.ebl)
- **All other boolean fields**: Treat FALSE as UNKNOWN in analyses.
  Do NOT report 'X% had RLN monitoring' based on rln_monitoring_flag.

## Validation Tables Created

- `val_operative_field_semantics_v1` — per-field parse status audit

Generated: 2026-03-13T14:11:41.741186