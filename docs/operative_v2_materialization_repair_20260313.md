# Operative V2 Materialization Repair Report

**Date:** 2026-03-13

---

## Summary

Script 71 (`71_operative_nlp_to_motherduck.py --md`) was executed to close the operative
NLP enrichment gap. The V2 OperativeDetailExtractor successfully extracted 13,186 operative
entities from 10,499 clinical notes in 112.8 seconds. However, the UPDATE produced zero
delta due to COALESCE guards.

## Root Cause Analysis

### Why Zero Delta

1. Script 22 creates `operative_episode_detail_v2` via SQL and populates boolean flags with
   explicit values (TRUE/FALSE, not NULL) from structured sources during the initial build.
2. Script 71 uses `COALESCE(new_value, old_value)` to avoid overwriting existing non-NULL
   values.
3. Since existing values are FALSE (non-NULL), COALESCE correctly preserves them even when
   the NLP extractor finds positive mentions.

### Field-by-Field Status

| Field | Before | After | Delta | Reason |
|-------|--------|-------|-------|--------|
| rln_monitoring_flag | 1,702 (18.2%) | 1,702 | +0 | Already populated by SQL |
| rln_finding_raw | 371 (4.0%) | 371 | +0 | Already populated |
| drain_flag | 169 (1.8%) | 169 | +0 | Already populated |
| operative_findings_raw | 588 (6.3%) | 588 | +0 | Already populated |
| gross_ete_flag | 22 (0.2%) | 22 | +0 | Already populated |
| strap_muscle_involvement_flag | 186 (2.0%) | 186 | +0 | Already populated |
| parathyroid_autograft_flag | 40 (0.4%) | 40 | +0 | Already populated |
| reoperative_field_flag | 46 (0.5%) | 46 | +0 | Already populated |
| berry_ligament_flag | 0 (0%) | 0 | +0 | Phase 76A column, not in script 71 UPDATE scope |
| frozen_section_flag | 0 (0%) | 0 | +0 | Phase 76A column, not in script 71 UPDATE scope |
| parathyroid_identified_count | 0 (0%) | 0 | +0 | Phase 76A column, not in script 71 UPDATE scope |
| ebl_ml_nlp | 0 (0%) | 0 | +0 | Phase 76A column, not in script 71 UPDATE scope |
| op_enrichment_source | 0 (0%) | 0 | +0 | Phase 76A column, not in script 71 UPDATE scope |

### Phase 76A Columns

These 5 columns were added by `ALTER TABLE` in script 76 Phase A. They are genuinely NULL
(not FALSE). However:
- Script 71 does not target these columns in its UPDATE statement
- Script 76 Phase A attempted to populate them from `note_entities_procedures` but those
  entity types (berry_ligament, frozen_section, parathyroid_identified_count, ebl) do not
  exist in the NLP entity vocabulary

## Conclusion

**This is a pipeline architecture gap, not a data gap.** The V2 OperativeDetailExtractor
CAN extract these fields (13,186 entities were found), but the materialization pathway
does not connect the extracted entities to the Phase 76A columns.

### To Fix (Future Work)

1. Restructure script 22 to use NULL defaults instead of FALSE for NLP-derived boolean fields
2. Extend script 71's UPDATE scope to include Phase 76A columns
3. OR: Create a new script that extracts berry_ligament/frozen_section/EBL specifically
   and UPDATEs only the NULL columns

### Acceptance Criteria

- [x] Extraction pipeline verified (13,186 entities extracted)
- [x] Root cause of zero-delta documented
- [x] Source limitation precisely documented
- [x] No ambiguity about what remains unfixed
