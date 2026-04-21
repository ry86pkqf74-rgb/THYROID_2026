# Schema Reorg Final Audit (Script 340)

Generated: 2026-04-21T05:38:55.639739Z
Git SHA: d0298eecaaca

## Summary

- main: 120 objects (118 tables, 2 views)
- tier2: 13 tables
- verify: 2 tables
- Reference orphans logged: 0
- Invariants: PASS

## Detailed audit


## 1. Schema inventory (publication DB)
  main: 118 tables, 2 views (120 objects)
  manuscript_workspace: 42 tables, 68 views (110 objects)
  tier2: 13 tables, 0 views (13 objects)
  verify: 2 tables, 0 views (2 objects)
  views_readable: 0 tables, 46 views (46 objects)
  Leftover Prompt-2 outputs in main: 0

## 2. Reference sweep
  Found 0 view references to dropped main tables

## 3. schema_reorg_move_log_v1 audit
  Total rows: 48
    merge_join: 12
    merge_melt: 12
    merge_union: 12
    move: 12

## 4. archive_move_log_v1 — schema-reorg entries
  Total archive entries from 337/338/339: 48
    337_build_verify_concordance_master: 12
    338_build_verify_long: 12
    339_build_tier2_master_and_move_events: 24

## 5. New schema content checks
  tier2.patient_tier2_master_v1: rows=10871, distinct_rid=10871, ncols=213
  verify.concordance_master_v1: rows=31, distinct_domains=12
  verify.verify_long_v1: rows=160023, distinct_domains=12

## 6. tier2 event tables
  12 event tables in tier2:
    airway_invasion_event_v1: 11601 rows
    dynamic_risk_response_event_v1: 53 rows
    frozen_section_event_v1: 8640 rows
    functional_outcomes_event_v1: 3322 rows
    parathyroid_detail_event_v1: 10130 rows
    past_medical_hx_event_v1: 865 rows
    past_surgical_hx_event_v1: 3919 rows
    patient_decision_adherence_event_v1: 641 rows
    physical_exam_event_v1: 2025 rows
    presenting_symptoms_event_v1: 280 rows
    rad_treatment_event_v1: 580 rows
    vascular_invasion_event_v1: 22800 rows

## 7. __readme refresh
  __readme refreshed (sha=d0298eecaaca, body=2485 chars)

## Quick-reference query examples

```sql
-- Manuscript concordance summary for pathology synoptics:
SELECT * FROM verify.concordance_master_v1
 WHERE domain='pathology_synoptics';

-- All discordant LN field comparisons:
SELECT * FROM verify.verify_long_v1
 WHERE domain='ln' AND concordance_status='disagree';

-- All Tier 2 flags for one patient:
SELECT * FROM tier2.patient_tier2_master_v1
 WHERE research_id='RID00001';

-- Per-event frozen section detail for one patient:
SELECT * FROM tier2.frozen_section_event_v1
 WHERE research_id='RID00001';
```
