# Cursor Prompt — mig_187 canonical_us_exam_master rebuild (CF-mig171b-EXAM-MASTER-REBUILD)

**Date:** 2026-04-30
**Lane:** mig_187 / canonical_us_exam_master_rebuild
**Batch (proposed):** `mig_187_canonical_us_exam_master_rebuild_20260430`
**Predecessor:** mig_171b CLOSED at `9301b58` — built `canonical_us_lymph_node_events_v2` with 159 fallback `us_exam_id` values pending exam-master rebuild
**Posture:** **READ-ONLY scoping + skeleton SQL.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only)

---

## Mission

The mig_171b build emitted 159 `fallback_ln_only_exam_id` values of the form `md5('US_EXAM_V2|' || rid || '|' || exam_date)` because the corresponding (rid, exam_date) wasn't present in `canonical_us_exam_master_VIEW_v2`. mig_187 rebuilds the exam-master to incorporate these LN-only dates, resolving the fallback IDs to canonical ones.

**Live MD probe (Cowork 2026-04-30):**
- 159 events in `canonical_us_lymph_node_events_v2` with `exam_id_source='fallback_ln_only_exam_id'`
- All flagged by validation gate G9 (WARN expected per design)
- `canonical_us_exam_master_VIEW_v2` has 11,759 rows (across all US imaging modalities)

---

## Required scope

### §1 Profile the 159 fallback exam IDs

```sql
SELECT research_id, exam_date, COUNT(*) AS n_ln_events_for_this_fallback,
       MIN(side) AS sample_side, MIN(neck_level) AS sample_level,
       MIN(source_table) AS source_table_mix
FROM main.canonical_us_lymph_node_events_v2
WHERE exam_id_source = 'fallback_ln_only_exam_id'
GROUP BY 1, 2 ORDER BY 1, 2;
```

How many distinct (rid, exam_date) does this represent? How many events per fallback ID?

### §2 Inspect why these (rid, exam_date) are missing from exam_master

For each of the ~159 (rid, exam_date) pairs:
- Is there ANY exam in `canonical_us_exam_master_VIEW_v2` for that rid (just on a different date)?
- Is the exam_date format off (e.g., year drift)?
- Is the exam from a non-thyroid US that wasn't ingested?

### §3 Determine `canonical_us_exam_master_VIEW_v2` source build

Read the VIEW definition (`information_schema.views` or `pg_get_viewdef` equivalent). What's the underlying build script? Likely `scripts/<N>_canonical_us_exam_master_*.py` or similar.

### §4 Propose rebuild approach

Three options:

| Rule | Approach |
|---|---|
| R-A | Insert the 159 missing (rid, exam_date) into the underlying exam_master source table; rebuild VIEW; rebuild mig_171b events to reuse new exam_ids | Most disruptive but cleanest |
| R-B | Add a "supplemental US LN exams" patch table; UNION into the VIEW; rebuild mig_171b events | Less destructive |
| R-C | Accept the fallback IDs permanently; drop G9 WARN; document in CF as design choice | Cheapest; manuscript-acceptable if the 159 events are well-documented |

Recommend per cohort impact analysis. Logan ratifies.

### §5 Author placeholder skeleton apply SQL

`qc_framework_v1/migrations/187_canonical_us_exam_master_rebuild_TBD_20260430.sql`:
- §A pre-snapshot exam_master VIEW + canonical_us_lymph_node_events_v2
- §B per-rule rebuild variants (clearly marked `-- LOGAN MUST RATIFY RULE BEFORE EXECUTION`)
- §C re-rebuild canonical_us_lymph_node_events_v2 to reuse new IDs (rerun mig_171b §B)
- §D re-validate via mig_171b val table; G9 should flip to PASS

### §6 Audit/report

`qc_framework_v1/reports/mig_187_canonical_us_exam_master_rebuild_scoping_20260430.md`:
- §1 159 fallback ID inventory + (rid, exam_date) profile
- §2 root-cause analysis of why these are missing from exam_master
- §3 R-A/R-B/R-C comparison
- §4 manuscript implications (does the fallback ID treatment differ from a real exam_master ID for downstream linkage?)
- §5 sample 10 fallback IDs for Logan spot-check

---

## Governance reminders

- Read-only investigation only. Author = `Logan Glosser <logan.glosser@gmail.com>`.

---

## Deliverables

1. `qc_framework_v1/migrations/187_canonical_us_exam_master_rebuild_TBD_20260430.sql`
2. `qc_framework_v1/reports/mig_187_canonical_us_exam_master_rebuild_scoping_20260430.md`

Commit message: `qc: mig_187 canonical_us_exam_master rebuild scoping (CF-mig171b-EXAM-MASTER-REBUILD; 159 fallback IDs)`

---

End of prompt.
