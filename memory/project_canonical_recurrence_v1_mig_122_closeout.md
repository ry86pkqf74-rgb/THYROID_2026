# canonical_recurrence_v1 — mig_122 close-out (Protocol v2)

**Date:** 2026-04-29  
**Migration:** `qc_framework_v1/migrations/122_canonical_recurrence_v1_signoff.sql`  
**Lane:** Cursor lane 14 (cohort-wide derivation family)

## Summary

Verified `main.canonical_recurrence_v1` (12 cols: 11 verified + `research_id` na) against MotherDuck publication catalog live probes.

### Live cohort shape

- **10,871 rows** = **10,871** distinct `research_id` = `canonical_patient_master` — parity PASS.
- **Degenerate shell:** `recurrence_confirmed = FALSE` for all rows; `recurrence_type = 'none'`; `recurrence_definition = 'no_recurrence_evidence'`; `recurrence_evidence_source` NULL everywhere (INTEGER column dormant until tier-encoding lands).
- **Biochemical cols:** INTEGER; both NULL all rows (consistent with uniform shell).

### Acceptance gates run on live table

| Gate | Result |
|------|--------|
| `FALSE ⇒ recurrence_date NULL` | PASS (10,871 / 10,871) |
| `FALSE ⇒ time_to_recurrence_days NULL` | PASS |
| `biochemical_tg_nadir > biochemical_tg_at_recurrence` where both set | 0 rows |
| Confirmed `< 10,871` | YES — **0 confirmed** |

### Builder SSOT

Repo logic remains `scripts/203_canonical_recurrence.py`. Publication DB lacks legacy `operative_episode_detail_v2` / `gold_master_patient_facts_v1`, so full tier replay cannot match historical parquet uploads without an explicit rebuild pass.

### first_surgery_date

- Stored as **TIMESTAMP** → CF **CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE** (calendar-only joins — CAST/`DATE_TRUNC('day', …)` vs survival/recurrence DATE surfaces).
- Proxy comparison vs `MIN(canonical_operative_events_v1.surgery_date_native)` **CAST AS DATE**: **2,329** day mismatches; **2,140** recurrence rows NULL fs while operative MIN exists — documented as **CF-mig122-RECURRENCE-FIRST-SURGERY-OPERATIVE-PROXY-DRIFT**, pending Script **203** spine harmonization (**CF-mig122-RECURRENCE-203-REBUILD-PENDING**).

### Lane 13 follow-through

Migration **122c** appends notes on `canonical_ete_event_resolved_v1` recurrence columns clarifying **derivation_re_derivation_post_recurrence_verified** labeling vs legacy CF **CF-mig121-ETE-EVENT-RESOLVED-RECURRENCE-PENDING** (faithfulness vs `canonical_recurrence_resolved_v1` unchanged).

## References

- Template cohort-wide rollup: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql`
- `feedback_clinical_dates_calendar_only.md`, `feedback_motherduck_direct_check.md`
