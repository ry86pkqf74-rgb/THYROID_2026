# Cowork-Direct Pre-Flight Findings — v14 Round

**Date:** 2026-04-30
**Round:** v14 pre-flight (HEAD `5d6aa85`)
**Author:** Cowork (post-handoff, pre-agent-batch dispatch)
**Companion:** `cursor_prompts/PARALLEL_AGENT_BATCH_20260430_v14.md`

Two read-only Cowork-direct probes ahead of the v14 agent dispatch. Both findings inform open carry-forwards + Lane LN open-questions.

---

## §1 — CF-mig220-QUEUE-CURRENT-V2-DRIFT: **CLOSED at current MD state**

**v13 carry-forward language** (from `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-30_v13.md` §"Carry-forwards opened"):
> 6 high-pri queue rows didn't map to current `canonical_us_nodule_v2`. Investigate post-mig_177c_apply or treat as valid orphans. Non-blocking.

**Cowork-direct probe (current MD state):**

```sql
WITH queue_keys AS (
  SELECT DISTINCT research_id, us_exam_id, nodule_index_within_exam
  FROM manuscript_workspace.us_nodule_conflict_queue_v1
  WHERE review_priority='high'
)
SELECT
  (SELECT COUNT(*) FROM queue_keys)                                      AS distinct_high_pri_keys,
  (SELECT COUNT(*) FROM queue_keys q
   WHERE EXISTS(SELECT 1 FROM main.canonical_us_nodule_v2 v2
                WHERE v2.research_id=q.research_id
                  AND v2.us_exam_id=q.us_exam_id
                  AND v2.nodule_index_within_exam=q.nodule_index_within_exam)) AS keys_in_v2;
```

**Result:** `2506 / 2506` (100% mapped). Zero unmapped tuples.

**Diagnosis:** Between the v13 close-out write and current MD state, `mig_222_multi_nodule_under_explosion_triage_20260430` (Cline GPT-5.5, pushed `4f4f979`) absorbed the 6 orphan tuples into `canonical_us_nodule_v2`. The CF was self-closing under multi-nodule absorption.

**Recommendation for Prompt 3 (Copilot GPT-5.5 CF reconciliation):**
- Mark CF-mig220 as `closed` in the reconciliation report.
- Confirm via re-running the probe above.
- Don't open a remediation mig.

**Recommendation for Prompt 4 (Cline Sonnet 4.6 ISSUE_REGISTRY):**
- Add CF-mig220 with `status=closed`, `closed_in_mig=mig_222`, `discovered_in_mig=mig_220`.

---

## §2 — Lane LN Open-Question 3 evidence: 6 dedup-impossible rows are all PTC

**Question** (from `qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md` §7 OQ3):

> When `ln_examined=0.0` (DOUBLE) but `nodal_disease_total_count > 0` (INTEGER), which source wins?
>   - (a) prefer non-zero
>   - (b) prefer typed-stronger (`ln_examined`) — i.e., trust the 0
>   - (c) MAX() across the two
>   - (d) keep both as separate cols + flag conflict

**Cowork-direct probe (all 6 rows enumerated):**

| research_id | path_surgery_id | tumor_ordinal | primary_histology | ln_examined | nodal_disease_total_count | nodal_disease_positive_count |
|---:|:---|:---|:---|---:|---:|---:|
| 744  | 1 | 1 | PTC | 0.0 | 10 | 7 |
| 4426 | 1 | 1 | PTC | 0.0 | 1  | 1 |
| 4560 | 1 | 1 | PTC | 0.0 | 5  | 5 |
| 5197 | 1 | 1 | PTC | 0.0 | 55 | 6 |
| 5917 | 1 | 1 | PTC | 0.0 | 6  | 5 |
| 8482 | 1 | 1 | PTC | 0.0 | 1  | 1 |

**Pattern:**
- 100% PTC primary histology
- 100% `path_surgery_id=1, tumor_ordinal=1` (single-tumor primary surgery)
- 100% have `nodal_disease_total_count >= nodal_disease_positive_count > 0` (internally consistent)
- 100% have `ln_examined=0.0` exactly (not NULL — actual zero)
- The `nodal_disease_total_count` integer source carries the real value in every case

**Worst-case denominator delta:** rid 5197 — the integer source says `55 examined / 6 positive`, the double source says `0 examined`. Treating the double as truth would assign a 0-denominator and lose 6 LN-positive nodes.

**Recommendation for Lane LN (Prompt 2, Cursor Composer mig_225):**

Adopt **Option (d)** as recommended in the assessment plan, with these specific defaults baked into `vw_ln_surgery_publication_safe_VIEW_v1`:
- `ln_examined_double` = original `ln_examined` (preserved)
- `nodal_disease_total_count_int` = original integer source (preserved)
- `ln_examined_safe` = `COALESCE(NULLIF(ln_examined::INT, 0), nodal_disease_total_count, 0)` — i.e., int wins when double is 0
- `ln_denominator_source_conflict_flag` = TRUE when `(ln_examined::INT = 0 AND nodal_disease_total_count > 0)` OR `(ln_examined::INT > 0 AND nodal_disease_total_count IS NOT NULL AND ln_examined::INT <> nodal_disease_total_count)`

**Predicted impact:** these 6 rows shift from `ln_impossible_count_flag=TRUE` (in raw dedup) to `ln_denominator_source_conflict_flag=TRUE, ln_impossible_count_flag=FALSE` (in safe view). Manuscript LN-positive denominator increases by 78 examined nodes (10+1+5+55+6+1). Methods note: document this 78-node reconciliation explicitly.

---

## §3 — Outstanding for Prompt 3 (Copilot GPT-5.5)

CF-mig220 is closed; **Prompt 3's primary remaining work is CF-mig219** (the 24,371 vs 8,243 row delta on `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1`). Cowork has **not** investigated this in pre-flight; leave it for Copilot.

---

## §4 — No mutating SQL was run for either finding

- 0 `query_rw` calls
- 0 schema changes
- 5-gate v2 audit unchanged: 190/0/0/0/0
- Cohort parity unchanged: 10,871 / 10,871 / 10,871

---

**End of pre-flight findings.**
