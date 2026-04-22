# Script 389 — Phase 0B Classifier + Baseline Patch

**Stamp:** 2026-04-22
**Type:** Continuation patch (do NOT restart the script thread or spawn 389b)
**Scope:** surgical — classifier CTE + Phase 0B drift-gate thresholds only
**Predecessors preserved:** Phase 0A discovery output, Phase 2D NULL-date fix, Phase 2E has_any_us patch, Phase 2H complications Rule B scaffold, Phase 2I CPM backfill archive
**Reason:** the Phase 0B baselines `18,310 / 17,090 / 2,152 / 27` in the original prompt are not reproducible from live `main.canonical_us_nodule_v2` state. Direct MotherDuck probe (2026-04-22) confirms they were phantom from compressed context, not a real prior classifier. The drift gate correctly halted. Fix is to replace classifier + baselines with live-derived partitions.

---

## Live-state probe (2026-04-22, this session)

Target: `main.canonical_us_nodule_v2` — **37,579 rows total** (37,438 non-agg + 141 agg).

**Source-flag partition** (the only actually-consistent partition of the row set):

| is_aggregate_row | source_tirads_llm | source_base | nlp_backfill_pending | n |
|---|---|---|---|---|
| FALSE | TRUE | TRUE | FALSE | **26,402** |
| FALSE | FALSE | TRUE | FALSE | **8,353** |
| FALSE | FALSE | FALSE | TRUE | **2,061** |
| FALSE | TRUE | FALSE | FALSE | **566** |
| FALSE | FALSE | FALSE | FALSE | **56** |
| TRUE | TRUE | TRUE | FALSE | **141** |

Sum = 37,579 ✓

**location_raw length/semicolon profile** (for any content-based sub-classifier):
- length NULL: 5,094
- length < 200: 26,351 non-agg
- length 200–400: 5,787 non-agg + 98 agg
- length 400–800: 206 non-agg + 43 agg
- length ≥ 800: 0
- semicolons=0: 31,197; =1: 761; ≥2: 527

No combination of these signals produces `18,310 / 17,090 / 2,152 / 27`. Those numbers were never a real partition.

---

## Patch 1 — `_classifier_case_sql()` in `scripts/389_us_zombie_view_rewrites_and_complications_audit.py`

**Remove** the current classifier (whatever `location_raw LIKE '%;%'` / length-threshold logic is in there — the one that produced `25,750 / 10,441 / 13 / 1,234`).

**Replace with** a 4-bucket partition derived purely from existing boolean flags on the table. Rationale: these are the only signals that partition the row set consistently and reproducibly; any content-based blob classifier is an ADDITIONAL audit axis, not a replacement for the source-flag one.

```python
def _classifier_case_sql(alias: str = "n") -> str:
    """
    Classify every row in canonical_us_nodule_v2 into exactly one of four buckets.
    Partition is derived from the source-provenance boolean flags + is_aggregate_row.
    Buckets sum to 37,579 (2026-04-22 baseline).

    Buckets:
      - clean_dual_source : both sources parsed, non-aggregate    (26,402)
      - clean_base_only   : base-only or tirads-only single-source (8,919 = 8,353+566)
      - needs_backfill    : no source parsed; backfill pending OR orphan (2,117 = 2,061+56)
      - aggregate_rollup  : per-exam aggregate row, not per-nodule   (141)
    """
    return f"""
    CASE
      WHEN {alias}.is_aggregate_row = TRUE
        THEN 'aggregate_rollup'
      WHEN {alias}.source_tirads_llm = TRUE AND {alias}.source_base = TRUE
        THEN 'clean_dual_source'
      WHEN ({alias}.source_tirads_llm = TRUE AND COALESCE({alias}.source_base, FALSE) = FALSE)
        OR (COALESCE({alias}.source_tirads_llm, FALSE) = FALSE AND {alias}.source_base = TRUE)
        THEN 'clean_base_only'
      ELSE
        'needs_backfill'
    END
    """
```

## Patch 2 — Phase 0B drift-gate thresholds

Replace the four baseline constants in the prestate report / drift gate:

```python
# OLD (phantom baselines — retired)
# EXPECTED_CLEAN_LLM_PARSED = 18310
# EXPECTED_CLEAN_NON_LLM    = 17090
# EXPECTED_ZOMBIE           = 2152
# EXPECTED_BLOB             = 27

# NEW (2026-04-22 live-derived baselines)
EXPECTED_CLEAN_DUAL_SOURCE = 26_402
EXPECTED_CLEAN_BASE_ONLY   = 8_919   # 8,353 base-only + 566 tirads-only
EXPECTED_NEEDS_BACKFILL    = 2_117   # 2,061 backfill_pending + 56 orphan
EXPECTED_AGGREGATE_ROLLUP  = 141
EXPECTED_TOTAL             = 37_579

DRIFT_TOLERANCE_PCT = 2.0  # unchanged; 2% per-bucket still applies
```

## Patch 3 — Phase 0 prestate report section

Rename the table in `scripts/output/389_prestate.md`:
- **OLD:** `clean_llm_parsed / clean_non_llm / zombie / blob`
- **NEW:** `clean_dual_source / clean_base_only / needs_backfill / aggregate_rollup`

Add a narrative paragraph at the top of the section:

> **Classifier reset 2026-04-22.** The original prompt's baselines (18,310 / 17,090 / 2,152 / 27) were not reproducible against live state — direct MotherDuck probe showed no combination of source-flags or `location_raw` content signals yields that partition. They were phantom from compressed context. The classifier is now grounded in the four boolean flags that actually do partition the table: `is_aggregate_row`, `source_tirads_llm`, `source_base`, `nlp_backfill_pending`. Baselines below are frozen from the 2026-04-22 probe.

## Patch 4 — Phase 2 "zombie archive" action — RETIRE

The original Phase 2 step that DELETE'd the `zombie` bucket rows from `canonical_us_nodule_v2` is now **out of scope for 389**. Reason: the 4 new buckets are all legitimate, non-destructive partitions of the table. No row is "zombie" in the structural sense — `needs_backfill` rows are legitimate entries awaiting NLP extraction, not remnants to delete.

**Action:** remove the Phase 2 DELETE step entirely. Replace with a read-only write to `main.__readme`:

```sql
INSERT INTO main.__readme(content, updated_at) VALUES
('Script 389 Phase 0B classifier reset 2026-04-22. canonical_us_nodule_v2 partition frozen at 26,402 / 8,919 / 2,117 / 141 (clean_dual_source / clean_base_only / needs_backfill / aggregate_rollup). No rows deleted; prior "zombie" concept retired — source-flag partition is non-destructive.',
 CAST(CURRENT_TIMESTAMP AS TIMESTAMP));
```

If Logan still wants a content-based blob audit (rows with `length(location_raw) >= 400 OR semicolons >= 2` that were never split by the v2 nodule splitter — ~750 candidates) that becomes a **new standalone Script 389b**, not part of this run. Draft only after 389 closes.

## Patch 5 — Preserve unchanged phases

Do NOT modify:
- Phase 2A–2G US VIEW rewrites (the canonical_us_exam_master_VIEW_v2 phantom-date fix, has_any_us patch, exam_master rebuild)
- Phase 2H complications Rule A/B/C scaffold (Logan still needs to pick the rule in the plan-approval file)
- Phase 2I CPM backfill archive (`cupm_v2_canonical_backfill_v1` → archive)
- Phase 3 post-state verification
- Phase 4 commit + tag
- Idempotency guard (archive snapshot + `__readme` row detection)

## Patch 6 — Re-entry flow

After patches 1–5 applied:

1. Re-run probe-only. Expected output:

   ```
   === Phase 0 partition probe on canonical_us_nodule_v2 ===
   clean_dual_source : 26,402  (expected 26,402, ∆ 0.00%)
   clean_base_only   :  8,919  (expected  8,919, ∆ 0.00%)
   needs_backfill    :  2,117  (expected  2,117, ∆ 0.00%)
   aggregate_rollup  :    141  (expected    141, ∆ 0.00%)
   total             : 37,579  (expected 37,579, ∆ 0.00%)
   drift gate : PASS
   ```

2. If drift gate passes, Phase 0 halts normally at the plan-review gate. Logan writes `scripts/output/389_plan_approval.txt` with the complications rule choice (`Rule A` / `Rule B` / `Rule C` per the original prompt's Phase 2H scaffold — this is the plan-review decision that was always intended).

3. Proceed to Phase 2 with the complications audit + VIEW rewrites + CPM archive. No nodule DELETE.

## Patch 7 — Close-out carry-forwards to add

Append to `scripts/output/389_close_out.md` when it lands:

- **CF-7 (new):** Script 389 classifier was reset 2026-04-22 from phantom baselines (18,310 / 17,090 / 2,152 / 27) to live-derived source-flag partition (26,402 / 8,919 / 2,117 / 141). Original "zombie / blob" concept retired; if content-based multi-nodule-blob audit is still wanted, drafts as 389b.
- **CF-8 (new):** 2,117 rows (`needs_backfill`) on `canonical_us_nodule_v2` have neither source_tirads_llm nor source_base parsed — 2,061 with `nlp_backfill_pending=TRUE`, 56 orphaned. Orphan cohort (56 rows) needs separate probe — why were these ingested without a source flag?

---

## Files to touch

1. `scripts/389_us_zombie_view_rewrites_and_complications_audit.py`
   - `_classifier_case_sql()` — rewrite per Patch 1
   - Baseline constants — rewrite per Patch 2
   - Phase 0 prestate report table — rename per Patch 3
   - Phase 2 DELETE step — remove per Patch 4
   - `__readme` provenance row — add per Patch 4

2. `cursor_prompts/CURSOR_PROMPT_US_ZOMBIE_VIEW_REWRITES_COMPLICATIONS_AUDIT_20260422_SCRIPT_389.md`
   - Add a "Phase 0B classifier reset 2026-04-22" note at the top so future re-reads are grounded

3. No changes to `scripts/_investigate_zombie_classifier.py` / `_investigate_zombie_classifier_v2.py` — leave as reference artifacts, they informed the drift-gate diagnosis.

---

## First action for the agent

Apply Patches 1–4 to `scripts/389_us_zombie_view_rewrites_and_complications_audit.py`. Then re-run `python3 scripts/389_us_zombie_view_rewrites_and_complications_audit.py --phase 0`. Confirm drift gate PASS at the new baselines. Report back with the probe output and wait for the Rule A/B/C decision on the plan-approval file before any Phase 2 writes.

Do NOT rebuild the script from scratch. Do NOT open a new chat. Patch in place.
