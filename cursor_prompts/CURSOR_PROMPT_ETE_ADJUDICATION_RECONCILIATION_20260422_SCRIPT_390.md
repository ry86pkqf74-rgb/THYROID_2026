# Script 390 — ETE Adjudication Reconciliation

**Stamp:** 2026-04-22
**DB:** `thyroid_canonical_publication_v1_0` (PUB)
**Archive DB:** same (PUB-resident `archive_pub_v1_0.*` + `ete_legacy_<stamp>` schema if needed)
**Predecessors on this cleanup arc:** 387 (dedup) → 388 (round-2 classifier) → 389 (US zombie VIEW rewrites + complications audit)
**Relation to 389:** runs AFTER 389 closes. Does not touch any 389 deliverable. Independent commit + tag.

---

## Problem statement

Direct MotherDuck probe (2026-04-22) exposes a systemic adjudication contradiction on `main.canonical_patient_master`:

**`ete_grade_final_v2` vs `gross_ete_flag` / `op_intraop_gross_ete_any` / `path_gross_ete_flag`:**

| bucket | n | gross_ete_flag=T | op_intraop_gross=T | path_gross_ete=T |
|---|---|---|---|---|
| microscopic | 3,643 | **949** | 1,031 | 937 |
| gross | 190 | 189 | 49 | 38 |
| present_ungraded | 32 | 6 | 12 | 6 |

In the "microscopic" bucket, **896 patients have BOTH `path_gross_ete_flag=TRUE` AND `op_intraop_gross_ete_any=TRUE`** yet remain graded microscopic. All 949 contradictions came from `ete_grade_source='extraction_audit_engine_v7'` — zero are adjudicated.

**`ete_grade_final_v2` vs `canonical_invasion_patient_rollup_v1`:**

| CPM ete_grade_final_v2 | any_gross_ete_anywhere | any_micro_ete_anywhere | n |
|---|---|---|---|
| microscopic | T | F | 813 |
| microscopic | T | T | 218 |
| microscopic | F | F | 2,551 |
| gross | F | F | **141** |
| gross | T | F | 49 |

`canonical_invasion_patient_rollup_v1` was built by 363 (2026-04-22) with its own ETE aggregation; CPM's `ete_grade_final_v2` still runs on the older extraction_audit_engine_v7 output. The two pipelines disagree at scale and were never reconciled.

**Pre-existing queue:** `manuscript_workspace.cpm_ete_self_contradiction_queue_v1` holds exactly **1 row** (research_id 8254, flagged by 266b Phase 3 overshoot probe on 2026-04-17). It catches 1 of ~949 cases — effectively inert.

**Consistency of `ete_ordinal_worst`:** in the 949-contradiction cohort, `ete_ordinal_worst` is `1` (or NULL) in every row — never `2`. So the ordinal tracks the same (path-synoptic-extraction) source as `ete_grade_final_v2`; both contradict the gross-flag triplet. This is a 2-pipeline divergence, not a within-pipeline overwrite bug.

---

## Scope

### In-scope
- `main.canonical_patient_master` — `ete_grade_final_v2`, `ete_ordinal_worst`, `ete_grade_source`, `n_tumors_ete_present`, derived flags (`microscopic_ete_t3b_corrected`, `ajcc8_t_stage_with_microete_t3b_DEPRECATED` if dependent)
- `main.manuscript_cohort_v1` — `ete_grade_final`, `ete_grade_source` (inherit from CPM post-rebuild)
- `main.canonical_invasion_patient_rollup_v1` — read-only SOURCE for cross-pipeline signal
- `manuscript_workspace.cpm_ete_self_contradiction_queue_v1` — repopulated (drop + reinsert + retain any `status != awaiting_manual_review` rows if present)

### Out-of-scope
- `main.ete_adjudication_v1` — 45 clinician-adjudicated rows, source of truth, DO NOT MUTATE
- `main.canonical_path_malignant_events_v1.extrathyroidal_extension` / `.gross_ete` — event-grain, per-tumor; reconciliation happens at the patient rollup
- `main.canonical_operative_events_v1.gross_ete_flag` — event-grain, source signal, DO NOT MUTATE
- `main.canonical_us_nodule_v2.extrathyroidal_extension_on_us` — imaging-grain, separate modality
- Any `archive_legacy.*` or `archive_pub_v1_0.*` artifact — snapshots only, never touched
- Registry schema changes (tracked as Task #16, not part of 390)

### Explicit guards (do not violate)
- `ete_adjudicated_flag = TRUE AND ete_grade_adjudicated IN ('microscopic','absent')` → adjudicator wins, grade is STICKY
- `canonical_patient_master` row count: 10,871 → 10,871 (must hold exactly)
- `ete_adjudication_v1` row count: 45 → 45
- PHI: no clinical notes printed in any log; `research_id` only

---

## Rule options (Phase 1 plan-review gate — Logan picks ONE)

### Rule A — Worst-of (aggressive)
Any gross signal flips the grade:
```
new_grade = CASE
  WHEN ete_adjudicated_flag = TRUE AND ete_grade_adjudicated IN ('microscopic','absent')
    THEN ete_grade_final_v2  -- adjudicator sticky
  WHEN (gross_ete_flag = TRUE
        OR op_intraop_gross_ete_any = TRUE
        OR path_gross_ete_flag = TRUE
        OR inv.any_gross_ete_anywhere = TRUE)
    THEN 'gross'
  WHEN (inv.any_microscopic_ete_anywhere = TRUE
        OR ete_any_present_path = TRUE)
    THEN 'microscopic'
  ELSE ete_grade_final_v2
END
```
**Blast radius (direct probe):** 190 → **1,324** gross (∆ +1,091 flip-up, 1 flip-down on unsupported current-gross).
**Semantics:** canonical AJCC8 T3b definition ("any gross ETE anywhere in op or path"). Safest for publication denominators.
**Risk:** aggressive reclassification; may overcount if any upstream gross flag has false positives (needs 386b-style audit after).

### Rule B — Conservative (both-sources required)
Flip only where BOTH path AND op confirm gross:
```
new_grade = CASE
  WHEN ete_adjudicated_flag = TRUE AND ete_grade_adjudicated IN ('microscopic','absent')
    THEN ete_grade_final_v2
  WHEN path_gross_ete_flag = TRUE AND op_intraop_gross_ete_any = TRUE
    THEN 'gross'
  WHEN inv.any_microscopic_ete_anywhere = TRUE OR ete_any_present_path = TRUE
    THEN 'microscopic'
  ELSE ete_grade_final_v2
END
```
**Blast radius (direct probe):** ~749 flip-up (most of the 896-patient both-gross cohort), 0 flip-down.
**Semantics:** requires multi-source confirmation; treats single-source gross as a mention that needs adjudication.
**Risk:** undercounts gross-ETE in patients where only one source mentioned it — but that's conservative by design.

### Rule C — Queue-only (no mutation)
Do not touch `ete_grade_final_v2`. Instead, repopulate `manuscript_workspace.cpm_ete_self_contradiction_queue_v1` with all 949 contradictions + 141 unsupported-gross + 2,551 grade-without-any-source rows.
**Blast radius:** 0 CPM mutations; queue grows from 1 → ~3,641 rows.
**Semantics:** defers decision to human review; maintains current published denominators.
**Risk:** publication cohort stays inconsistent until queue is worked. 390 becomes a discovery-only script.

### Default recommendation: **Rule A + re-queue residuals**
Rule A covers the AJCC8 T3b clinical definition. Residuals (2,551 "microscopic" with zero invasion-rollup support) get added to the contradiction queue for a separate pass. One script, one commit, one tag.

---

## Execution phases

### Phase 0 — Discovery + probe (no writes)
Before any plan approval, the script must emit:

1. **Pre-state snapshot** (log + stdout):
   - `ete_grade_final_v2` distribution on CPM (bucket × count)
   - `ete_grade_final_v2` × `ete_ordinal_worst` matrix
   - `ete_grade_final_v2` × `any_gross_ete_anywhere` × `any_microscopic_ete_anywhere` matrix (CPM LEFT JOIN invasion_rollup)
   - `ete_adjudicated_flag=TRUE` cohort breakdown (must = 45)
   - Current `cpm_ete_self_contradiction_queue_v1` row count + status distribution

2. **Rule simulations** (each rule, separately, as read-only CTE):
   - Rule A: total would-be-gross, flip-up n, flip-down n, queue residual n (non-gross with no signal and no adjudication)
   - Rule B: total would-be-gross, flip-up n, flip-down n, queue residual n
   - Rule C: queue grow n

3. **Halt file:** `scripts/output/390_probe_report.md` with the three rule simulations side-by-side + any drift warning if the direct numbers diverge from this prompt's baselines (949 / 190 / 1,324 / ~749) by more than 2%.

4. **Plan approval file:** `scripts/output/390_plan_approval.txt` — Logan writes one line: `Rule A` | `Rule B` | `Rule C`. Script will not proceed to Phase 2 without this file.

### Phase 1 — Plan-review gate
Halt. Human review of the probe report. Logan writes the approval file. Re-entry re-reads the file and re-runs Phase 0 probe (re-verify the pre-state numbers are still within 2% of the approved plan — halt on drift).

### Phase 2 — Apply writes (chosen rule only)
Atomic within a transaction where possible. Per-step:

2A. **Snapshot CPM ETE columns:**
```sql
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_ete_pre390_<STAMP> AS
SELECT research_id, ete_grade_final_v2, ete_ordinal_worst, ete_grade_source,
       ete_grade_final, ete_grade_adjudicated, ete_adjudicated_flag,
       n_tumors_ete_present, microscopic_ete_t3b_corrected,
       gross_ete_flag, op_intraop_gross_ete_any, path_gross_ete_flag,
       ete_any_present_path
FROM main.canonical_patient_master;
```

2B. **Apply UPDATE to CPM** (per chosen rule; SELECT-then-UPDATE pattern so no column is touched on no-op rows):
```sql
UPDATE main.canonical_patient_master cpm
SET ete_grade_final_v2 = t.new_grade,
    ete_grade_source = CASE
      WHEN t.new_grade != cpm.ete_grade_final_v2
        THEN 'script_390_' || '<RULE>' || '_' || '<STAMP>'
      ELSE cpm.ete_grade_source
    END,
    ete_ordinal_worst = CASE
      WHEN t.new_grade = 'gross' AND cpm.ete_ordinal_worst < 2 THEN 2
      WHEN t.new_grade = 'microscopic' AND cpm.ete_ordinal_worst < 1 THEN 1
      ELSE cpm.ete_ordinal_worst
    END
FROM (
  <rule-specific CTE that joins invasion_rollup and yields (research_id, new_grade)>
) t
WHERE cpm.research_id = t.research_id
  AND t.new_grade != cpm.ete_grade_final_v2;
```

2C. **Provenance row on `__readme`** (per the 386b 4-place audit pattern):
```sql
INSERT INTO main.__readme(content, updated_at) VALUES
('Script 390 applied <RULE> ETE adjudication reconciliation on canonical_patient_master. Source: path_gross_ete_flag OR op_intraop_gross_ete_any OR invasion_rollup.any_gross_ete_anywhere. Adjudicator-sticky: ete_adjudicated_flag=TRUE AND ete_grade_adjudicated IN (microscopic,absent). Rows mutated: <N>. Snapshot: archive_pub_v1_0.cpm_ete_pre390_<STAMP>.',
 CAST(CURRENT_TIMESTAMP AS TIMESTAMP));
```

2D. **Repopulate contradiction queue** (TRUNCATE + INSERT, preserving any human-reviewed rows):
```sql
CREATE OR REPLACE TABLE manuscript_workspace.cpm_ete_self_contradiction_queue_v1 AS
-- keep any previously-reviewed rows
SELECT * FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1
WHERE status != 'awaiting_manual_review'
UNION ALL
-- add current residuals (cohort varies by rule)
SELECT ... ;
```

2E. **Rebuild downstream inherit:** rebuild `main.manuscript_cohort_v1.ete_grade_final` + `.ete_grade_source` from CPM (simple `UPDATE FROM` join on research_id; no schema change).

### Phase 3 — Post-state verification (halt-on-fail)
- Row count CPM = 10,871 ✓
- Row count ete_adjudication_v1 = 45 ✓ (untouched)
- Every row where `ete_adjudicated_flag=TRUE AND ete_grade_adjudicated='microscopic'` → `ete_grade_final_v2='microscopic'` (adjudicator-sticky verified) ✓
- Post-rule contradiction count within tolerance (rule-specific: Rule A expects 0 contradictions of form `ete_grade_final_v2='microscopic' AND any_gross_ete_anywhere=TRUE AND NOT adjudicator-micro`; Rule B expects 0 contradictions of form `ete_grade_final_v2='microscopic' AND path_gross_ete_flag=TRUE AND op_intraop_gross_ete_any=TRUE AND NOT adjudicator-micro`)
- `ete_ordinal_worst` ≥ 2 for every row with `ete_grade_final_v2='gross'` ✓
- manuscript_cohort_v1.ete_grade_final matches CPM.ete_grade_final_v2 (100% join) ✓
- `__readme` row landed with correct timestamp ✓
- Snapshot table row count = 10,871 ✓

### Phase 4 — Commit + tag
- Single commit: `Script 390: ETE adjudication reconciliation — <RULE> applied; <N> CPM rows mutated; contradiction queue repopulated`
- Staged paths: `scripts/390_ete_adjudication_reconciliation.py`, `scripts/output/390_probe_report.md`, `scripts/output/390_plan_approval.txt`, `scripts/output/390_run.log`, `scripts/output/390_close_out.md`
- Tag: `v1_0-ete-reconciled-<stamp>` (only if Rule A or B executed; Rule C gets a lighter `ete-queue-refreshed-<stamp>`)

---

## Idempotency

Script must detect a prior successful run via both:
1. `archive_pub_v1_0.cpm_ete_pre390_*` snapshot presence (any stamp suffix)
2. `__readme` row whose `content` starts with `Script 390 applied`

If both present AND post-state invariants all hold → exit 0 with "NO-OP, prior run detected" without writing.
If only one present → halt with "partial prior run; manual cleanup required".

---

## Carry-forwards from 390

Expected to carry forward regardless of rule choice:
- **Residual queue review** — however many rows land in `cpm_ete_self_contradiction_queue_v1` need human pathology review (Rule A: ~2,551 zero-signal "microscopic"; Rule B: ~949 single-source contradictions; Rule C: ~3,641 all residuals)
- **T-stage downstream** — `microscopic_ete_t3b_corrected`, `ajcc8_t_stage_with_microete_t3b_DEPRECATED`, and any staging column that reads `ete_grade_final_v2` must be re-probed after 390 closes. Likely separate Script 391.
- **386b audit template** — follow the 4-place pattern (archive snapshot + `__readme` + run log + first-run stdout) for the in-place UPDATE.
- **Invasion rollup alignment audit** — after 390, re-run the CPM×invasion_rollup cross-tab and assess whether `canonical_invasion_patient_rollup_v1` ETE aggregation rule itself needs a pass.

---

## Non-goals (explicit)

- Do NOT change column names, data types, or schema on CPM
- Do NOT rebuild invasion_rollup — 363's output is the signal, not a target
- Do NOT touch path_synoptic extraction or extraction_audit_engine_v7 — those are upstream, 390 patches the rollup only
- Do NOT expand scope to other pathology-grade columns (e.g., `lvi_grade`, `margin_status`) — one domain at a time
- Do NOT add new columns — reuse existing `ete_grade_source` for provenance via string value change

---

## First action for the agent

Run Phase 0 probe-only. Print:
1. The 7-row pre-state distribution table (baseline 949 / 190 / 3,643)
2. The 3-rule simulation side-by-side (Rule A / B / C blast radii)
3. The halt message: "Halted at Phase 1 plan-review gate. Write `scripts/output/390_plan_approval.txt` with `Rule A` | `Rule B` | `Rule C` to proceed."

DO NOT write to `canonical_patient_master`, `manuscript_cohort_v1`, `cpm_ete_self_contradiction_queue_v1`, or `archive_pub_v1_0` in this phase. Probe-only means reads + report-only writes.
