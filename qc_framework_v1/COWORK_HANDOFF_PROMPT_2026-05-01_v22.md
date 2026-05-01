# Cowork Handoff Prompt v22 — Post M038 Audit / mig_252 + mig_253 Dispatched
**Generated:** 2026-05-01 by Cowork at end of v21-era session (post-M038-audit)
**Tip of `origin/main` at write:** `6fd1710` — `manuscript(M038): planning doc — RQ locked, paused on mig_252/253`
**Supersedes:** v21 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v21.md`
**Two new strategic items:**
- `cursor_prompts/CURSOR_PROMPT_MIG_252_COMP_CONFIRMED_ROLLUP_FIX_20260501.md` (cohort-wide complications-rollup repair — IN FLIGHT, dispatch this to Cursor Composer)
- `cursor_prompts/CURSOR_PROMPT_MIG_253_SURG_PROCEDURE_TYPE_FILL_20260501.md` (cohort-wide procedure-type fill — IN FLIGHT, dispatch this to Cursor Composer)

---

## §0 — First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v22.md` end-to-end before any tool use. Then read both Cursor Composer dispatch prompts referenced in §2 to understand the in-flight upstream fixes.
>
> **Standing context:** I'm Logan Glosser, Emory thyroid-cancer surgery researcher. The v21 → v22 session (2026-05-01) audited the M038 cohort as part of starting the massive-goiter manuscript and uncovered TWO cohort-wide canonicalization bugs that block M038 (and partially M032). Both have been dispatched to Cursor Composer as mig_252 and mig_253. The v22 chat starts with both fixes in flight.
>
> **Tooling on this machine:**
> - **Desktop Commander MCP** for git/shell. Always use Desktop Commander rather than the bash sandbox for git ops — bash sandbox can't unlink `.git/index.lock` (FileVault). `feedback_use_desktop_commander_first.md`.
> - **MotherDuck MCP** authed to `logan.glosser.eras@gmail.com`. Four databases attached; **the master canonical pub V1.0 is `thyroid_canonical_publication_v1_0`**. Always use this DB unless explicitly told otherwise — `current_database()` should return it before any DDL/DML.
> - **GitHub repo** at `/Users/loganglosser/THYROID_2026`. `origin/main` is canonical. Surgical git add per `feedback_surgical_git_add.md` (explicit paths only, never `-A`).
>
> **Run the §3 first-action checklist before any new analytical work.**
>
> **HARD CONSTRAINT (carry-over from v21):** I'm working on the **ETE manuscript (M044)** in ChatGPT. **Do NOT touch M044 or M051** here — they're owned by the ChatGPT lane.

---

## §1 — Round delta v21 → v22 (what landed in the v21 → v22 session)

| Mig / Doc | Lane | Commit | Outcome |
|---|---|---|---|
| **mig_249** (Cursor Composer) | feasibility re-refresh | `37911d0` | ✓ Landed mid-session. 83 rows re-scored; 5 RED→YELLOW (M025/M029/M037/M045/M075); stale gating cleared. `canonical_version_at_scoring = 'v1_0_post_mig_248'`. |
| **M038 first draft (parallel session)** | Cursor Composer | `2fc6fef` | ✓ A 249-line first draft of M038 was authored in a parallel session at 2026-05-01 05:26 EDT (ahead of my session's planning doc). Uses **composite OR exposure** (≥100g OR substernal OR airway → n=2,501). **CAUTION:** uses the buggy `any_confirmed_complication_flag` (35.5% massive vs 19.1% non-massive — both rates are inflated by the comp_*_confirmed rollup bug; see §4). Already flags the procedure-type 30.4% null rate (the issue dispatched as mig_253). |
| **Standing rule: demographics + full column review** | feedback file | `89a505b` | ✓ `memory/feedback_manuscript_demographics_and_full_column_review.md`. Set by Logan: every manuscript MUST include a demographic Table 1 and a systematic full-dataset column review pass before the cohort view is finalized. |
| **mig_251** (Cowork-direct) | M038 cohort extension | `f673f09` | ✓ `cohort_m038_massive_goiter_v1` extended from 24 → ~95 columns. 71 columns added across demographics, comorbidities, surgical context, anatomy, pathology, LOS, expanded confirmed complications, NSQIP perioperative outcomes, tracheostomy, recurrence. Verified: 10,871 rows; gate1=218; cohort_parity TRUE. |
| **mig_252 dispatch authored** | Cursor Composer dispatch | `55aade0` | ✓ `cursor_prompts/CURSOR_PROMPT_MIG_252_COMP_CONFIRMED_ROLLUP_FIX_20260501.md`. **STATUS AT V22 WRITE: NOT YET LANDED.** Logan to paste into Cursor Composer after reading the dispatch prompt. |
| **mig_253 dispatch authored** | Cursor Composer dispatch | `55aade0` | ✓ `cursor_prompts/CURSOR_PROMPT_MIG_253_SURG_PROCEDURE_TYPE_FILL_20260501.md`. **STATUS AT V22 WRITE: NOT YET LANDED.** |
| **M038 planning doc** | Cowork-direct | `6fd1710` | ✓ `manuscript_outputs/v1_0_20260501/M038_definition_paper_PLANNING_v1.md`. Sets the M038 lane as a **definition paper** comparing weight-based vs. anatomic-compression operationalizations head-to-head (different framing from `2fc6fef` first draft — see §5). Status: ⏸ paused on mig_252 + mig_253. |

---

## §2 — Cursor Composer dispatches in flight

These two prompts must be copy-pasted into a Cursor Composer chat by Logan. They are NOT auto-dispatched. Until they land, M038 is paused and M032 carries a known complications-section bug.

### mig_252 — comp_*_confirmed rollup repair

**File:** `cursor_prompts/CURSOR_PROMPT_MIG_252_COMP_CONFIRMED_ROLLUP_FIX_20260501.md`

**The bug:** `canonical_patient_master.comp_*_confirmed` columns and `any_confirmed_complication_flag` count `finding_status='absent'` events (negation evidence) as confirmation. Patient-level audit:

| Complication | n flagged confirmed | with ≥1 'present' event | strict (present + def/prob) | % failing 'present' |
|---|---:|---:|---:|---:|
| chyle_leak | 1,576 | 5 | 3 | **99.7%** |
| seroma | 871 | 45 | 39 | **94.8%** |
| rln_injury | 690 | 21 | 21 | **97.0%** |
| hematoma | 250 | 70 | 68 | **72.0%** |
| hypoparathyroidism | 406 | 298 | 296 | 26.6% |

**M038 primary outcome under strict definition vs buggy rollup:**

| Subset | Buggy rollup | Strict (present + def/prob) |
|---|---:|---:|
| ≥200g focal cohort (n=475) | 30.7% (146) | **2.1% (10)** |
| <200g (n=8,655) | 21.3% | **3.4% (296)** |
| Full cohort | 22.9% | **3.6% (388)** |

**Spec:** corrected rollup is `EXISTS (... finding_status='present' AND evidence_strength IN ('definitive','probable') ...)`.

**Acceptance:** `comp_seroma_confirmed` count drops 618 → 27; `any_confirmed_complication_flag` count drops ~2,490 → ~388; gate1=218 unchanged.

### mig_253 — surg_procedure_type fill

**File:** `cursor_prompts/CURSOR_PROMPT_MIG_253_SURG_PROCEDURE_TYPE_FILL_20260501.md`

**The gap:** 2,138 of 10,871 patients (19.7%) have `surg_procedure_type IS NULL AND surg_total_thyroidectomy IS NULL AND surg_hemithyroidectomy IS NULL` simultaneously, despite all 2,138 having `first_surgery_date` and `n_surgeries` populated. Source data exists in `canonical_operative_events_v1` and the NSQIP CPT fields (CPT 60240 / 60252 / 60271 / 60260 cover 348 of the 2,138 with unambiguous total-thyroidectomy mappings).

**Spec:** map procedure source values + nsqip_cpt_code → procedure-type buckets via the table in §3 of the dispatch prompt. UPDATE the three CPM columns for the 2,138 affected rows.

**Acceptance:** NULL count drops 2,138 → ≤50 (residual chart-review queue); M038 ≥200g NULL count drops 121 → ≤5; gate1=218 unchanged.

---

## §3 — First-action checklist for the new chat

### Step 3.1 — Confirm git state via Desktop Commander

```bash
cd /Users/loganglosser/THYROID_2026
git fetch origin
git log --oneline -15
git status --porcelain
```

Expect HEAD ≥ `6fd1710`. If there are commits past `6fd1710` from Cursor Composer, mig_252 and/or mig_253 may have landed — check the commit messages.

### Step 3.2 — Confirm DB context

```sql
SELECT current_database() AS db, current_schema() AS schema;
-- expected: thyroid_canonical_publication_v1_0, main
```

If `current_database()` is anything else, the MotherDuck context is wrong. Re-attach or re-issue `USE thyroid_canonical_publication_v1_0;` before any DDL/DML.

### Step 3.3 — Lakehouse health gate

```sql
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
```

Required: gate1=218, gates 2-5 = 0, cohort_parity TRUE (10871×3). mig_252 and mig_253 should NOT change gate1 (they're column-value fixes, not registry changes).

### Step 3.4 — Did mig_252 land?

```sql
-- Audit: post-mig_252, comp_seroma_confirmed should be ~27 (was 618)
SELECT
  SUM(CASE WHEN comp_seroma_confirmed THEN 1 ELSE 0 END) AS seroma_confirmed,
  SUM(CASE WHEN comp_chyle_leak_confirmed THEN 1 ELSE 0 END) AS chyle_confirmed,
  SUM(CASE WHEN comp_rln_injury_confirmed THEN 1 ELSE 0 END) AS rln_confirmed,
  SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) AS any_comp_flag
FROM main.canonical_patient_master;
-- pre-mig_252: 618 / 1576 / 690 / 2490
-- post-mig_252: ~27 / ~3 / ~21 / ~388
```

### Step 3.5 — Did mig_253 land?

```sql
SELECT COUNT(*) AS still_null
FROM main.canonical_patient_master
WHERE surg_procedure_type IS NULL
  AND surg_total_thyroidectomy IS NULL
  AND surg_hemithyroidectomy IS NULL;
-- pre-mig_253: 2,138
-- post-mig_253: ≤50
```

### Step 3.6 — Reconcile the two M038 streams (decision needed)

There are now **two M038 artifacts in the repo**:

1. **`manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v1.md`** (commit `2fc6fef`, parallel session) — descriptive paper with composite OR exposure (≥100g OR substernal OR airway, n=2,501). 249-line first draft. **Uses the buggy any_confirmed_complication_flag.**
2. **`manuscript_outputs/v1_0_20260501/M038_definition_paper_PLANNING_v1.md`** (commit `6fd1710`, this session) — analytical paper with three head-to-head exposure operationalizations and strict-definition primary outcome. Pre-draft planning only.

These are two different RQs and two different analytical frames using the same cohort view. Logan needs to pick:

- **(A)** Both papers, sequenced — descriptive paper (M038-A) goes first, definition paper (M038-B) goes second. Two manuscripts from one cohort.
- **(B)** Merge — revise the descriptive draft to use the strict outcome and add the head-to-head definition analysis as a Section 3.4 stratified table.
- **(C)** Pick one — drop the descriptive draft and use only the definition paper, OR drop the definition paper and use only the descriptive draft.

Surface this as an `AskUserQuestion` early in the v22 chat unless Logan opens with a clear directive.

---

## §4 — Updated lakehouse state (post-v21, pre-v22-Cursor-dispatches)

| Schema | Object count | Notes |
|---|---:|---|
| `main` | 110 tables, 10 views | Two known bugs: comp_*_confirmed rollup (mig_252) and surg_procedure_type fill (mig_253). |
| `manuscript_workspace` | 86 tables, 64 views | mig_251 added 71 columns to `cohort_m038_massive_goiter_v1`. Other 62 cohort views unchanged. |
| `views_readable` | 0 tables, 62 views | Unchanged. |
| `semantic_publication` | 1 table, 16 views | Unchanged. Gate-1 verified at 218. |
| `raw` | 2 tables | Unchanged. |
| **Total** | **422+ objects** | |

**Open carry-forwards (active at v22 write):**

| ID | Description | Trigger to close |
|---|---|---|
| **CF-COMP-CONFIRMED-ROLLUP-FIX** | mig_252 — repair comp_*_confirmed rollup cohort-wide | Cursor Composer applies mig_252 |
| **CF-SURG-PROC-TYPE-FILL** | mig_253 — fill surg_procedure_type for 2,138 NULL pts | Cursor Composer applies mig_253 |
| **CF-M038-RECONCILIATION** | Choose between descriptive draft (`2fc6fef`) and definition-paper plan (`6fd1710`), or merge | Logan picks A/B/C in §3.6 |
| **CF-M038-PAUSED-ON-MIG_252** | M038 (whichever framing wins) blocked on outcome correctness | mig_252 lands |
| **CF-M032-COMPLICATIONS-REBUILD** | M032 25-yr descriptive draft (commit `f9f848c`) references buggy 23% any-comp rate; needs rebuild | mig_252 lands |
| **CF-M044-CHATGPT-LANE** | M044 (AJCC ETE) being drafted in ChatGPT — Cowork hands-off | Logan signals lane done |
| **CF-M038-AUTHOR-INPUT-GAPS** | Existing draft has 10 author-input gaps (per `2fc6fef` commit message) | Logan fills inline |
| **CF-M038-SEX-CODING** | Sex coded `'female'`/`'male'`, not `'F'`/`'M'` — query-template note | Caught at first draft |
| **CF-EMORY-IT-ENTITLEMENTS** | 5 questions for Emory IT in `MD_MIGRATION_PLAN_v1` §8 | Emory IT responds |
| **CF-MD-MIGRATION** | Move thyroid v1.0 publication out of MotherDuck to Emory-tenant target | Emory IT entitlement confirmed |
| **CF-METHODS-V17-V20-ADDENDUM** | Methods doc references mig_212-234 but not v17/v20/v21/v22 round | ~10 min Cowork edit |
| **CF-MIG_252-DOWNSTREAM** | After mig_252, recompute event counts for all complications-touching manuscripts | mig_252 lands |
| **CF-COMP-CONFIRMED-VARIANTS** | mig_252 also needs to fix `_definitive`, `_probable_or_better`, `_any_evidence`, `_suspected` family-flag variants if same bug pattern | mig_252 lands |
| **CF-SURG-CPT-VOCAB-REGISTRY** | Canonicalize CPT → procedure-type mapping into a registry table (post-mig_253) | mig_253 lands |
| **CF-PARATHYROID-EVENT-SAFE** | Events-grain `intact_pth_value_ngL` safe view (deferred from mig_243) | If M039 needs per-event PTH |
| **CF-LN-METS-ARRAY-EMPTY-2801** | 2,801 LN-positive cases lack histology-attribution evidence | Methods caveat only |
| **Future-Gate6-Col-Registry** | Add gate6 to `qc_audit_dashboard_VIEW_v1` for col_registry dup-key detection | Small Cowork lane if greenlit |

**Closed in this session:**

- ~~CF-MIG_247-RERUN~~ (mig_249 landed at `37911d0`)
- ~~CF-MIG_249-IN-FLIGHT~~ (landed mid-session)
- ~~CF-M038-COHORT-EXTEND~~ (landed as mig_251 with substantially expanded scope)
- ~~CF-M038-SURG-TYPE-NULL~~ (escalated to cohort-wide CF-SURG-PROC-TYPE-FILL / mig_253)

---

## §5 — Two M038 framings — side-by-side

| | Existing draft (`2fc6fef`, parallel session) | New planning doc (`6fd1710`, this session) |
|---|---|---|
| Status | 249-line first draft v1 | Pre-draft planning, paused |
| RQ type | Descriptive cohort | Analytical / definition paper |
| Title | "Massive Goiter at a Tertiary Referral Center: A Composite-Definition Descriptive Cohort of 2,501 Patients (Emory University, 1999–2025)" | "Definition of 'Massive' Goiter and Perioperative Complication Risk" |
| Exposure | Composite OR: ≥100g OR substernal OR airway (n=2,501) | Three separate definitions tested head-to-head: ≥200g (n=475), substernal (n~1,000), airway (n~1,500) |
| Primary outcome | `any_confirmed_complication_flag` (35.5% massive vs 19.1% non-massive — **buggy**) | Strict-definition composite (post-mig_252; 2.1% in ≥200g focal) |
| Cohort denominator | n=2,501 (composite-defined) | n=475 (≥200g focal) for arm-specific; n=10,871 for full-cohort interaction model |
| Strength | Reads as a clean institutional cohort paper; large n | Novel methodological contribution (re-frames a literature with inconsistent definitions) |
| Risk | Buggy primary outcome; composite OR obscures the very finding (anatomic compression > weight) | Underpowered focal cohort with strict outcome (n=10 events); needs full-cohort interaction model |

**My recommendation, which Logan can accept or override:**

Treat them as **two papers from the same cohort**, sequenced. Paper 1 (descriptive, the existing draft) goes through review first — it's the easier story and establishes the cohort. Paper 2 (definition, my planning doc) is more novel and benefits from the descriptive paper as a citable foundation. Both block on mig_252; Paper 1 also gets a procedure-type bump from mig_253.

If Logan wants only one paper, Paper 2 is the more contributory of the two — but it requires more analytical work and longer writing time.

---

## §6 — Workflow reminders (v21 §7 carried forward, with v22 additions)

- **Workspace path:** `/Users/loganglosser/THYROID_2026`
- **Master canonical pub V1.0 DB:** `thyroid_canonical_publication_v1_0` — confirm via `SELECT current_database()` before any DDL/DML.
- **Surgical git add per `feedback_surgical_git_add.md`**: explicit paths only (the repo has 100+ untracked files; never `git add -A`).
- **Always commit + push per `feedback_commit_workflow.md`**.
- **PHI safety per `feedback_phi_safety.md`**: research_id only.
- **Desktop Commander > bash sandbox per `feedback_use_desktop_commander_first.md`**: bash sandbox can't unlink `.git/index.lock` (FileVault).
- **DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ** per `reference_duckdb_timestamp_tz.md`: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols.
- **VIEW naming** per `reference_view_naming_convention.md`: any `main.*` or `manuscript_workspace.*` or `semantic_publication.*` VIEW must carry `_VIEW_v1` suffix.
- **research_id is VARCHAR everywhere in `semantic_publication.*`** (mig_239).
- **NEW (v22): Demographics + full column review for every manuscript** per `feedback_manuscript_demographics_and_full_column_review.md`. Every cohort view must include the demographic Table 1 set + a documented column-inventory pass against `canonical_patient_master`.

---

## §7 — Recent commit log (v22 era)

```
6fd1710  manuscript(M038): planning doc — RQ locked, paused on mig_252/253
55aade0  docs(qc): mig_252 + mig_253 dispatch prompts — comp rollup + surg procedure-type fixes
f673f09  feat(qc): mig_251 — cohort_m038_massive_goiter_v1 extension (24 -> ~95 cols)
89a505b  docs(feedback): standing rule — every manuscript needs demographics + full-dataset column review
2fc6fef  manuscript(M038): first draft — massive goiter composite-definition descriptive cohort      [parallel session]
37911d0  feat(qc): mig_249 — manuscript_feasibility_v1 re-refresh post-mig_248                       [Cursor Composer]
e821e97  docs(qc): v21 handoff — post-mig_250, M044 ETE work moves to ChatGPT lane
0ae2881  feat(qc): mig_250 — MD optimization, safe drops + cleanup report
f6b00a1  docs(qc): MD_MIGRATION_PLAN_v1 — sovereignty migration off MotherDuck
f13747a  docs(qc): mig_249 dispatch prompt — feasibility re-refresh post-mig_248
```

(Expected if Cursor Composer dispatches land: `feat(qc): mig_252 — comp_*_confirmed rollup repair` and `feat(qc): mig_253 — surg_procedure_type fill`.)

---

## §8 — Quick links

- [v22 handoff (this doc)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v22.md)
- [v21 handoff (predecessor)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v21.md)
- [Standing rule: demographics + column review](computer:///Users/loganglosser/THYROID_2026/memory/feedback_manuscript_demographics_and_full_column_review.md)
- [mig_251 SQL — M038 cohort extension](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/251_cohort_m038_extension_20260501.sql)
- [mig_252 dispatch prompt — comp rollup fix](computer:///Users/loganglosser/THYROID_2026/cursor_prompts/CURSOR_PROMPT_MIG_252_COMP_CONFIRMED_ROLLUP_FIX_20260501.md)
- [mig_253 dispatch prompt — surg procedure-type fill](computer:///Users/loganglosser/THYROID_2026/cursor_prompts/CURSOR_PROMPT_MIG_253_SURG_PROCEDURE_TYPE_FILL_20260501.md)
- [M038 planning doc (definition paper)](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M038_definition_paper_PLANNING_v1.md)
- [M038 first draft (descriptive paper, parallel session)](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v1.md)
- [M032 first draft (8 author-input gaps; complications section needs mig_252 rebuild)](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md)
- [Methods doc](computer:///Users/loganglosser/THYROID_2026/docs/Methods_thyroid_canonical_pub_v1_0_20260501.md)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

## §9 — Decision menu for the v22 chat

In recommendation order:

- **(A) Verify mig_252 + mig_253 status (Recommended first action).** Run §3 checklist. If both have landed, proceed to (B) or (C). If still in flight, ask Logan whether to wait or work on a non-blocked manuscript (e.g., M039 PTH/Calcium).
- **(B) Reconcile the two M038 framings.** Use AskUserQuestion to choose A/B/C from §3.6. Most likely outcome: keep both, sequence the descriptive paper first.
- **(C) Once mig_252 lands:** rebuild M038 outcome metrics with strict definition; rebuild M032 complications section. The mig_252 dispatch's §3.7 includes downstream-impact rebuild guidance.
- **(D) Once mig_253 lands:** verify M038 procedure-type NULL count drops in the cohort view; close `CF-M038-SURG-TYPE-NULL` permanently.
- **(E) Pick a non-blocked manuscript and start drafting.** Per v21 §5, candidates are M039 PTH/Calcium (READY_TO_DRAFT, never drafted), M025 TIRADS (post-mig_249 RED→YELLOW), or M046 NIFTP/M047 Frozen Section (CAVEATS_BUT_ACTIVE).

**Cowork's recommendation:** Open with §3 verification + §3.6 reconciliation as a single AskUserQuestion. Then pick (C)/(D) if mig_252/253 landed, or (E) if they haven't.

---

## §10 — Pre-flight reminder for the new chat agent

Before any analytical work:

1. **Run the §3 first-action checklist.** Confirm gate health, DB context, mig_252/253 status.
2. **Read both Cursor dispatch prompts in §2.** Even if mig_252/253 have landed, the prompts contain the spec that the corrected outcome and procedure-type fill should match. Verify post-landing state matches the acceptance criteria.
3. **If any new manuscript work is requested**, apply `feedback_manuscript_demographics_and_full_column_review.md` immediately — do the full-dataset column review pass before scoping the cohort view.
4. **Use `AskUserQuestion`** before substantial analytical choices. Never drift on cohort exclusions, primary outcome definition, statistical method, or table structure.
5. **Save manuscript drafts to `manuscript_outputs/v1_0_20260501/M0XX_*_DRAFT_v1.md`** following the file-naming convention.
6. **For git ops always use Desktop Commander** (`mcp__Desktop_Commander__start_process` + `interact_with_process`). The bash sandbox cannot remove `.git/index.lock` files due to FileVault.

---

**End of v22 handoff.** The new chat begins by reading this doc + reading both Cursor dispatch prompts in §2 + running the §3 first-action checklist. Most likely first action: verify mig_252/253 status, then reconcile the two M038 framings via AskUserQuestion. HARD: do not touch M044 / M051 (ChatGPT lane).
