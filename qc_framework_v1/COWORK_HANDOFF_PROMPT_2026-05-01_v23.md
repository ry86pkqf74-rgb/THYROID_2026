# Cowork Handoff Prompt v23 — Post mig_252 + mig_253 Verified, M038 v2 Drafted

**Generated:** 2026-05-01 by Cowork at end of v22-era session (post-mig_252/253-landing + M038 v2 rebuild)
**Tip of `origin/main` at write:** `0143539` — `fix: fill CPM surgical procedure types`
**Supersedes:** v22 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v22.md`

**Headline:**

- **mig_252** (comp_*_confirmed rollup repair) and **mig_253** (surg_procedure_type fill) **landed and verified to acceptance**. Both fixes were authored as Cursor Composer dispatches in the v21→v22 session and applied in a parallel session. v22's open-flight items are closed.
- **M038 descriptive draft rebuilt as v2** (`manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v2_post_mig_252_253.md`) against the corrected complications rollup and post-mig_253 procedure-type completeness. v1 preserved unchanged for diff comparison.
- **M032 carry-forward closed as RESOLVED-NA** — investigation showed the M032 draft never contained a complications section or 23% rate; the carry-forward was based on an inaccurate read of the M032 source.

---

## §0 — First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v23.md` end-to-end before any tool use.
>
> **Standing context:** I'm Logan Glosser, Emory thyroid-cancer surgery researcher. The v22 → v23 session (2026-05-01) verified that mig_252 and mig_253 landed cleanly via Cursor Composer dispatch (acceptance criteria met), rebuilt the M038 descriptive manuscript into v2 against the corrected rollups + the standing demographics-Table-1 rule, and closed the M032 complications-rebuild carry-forward as not-applicable. The v23 chat starts with both upstream fixes verified and M038 v2 ready for PI review.
>
> **Tooling on this machine:**
> - **Desktop Commander MCP** for git/shell. Always use Desktop Commander rather than the bash sandbox for git ops — bash sandbox can't unlink `.git/index.lock` (FileVault). `feedback_use_desktop_commander_first.md`.
> - **MotherDuck MCP** authed to `logan.glosser.eras@gmail.com`. Master canonical pub V1.0 DB is `thyroid_canonical_publication_v1_0`.
> - **GitHub repo** at `/Users/loganglosser/THYROID_2026`. `origin/main` is canonical. Surgical git add per `feedback_surgical_git_add.md`.
>
> **Run the §3 first-action checklist before any new analytical work.**
>
> **HARD CONSTRAINT (carry-over from v21/v22):** I'm working on the **ETE manuscript (M044)** in ChatGPT. **Do NOT touch M044 or M051** here — they're owned by the ChatGPT lane. Note: a parallel session committed `a953ae1 manuscript(M044): ...` on `origin/main` during this round; Cowork did not touch it.

---

## §1 — Round delta v22 → v23

| Item | Lane | Commit | Outcome |
|---|---|---|---|
| **mig_252** (Cursor Composer) | comp_*_confirmed rollup repair | `32beb7b` | ✓ Landed in parallel session. Verified post-apply: any_confirmed_complication_flag 2,490→**400**; seroma 618→**39**; chyle_leak 1,576→**3**; rln_injury 690→**21**; hematoma 250→**68**; hypoparathyroidism 406→**296**. Strict definition (`finding_status='present' AND evidence_strength IN ('definitive','probable')`) confirmed. |
| **mig_253** (Cursor Composer) | surg_procedure_type fill | `0143539` | ✓ Landed in parallel session. Verified: NULL-all-three count 2,138→**2** (target ≤50, exceeded). M038 ≥200g focal cohort: 121→**0** NULL procedure-type. signoff_registry most-recent now `mig_253_surg_procedure_type_fill_20260501`. |
| **M038 descriptive draft v2** (Cowork-direct) | manuscript_outputs | TBD on this commit | ✓ `manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v2_post_mig_252_253.md`. 352 lines (vs v1 249 lines). Abstract/§3.4/§3.5 fully refreshed; §3.2 elevated to a proper Table 1 per standing rule; new Column Inventory section appended; §5 Limitations updated; author-input gap #6 closed by mig_253; new author-input gap #11 added (CIs on RR estimates). |
| **M044 work** (ChatGPT lane — flagged only) | parallel | `a953ae1` | Not touched by Cowork. Flagging for awareness only. |
| **CF-M032-COMPLICATIONS-REBUILD** investigation | audit | (no commit) | ✓ Closed as RESOLVED-NA. M032 draft contains no complication numbers; the 23% reference traced to lines 62/177 of `M038_definition_paper_PLANNING_v1.md`, not M032. M032 stands unchanged. |

---

## §2 — Verified post-fix state at v23 write

```sql
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
-- gate1=218, gate2..5=0, cohort_parity=TRUE
-- release_id='pub_v1_0_20260430'
-- most_recent_signoff_migration='qc_framework_v1/migrations/253_surg_procedure_type_fill_20260501.sql'
-- most_recent_signoff_ts=2026-05-01 06:41:00 UTC
```

**Cohort-wide complication confirmed-flag counts (post-mig_252):**

| Flag | Count | (was, pre-mig_252) |
|---|---:|---:|
| comp_seroma_confirmed | 39 | 618 |
| comp_chyle_leak_confirmed | 3 | 1,576 |
| comp_rln_injury_confirmed | 21 | 690 |
| comp_hematoma_confirmed | 68 | 250 |
| comp_hypoparathyroidism_confirmed | 296 | 406 |
| any_confirmed_complication_flag | 400 | 2,490 |

**Cohort-wide procedure-type completeness (post-mig_253):**

| Subset | NULL-all-three | (was, pre-mig_253) |
|---|---:|---:|
| Full CPM (n=10,871) | 2 | 2,138 |
| M038 ≥200g focal (n=475) | 0 | 121 |
| M038 composite-OR cohort (n=2,501) | 0 | 760 |

---

## §3 — First-action checklist for the new chat

### Step 3.1 — Confirm git state

```bash
cd /Users/loganglosser/THYROID_2026
git fetch origin
git log --oneline -10
```

Expect HEAD ≥ this round's commit (will include this v23 doc + M038 v2 file). If commits past this v23 commit exist from Cursor Composer or ChatGPT lane, surface them.

### Step 3.2 — Confirm DB context

```sql
SELECT current_database();
-- expected: thyroid_canonical_publication_v1_0
```

### Step 3.3 — Lakehouse health gate

```sql
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
-- gate1=218, gates 2-5=0, cohort_parity TRUE, most-recent signoff=mig_253
```

### Step 3.4 — Pick a manuscript

The M038 reconciliation is now resolved (sequenced two-paper plan, v2 done). Candidates ordered by readiness:

| Candidate | Status | Blockers |
|---|---|---|
| **M038-A descriptive (v2)** | Ready for PI review | None (post-mig_252/253) |
| **M038-B definition paper** | Planning doc only (`6fd1710`) | Decide: build out or wait until M038-A clears review |
| **M039 PTH/Calcium** | READY_TO_DRAFT (per v21 §5); never drafted | None known |
| **M025 TIRADS** | YELLOW post-mig_249 | Verify feasibility row freshness |
| **M046 NIFTP / M047 Frozen Section** | CAVEATS_BUT_ACTIVE | Verify feasibility row |

Recommended next step is **M038-B definition paper buildout** (the focal-cohort head-to-head exposure analysis described in `M038_definition_paper_PLANNING_v1.md`). The descriptive paper is now a citable foundation for it.

---

## §4 — Updated lakehouse state at v23 write

| Schema | Object count | Notes vs v22 |
|---|---:|---|
| `main` | 110 tables, 10 views | comp_*_confirmed and surg_procedure_type fixed via mig_252/253. |
| `manuscript_workspace` | 86 tables, 64 views | No new cohort-view changes this round. |
| `views_readable` | 0 tables, 62 views | Unchanged. |
| `semantic_publication` | 1 table, 16 views | Unchanged. Gate-1 verified at 218. |
| `raw` | 2 tables | Unchanged. |
| **Total** | **422+ objects** | |

**Open carry-forwards at v23 write:**

| ID | Description | Trigger to close |
|---|---|---|
| **CF-M038-A-PI-REVIEW** | M038 v2 descriptive draft awaits PI review | PI signs off |
| **CF-M038-B-DECISION** | Decide whether to build out the definition-paper companion | PI signal |
| **CF-M038-AUTHOR-INPUT-GAPS** | 10 author-input gaps in v2 (#6 closed, #11 added — CIs on RRs) | Logan fills inline |
| **CF-M044-CHATGPT-LANE** | M044 (AJCC ETE) being drafted in ChatGPT | Logan signals lane done |
| **CF-EMORY-IT-ENTITLEMENTS** | 5 questions for Emory IT in `MD_MIGRATION_PLAN_v1` §8 | Emory IT responds |
| **CF-MD-MIGRATION** | Move thyroid v1.0 publication out of MotherDuck to Emory-tenant target | Emory IT entitlement confirmed |
| **CF-METHODS-V17-V20-ADDENDUM** | Methods doc references mig_212-234 but not v17/v20/v21/v22/v23 | ~10 min Cowork edit |
| **CF-COMP-CONFIRMED-VARIANTS-AUDIT** | Verify whether mig_252 also corrected the `_definitive`/`_probable_or_better`/`_any_evidence`/`_suspected` family-flag variants on CPM | Run the §3.3 audit from mig_252 dispatch against the post-apply state |
| **CF-COHORT-VIEW-DUPLICATE-COLUMNS** | `cohort_m032_descriptive_25yr_v1` reports duplicate column names (`any_confirmed_complication_flag`, `comp_*_confirmed` each appear twice) — likely a JOIN-side artifact in the view definition | Audit the cohort_m032 view DDL; possibly true for other cohort views |
| **CF-SURG-RESIDUAL-CHART-REVIEW** | Post-mig_253 residual NULL-procedure-type pts (n=2 cohort-wide; ≤50 acceptance) | Chart review queue |
| **CF-SURG-CPT-VOCAB-REGISTRY** | Canonicalize CPT → procedure-type mapping into a registry table | Future small lane |
| **CF-PARATHYROID-EVENT-SAFE** | Events-grain `intact_pth_value_ngL` safe view (deferred from mig_243) | If M039 needs per-event PTH |
| **CF-LN-METS-ARRAY-EMPTY-2801** | 2,801 LN-positive cases lack histology-attribution evidence | Methods caveat only |
| **Future-Gate6-Col-Registry** | Add gate6 to `qc_audit_dashboard_VIEW_v1` for col_registry dup-key detection | Small Cowork lane if greenlit |

**Closed in this v22→v23 round:**

- ~~CF-COMP-CONFIRMED-ROLLUP-FIX~~ (mig_252 landed at `32beb7b`)
- ~~CF-SURG-PROC-TYPE-FILL~~ (mig_253 landed at `0143539`)
- ~~CF-M038-PAUSED-ON-MIG_252~~ (M038 v2 drafted post-mig_252)
- ~~CF-M038-RECONCILIATION~~ (sequenced two-paper plan; v2 descriptive done; M038-B planning doc remains)
- ~~CF-M038-SURG-TYPE-NULL~~ (closed at v22; mig_253 confirmed acceptance)
- ~~CF-MIG_252-DOWNSTREAM~~ (M038 outcome metrics rebuilt in v2 draft)
- ~~CF-M032-COMPLICATIONS-REBUILD~~ (RESOLVED-NA — M032 never contained a complications section)

---

## §5 — Workflow reminders (v22 §6 carried forward)

- **Workspace path:** `/Users/loganglosser/THYROID_2026`
- **Master canonical pub V1.0 DB:** `thyroid_canonical_publication_v1_0` — confirm via `SELECT current_database()` before any DDL/DML.
- **Surgical git add per `feedback_surgical_git_add.md`**: explicit paths only.
- **Always commit + push per `feedback_commit_workflow.md`**.
- **PHI safety per `feedback_phi_safety.md`**: research_id only.
- **Desktop Commander > bash sandbox per `feedback_use_desktop_commander_first.md`**: bash sandbox can't unlink `.git/index.lock`.
- **DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ** per `reference_duckdb_timestamp_tz.md`.
- **VIEW naming** per `reference_view_naming_convention.md`: any `main.*` or `manuscript_workspace.*` or `semantic_publication.*` VIEW must carry `_VIEW_v1` suffix.
- **research_id is VARCHAR everywhere in `semantic_publication.*`** (mig_239).
- **Demographics + full column review for every manuscript** per `feedback_manuscript_demographics_and_full_column_review.md` — applied in this round to the M038 v2 draft (Table 1 expansion, Column Inventory section).

---

## §6 — Recent commit log (v22→v23 era)

```
[NEW v23]   docs(qc): v23 handoff + M038 v2 draft post-mig_252/253
0143539     fix: fill CPM surgical procedure types                          [mig_253, parallel session]
a953ae1     manuscript(M044): fit primary multivariable models, ...         [ChatGPT lane, parallel session]
32beb7b     fix(mig252): repair CPM complication confirmed rollups         [mig_252, parallel session]
58cfd19     docs(qc): v22 handoff — post M038 audit, mig_252+253 dispatched
6fd1710     manuscript(M038): planning doc — RQ locked, paused on mig_252/253
55aade0     docs(qc): mig_252 + mig_253 dispatch prompts
f673f09     feat(qc): mig_251 — cohort_m038_massive_goiter_v1 extension
89a505b     docs(feedback): standing rule — every manuscript needs demographics + full-dataset column review
2fc6fef     manuscript(M038): first draft — massive goiter composite-definition descriptive cohort  [parallel]
37911d0     feat(qc): mig_249 — manuscript_feasibility_v1 re-refresh post-mig_248
e821e97     docs(qc): v21 handoff — post-mig_250, M044 ETE work moves to ChatGPT lane
```

---

## §7 — Quick links

- [v23 handoff (this doc)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v23.md)
- [v22 handoff (predecessor)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v22.md)
- [M038 v2 descriptive draft (post-mig_252/253)](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v2_post_mig_252_253.md)
- [M038 v1 descriptive draft (preserved for diff)](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v1.md)
- [M038-B definition-paper planning doc](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M038_definition_paper_PLANNING_v1.md)
- [M032 25-yr descriptive draft (unchanged this round)](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md)
- [Standing rule: demographics + column review](computer:///Users/loganglosser/THYROID_2026/memory/feedback_manuscript_demographics_and_full_column_review.md)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

## §8 — Decision menu for the v24 chat

In recommendation order:

- **(A) Build out M038-B definition paper (Recommended).** The descriptive paper (M038 v2) now serves as a citable foundation. M038-B's planning doc at `M038_definition_paper_PLANNING_v1.md` defines three exposure operationalizations head-to-head; the focal ≥200g cohort has 10 strict-definition complication events (post-mig_252) which will require careful interaction-model power discussion. Apply the standing demographics + column-review rule to the M038-B cohort scope.
- **(B) M039 PTH/Calcium.** READY_TO_DRAFT, never drafted. Apply standing rule + check for parathyroid event-grain safe view (CF-PARATHYROID-EVENT-SAFE).
- **(C) Address CF-COMP-CONFIRMED-VARIANTS-AUDIT** by running the mig_252 §3.3 audit against the post-apply state. ~15 min. Confirms that the `_definitive` / `_probable_or_better` / `_any_evidence` family flags are also strict-rolled.
- **(D) Address CF-COHORT-VIEW-DUPLICATE-COLUMNS** by auditing the cohort-view DDLs. ~30 min. May affect any manuscript that selects `*` from a cohort view.
- **(E) Methods doc addendum.** Roll v17/v20/v21/v22/v23 round notes into `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`. ~10 min.

**Cowork's recommendation:** (A) is the natural next analytical step. (C) and (D) are housekeeping that can be done opportunistically.

---

## §9 — Pre-flight reminder for the new chat agent

Before any analytical work:

1. **Run the §3 first-action checklist.** Confirm gate health, DB context, HEAD position.
2. **Read the M038 v2 draft** if working on M038-B; it documents the strict-definition complication outcomes that M038-B will reanalyze with the head-to-head exposure operationalizations.
3. **Apply `feedback_manuscript_demographics_and_full_column_review.md`** for any new manuscript work.
4. **Use `AskUserQuestion`** before substantial analytical choices.
5. **For git ops always use Desktop Commander.** Bash sandbox cannot remove `.git/index.lock` files due to FileVault.

---

**End of v23 handoff.** Most likely first action: M038-B definition-paper buildout (or any of B–E from §8 per Logan's preference). HARD: do not touch M044 / M051 (ChatGPT lane).
