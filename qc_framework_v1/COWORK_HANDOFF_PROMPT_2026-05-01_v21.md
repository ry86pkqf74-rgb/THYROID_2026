# Cowork Handoff Prompt v21 — Thyroid Canonical Publication v1.0

**Generated:** 2026-05-01 by Cowork at end of v20-era session
**Tip of `origin/main` at write:** `0ae2881` — `feat(qc): mig_250 — MD optimization, safe drops + cleanup report`
**Supersedes:** v20 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v20.md`
**New strategic doc:** `qc_framework_v1/MD_MIGRATION_PLAN_v1_20260501.md` (sovereignty migration off MotherDuck)

---

## §0 — First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v21.md` end-to-end before any tool use.
>
> **Then run the §3 first-action checklist:**
> 1. `git fetch origin && git log --oneline -15` from `/Users/loganglosser/THYROID_2026` — confirm HEAD ≥ `0ae2881`. Check whether `mig_249` (feasibility re-refresh) landed from Cursor Composer.
> 2. `SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;` — expect gate1=218, gates 2-5=0, cohort_parity TRUE (10871×3).
> 3. Reconcile `manuscript_workspace.manuscript_dashboard_VIEW_v1` against §4. If mig_249 landed, READY_TO_DRAFT bucket should have grown from 3 to ~6-9 (M025/M029/M030/M037/M043/M045 likely restored).
> 4. Check `manuscript_feasibility_v1.canonical_version_at_scoring` — should be `'v1_0_post_mig_248'` if mig_249 landed.
>
> **Standing context:** I'm Logan Glosser, Emory thyroid-cancer surgery researcher. Previous chat closed out the v20 §3 checklist (mig_248 verified clean), dispatched **mig_249** (feasibility re-refresh) to Cursor Composer, authored **`MD_MIGRATION_PLAN_v1`** (sovereignty migration plan — moving off MotherDuck to Emory-tenant storage), and executed **mig_250** (MD optimization — 32 safe drops, all 5 gates unchanged). The full `archive_pub_v1_0` schema is gone; total objects 454 → 422.
>
> **HARD CONSTRAINT for this chat:** I'm working on the **ETE manuscript (M044)** in ChatGPT. **Do NOT touch M044 or M051** here — they're owned by ChatGPT lane. Work on a different manuscript.
>
> **You have:** Desktop Commander MCP for git/shell; MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; GitHub at `/Users/loganglosser/THYROID_2026`. Use Desktop Commander for git ops.
>
> **Most likely first task:** Start **M039 PTH/Calcium Protocol Post-Thyroidectomy** first draft (cohort_n=4,561; READY_TO_DRAFT per v20 §4; hasn't been drafted yet). Mirror the M032 first-draft pattern (no new MD analyses required for the first pass). Fallback options in §5.

---

## §1 — Round delta v20 → v21 (what landed in the previous Cowork chat)

| Mig / Doc | Lane | Commit | Outcome |
|---|---|---|---|
| **v20 §3 verification** | QC checklist | (Cowork-direct) | ✓ mig_248 verified clean: all 5 gates pass, all 63 cohort views queryable, cohort_parity TRUE. Found mig_248 was MORE comprehensive than v20 §1 implied (added backward-compat aliases for `tumor_size_cm`, `tirads_best_category_v12`, etc. in `cohort_descriptive_full_cohort_v1`). |
| **mig_249 dispatch prompt** | Lane Q (Cursor Composer dispatch) | `f13747a` | ✓ Cursor Composer prompt for feasibility re-refresh authored. Closes carry-forward CF-MIG_247-RERUN. Predicts M025/M029/M030/M037/M043/M045 flip RED→GREEN. **In flight at v21 write — may have landed by the time you read this.** |
| **MD_MIGRATION_PLAN_v1** | Strategic doc | `f6b00a1` | ✓ Comprehensive plan for sovereignty migration off MotherDuck. Logan picked: hybrid (Fabric Lakehouse + OneDrive parquet) initially, with Emory AWS S3 admitted as cheaper alternative if entitled. Two-tier PHI handling (scrubbed primary + restricted PHI store). Cowork recommends AWS S3 + DuckDB-on-parquet primary if Emory entitlement exists. |
| **mig_250 + cleanup report** | MD optimization | `0ae2881` | ✓ 32 objects dropped (entire `archive_pub_v1_0` schema + 3 val_mig + 11 manuscript_workspace prestate/scratch). Gate health unchanged. 5 empty governance-registered tables held back (would require coordinated signoff-registry update). 2 load-bearing tables held back (`cupm_v2_canonical_backfill_v1`, `biochemical_concern_backfill_v1`). |

**Lakehouse health unchanged at handoff write:** gate1=218, gates 2–5 = 0, cohort_parity TRUE (10871×3), zero broken views.

**Object inventory after mig_250:**
- Total: **422** objects (was 454)
- Tables: **196** (was 228)
- Views: 226 (unchanged)
- Schemas: **5** (was 6 — `archive_pub_v1_0` removed)
- `main` tables: 110 (was 113)
- `manuscript_workspace` tables: 85 (was 94)

---

## §2 — ETE manuscript hands-off (HARD CONSTRAINT)

Logan is drafting the ETE manuscript in **ChatGPT**, not Cowork. The two ETE candidates surfaced from the registry are:

| ID | Title | n | Status | **Cowork action** |
|---|---|---:|---|---|
| **M044** | AJCC 8th Ed: Microscopic vs Gross ETE | 4,128 | Proposed (High) | **DO NOT TOUCH** — owned by ChatGPT lane |
| **M051** | ETE and lymph node involvement significance | 8,733 | Idea (Medium) | **DO NOT TOUCH** — companion to M044 in ChatGPT lane |

If Logan asks to "switch back to ETE work in Cowork", confirm explicitly first that he's leaving the ChatGPT lane — there's a coordination risk if both lanes touch the same cohort views simultaneously.

---

## §3 — First-action checklist for new chat

### Step 3.1 — Confirm git state via Desktop Commander
```bash
cd "/Users/loganglosser/THYROID_2026"
git fetch origin
git log --oneline -15
git status --porcelain
```
Expect HEAD ≥ `0ae2881`. If `mig_249` commit appears past that, Cursor Composer landed it. If not, the agent is still running — surface to Logan and either wait or check Cursor's status.

### Step 3.2 — One-query lakehouse health
```sql
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
```
**Required baseline:** gate1=218, gates 2-5=0, cohort_parity TRUE (10871×3). mig_249 (if landed) should NOT change gate1 — it's a `manuscript_feasibility_v1` UPDATE, not a registry change.

### Step 3.3 — Did mig_249 land? (feasibility re-refresh status)
```sql
SELECT canonical_version_at_scoring,
       MAX(scored_at) AS last_scored_at,
       COUNT(*) AS rows_with_this_version
FROM manuscript_workspace.manuscript_feasibility_v1
GROUP BY canonical_version_at_scoring
ORDER BY last_scored_at DESC;
```
- If `'v1_0_post_mig_248'` is the only/dominant version → mig_249 landed.
- If still `'v1_0_post_mig_246'` → still in flight or stalled. Check Cursor Composer.

### Step 3.4 — Dashboard distribution check
```sql
SELECT draft_readiness_signal, COUNT(*) AS n,
       STRING_AGG(CAST(manuscript_id AS VARCHAR), ',' ORDER BY manuscript_id) AS ids
FROM manuscript_workspace.manuscript_dashboard_VIEW_v1
GROUP BY draft_readiness_signal
ORDER BY draft_readiness_signal;
```
**Pre-mig_249 baseline (v20 end-state):**
- READY_TO_DRAFT: 3 (M032, M038, M039)
- CAVEATS_BUT_ACTIVE: 3 (M044, M046, M047)
- GREEN_BUT_IDEA_STAGE: 24
- YELLOW_IDEA: 2
- DEFERRED: 31

**Expected post-mig_249:**
- READY_TO_DRAFT: ~6-9 (gain M025/M029/M030/M037/M043/M045 if their RED was driven by column-rename hints, which mig_248 invalidated)
- DEFERRED should shrink correspondingly

### Step 3.5 — Reconcile against §4 manuscript queue and proceed

---

## §4 — Manuscript priority queue (v21 snapshot)

### NOT AVAILABLE (ChatGPT lane — do not touch)
- M044 — AJCC 8th Ed: Micro vs Gross ETE
- M051 — ETE and LN involvement

### READY_TO_DRAFT in Cowork lane (pre-mig_249 confirmed; post-mig_249 likely expansion)

| ID | Title | n | Status | Notes |
|---|---|---:|---|---|
| **M032** | 25-Year Descriptive Analysis | 10,871 | In Progress | First draft pushed at `f9f848c`. **8 author-input gaps remain** in `manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md` — finish-the-paper task |
| **M038** | Massive Goiter Surgery (ECMO support) | 10,871 | Ready to Submit | **Needs RQ definition** before drafting (volume? tracheal extension? complication rates?) |
| **M039** | PTH/Calcium Protocol Post-Thyroidectomy | 4,561 | In Progress | **NEW post-v17 — never drafted.** Strong candidate for "next manuscript" task |

### Likely post-mig_249 restoration set (will become READY_TO_DRAFT after re-score lands)

| ID | Title | n | Pre-mig_249 RED reason | Likely status post-mig_249 |
|---|---|---:|---|---|
| **M025** | TIRADS Performance | 3,375 | tirads_best_category_v12 MISSING | GREEN (alias resolves) |
| **M029** | FNA Concordance | 2,401 | tumor_size_cm MISSING | GREEN (alias resolves) |
| **M030** | Genetic Predictive | 1,286 | tirads_best_category_v12 MISSING | GREEN (alias resolves) |
| **M037** | LN Metastasis | 2,271 | tumor_size_cm MISSING | GREEN (alias resolves) |
| **M043** | LN Predictors | 4,137 | tumor_size_cm MISSING | GREEN (alias resolves) |
| **M045** | Multimodal Risk | 1,165 | both above MISSING | GREEN (both resolve) |

### Carrying forward (CAVEATS_BUT_ACTIVE — close-to-ready)
- **M046** NIFTP Era Bethesda — n=5,026
- **M047** Frozen Section — n=10,871

### Always-deferred (real coverage shortfalls or external data, won't return)
- M001, M005, M010, M015, M021, M022, M026, M027, M031, M033, M034, M035, M036, M040, M041, M042, M067, M072, M073, M074, M075, M077, M078, M080, M081, M082, M083

---

## §5 — Decision menu for next chat (Logan asked for "different manuscript than ETE")

In recommendation order:

- **(A) M039 PTH/Calcium Protocol first draft (Recommended)** — cohort_n=4,561; READY_TO_DRAFT; never drafted. Mirror the M032 first-draft path (no new MD analyses needed). Cowork-direct lane. Estimated: 1-2 hours of drafting + MD reads. **Strongest "start writing the next manuscript" candidate.**
- **(B) M038 Massive Goiter / ECMO RQ definition + first draft** — needs you to pick the specific outcome first (ECMO-supported volume? tracheal extension? operative complications?). Once defined, drafting is ~2 hours. Use AskUserQuestion for the RQ pick at chat start.
- **(C) M032 author-input gap fills** — already drafted; 8 gaps need your text. Faster path to "submission-ready" but it's "finishing", not "starting next". Best done in collaboration mode where you respond inline.
- **(D) Wait for mig_249, then pick from the restoration set** — if mig_249 landed and M025/M029/M030/M037/M043/M045 are now READY_TO_DRAFT, pick one of those (M025 TIRADS Performance is "Ready to Submit" status — high-priority once unblocked).
- **(E) Methods doc v17/v20 addendum** — small Cowork-direct edit. Documents v17 closeout + mig_245-250 + future migration. ~10 min.

**Cowork's recommendation:** Start with (A) M039 first draft. It's the clearest "new manuscript" task, has clean cohort, no RQ ambiguity, and proves the M032-pattern for additional manuscripts.

---

## §6 — MotherDuck migration follow-ups (carry-forward from this chat)

Logan owns these; Cowork can help once entitlements are confirmed.

### Open Emory IT questions (from `MD_MIGRATION_PLAN_v1` §8)
1. **AWS Research Computing entitlement?** (S3 + BAA bucket access). If YES → cheapest path ($1-15/mo).
2. **Microsoft Fabric / Power BI Premium tenant entitlement?** (F-SKU). If YES → enables hybrid Option C.
3. **PHI handling rules** — for `clinical_notes_long` (11,050 rows) + `note_entities_*` (~25 tables): access control model? audit logging requirements?
4. **Researcher tooling permissions** — DuckDB CLI? AWS CLI? Power BI Desktop? Cursor?
5. **HIPAA tier** — does PHI need to stay on Emory-owned hardware (rules out cloud)?

### Migration-related Cowork lanes (deferred until target chosen)
- **mig_251 (optional governance):** coordinated drop + signoff-registry update for the 5 empty governance-registered tables (would shift gate1 218 → 213).
- **mig_252 (parquet refresh):** refresh `parquet_export/pub_v1_0_20260430/` to current state for use as migration payload.
- **Methods doc cutover addendum:** when migration cuts over.

### Cost-aware reminder
Dataset is small (~5 GB on disk after mig_250). Emory AWS S3 is the cheapest viable target if entitled. Fabric F2 SKU floor is $262/mo — only worth it if Power BI dashboards become a primary deliverable.

---

## §7 — Repo + tooling reminders (unchanged from v19/v20)

- **Workspace path:** `/Users/loganglosser/THYROID_2026`
- **Surgical git add per `feedback_surgical_git_add.md`**: explicit paths only (the repo has 100+ untracked files; never `git add -A`)
- **Always commit + push per `feedback_commit_workflow.md`**
- **PHI safety per `feedback_phi_safety.md`**: research_id only
- **Desktop Commander > bash sandbox per `feedback_use_desktop_commander_first.md`**: bash sandbox can't unlink `.git/index.lock` (FileVault)
- **DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ** per `reference_duckdb_timestamp_tz.md`: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols
- **VIEW naming** per `reference_view_naming_convention.md`: any `main.*` or `manuscript_workspace.*` or `semantic_publication.*` VIEW must carry `_VIEW_v1` suffix
- **research_id is VARCHAR everywhere in `semantic_publication.*`** (mig_239)

---

## §8 — Reference object inventory (v21 baseline, post-mig_250)

### `semantic_publication` schema (publication-tier analyst SSOT) — UNCHANGED from v20
- `release_manifest_v1` — BASE TABLE, 1 row (`pub_v1_0_20260430`)
- 16 safe views including the v17 additions (us_exam, frozen, 3 LN, snake_case_aliases, patient_domain_wide)

### `manuscript_workspace` schema — UPDATED post-v20
- `manuscript_dashboard_VIEW_v1` (mig_246), `manuscript_feasibility_v1` (mig_247; **may be re-refreshed post-mig_249 by chat start**)
- All 63 cohort views queryable post-mig_248
- `dive_cohort_size_v1` materialized count snapshot (built by mig_248)
- Net tables: **85** (was 94 — mig_250 dropped 11)

### `views_readable` schema — UNCHANGED
- 62 views, all dependent only on `main.*` (zero internal references — by design as user-facing presentation tier)

### `main` schema — UPDATED post-v20
- 110 tables (was 113 — mig_250 dropped 3 val_mig)
- 10 views unchanged
- `canonical_table_signoff_registry_v1` (218 verified rows — UNCHANGED post-mig_250)

### `archive_pub_v1_0` schema — REMOVED in mig_250

### `raw` schema — 2 tables, untouched

---

## §9 — Recent commit log (v21 era)

```
0ae2881  feat(qc): mig_250 — MD optimization, safe drops + cleanup report
f6b00a1  docs(qc): MD_MIGRATION_PLAN_v1 — sovereignty migration off MotherDuck
f13747a  docs(qc): mig_249 dispatch prompt — feasibility re-refresh post-mig_248
78b1ae3  feat(qc): mig_248 — column-rename drift repair across cohort views   [Cursor Composer]
8654a83  docs(qc): v20 handoff — comprehensive session summary
f9f848c  manuscript(M032): first draft — 25-year descriptive cohort paper
80b3c43  feat(qc): mig_247 — manuscript_feasibility_v1 refresh post-v17 schema [Cursor Composer]
a831828  docs(qc): cursor prompts for mig_247 + mig_248
5bbcee0  feat(qc): mig_246 — manuscript_workspace.manuscript_dashboard_VIEW_v1
96e8ce3  feat(qc): mig_245 — stale view reference repair (8 views unbroken)
e4254fe  docs(qc): v17 round closeout + v19 handoff
```

(Expected if mig_249 lands: `feat(qc): mig_249 — manuscript_feasibility_v1 re-refresh post-mig_248` from Cursor Composer.)

---

## §10 — Quick links

- [v21 handoff (this doc)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v21.md)
- [v20 handoff (predecessor)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v20.md)
- [MD_MIGRATION_PLAN_v1](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/MD_MIGRATION_PLAN_v1_20260501.md)
- [MD_CLEANUP_REPORT_20260501](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/MD_CLEANUP_REPORT_20260501.md)
- [mig_250 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/250_md_optimization_safe_drops_20260501.sql)
- [mig_249 dispatch prompt (Cursor Composer)](computer:///Users/loganglosser/THYROID_2026/cursor_prompts/CURSOR_PROMPT_MIG_249_FEASIBILITY_RE_REFRESH_20260501.md)
- [mig_248 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/248_column_rename_drift_repair_20260501.sql)
- [M032 first draft (8 author-input gaps)](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md)
- [Lane M Methods doc](computer:///Users/loganglosser/THYROID_2026/docs/Methods_thyroid_canonical_pub_v1_0_20260501.md)
- [Manuscript outputs folder](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

## §11 — Open carry-forwards

| ID | Description | Status | Trigger to close |
|---|---|---|---|
| **CF-M044-CHATGPT-LANE** | M044 (AJCC ETE) being drafted in ChatGPT — Cowork hands-off | Active in ChatGPT | When Logan signals lane is done |
| **CF-MIG_249-IN-FLIGHT** | Cursor Composer running mig_249 (feasibility re-refresh) at v21 write | In flight | Verify via §3.3 at chat start |
| **CF-M039-DRAFT** | M039 (PTH/Calcium) — recommended next manuscript for Cowork lane | Open — primary recommendation for next chat | Logan picks (A) in §5 |
| **CF-M038-RQ** | M038 (Massive Goiter / ECMO) needs RQ definition before drafting | Open | Logan defines RQ |
| **CF-M032-AUTHOR-GAPS** | M032 first draft has 8 author-input gaps | Open | Logan fills inline |
| **CF-MIG_251-OPTIONAL** | Coordinated drop of 5 empty governance-registered tables (would shift gate1 218→213) | Optional | Only if you want gate1 reduced |
| **CF-EMORY-IT-ENTITLEMENTS** | 5 questions for Emory IT in MD_MIGRATION_PLAN_v1 §8 | Open — Logan owns | When Emory IT responds |
| **CF-MD-MIGRATION** | Move thyroid v1.0 publication out of MotherDuck to Emory-tenant target | Plan written, gated on §8 | When Emory IT entitlement confirmed |
| **CF-METHODS-V17-V20-ADDENDUM** | Methods doc references mig_212-234 but not v17/v20/v21 round | Open — quick Cowork edit | ~10 min lane |
| **CF-MIG_252-PARQUET-REFRESH** | Refresh `parquet_export/pub_v1_0_20260501/` to current post-mig_250 state | Open suggestion | When migration target chosen |
| **CF-PARATHYROID-EVENT-SAFE** | Events-grain `intact_pth_value_ngL` safe view (deferred from mig_243) | Open suggestion | If M039 needs per-event PTH — likely YES |
| **CF-LN-METS-ARRAY-EMPTY-2801** | 2,801 LN-positive cases lack histology-attribution evidence | Methods caveat only | Chart-review remediation |
| **Future-Gate6-Col-Registry** | Add gate6 to `qc_audit_dashboard_VIEW_v1` for col_registry dup-key detection | Open suggestion | Small Cowork lane if greenlit |
| **Future-H-Power-BI** | `bi_powerbi.*` star-schema marts | Deferred — superseded by MD_MIGRATION_PLAN_v1 | When migration target chosen |

---

## §12 — Architectural decisions made in v20-era chat (post-v20 write)

1. **Migration target: hybrid (Fabric+OneDrive) or AWS S3 if Emory-entitled.** Logan's decision 2026-05-01. Cowork recommends AWS S3 + DuckDB-on-parquet primary based on dataset size (~5 GB), read-mostly access pattern, and Cursor+DuckDB analytics workflow.
2. **PHI scope: two-tier.** `semantic_publication.*` to broadly-accessible target; `clinical_notes_long` + `note_entities_*` to restricted store with audit logging.
3. **MD optimization done before migration export.** mig_250 dropped 32 objects (the entire `archive_pub_v1_0` schema + scaffolds). 5 empty governance-registered tables held back.
4. **mig_248 was more comprehensive than v20 §1 implied.** It added backward-compat aliases (`tumor_size_cm`, `tirads_best_category_v12`, etc.) on the parent `cohort_descriptive_full_cohort_v1` view, which propagate to all child cohort views via SELECT inheritance. Cleaner fix than expected.
5. **ETE manuscript work moved to ChatGPT lane.** Cowork hands-off for M044 + M051 until Logan signals otherwise.
6. **Lane M next-up: M039 PTH/Calcium Protocol first draft.** Strongest "new manuscript" candidate given M032 already drafted, M038 needs RQ, and ETE is in ChatGPT.

---

## §13 — Pre-flight reminder for the new chat agent

Before you start drafting M039 (or whichever manuscript Logan picks):
1. **Run the §3 first-action checklist.** Confirm gate health + mig_249 status.
2. **Check `manuscript_workspace.cohort_m039_pth_calcium_v1`** — count, columns, sample row. This is your read-source.
3. **Look at `manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md`** as the template/style guide.
4. **Look at `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`** for the standard Methods boilerplate.
5. **Use `AskUserQuestion`** before any substantial analytical choice (cohort exclusions, primary outcome definition, statistical method, table structure).
6. **Save manuscript drafts to `manuscript_outputs/v1_0_20260501/M039_pth_calcium_DRAFT_v1.md`** following the file-naming convention.

---

**End of v21 handoff. The new chat begins by reading this doc + running §3 first-action checklist. Most likely first work: M039 PTH/Calcium Protocol first draft (Cowork lane). HARD: do not touch M044/M051 (ChatGPT lane).**
