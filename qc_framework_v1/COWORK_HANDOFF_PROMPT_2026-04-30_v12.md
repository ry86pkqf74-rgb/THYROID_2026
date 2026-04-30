# Cowork Handoff Prompt v12 — Thyroid Canonical Publication v1.0

**Generated:** 2026-04-30 (post v11 + verification-suite execution + ChatGPT review + 5-lane agent batch)
**Tip of `origin/main` at handoff:** `32fc584` (verify with `git fetch && git log --oneline -15`)
**Supersedes:** v11 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v11.md`
**Companion:** `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-30_v12.md` (full v12 round log)

---

## §0 First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md` end-to-end before any tool use. Then run the §3 first-action checklist (git fetch + log, run the v2 verification suite from `qc_framework_v1/queries/cowork_verification_suite_20260430.md`, list pending lanes from `cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md`).
>
> **Standing context:** I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're in the final ~3% cleanup of the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`). v12 round closed: **5-gate 186/0/0/0/0** post 4 Cowork-direct migs (207/208/209/210) + 5 agent-applied lanes (mig_211 A, mig_212 B, mig_213 C, mig_214 D, mig_215+216 E1/E2/E3). Manuscript readiness verdict: **READY**. You're the orchestrator + verifier + applier; agents do the bulk authoring; I'm the final ratifier.
>
> **You have:**
> - **Desktop Commander MCP** for git/shell on my Mac (FileVault — `.git/index.lock` cleanup may be needed; bash sandbox can't unlink)
> - **MotherDuck MCP** (read-only `query` + `query_rw`) — primary DB `thyroid_canonical_publication_v1_0`; archive DB `"Thyroid 2026 UPdated".archive_pub_v1_0`. Cowork's MCP is authed to `logan.glosser.eras@gmail.com`.
> - **GitHub repo** at `/Users/ros/THyroid 2026` (URL `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`)
> - **Auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
>
> **3 LANES PENDING** (prompts at `cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md`):
> 1. **Lane E continuation** (E4/E5/E6) — Cursor composer; mig labels `mig_219`/`mig_220`/`mig_221`. Build 4 TIRADS cohort views + resolve 2,640 high-pri TIRADS conflicts + clarify acr2017_feature_points_complete semantic.
> 2. **Lane F** — Cline GPT-5.5; mig label `mig_222`. Triage 448 multi-nodule under-explosion exams + 825 deferred LLM absorption patients.
> 3. **Lane G** — Cline GPT-5.5; mig label `mig_223`. Build `semantic_publication` schema + `release_manifest_v1` + 8 vw_*_safe_VIEW_v1 manuscript-safe views.
>
> **DEFERRED FUTURE TASKS** (not pending; trigger when stated condition is met):
> - **Future H** — `bi_powerbi.*` star-schema marts; trigger when Phase 4 Power BI Desktop migration starts
> - **Future I** — Parquet export of frozen tables; trigger after all current cleanup lanes finish
>
> **CRITICAL RIGOR REMINDER:** verify all agent work directly against MotherDuck. Pre-snapshot every mutating lane to `archive_pub_v1_0`. Verify col existence + dependent VIEW recompile before any ALTER. The verification suite v2 (`qc_framework_v1/queries/cowork_verification_suite_20260430.md`) is the SSOT post-lane.
>
> **First task:** §3 first-action checklist — confirm HEAD = `32fc584`, run 15-§ verification suite, then choose A/B/C from §6:
> - **(A)** Logan pasted an agent summary → verify per Path C (probe live MD for the agent's batch_id; verify acceptance criteria from prompt)
> - **(B)** Logan ratified a pending decision → author + apply final SQL Cowork-direct
> - **(C)** Pending lane (E continuation / F / G) needs queueing or follow-up

---

## §1 Project mission

**Logan Glosser**, Emory thyroid-cancer surgery researcher. Database: `thyroid_canonical_publication_v1_0` on MotherDuck (account `logan.glosser.eras@gmail.com`). Goal: manuscript-grade survival/recurrence/outcomes analyses on a single-institution thyroid cancer cohort.

- Cohort: **10,871 distinct research_id**
- Backbone: `canonical_patient_master` (1,630 cols; 1,606 v / 24 na)
- Tier-2 events / patient_rollup canonicals: 62/62 verified (100%)
- Plus 10 deferred analytic composites verified this v12 round (mig_211 Lane A)
- Plus dedup VIEW (`canonical_path_malignant_events_dedup_VIEW_v1` from mig_212 Lane B)
- Plus path_indeterminate landing table (mig_207)
- Authoritative SSOT: live MotherDuck — never trust prior summaries

---

## §2 v12 round delta from v11

| Metric | v11 baseline | v12 final | Delta |
|---|---|---|---|
| 5-gate gate1 (verified tables) | 174 | **186** | +12 |
| 5-gate gates 2-5 | 0/0/0/0 | **0/0/0/0** | unchanged |
| §12 governance gaps | 2 (path_indeterminate + val_mig180b) | **0** | -2 ✓ |
| §14 clinical date type violations | 30+ (mostly false positives) | **0** | cleaned ✓ |
| Hard data invariants (cohort/PM/etc.) | unchanged | unchanged | ✓ |
| Pre-1990 first_surgery_date | 1 (rid 610) | **0** | -1 ✓ |
| Closed CFs | — | 6 (CF-mig160b, CF-117/100-DATE-RETYPE, mig_127 deferred, ChatGPT-P1, ChatGPT-P7) | — |

**11 migrations landed:** mig_207, 208 (verify suite); mig_209, 210 (ChatGPT direct); mig_211 (A), 212 (B), 213 (C), 214 (D), 215+216 (E1/E2/E3)

---

## §3 First-action checklist

```
1. cd "/Users/ros/THyroid 2026" && git fetch origin && git log --oneline -15
   Expect tip = 32fc584 (or later if a Lane E continuation / F / G has landed)

2. Run the v2 5-gate audit (cowork_verification_suite_20260430.md §1):
   Expect: gate1 = 186 (or higher if new lanes landed); gates 2-5 = 0

3. Verify §12 (ungoverned tables) = 0 rows

4. Verify §14 v2 (canonical_*-scoped + extended allowlist) = 0 rows

5. ls cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md
   Confirm 3 pending lanes documented

6. Read latest closeout reports + memory entries:
   - memory/project_mig_207_208_closeout_2026-04-30.md
   - memory/project_chatgpt_review_followup_2026-04-30.md
   - qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-30_v12.md
```

---

## §4 Currently pending (in flight or ready to fire)

**3 lanes documented in `cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md`:**

### Lane E continuation (E4/E5/E6) — Cursor composer
- **E4** — Build 4 TIRADS cohort views in `manuscript_workspace`: `vw_us_nodule_tirads_strict_acr2017_VIEW_v1`, `vw_us_nodule_tirads_any_reported_VIEW_v1`, `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1`, `vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1`. Expected strict-cohort row count ≈ 5,149.
- **E5** — Resolve 2,640 high-priority TIRADS conflicts in `manuscript_workspace.us_nodule_conflict_queue_v1` (2,494 tirads_reported + 123 tirads_category_v2 + 23 tirads_score_2017).
- **E6** — Clarify why 21,454 rows have all 5 ACR point fields non-null but only 5,149 have `acr2017_feature_points_complete=TRUE` (4× gap). Document semantic.
- Mig labels: `mig_219` / `mig_220` / `mig_221`

### Lane F — Cline GPT-5.5
- Triage 448 multi-nodule under-explosion candidate exams (`manuscript_workspace.qc_tir03_llm_candidates_v1`) + 825 deferred LLM absorption patients (`manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1`)
- Decision per exam: absorb (if unambiguous) / document as limitation / escalate
- Mig label: `mig_222`

### Lane G — Cline GPT-5.5
- Build `semantic_publication` schema (does NOT yet exist — verified 2026-04-30)
- `release_manifest_v1` table (manuscript reproducibility)
- 8 manuscript-safe views: `vw_patient_master_safe_VIEW_v1`, `vw_path_malignant_tumor_safe_VIEW_v1`, `vw_recurrence_safe_VIEW_v1`, `vw_molecular_safe_VIEW_v1`, `vw_fna_safe_VIEW_v1`, `vw_us_nodule_safe_VIEW_v1`, `vw_labs_long_safe_VIEW_v1`, `vw_cohort_membership_safe_VIEW_v1`
- Mig label: `mig_223`

**Suggested execution:** E continuation + G in parallel (different tables); F after E continuation closes (both touch `canonical_us_nodule_v2`).

---

## §5 Future tasks (deferred — do not dispatch unless triggered)

| Task | Trigger | Agent (TBD) |
|---|---|---|
| **Future H** — `bi_powerbi.*` star-schema marts (13 dim/fact tables) | Phase 4 Power BI Desktop migration begins | TBD (likely Cursor composer; multi-day) |
| **Future I** — Parquet export of frozen tables to durable storage | All current cleanup lanes (E continuation + F + G) close clean | Cline Sonnet 4.6 (~1-2 hours) |

---

## §6 Decision menu (orient new chat to right action)

After §3 first-action checklist, choose:

- **(A)** Agent summary just arrived (Logan pasted) → run Path C verification: probe live MD for batch_id pre-and-post; verify acceptance criteria from the prompt; commit + push if clean
- **(B)** Logan ratified a pending decision (e.g., picked from E5 sample CSV, F absorption rule, G view design) → author + apply final SQL Cowork-direct following mig_205/mig_209 pattern with pre-snapshots
- **(C)** Pending lane queue management → re-confirm prompt content at `cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md`; surface to Logan if any lane prompt needs update

---

## §7 Path-C verification protocol (mandatory for agent-applied lanes)

For every agent summary Logan pastes:

1. **Probe live MD** for the agent's `batch_id`:
   ```sql
   SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE batch_id='<agent_batch_id>';
   SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 WHERE run_id='<agent_run_id>';
   ```
2. **Verify acceptance criteria** from the prompt are met (row counts, view existence, flag presence)
3. **Re-run 5-gate audit** — confirm no regression
4. **Re-run §12 (governance gap)** — should stay 0
5. **Re-run §14 v2 (clinical date type)** — should stay 0
6. **If clean**: commit Logan's local repo (`git add` per memory's surgical-paths rule, never `-A`) + push
7. **If issues**: surface to Logan with hypothesis + propose remediation mig

---

## §8 Repo + tooling reminders

- **Surgical git add per `feedback_surgical_git_add.md`**: never `git add -A` or directory-wide; explicit paths only
- **Always commit + push per `feedback_commit_workflow.md`**: stage → commit → push; lint Python first
- **PHI safety per `feedback_phi_safety.md`**: never print clinical notes; research_id only
- **Desktop Commander > bash sandbox per `feedback_use_desktop_commander_first.md`**: bash sandbox can't unlink `.git/index.lock` (FileVault); use Desktop Commander
- **DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ per `reference_duckdb_timestamp_tz.md`**: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols
- **No cross-DB canonical sourcing per `feedback_no_cross_db_canonical_sourcing.md`**: canonicals are standalone live objects in `main`; never `FROM archive_pub_v1_0.*`

---

## §9 Memory entry points (read in this order if new context needed)

1. `MEMORY.md` — index of all memories
2. `project_chatgpt_review_followup_2026-04-30.md` — most recent v12 round summary
3. `project_mig_207_208_closeout_2026-04-30.md` — verification suite §12+§14 closure
4. `project_2026-04-30_v11_round_complete.md` — prior round baseline
5. `feedback_*.md` — workflow rules (commit, surgical git add, PHI, etc.)
6. `reference_*.md` — environment + conventions (MD accounts, view naming, etc.)

---

## §10 Hand-off summary

**State at handoff:** v1.0 publication is manuscript-ready. 5-gate clean (186/0/0/0/0). All hard data invariants stable. 3 tighten-the-screws lanes pending — none block manuscript writing. 2 future tasks (Power BI marts + Parquet export) deferred until triggered.

**Logan's manuscript writing can proceed in parallel with the 3 pending agent lanes.**
