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
> **Standing context:** I'm Logan Glosser, Emory thyroid-cancer surgery researcher. The v22 → v23 session (2026-05-01) verified that mig_252 and mig_253 landed cleanly via Cursor Composer dispatch (acceptance criteria met), rebuilt the M038 descriptive manuscript into v2 against the corrected rollups + the standing demographics-Table-1 rule, and closed the M032 complications-rebuild carry-forward as not-applicable. The v23 chat starts with both upstream fixes verified and M038 v2 ready for **data-validity audit + Excel deliverable build**.
>
> **Your two-step assignment for this chat (do these in order, before any other work):**
>
> 1. **Verify the M038 v2 descriptive manuscript's data validity, accuracy, and logic for every single data point.** Read `manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v2_post_mig_252_253.md` end-to-end, then for each numeric cell, percentage, count, denominator, ratio, and derived statistic in the abstract, results, and tables: re-run the underlying SQL against `thyroid_canonical_publication_v1_0` and confirm the manuscript number matches. Flag any discrepancy, any percentage that doesn't sum to 100% within rounding, any inclusion-exclusion check that doesn't reconcile, any RR computation error, and any number cited in the abstract that isn't reproduced in the body. Produce an audit report at `manuscript_outputs/v1_0_20260501/M038_v2_DATA_VALIDITY_AUDIT_20260501.md` showing every data point, the executable SQL that reproduces it, the live result, and a PASS / DIFF / FAIL flag.
> 2. **Then build the Excel deliverable** at `manuscript_outputs/v1_0_20260501/M038_v2_DATA_AND_SOURCES.xlsx` containing every data point used in the manuscript across structured tabs, plus the executable source SQL for each table as a separate "source" tab so the manuscript is fully reproducible from the spreadsheet alone. Use the xlsx skill (`/var/folders/.../skills/xlsx/SKILL.md`).
>
> Only after these two steps clear should you propose moving on to M038-B or any other manuscript work.
>
> **Tooling on this machine:**
> - **Desktop Commander MCP** for git/shell. Always use Desktop Commander rather than the bash sandbox for git ops — bash sandbox can't unlink `.git/index.lock` (FileVault). `feedback_use_desktop_commander_first.md`.
> - **MotherDuck MCP** authed to `logan.glosser.eras@gmail.com`. Master canonical pub V1.0 DB is `thyroid_canonical_publication_v1_0`.
> - **GitHub repo** at `/Users/loganglosser/THYROID_2026`. `origin/main` is canonical. Surgical git add per `feedback_surgical_git_add.md`.
>
> **Run the §3 first-action checklist (gates + DB context) before starting the audit.**
>
> **HARD CONSTRAINT (carry-over from v21/v22):** I'm working on the **ETE manuscript (M044)** in ChatGPT. **Do NOT touch M044 or M051** here — they're owned by the ChatGPT lane. Note: parallel sessions committed `a953ae1`, `e17d62b`, `7a21306` on `origin/main` during the v22→v23 round; Cowork did not touch them.

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

### Step 3.4 — Required first work: M038 v2 data-validity audit + Excel deliverable

Before any other manuscript work, complete the two-step assignment from §0:

**Step 3.4.1 — Data-validity audit of M038 v2.** Re-run every numeric cell, percentage, count, denominator, ratio, and derived statistic against the live database and produce `manuscript_outputs/v1_0_20260501/M038_v2_DATA_VALIDITY_AUDIT_20260501.md`. Specific check-points (non-exhaustive — audit every number you see in the draft):

| Section | Data points to re-derive |
|---|---|
| Abstract | n=10,871; n=2,501 (23.0%); 1,429 (57.1%); 1,047 (41.9%); 1,440 (57.6%); median age 56 [IQR 45–66] vs 50 [39–62]; 70.8% / 79.9% female; 62.2% / 31.2% Black or AA; 28.5% / 54.4% White; 25.8% / 41.7% malignant; 64.6% / 80.9% PTC; 1,672/2,501 (66.9%) vs 4,327/8,370 (51.7%) total thyroidectomy; 5.28% / 3.20% any-comp (RR ≈ 1.65); RLN 14 (0.56%) vs 7 (0.084%); hematoma 23 (0.92%) vs 45 (0.54%); mortality 2.36% vs 1.59%; era prevalences 12% / 24.9% / 28.5% |
| §3.1 cohort assembly | All 10 component-overlap counts; inclusion-exclusion sum to 2,501 |
| §3.2 Table 1 | All 30+ rows (age, sex, race, BMI, NLP comorbidities, thyroid-specific history, ASA, era, pathology, follow-up) |
| §3.3 Table 2 | 11 histology counts + percentages |
| §3.4 Table 3 | 5 procedure-type rows + 11 operative-context rows; 100% / 99.98% completeness claim |
| §3.5 Table 4 | 10 complication outcomes × 2 arms × counts/percents/RR; verify each RR computation |
| §3.6 era stratification | 6 era rows × total / massive / % massive |

For each row, the audit doc should record: SQL query, live result, manuscript value, PASS/DIFF/FAIL.

**Step 3.4.2 — Excel deliverable.** Build `manuscript_outputs/v1_0_20260501/M038_v2_DATA_AND_SOURCES.xlsx` per the structure in §8(A) below. **Use the xlsx skill** before authoring the file — read `/var/folders/x8/nj9jzq591439vh50w8wtznh80000gn/T/claude-hostloop-plugins/59d1345f5677e124/skills/xlsx/SKILL.md` first.

Only after both deliverables clear, propose advancing to M038-B or other manuscript work.

### Step 3.5 — After the audit/Excel work, candidate next manuscripts

| Candidate | Status | Blockers |
|---|---|---|
| **M038-A descriptive (v2)** | Ready for PI review (after audit clears) | None (post-mig_252/253) |
| **M038-B definition paper** | Planning doc only (`6fd1710`) | Decide: build out or wait until M038-A clears review |
| **M039 PTH/Calcium** | READY_TO_DRAFT (per v21 §5); never drafted | None known |
| **M025 TIRADS** | YELLOW post-mig_249 | Verify feasibility row freshness |
| **M046 NIFTP / M047 Frozen Section** | CAVEATS_BUT_ACTIVE | Verify feasibility row |

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

**REQUIRED FIRST WORK (in order, before anything else):**

### (A) M038 v2 data-validity audit + Excel deliverable

This is the explicit first assignment from Logan for the v24 chat.

**(A.1) Data-validity audit.** Produce `manuscript_outputs/v1_0_20260501/M038_v2_DATA_VALIDITY_AUDIT_20260501.md`. For every numeric cell, percentage, count, denominator, ratio, and derived statistic in `M038_massive_goiter_DRAFT_v2_post_mig_252_253.md` (abstract + §3.1 + §3.2 Table 1 + §3.3 Table 2 + §3.4 Table 3 + §3.5 Table 4 + §3.6 era table + §4 discussion + §5 limitations footnotes), re-derive the value via live SQL against `thyroid_canonical_publication_v1_0` and record:

```
| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
```

Status is one of `PASS` (matches within rounding tolerance), `DIFF` (numeric mismatch — surface it; recommend a manuscript edit), or `FAIL` (the underlying query errors or the column doesn't exist). Flag any percentage that doesn't sum to 100% within rounding, any inclusion-exclusion check that doesn't reconcile (the §3.1 sum-to-2,501 check is the most important), any RR computation that doesn't match the cited counts, any number cited in the abstract that isn't reproduced in the body, and any claimed coverage rate (e.g., "100% completeness in massive") that the live data doesn't support.

If everything PASSES, the audit doc should still record every check, since the audit IS the deliverable. If anything DIFFs, surface it to Logan via AskUserQuestion before editing the manuscript.

**(A.2) Excel deliverable.** Build `manuscript_outputs/v1_0_20260501/M038_v2_DATA_AND_SOURCES.xlsx`. **Use the xlsx skill — `Read` `/var/folders/x8/nj9jzq591439vh50w8wtznh80000gn/T/claude-hostloop-plugins/59d1345f5677e124/skills/xlsx/SKILL.md` before authoring the file.**

Tab structure:

| # | Tab name | Contents |
|---:|---|---|
| 1 | `00_Cover` | Manuscript metadata: title, authors, target journal, release_id, mig_252/253 references, Cowork commit `4b48107`, generation date, point-of-contact |
| 2 | `01_Cohort_Assembly` | §3.1 cohort assembly — denominator counts, component-overlap class table, inclusion-exclusion check |
| 3 | `02_Table1_Demographics` | §3.2 Table 1 — every row, both arms, counts + percentages, coverage notes |
| 4 | `03_Table2_Histology` | §3.3 Table 2 — malignant subset histology distribution |
| 5 | `04_Table3_Procedure_OpContext` | §3.4 Table 3 (procedure type) + the operative-context block (CND/LND/op duration/LOS/transfusion/tracheostomy/readmission) |
| 6 | `05_Table4_Complications` | §3.5 Table 4 — 10 complication outcomes × counts × percentages × RR. Include a footer with the strict-definition mig_252 spec |
| 7 | `06_Era_Stratification` | §3.6 era table — six era rows |
| 8 | `07_Component_Coverage` | Per-era and per-arm coverage of `gland_weight_final_g`, `ct_substernal_extension_any`, `mri_substernal_any`, `ct_tracheal_*` columns (supports the §5 limitations claim) |
| 9 | `08_Race_Detail` | Full race breakdown (9 categories per arm) |
| 10 | `09_ASA_Detail` | Full ASA-class breakdown |
| 11 | `10_Source_SQL` | One row per query: `[query_id, target_section, sql_text, source_view, last_run_ts, n_rows]`. Every number in tabs 1–9 must trace back to a query_id here |
| 12 | `11_Column_Inventory` | The Column Inventory section from the manuscript, materialized as a row-per-column table with `[column_name, data_type, source_view, included_in_M038, rationale]` |
| 13 | `12_Reproducibility` | release_id, signoff_registry timestamp, mig_252/253 acceptance summary, gate1=218 attestation, cohort_parity attestation, the upstream Cursor dispatch commit SHAs (32beb7b, 0143539), the Cowork v23 commit (4b48107) |

Format conventions: header row in bold; counts as integers; percentages as proper percent-formatted cells (not text); RR values to 2 decimal places; null/missing as empty cell, not the literal string "NULL"; freeze the header row on every tab; every tab gets a one-line description in cell A1 above the data block; cell A2 is blank; data block starts at A3.

After both deliverables are produced, surgical-git-add + commit + push them in a single commit titled along the lines of `qa(M038): v2 data-validity audit + Excel deliverable`.

### (B–E) Subsequent options (after A clears)

- **(B) Build out M038-B definition paper.** The descriptive paper (M038 v2, post-audit) becomes a citable foundation. M038-B's planning doc at `M038_definition_paper_PLANNING_v1.md` defines three exposure operationalizations head-to-head; the focal ≥200g cohort has 10 strict-definition complication events (post-mig_252) which will require careful interaction-model power discussion. Apply the standing demographics + column-review rule to the M038-B cohort scope.
- **(C) M039 PTH/Calcium.** READY_TO_DRAFT, never drafted. Apply standing rule + check for parathyroid event-grain safe view (CF-PARATHYROID-EVENT-SAFE).
- **(D) Address CF-COMP-CONFIRMED-VARIANTS-AUDIT** by running the mig_252 §3.3 audit against the post-apply state. ~15 min. Confirms that the `_definitive` / `_probable_or_better` / `_any_evidence` family flags are also strict-rolled.
- **(E) Address CF-COHORT-VIEW-DUPLICATE-COLUMNS** by auditing the cohort-view DDLs. ~30 min. May affect any manuscript that selects `*` from a cohort view.
- **(F) Methods doc addendum.** Roll v17/v20/v21/v22/v23 round notes into `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`. ~10 min.

**Cowork's recommendation:** Complete (A) before anything else. Then surface the audit results to Logan and let him pick from B–F.

---

## §9 — Pre-flight reminder for the new chat agent

Before any analytical work:

1. **Run the §3 first-action checklist.** Confirm gate health, DB context, HEAD position.
2. **Required first work** is the M038 v2 data-validity audit + Excel deliverable per §0 / §3.4 / §8(A). Do this BEFORE M038-B or any other manuscript.
3. **Read the xlsx SKILL.md** before authoring the Excel file.
4. **Read the M038 v2 draft end-to-end** before re-deriving any data point — every number in the abstract should appear somewhere in §3 or §4 with the supporting count, and the audit should confirm both occurrences match the live SQL.
5. **Apply `feedback_manuscript_demographics_and_full_column_review.md`** for any new manuscript work after the audit clears.
6. **Use `AskUserQuestion`** before substantial analytical choices, and especially if any audit row returns DIFF or FAIL.
7. **For git ops always use Desktop Commander.** Bash sandbox cannot remove `.git/index.lock` files due to FileVault.

---

**End of v23 handoff.** **Required first action: M038 v2 data-validity audit (`M038_v2_DATA_VALIDITY_AUDIT_20260501.md`) + Excel deliverable (`M038_v2_DATA_AND_SOURCES.xlsx`).** Then the v24 chat picks from §8(B–F) per Logan's preference. HARD: do not touch M044 / M051 (ChatGPT lane).
