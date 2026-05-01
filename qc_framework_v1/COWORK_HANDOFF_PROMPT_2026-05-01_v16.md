# Cowork Handoff Prompt v16 — Thyroid Canonical Publication v1.0

**Generated:** 2026-05-01 (post v15 round CLOSED CLEAN — all 4 prompts + Prompt 5 carryover landed + Path-C verified)
**Tip of `origin/main` at write:** `c49b971` — `feat(qc): mig_233 audit dashboard snapshot view` (handoff doc to be appended at next commit)
**Supersedes:** v12 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md` and v13/v14/v15 batch docs
**Companion:** v15 batch at `cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v15.md`; v14 batch at `cursor_prompts/PARALLEL_AGENT_BATCH_20260430_v14.md`

---

## §0 — First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md` end-to-end before any tool use. Then run the §3 first-action checklist (`git fetch && git log --oneline -10`, run the v2 verification suite from `qc_framework_v1/queries/cowork_verification_suite_20260430.md`, query `manuscript_workspace.qc_audit_dashboard_VIEW_v1` for instant 5-gate state, and confirm metrics match §2 expected).
>
> **Standing context:** I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're in the final cleanup of the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`). **v15 round closed clean — all 4 prompts + Prompt 5 carryover landed + Path-C verified.** Lane G `mig_223` (semantic_publication schema + 9 manuscript-safe views) + Lane LN `mig_224-229` (LN/histology safe views + QC + borderline quarantine) shipped in v14; Copilot CF reconciliation closed CF-mig219 + CF-mig220; Cline Sonnet 4.6 ran ISSUE_REGISTRY refresh + Lane J CPM 24-na audit; Cursor Composer shipped Lane M Manuscript Methods + Table 1–5 refresh; Cline Sonnet 4.6 shipped `mig_230` Parquet export of 133 frozen tables; Cline Sonnet 4.6 shipped `mig_233 qc_audit_dashboard_VIEW_v1` (single-row 5-gate dashboard). Manuscript readiness verdict: **READY** — Lane M outputs are ready for manuscript drafting against `manuscript_outputs/v1_0_20260501/` + `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`. You're the orchestrator + verifier + applier; agents do bulk authoring; I'm the final ratifier.
>
> **You have:**
> - **Desktop Commander MCP** for git/shell on my Mac (FileVault — `.git/index.lock` cleanup may be needed; bash sandbox can't unlink)
> - **MotherDuck MCP** (read-only `query` + `query_rw`) — primary DB `thyroid_canonical_publication_v1_0`; archive DB `"Thyroid 2026 UPdated".archive_pub_v1_0`. Cowork's MCP is authed to `logan.glosser.eras@gmail.com`.
> - **GitHub repo** at `/Users/ros/THyroid 2026` (URL `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`)
> - **Auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
>
> **NO LANES IN FLIGHT** at handoff. v15 round fully closed. Live `manuscript_workspace.qc_audit_dashboard_VIEW_v1` reports gate1=210, gates 2-5=0, cohort_parity_ok=TRUE.
>
> **DEFERRED FUTURE TASKS** (not pending; trigger when stated condition is met):
> - **Future H** — `bi_powerbi.*` star-schema marts; trigger when Phase 4 Power BI Desktop migration starts
> - **CF-LN-METS-ARRAY-EMPTY-2801** — 2,801 of 2,847 LN-positive cases lack histology-attribution evidence; methods caveat only (no remediation mig); already documented in Lane M Methods §9.2
>
> **CRITICAL RIGOR REMINDER:** verify all agent work directly against MotherDuck. Pre-snapshot every mutating lane to `archive_pub_v1_0`. The verification suite v2 (`qc_framework_v1/queries/cowork_verification_suite_20260430.md`) is the SSOT post-lane.
>
> **First task:** §3 first-action checklist — confirm HEAD ≥ `7b61dba`, run 5-§ verification suite, then choose A/B/C from §6:
> - **(A)** Logan pasted an agent summary → verify per Path-C (probe live MD for the agent's batch_id; verify acceptance criteria from prompt)
> - **(B)** Logan ratified a pending decision → author + apply final SQL Cowork-direct
> - **(C)** Logan wants to start manuscript writing → orient toward `manuscript_outputs/v1_0_20260501/` + `docs/Methods_*.md` + `vw_*_safe_VIEW_v1` surfaces

---

## §1 — Project mission

**Logan Glosser**, Emory thyroid-cancer surgery researcher. Database: `thyroid_canonical_publication_v1_0` on MotherDuck (account `logan.glosser.eras@gmail.com`). Goal: manuscript-grade survival/recurrence/outcomes analyses on a single-institution thyroid cancer cohort.

- Cohort: **10,871 distinct research_id**
- Backbone: `canonical_patient_master` (1,630 cols; **1,607 verified / 23 na** post Lane J mig_235; was 1,606 v / 24 na pre-mig_235)
- Tier-2 events / patient_rollup canonicals: 62/62 verified (100%)
- 10 deferred analytic composites verified in v12 round (mig_211 Lane A)
- Dedup VIEW (`canonical_path_malignant_events_dedup_VIEW_v1` from Lane B mig_212)
- Path indeterminate landing table (mig_207)
- **Lane G semantic_publication schema** (mig_223) — 1 manifest table + 8 manuscript-safe views
- **Lane LN manuscript_workspace surfaces** (mig_224–229) — 4 LN/histology safe views + 5 QC tables + borderline quarantine flag
- **CF-mig219 narrow ACR missing view** (mig_232) — 7,270 rows
- **Lane M manuscript outputs** (mig_234) — 6 CSVs + Methods .md drafted
- **mig_230 Parquet export** — 133 tables, 1.16M rows, 64 MB ZSTD frozen reproducibility mirror
- Authoritative SSOT: live MotherDuck — never trust prior summaries

---

## §2 — Round delta v15 vs v14

| Metric | v14 final | v15 final (post Prompt 4 expected) | Δ |
|---|---:|---:|---:|
| 5-gate gate1 (verified tables, distinct objects) | 209 (with 1 dup → 208 distinct) | **210 (gate1_total = gate1_distinct)** | -1 dup cleaned (mig_231) + mig_232 view (+1) + mig_233 dashboard view (+1) |
| 5-gate gates 2–5 | 0/0/0/0 | **0/0/0/0** | unchanged |
| Cohort parity (CPM / US gland v2 / US LN v2) | 10871/10871/10871 | **10871/10871/10871** | unchanged |
| CPM column counts | 1606 v / 24 na | **1607 v / 23 na / 0 failed** | +1 verified, -1 na (Lane J mig_235; `pmhx_nlp_family_hx_thyroid` reclassified) |
| Open carry-forwards | CF-mig219 + CF-mig220 + 8 Lane LN implicit | **CF-LN-METS-ARRAY-EMPTY-2801 only (Methods caveat)** | -10 closed/migrated to ISSUE_REGISTRY |
| Manuscript outputs | none | **6 CSVs + Methods .md + .bib stubs** | full Lane M deliverable |
| Frozen reproducibility | none | **133 Parquet files / 64 MB ZSTD / sha256 manifest** | mig_230 Parquet export |

**v15 migrations landed:** `mig_230` (Parquet), `mig_231` (registry cleanup, Cowork-direct), `mig_232` (narrow ACR view), `mig_233` (audit dashboard view), `mig_234` (Lane M Table 1 refresh), `mig_235` (CPM 24-na audit). Plus Lane M Methods .md + 6 CSVs + ISSUE_REGISTRY refresh + 7-typo `histology_vocab_normalization_map_v1` extension via Lane LN mig_224.

---

## §3 — First-action checklist

```
1. cd "/Users/ros/THyroid 2026" && git fetch origin && git log --oneline -10
   Expect tip ≥ c49b971 (or later if new work has landed)

2. ONE-QUERY HEALTH CHECK (preferred over manual 5-gate query — uses mig_233 dashboard):
   SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
   Expect: gate1=210, gate1_distinct=210, gates 2-5=0,
           cpm_pts=us_gland_v2_pts=us_ln_v2_pts=10871,
           cohort_parity_ok=TRUE

3. (Optional fallback) Manual v2 5-gate audit per cowork_verification_suite_20260430.md §1
   if dashboard view returns unexpected — both should agree

4. Read latest reports + close-outs:
   - qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md (this doc)
   - cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v15.md (v15 dispatch context)
   - qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260501.md
   - qc_framework_v1/reports/cpm_24_na_audit_20260501.md
   - qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md
   - docs/Methods_thyroid_canonical_pub_v1_0_20260501.md
   - qc_framework_v1/ISSUE_REGISTRY.md (1,293 lines — full project history)

5. ls cursor_prompts/ for any pending lane prompts authored but not yet dispatched
```

---

## §4 — Currently in flight at handoff

**None.** v15 round closed. Path-C verification confirmed `mig_233 qc_audit_dashboard_VIEW_v1` clean at commit `c49b971`:

```
gate1_verified_tables       = 210
gate1_distinct_objects      = 210  (matches; no dups)
gate2_missing_signoff       = 0
gate3_count_mismatch        = 0
gate4_verified_cols_meta    = 0
gate5_clinical_date_violations = 0
cpm_pts                     = 10871
us_gland_v2_pts             = 10871
us_ln_v2_pts                = 10871
cohort_parity_ok            = TRUE
most_recent_signoff_migration = qc_framework_v1/migrations/233_qc_audit_dashboard_VIEW_20260501.sql
```

---

## §5 — Future tasks (deferred — do not dispatch unless triggered)

| Task | Trigger | Agent (TBD) |
|---|---|---|
| **Future H** — `bi_powerbi.*` star-schema marts (13 dim/fact tables) | Phase 4 Power BI Desktop migration begins | TBD (likely Cursor Composer; multi-day star-schema design) |
| **Manuscript drafting** — author paper text against Lane M outputs | Logan starts writing | Logan + Cowork support |
| **CF-LN-METS-ARRAY-EMPTY-2801 chart-review remediation** | Logan wants tumor-type-specific LN claims | Logan + chart review queue (low priority; affects only refined subtype-LN claims) |
| **Refresh Parquet export after any future mutating mig** | Any new mig that changes a frozen table | Cline Sonnet 4.6 (mechanical re-export) |

---

## §6 — Decision menu (orient new chat to right action)

After §3 first-action checklist, choose:

- **(A) Verify Prompt 4 mig_233 if it landed** → Path-C probe (view exists, single-row output sane); commit any verification scratch; close v15 round + write v16 closeout summary `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v15.md` + this v16 handoff updates.
- **(B) Logan ratified a pending decision** → author + apply final SQL Cowork-direct following mig_205/mig_209/mig_231 patterns (pre-snapshot + provenance + 5-gate post-check).
- **(C) Logan wants to start manuscript writing** → orient toward Lane M outputs:
  - `manuscript_outputs/v1_0_20260501/` (Tables 1–5 + cohort flow CSVs)
  - `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` (90-line Methods draft)
  - `docs/Methods_*.bib` (BibTeX stubs)
  - `qc_framework_v1/manuscript/mig234_lane_m/` (executable SELECT definitions for any custom analyses)
  - `semantic_publication.vw_*_safe_VIEW_v1` (read-path SSOT for all analyst code)
  - `parquet_export/pub_v1_0_20260430/` (offline reproducibility mirror)
- **(D) New CF discovered or new lane needed** → write a new Cursor/Cline lane prompt to `cursor_prompts/`, document non-overlap zones, surface to Logan with agent assignment recommendation.
- **(E) Future H — Power BI marts begin** → ratify trigger with Logan; if green-light, write the multi-day Cursor Composer prompt for `bi_powerbi.*` star-schema build.

---

## §7 — Path-C verification protocol (mandatory for agent-applied lanes)

For every agent summary Logan pastes, or every commit landing while you watch:

1. **Probe live MD** for the agent's `batch_id`:
   ```sql
   SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE batch_id='<agent_batch_id>';
   SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 WHERE run_id='<agent_run_id>';
   ```
2. **Verify acceptance criteria** from the prompt (row counts, view existence, flag presence)
3. **Re-run 5-gate v2 audit** — confirm no regression (gate1 should grow, gates 2–5 stay 0)
4. **Re-run cohort parity** — should stay 10,871/10,871/10,871
5. **For mutating lanes**: confirm pre-snapshot exists in `archive_pub_v1_0`
6. **If clean**: commit Logan's local repo (surgical `git add` per `feedback_surgical_git_add.md`, never `-A`); push
7. **If issues**: surface to Logan with hypothesis + propose remediation mig

---

## §8 — Repo + tooling reminders

- **Surgical git add per `feedback_surgical_git_add.md`**: never `git add -A` or directory-wide; explicit paths only
- **Always commit + push per `feedback_commit_workflow.md`**: stage → commit → push; lint Python first
- **PHI safety per `feedback_phi_safety.md`**: never print clinical notes; research_id only
- **Desktop Commander > bash sandbox per `feedback_use_desktop_commander_first.md`**: bash sandbox can't unlink `.git/index.lock` (FileVault); use Desktop Commander
- **DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ** per `reference_duckdb_timestamp_tz.md`: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols
- **No cross-DB canonical sourcing** per `feedback_no_cross_db_canonical_sourcing.md`: canonicals are standalone live objects in `main`; never `FROM archive_pub_v1_0.*` at build time
- **VIEW naming** per `reference_view_naming_convention.md`: any `main.*` or `manuscript_workspace.*` VIEW must carry `_VIEW_v1` suffix (or `_VIEW_v2`/`_VIEW_v3` for revisions)

---

## §9 — Memory entry points (read in this order if new context needed)

1. `MEMORY.md` — index of all memories
2. `project_lane_j_cpm_na_audit_20260501.md` — most recent v15 round closeout (Lane J)
3. `project_mig_232_narrow_acr_view_closeout_20260501.md` — CF-mig219 follow-up
4. `project_semantic_publication_layer_20260430.md` — Lane G mig_223
5. `project_chatgpt_review_followup_2026-04-30.md` — v12 round summary
6. `feedback_*.md` — workflow rules (commit, surgical git add, PHI, etc.)
7. `reference_*.md` — environment + conventions (MD accounts, view naming, etc.)

---

## §10 — Reference object inventory

### Cowork-built or Cowork-coordinated views/tables (key surfaces)

**`semantic_publication` schema** (Lane G mig_223):
- `release_manifest_v1` (1 row: pub_v1_0_20260430)
- `vw_patient_master_safe_VIEW_v1` (10,871 rows)
- `vw_cohort_membership_safe_VIEW_v1` (10,871 rows)
- `vw_path_malignant_tumor_safe_VIEW_v1` (5,944 rows)
- `vw_recurrence_safe_VIEW_v1` (10,739 rows = 10,871 − 132 quarantined)
- `vw_fna_safe_VIEW_v1` (8,050 rows)
- `vw_us_nodule_safe_VIEW_v1` (29,504 rows)
- `vw_molecular_safe_VIEW_v1` (1,384 rows)
- `vw_labs_long_safe_VIEW_v1` (44,124 rows; UNION of 5 lab canonicals)

**`manuscript_workspace` schema** (Lane LN mig_224–229 + earlier):
- `dim_histology_standardized_VIEW_v1` (13 canonical codes)
- `vw_ln_surgery_publication_safe_VIEW_v1` (4,008 rows; 14 borderline-quarantined patients excluded)
- `vw_ln_patient_publication_safe_VIEW_v1` (4,008 patients)
- `vw_ln_histology_attribution_VIEW_v1` (5,918 rows; 26 quarantined tumor rows excluded)
- `qc_ln_impossible_counts_v1` (55 rows: 4 CPM + 11 rollup + 40 safe-view-level)
- `qc_ln_duplicate_rollup_patients_v1` (256 rows)
- `qc_ln_multihistology_attribution_queue_v1` (47 rows)
- `qc_histology_borderline_in_malignant_table_v1` (26 rows)
- `qc_histology_vocab_typos_v1` (1 row — `Follicular caricinoma` persists in raw col by design)
- `vw_us_nodule_tirads_*_VIEW_v1` (4 cohort views: strict_acr2017, any_reported, reported_not_fully_parsed, unresolved_or_excluded — Lane E mig_219)
- `vw_us_nodule_tirads_derived_acr_missing_VIEW_v1` (7,270 rows — mig_232 narrow CF-mig219 follow-up)

**`main` schema canonical changes** (recent migs):
- `histology_vocab_normalization_map_v1` (104 rows; +8 typos in Lane LN mig_224)
- `canonical_path_malignant_events_v1.is_borderline_or_benign_with_staging` BOOLEAN (mig_229; 27 rows TRUE)
- `canonical_recurrence_resolved_v1.is_implausible_date_quarantine` BOOLEAN (mig_213; 132 rows TRUE)
- `canonical_column_verification_registry_v1.na_rationale` VARCHAR (Lane J mig_235; 23 CPM rows populated)

### Manuscript outputs (Lane M mig_234 — `manuscript_outputs/v1_0_20260501/`)

- `Table_1_cohort_demographics_v1_0_20260501.csv` (4 KB)
- `Table_2_tumor_stage_distribution_v1_0_20260501.csv` (163 B)
- `Table_3_LN_summary_safe_v1_0_20260501.csv` (548 B)
- `Table_4_recurrence_survival_v1_0_20260501.csv` (976 KB / 10,740 rows)
- `Table_5_molecular_distribution_v1_0_20260501.csv` (258 B)
- `cohort_flow_v1_0_20260501.csv` (CONSORT-style 9-step flow)
- `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` (90 lines, 9 sections)
- `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib`

### Frozen reproducibility (mig_230 Parquet — `parquet_export/pub_v1_0_20260430/`)

- 133 .parquet files (114 main + 9 semantic_publication + 10 manuscript_workspace)
- 1,163,387 total rows
- 63.97 MB ZSTD compressed
- `_MANIFEST.md` with sha256 per file
- .parquet binaries gitignored; manifest committed

---

## §11 — Hand-off summary

**State at handoff:** v1.0 publication is **manuscript-ready**. Lane G semantic_publication layer is the analyst SSOT. Lane LN LN/histology surfaces are clean. Lane M Methods + Tables 1–5 + cohort flow are drafted and ready for manuscript text. Frozen Parquet mirror at `parquet_export/pub_v1_0_20260430/` provides offline reproducibility. CPM column governance is now 1,607 v / 23 na / 0 failed (every na has documented rationale). One-query lakehouse health via `manuscript_workspace.qc_audit_dashboard_VIEW_v1` (mig_233) — current state 210/0/0/0/0 with cohort_parity_ok=TRUE.

**Active work:** none. v15 round closed clean.

**Next likely user action:** start manuscript drafting against Lane M outputs (option C in §6 decision menu) OR ask for a new lane to address residual carry-forwards (option D) OR trigger Future H Power BI marts (option E).

**Future H Power BI marts** are deferred until Logan triggers Phase 4 — no work needed until then.

---

## §12 — Recent commit log (for context)

```
c49b971  feat(qc): mig_233 audit dashboard snapshot view                   [Cline Sonnet 4.6, v15 Prompt 4]
7b61dba  feat(qc): Lane J — CPM 24-na column audit (mig_235)              [Cline GPT-5.5, v15 Prompt 3]
2d71885  docs(manuscript): Lane M Methods + Table 1 refresh                 [Cursor Composer, v15 Prompt 2]
96a89f1  feat(qc): mig_232 narrow ACR missing view (CF-mig219 follow-up)   [Cline Sonnet 4.6, v15 Prompt 1]
16dd9e0  mig_230: Parquet export pub_v1_0_20260430 — 133 tables / 64 MB    [Cline Sonnet 4.6, v14 Prompt 5]
d27f0d1  qc: v15 round dispatch — mig_231 cleanup + 4-prompt parallel batch [Cowork-direct + v15 batch]
95ef464  docs(qc): ISSUE_REGISTRY refresh + Lane LN mig_224-229 run log    [Cline Sonnet 4.6, v14 Prompt 4]
b5e1fdd  qc: reconcile CF-mig219 and CF-mig220                              [Copilot GPT-5.5, v14 Prompt 3]
358cf7b  Add mig223 semantic publication layer                              [Cline GPT-5.5, v14 Prompt 1]
6d8ddb9  Lane LN: mig_224–229 SQL + apply runner (229 before 228)          [Cursor Composer, v14 Prompt 2]
1be11e5  qc: v14 round pre-flight — parallel agent batch + Cowork-direct CF closures
5d6aa85  qc: lymph nodes + histology — Cowork validation of ChatGPT plan + Lane LN proposal
```

---

## §13 — Quick links

- [v12 handoff (project mission)](computer:///Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md)
- [v14 prompt batch](computer:///Users/ros/THyroid 2026/cursor_prompts/PARALLEL_AGENT_BATCH_20260430_v14.md)
- [v15 prompt batch](computer:///Users/ros/THyroid 2026/cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v15.md)
- [Lane M Methods .md](computer:///Users/ros/THyroid 2026/docs/Methods_thyroid_canonical_pub_v1_0_20260501.md)
- [Manuscript outputs dir](computer:///Users/ros/THyroid 2026/manuscript_outputs/v1_0_20260501/)
- [Lane LN assessment plan](computer:///Users/ros/THyroid 2026/qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md)
- [CF reconciliation (mig_232 input)](computer:///Users/ros/THyroid 2026/qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260501.md)
- [CPM 24-na audit (Lane J)](computer:///Users/ros/THyroid 2026/qc_framework_v1/reports/cpm_24_na_audit_20260501.md)
- [Verification suite v2](computer:///Users/ros/THyroid 2026/qc_framework_v1/queries/cowork_verification_suite_20260430.md)
- [ISSUE_REGISTRY](computer:///Users/ros/THyroid 2026/qc_framework_v1/ISSUE_REGISTRY.md)
- [Parquet manifest](computer:///Users/ros/THyroid 2026/parquet_export/pub_v1_0_20260430/_MANIFEST.md)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

**End of v16 handoff. Begin with §3 first-action checklist. Once oriented, the most likely next action is (a) Path-C verify Prompt 4 if it has landed, then (b) orient toward manuscript drafting per §6 option C, OR (c) field whatever Logan asks next.**
