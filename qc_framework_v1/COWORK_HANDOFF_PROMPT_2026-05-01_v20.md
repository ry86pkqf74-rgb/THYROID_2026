# Cowork Handoff Prompt v20 — Thyroid Canonical Publication v1.0

**Generated:** 2026-05-01 by Cowork mid-flight (mig_248 still running in Cursor Composer)
**Tip of `origin/main` at write:** `f9f848c` — `manuscript(M032): first draft — 25-year descriptive cohort paper`
**Supersedes:** v19 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v19.md`
**Closeout retrospective:** v17 at `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md`

---

## §0 — First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v20.md` end-to-end before any tool use.
>
> **Then run the §3 first-action checklist:**
> 1. `git fetch origin && git log --oneline -15` from `/Users/loganglosser/THYROID_2026` — check whether `mig_248` (column-rename drift repair) landed since v20 was written
> 2. `SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;` — expect gate1=218, gates 2-5=0, cohort_parity TRUE
> 3. Probe whether mig_248 fixed the broken cohort views by re-running `SELECT COUNT(*) FROM manuscript_workspace.cohort_m031_nuclear_medicine_v1` (and a sample of M048-M066)
> 4. Reconcile `manuscript_workspace.manuscript_dashboard_VIEW_v1` readiness signals against §4 expected-state matrix
>
> **Standing context:** I'm Logan Glosser, Emory thyroid-cancer surgery researcher. We're moving the thyroid v1.0 publication data **out of MotherDuck soon**, so manuscript-throughput is the priority — share-rebuild + most v17.5-style cleanup is deprioritized except where it directly blocks manuscript drafting. Current chat closed v17 round (mig_236-244), repaired stale view refs (mig_245), built manuscript dashboard (mig_246), dispatched feasibility refresh (mig_247, landed) + column-rename drift repair (mig_248, in flight at v20 write). M032 first draft pushed. Next chat picks up post-mig_248.
>
> **You have:** Desktop Commander MCP for git/shell; MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; GitHub at `/Users/loganglosser/THYROID_2026`. Use Desktop Commander for git ops.
>
> **Most likely first task:** Path-C verify mig_248. If clean, re-run mig_247 (or have agent re-score) for the manuscripts whose cohort views were broken pre-mig_248 — that batch should mostly return to GREEN, restoring the priority queue. Then continue Lane M drafting (M039 next-up; M038 needs research-question definition).

---

## §1 — Round delta v19 → v20 (what landed)

| Mig | Lane | Commit | When (UTC-4) | Outcome |
|---|---|---|---|---|
| **mig_245** | Stale view reference repair (8 views) | `96e8ce3` | 2026-05-01 ~02:55 | ✓ All 8 views now queryable (cohort_m011/m025/m045/m075 + 4 views_readable). +Dual-TIRADS split on US_Nodules_Wide_v2 (40 cells split into _acr2017 + _updated). |
| **mig_246** | manuscript_dashboard_VIEW_v1 | `5bbcee0` | 2026-05-01 ~03:05 | ✓ Live dashboard JOINing feasibility_v1 + dive_map_v1 with computed `draft_readiness_signal`. |
| **dispatch** | Cursor prompts for mig_247 + mig_248 | `a831828` | 2026-05-01 ~03:10 | ✓ Self-contained dispatch docs in cursor_prompts/ |
| **mig_247** | manuscript_feasibility_v1 refresh | `80b3c43` | 2026-05-01 ~04:11 (Cursor Composer) | ✓ All 83 manuscripts re-scored. canonical_version_at_scoring='v1_0_post_mig_246'. **MAJOR shift in priority queue** (see §4). Surfaced widespread column-rename drift requiring mig_248. |
| **manuscript** | M032 first draft | `f9f848c` | 2026-05-01 ~04:12 | ✓ 171-line draft with 8 author-input gaps flagged. |
| **mig_248** | Column-rename drift repair (cohort views) | (pending) | (in flight) | Cursor Composer in progress at v20 write. Will fix `syn_isthmus_size_cm` rename across ~24 cohort views (M048-M066/M068-M076 series + cohort_m031). |

**Lakehouse health unchanged:** gate1=218, gates 2–5 = 0, cohort_parity TRUE (10871×3), gov_gap = 0.

---

## §2 — Critical findings from mig_247 (priority-queue shift)

### Color distribution shift

| Bucket | Pre-mig_247 (2026-04-16) | Post-mig_247 (2026-05-01) |
|---|---:|---:|
| GREEN feasibility | 46 | **27** |
| YELLOW | 17 | **5** |
| RED | 20 | **51** |

### Dashboard `draft_readiness_signal` shift

| Bucket | Pre-mig_247 | Post-mig_247 |
|---|---:|---:|
| **READY_TO_DRAFT** | 16 | **3** |
| GREEN_BUT_IDEA_STAGE | 30 | 24 |
| CAVEATS_BUT_ACTIVE | 3 | 3 |
| YELLOW_IDEA | 14 | 2 |
| DEFERRED | 0 | 31 |

### What caused the shift

The mig_247 agent flagged **column-rename drift** in cohort_view DDLs:
- `tirads_best_category_v12` referenced in many cohort views — column doesn't exist in `canonical_patient_master` (it's a v12 alias used in cohort views; CPM uses `nlp_tirads_max_category` or `cupm.max_tirads_category_ever`)
- `tumor_size_cm` referenced — renamed to `path_tumor_size_cm` or `tumor_size_cm_max`
- `syn_isthmus_size_cm` referenced — renamed to `syn_isthmus_size_cm_legacy_raw` (the bug we found that originally broke `cohort_m031`); breaks ~24 thematic-T1 views (M048–M066, M068–M076)

These are **silently broken cohort views** — the same kind of drift mig_245 fixed but at the column level, not the inter-object level. mig_248 (in flight) addresses this.

**Expected post-mig_248 + re-score**: many manuscripts should return to GREEN (the cohort views will work again, candidate_n will recompute correctly, no MISSING-column flag).

### 3 surviving READY_TO_DRAFT manuscripts (current priority queue)

| ID | Title | Status | N | Notes |
|---|---|---|---:|---|
| **M032** | 25-Year Descriptive Analysis | In Progress (High) | 10,871 | **First draft pushed at `f9f848c`** — 171 lines, 8 author-input gaps |
| **M038** | Massive Goiter Surgery (ECMO support) | Ready to Submit (Medium) | 10,871 | Recommended next step: "Define research question" |
| **M039** | PTH/Calcium Protocol Post-Thyroidectomy | In Progress (Medium) | 4,561 | **NEW GREEN** (was YELLOW) — coverage gate cleared post-v17 |

---

## §3 — First-action checklist for new chat

### Step 3.1 — Confirm git state via Desktop Commander

```bash
cd "/Users/loganglosser/THYROID_2026"
git fetch origin
git log --oneline -15
git status --porcelain
```

Expect HEAD ≥ `f9f848c`. If a `mig_248` commit appears past that, Cursor Composer landed it. If not, the agent is still running — surface to Logan and either wait or check Cursor's status.

### Step 3.2 — One-query lakehouse health

```sql
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
```

**Required baseline:** gate1=218, gates 2-5=0, cohort_parity TRUE (10871×3), gov_gap=0. **mig_245/246/247/248 should NOT change gate1** — they're all in `manuscript_workspace`/`views_readable`, not in `canonical_table_signoff_registry_v1`.

### Step 3.3 — Path-C verify mig_248 (if landed)

```sql
-- Test the previously-broken cohort views
SELECT 'm031' AS v, COUNT(*) FROM manuscript_workspace.cohort_m031_nuclear_medicine_v1
UNION ALL SELECT 'm048', COUNT(*) FROM manuscript_workspace.cohort_m048_tnm_multifocal_v1
UNION ALL SELECT 'm057', COUNT(*) FROM manuscript_workspace.cohort_m057_risk_stratification_v1
UNION ALL SELECT 'm076', COUNT(*) FROM manuscript_workspace.cohort_m076_ln_surveillance_v1;
```

If all 4 return COUNT(*) without errors → mig_248 succeeded for the M048-series.

```sql
-- Full cohort view scan (use the script generated in mig_246, but as a per-view loop in case some still error)
SELECT cohort_view_name FROM manuscript_workspace.manuscript_dive_map_v1;
-- Then per-view: SELECT COUNT(*) FROM manuscript_workspace.<each>;
```

### Step 3.4 — Re-score feasibility post-mig_248

If mig_248 fixed cohort views, the feasibility scores in `manuscript_feasibility_v1` are now stale-by-mig_248. Two options:
- **Option A**: Re-dispatch mig_247 to Cursor Composer with a note "the cohort views are now fixed; re-score the manuscripts that previously had cohort SELECT failures". Quick re-run.
- **Option B**: Manual touch-up — for each manuscript whose `gating_issues` mentions "cohort SELECT failed", re-COUNT its cohort view, update `candidate_n`, and adjust `feasibility_color` to GREEN if the underlying coverage was clean.

### Step 3.5 — Reconcile dashboard against §4 expected state

```sql
SELECT draft_readiness_signal, COUNT(*) AS n,
       STRING_AGG(CAST(manuscript_id AS VARCHAR), ',' ORDER BY manuscript_id) AS ids
FROM manuscript_workspace.manuscript_dashboard_VIEW_v1
GROUP BY draft_readiness_signal;
```

After mig_248 + re-score, expected state (rough):
- READY_TO_DRAFT: ~6-10 (M032, M038, M039 + M046/M047/M044 if YELLOW→GREEN)
- GREEN_BUT_IDEA_STAGE: ~24 (M048-M066/M068-M076 thematic series, after their cohort views are fixed)
- DEFERRED: ~30 (true deferrals — labs/external/literature-only)

---

## §4 — Manuscript priority queue (current snapshot at v20 write)

**16 originally READY_TO_DRAFT (pre-mig_247):** M025, M029, M030, M031, M032, M033, M035, M036, M037, M038, M042, M043, M044, M045, M046, M047

**3 surviving post-mig_247 (pre-mig_248):** M032, M038, M039

**Likely post-mig_248 + re-score restoration candidates:**
- **M025** (TIRADS Performance, "Ready to Submit") — IF mig_248 fixes the `tirads_best_category_v12` rename in cohort_m025
- **M029** (FNA Concordance) — IF tumor_size_cm rename resolved (likely via path_tumor_size_cm)
- **M030, M033** (Genetic Predictive, Afirma/ThyroSeq) — schema clean once cohort views fixed
- **M035** (Bethesda V) — small N=246 cohort; should restore once view works
- **M036, M037, M042–M047** — most should return to GREEN

**Always-deferred (won't return to GREEN):**
- M005 (medications), M010 (zip codes), M015 (QoL), M021/M022 (rare cases), M026 (heavy metals), M027 (metabolomics), M034 (literature review), M041 (thyroglossal duct cyst — not in schema), M074 (zip codes), M077 (QoL), M083 (rare cases) — all blocked on data not in publication DB

---

## §5 — Decision menu (post-mig_248 verification)

After §3 first-action checklist, choose:

- **(A) Path-C verify mig_248 + re-score feasibility** *(most likely)* — Cowork-direct verify each repaired cohort view; re-dispatch mig_247 to refresh feasibility post-fix; reconcile dashboard.
- **(B) Manuscript drafting: M039 next** — PTH/Calcium Protocol emerged as NEW READY_TO_DRAFT post-v17. Has cohort_n=4,561 (87 TT patients with calcium labs / 76% PTH coverage of those). Worth a draft pass while mig_247 reverification happens.
- **(C) Manuscript drafting: M038 research question definition** — Massive Goiter / ECMO support; Logan to pick the specific outcome (e.g., ECMO-supported volume, tracheal extension, complication rates).
- **(D) Author mig_249: per-manuscript Tables generator** — for the 13 cohort-specific READY_TO_DRAFT manuscripts that aren't whole-cohort (M025, M029, M030, M033, M035-M037, M042-M047), generate per-manuscript Table 1 (demographics) + Table 2 (key outcome) from each cohort_m0XX_v1 view. Cursor Composer dispatch.
- **(E) Methods doc v17 addendum** — small Cowork-direct file edit; add a paragraph documenting v17 closeout + mig_245/246/247/248 to keep Methods aligned with current schema state.
- **(F) Power BI prep / parquet refresh** — if MD migration is imminent, refresh `parquet_export/pub_v1_0_20260430/` mirror with post-v17/post-mig_245/post-mig_248 state. Multi-day Cursor Composer lane (Future-H queue).

---

## §6 — Path-C verification protocol (when mig_248 lands)

For each broken cohort view repaired by mig_248:

1. Query the repaired view: `SELECT COUNT(*) FROM manuscript_workspace.<view> LIMIT 0`
2. Confirm it returns without Catalog Error / Binder Error
3. Compare row count to feasibility_v1's `candidate_n` (should be similar or improved)
4. Spot-check 1 row of contents to verify expected columns are present

If any view STILL errors post-mig_248, surface to Logan with the specific error message + propose remediation (likely a column rename the agent missed).

---

## §7 — Repo + tooling reminders (unchanged from v19 §6)

- **Workspace path:** `/Users/loganglosser/THYROID_2026`
- **Surgical git add per `feedback_surgical_git_add.md`**: explicit paths only
- **Always commit + push per `feedback_commit_workflow.md`**
- **PHI safety per `feedback_phi_safety.md`**: research_id only
- **Desktop Commander > bash sandbox per `feedback_use_desktop_commander_first.md`**: bash sandbox can't unlink `.git/index.lock` (FileVault)
- **DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ** per `reference_duckdb_timestamp_tz.md`: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols
- **VIEW naming** per `reference_view_naming_convention.md`: any `main.*` or `manuscript_workspace.*` or `semantic_publication.*` VIEW must carry `_VIEW_v1` suffix
- **research_id is VARCHAR everywhere in `semantic_publication.*`** (mig_239)

---

## §8 — Reference object inventory (v20 baseline)

### `semantic_publication` schema (publication-tier analyst SSOT) — UNCHANGED from v19
- `release_manifest_v1` — BASE TABLE, 1 row (`pub_v1_0_20260430`)
- 15 safe views including the v17 additions (us_exam, frozen, 3 LN, snake_case_aliases, patient_domain_wide)

### `manuscript_workspace` schema — UPDATED post-v19
- **NEW (mig_246):** `manuscript_dashboard_VIEW_v1` — single-pane JOIN view of feasibility + dive_map + readiness signals
- **REFRESHED (mig_247):** `manuscript_feasibility_v1` — re-scored 83 manuscripts; `canonical_version_at_scoring = 'v1_0_post_mig_246'`
- **STILL BROKEN (until mig_248 lands):** `cohort_m031`, `cohort_m048-m066`, `cohort_m068-m076` series — column-rename drift on `syn_isthmus_size_cm`
- Existing infrastructure preserved: `manuscript_dive_map_v1` (63 manuscripts → 31 Dives), `qc_audit_dashboard_VIEW_v1`, LN safe views, TIRADS cohort views

### `views_readable` schema — UPDATED post-v19
- All 4 broken views fixed in mig_245: `Genetics_Variants`, `US_Lymph_Nodes_Wide_v2`, `US_Nodules_Wide_v2` (with dual-TIRADS split), `US_Thyroid_Gland_Wide_v2`

### `main` schema — UNCHANGED from v19
- All 7 `_VIEW_v1`/`_VIEW_v2` named views queryable
- `canonical_table_signoff_registry_v1` (218 verified rows)

---

## §9 — Recent commit log (v20 era)

```
f9f848c  manuscript(M032): first draft — 25-year descriptive cohort paper
80b3c43  feat(qc): mig_247 — manuscript_feasibility_v1 refresh against post-v17 schema   [Cursor Composer]
a831828  docs(qc): cursor prompts for mig_247 (feasibility refresh) + mig_248 (column-rename drift repair)
5bbcee0  feat(qc): mig_246 — manuscript_workspace.manuscript_dashboard_VIEW_v1
96e8ce3  feat(qc): mig_245 — stale view reference repair (8 views unbroken)
e4254fe  docs(qc): v17 round closeout (mig_236-244 verified clean) + v19 handoff
273eb75  feat(qc): mig_244 — semantic_publication.vw_patient_domain_wide_safe_VIEW_v1   [Cursor Composer]
35f29d3  mig_241: LN safe-view promotion to semantic_publication (3 views)             [Cline Sonnet 4.6]
9cf03cd  feat(qc): mig_243 — snake_case alias view                                     [Cline GPT-5.5]
1b0e143  docs(qc): v18 handoff — mid-round
c2a7b5f  feat(qc): mig_242 — semantic_publication.vw_frozen_section_safe_VIEW_v1       [Cursor Composer]
e0d3471  mig_240: add semantic_publication.vw_us_exam_safe_VIEW_v1                     [Cline Sonnet 4.6]
6fc6f89  feat(qc): mig_239 — semantic research_id VARCHAR + col_registry dedup         [Cowork-direct]
```

(Expected next: `feat(qc): mig_248 — column-rename drift repair across cohort views` from Cursor Composer.)

---

## §10 — Quick links

- [v20 handoff (this doc)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v20.md)
- [v19 handoff (predecessor — pre-mig_245)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v19.md)
- [v17 closeout retrospective](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md)
- [mig_245 SQL — stale view ref repair](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/245_stale_view_ref_repair_20260501.sql)
- [mig_246 SQL — manuscript dashboard view](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/246_manuscript_dashboard_VIEW_v1_20260501.sql)
- [mig_247 dispatch prompt](computer:///Users/loganglosser/THYROID_2026/cursor_prompts/CURSOR_PROMPT_MIG_247_FEASIBILITY_REFRESH_20260501.md)
- [mig_248 dispatch prompt](computer:///Users/loganglosser/THYROID_2026/cursor_prompts/CURSOR_PROMPT_MIG_248_COLUMN_RENAME_DRIFT_REPAIR_20260501.md)
- [M032 first draft](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md)
- [Lane M Methods](computer:///Users/loganglosser/THYROID_2026/docs/Methods_thyroid_canonical_pub_v1_0_20260501.md)
- [Manuscript outputs folder](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

## §11 — Open carry-forwards

| ID | Description | Status | Trigger to close |
|---|---|---|---|
| **CF-MIG_248-IN-FLIGHT** | Cursor Composer running mig_248 (column-rename drift repair across ~24 cohort views) | In flight at v20 write | When commit lands; new chat verifies via §3.3 |
| **CF-MIG_247-RERUN** | After mig_248 lands, re-score the manuscripts whose cohort views were broken pre-mig_248 (~22 thematic-T1 series) | Pending | After mig_248 verified clean |
| **CF-M039-DRAFT** | M039 (PTH/Calcium Protocol) emerged as NEW READY_TO_DRAFT post-v17 — needs first draft | Open | Logan picks up after mig_248 verification |
| **CF-M038-RQ** | M038 (Massive Goiter / ECMO) needs research-question definition per agent's recommended_next_step | Open | Logan defines RQ |
| **CF-MIG_249-PROPOSED** | Per-manuscript Tables generator for cohort-specific manuscripts (M025, M029, M030, M033, M035-M037, M042-M047) | Open suggestion | Cursor Composer dispatch when ready |
| **CF-METHODS-V17-ADDENDUM** | Methods doc references mig_212-234 but not v17 round (mig_236-244) or mig_245/246/247/248 | Open | Small Cowork-direct file edit (~10 min) |
| **CF-PARATHYROID-EVENT-SAFE** | Events-grain `intact_pth_value_ngL` deferred from mig_243 (grain mismatch) | Open suggestion | Author `semantic_publication.vw_parathyroid_event_safe_VIEW_v1` if Logan needs per-event PTH |
| **CF-LN-METS-ARRAY-EMPTY-2801** | 2,801 LN-positive cases lack histology-attribution evidence | Methods caveat only | Chart-review remediation |
| **Future-Gate6-Col-Registry** | Add gate6 to `qc_audit_dashboard_VIEW_v1` for col_registry dup-key detection | Open suggestion | Small Cowork-direct lane if greenlit |
| **Future-H-Power-BI** | `bi_powerbi.*` star-schema marts | Deferred | Phase 4 trigger; pre-MD-migration parquet refresh may suffice |
| **CF-MD-MIGRATION** | Moving thyroid v1.0 publication out of MotherDuck | In planning | Triggered when Logan picks new platform |

---

## §12 — Architectural decisions made this session

1. **PHI / share rebuild deprioritized** — Cowork discovered the existing share `thyroid_publication_v1_0_readonly` exposes 11,050 clinical_notes_long rows + 25+ note_entities tables. Logan elected NOT to rebuild the share because MD migration is coming; PHI exposure has a finite-lifetime risk. Long-term fix is migration off MD, not share architecture.

2. **mig_245's regex scope was inter-object only** — caught `_v2` → `_VIEW_v2` renames in main but missed intra-table column renames (caught by mig_247's per-row schema check + scheduled for mig_248 repair).

3. **Manuscript registry already exists** — Logan's `manuscript_workspace.manuscript_feasibility_v1` (83 rows) + `manuscript_dive_map_v1` (63 manuscripts → 31 Dives) was already 80% of what ChatGPT proposed building. mig_246 added the live JOIN dashboard; no new schema needed.

4. **Lane M scope clarified** — Tables 1-5 are aligned to whole-cohort manuscripts (M032, M038, T1-thematic series). The 16 cohort-specific READY_TO_DRAFT manuscripts each need their own per-manuscript Tables — proposed as mig_249.

5. **M032 manuscript drafted from existing artifacts** — no new MD analyses required for the first draft. Demonstrates the path others can follow.

---

**End of v20 handoff. The new chat begins with §3 first-action checklist (verify mig_248). Most likely next action: Path-C verify mig_248 → re-score feasibility → resume manuscript drafting (M039 likely next-up; M038 needs RQ; M032 already drafted).**
