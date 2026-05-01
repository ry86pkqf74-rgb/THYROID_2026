# Parallel Agent Batch — v15 Round (post v14 close, post Prompt 4 + Prompt 5 in flight)

**Generated:** 2026-05-01 by Cowork (post-v14 verification of Prompts 1–4 + Prompt 5 Parquet export in flight)
**For:** Logan to dispatch in parallel across 3 agents
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `95ef464` — `docs(qc): ISSUE_REGISTRY refresh + Lane LN mig_224-229 run log`
**MotherDuck state at write:** 5-gate v2 = **209 / 0 / 0 / 0 / 0** (208 distinct + 1 dup row to be cleaned in mig_231) ✓ — cohort parity 10,871 / 10,871 / 10,871 ✓

---

## TL;DR — 4 prompts, 3 agents, all parallel-safe by design

| # | Lane | Agent / model | Mig labels | Dispatch | Est. time |
|---|---|---|---|---|---|
| 1 | **mig_232 — narrow-ACR-missing view (CF-mig219 follow-up)** | **Cline Sonnet 4.6** | `mig_232` | Now | 30–45 min |
| 2 | **Lane M — Manuscript Methods + Table 1 refresh** | **Cursor Composer** | `mig_234` (CSV regen) + Methods .md | Now | 90–150 min |
| 3 | **Lane J — CPM 24-na column audit** | **Cline GPT-5.5** | `mig_235` (col_registry refresh) | Now | 60–90 min |
| 4 | **mig_233 — qc_audit_dashboard snapshot view** | **Cline Sonnet 4.6** | `mig_233` | Now (after #1 commits) | 20–30 min |

**Non-overlap matrix:** #1 touches `manuscript_workspace.vw_us_nodule_*` (new view, no UPDATE on existing). #2 touches `manuscript_workspace.manuscript_*` analytic tables + `docs/Methods_*.md` (file-only) — no overlap with active Lane LN/G surfaces. #3 reads-then-UPDATEs `main.canonical_column_verification_registry_v1` for the 24 CPM `na` cols only — no overlap with anything else. #4 builds `manuscript_workspace.qc_audit_dashboard_VIEW_v1` (new view, read-only over registries). **Zero write conflicts.**

**Cowork-direct concurrent work** (pre-batch dispatch):
- **mig_231** — micro-cleanup of two registry artifacts from v14 (Lane G `signoff_migration` path mismatch + Lane LN `dim_histology_standardized_VIEW_v1` duplicate row). Cowork applies directly + commits before batch dispatch.

---

## §1 — Prompt 1: mig_232 — narrow-ACR-missing view (Cline Sonnet 4.6)

**Why Cline Sonnet 4.6:** Mechanical view DDL based on Copilot's already-published CF-mig219 reconciliation crosstab. Single-shot, cheap, no architectural decisions.

**Mig label:** `mig_232_vw_us_nodule_tirads_derived_acr_missing_20260501`

### Context

Copilot's CF-mig219 reconciliation report (`qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260501.md`) closed CF-mig219 with semantic clarification: the live `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` (24,371 rows) reflects descriptor-incomplete TIRADS (per mig_221 semantics), not derived-ACR-missing. ChatGPT's expected 8,243 corresponds to a different filter that does not currently exist as a named view.

Build that named view now so manuscript Methods can choose between the two cohorts unambiguously.

### Prompt

> **mig_232 dispatch — v15 round.** Read `qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260501.md` end-to-end before tool use, especially the "Diagnosis" section + the descriptor-state × derived-state crosstab.
>
> **Build:** `manuscript_workspace.vw_us_nodule_tirads_derived_acr_missing_VIEW_v1`
>
> **Filter logic** (extracted from the Copilot crosstab — should target ~7,304 rows = `descriptor_incomplete` ∧ `derived_points_or_category_missing`):
> ```sql
> CREATE OR REPLACE VIEW manuscript_workspace.vw_us_nodule_tirads_derived_acr_missing_VIEW_v1 AS
> SELECT
>   research_id, us_exam_id, nodule_index_within_exam, exam_date,
>   tirads_reported_in_text, updated_tirads_category, acr2017_tirads_category,
>   acr2017_tirads_points, acr2017_feature_points_complete,
>   -- new derived col making intent explicit
>   CASE
>     WHEN acr2017_tirads_points IS NULL OR acr2017_tirads_category IS NULL THEN TRUE
>     ELSE FALSE
>   END AS derived_acr_missing,
>   'mig_232_narrow_acr_missing_filter' AS view_filter_provenance
> FROM manuscript_workspace.vw_us_nodule_tirads_any_reported_VIEW_v1
> WHERE acr2017_feature_points_complete = FALSE
>   AND (acr2017_tirads_points IS NULL OR acr2017_tirads_category IS NULL);
> ```
>
> Note: this is the narrow definition. If row count materially differs from ~7,304 (Copilot's `any_reported_descriptor_incomplete_and_derived_missing`), surface to Logan with the exact delta and your reasoning before applying.
>
> **Pre-snapshot to archive:** N/A (new view; nothing to snapshot).
>
> **Register in signoff + col registries:**
> - INSERT row into `main.canonical_table_signoff_registry_v1`: `schema_name='manuscript_workspace'`, `table_name='vw_us_nodule_tirads_derived_acr_missing_VIEW_v1'`, `table_status='verified'`, `signoff_migration='qc_framework_v1/migrations/232_vw_us_nodule_tirads_derived_acr_missing_20260501.sql'`, `priority_tier='tier2_canonical_view'`.
> - INSERT col rows for every output column with `verified_by='cline_sonnet_4_6_mig_232'`, `batch_id='mig_232_narrow_acr'`, `verification_method='view_ddl_with_explicit_filter_provenance'`.
>
> **Provenance row** in `manuscript_workspace.cpm_reconciliation_provenance_v1` with `run_id='mig_232_narrow_acr_v15'`.
>
> **Output**:
> - SQL file: `qc_framework_v1/migrations/232_vw_us_nodule_tirads_derived_acr_missing_20260501.sql`
> - Memory note: `memory/project_mig_232_narrow_acr_view_closeout_20260501.md` documenting why the view exists alongside `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` (the descriptor-incomplete vs derived-missing distinction).
>
> **Acceptance:**
> - View exists in `manuscript_workspace`; row count between 7,200 and 7,400 (per Copilot crosstab evidence)
> - Registered as verified (gate1 += 1)
> - 5-gate v2 audit otherwise unchanged (gate2-5 stay 0)
> - Surgical git add per `feedback_surgical_git_add.md`; commit `feat(qc): mig_232 narrow ACR missing view (CF-mig219 follow-up)`; push.

---

## §2 — Prompt 2: Lane M — Manuscript Methods + Table 1 refresh (Cursor Composer)

**Why Cursor Composer:** Multi-file, multi-deliverable (Methods .md + 5 analytic CSVs + cohort flow + Reproducibility section), needs file-system + git context, requires reading the full canonical layer + Lane G safe views to author the manuscript-facing surface text. Composer's strength.

**Mig label:** `mig_234_table1_csv_refresh_20260501` (CSV/data part) + `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` (text part)

### Context

`mig_204` (commit `bb6d8b6`, 2026-04-29) populated the original Table 1 + 5 analytic template CSVs from live MD. That snapshot is now 7+ migs stale. Many things have changed since:
- Lane B mig_212 dedup VIEW (5,944 distinct tumor rows)
- Lane C mig_213 recurrence quarantine flag (132 rows quarantined)
- Lane D mig_214 patient-level molecular evidence flag
- Lane E1-E6 TIRADS work (mig_215/216/219/220/221)
- Lane F mig_222 multi-nodule absorption
- Lane G mig_223 semantic_publication layer (this is the SSOT going forward)
- Lane LN mig_224-229 (LN/histology safe views + borderline quarantine flag — 27 rows quarantined)
- Soon: mig_232 narrow ACR view (Prompt 1) + mig_233 audit dashboard (Prompt 4)

Refresh against current state using `semantic_publication.vw_*_safe_VIEW_v1` as the SSOT. Methods text must reference `release_manifest_v1` for reproducibility.

### Prompt

> **Lane M dispatch — v15 round.** Read these inputs before tool use:
> - `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md` (project context)
> - `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-30_v13.md` (Lane A-F summary)
> - `cursor_prompts/PARALLEL_AGENT_BATCH_20260430_v14.md` §8 round delta projection
> - `qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md` (Lane LN context — quarantine + LN safe views)
> - `qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260501.md` (TIRADS view-name semantics)
> - `qc_framework_v1/ISSUE_REGISTRY.md` (Lane LN run log + historical issue context)
> - The `mig_204` outputs at `qc_framework_v1/migrations/204_*.sql` + any existing manuscript CSVs (find via `find . -name "Table_1*" -o -name "cohort_flow*" -o -name "manuscript_table*" 2>/dev/null`)
>
> **Authoritative read path:** `semantic_publication.*` views from Lane G mig_223. Do NOT pull directly from canonical_* tables for analyst-facing text — go through the safe views so the Methods description matches the manuscript-export semantics. Cross-validate counts against `vw_ln_patient_publication_safe_VIEW_v1` (Lane LN) when LN claims are made.
>
> **Build, in order:**
>
> ### A. `mig_234_table1_csv_refresh_20260501.sql` + Python helper if needed
> Refresh Table 1 (demographics + tumor stage + LN + recurrence + survival) and 4 ancillary analytic template CSVs against live MD via `COPY (SELECT * FROM <view>) TO 'manuscript_outputs/<csv>' (FORMAT CSV, HEADER)`. Use `semantic_publication.vw_*_safe_VIEW_v1` as the source. Match the schema of the `mig_204` originals (compare schemas before writing).
>
> Required CSVs (in `manuscript_outputs/v1_0_20260501/`):
> 1. `Table_1_cohort_demographics_v1_0_20260501.csv`
> 2. `Table_2_tumor_stage_distribution_v1_0_20260501.csv`
> 3. `Table_3_LN_summary_safe_v1_0_20260501.csv` (NEW — uses `vw_ln_patient_publication_safe_VIEW_v1`)
> 4. `Table_4_recurrence_survival_v1_0_20260501.csv` (uses quarantine flag)
> 5. `Table_5_molecular_distribution_v1_0_20260501.csv` (uses `is_patient_level_only_evidence` flag)
> 6. `cohort_flow_v1_0_20260501.csv` — CONSORT-style cohort selection from raw → final cohort with exclusion counts at each step (incl. 27 borderline-quarantined, 132 recurrence-quarantined, etc.)
>
> ### B. `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`
> Single Methods section draft for the manuscript. Sections required:
>
> 1. **Data sources** — institutional EHR + research registry; date range; total raw vs cohort.
> 2. **Cohort definition** — link to cohort_flow CSV; explicit reference to `release_manifest_v1` + `git_commit_hash` for reproducibility.
> 3. **Pathology adjudication** — `canonical_path_malignant_events_dedup_VIEW_v1` (n=5,944 distinct tumor rows from 4,022 patients); borderline/benign quarantine (FTUMP + follicular adenoma with N1/M1 staging, n=27 rows); histology vocab normalization (`histology_vocab_normalization_map_v1`, n=104).
> 4. **TIRADS adjudication** — describe the four `vw_us_nodule_tirads_*_VIEW_v1` cohort views (strict ACR2017, any reported, descriptor-not-fully-parsed, narrow-ACR-missing — that last is mig_232 in flight); specify which view is the manuscript denominator for which claim.
> 5. **Lymph node analysis** — `vw_ln_surgery_publication_safe_VIEW_v1` + `vw_ln_patient_publication_safe_VIEW_v1` + `vw_ln_histology_attribution_VIEW_v1`; explicit mention of the 4 confidence categories + the LN denominator source-priority rule from Open-Question 3.
> 6. **Recurrence + survival** — `canonical_recurrence_resolved_v1` with `is_implausible_date_quarantine=FALSE` filter (132 quarantined); `canonical_survival_followup_v1`.
> 7. **Molecular** — `canonical_molecular_genetics_v2`; patient-level evidence flag.
> 8. **Reproducibility** — explicit `release_id='pub_v1_0_20260501'` reference + how to re-run analyses against the frozen Parquet export (mig_230, in flight) + the GitHub commit hash + the MotherDuck DB name.
> 9. **Limitations** — open carry-forwards (`CF-LN-METS-ARRAY-EMPTY-2801` → 2,801 of 2,847 LN-positive cases lack histology-attribution evidence; affects tumor-type-specific LN claims).
>
> ### C. `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib` (optional but encouraged)
> BibTeX entries for any methods-section citations (TIRADS ACR 2017, AJCC8, WHO histology classifications). At minimum 5 entries.
>
> **Acceptance:**
> - All 6 CSVs exist in `manuscript_outputs/v1_0_20260501/` with non-zero rows
> - Methods .md is 6–10 pages of structured prose; cohort numbers match live MD; manuscript safe view names cited verbatim
> - `mig_234` SQL exists at `qc_framework_v1/migrations/234_table1_csv_refresh_20260501.sql`
> - Provenance row in `cpm_reconciliation_provenance_v1` with `run_id='mig_234_table1_refresh_v15'`
> - 5-gate v2 audit unchanged (these are read-only against MD; CSV writes are local FS)
> - Surgical git add the .md + .sql + the 6 CSVs (NOT the entire `manuscript_outputs/` dir); commit with message `docs(manuscript): Lane M Methods + Table 1 refresh against v1_0 release_manifest`; push.

---

## §3 — Prompt 3: Lane J — CPM 24-na column audit (Cline GPT-5.5)

**Why Cline GPT-5.5:** Deep, autonomous, multi-step reasoning over the 24 columns; needs to read each column's history (pre-build NULL handling, intentional na vs deferred), cross-reference verification methodology, and produce a structured per-column verdict. GPT-5.5's reasoning is the right fit; not a mechanical task.

**Mig label:** `mig_235_cpm_na_col_audit_20260501` (col_registry refresh if any reclassifications happen)

### Context

`canonical_patient_master` has 1,630 cols total: **1,606 verified + 24 na** per v12 round close-out (`COWORK_HANDOFF_PROMPT_2026-04-30_v12.md` §1). The `na` flag is a workflow shortcut for "verified-by-omission" cols where data is intentionally absent (e.g., placeholder cols for future RAI cycles never administered, or schema cols inherited from a parent table that don't apply at the patient grain).

But the `na` flag is also a tempting destination for "I can't verify this right now" deferrals. We need to confirm each of the 24 is genuinely na with a documented reason — not an unaudited deferral.

### Prompt

> **Lane J dispatch — v15 round.** Audit the 24 `na`-flagged columns in `canonical_patient_master` for genuine na vs unaudited deferral. Read these inputs before tool use:
> - `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md` §1 (CPM scope: 1,606 v / 24 na)
> - `memory/feedback_surgical_git_add.md` + `memory/feedback_phi_safety.md` + `memory/reference_protocol_v2_md_accounts.md`
>
> **Step 1 — Probe MD for the 24 na cols + their metadata:**
>
> ```sql
> SELECT column_name, verification_status, verified_by, verification_method,
>        batch_id, verified_ts, na_rationale
> FROM main.canonical_column_verification_registry_v1
> WHERE schema_name='main' AND table_name='canonical_patient_master'
>   AND verification_status='na'
> ORDER BY column_name;
> ```
>
> Expected: 24 rows. Note any with NULL `na_rationale` — those are the highest-priority audit candidates.
>
> **Step 2 — For each of the 24 cols, classify into one of:**
>
> - **`na_genuine`**: data is intentionally absent. Examples: a placeholder col for `rai_cycle_5_dose` when the cohort max is 4 cycles; a parent-table inherited col that doesn't apply at patient grain (e.g., per-event stage cols).
> - **`na_deferred_should_verify`**: data is present in the col but the verification work was skipped. Should be reclassified as `verified` after rule-based check, or as `not_started` if more work needed.
> - **`na_failed_in_disguise`**: col has data but data quality is below threshold (e.g., free-text that was never normalized). Should be reclassified as `failed`.
>
> **Step 3 — Apply reclassifications via `query_rw`:**
>
> Per row, UPDATE `canonical_column_verification_registry_v1` to:
> - Add a `na_rationale` text col (if missing — DDL change, pre-snapshot first) with the explicit reason for `na_genuine`.
> - Reclassify `na_deferred_should_verify` → `verified` with `verification_method='lane_j_na_audit_rule_check'` (after Cowork-direct re-derivation if rule is clean).
> - Reclassify `na_failed_in_disguise` → `failed` with `verification_method='lane_j_na_audit_data_quality'`.
>
> **Step 4 — Update n_verified / n_na / n_failed counts on the table-level signoff registry row** to match the new col-level state. Should still net to 1,630 cols total; expect `n_failed` to grow if any were `na_failed_in_disguise`. Do NOT reset `table_status` from `verified` if `n_failed > 0`; instead surface the count + rationale to Logan and ask whether to keep CPM at `verified` (with documented failures) vs revert to `partially_verified`.
>
> **Step 5 — Write outputs:**
> - SQL file: `qc_framework_v1/migrations/235_cpm_na_col_audit_20260501.sql` (UPDATE statements)
> - Report: `qc_framework_v1/reports/cpm_24_na_audit_20260501.md` — 24 rows × 5 cols (col_name, original na_rationale, audit verdict, action taken, evidence) + summary counts
> - Memory note: `memory/project_lane_j_cpm_na_audit_20260501.md`
> - Provenance row in `cpm_reconciliation_provenance_v1` with `run_id='mig_235_cpm_na_audit_v15'`
> - Pre-snapshot of `canonical_column_verification_registry_v1` before any UPDATE (snapshot to `archive_pub_v1_0` named `canonical_column_verification_registry_v1_pre_mig235_20260501`)
>
> **Acceptance:**
> - 24 rows audited; each has a documented verdict + action
> - 5-gate v2 audit clean (gate3 might need review if reclassifications happen — but should still net 0 if updates are atomic)
> - Cohort parity stays 10,871/10,871/10,871
> - Logan-touch surface: if any of the 24 are `na_failed_in_disguise`, surface to Logan via the report + AskUserQuestion (or wait for ratification before applying the failed reclassification)
> - Surgical git add the .sql + .md + memory file; commit `feat(qc): Lane J — CPM 24-na column audit (mig_235)`; push.

---

## §4 — Prompt 4: mig_233 — qc_audit_dashboard snapshot view (Cline Sonnet 4.6)

**Why Cline Sonnet 4.6:** Mechanical view DDL over the verification suite. Small scope, deterministic. Dispatch AFTER mig_232 commits so the dashboard's gate1 count is right.

**Mig label:** `mig_233_qc_audit_dashboard_VIEW_20260501`

### Context

The 5-gate v2 audit at `qc_framework_v1/queries/cowork_verification_suite_20260430.md` §1 is the SSOT for cleanliness state. Currently you re-run it manually. mig_233 builds a single-query snapshot view so any agent can probe lakehouse health in one call.

### Prompt

> **mig_233 dispatch — v15 round (gated on mig_232 commit landing).** Pre-flight check:
>
> ```bash
> git log --grep "mig_232" --oneline | head -1
> ```
>
> If empty → STOP, wait for Cline mig_232 to commit. Otherwise proceed.
>
> **Build:** `manuscript_workspace.qc_audit_dashboard_VIEW_v1`
>
> The view rolls up 5 gates + cohort parity + open CFs + most-recent-mig timestamp into one row, refreshable by re-querying.
>
> **DDL** (extract the 5-gate query from `qc_framework_v1/queries/cowork_verification_suite_20260430.md` §1 verbatim, wrap in CTEs, expose as a single-row view with these output cols):
> - `gate1_verified_tables INTEGER`
> - `gate1_distinct_objects INTEGER` (DISTINCT (schema_name, table_name); should equal gate1 unless dup rows exist)
> - `gate2_missing_signoff INTEGER`
> - `gate3_count_mismatch INTEGER`
> - `gate4_verified_cols_missing_metadata INTEGER`
> - `gate5_clinical_date_violations INTEGER`
> - `cpm_pts INTEGER`
> - `us_gland_v2_pts INTEGER`
> - `us_ln_v2_pts INTEGER`
> - `cohort_parity_ok BOOLEAN` (= all three counts equal AND equal 10871)
> - `most_recent_signoff_ts TIMESTAMP`
> - `most_recent_signoff_migration VARCHAR`
> - `dashboard_built_at TIMESTAMP` (= `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`)
>
> **Acceptance criterion at build time** (verifies the dashboard reports correctly):
> ```sql
> SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
> ```
> Should return 1 row with `gate1_verified_tables` ≥ 209 + 1 (mig_232) + 1 (this view's own self-registration) = 211, all gates 2-5 = 0, cohort parity ok = TRUE.
>
> **Register in signoff + col registries** + provenance row with `run_id='mig_233_audit_dashboard_v15'`.
>
> **Output:**
> - SQL file: `qc_framework_v1/migrations/233_qc_audit_dashboard_VIEW_20260501.sql`
> - Memory note: `memory/project_mig_233_audit_dashboard_closeout_20260501.md`
>
> **Acceptance:**
> - View exists; single-row output matches expected
> - Re-running the view returns fresh values (test by INSERT a dummy provenance row + re-query — should reflect new most_recent_signoff_ts)
> - Surgical git add; commit `feat(qc): mig_233 audit dashboard snapshot view`; push.

---

## §5 — Suggested dispatch ordering

```
T+0:    Cowork applies mig_231 (registry cleanup) Cowork-direct + commits
T+5:    Cowork commits + pushes v15 batch doc + mig_231
T+5:    Logan dispatches Prompts 1 + 2 + 3 in parallel
T+30:   Prompt 1 (Cline Sonnet) lands → mig_232 narrow ACR view
T+35:   Logan dispatches Prompt 4 (Cline Sonnet) → mig_233 dashboard
T+90:   Prompt 4 lands → dashboard view live
T+120:  Prompt 3 (Cline GPT-5.5) lands → Lane J CPM na audit + Logan-touch on any failed reclassifications
T+150:  Prompt 2 (Cursor Composer) lands → Methods + Table 1 refresh
T+155:  Cowork verifies all 4 prompts Path-C + writes v15 close-out + v16 handoff
```

**Cowork-direct in parallel** (while agents run):
- After mig_231: monitor Prompt 5 Parquet export (Cline Sonnet 4.6 from v14 batch) for landing → Path-C verify
- Spot-check Lane J's na verdicts as they come in (Cowork can sample-check 5 of 24 to confirm Cline's classification)
- Once mig_232 lands: confirm row count is in 7,200–7,400 band per Copilot crosstab evidence
- Once mig_233 lands: run a quick stress test (insert dummy provenance, re-query, confirm dashboard updates)
- Once Lane M's CSVs land: spot-verify Table_1 patient counts against `vw_patient_master_safe_VIEW_v1`

---

## §6 — Path-C verification (mandatory for every agent commit)

Per `COWORK_HANDOFF_PROMPT_2026-04-30_v12.md` §7. Same protocol as v14 batch.

---

## §7 — Round delta projection (post-batch)

| Metric | v14 final | v15 projected | Δ |
|---|---:|---:|---:|
| 5-gate gate1 (registry rows) | 209 | **212–213** | +3 (mig_231 fixes dup −1; mig_232 +1; mig_233 +1; mig_234 +0 CSVs not registered; mig_235 +0 reclass-only) |
| 5-gate gates 2-5 | 0/0/0/0 | **0/0/0/0** | unchanged |
| Cohort parity | 10871/10871/10871 | **same** | unchanged |
| Open CFs | per ISSUE_REGISTRY | -1 (CF-mig219 closed-with-narrow-view-built) + possible +N from Lane J failed reclass | net -1 to +5 |
| Manuscript readiness | READY | **READY + Methods drafted + Table 1 refreshed + audit dashboard live** | → ready-to-write |

---

## §8 — Quick links

- [v14 prompt batch (predecessor)](computer:///Users/ros/THyroid 2026/cursor_prompts/PARALLEL_AGENT_BATCH_20260430_v14.md)
- [v14 pre-flight findings](computer:///Users/ros/THyroid 2026/qc_framework_v1/reports/cowork_direct_findings_20260430_v14_preflight.md)
- [CF reconciliation report (mig_232 input)](computer:///Users/ros/THyroid 2026/qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260501.md)
- [Lane LN assessment plan](computer:///Users/ros/THyroid 2026/qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md)
- [ISSUE_REGISTRY (Lane M input)](computer:///Users/ros/THyroid 2026/qc_framework_v1/ISSUE_REGISTRY.md)
- [Verification suite v2 (mig_233 source)](computer:///Users/ros/THyroid 2026/qc_framework_v1/queries/cowork_verification_suite_20260430.md)
- [v12 handoff (Lane J input — CPM scope)](computer:///Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

**End of v15 parallel agent batch. Dispatch Prompts 1, 2, 3 immediately; Prompt 4 after Prompt 1 commits.**
