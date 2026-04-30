# Cowork Handoff Prompt v11 — Thyroid Canonical Publication v1.0

**Generated:** 2026-04-30 (post-188b/186b/185b/187 chain + mig_160b + mig_201/203 + mig_191 audit)
**Tip of `origin/main` at handoff:** `bb6d8b6` (verify with `git fetch && git log --oneline -25`)
**Supersedes:** v10 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v10.md`

---

## §0 First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v11.md` end-to-end before any tool use. Then complete the §11 first-action FULL SCOPE REVIEW (git fetch + log, run the v11 5-gate audit query from §14, list all in-flight Cursor prompts in `cursor_prompts/`, read the latest 5 close-out reports + the TODO queue at `qc_framework_v1/TODO_QUEUE.md`).
>
> **Standing context:** I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're in the final ~5% cleanup of the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`) for manuscript-grade survival/recurrence/outcomes analyses. **62/62 Tier-2 canonicals verified (100%); PM 1,606 v / 24 na / 1,630 physical cols (post mig_203).** Manuscript readiness verdict is **READY**; v11 five-gate audit is **172 / 0 / 0 / 0 / 0** on live MD. You're the orchestrator + verifier + applier; Cursor agents do the bulk authoring/lane work; I'm the final ratifier.
>
> **You have:**
> - **Desktop Commander MCP** for git/shell on my actual Mac
> - **MotherDuck MCP** (read-only `query` + `query_rw`) — primary DB `thyroid_canonical_publication_v1_0`; archive DB `"Thyroid 2026 UPdated".archive_pub_v1_0`
> - **GitHub repo** at `/Users/ros/THyroid 2026` (URL `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`)
> - **Auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
>
> **CURRENTLY IN FLIGHT (verify each summary as Logan pastes it):**
> 1. **mig_193** r1b/r1d/r1e + r1c CSV regen post-mig_188b (Cline + GPT-5.5; prior-round-pending if not yet regenerated)
> 2. **mig_198** mig_194 Option B apply (shell-only US gland v2 events/rollup; closes CF-117-US-GLAND-PARENCHYMA) — Cursor Composer
> 3. **mig_202** Script 366 Python source audit + fix (CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION) — Cline + GPT-5.5
>
> **LANDED THIS SESSION / PRIOR ROUND (facts on repo + MotherDuck; no longer dispatch):**
> - ✅ **mig_191** post-apply audit + **`v1_0_manuscript_readiness_report_post_mig187_20260430.md`** — see `qc_framework_v1/reports/mig_191_post_apply_audit_v11_20260430.md` + exports `exports/mig191_post_apply_audit_20260430/`
> - ✅ **mig_201** disposition-C CF registry hygiene — provenance row `mig_201_disposition_c_cf_closure_apply_20260430`
> - ✅ **mig_203** gate5→0 via `cleanliness_audit_v11.sql` + PM bumps — provenance row `mig_203_gate5_zero_audit_allowlist_extension_20260430`
> - ✅ **mig_204** manuscript CSV population — git tip includes `mig_204: populate Table 1 + cohort flow + …` (**bb6d8b6**)
>
> **CRITICAL RIGOR REMINDER:** verify all Cursor/Cline work directly against MotherDuck. EVERY round has shipped with agent QA misses Cowork had to clean up — Path-C protocol mandatory. Probe live MD for the agent's batch_id; should be 0 rows pre-apply (read-only governance). Verify col existence + dependent VIEW recompile before any ALTER. Pre-snapshot every mutating lane to `archive_pub_v1_0`.
>
> **First task:** §11 first-action checklist (FULL SCOPE REVIEW — don't skip). Then choose A/B/C from §13:
> - **(A)** Cursor lane summary just arrived → verify per §6.2 Path C
> - **(B)** Logan ratified a pending decision → author + apply final SQL
> - **(C)** Residual apply queue (**mig_198** primarily; mig_193/mig_202 as authoring lands) — low-risk hygiene / Path C
>
> I'll paste agent summaries from in-flight Cursor runs separately as they come in — verify each against live MD per Path C and apply if AGENTS-governance was respected.

---

## §1 Project mission

**Logan Glosser**, Emory thyroid-cancer surgery researcher. Database: `thyroid_canonical_publication_v1_0` on MotherDuck (account `logan.glosser.eras@gmail.com`). Goal: manuscript-grade survival/recurrence/outcomes analyses on a single-institution thyroid cancer cohort.

- Cohort: **10,871 distinct research_id**
- Backbone: `canonical_patient_master` (**1,630** cols live; **1,606** verified, **24** na, 0 not_started — post mig_203 resync)
- Tier-2 events / patient_rollup canonicals: **62/62 verified (100%)**
- Authoritative SSOT: live MotherDuck — never trust prior summaries

**You are: orchestrator + verifier + applier.** Cursor agents do bulk lane work; Logan ratifies clinical decisions; you verify against live MD per Path C, apply if AGENTS-governance respected, ship b-cleanup migrations for any agent QA misses.

---

## §2 Current state (git `bb6d8b6` + live MotherDuck)

| Metric | Value |
|---|---:|
| Origin tip | `bb6d8b6` (verify after `git pull`) |
| **gate1 (verified canonicals)** | **172** |
| gate2/3/4 | 0 / 0 / 0 ✓ |
| gate5 (v11 audit; `cleanliness_audit_v11.sql`) | **0** ✓ |
| **Tier-2 canonicals at table_status='verified'** | **62/62 (100%)** |
| **PM signoff** | **1,606 v / 24 na / 0 not_started / 1,630** (`verified`) |
| Cohort parity | 10,871 / 10,871 ✓ |

### Recently closed CFs (chain + hygiene)
- **CF-87-AJCC** — closed by mig_188b. Manuscript SQL prefers `*_resolved`.
- **CF-mig171b-EXAM-MASTER-REBUILD** — closed by mig_187 R-A. G9 PASS.
- **CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION** — closed by mig_203 (v11 audit template + registry refresh).
- **Four disposition-C registry tags** — closed by mig_201 (see provenance `mig_201_disposition_c_cf_closure_apply_20260430`).

### Newly opened CFs (informational; chain carry-forward)
- **CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION** — Python source bug; live VIEW patched. mig_202 fixes Python.
- **CF-mig186-WHO-2017-NIFTP-RECLASS** — 220 events excluded; preserved in indeterminate landing.
- **CF-mig186-EDGE-NO-MALIGNANT-EVENT-AFTER-EXCLUSION** — ~115 edge patients.
- **CF-mig185-EVENT-GRAIN-SOURCE-DISTINCT-PRESERVED** — 525 source-distinct dups flagged on events.

### Remaining open CFs
- **CF-117-US-GLAND-PARENCHYMA** (28 cols) — mig_198 Option B closes.
- **CF-117-US-EXAM-ID-PORTABILITY** (53 cols) — partial; remaining = US-nodule rebuild (deferred to v2 future).
- **7 mid-tier CFs** disposition B (manuscript appendix candidates) — see mig_190 report.

---

## §3 Tools & access

### §3.1 Desktop Commander (push to GitHub via Logan's actual Mac)
```
mcp__Desktop_Commander__start_process({command:"zsh", timeout_ms:5000})
mcp__Desktop_Commander__interact_with_process({pid, input:"cd '/Users/ros/THyroid 2026' && git push origin main"})
mcp__Desktop_Commander__force_terminate({pid})
```
Restart bash if process dies between calls (no session continuity). Per `feedback_use_desktop_commander_first.md`: Desktop Commander > Claude in Chrome > computer-use.

### §3.2 GitHub repo
- **Path:** `/Users/ros/THyroid 2026` (mounted as Cowork workspace folder — full read/write)
- **URL:** `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`
- **Branch:** `main` tracked to `origin/main`; tip at handoff **`bb6d8b6`**
- **Author:** `Logan Glosser <logan.glosser@gmail.com>` for all commits
- Surgical git add ONLY — explicit paths/file-globs; never `-A` or `scripts/output/` (per `feedback_surgical_git_add.md`)

### §3.3 MotherDuck
- **Tools:** `mcp__eaae7896-...__query` (read-only) + `mcp__eaae7896-...__query_rw` (writes)
- **Primary DB:** `thyroid_canonical_publication_v1_0` (live publication; MD account `logan.glosser.eras@gmail.com`)
- **Archive DB:** `"Thyroid 2026 UPdated"` schema `archive_pub_v1_0`
- **DuckDB quirks:**
  - `CURRENT_TIMESTAMP` is TIMESTAMPTZ → cast to TIMESTAMP for build_ts cols
  - FILTER not supported on window funcs (use SUM(CASE) OVER)
  - Cross-DB FROM in canonicals forbidden (`main.*` only, except for archive snapshots)
  - `ALTER COLUMN ... SET DATA TYPE T USING <expr>` works for in-place retypes
- **MCP wrapper:** one statement per call — do NOT use `BEGIN TRANSACTION;` / `COMMIT;`

### §3.4 Cursor agents
Logan runs Cursor agents on his other machine to author bulk SQL. AGENTS governance: agents commit SQL only; **Cowork applies via Path C after independent verification.** Cursor prompts in `cursor_prompts/`; Logan dispatches. Logan pastes agent summaries to you when each lane lands.

### §3.5 Tool routing per Cursor/Cline prompt

| Tool | Best for |
|---|---|
| **Cursor Composer** | Multi-file edits + pattern-following from existing repo (mig_171b → mig_198 mirror; v10 → v11 doc transformation; mig_184_v2 → mig_188b patches). IDE-integrated; sees codebase context easily. |
| **Cline + GPT-5.5** | Investigative judgment + diagnostic reasoning + complex multi-step workflows. Use when the answer requires "figure out what's wrong here" rather than pattern-matching. mig_194 NLP unblock; mig_193 r1b 0-row diagnosis; mig_202 Script 366 source audit. |
| **Cline + Sonnet 4.6** | Long-running SQL execution + clinical interpretation + iterative SQL fixing. Strong at clinical/medical judgment. mig_204 manuscript CSV population + spot-check; future r1d/r1e clinical adjudication tooling. |

### §3.6 Auto-memory
- **Path:** `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
- **Index:** `MEMORY.md` (~150+ entries)

---

## §4 Reference documents

### §4.1 In repo (`/Users/ros/THyroid 2026`)

**Operational docs:**
- `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v11.md` — **this doc**
- `qc_framework_v1/TODO_QUEUE.md` — current TODO queue with decisions/plans
- `qc_framework_v1/runbooks/COWORK_APPLY_RUNBOOK_188_186_185_187_20260430.md` — last applied runbook (reference for future chain apply patterns)

**Recent close-out reports (read first):**
- `qc_framework_v1/reports/mig_191_post_apply_audit_v11_20260430.md` — **mig_191** post-apply audit + evidence pointers
- `qc_framework_v1/reports/v1_0_manuscript_readiness_report_post_mig187_20260430.md` — **SSOT manuscript readiness** after mig_187 chain + mig_203
- `qc_framework_v1/reports/chain_188b_186b_185b_187_closeout_20260430.md` — comprehensive chain apply close-out
- `qc_framework_v1/reports/mig_203_gate5_zero_audit_allowlist_extension_20260430.md` — gate5→0 + PM registry refresh
- `qc_framework_v1/reports/mig_192_apply_readiness_patches_20260430.md` — patch design rationale
- `qc_framework_v1/reports/mig_194_thyroid_us_nlp_source_unblock_20260430.md` — Option B recommendation
- `qc_framework_v1/reports/mig_190_smaller_cf_triage_sweep_20260430.md` — 11 mid-tier CFs classified

**Manuscript scaffolding (mig_195/196/197 deliverables):**
- `qc_framework_v1/manuscript/table_1_cohort_characteristics.sql` (+ `.csv` placeholder)
- `qc_framework_v1/manuscript/cohort_flow_diagram.sql` (+ `.csv` placeholder)
- `qc_framework_v1/manuscript/methods_section_starter.md`
- `qc_framework_v1/manuscript/analytic_templates/0{1..5}_*.sql` + previews
- `qc_framework_v1/manuscript/canonical_methods_footnotes/<table>.md` (~83 footnote files)
- `qc_framework_v1/manuscript/data_dictionary.{sql,csv}`
- `qc_framework_v1/manuscript/supplementary_appendix_starter.md`

**Cursor prompt archive (in `cursor_prompts/`; verify landing state before re-dispatch):**
- `CURSOR_PROMPT_mig191_post_apply_manuscript_readiness_v11_20260430.md` — **LANDED** (reports + exports 2026-04-30)
- `CURSOR_PROMPT_mig193_r1bde_logan_review_csv_unblock_20260430.md` (Cline GPT-5.5; **may still be pending**)
- `CURSOR_PROMPT_mig198_us_gland_v2_shell_only_apply_RATIFIED_20260430.md` (Cursor Composer; **in flight**)
- `CURSOR_PROMPT_mig201_disposition_c_cf_closure_apply_20260430.md` — **APPLIED** (provenance `mig_201_disposition_c_cf_closure_apply_20260430`)
- `CURSOR_PROMPT_mig202_script366_python_source_audit_fix_20260430.md` (Cline GPT-5.5; **in flight**)
- `CURSOR_PROMPT_mig203_gate5_zero_audit_allowlist_extension_20260430.md` — **APPLIED** (provenance `mig_203_gate5_zero_audit_allowlist_extension_20260430`)
- `CURSOR_PROMPT_mig204_populate_manuscript_csvs_run_analytic_templates_20260430.md` — **LANDED** on `main` (**bb6d8b6**)

### §4.2 Auto-memory key files

**Methodology / pattern memories (cross-cutting):**
- `feedback_motherduck_direct_check.md` — verify against live MD every round
- `feedback_clinical_dates_calendar_only.md` — clinical event dates MUST be DATE
- `feedback_no_cross_db_canonical_sourcing.md` — canonicals are `main.*` standalone
- `feedback_findings_vs_staging.md` — anatomic findings primary; staging follows
- `feedback_extraction_faithfulness_llm_canonical.md` — re-derive from upstream WHERE error=0
- `feedback_surgical_git_add.md` — explicit path/glob; never -A
- `feedback_use_desktop_commander_first.md` — Desktop Commander > Chrome > computer-use
- `feedback_audit_regex_word_boundary.md` — gate-5 audit needs word boundaries
- `feedback_alter_view_dependents.md` — type changes break dependent VIEW bodies
- `feedback_ln_only_pt0_prior_thy_upstage.md` — Logan-ratified r1c LN-only rule

**Reference memories:**
- `reference_2digit_year_convention.md` — 20YY rule
- `reference_protocol_v2_md_accounts.md` — MD account gotcha (.eras for Cowork MCP)
- `reference_view_naming_convention.md` — `_VIEW` suffix required
- `reference_canonical_naming_convention.md` — Tier-2 masters naming
- `reference_detail_table_registry_schema.md` — `feeds_master_columns_array` is `VARCHAR[]`
- `reference_duckdb_timestamp_tz.md` — TZ trap; cast to TIMESTAMP

**Recent project memories:**
- `project_2026-04-30_major_round_complete.md` — 6-lane round overview
- `project_mig_160b_closeout_2026-04-30.md` — 21 PM date cols retyped + 4 reusable patterns

---

## §5 Database architecture

### §5.1 Tier structure
- **Tier 1** — `note_entities_llm_*`: raw LLM extraction outputs. Registry-seeded `na` raw-mirror exempt.
- **Tier 2** — `canonical_*_events_v1/v2`: event-grain typed tables. ROW = one event/finding/specimen.
- **Tier 2 rollup** — `canonical_*_patient_rollup_v1/v2`: patient-grain rollups.
- **Tier 3** — `canonical_patient_master`: THE master patient-grain table.

### §5.2 Verification registries
- `canonical_table_signoff_registry_v1` — 1 row per canonical_*. Cols: `n_verified`, `n_na`, `n_not_started`, `n_columns_total`, `signoff_migration`, `table_status`.
- `canonical_column_verification_registry_v1` — 1 row per col. Cols: `verification_status`, `verified_by`, `verification_method`, `batch_id`, `notes`.
- `manuscript_workspace.cpm_reconciliation_provenance_v1` — provenance log.

### §5.3 Indeterminate landing (mig_186b)
- `main.canonical_path_indeterminate_events_v1` — 220 NIFTP/UMP events excluded from malignant cohort but preserved with full audit trail.

### §5.4 New event-grain flag (mig_185b)
- `main.canonical_path_malignant_events_v1.is_source_distinct_duplicate_grain` — BOOLEAN; 525 rows TRUE. Analytic SQL must use `COUNT(DISTINCT (research_id, surgery_episode_id, tumor_ordinal))` for tumor counts.

### §5.5 New AJCC `*_resolved` cols (mig_188b)
- `canonical_path_malignant_events_v1`: `t_stage_ajcc8_resolved`, `n_stage_ajcc8_resolved`, `m_stage_ajcc8_resolved`, AJCC7 equivalents, `ajcc_resolution_source`, `ajcc_resolution_confidence` (8 cols)
- `canonical_patient_master`: same family + `ajcc8_stage_group_resolved`, `ajcc7_stage_group_resolved` (10 cols)
- 60 events have `t_stage_ajcc8_resolved='T0'` for transparency about LN-only / no-primary cases

### §5.6 Exam master extension (mig_187 R-A)
- `canonical_us_exam_master_VIEW_v2` row count 11,759 → **11,880** (+121 LN-NLP-only seeded with deterministic md5 IDs)
- New col `exam_id_source ∈ {NULL (structured), 'ln_nlp_only'}`
- All 6,973 LN events now have `exam_id_source='exam_master_reused'` (G9 PASS)

---

## §6 Workflow: Cowork ↔ Cursor/Cline ↔ Logan

### §6.1 Roles
- **Logan**: clinical-domain expert; ratifies clinical decisions; pastes agent summaries; runs Cursor/Cline agents on his other machine; reviews CSVs that need clinical adjudication.
- **Cursor/Cline agents** (per AGENTS protocol): bulk SQL/markdown authors; commit + push to GitHub but do NOT write to MD. **Watch for governance violations** — Cowork has caught 3+ in prior rounds.
- **Cowork (you)**: orchestrator + verifier + applier + small-fix author. Run Path-C verification on all agent work directly against live MD. Catch violations and shortfalls. Apply registry-only / low-risk lanes directly. Author Cursor prompts for heavier work.

### §6.2 Path C — the standard apply protocol

For any Cursor/Cline-authored migration SQL, do all of these BEFORE any `query_rw`:

1. **Read the SQL file end-to-end** — understand each block + claimed SSOTs
2. **Verify governance**: query live MD for any rows matching the agent's batch_id; if 0, agent honored governance; if >0, agent applied without authorization (governance violation; run §8 retro audit pattern)
3. **Pre-flight probes** (read-only): col count matches prompt; upstream tables live in `main`; cohort parity 10,871; schema compatibility for any UNION ALL or INSERT BY NAME
4. **Cohort-uniformity sweep BOTH directions** on every BOOLEAN flipped:
   - 0 TRUE → Type-B placeholder → reclassify verified→na in `mig_<N>b`
   - 0 FALSE / TRUE-only / NULL → Type-A presence flag → keep verified, add `CF-COHORT-NEAR-UNIFORM-TRUE-<col>` informational note
5. **Date-type check** — `*_date` cols MUST be DATE (not TIMESTAMP/VARCHAR)
6. **Data-type sanity** — numeric measurements as DOUBLE (not VARCHAR-with-units)
7. **Dependent-VIEW recompile risk** — for any ALTER COLUMN, query `information_schema.views`
8. **Cross-source spot-check** on 5+ random rids; trace 1 col's derivation back to upstream
9. **Pre-snapshot** affected registry rows + any data-write tables to `archive_pub_v1_0`
10. **Apply** via query_rw (block-by-block due to MCP wrapper)
11. **Verify post-state**: math, signoff resync, 5-gate audit
12. **Author + apply b-cleanup** for any agent-QA misses
13. **Write traceability close-out report**, commit + push

### §6.3 When to apply directly vs ask Logan

**Apply directly (Cowork-direct):**
- Registry-only Cowork-authored migrations
- Single-col retype with full preservation probe
- Focused data-write with clear rule (after Logan ratifies the rule)
- Path-C-compliant agent SQL where Logan has already ratified the design
- Examples this round: mig_198 (ratified rebuild / Path-C), mig_193 (CSV authoring only), mig_202 (Python fix + Script 366 redeploy)

**Ask Logan first:**
- Cross-canonical reconciles affecting >50 patients with clinical adjudication needed
- Structural schema changes (new tables, dropped tables, mass type changes)
- Clinical definition disputes
- Anything that requires picking between options (R1/R2/R3, Option A/B, R-A/R-B/R-C/R-D, etc.)

### §6.4 Pre-snapshot rule

ALWAYS pre-snapshot before mutating verified canonicals:
```sql
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_<table>_pre_mig<N>_<short>_20260430 AS
SELECT research_id, <affected cols>, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig<N>_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_<table>;
```

---

## §7 Currently-clean / verified state

### §7.1 Closed CFs (chain + subsequent hygiene)
- CF-87-AJCC — closed by mig_188b
- CF-mig171b-EXAM-MASTER-REBUILD — closed by mig_187 R-A
- CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION — closed by mig_203 (v11 audit)
- Four disposition-C registry tags — closed by mig_201 (see provenance row)

### §7.2 Top open CFs

| CF | Col-impact | Closes via |
|---|---:|---|
| **CF-117-US-GLAND-PARENCHYMA** | 28 | **mig_198** (Option B shell-only) |
| **CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION** | — | **mig_202** |
| CF-117-US-EXAM-ID-PORTABILITY (US-nodule remaining) | ~53 | future v2 lane |
| 7 disposition-B tags (mig_190 manuscript appendix) | ~99 | manuscript footnotes only; no further apply |
| CF-mig186-* / CF-mig185-* (informational) | — | manuscript methods / appendix (no blocking apply required) |

### §7.3 PM verified column status (post mig_188b + mig_203)

Live `canonical_table_signoff_registry_v1` for **`canonical_patient_master`**: **1,606** verified column-rows / **24** na / **1,630** total physical columns (counts resynced with mig_203).

---

## §8 In-flight Cursor/Cline lanes (verify when Logan pastes summaries)

| Lane | Tool | Logan ratification needed? | Apply target |
|---|---|---|---|
| mig_191 | Cursor Composer | — | ✅ **COMPLETE** — reports + CSV exports |
| mig_193 | Cline + GPT-5.5 | No (post-apply CSV regen) | Authoring only (CSVs for Logan review) |
| mig_198 | Cursor Composer | Logan ratified Option B in TODO | After authoring → Cowork apply Path-C |
| mig_201 | Cursor Composer | — | ✅ **APPLIED** (registry-only; provenance logged) |
| mig_202 | Cline + GPT-5.5 | No (Python source fix; no MD writes) | Logan/Cowork applies the fix diff to Python + redeploys Script 366 |
| mig_203 | Cursor Composer | — | ✅ **APPLIED** (registry + audit v11 template) |
| mig_204 | Cline + Sonnet 4.6 | — | ✅ **LANDED on main** (**bb6d8b6**) — Cowork CSV spot-check |

### §8.1 Verification approach per lane

For each, when Logan pastes the agent summary:
1. Read agent's SQL/diff/CSV deliverables end-to-end
2. Verify governance: probe live MD for batch_id (should be 0 rows pre-Cowork-apply)
3. Run pre-flight probes per Path C §6.2
4. For apply lanes (**mig_198**): apply per Path C; verify gate3 = 0 after; final 5-gate audit
5. For audit-only lanes (**mig_193**): verify outputs match expected; surface any anomalies
6. For source-fix lanes (mig_202): Cowork applies the Python diff via Desktop Commander, then runs Script 366, then verifies live VIEW state
7. For r1bde regen (mig_193): surface CSVs to Logan for clinical adjudication

---

## §9 Apply queue priority order

After §11 first-action checklist:

1. **Verify each remaining in-flight lane summary as Logan pastes it** — Path-C verify; apply if compliant
2. **mig_198 (US gland v2 shell-only build)** — Path-C apply when SQL lands
3. **mig_202 (Script 366 fix)** — apply Python diff + redeploy via Desktop Commander; re-verify VIEW row count vs NULL-date leakage
4. **mig_193 (CSV regen)** — verify adjudication bundles for Logan
5. **Standing v11 5-gate audit** — expect **`172 / 0 / 0 / 0 / 0`** after every structural lane (baseline now clean on MD)

---

## §10 Pending Logan ratifications (don't touch yet without explicit confirmation)

- **r1c bucket-3 (50 ambiguous PM-only-size events)** — manuscript treatment: keep as `t_resolution_source='ambiguous_pm_size_only_logan_pending'` indefinitely OR Logan hand-curate each
- **r1d adjudication CSV (374 candidate T4 events)** — Logan reviews CSV after mig_193 regenerates it
- **r1e adjudication CSV (168 mixed-histology events)** — Logan reviews after mig_193 regen
- **Methods section voice pass** — mig_195 starter has ~12 placeholders; defer until manuscript writing phase
- **mig_198 Option B vs A vs C** — implicit ratification of Option B per TODO queue; explicit confirmation welcome before apply

---

## §11 First-action checklist

```
1. git fetch origin && git pull --rebase origin main && git log --oneline -25
2. Run §14 v11 5-gate audit (expect **172 / 0 / 0 / 0 / 0** post-mig_203)
3. Check PM batch progress + recent registry activity:
     SELECT n_verified, n_na, n_not_started, table_status, signoff_migration
     FROM main.canonical_table_signoff_registry_v1
     WHERE table_name='canonical_patient_master';
4. Check active in-flight lanes (Cursor/Cline activity since handoff):
     SELECT batch_id, COUNT(*) AS n, MAX(verified_ts)
     FROM main.canonical_column_verification_registry_v1
     WHERE batch_id LIKE 'mig_193%' OR batch_id LIKE 'mig_198%' OR batch_id LIKE 'mig_202%'
     GROUP BY 1 ORDER BY 3 DESC;
     -- Expect: 0 rows (pre-Cowork-apply governance)
5. Read MEMORY.md end-to-end (auto-memory index)
6. Read this v11 handoff doc end-to-end
7. Read TODO_QUEUE.md (top-level state of all open work)
8. Read chain_188b_186b_185b_187_closeout_20260430.md (most recent close-out)
9. Read mig_194 / mig_195 / mig_196 / mig_197 reports (manuscript scaffolding context)
10. Read residual Cursor prompts (**mig_193 / mig_198 / mig_202**) + archived prompts for landed lanes (**mig_191 / mig_201 / mig_203 / mig_204**)
11. Re-read §6.2 Path C protocol + §8 in-flight verification approaches
12. Decide A/B/C from §13
```

**FULL SCOPE REVIEW** — complete steps 1–11 before doing anything else.

---

## §12 Critical reminders

**Verify all agent work directly and thoroughly.** Cursor/Cline agents have produced shortcuts that needed cleanup in EVERY round. Specific patterns to watch:

| Pattern | What happened | Lesson |
|---|---|---|
| mig_185 BEGIN TRANSACTION | Used SQL transactions; MD MCP wrapper bans them | Strip in patch lane |
| mig_186 verification_status='not_started' | Broke gate3 by setting verified cols to not_started | Use verified-with-CF-note instead |
| mig_188 missing T0 | Logan asked for explicit T0; Cursor used implicit | Re-author with explicit T0 |
| mig_187 Script 366 redeploy | Dropped exam_date IS NOT NULL filter from VIEW | Cowork-direct CREATE OR REPLACE VIEW patch |
| mig_188 self-join row count | UPDATE ... FROM reported 7857 rows when only 6689 unique | Verify by querying actual distribution, not row count |
| mig_188 PM signoff stale | ALTER COLUMN ADD didn't bump n_columns_total | mig_203 inserts 10 registry rows + bumps count |

**Standing rules:**
- **Cohort parity 10,871 invariant**
- Run cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions
- Pre-check `information_schema.tables` for every methodology string
- Audit `data_type` for every numeric measurement col
- Check `*_date` cols are DATE not TIMESTAMP/VARCHAR
- **Check MotherDuck directly every round — never trust prior summaries**
- Verify governance compliance before applying any agent SQL (probe live MD for the agent's batch_id; should be 0 rows pre-apply)
- After applying, run 5-gate audit; should remain **172/0/0/0/0** under v11 template unless lane explicitly changes verified inventory
- Pre-snapshot all affected slices to `archive_pub_v1_0` before mutating
- Surgical git add only (explicit paths)
- For ALTER COLUMN: pre-flight scan dependent VIEWs; patch broken ones in same commit

---

## §13 Decision tree

After §11 first-action checklist, decide:

**A. New Cursor/Cline lane summary just arrived from Logan** → verify it via §8 verification approach for that specific lane. Apply if governance-clean. Surface to Logan if ratification needed.

**B. Logan ratified a pending decision** → author + apply final SQL per Path C.

**C. No new Cursor lanes pending** → either (i) author next round of Cursor prompts, or (ii) **apply remaining Path-C work** (**mig_198**), or (iii) consolidate memory + status report.

---

## §14 Standing reference — v11 5-Gate Cleanliness Audit

```sql
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name FROM (VALUES
    ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),('llm_extracted_at'),
    ('verified_ts'),('signed_off_ts'),('registered_ts'),('updated_at'),('created_at'),
    ('promoted_at'),('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
    ('ingestion_date'),('lab_datetime'),
    -- v11 extension (mig_203):
    ('cpm_built_at'),('rollup_built_at'),('resolved_at'),('reclassified_at')
  ) v(col_name)
)
SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified' AND signoff_migration IS NULL) AS gate2,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t WHERE t.table_status='verified' AND (t.n_verified+t.n_na<>t.n_columns_total OR t.n_not_started<>0 OR COALESCE(t.n_failed,0)<>0)) AS gate3,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 r JOIN main.canonical_table_signoff_registry_v1 t USING (schema_name,table_name) WHERE t.table_status='verified' AND r.verification_status='verified' AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL)) AS gate4,
  (SELECT COUNT(*) FROM information_schema.columns c
     JOIN verified_tables v ON c.table_name=v.table_name
     LEFT JOIN main.canonical_column_verification_registry_v1 r ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
   WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
     AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
     AND NOT regexp_matches(c.column_name, '_built_at$')
     AND NOT regexp_matches(c.column_name, '_derived_at$')
     AND NOT regexp_matches(c.column_name, '_resolved_at$')
     AND NOT regexp_matches(c.column_name, '_confidence$')
     AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source'
     AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw'
     AND COALESCE(r.verification_status,'unknown')!='na'
     AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
          OR (c.data_type='VARCHAR' AND (regexp_matches(c.column_name,'(^|_)dates?(_|$)') OR regexp_matches(c.column_name,'(^|_)dt(_|$)'))))
  ) AS gate5;
```

**Expected on current MD (post-mig_203):** `172 / 0 / 0 / 0 / 0` ✓. Pre-mig_203 legacy chain state was `172 / 0 / 0 / 0 / 6` (allowlist gap only — not data defects).

---

## §15 Manuscript readiness — quantified

### Already done (~95%)
✅ All 62/62 Tier-2 canonical tables verified
✅ Patient master backbone 100% verified
✅ AJCC `*_resolved` cols populated; T0 cohort transparently labeled
✅ NIFTP/UMP exclusion with audit trail
✅ Source-distinct duplicate flag on path_malignant
✅ LN-NLP exam-date integration (G9 PASS)
✅ Cohort parity 10,871/10,871
✅ All clinical date cols DATE-typed
✅ v11 five-gate audit **172 / 0 / 0 / 0 / 0** (mig_203 + `cleanliness_audit_v11.sql`)
✅ **mig_191** post-apply manuscript readiness report + audit exports (`exports/mig191_post_apply_audit_20260430/`)
✅ **mig_201** disposition-C registry closures (provenance logged)
✅ Manuscript Table 1 SQL + cohort flow + analytic templates authored
✅ Manuscript Table 1 + cohort flow + template **CSVs populated from live MD** (mig_204 on `main`)
✅ Per-canonical methods footnotes for ~83 tables
✅ Data dictionary CSV/SQL exported

### Remaining hygiene before every CF carries disposition-B footnote automation
- **mig_198** — shell-only US gland v2 events/rollup (**CF-117-US-GLAND-PARENCHYMA**).
- **mig_202** — Script 366 Python source fix (**CF-mig187-SCRIPT-366-…**).
- **mig_193** — regenerate r1b/r1d/r1e + r1c CSV bundles for Logan review.
- r1c bucket-3 / r1d / r1e Logan adjudication queues.
- Methods section Logan voice pass + template placeholder burn-down.

### Estimated time on remaining automate-able scope
- **Cowork-directed apply:** mig_198 Path-C (**~variable** depending on migration size).
- **Cursor/Cline authoring:** mig_193 + mig_202 (**~few hours**, parallelizable).
- **Logan adjudication:** r1*-CSVs (**~half day+** depending on curation depth).

### Manuscript-statistical-analysis-ready RIGHT NOW for:
- Overall survival
- Recurrence-free survival
- Stage group distribution by histology
- Complication rates by surgery type
- Cohort flow / inclusion-exclusion sensitivity analyses

The remaining 5% is operational hygiene + Logan's clinical adjudication of edge cases — none of which prevent core analyses from running today.

---

End of v11 handoff doc.
