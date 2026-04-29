# Cowork Handoff Prompt v5 — Thyroid Canonical Publication v1.0 Cleanup
Generated: 2026-04-29 (very late evening) — supersedes v1/v2/v3/v4
Tip of `origin/main`: `522942e` at handoff time — `git fetch && git pull` before anything

## 0. TL;DR / First Actions

You are continuing a multi-week cleanup of the Thyroid Canonical Publication v1.0 lakehouse on MotherDuck so it can support a manuscript pipeline. **PM is now at 1,441 / 1,598 (90.2%) verified.** ~150 cols remain across mig_152 NLP (in-flight), mig_159 final residual (prompt shipped), mig_160 date-retype (prompt shipped), mig_161 mig_155 retro-verify (prompt shipped), and mig_162 PM finalization (prompt shipped, runs last). 4 Cursor agents are working on those right now.

Read sections 1-6 to orient, then 7-8 for current state, then run §10 first-action checklist. Then choose between (A) verify Cursor lanes as they land + apply via Path C (most likely path), (B) execute the **mig_163 ANY-RECURRENCE clinical-adjudication investigation** described in §9, or (C) generate the next batch of prompts (post-PM finalization, will pivot to non-PM canonicals).

**Hard rules** (Logan-ratified, do not violate):
- Always check MotherDuck directly before recommending — never trust prior summaries (`feedback_motherduck_direct_check.md`).
- Verify Cursor work yourself by querying MD. Cursor agents have produced shortcuts (mig_135 21 degenerate-FALSE, mig_138 447-pt undercount, mig_141 2 near-uniform-TRUE missed, mig_144 VARCHAR units, mig_145 tracheal not_mentioned overreach, mig_147 nucmed-vs-RAI 83% drift, mig_151 archived-table verification_method, mig_154 missed Type-A on lvi/vi, mig_156 missed prm_high_risk Type-B + 349-pt canon-only gap, mig_157 missed high_risk_molecular_v7 Type-B). Don't accept "verified" claims at face value.
- **Cohort-uniformity sweep BOTH directions** on every BOOLEAN col flipped. Watch for (a) near-uniform-FALSE (Type-B placeholder → na) AND (b) near-uniform-TRUE / TRUE-only / 0 FALSE / NULL (Type-A presence flag → keep verified informational).
- Clinical event date columns must be DATE, not TIMESTAMP/VARCHAR (`feedback_clinical_dates_calendar_only.md`). Audit/provenance timestamps exempt.
- 2-digit year → 20YY (`reference_2digit_year_convention.md`, Logan-ratified 2026-04-27).
- PHI safety — never print clinical notes; research_id only; no cloud PHI.
- **Surgical git add** — never `git add -A`. Stage by explicit path.
- Verification methods MUST name LIVE `main.*` tables — pre-check `information_schema.tables`. If table is archive-only, use live successor / explicit `_archive_<snapshot>` suffix / reclassify to na (mig_151b precedent).
- **AGENTS governance** — Cursor agents commit SQL only; Logan/Cowork applies via Path C after independent verification. (mig_155 violated this — flagged in mig_161 retroactive-verify lane.)
- Ask before query_rw / write — describe SQL changes first; wait for explicit go. Logan has been pre-authorizing "fix any issues" in cleanup rounds; still describe what you're about to do.

## 1. Project Mission

Logan Glosser is a thyroid-cancer surgery researcher at Emory. The lakehouse is a multi-domain canonical clinical research database (`thyroid_canonical_publication_v1_0`) on MotherDuck that backs the v1.0 publication. Goal: produce a clean, documented, audit-passing canonical layer with verified columns/tables, ready for the manuscript pipeline (survival analysis, recurrence outcomes, etc.).

Cleanup sub-goals:

1. Every analytic column in `main.canonical_*` is registered in `canonical_column_verification_registry_v1` and signed off in `canonical_table_signoff_registry_v1`.
2. Each column has a real verification method tied to a `batch_id` (`mig_<N>_*`).
3. Standardized values on every analytic column (controlled vocabularies; SSOT enums).
4. Old / archived tables and columns removed (or methodology strings name explicit archive snapshot suffix).
5. Patient-level rollups + view layer aligned with verified events tables.
6. Lakehouse passes the 5-gate cleanliness audit (§11).
7. CFs (carry-forwards) resolved (date-retype batch, lineage fixes, etc.).

## 2. Tools & Access

### 2.1 Cowork environment
You're running in Cowork mode (Claude desktop app). Your scratchpad is a temp dir; the user's workspace folder is `/Users/ros/THyroid 2026` — that's the GitHub repo on disk. Final outputs go there.

### 2.2 Desktop Commander (preferred for git push)
`mcp__Desktop_Commander__*` tools control the user's actual Mac via a shell process. Use this to `git push` because the Cowork sandbox doesn't have GitHub credentials. Pattern:

```
1. start_process({command: "zsh", timeout_ms: 5000})  → returns PID
2. interact_with_process({pid, input: "cd '/Users/ros/THyroid 2026' && git push origin main"})
```

If the bash process dies between sessions, restart with `mcp__Desktop_Commander__start_process`. Per `feedback_use_desktop_commander_first.md`: Desktop Commander > Claude in Chrome > computer-use.

### 2.3 GitHub access
- Logan's repo at `/Users/ros/THyroid 2026` (mounted as Cowork workspace folder). Origin = `origin/main`. URL: `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`. Commit author: `Logan Glosser <logan.glosser@gmail.com>`.
- Workflow: `git add <explicit-path>` only; `git commit -c user.name=... -c user.email=...`; push via Desktop Commander.
- Surgical git add ONLY: `git add scripts/output/` and `git add -A` are FORBIDDEN.
- Lint Python before commit if any .py changed: `python3 -m py_compile <file>` + `pyflakes` (`feedback_commit_workflow.md`).

### 2.4 MotherDuck access
- Tools: `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query` (read-only) and `_query_rw` (writes).
- Primary DB: `thyroid_canonical_publication_v1_0` (live publication)
- Archive DB: `"Thyroid 2026 UPdated"` schema `archive_pub_v1_0` (snapshots before structural mutations — pre-snapshot ANY mutating UPDATE/ALTER here BEFORE applying)
- Auth: on `logan.glosser.eras@gmail.com` MD account (`reference_protocol_v2_md_accounts.md`)
- DuckDB quirks:
   - `CURRENT_TIMESTAMP` returns TIMESTAMPTZ — always `CAST(... AS TIMESTAMP)` for build_ts cols
   - FILTER not supported on window funcs (use `SUM(CASE) OVER`)
   - Cross-DB FROM in canonicals is forbidden — `main.*` only (`feedback_no_cross_db_canonical_sourcing.md`)
   - `ALTER COLUMN ... SET DATA TYPE T USING <expr>` works for in-place retypes; pre-snapshot first

### 2.5 Cursor agents (parallel work on Logan's other machine)
- Logan runs Cursor agents to do bulk verification work in parallel (one prompt per "lane"). Cursor has its own MD + GitHub access via local CLI.
- Cursor agents commit + push themselves with Logan's authorship.
- Logan forwards agent summaries to you for verification. Always verify against live MD; agent summaries are claims, not facts.
- AGENTS governance (Logan-ratified): Cursor agents commit SQL only; Cowork applies via Path C after independent verification. Some agents violate this (mig_155 example) — flag and run retroactive verification.

### 2.6 Auto-memory
At `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`. Persists across sessions. Index in `MEMORY.md`. Read `MEMORY.md` first. ~95 entries — feedback rules, project closeouts, references. The most useful for this round are listed in §3.2.

## 3. Reference Documents

### 3.1 In repo (`/Users/ros/THyroid 2026`)
| Path | What it is |
|---|---|
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v5.md` | This doc |
| `qc_framework_v1/REMAINING_WORK_INVENTORY.md` | Inventory of all canonicals + verification status |
| `qc_framework_v1/VERIFIED_TABLES.md` | Verified-tables log |
| `qc_framework_v1/AGENTS.md` | Continual-learning policy: governance, calendar policy |
| `qc_framework_v1/migrations/*.sql` | All migration sign-off SQL files (1-158 + b-cleanups). Number range relevant for this round: 149-162. |
| `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` | Latest 5-gate audit query |
| `cursor_prompts/CURSOR_PROMPT_*.md` | All Cursor agent prompts (~75 files). The 4 in-flight are listed in §8. |
| `scripts/203b_canonical_recurrence_harmonized_20260429.py` | Lane 19 harmonized recurrence rebuild script |

### 3.2 Auto-memory key files (read first)
**Methodology / pattern memories** (high relevance):
- `feedback_motherduck_direct_check.md` — verify against live MD every round
- `feedback_audit_regex_word_boundary.md` — gate-5 audit needs word boundaries
- `feedback_etevent_resolved_cross_check.md` — event-grain INNER JOIN with `CAST(rid AS VARCHAR)`
- `feedback_recurrence_imaging_n_events_null.md` — NULL not 0 for absent-event case
- `feedback_clinical_dates_calendar_only.md` — clinical event dates MUST be DATE
- `feedback_no_crossdomain_linkage_ids.md` — query-time JOIN, not linked_X_episode_id bloat
- `feedback_alter_view_dependents.md` — CREATE OR REPLACE dependents in same commit
- `feedback_surgical_git_add.md` — explicit path/glob; never -A
- `feedback_use_desktop_commander_first.md` — Desktop Commander > Chrome > computer-use
- `feedback_extraction_faithfulness_llm_canonical.md` — re-derive from upstream WHERE error=0
- `feedback_no_cross_db_canonical_sourcing.md` — canonicals are `main.*` standalone
- `feedback_findings_vs_staging.md` — anatomic findings primary; staging follows findings
- `feedback_mention_grain_partition_probe.md` — probe COUNT vs COUNT DISTINCT before ROW_NUMBER

**Project-state memories** (this and prior rounds):
- `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md` — Lane 19 RESUME closeout (87th canonical)
- `project_recurrence_resolved_v1_mig_125_closeout.md` — SSOT enum `imaging_only_unconfirmed`
- `project_invasion_family_signoff_2026-04-28.md` — invasion family complete
- `project_complications_events_verified_2026-04-28.md` — 8 complication categories
- `project_psh_events_mig_104_closeout.md` — Script 365 deterministic rebuild SSOT
- `project_op_procedure_codes_mig_118_closeout.md` — hybrid pattern #9
- `project_path_gland_family_complete_2026-04-28.md` — rebuild-then-verify pattern
- `project_lab_consolidation_script_347_closeout.md` — Tg+TgAb shared canonical
- `project_medications_parathyroid_families_complete_2026-04-29.md` — STRING_AGG ordering pattern
- `project_round2_llm_integration_script_386_closeout.md` — LLM round-2 integration

**Reference memories**:
- `reference_2digit_year_convention.md` — 20YY rule
- `reference_protocol_v2_md_accounts.md` — MD accounts
- `reference_synoptic_row_ix.md` — synoptic_row_ix is Script 108 pandas-load-order
- `reference_view_naming_convention.md` — `_VIEW` suffix required
- `reference_canonical_naming_convention.md` — Tier-2 masters naming

## 4. Database Architecture

### 4.1 Tier structure
- Tier 1 — `note_entities_llm_*`: raw LLM extraction outputs (per-note, per-domain). Source of truth for unstructured signals. Registry-seeded as `na` raw-mirror exempt.
- Tier 2 — `canonical_*_events_v1`: event-grain typed tables built from Tier 1 + structured upstream. Most cleanup work happens here.
- Tier 2 rollup — `canonical_*_patient_rollup_v1`: patient-grain rollups from events.
- Tier 3 — `canonical_patient_master`: THE master patient-grain table. 1,598 columns. Currently 1,441/1,598 verified (90%). This is the analytic SSOT.

### 4.2 Verification registries
- `canonical_table_signoff_registry_v1` — 1 row per canonical_*. Status = `not_started | in_progress | verified | failed`. Cols: `n_verified`, `n_columns_total`, `n_na`, `n_not_started`, `n_failed`, `signoff_migration`, `table_status`.
- `canonical_column_verification_registry_v1` — 1 row per canonical column. `verification_status`, `verified_by`, `verified_ts`, `verification_method`, `batch_id`, `notes`. The `notes` field accumulates per-migration appendices (separated by `| mig_<N>: ...`).

### 4.3 Naming conventions
- Tier 2 masters: `canonical_<domain>_events_v1` and `canonical_<domain>_patient_rollup_v1`
- VIEWs: `canonical_<domain>_<grain>_VIEW_v<N>` — must carry `_VIEW` suffix
- QA: `qa_*`
- No new `tier2.*` / `verify.*` schemas — being dropped

### 4.4 Verification methods (controlled vocabulary)
- `derivation_vs_canonical_<source>_<col>` — re-derive from upstream
- `extraction_faithfulness_llm_canonical` — re-derive from `note_entities_llm_*` WHERE error=0
- `internal_consistency` — pairwise col rule (e.g., positive ≤ examined)
- `auto_identifier_skip` — research_id, primary keys (na tier)
- `auto_provenance_skip` — build_ts, extracted_at (na tier)
- `cross_validate_event_grain_inner_join_cast_varchar` — for ete/recurrence cross-checks
- `helper_<placeholder>_pending_real_extraction` — for Type-B/C reclassifications (na)
- `extraction_faithfulness_vs_archive_pub_v1_0_<table>_<snapshot_ts>` — when source is archive-only (mig_151b precedent)
- Hybrid combinations OK; spell out the pipeline.

## 5. Workflow: Cowork ↔ Cursor ↔ Logan

### 5.1 Roles
- **Logan** — final ratifier; clinical-domain expert. Reviews proposed fixes, ratifies CFs, decides scope. He's a researcher not an engineer — explanations should match a senior researcher's perspective.
- **Cursor agents** — bulk lane workers. Run on Logan's other machine. Take a Cursor prompt (one lane = one cluster), do MD reads + author SQL, commit, push, and report back. Per AGENTS governance: agents do NOT write to MD; Cowork applies after Path-C verification. Some agents violate this — flag.
- **Cowork (you)** — orchestrator + verifier + applier + small-fix-author. Generate Cursor prompts, fact-check landed work via independent MD probes, propose fixes, apply migrations + small registry-only or focused-data-write fixes via query_rw with pre-snapshot, manage open CFs, write close-out memories, run the 5-gate audit, surface findings to Logan.

### 5.2 Path C — the standard apply protocol

For any Cursor-authored migration SQL:

1. **Read the SQL file end-to-end** — understand what each block does + which SSOTs are claimed.
2. **Pre-flight probes** (read-only MD):
   - Confirm scope (col count matches prompt's expected count)
   - Verify all upstream tables in `verification_method` strings live in `main` (`information_schema.tables`)
   - Confirm cohort parity (PM = 10,871)
3. **Cohort-uniformity sweep** on every BOOLEAN col flipped — both directions:
   ```sql
   SELECT '<col>', SUM(CASE WHEN <col> THEN 1 ELSE 0 END) AS t,
          SUM(CASE WHEN NOT <col> THEN 1 ELSE 0 END) AS f,
          SUM(CASE WHEN <col> IS NULL THEN 1 ELSE 0 END) AS n
   FROM main.canonical_patient_master;
   ```
   Decision rules:
   - **0 TRUE / N FALSE / M NULL** (FALSE-only) → Type-B placeholder → reclassify verified→na in a `b-cleanup` migration
   - **N TRUE / 0 FALSE / M NULL** (TRUE-only) → Type-A presence flag → keep verified, add `CF-COHORT-NEAR-UNIFORM-TRUE-<col>` informational note
   - **>99% TRUE or <1% TRUE AND not Type-A** → investigate clinical plausibility
4. **Date-type check** — any `*_date` cols TIMESTAMP or VARCHAR-with-date-name? Open `CF-mig<N>-CLINICAL-DATE-RETYPE` for follow-up in the global mig_160 retype migration.
5. **Data-type sanity** — any numeric measurements stored as VARCHAR with embedded units? Apply mig_144b retype pattern.
6. **Cross-source spot-check** — pick 5+ random rids, manually trace 1 col's derivation back to upstream. Document evidence in apply commit message.
7. **Cross-canonical reconciliation** for cols that have multiple SSOTs (e.g., recurrence proxies vs canonical_recurrence_v1) — pairwise IS DISTINCT FROM count.
8. **Pre-snapshot** affected registry rows (and any data-write tables) to `archive_pub_v1_0` with explicit name `<table>_pre_mig<N>_<short>_20260429`.
9. **Apply** the SQL via `query_rw` (block by block; one statement per call due to MCP wrapper).
10. **Verify** post-state: `n_verified` math, signoff registry resync, 5-gate audit unchanged.
11. **Author and apply b-cleanup** for any agent-QA misses (Type-A presence flags, Type-B reclassifications, methodology rename, multi-source notes, etc.).
12. **Write traceability SQL files** for any b-cleanup; commit + push.

### 5.3 When to ask Logan vs apply directly
- **Apply directly via query_rw**: registry-only updates, single-col retype with full preservation probe, focused data-write fixes (e.g., re-derive a col from the same upstream with a corrected enum filter), AGENTS-governance-compliant Path-C apply of a Cursor SQL.
- **Ask Logan first**: cross-canonical reconciles affecting >50 patients with clinical adjudication needed, structural schema changes (renames, drops), anything touching PHI semantics, novel SSOT precedence calls, **clinical definition disputes** (e.g., the mig_163 ANY-RECURRENCE plan in §9 — Logan needs to decide what ARF should mean).

### 5.4 Pre-snapshot rule for any data write
ALWAYS pre-snapshot to `archive_pub_v1_0` before mutating PM data or any verified canonical:

```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_<table>_pre_mig<N>_<short>_20260429 AS
SELECT research_id, <affected cols>, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig<N>_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_<table>;
```

## 6. Current State (as of 2026-04-29 late-evening)

### 6.1 Top-line metrics
| Metric | Value |
|---|---|
| Verified canonicals (gate1) | 88 / 175 (50%) |
| In-progress | 1 (`canonical_patient_master`: 1,441 / 1,598 cols ≈ 90.2%) |
| Cross-SSOT drift | 0 between cr / crr (post-mig_153) |
| 5-gate audit | gate1=88, gate2=0, gate3=0, gate4=0, gate5=21 (gate 5 closure pending mig_160) |
| Latest commit | `522942e` |
| Pre-snapshots in archive | many (canonical_column_verification_registry pre_mig142b/149/150/151b/154/156/157, etc.) |

### 6.2 Patient-master cluster progress (lanes that have landed THIS SESSION)
| Lane | mig | Cluster | Cols flipped | Status |
|---|---|---|---|---|
| 39 | 149 | Synoptic-pathology | 32 verified | landed (Path C, mig_149b for Type-A CFs on 12 BOOLEANs) |
| 40 | 150 | Para+postop+TP LN | 37 verified | landed (Path C, mig_150b for Type-A + multi-source PTH note + tumor_pathology not-in-main note) |
| 41b | 151b | Meds+radtx cleanup | 15 method renamed + 5 method renamed + 3 verified→na | landed (post-mig_151) |
| 47 | 158 | rid 68 LN integrity | 1 row data-write | landed |
| 43 | 154 | Pathology invasion | 38 verified | **landed today via Path C** |
| 43b | 154b | Cohort-uniformity CF | 2 CF appendices (lvi/vi Type-A) | **landed today** |
| 44 | 155 | Risk scoring + survival | 31 verified | landed (Cursor agent applied without governance, awaits mig_161 retro-verify) |
| 45 | 156 | Framework + provenance | 71 verified | **landed today via Path C** |
| 45b | 156b | Cleanup | 1 verified→na (prm_high_risk_marker_any) + canon_only_undercount-349pt CF on any_recurrence_flag | **landed today** |
| 46 | 157 | Clinical residual | 60 verified | **landed today via Path C** |
| 46b | 157b | Cleanup | 1 verified→na (high_risk_molecular_v7) | **landed today** |

### 6.3 Critical session findings to carry forward

**The "any_recurrence_flag derivation gap" (CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT)**:
- PM `any_recurrence_flag`=TRUE: 384 patients
- canonical_recurrence_v1.recurrence_confirmed=TRUE: 514 patients
- Both: 165, PM-only: 219, **canon-only: 349**
- Naive OR rule (biochemical OR structural OR distant OR canonical) would flip 1,805 patients TRUE (jump 384→2,187), suggesting `any_recurrence_flag` has a stricter clinical definition than naive union
- **Detail in §9 below — needs Logan ratification before mig_163**

**PM is a MIXED-COHORT table**: `is_malignant`=4,137 TRUE / 6,734 FALSE. 62% of PM is benign. Manuscript-pipeline analyses on cancer-only outcomes need explicit `is_malignant=TRUE` filter. Worth documenting prominently in mig_162 manuscript-readiness report.

## 7. In-Flight Cursor Lanes — Active When This Doc Was Written

Logan launched these from prompts in `cursor_prompts/`. They will report to him when done; he'll forward summaries to you for verification. Per the v4 prompt batch:

| Lane | mig | Cluster | Expected cols | Prompt file |
|---|---|---|---|---|
| 47 | mig_159 | PM final residual (molecular single-gene + completion + bilateral + stim-Tg/anti-Tg + laryngoscopy + misc) | 27 | `cursor_prompts/CURSOR_PROMPT_patient_master_final_residual_cluster_20260429.md` |
| 48 | mig_160 | Global clinical-date retype (gate-5 closure) | 21 cols × 6 tables | `cursor_prompts/CURSOR_PROMPT_global_clinical_date_retype_20260429.md` |
| 49 | mig_161 | mig_155 retroactive Path-C verification | 31 cols audit-only | `cursor_prompts/CURSOR_PROMPT_mig155_independent_reverification_20260429.md` |
| 50 | mig_162 | PM finalization + lakehouse coverage report (run last) | meta | `cursor_prompts/CURSOR_PROMPT_patient_master_finalization_and_lakehouse_audit_20260429.md` |

Plus the older mig_152 NLP cluster (~116 cols) — the original prompt at `cursor_prompts/CURSOR_PROMPT_patient_master_nlp_cluster_20260429.md` — may or may not have a Cursor agent assigned.

### 7.1 Verification rigor (don't skip) — extra critical for these lanes

For any landed lane, run §5.2 Path C protocol fully. Specific lane-by-lane gotchas:

- **mig_159 (final residual, 27 cols)**: Watch single-gene molecular markers (alk/eif1ax/hras/kras/nras/ntrk/pax8_pparg/tp53 — most should be 1-15% TRUE depending on gene; 0 TRUE → degenerate placeholder). `total_ln_positive_v10` should match `tp_ln_positive` (mig_150) and `ln_total_positive` (mig_133) — 3-source reconcile.
- **mig_160 (global date retype)**: Structural lane — table-level pre-snapshots required. Format-inventory pass per VARCHAR col before TRY_STRPTIME ALTER. 2-digit year → 20YY rule. Verify TRY_STRPTIME 0 unparseable before applying retype. After apply, gate 5 should drop 21 → 0.
- **mig_161 (mig_155 retroactive verify)**: Audit-only lane. Output is CF appendices on the 31 mig_155 cols + (if needed) verified→na flips for any degenerate FALSE BOOLEANs the original agent missed. mig_155's known issues per Cowork review: `canonical_dynamic_risk_response_*` doesn't exist in main (agent fell back to live `note_entities_llm_dynamic_risk_response`); `ata_initial_risk` = `ata_risk_category` 100% identical (CF-DUP); `resolved_layer_version` single value 'v1' (CF-VALUE-DEGENERATE); recurrence proxy bidirectional drift vs canonical_recurrence_v1.
- **mig_162 (PM finalization, runs LAST)**: Pre-condition: mig_152 + 159 + 160 + 161 ALL landed AND `n_not_started=0`. Output: PM table_status verified→`verified`, full 5-gate audit (expect 89/0/0/0/0), `qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260429.md` with carry-forward inventory. **Do NOT flip PM table_status if any prerequisite is missing — STOP and surface to Logan.**

## 8. Open Carry-Forwards (high-priority follow-ups)

These need explicit follow-up migrations after PM finalization:

- **CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT** — see §9 detailed plan. Needs Logan-ratified clinical definition for ARF.
- **CF-mig142-RAI-AVIDITY-PLACEHOLDER-ZERO** — V2 RAI NLP backfill pending; rai_avid_flag/rai_avidity stay na until backfill.
- **CF-mig150-PTH-MULTI-SOURCE-DERIVATION** — notes-PTH source restoration pending; pth_nadir family derivation has ~80% non-canonical_labs_pth_v1 origin.
- **CF-mig150-TP-UPSTREAM-NOT-IN-MAIN** — tp_* cols use script-based methodology; live LN canonical (tier-2 lymph_node) for tp_* re-derivation pending.
- **CF-mig151-RADTX-DERIVATION-GAP** — Tier-1 archive 5,641 distinct pts vs PM 210; deferred unless manuscript radtx scope expands.
- **CF-mig156-IS-MALIGNANT-MIXED-BENIGN-COHORT** — informational; manuscript filtering implication; document in mig_162.
- **CF-mig154-PM-VI/CAPSULAR/LVI/PNI-VS-EVENT-PRESENT** — PM legacy-rollup vs canonical_invasion_events_v1 grain divergence; needs reconciliation if invasion-grain analytics are primary.
- **CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT** — 1,065 v1/v2 mismatches; cross-feed reconcile pending.

## 9. mig_163 Plan — ANY-RECURRENCE Investigation (Logan needs to ratify)

### 9.1 The finding

PM `any_recurrence_flag` is supposed to be a cross-domain "any recurrence anywhere" boolean. Cowork independent reconcile found:

| Cell | Count | Meaning |
|---|---|---|
| ARF=TRUE / canon_recurrence_confirmed=TRUE | **165** | Both flag (consistent) |
| ARF=TRUE / canon=FALSE | **219** | PM-only (envelope wider than canonical_recurrence_v1) |
| ARF=FALSE / canon=TRUE | **349** | **canon-only — derivation gap** |
| ARF=FALSE / canon=FALSE | 10,138 | No recurrence either side |

Of the 349 canon-only:
- 11 have `biochemical_recurrence_flag=TRUE`
- 149 have `structural_recurrence_flag=TRUE`
- 149 have `distant_mets_proxy=TRUE`
- 345 are `is_malignant=TRUE`

The 349 split by canonical recurrence_definition:
- 246 surgical_pathology / structural_confirmed (largest)
- 53 fna_bethesda_vi_malignant / fna_confirmed
- ~50 other

### 9.2 Why a naive OR-fix won't work

A naive `ARF = bioch_flag OR struct_flag OR distant_proxy OR canon_recurrence_confirmed` would flip **1,805 patients** TRUE (jump 384 → 2,187). That's because:
- `structural_recurrence_flag`=TRUE has 1,818 patients (much wider than canonical's 514 confirmed)
- `distant_mets_proxy`=TRUE also 1,818 patients
- These are likely from a broader definition (imaging-suspicious + clinical suspicion) than canonical's path/FNA-confirmed

A naive OR would over-correct from undercount to over-count.

### 9.3 What needs to happen

**Step 1 (investigation, read-only)**: Profile the 1,818 structural_recurrence_flag=TRUE patients. What's the source? Are they imaging-suspicious recurrences from `canonical_recurrence_resolved_v1.recurrence_status_final='imaging_only_unconfirmed'` (747 pts) plus path_proven (145 pts)? Are they from `note_entities_llm_recurrence` Tier-1 mentions?

**Step 2 (clinical adjudication, ASK LOGAN)**: What should `any_recurrence_flag` mean? Three plausible definitions:
- (a) **STRICT** — only patients with canonical_recurrence_v1.recurrence_confirmed=TRUE (514 pts). Eliminates the 219 PM-only AND adds the 349 canon-only.
- (b) **WIDE** — OR of all PM proxies + canonical (1,818+ pts). Captures imaging-suspicious + clinical suspicion.
- (c) **HYBRID** — canonical_recurrence_v1.recurrence_confirmed=TRUE OR canonical_recurrence_resolved_v1.recurrence_status_final='path_proven' (more conservative than option b).

**Step 3 (mig_163 application)**: Once Logan picks definition, write mig_163 to UPDATE PM.any_recurrence_flag accordingly. Pre-snapshot. Re-derive. Update CF notes. Resync signoff registry.

### 9.4 Recommended kickoff query for new Cowork

```sql
-- Profile the 1,818 PM structural_recurrence_flag=TRUE pts: source distribution
WITH crr_path AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='path_proven'),
     crr_imaging AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='imaging_only_unconfirmed'),
     cr_conf AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE)
SELECT
  SUM(CASE WHEN pm.structural_recurrence_flag THEN 1 ELSE 0 END) AS struct_t,
  SUM(CASE WHEN pm.structural_recurrence_flag AND cr_conf.rid IS NOT NULL THEN 1 ELSE 0 END) AS struct_in_cr_conf,
  SUM(CASE WHEN pm.structural_recurrence_flag AND crr_path.rid IS NOT NULL THEN 1 ELSE 0 END) AS struct_in_crr_path,
  SUM(CASE WHEN pm.structural_recurrence_flag AND crr_imaging.rid IS NOT NULL THEN 1 ELSE 0 END) AS struct_in_crr_imaging,
  SUM(CASE WHEN pm.structural_recurrence_flag AND cr_conf.rid IS NULL AND crr_path.rid IS NULL AND crr_imaging.rid IS NULL THEN 1 ELSE 0 END) AS struct_no_canonical_source
FROM main.canonical_patient_master pm
LEFT JOIN crr_path USING (rid) -- requires aliasing
LEFT JOIN crr_imaging USING (rid)
LEFT JOIN cr_conf USING (rid);
```

(That's a sketch — fix the JOIN syntax; the rid binding requires explicit `ON CAST(pm.research_id AS VARCHAR) = <alias>.rid`.)

## 10. First-Action Checklist

```
1. git fetch && git pull
2. git -C "/Users/ros/THyroid 2026" log --oneline -25  # see what's landed since v5 doc was written (522942e)
3. Run §11 5-gate audit  # confirm gates 2/3/4 still 0; record gate 1, gate 5 (expect 21 unless mig_160 ran)
4. Check PM verification progress + active in-flight lanes:
     SELECT batch_id, COUNT(*) AS n, MAX(verified_ts) FROM main.canonical_column_verification_registry_v1
     WHERE table_name='canonical_patient_master' AND verified_ts > '2026-04-29 16:00:00' GROUP BY 1 ORDER BY 3;
5. Read MEMORY.md end-to-end (the auto-memory index)
6. Re-read §6.3 critical session findings
7. Re-read §7.1 lane-specific gotchas for the in-flight prompts
8. Re-read §9 mig_163 plan
9. Decide A/B/C from §12 below
```

## 11. Standing Reference — 5-Gate Cleanliness Audit

```sql
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name FROM (VALUES
    ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),
    ('llm_extracted_at'),('verified_ts'),('signed_off_ts'),
    ('registered_ts'),('updated_at'),('created_at'),('promoted_at'),
    ('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
    ('ingestion_date'),('lab_datetime')
  ) v(col_name)
)
SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified' AND signoff_migration IS NULL) AS gate2,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t WHERE t.table_status='verified' AND (t.n_verified + t.n_na <> t.n_columns_total OR t.n_not_started <> 0 OR t.n_failed <> 0)) AS gate3,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 r JOIN main.canonical_table_signoff_registry_v1 t USING (schema_name, table_name) WHERE t.table_status='verified' AND r.verification_status='verified' AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL)) AS gate4,
  (SELECT COUNT(*) FROM information_schema.columns c JOIN verified_tables v ON c.table_name = v.table_name LEFT JOIN main.canonical_column_verification_registry_v1 r ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main' AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist) AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source' AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw' AND COALESCE(r.verification_status,'unknown') != 'na' AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE') OR (c.data_type='VARCHAR' AND (regexp_matches(c.column_name, '(^|_)dates?(_|$)') OR regexp_matches(c.column_name, '(^|_)dt(_|$)'))))) AS gate5;
```

Expected at handoff: gate1=88, gate2=0, gate3=0, gate4=0, gate5=21. After mig_152/159/160/161/162 land, expect gate1=89, gate5=0.

## 12. Decision Tree After Reading This Doc

After §10 first-action checklist, decide:

**A. If 1+ Cursor lane has landed** (mig_152, 159, 160, 161 — check git log + registry) → **verify it (most likely path)**

For each landed lane, run §5.2 Path C protocol fully. Surface findings to Logan. Apply via query_rw with pre-snapshot if AGENTS-governance was respected (i.e., agent didn't already write to MD); Logan has been pre-authorizing "fix any issues" in this round so don't bottleneck on him for clear cleanups. Author and apply b-cleanup migrations for any agent-QA misses. Commit + push via Desktop Commander. Move on to next lane.

**B. If no lane has landed yet OR Logan asks** → **execute mig_163 ANY-RECURRENCE investigation** (§9)

Run the kickoff query in §9.4. Profile the 1,818 structural_recurrence_flag=TRUE source distribution. Surface findings to Logan with the 3-option clinical definition decision. After Logan ratifies, write + apply mig_163.

**C. If all in-flight lanes have landed and Logan needs more prompts** → **generate next batch**

After mig_152/154/156/157/159/160/161/162 land, PM should be at table_status='verified' with 0 not_started. Pivot to non-PM canonicals — there are 87 unverified canonical_* tables remaining (175 total - 88 verified). Generate prompts for the next-priority canonicals:
- canonical_us_lymph_node_v2 (LN Tier-2 canonical pending — would close mig_150 tp_* CF)
- canonical_recurrence_resolved_v1 (verify post-mig_125)
- canonical_*_VIEW_v* compat views
- Tier-1 raw mirrors verification (mostly auto-na)

## 13. Critical reminder for the new Cowork

**Verify all Cursor work directly and thoroughly.** Cursor agents have produced shortcuts that needed cleanup in EVERY round of this project. Specific patterns to watch:

1. **Agent cohort-uniformity sweep is incomplete.** Always missing one or both directions. Run sweep on every BOOLEAN flipped, both T-only and F-only patterns.
2. **Verification methods can name dead tables.** Pre-check `information_schema.tables` for every methodology string. Open CF-DEAD-TABLE if found; rename or reclassify.
3. **VARCHAR-with-units sneaks through.** Numeric measurements (sizes, weights, doses) sometimes stored as `"3.6 mm"`. Audit data_type + sample values.
4. **TIMESTAMP date cols sneak through.** Clinical event dates MUST be DATE. Audit data_type for any `*_date` col.
5. **Cross-canonical reconciles surface derivation gaps** that look like envelope-width issues but are actually undercounts. Always check both directions of drift (PM-only AND canon-only).
6. **AGENTS governance sometimes violated.** Some Cursor agents apply directly to MD instead of letting Cowork apply. mig_155 violated; mig_161 retroactive verify covers it. Watch for new violations and apply retroactive verification.

## 14. Verbatim opening message to paste into the new Cowork chat

---

Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v5.md` end-to-end before any tool use. Then run the §10 first-action checklist (git fetch + log, 5-gate audit, PM batch progress + active lane status, read MEMORY.md, re-read §6.3 + §7.1 + §9 mig_163 plan).

Standing context: I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're cleaning up the `thyroid_canonical_publication_v1_0` lakehouse on MotherDuck so it's ready for manuscript-grade statistical analysis. **PM is at 90.2% verified (1,441 / 1,598).** You're the orchestrator + verifier + applier; Cursor agents do the bulk lane work on my other machine; I'm the final ratifier.

You have:
- **Desktop Commander MCP** (push to GitHub via my actual Mac — sandbox has no creds; restart bash if process died)
- **MotherDuck MCP** (read + write against `thyroid_canonical_publication_v1_0` and `"Thyroid 2026 UPdated".archive_pub_v1_0`)
- **GitHub repo** at `/Users/ros/THyroid 2026` mounted as your workspace folder; URL `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`; tip `522942e`
- **Auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/` with ~95 entries
- All migration SQL files in `qc_framework_v1/migrations/` (1-158 + b-cleanups; range relevant: 149-162)
- All Cursor prompts in `cursor_prompts/`

**Currently in flight** (Cursor agents working on them — verify when they land; do NOT touch these clusters yourself):

1. mig_152 NLP cluster (~116 cols) — original prompt `cursor_prompts/CURSOR_PROMPT_patient_master_nlp_cluster_20260429.md`
2. mig_159 PM final residual (27 cols) — `cursor_prompts/CURSOR_PROMPT_patient_master_final_residual_cluster_20260429.md`
3. mig_160 global clinical-date retype (21 cols × 6 tables, gate-5 closure) — `cursor_prompts/CURSOR_PROMPT_global_clinical_date_retype_20260429.md`
4. mig_161 mig_155 retroactive Path-C verify (31 cols audit-only) — `cursor_prompts/CURSOR_PROMPT_mig155_independent_reverification_20260429.md`
5. mig_162 PM finalization + lakehouse coverage report (RUN LAST) — `cursor_prompts/CURSOR_PROMPT_patient_master_finalization_and_lakehouse_audit_20260429.md`

**First task**: Start with the §10 first-action checklist. After git pull + 5-gate audit + memory read, decide:

- **(A)** If any of the in-flight lanes have already landed (check git log + registry batch_ids), verify them via §5.2 Path C protocol. Logan will paste the agent summaries from those Cursor runs separately — verify them against live MD before applying.
- **(B)** If no lane has landed yet, execute the **mig_163 ANY-RECURRENCE clinical-adjudication investigation** described in §9. Run the kickoff query, profile the 1,818 structural_recurrence_flag=TRUE source distribution, and surface the 3-option clinical definition decision to Logan. After Logan ratifies, write + apply mig_163.
- **(C)** If all in-flight lanes have landed and PM is finalized, generate the next batch of cursor prompts for non-PM canonicals (87 unverified canonical_* tables remaining).

**Critical rigor reminder**: verify all Cursor work directly against MotherDuck. Lessons from prior rounds (each landed with agent-QA misses Cowork had to clean up):
- mig_135 shipped with 21 degenerate-FALSE cols
- mig_138 shipped with 447-pt undercount on recurrence_confirmed
- mig_141 shipped with 2 near-uniform-TRUE BOOLEANs missed
- mig_144 shipped with 4 VARCHAR measurement cols
- mig_145 shipped with CT tracheal not_mentioned overreach
- mig_146/147 shipped with VARCHAR date cols
- mig_147 shipped with nucmed_cumulative_therapeutic_dose 83% drift vs RAI canonical
- mig_151 shipped with 3 radtx degenerate-FALSE BOOLEANs + verification_method strings naming archived tables
- mig_154 shipped with 2 Type-A presence flags missed (lvi/vi)
- mig_155 was applied directly to MD by agent without governance — needs retroactive verify (mig_161)
- mig_156 shipped with prm_high_risk_marker_any 0 TRUE Type-B + 349-pt canon-only any_recurrence undercount
- mig_157 shipped with high_risk_molecular_v7 0 TRUE Type-B + 2 TIMESTAMP date cols

Be skeptical of every "verified clean" agent claim. Run the cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions. Run the date-type check on EVERY date col. Compare numeric-measurement cols against their upstream data types. Cross-validate any col with multiple authoritative upstreams. Verify methodology strings name LIVE `main.*` tables.

**Standing reminders from auto-memory you must follow:**
- feedback_motherduck_direct_check.md (always check MD)
- feedback_clinical_dates_calendar_only.md (clinical event dates MUST be DATE)
- reference_2digit_year_convention.md (YY → 20YY)
- feedback_extraction_faithfulness_llm_canonical.md
- feedback_findings_vs_staging.md
- feedback_recurrence_imaging_n_events_null.md
- feedback_etevent_resolved_cross_check.md
- feedback_no_cross_db_canonical_sourcing.md
- feedback_surgical_git_add.md
- feedback_use_desktop_commander_first.md

The handoff doc is ~700 lines and self-contained. You should be productive within ~5 minutes of reading it. I'll paste the agent summaries from the 4 in-flight Cursor runs separately as they come in — verify each one against live MD per Path C and apply if AGENTS-governance was respected.

End of handoff doc.
