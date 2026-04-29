# Cowork Handoff Prompt v7 — Thyroid Canonical Publication v1.0 Cleanup
Generated: 2026-04-29 (very late evening) — supersedes v6
Tip of `origin/main` at handoff: `8a48334` (or later — `git fetch && git pull` first)

## §0 TL;DR / first actions

You are continuing a multi-week cleanup of `thyroid_canonical_publication_v1_0` (MotherDuck) to support a manuscript pipeline. **PM is at 1,441 / 1,598 (90.2%) verified; gate1=165 verified canonicals.** Chat-2 took us through mig_163-174 with substantial progress on auxiliary registries, vocabulary audits, and design packages. Multiple Cursor agents are currently working on mig_171/173/174/175/176/177; agent summaries will be pasted to you for Path-C verification.

**Read in this order before any tool use:**

1. This handoff doc (§0-§14) end-to-end
2. `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-29.md` — chat-1 + chat-2 audit
3. `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` — apply queue
4. `exports/mig168_pm_vocab_audit_20260429_175417/RATIFICATION_NOTES_20260429.md` — Logan's clinical rules for histology vocabulary (key for §9 below)
5. `MEMORY.md` index + the high-relevance memories listed in §4

**Then run the §11 first-action checklist, then decide A/B/C from §13.**

## §1 Project mission

Logan Glosser, Emory thyroid-cancer surgery researcher. Database: `thyroid_canonical_publication_v1_0` on MotherDuck. Goal: produce a clean, audit-passing canonical layer ready for manuscript-grade survival/recurrence/outcomes analyses. Cohort: **10,871 distinct research_id**. Backbone: `canonical_patient_master` (1,598 cols, 1,441 verified). Tier-2 events / patient_rollup canonicals: 165 verified (gate1).

**You are: orchestrator + verifier + applier.** Cursor agents do bulk lane work on Logan's other machine. Logan ratifies decisions and pastes agent summaries to you. You verify against live MD per Path C, apply if AGENTS-governance was respected, and ship b-cleanup migrations for any agent QA misses.

## §2 Current state

| Metric | Value |
|---|---:|
| Latest commit on origin/main | `8a48334` (or later — fetch first) |
| gate1 (verified canonicals) | **165** |
| gate2 / gate3 / gate4 | **0 / 0 / 0** ✓ |
| gate5 (date retype violators) | 21 (closes to 0 on mig_160 apply) |
| PM verified cols | 1,441 / 1,598 (90.2%) |
| PM not_started | 144 (mig_152 NLP + mig_159 final residual cover this) |
| Cohort parity | 10,871 / 10,871 ✓ |
| Distinct CF tags | ~140 |

### §2.1 Recent migrations history (chat-1 → chat-2)
- chat-1 (my session): mig_142b → mig_157 + b-cleanups (PM 1,177 → 1,441 verified)
- chat-2: mig_163-174 (gate1 88 → 165 mostly via mig_165 mass auto-na on auxiliary tables + mig_164 VIEW layer registration)

### §2.2 Migrations committed but not yet applied to MD (Cowork apply queue)
- **mig_152** (PM NLP cluster, ~116 cols) — Cursor agent assigned; landing pending
- **mig_159** (PM final residual, 27 cols)
- **mig_160** (global date retype, 21 cols × 5 tables; closes gate5 to 0)
- **mig_161** (mig_155 retro Path-C verify, 31 col notes)
- **mig_162** (PM finalization + lakehouse coverage report — runs LAST)
- **mig_164** (VIEW layer signoff, 4 views) — may be landed already; verify
- **mig_166** (canonical_cleanup_audit_v1 ledger sign-off)
- **mig_167** (mig_165 retro-verify, notes-only)

### §2.3 Migration applied without governance (retroactively verified)
- **mig_165** auxiliary registry hygiene — 76 auxiliary tables auto-na'd; mig_167 covered the retro-verify

## §3 Tools & access

### §3.1 Desktop Commander
```
mcp__Desktop_Commander__start_process({command: "zsh", timeout_ms: 5000})
mcp__Desktop_Commander__interact_with_process({pid, input: "cd '/Users/ros/THyroid 2026' && git push origin main"})
```
Restart bash if process dies between calls (no session continuity). Per `feedback_use_desktop_commander_first.md`: Desktop Commander > Claude in Chrome > computer-use.

### §3.2 GitHub repo
- Path: `/Users/ros/THyroid 2026` (mounted as Cowork workspace folder)
- URL: `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`
- Branch: `main` tracked to `origin/main`
- Tip at handoff: `8a48334`
- Author: `Logan Glosser <logan.glosser@gmail.com>` for all commits
- **Surgical git add ONLY**: explicit paths; never `-A` or `scripts/output/`. Lint Python before commit if `.py` changed.

### §3.3 MotherDuck
- Tools: `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query` (read-only), `_query_rw` (writes)
- Primary DB: `thyroid_canonical_publication_v1_0` (live publication, MD account `logan.glosser.eras@gmail.com`)
- Archive DB: `"Thyroid 2026 UPdated"` schema `archive_pub_v1_0` (pre-snapshots BEFORE any mutating UPDATE/ALTER)
- DuckDB quirks: `CURRENT_TIMESTAMP` is TIMESTAMPTZ → cast to TIMESTAMP; FILTER not allowed on window funcs; cross-DB FROM in canonicals forbidden (`main.*` only); `ALTER COLUMN ... SET DATA TYPE T USING <expr>` works for in-place retypes.

### §3.4 Cursor agents
- Logan runs Cursor agents to author bulk SQL on his other machine. Per AGENTS governance: agents commit SQL only; **Cowork applies via Path C after independent verification.** mig_155 + mig_165 violated this — flag, run retroactive verification.

### §3.5 Auto-memory
- Path: `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
- Index: `MEMORY.md` (~140 entries; index lines after 200 truncated)
- Always read before deciding; updates persist across sessions

## §4 Reference documents

### §4.1 In repo (`/Users/ros/THyroid 2026`)
| Path | What it is |
|---|---|
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v7.md` | This doc |
| `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-29.md` | Chat-1 + chat-2 full audit + state |
| `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` | The Cowork apply queue (9-step plan) |
| `qc_framework_v1/migrations/*.sql` | All migration SQL (1-175). Range relevant: 159-178. |
| `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` | Latest 5-gate audit query |
| `qc_framework_v1/reports/*.md` | Read-only design + audit reports (mig_163, 165, 167, 168, 169, 170, 171, 173, 174a) |
| `cursor_prompts/CURSOR_PROMPT_*.md` | All Cursor agent prompts (~85 files) |
| `exports/mig168_pm_vocab_audit_20260429_175417/RATIFICATION_NOTES_20260429.md` | **Logan's clinical rules for histology vocabulary** (key for §9) |
| `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft_ratified.csv` | 95-row ratified mapping (4 enum cols) |
| `exports/mig168_pm_vocab_audit_20260429_175417/pm_histology_per_patient_overrides_20260429.csv` | 4 per-patient overrides |

### §4.2 Auto-memory key files (read first)
**Methodology / pattern memories**:
- `feedback_motherduck_direct_check.md` — verify against live MD every round
- `feedback_clinical_dates_calendar_only.md` — clinical event dates MUST be DATE
- `feedback_no_cross_db_canonical_sourcing.md` — canonicals are `main.*` standalone
- `feedback_findings_vs_staging.md` — anatomic findings primary; staging follows
- `feedback_extraction_faithfulness_llm_canonical.md` — re-derive from upstream WHERE error=0
- `feedback_surgical_git_add.md` — explicit path/glob; never -A
- `feedback_use_desktop_commander_first.md` — Desktop Commander > Chrome > computer-use
- `feedback_audit_regex_word_boundary.md` — gate-5 audit needs word boundaries

**Reference memories**:
- `reference_2digit_year_convention.md` — 20YY rule (Logan-ratified 2026-04-27)
- `reference_protocol_v2_md_accounts.md` — MD accounts
- `reference_synoptic_row_ix.md` — synoptic_row_ix is Script 108 pandas-load-order
- `reference_view_naming_convention.md` — `_VIEW` suffix required
- `reference_canonical_naming_convention.md` — Tier-2 masters naming

## §5 Database architecture

### §5.1 Tier structure
- **Tier 1** — `note_entities_llm_*`: raw LLM extraction outputs. Registry-seeded as `na` raw-mirror exempt.
- **Tier 2** — `canonical_*_events_v1`: event-grain typed tables. Each ROW = one event/finding/specimen.
- **Tier 2 rollup** — `canonical_*_patient_rollup_v1`: patient-grain rollups from events.
- **Tier 3** — `canonical_patient_master`: THE master patient-grain table. 1,598 cols.

### §5.2 Verification registries
- `canonical_table_signoff_registry_v1` — 1 row per canonical_*. `n_verified`, `n_columns_total`, `n_na`, `n_not_started`, `signoff_migration`, `table_status`.
- `canonical_column_verification_registry_v1` — 1 row per col. `verification_status`, `verified_by`, `verification_method`, `batch_id`, `notes` (CF appendices accumulate via `| mig_<N>: ...`).

### §5.3 Verification methods (controlled vocabulary)
- `derivation_vs_canonical_<source>_<col>` — re-derive from upstream
- `extraction_faithfulness_vs_note_entities_llm_<domain>` — re-derive from Tier 1 WHERE error=0
- `internal_consistency` — pairwise rule
- `auto_provenance_skip` (na) — build_ts, extracted_at, etc.
- `helper_<placeholder>_pending_real_extraction` (na) — Type-B placeholder pattern
- `extraction_faithfulness_vs_archive_pub_v1_0_<table>_<snapshot_ts>` — archive-only source (mig_151b precedent)

## §6 Workflow: Cowork ↔ Cursor ↔ Logan

### §6.1 Roles
- **Logan**: clinical-domain expert; ratifies clinical decisions; pastes agent summaries
- **Cursor agents**: bulk SQL authors; commit + push but do NOT write to MD per AGENTS governance
- **Cowork (you)**: orchestrator + verifier + applier + small-fix author

### §6.2 Path C — the standard apply protocol
For any Cursor-authored migration SQL, do all of these BEFORE any `query_rw`:

1. **Read the SQL file end-to-end** — understand each block + claimed SSOTs
2. **Pre-flight probes** (read-only): col count matches prompt; upstream tables live in `main` (`information_schema.tables`); cohort parity 10,871
3. **Cohort-uniformity sweep BOTH directions** on every BOOLEAN flipped:
   - 0 TRUE → Type-B placeholder → reclassify verified→na in `mig_<N>b`
   - 0 FALSE / TRUE-only / NULL → Type-A presence flag → keep verified, add `CF-COHORT-NEAR-UNIFORM-TRUE-<col>` informational note
4. **Date-type check** — `*_date` cols MUST be DATE (not TIMESTAMP/VARCHAR); open `CF-mig<N>-CLINICAL-DATE-RETYPE` if violated
5. **Data-type sanity** — numeric measurements as DOUBLE (not VARCHAR-with-units); apply mig_144b retype pattern if needed
6. **Cross-source spot-check** on 5+ random rids; trace 1 col's derivation back to upstream
7. **Cross-canonical reconciliation** for cols with multiple SSOTs (e.g., recurrence proxies vs canonical_recurrence_v1)
8. **Pre-snapshot** affected registry rows + any data-write tables to `archive_pub_v1_0`
9. **Apply** via query_rw (block-by-block due to MCP wrapper)
10. **Verify post-state**: math, signoff resync, 5-gate audit
11. **Author + apply b-cleanup** for any agent-QA misses
12. **Write traceability SQL**, commit + push

### §6.3 When to apply directly vs ask Logan
- **Apply directly**: registry-only, single-col retype with full preservation probe, focused data-write with clear rule, Path-C-compliant Cursor SQL
- **Ask Logan**: cross-canonical reconciles affecting >50 patients with clinical adjudication needed, structural schema changes, **clinical definition disputes** (this is what §9 is — Logan needs to ratify rejection of `mtc_ptc_mixed`)

### §6.4 Pre-snapshot rule
ALWAYS pre-snapshot before mutating verified canonicals:
```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_<table>_pre_mig<N>_<short>_20260429 AS
SELECT research_id, <affected cols>, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig<N>_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_<table>;
```

## §7 In-flight Cursor lanes — verify when summaries arrive

Logan will paste agent summaries; verify against live MD per Path C.

| Lane | mig | Cluster | Status | Prompt file |
|---|---|---|---|---|
| — | mig_152 | PM NLP cluster (~116 cols) | in-flight | `cursor_prompts/CURSOR_PROMPT_patient_master_nlp_cluster_20260429.md` |
| 47 | mig_159 | PM final residual (27 cols) | committed, awaiting apply | `cursor_prompts/CURSOR_PROMPT_patient_master_final_residual_cluster_20260429.md` |
| 48 | mig_160 | Global date retype (21 cols) | committed, awaiting apply | `cursor_prompts/CURSOR_PROMPT_global_clinical_date_retype_20260429.md` |
| 49 | mig_161 | mig_155 retro Path-C verify (31 cols) | committed, awaiting apply | `cursor_prompts/CURSOR_PROMPT_mig155_independent_reverification_20260429.md` |
| 50 | mig_162 | PM finalization + lakehouse coverage report | committed, runs LAST | `cursor_prompts/CURSOR_PROMPT_patient_master_finalization_and_lakehouse_audit_20260429.md` |
| 60 | mig_171 | canonical_us_lymph_node_v2 build (closes CF-mig150-TP-UPSTREAM) | design/skeleton ready | `cursor_prompts/CURSOR_PROMPT_mig171_canonical_us_lymph_node_v2_build_20260429.md` |
| 61 | mig_172 | Vocabulary normalization apply (4 enum cols + 4 variant_subtype cols + 4 per-patient overrides) | **ratification draft committed `8a48334`; awaiting Logan spot-check + §9 reject of mtc_ptc_mixed** | `cursor_prompts/CURSOR_PROMPT_mig172_vocabulary_normalization_apply_20260429.md` |
| 62 | mig_173 | syn size_cm dtype reform | dtype reform SQL committed `2b786de` | `cursor_prompts/CURSOR_PROMPT_mig173_syn_size_cm_dtype_reform_20260429.md` |
| 63 | mig_174a | cnln/lateral/ene multi-label parser design | read-only design committed `955801f` | `cursor_prompts/CURSOR_PROMPT_mig174_cnln_laterality_multilabel_parser_20260429.md` |
| 64 | mig_175 | days_postop semantic adjudication | probes committed | `cursor_prompts/CURSOR_PROMPT_mig175_mig136_days_semantic_adjudication_20260429.md` |
| 65 | mig_176 | dominant_nodule_size_cm v1/v2 reconcile | prompt shipped | `cursor_prompts/CURSOR_PROMPT_mig176_dominant_nodule_v1_v2_reconcile_20260429.md` |
| 66 | mig_177 | mig_154 invasion family reconcile | prompt shipped | `cursor_prompts/CURSOR_PROMPT_mig177_mig154_invasion_family_reconcile_20260429.md` |

### §7.1 Lane-specific gotchas

- **mig_172 vocabulary apply**: blocked on §9 — Logan's `mtc_ptc_mixed` reject must be incorporated before apply. Cowork should rewrite the ratified CSV to remove that canonical_code, then apply. The 4 cols (recurrence_histology, recurrence_histology_v2, completion_prior_histology, completion_histology_type) get rewritten to canonical_code; 4 new variant_subtype cols added.
- **mig_171 canonical_us_lymph_node_v2 build**: this is a NEW canonical, not a verification — design/skeleton SQL exists; needs full author + apply. Closes CF-mig150-TP-UPSTREAM-NOT-IN-MAIN.
- **mig_173 syn size_cm dtype reform**: VARCHAR `"4.5 x 2.5 x 2.0"` 3D dimensions are NOT retypable to DOUBLE — agent design must preserve as VARCHAR but normalize the format (sorted dims, consistent separator). Verify the reform doesn't lose data.
- **mig_174a → mig_174b**: design phase done; Logan needs to ratify representation choice (delimited array vs per-side BOOLEANs) before mig_174b apply.
- **mig_177 invasion reconcile**: closes CF-mig154-PM-VI/CAPSULAR/LVI/PNI-VS-EVENT-PRESENT. PM legacy-rollup count vs canonical_invasion_events_v1 grain divergence (e.g., 2,514 PM TRUE with no event-grain present row).

## §8 Open carry-forwards (high-priority follow-ups)

| CF tag | Description | Plan |
|---|---|---|
| **CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT** | Closed by mig_163b (HYBRID definition Logan-ratified at `1476a46`); apply pending | Verify mig_163b applied; close CF |
| **CF-mig154-PM-VI/CAPSULAR/LVI/PNI-VS-EVENT-PRESENT** | PM legacy-rollup vs canonical_invasion_events_v1 drift | mig_177 covers |
| **CF-mig150-TP-UPSTREAM-NOT-IN-MAIN** | tp_* cols use script-based methodology; live LN canonical pending | mig_171 covers |
| **CF-mig142-RAI-AVIDITY-PLACEHOLDER-ZERO** | V2 RAI NLP backfill pending | unblock when V2 RAI NLP runs |
| **CF-mig150-PTH-MULTI-SOURCE-DERIVATION** | notes-PTH source restoration pending | track |
| **CF-mig151-RADTX-DERIVATION-GAP** | 5,431-pt gap; deferred unless manuscript radtx scope expands | track |
| **CF-mig168-VOCAB-DRIFT-* (multiple)** | recurrence_histology + 3 cols vocabulary drift | mig_172 covers (after §9 rewrite) |
| **CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT** | 1,065 v1/v2 mismatches | mig_176 covers |

## §9 ⚠️ Histology vocabulary decision — Logan-ratified update 2026-04-29 (post-mig_174a)

### §9.1 Logan's new directive
After reviewing the mig_174a session output and seeing `histologic_types_all` values like `"PTC | MTC"`, `"PTC | MTC\nPTC mixed composit | MTC"`, `"MTC | metastatic PTC?"`, etc., Logan ratified:

> "There shouldn't be 'mixed' — each tumor should be a single diagnosis. Histology should keep uniformity across all columns/tables."

### §9.2 What this means concretely

**Reject `mtc_ptc_mixed` from the chat-2 ratification.** The chat-2 `RATIFICATION_NOTES_20260429.md` had 12 canonical_code values including `mtc_ptc_mixed` (composite tumor). Logan now wants this REMOVED. Each tumor at the **event grain** (`canonical_path_malignant_events_v1`) gets a single canonical_code. Patients with multiple tumors of different histologies have multiple event rows — already the case in 32 patients with PTC+MTC.

**Investigation result (2026-04-29 Cowork)**:
- 32 patients have `histologic_types_all` containing both PTC and MTC tokens
- All 32 have **separate single-diagnosis events** in canonical_path_malignant_events_v1 (e.g., rid 459 has 1 PTC microcarcinoma row + 1 MTC row)
- The PM rollup correctly captures them via STRING_AGG, but: ordering is unstable (`"PTC | MTC"` vs `"MTC | PTC"`); 1 patient (rid 2168) has a literal `"MTC\nPTC mixed composit"` value embedded; 1 patient (rid 3331) has `"metastatic PTC?"`

### §9.3 Required fixes (mig_178 lane)

1. **Update `RATIFICATION_NOTES_20260429.md`**: remove `mtc_ptc_mixed` from canonical_code list (12 → 11). Document rationale: "rejected per Logan 2026-04-29 — each tumor = single diagnosis at event grain."
2. **Update `pm_ssot_enum_dictionary_draft_ratified.csv`**: any rows mapping to `mtc_ptc_mixed` get re-mapped (likely split into separate PTC and MTC events at the source canonical, or per-patient override).
3. **Cleanup raw values in `canonical_path_malignant_events_v1.primary_histology`**:
   - Replace `"MTC\nPTC mixed composit"` (rid 2168) with appropriate split (likely 2 separate events: `MTC` + `PTC microcarcinoma`)
   - Replace `"metastatic PTC?"` (rid 3331) with `PTC` (strip metastatic prefix per Rule 1; drop trailing `?` after clinical confirmation)
   - Strip newline contamination, casing inconsistency, spelling errors (cribiform → cribriform; microcarcioma → microcarcinoma; varaint → variant; ONcocytic → oncocytic)
4. **Re-derive `histologic_types_all` and `histologic_variants_all`** with sorted STRING_AGG (alphabetical, distinct, normalized casing)
5. **Cross-table histology vocabulary uniformity audit**: check that the same enum is used across:
   - `canonical_path_malignant_events_v1.primary_histology` + `histology_variant`
   - `canonical_path_malignant_patient_rollup_v1.dominant_histology`
   - `canonical_patient_master.diagnosis_primary` + `diagnosis_full` + `diagnosis_variant` + `histologic_types_all` + `histologic_variants_all` + `recurrence_histology` + `completion_*_histology`
6. **mig_172 apply unblocks** once mig_178 reject of `mtc_ptc_mixed` lands.

### §9.4 Plan for new Cowork

1. Read `exports/mig168_pm_vocab_audit_20260429_175417/RATIFICATION_NOTES_20260429.md` end-to-end
2. Investigate the 32 PTC|MTC patients via `canonical_path_malignant_events_v1` to confirm each has separate single-diagnosis events
3. Identify all histology-related cols across the lakehouse (`information_schema.columns` filter on `column_name ILIKE '%histolog%' OR ILIKE '%diagnos%'`)
4. Author **mig_178 prompt** for a Cursor agent (read-only audit + reject `mtc_ptc_mixed` from ratification + clean up raw values + cross-table vocabulary uniformity audit)
5. Apply mig_178 in two phases (audit first, then apply after Logan ratifies the cleanup proposal)
6. Re-trigger mig_172 apply once mig_178 lands

## §10 Apply queue (priority order)

After §11 first-action checklist, work through the apply queue in this order:

1. **Verify any in-flight lane that has landed** (Path C). Logan will paste summaries.
2. **Apply mig_159** (PM final residual, 27 cols) — registry-only flips, low risk
3. **Apply mig_160** (global date retype, 21 cols × 5 tables) — STRUCTURAL DATA WRITE, higher risk; full table-level pre-snapshots required; closes gate5 to 0
4. **Apply mig_161** (mig_155 retro-verify) — notes-only
5. **Apply mig_164** (VIEW layer, 4 views) — verify if already landed
6. **Apply mig_166** (canonical_cleanup_audit_v1 ledger sign-off)
7. **Apply mig_167** (mig_165 retro-verify, notes-only)
8. **Author + apply mig_178** (histology cleanup per §9) — depends on Logan ratifying reject of `mtc_ptc_mixed`
9. **Re-trigger mig_172 apply** post-mig_178
10. **Apply mig_152** (PM NLP cluster) — when Cursor agent lands
11. **Apply mig_171** (canonical_us_lymph_node_v2 build) — when ready
12. **Apply mig_162** (PM finalization + lakehouse coverage report) — runs LAST, after everything else

## §11 First-action checklist

```
1. git fetch origin && git pull --rebase origin main && git log --oneline -25
2. Run §14 5-gate audit (expect gate1=165 / 0 / 0 / 0 / 21)
3. Check PM batch progress + recent registry activity:
     SELECT n_verified, n_na, n_not_started, table_status FROM main.canonical_table_signoff_registry_v1 WHERE table_name='canonical_patient_master';
4. Check active in-flight lanes:
     SELECT batch_id, COUNT(*) AS n, MAX(verified_ts) FROM main.canonical_column_verification_registry_v1
     WHERE table_name='canonical_patient_master' AND verified_ts > '2026-04-29 18:00:00' GROUP BY 1 ORDER BY 3;
5. Read MEMORY.md end-to-end (auto-memory index)
6. Read qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-29.md (chat-1 + chat-2 audit)
7. Read qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md (the apply queue)
8. Read exports/mig168_pm_vocab_audit_20260429_175417/RATIFICATION_NOTES_20260429.md (histology rules)
9. Re-read §9 above (Logan's mtc_ptc_mixed reject + mig_178 plan)
10. Re-read §6.2 Path C protocol + §7.1 lane gotchas
11. Decide A/B/C from §13 below
```

## §12 Critical reminders for new Cowork

**Verify all Cursor work directly and thoroughly.** Cursor agents have produced shortcuts that needed cleanup in EVERY round. Specific patterns to watch:

| Lesson | What happened | Lesson learned |
|---|---|---|
| mig_135 | 21 degenerate-FALSE cols not flagged | Run cohort-uniformity sweep on every BOOLEAN |
| mig_138 | 447-pt undercount on recurrence_confirmed | Cross-canonical reconciliation before accepting |
| mig_141 | 2 near-uniform-TRUE BOOLEANs missed | Sweep both directions (T-only AND F-only) |
| mig_144 | 4 VARCHAR measurement cols left un-retyped | Audit data_type + sample values for every measurement col |
| mig_145 | CT tracheal `not_mentioned` counted as TRUE | Trace BOOLEAN derivations to upstream enum semantics |
| mig_147 | nucmed_cumulative_dose 83% drift vs RAI | Cross-validate cols with multiple authoritative upstreams |
| mig_148 | iodine_avidity_flag placeholder (Type-B → na) | Recognize Type-B placeholder pattern across rounds |
| mig_151 | 3 radtx degenerates + verification_method named ARCHIVED tables | Pre-check `information_schema.tables` for every methodology string |
| mig_154 | 2 Type-A presence flags missed (lvi/vi) | Sweep TRUE-only patterns alongside FALSE-only |
| mig_155 | Agent applied directly to MD without governance | Watch for governance violations; run retroactive verification |
| mig_156 | prm_high_risk_marker_any 0 TRUE Type-B + 349-pt ARF undercount | Sweep both directions; cross-canonical drift |
| mig_157 | high_risk_molecular_v7 0 TRUE Type-B + 2 TIMESTAMP date cols | Sweep + date-type check |
| mig_165 | Mass auto-na on 76 aux tables without governance | Run retroactive verification (mig_167 covered) |
| mig_172 ratification | mtc_ptc_mixed canonical_code Logan now rejects (clinical reversal) | When Logan flags clinical concerns, treat as ratification reversal — adapt downstream |
| mig_174a | Multi-label fields with literal `'null'` token, casing/whitespace drift, embedded newlines | Token-level enumeration before any parser design |

## §13 Decision tree

After §11 first-action checklist, decide:

**A. If 1+ in-flight Cursor lane has landed** (mig_152, 159, 160, 161, 162, 164, 166, 167, 171, 172, 173, 174b, 175, 176, 177) → **verify it** via §6.2 Path C and apply if clean. Apply b-cleanup for any agent QA misses. Logan will paste summaries.

**B. If Logan has ratified the §9 mtc_ptc_mixed reject** → **author mig_178 plan**: read the ratification notes + the 32 PTC|MTC patient evidence, write a Cursor prompt for the histology cleanup (raw value cleanup + STRING_AGG sorted re-derivation + cross-table uniformity audit), commit + push.

**C. If no in-flight lane has landed AND mig_178 doesn't need to start yet** → **work through apply queue (§10)** starting with mig_159.

## §14 Standing reference — 5-Gate Cleanliness Audit

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

Expected at handoff: gate1=165, gate2=0, gate3=0, gate4=0, gate5=21. After mig_160 apply: gate5=0. After mig_152/159/162 apply: gate1=166, PM finalized.

## §15 Verbatim opening message to paste into the new Cowork chat

---

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v7.md` end-to-end before any tool use. Then read in order:
> 1. `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-29.md` (chat-1 + chat-2 audit)
> 2. `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` (apply queue)
> 3. `exports/mig168_pm_vocab_audit_20260429_175417/RATIFICATION_NOTES_20260429.md` (histology rules)
> 4. `MEMORY.md` (auto-memory index)
>
> Then run the §11 first-action checklist (git fetch + log, 5-gate audit, PM batch progress + active lane status). Then decide A/B/C from §13.
>
> Standing context: I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're closing out the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`) for manuscript-grade analyses. **gate1 is at 165 / PM at 1,441 / 1,598 (90.2%) verified.** You're the orchestrator + verifier + applier; Cursor agents do the bulk lane work; I'm the final ratifier.
>
> You have **Desktop Commander MCP** (push to GitHub via my actual Mac), **MotherDuck MCP** (read + write against `thyroid_canonical_publication_v1_0` and `"Thyroid 2026 UPdated".archive_pub_v1_0`), **GitHub repo** at `/Users/ros/THyroid 2026` (tip `8a48334`; URL `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`), and **auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/` with ~140 entries.
>
> **Currently in flight** (Cursor agents working on them — verify when they land):
> - mig_152 NLP cluster, mig_159 PM final residual, mig_160 global date retype, mig_161 mig_155 retro-verify, mig_162 PM finalization
> - mig_171 canonical_us_lymph_node_v2 build, mig_172 vocabulary normalization apply (BLOCKED on §9 reject of mtc_ptc_mixed), mig_173 syn size_cm dtype reform, mig_174a multi-label parser design (mig_174b apply pending Logan ratify)
> - mig_175 days semantic, mig_176 dominant_nodule reconcile, mig_177 invasion family reconcile
>
> **Critical clinical decision pending (§9)**: I told you in the last chat that **each tumor should have a single diagnosis** — reject the `mtc_ptc_mixed` canonical_code from the chat-2 ratification. The 32 PTC|MTC patients have separate single-diagnosis events at the canonical_path_malignant_events_v1 grain; the PM rollup STRING_AGG ordering is the issue, not "mixed composite tumors". You need to author mig_178 for histology cleanup (raw value scrub, sorted STRING_AGG re-derivation, cross-table vocabulary uniformity audit). mig_172 apply is blocked until mig_178 lands.
>
> **Critical rigor reminder**: verify all Cursor work directly against MotherDuck. Lessons from prior rounds (every round shipped with agent QA misses Cowork had to clean up — full table at §12). Be skeptical of every "verified clean" agent claim. Run cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions. Pre-check `information_schema.tables` for every methodology string. Audit data_type for every numeric measurement col. Check date-type for every `*_date` col.
>
> **First task**: §11 first-action checklist, then choose:
> - **(A)** If any in-flight lane has landed, verify via Path C and apply
> - **(B)** Author mig_178 histology cleanup plan (per §9)
> - **(C)** Work through apply queue (§10) — start with mig_159 if no in-flight lane has landed yet
>
> I'll paste agent summaries from the in-flight Cursor runs separately as they come in — verify each against live MD per Path C and apply if AGENTS-governance was respected.

---

End of handoff doc.
