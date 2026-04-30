# Cowork Handoff Prompt v9 — Thyroid Canonical Publication v1.0 Cleanup

Generated: 2026-04-29 (very late evening) — supersedes v8
Tip of `origin/main` at handoff: **`06a2bdb`** (verify with `git fetch && git log --oneline -10`)

---

## §0 TL;DR / first actions

You are continuing a multi-week cleanup of `thyroid_canonical_publication_v1_0` (MotherDuck) toward a manuscript-grade publication lakehouse for thyroid cancer survival/recurrence/outcomes analyses. **gate1 = 169 verified canonicals; PM = 1,575 / 1,615 (97.5%) verified.** Marathon round just closed: 8 CFs + 3 governance debts cleared in a single session.

**Read in this order before any tool use:**

1. This handoff doc (§0–§14) end-to-end
2. `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v8.md` (predecessor; partial overlap)
3. `qc_framework_v1/reports/mig_179_apply_closeout_20260429.md` — events extractor LVI rebuild
4. `qc_framework_v1/reports/mig_180_apply_closeout_20260429.md` — PM nlp_* cluster closure
5. `qc_framework_v1/reports/mig_172b_apply_closeout_20260429.md` — histology vocab normalization
6. `qc_framework_v1/reports/section_8_retro_audits_closeout_20260429.md` — governance debt cleared
7. `MEMORY.md` index + the high-relevance memories listed in §4

**Then run the §11 first-action checklist (FULL SCOPE REVIEW first), then decide A/B/C from §13.**

**Workflow is:** Cowork (you) is the orchestrator + verifier + applier. Cursor agents do bulk SQL authoring on Logan's other machine. Logan ratifies design decisions and pastes agent summaries to you. **VERIFY EVERY CURSOR CLAIM DIRECTLY AGAINST LIVE MOTHERDUCK** — every prior round has shipped with agent QA misses Cowork had to clean up.

---

## §1 Project mission

**Logan Glosser**, Emory thyroid-cancer surgery researcher. Database: `thyroid_canonical_publication_v1_0` on MotherDuck (account `logan.glosser.eras@gmail.com`). Goal: produce a clean, audit-passing canonical layer ready for **manuscript-grade survival/recurrence/outcomes analyses**.

- Cohort: **10,871 distinct research_id**
- Backbone: `canonical_patient_master` (1,615 cols, 1,575 verified)
- Tier-2 events / patient_rollup canonicals: 169 verified (gate1)
- Authoritative SSOT: live MotherDuck — never trust prior summaries

**You are: orchestrator + verifier + applier.** Cursor agents do bulk lane work on Logan's other machine. Logan ratifies clinical decisions and pastes agent summaries to you. You verify against live MD per Path C, apply if AGENTS-governance was respected, and ship b-cleanup migrations for any agent QA misses or governance violations.

---

## §2 Current state (post-marathon round 2026-04-29 evening)

| Metric | Value |
|---|---:|
| Latest origin/main tip | `06a2bdb` (mig_172b apply close-out) |
| **gate1 (verified canonicals)** | **169** |
| gate2 / gate3 / gate4 | **0 / 0 / 0** ✓ |
| gate5 (date retype violators) | 21 (closes to 0 on mig_160 apply — STILL PENDING) |
| **PM verified cols** | **1,575 / 1,615 (97.5%)** |
| PM na | 24 |
| PM not_started | 16 (15 syn_*_size from mig_173b + 1 misc) |
| Cohort parity | 10,871 / 10,871 ✓ |
| Total CFs closed in marathon round | 8 + 3 governance debts |

### §2.1 Lanes applied this evening (chronological)
- ✅ **mig_179** events extractor LVI rebuild (commit `8d858b2` Cursor + `363b4e6` Cowork close-out) — events 51,751 → 58,582; lymph 780→989 / vasc 1,109→1,178 patients
- ✅ **mig_179b** CF closures notes (4 events/rollup CFs)
- ✅ **mig_177b** PM `lvi_*/vi_*` rederive vs refreshed events (commit `dd45a59`) — closes CF-mig177-PM-VASC-ALIAS-LVI + CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT
- ✅ **§8 retro Path-C audits** mig_178 / mig_173b / mig_163b — all VERIFIED CLEAN (commit `e70f36a`)
- ✅ **mig_176b** dominant_nodule R2 apply (commit `e70f36a`) — closes CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT; +2 new resolved cols
- ✅ **mig_180** PM `nlp_*` cluster apply (commit `103ffeb` Cursor + `9736a14` Cowork close-out) — 115 verified + 1 na = 116 cols
- ✅ **mig_172b** vocab normalization apply (commit `53c3fdb` Cursor + `06a2bdb` Cowork close-out) — closes CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES; 4 PM histology cols normalized

### §2.2 In-flight Cursor lanes (4 prompts dispatched 2026-04-29)
**Logan is sending these to Cursor one at a time.** Verify each agent summary against live MD per Path C when it lands:

- 🔄 **mig_180b** — NLP UPSTREAM-MISSING 12-family lineage investigation (38 cols). Prompt at `cursor_prompts/CURSOR_PROMPT_mig180b_nlp_upstream_missing_investigation_20260429.md`. Expected deliverables: `qc_framework_v1/migrations/180b_*.sql` + `qc_framework_v1/reports/mig_180b_upstream_lineage_audit_20260429.md`.

- 🔄 **mig_181** — PM `syn_*_size` 15 not_started cols verify+apply. Prompt at `cursor_prompts/CURSOR_PROMPT_mig181_pm_syn_size_15_cols_verify_apply_20260429.md`. Expected deliverables: `qc_framework_v1/migrations/181_*.sql` + `qc_framework_v1/reports/mig_181_syn_size_audit_20260429.md`. Drops PM not_started 16 → 1.

- 🔄 **mig_177c** — LVI+VI derivative reclean **scoping only** (Option A clear vs Option B rederive). Prompt at `cursor_prompts/CURSOR_PROMPT_mig177c_lvi_vi_derivatives_reclean_scoping_20260429.md`. Expected deliverables: `qc_framework_v1/reports/mig_177c_derivative_reclean_scoping_20260429.md` + placeholder skeleton SQL (not for execution). **Logan must ratify Option A or B before any apply.**

- 🔄 **mig_182** — CF-87-AJCC investigation (36 col-impact). Prompt at `cursor_prompts/CURSOR_PROMPT_mig182_cf_87_ajcc_investigation_20260429.md`. Expected deliverables: `qc_framework_v1/reports/mig_182_cf_87_ajcc_investigation_20260429.md` (read-only audit + R1/R2/R3 fix plan options). **Logan ratifies fix plan in follow-up.**

### §2.3 Awaiting Logan ratification (don't touch yet)
- **mig_171b** — `canonical_us_lymph_node_v2` BUILD (Cursor SQL committed at `123cebb`). Read-only preview: 6,973 events / 4,110 patients / 159 fallback exam IDs. Closes CF-117 triplet × 53 = 159 col-impact.
- **mig_174b** — `cnln_img_laterality` per-side BOOLEAN apply (Cursor prompt at `cursor_prompts/CURSOR_PROMPT_mig174b_apply_per_side_boolean_20260429.md`). Token-level multi-label parser.

### §2.4 High-risk structural lane deferred
- **mig_160** — global clinical-date retype (21 cols × 5 base tables). Cowork-direct; closes ~190 col-impact CFs + gate5 21→0 in one shot. Dependent-view recompile risk; ~48 query_rw calls. **Defer until Logan explicitly approves the structural sweep.**

---

## §3 Tools & access

### §3.1 Desktop Commander (push to GitHub via Logan's actual Mac)

```
mcp__Desktop_Commander__start_process({command: "zsh", timeout_ms: 5000})
mcp__Desktop_Commander__interact_with_process({pid, input: "cd '/Users/ros/THyroid 2026' && git push origin main"})
mcp__Desktop_Commander__force_terminate({pid})
```

Restart bash if process dies between calls (no session continuity). Per `feedback_use_desktop_commander_first.md`: Desktop Commander > Claude in Chrome > computer-use.

### §3.2 GitHub repo

- **Path:** `/Users/ros/THyroid 2026` (mounted as Cowork workspace folder — full read/write access)
- **URL:** `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`
- **Branch:** `main` tracked to `origin/main`
- **Tip at handoff:** `06a2bdb`
- **Author:** `Logan Glosser <logan.glosser@gmail.com>` for all commits
- **Surgical git add ONLY** — explicit paths/file-globs; never `-A` or `scripts/output/`. Lint Python before commit if `.py` changed (per `feedback_commit_workflow.md`).

### §3.3 MotherDuck

- **Tools:**
  - `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query` (read-only)
  - `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query_rw` (writes — requires explicit user-approval semantics; ask before mutating verified data; one-statement-per-call wrapper)
- **Primary DB:** `thyroid_canonical_publication_v1_0` (live publication, MD account `logan.glosser.eras@gmail.com`)
- **Archive DB:** `"Thyroid 2026 UPdated"` schema `archive_pub_v1_0` (pre-snapshots BEFORE any mutating UPDATE/ALTER)
- **DuckDB quirks:**
  - `CURRENT_TIMESTAMP` is TIMESTAMPTZ → cast to TIMESTAMP for build_ts cols
  - FILTER not supported on window funcs (use SUM(CASE) OVER)
  - Cross-DB `FROM` in canonicals forbidden (`main.*` only)
  - `ALTER COLUMN ... SET DATA TYPE T USING <expr>` works for in-place retypes
  - `INSERT BY NAME` requires source SELECT to have correct column types — `feeds_master_columns_array` is `VARCHAR[]` not `VARCHAR` (recurring trap)
- **MCP wrapper:** one statement per call — do NOT use `BEGIN TRANSACTION;` / `COMMIT;`

### §3.4 Cursor agents

- Logan runs Cursor agents on his other machine to author bulk SQL.
- **AGENTS governance:** agents commit SQL only; **Cowork applies via Path C after independent Path-C verification.** Verify each agent's commit is governance-compliant (no MD writes) before applying.
- Cursor prompts are dropped into `cursor_prompts/` directory; Cursor agents pull these and execute their assigned scope.
- Logan pastes agent summaries to you when each lane lands.

### §3.5 Auto-memory

- **Path:** `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
- **Index:** `MEMORY.md` (~150+ entries; index lines after 200 may be truncated — keep concise)
- Always read before deciding; updates persist across sessions.
- Session-relevant memories listed in §4.2.

---

## §4 Reference documents

### §4.1 In repo (`/Users/ros/THyroid 2026`)

| Path | What it is |
|---|---|
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v9.md` | **This doc** |
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v8.md` | Predecessor |
| `qc_framework_v1/migrations/*.sql` | All migration SQL (184 + counting) |
| `qc_framework_v1/reports/*.md` | Read-only design + audit + close-out reports |
| `qc_framework_v1/reports/mig_179_apply_closeout_20260429.md` | Events LVI rebuild close-out |
| `qc_framework_v1/reports/mig_180_apply_closeout_20260429.md` | PM nlp_* cluster closure |
| `qc_framework_v1/reports/mig_172b_apply_closeout_20260429.md` | Histology vocab apply |
| `qc_framework_v1/reports/section_8_retro_audits_closeout_20260429.md` | Governance debt cleared |
| `cursor_prompts/CURSOR_PROMPT_*.md` | All Cursor agent prompts |
| `cursor_prompts/CURSOR_PROMPT_mig180b_nlp_upstream_missing_investigation_20260429.md` | **In-flight 1** |
| `cursor_prompts/CURSOR_PROMPT_mig181_pm_syn_size_15_cols_verify_apply_20260429.md` | **In-flight 2** |
| `cursor_prompts/CURSOR_PROMPT_mig177c_lvi_vi_derivatives_reclean_scoping_20260429.md` | **In-flight 3** |
| `cursor_prompts/CURSOR_PROMPT_mig182_cf_87_ajcc_investigation_20260429.md` | **In-flight 4** |
| `cursor_prompts/CURSOR_PROMPT_mig174b_apply_per_side_boolean_20260429.md` | Awaiting Logan ratify |
| `exports/mig176_177_174_review_20260429/` | Raw-source review artifacts (mig_177 LVI bug discovery) |

### §4.2 Auto-memory key files (read first)

**Methodology / pattern memories (cross-cutting):**
- `feedback_motherduck_direct_check.md` — verify against live MD every round
- `feedback_clinical_dates_calendar_only.md` — clinical event dates MUST be DATE
- `feedback_no_cross_db_canonical_sourcing.md` — canonicals are `main.*` standalone
- `feedback_findings_vs_staging.md` — anatomic findings primary; staging follows
- `feedback_extraction_faithfulness_llm_canonical.md` — re-derive from upstream WHERE error=0
- `feedback_surgical_git_add.md` — explicit path/glob; never -A
- `feedback_use_desktop_commander_first.md` — Desktop Commander > Chrome > computer-use
- `feedback_audit_regex_word_boundary.md` — gate-5 audit needs word boundaries
- `feedback_mention_grain_partition_probe.md` — partition-key probe before ROW_NUMBER

**Reference memories:**
- `reference_2digit_year_convention.md` — 20YY rule
- `reference_protocol_v2_md_accounts.md` — MD account gotcha (.eras for Cowork MCP)
- `reference_view_naming_convention.md` — `_VIEW` suffix required
- `reference_canonical_naming_convention.md` — Tier-2 masters naming
- `reference_detail_table_registry_schema.md` — `detail_table_registry_v1` 15 cols; `feeds_master_columns_array` is `VARCHAR[]`

**Project memories — recent close-outs (read for round context):**
- `project_2026-04-29_evening_marathon_round.md` — **multi-lane round overview + 5 reusable patterns**
- `project_mig_179_invasion_events_lvi_rebuild_closeout.md` — supplemental-events architecture
- `project_invasion_family_signoff_2026-04-28.md` — 12-rule clinical library
- `project_motherduck_pro_trial_plan.md` — Pro trial 4-item plan

---

## §5 Database architecture

### §5.1 Tier structure

- **Tier 1** — `note_entities_llm_*`: raw LLM extraction outputs. Registry-seeded as `na` raw-mirror exempt.
- **Tier 2** — `canonical_*_events_v1`: event-grain typed tables. Each ROW = one event/finding/specimen.
- **Tier 2 rollup** — `canonical_*_patient_rollup_v1`: patient-grain rollups from events.
- **Tier 3** — `canonical_patient_master`: THE master patient-grain table. **1,615 cols (97.5% verified).**

### §5.2 Verification registries

- `canonical_table_signoff_registry_v1` — 1 row per canonical_*. Cols: `n_verified`, `n_columns_total`, `n_na`, `n_not_started`, `signoff_migration`, `table_status`.
- `canonical_column_verification_registry_v1` — 1 row per col. Cols: `verification_status`, `verified_by`, `verification_method`, `batch_id`, `notes` (CF appendices accumulate via `| mig_<N>: ...`).
- `manuscript_workspace.detail_table_registry_v1` — 15 cols; `feeds_master_columns_array` is `VARCHAR[]` (TYPE TRAP).
- `manuscript_workspace.cpm_reconciliation_provenance_v1` — provenance log for PM mutating lanes.

### §5.3 Verification methods (controlled vocabulary)

- `derivation_vs_canonical_<source>_<col>` — re-derive from upstream
- `extraction_faithfulness_vs_note_entities_llm_<domain>` — re-derive from Tier 1 WHERE error=0
- `internal_consistency` — pairwise rule
- `auto_provenance_skip` (na) — build_ts, extracted_at, etc.
- `helper_<placeholder>_pending_real_extraction` (na) — Type-B placeholder pattern
- `extraction_faithfulness_vs_archive_pub_v1_0_<table>_<snapshot_ts>` — archive-only source
- `Path C: PM nlp cluster lineage + source-discovery + cohort-uniformity sweep` — mig_180 methodology

---

## §6 Workflow: Cowork ↔ Cursor ↔ Logan

### §6.1 Roles

- **Logan**: clinical-domain expert; ratifies clinical decisions; pastes agent summaries; runs Cursor agents on his other machine.
- **Cursor agents** (per AGENTS protocol): bulk SQL authors; commit + push to GitHub but do NOT write to MD. **Watch for governance violations** — Cowork has caught 3+ in prior rounds (mig_178/173b/163b).
- **Cowork (you)**: orchestrator + verifier + applier + small-fix author. Run Path-C verification on all Cursor work directly against live MD. Catch governance violations and shortfalls. Apply registry-only / low-risk lanes directly. Author Cursor prompts for heavier work.

### §6.2 Path C — the standard apply protocol

For any Cursor-authored migration SQL, do all of these BEFORE any `query_rw`:

1. **Read the SQL file end-to-end** — understand each block + claimed SSOTs
2. **Verify governance**: query live MD for any rows matching the agent's batch_id; if 0, agent honored governance; if >0, agent applied without authorization (governance violation)
3. **Pre-flight probes** (read-only): col count matches prompt; upstream tables live in `main` (`information_schema.tables`); cohort parity 10,871; schema compatibility for any UNION ALL or INSERT BY NAME
4. **Cohort-uniformity sweep BOTH directions** on every BOOLEAN flipped:
   - 0 TRUE → Type-B placeholder → reclassify verified→na in `mig_<N>b`
   - 0 FALSE / TRUE-only / NULL → Type-A presence flag → keep verified, add `CF-COHORT-NEAR-UNIFORM-TRUE-<col>` informational note
5. **Date-type check** — `*_date` cols MUST be DATE (not TIMESTAMP/VARCHAR); open `CF-mig<N>-CLINICAL-DATE-RETYPE` if violated
6. **Data-type sanity** — numeric measurements as DOUBLE (not VARCHAR-with-units)
7. **Cross-source spot-check** on 5+ random rids; trace 1 col's derivation back to upstream
8. **Cross-canonical reconciliation** for cols with multiple SSOTs
9. **Pre-snapshot** affected registry rows + any data-write tables to `archive_pub_v1_0`
10. **Apply** via query_rw (block-by-block due to MCP wrapper)
11. **Verify post-state**: math, signoff resync, 5-gate audit
12. **Author + apply b-cleanup** for any agent-QA misses (registry CF-closure notes, type bugs, missing follow-up)
13. **Write traceability close-out report**, commit + push

### §6.3 When to apply directly vs ask Logan

**Apply directly (Cowork-direct):**
- Registry-only Cowork-authored migrations
- Single-col retype with full preservation probe
- Focused data-write with clear rule
- Path-C-compliant Cursor SQL where Logan has already ratified the design

**Ask Logan first:**
- Cross-canonical reconciles affecting >50 patients with clinical adjudication needed
- Structural schema changes (new tables, dropped tables, mass type changes — like mig_160)
- Clinical definition disputes
- Anything that requires picking between options (R1/R2/R3, Option A/B, etc.)

### §6.4 Pre-snapshot rule

ALWAYS pre-snapshot before mutating verified canonicals:
```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_<table>_pre_mig<N>_<short>_20260429 AS
SELECT research_id, <affected cols>, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig<N>_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_<table>;
```

For idempotency on retry, use `CREATE TABLE IF NOT EXISTS` if first apply may have succeeded partway.

---

## §7 Currently-clean / verified state per domain

### §7.1 Closed CFs (post-marathon round)

| CF | Closure |
|---|---|
| CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS | mig_179b |
| CF-mig177-EVENTS-LYMPHATIC_PRESENT_SEPARATE_MISS | mig_179b |
| CF-mig177-EVENTS-VOCAB-FOACL-EXTRENSIVE-INDETERMINENT-CA-X | mig_179b |
| CF-mig177-ROLLUP-VASC-ALIAS-LVI | mig_179b |
| CF-mig177-PM-VASC-ALIAS-LVI (196 pts) | mig_177b at `dd45a59` |
| CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT (vi axis) | mig_177b |
| CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT (1,065+166 pts) | mig_176b at `e70f36a` |
| CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES | mig_172b at `06a2bdb` |
| CF-mig172-MTC-PTC-MIXED-REJECT (reaffirmed) | mig_178 + mig_172b |
| §8 governance debt (mig_178/173b/163b) | retro audits at `e70f36a` |

### §7.2 Top open CFs (post-marathon)

| CF | Col-impact | Closes via |
|---|---:|---|
| CF-117-US-NODULE-RANGE / EXAM-ID-PORTABILITY / LATERALITY-RAW | 53 each (159 total) | **mig_171b** US LN v2 build (Cursor SQL committed at `123cebb`; needs Logan ratify) |
| CF-GEN07-ROM-OCR | 41 | requires raw extraction redo |
| CF-90-DATE-FORMAT | 38 | **mig_160 apply** (still pending — high-risk structural) |
| CF-87-AJCC | 36 | **mig_182 in-flight** (read-only investigation; fix plan pending Logan ratify) |
| CF-100-DATE-RETYPE | 29 | **mig_160 apply** |
| CF-mig137-PM-MOL-DATE-RETYPE | 25 | **mig_160 apply** |
| CF-mig180-NLP-UPSTREAM-MISSING-* (12 families) | 38 cols | **mig_180b in-flight** |
| CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN | 12 cols × 5,082 pts | **mig_177c in-flight** (scoping; needs Logan ratify Option A or B) |
| CF-mig179-COMBINED-CAP-VASC-DUPLICATION | informational | future canonical_invasion_events_v2 dedupe |
| CF-mig176b-V2-UPSTREAM-EXTRACTION-INFLATION-19PTS | informational | future canonical_us_nodule_v2 rebuild |
| CF-mig173b-VOLUME-FORMULA-CONVENTION | informational | manuscript volume analyses (rectangular vs ellipsoid) |

### §7.3 PM verified column status (post-marathon)

- 1,575 verified / 24 na / 16 not_started / 0 failed / 1,615 total / `in_progress`
- The 16 not_started: 15 `syn_*_size` cols (covered by **mig_181 in-flight**) + 1 misc col

---

## §8 In-flight Cursor lanes (verify when summary lands)

### §8.1 mig_180b — NLP UPSTREAM-MISSING 12-family lineage investigation

**Scope:** 38 cols across 12 nlp_* families flagged with `CF-mig180-NLP-UPSTREAM-MISSING-<family>`. Cursor investigates each family's upstream Tier 1 source, replays derivation where possible, reclassifies verified→na (Type-B) where genuinely missing.

**Verification approach when summary lands:**
1. Read the SQL artifact end-to-end
2. Verify governance: 0 rows in `canonical_column_verification_registry_v1` should have `batch_id LIKE 'mig_180b%'` before Cowork applies
3. Apply per Path C (registry-only writes)
4. Confirm CF-mig180-NLP-UPSTREAM-MISSING-* closure notes appear on the 38 col rows
5. Update PM signoff (counts shouldn't change much; some verified→na reclassifications expected)

### §8.2 mig_181 — PM syn_*_size 15 cols verify+apply

**Scope:** 15 not_started PM cols from mig_173b (right_lobe + left_lobe + isthmus × length_cm/width_cm/height_cm/volume_cc/parse_status). Drops PM not_started 16 → 1.

**Verification approach when summary lands:**
1. Read SQL + audit report
2. Verify 5 spot-check patients per lobe
3. Cohort-uniformity sweep on 3 parse_status cols (multi-valued enum, healthy)
4. Apply per Path C (registry-only)
5. PM verified should climb 1,575 → 1,590

### §8.3 mig_177c — LVI+VI derivative reclean SCOPING

**Scope:** 2,502 LVI + 2,580 VI TRUE→FALSE flippers retain non-null derivative values (lvi_grade, lvi_ordinal_worst, n_tumors_lvi_present + 9 vasc_grade family cols). Cursor surfaces Option A (clear) vs Option B (rederive from refreshed events) for Logan ratification.

**Verification approach when summary lands:**
1. Read scoping report
2. **Logan ratifies Option A or B** before any apply
3. Cursor produces final apply SQL in follow-up `mig_177c_apply` lane (NOT this one)
4. This lane is read-only investigation

### §8.4 mig_182 — CF-87-AJCC investigation

**Scope:** 36 col-impact CF-87-AJCC tagged in mig_87 close-out. Cursor surfaces actual AJCC drift content, manuscript impact, R1/R2/R3 fix options.

**Verification approach when summary lands:**
1. Read investigation report
2. **Logan ratifies fix plan** before any apply
3. Read-only audit only in this lane

---

## §9 Apply queue (priority order, post-marathon)

After §11 first-action checklist, work through in this order:

1. **Verify each in-flight Cursor lane** (mig_180b / mig_181 / mig_177c / mig_182) **as Logan pastes summaries** — verify against live MD per Path C; apply if AGENTS-governance respected; surface to Logan if ratification needed
2. **Apply mig_174b** (when Cursor lands SQL — token-level parser for cnln_img_laterality)
3. **Apply mig_171b** (canonical_us_lymph_node_v2 BUILD; Cursor SQL at `123cebb`; needs Logan ratify before any data write — closes 159 col-impact)
4. **Author + apply mig_177c_apply** (after Logan ratifies Option A or B from mig_177c scoping)
5. **Author + apply mig_182_apply** (after Logan ratifies CF-87-AJCC fix plan from mig_182 scoping)
6. **Apply mig_160** (global date retype, 21 cols × 5 tables) — STRUCTURAL; closes ~190 col-impact CFs and gate5 21→0; **defer until explicit Logan green-light** for the structural sweep
7. **Apply mig_162** (PM finalization + lakehouse coverage report) — runs LAST after PM not_started clears (currently 16 → after mig_181 → 1)

---

## §10 Pending Logan ratifications (don't touch yet)

- **mig_171b** US LN v2 build — Cursor SQL at `123cebb`. 6,973 events / 4,110 patients. Closes 159 col-impact (CF-117 triplet × 53).
- **mig_174b** cnln_img_laterality per-side BOOLEAN — Cursor prompt at `cursor_prompts/CURSOR_PROMPT_mig174b_apply_per_side_boolean_20260429.md`.
- **mig_177c apply path** (Option A clear or Option B rederive) — pending mig_177c scoping deliverable.
- **mig_182 fix plan** (R1/R2/R3) — pending mig_182 investigation deliverable.

---

## §11 First-action checklist

```
1. git fetch origin && git pull --rebase origin main && git log --oneline -25
2. Run §14 5-gate audit (expect gate1=169 / 0 / 0 / 0 / 21)
3. Check PM batch progress + recent registry activity:
     SELECT n_verified, n_na, n_not_started, table_status, signoff_migration
     FROM main.canonical_table_signoff_registry_v1
     WHERE table_name='canonical_patient_master';
     -- Expect: 1575 / 24 / 16 / in_progress / qc_framework_v1/migrations/180_*.sql
4. Check active in-flight lanes (Cursor activity since last commit):
     SELECT batch_id, COUNT(*) AS n, MAX(verified_ts)
     FROM main.canonical_column_verification_registry_v1
     WHERE verified_ts > '2026-04-29 22:00:00'
     GROUP BY 1 ORDER BY 3 DESC;
5. Read MEMORY.md end-to-end (auto-memory index)
6. Read this v9 handoff doc end-to-end
7. Read the 4 close-out reports referenced in §0
8. Read the 4 in-flight Cursor prompts (mig_180b/181/177c/182) so you know what to expect
9. Re-read §6.2 Path C protocol + §8 in-flight verification approaches + §9 apply queue
10. Decide A/B/C from §13
```

**FULL SCOPE REVIEW:** before doing anything else, complete steps 1–9. The marathon round closed many CFs but the database state is large and complex. Do not skip the scope review.

---

## §12 Critical reminders

**Verify all Cursor work directly and thoroughly.** Cursor agents have produced shortcuts that needed cleanup in EVERY round. Specific patterns to watch:

| Lesson | What happened | Lesson learned |
|---|---|---|
| mig_135 | 21 degenerate-FALSE cols not flagged | Cohort-uniformity sweep on every BOOLEAN |
| mig_138 | 447-pt undercount on recurrence_confirmed | Cross-canonical reconciliation before accepting |
| mig_141 | 2 near-uniform-TRUE BOOLEANs missed | Sweep both directions (T-only AND F-only) |
| mig_144 | 4 VARCHAR measurement cols left un-retyped | Audit data_type for every measurement col |
| mig_145 | CT tracheal `not_mentioned` counted as TRUE | Trace BOOLEAN derivations to upstream enum semantics |
| mig_147 | nucmed_cumulative_dose 83% drift vs RAI | Cross-validate cols with multiple authoritative upstreams |
| mig_148 | iodine_avidity_flag placeholder (Type-B → na) | Recognize Type-B placeholder pattern across rounds |
| mig_151 | 3 radtx degenerates + verification_method named ARCHIVED tables | Pre-check `information_schema.tables` for every methodology string |
| mig_154 | 2 Type-A presence flags missed (lvi/vi) | Sweep TRUE-only patterns alongside FALSE-only |
| mig_155 | Agent applied directly to MD without governance | Watch for governance violations; run retroactive verification |
| mig_165 | Mass auto-na on 76 aux tables without governance | Run retroactive verification |
| mig_172 | mtc_ptc_mixed canonical_code Logan rejects (clinical reversal) | When Logan flags clinical concerns, treat as ratification reversal |
| mig_174a | Multi-label fields with literal 'null' token, casing/whitespace drift, embedded newlines | Token-level enumeration before any parser design |
| mig_177 | Events extractor combined-CAP miss; rollup right; 91+ patients undocumented LVI | **Source-text review is the only way to catch extractor bugs** |
| mig_178/173b/163b | Cursor applied directly to MD without Cowork Path C | Retroactive verification mandatory |
| mig_179 | feeds_master_columns_array NULL::VARCHAR vs VARCHAR[] schema mismatch | INSERT BY NAME requires exact-type NULL casts |

**Standing rules:**
- **Cohort parity 10,871 invariant**
- Run cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions
- Pre-check `information_schema.tables` for every methodology string
- Audit `data_type` for every numeric measurement col
- Check `*_date` cols are DATE not TIMESTAMP/VARCHAR
- **Check MotherDuck directly every round — never trust prior summaries**
- Verify governance compliance before applying any Cursor SQL (probe live MD for the agent's batch_id; should be 0 rows pre-apply)
- After applying, run 5-gate audit; should remain 169/0/0/0/21 unless lane explicitly changes verified count
- Pre-snapshot all affected slices to `archive_pub_v1_0` before mutating
- Surgical git add only (explicit paths)

---

## §13 Decision tree

After §11 first-action checklist, decide:

**A. New Cursor lane summary just arrived from Logan** → **verify it** via §8 verification approach for that specific lane (mig_180b / mig_181 / mig_177c / mig_182). Apply if governance-clean. Surface to Logan if ratification needed.

**B. Logan ratified mig_171b or mig_174b** → apply per Path C. Both close significant CFs (mig_171b = 159 col-impact; mig_174b = parser fix).

**C. No new Cursor lanes pending; no Logan ratifications** → either (i) author next round of Cursor prompts (e.g., mig_183 for next CF backlog item), or (ii) apply mig_160 if Logan green-lights structural date retype, or (iii) consolidate memory + write status report.

---

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

**Expected at handoff: gate1=169, gate2=0, gate3=0, gate4=0, gate5=21.** After mig_160 apply: gate5=0.

---

## §15 Verbatim opening message to paste into the new Cowork chat

---

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v9.md` end-to-end before any tool use. Then read in order:
>
> 1. `qc_framework_v1/reports/mig_179_apply_closeout_20260429.md` — events extractor LVI rebuild close-out
> 2. `qc_framework_v1/reports/mig_180_apply_closeout_20260429.md` — PM nlp_* cluster closure
> 3. `qc_framework_v1/reports/mig_172b_apply_closeout_20260429.md` — histology vocab normalization
> 4. `qc_framework_v1/reports/section_8_retro_audits_closeout_20260429.md` — governance debt cleared
> 5. `cursor_prompts/CURSOR_PROMPT_mig180b_*.md` + `mig_181_*.md` + `mig_177c_*.md` + `mig_182_*.md` — the 4 in-flight Cursor lanes
> 6. `MEMORY.md` (auto-memory index)
>
> Then run the §11 first-action checklist (FULL SCOPE REVIEW first — git fetch + log, 5-gate audit, PM batch progress, in-flight lane status, all 4 in-flight Cursor prompts so you know what to expect when summaries land). Then decide A/B/C from §13.
>
> **Standing context:** I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're closing out the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`) for manuscript-grade survival/recurrence/outcomes analyses. **gate1 is at 169 / PM at 1,575 / 1,615 (97.5%) verified.** You're the orchestrator + verifier + applier; Cursor agents do the bulk lane work; I'm the final ratifier.
>
> **You have:**
> - **Desktop Commander MCP** for git/shell on my actual Mac
> - **MotherDuck MCP** (read + write against `thyroid_canonical_publication_v1_0` and `"Thyroid 2026 UPdated".archive_pub_v1_0`)
> - **GitHub repo** at `/Users/ros/THyroid 2026` (tip `06a2bdb`; URL `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`)
> - **Auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/` with ~150+ entries
>
> **CURRENTLY IN FLIGHT (Cursor working on these one at a time — verify each summary as it lands):**
> 1. **mig_180b** NLP UPSTREAM-MISSING 12-family lineage investigation (38 cols)
> 2. **mig_181** PM `syn_*_size` 15 cols verify+apply (drops PM not_started 16→1)
> 3. **mig_177c** LVI+VI derivative reclean SCOPING (Option A clear vs Option B rederive — I ratify before apply)
> 4. **mig_182** CF-87-AJCC investigation (36 col-impact; R1/R2/R3 fix plan — I ratify before apply)
>
> **AWAITING MY RATIFICATION (don't touch yet):**
> - **mig_171b** canonical_us_lymph_node_v2 BUILD — Cursor SQL at `123cebb`; closes 159 col-impact
> - **mig_174b** cnln_img_laterality per-side BOOLEAN apply — Cursor prompt drafted
> - **mig_177c apply path** (Option A vs B) — pending scoping deliverable
> - **mig_182 fix plan** (R1/R2/R3) — pending investigation deliverable
> - **mig_160** structural date retype — high-risk; defer until I explicitly green-light
>
> **CRITICAL RIGOR REMINDER:** verify all Cursor work directly against MotherDuck. EVERY round has shipped with agent QA misses Cowork had to clean up. Be skeptical of every "verified clean" Cursor claim. Run cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions. Pre-check `information_schema.tables` for every methodology string. Audit `data_type` for every numeric measurement col. Check date-type for every `*_date` col. Verify governance compliance before applying any Cursor SQL (probe live MD for the agent's batch_id; should be 0 rows pre-apply).
>
> **First task:** §11 first-action checklist (FULL SCOPE REVIEW — don't skip). Then choose A/B/C from §13:
> - **(A)** Cursor lane summary arrived → verify per §8 + apply per Path C
> - **(B)** I ratified mig_171b or mig_174b → apply per Path C
> - **(C)** Nothing new in flight → either author next-round Cursor prompts (e.g., mig_183 for next CF backlog), apply mig_160 if I green-light, or consolidate memory + status
>
> I'll paste agent summaries from in-flight Cursor runs separately as they come in — verify each against live MD per Path C and apply if AGENTS-governance was respected.

---

End of handoff doc.
