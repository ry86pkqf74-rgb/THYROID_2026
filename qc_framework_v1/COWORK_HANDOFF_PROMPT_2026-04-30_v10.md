# Cowork Handoff Prompt v10 — Thyroid Canonical Publication v1.0 Cleanup

Generated: 2026-04-30 (post-major-round) — supersedes v9
Tip of `origin/main` at handoff: **`16a9833`** (verify with `git fetch && git log --oneline -25`)

---

## §0 TL;DR / first actions

You are continuing a multi-week cleanup of `thyroid_canonical_publication_v1_0` (MotherDuck) toward a manuscript-grade publication lakehouse for thyroid cancer survival/recurrence/outcomes analyses.

**Current state — MAJOR PROGRESS this round:**
- gate1 = **172 verified canonicals** (was 169 at v9; +3 new: us_lymph_node_events_v2, us_lymph_node_patient_rollup_v2, val_mig171b)
- 5-gate audit: **172 / 0 / 0 / 0 / 25** (gate5=25 PM date cols remain — non-blocking)
- **PM = 1,596 / 24 / 0 / 1,620** (`table_status='verified'` — 100% backbone done)
- **62/62 Tier-2 canonicals verified (100%)**
- Cohort parity: **10,871 / 10,871** ✓
- Manuscript readiness verdict: **READY** for survival/recurrence/outcomes analyses (per `qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260430.md`)

**Read in this order before any tool use:**

1. This handoff doc (§0–§14) end-to-end
2. `qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260430.md` — overall state
3. `qc_framework_v1/reports/mig_171b_apply_closeout_20260430.md` — US LN v2 build
4. `qc_framework_v1/reports/mig_183_apply_closeout_20260430.md` — PM final not_started closure
5. `qc_framework_v1/reports/mig_177c_apply_174b_apply_closeout_20260430.md` — laterality + LVI/VI clean
6. `qc_framework_v1/reports/mig_160_apply_closeout_20260430.md` — date retype + VIEW patch
7. `qc_framework_v1/reports/section_8_retro_audits_closeout_20260430.md` — §8 retro round 2
8. `cursor_prompts/CURSOR_PROMPT_mig184_v2_*.md` + `mig_185_*.md` + `mig_186_*.md` + `mig_187_*.md` — 4 in-flight Cursor lanes
9. `MEMORY.md` (auto-memory index) + recent project memories

**Then run the §11 first-action checklist (FULL SCOPE REVIEW first), then dispatch per §13.**

**Workflow:** Cowork (you) is orchestrator + verifier + applier. Cursor agents do bulk SQL authoring on Logan's other machine. Logan ratifies design decisions and pastes agent summaries to you. **VERIFY EVERY CURSOR CLAIM DIRECTLY AGAINST LIVE MOTHERDUCK** — every prior round has shipped with agent QA misses Cowork had to clean up.

---

## §1 Project mission

**Logan Glosser**, Emory thyroid-cancer surgery researcher. Database: `thyroid_canonical_publication_v1_0` on MotherDuck (account `logan.glosser.eras@gmail.com`). Goal: produce a clean, audit-passing canonical layer ready for **manuscript-grade survival/recurrence/outcomes analyses**.

- Cohort: **10,871 distinct research_id**
- Backbone: `canonical_patient_master` (1,620 cols, 1,596 verified, table_status='verified')
- Tier-2 events / patient_rollup canonicals: **62/62 verified** (100%)
- Authoritative SSOT: live MotherDuck — never trust prior summaries

**You are: orchestrator + verifier + applier.** Cursor agents do bulk lane work on Logan's other machine. Logan ratifies clinical decisions and pastes agent summaries to you. You verify against live MD per Path C, apply if AGENTS-governance was respected, and ship b-cleanup migrations for any agent QA misses or governance violations.

---

## §2 Current state (post-2026-04-30 round)

| Metric | Value |
|---|---:|
| Origin tip | `16a9833` |
| **gate1 (verified canonicals)** | **172** |
| gate2/3/4 | 0/0/0 ✓ |
| gate5 (date retype) | **25** (PM cols residual; mig_160b closes) |
| **Tier-2 canonicals at table_status='verified'** | **62/62 (100%)** |
| **PM signoff** | **1,596 v / 24 na / 0 not_started / 1,620** (`verified`) |
| Cohort parity | 10,871/10,871 ✓ |

### §2.1 Lanes applied this evening (chronological)
- ✅ **mig_171b** canonical_us_lymph_node v2 BUILD (`9301b58`) — 6,973 events / 4,110 pts / 10,871 rollup; 10/10 validation gates G1-G8+G10 PASS, G9 WARN expected
- ✅ **mig_183** PM `vessel_count` last not_started col verified (`baaa2f4`) — PM table_status flips to `verified`
- ✅ **mig_174b** cnln_img_laterality per-side BOOLEANs (`e51d268`) — 5 BOOL cols added; counts match Cursor's prediction exactly
- ✅ **mig_177c_apply** Option A clear-only (`e51d268`) — 28,099 derivative cells cleared on 5,082 LVI/VI flippers; all 8 residual checks = 0
- ✅ **mig_160** structural date retype (`16a9833`) — 21 cols TIMESTAMP/VARCHAR → DATE; 1 dependent VIEW patched; 12 others auto-recompiled clean
- ✅ **mig_162** PM finalization + manuscript readiness report (`16a9833`)

### §2.2 In-flight Cursor lanes (4 prompts dispatched 2026-04-30)
**Logan dispatching these to Cursor.** Verify each agent summary against live MD per Path C when Logan pastes it:

- 🔄 **mig_184_v2** — R1 AJCC derivation (Logan-RATIFIED 8 rules). Prompt at `cursor_prompts/CURSOR_PROMPT_mig184_v2_r1_ajcc_RATIFIED_20260430.md`. Expected deliverables: `qc_framework_v1/migrations/184_v2_r1_ajcc_derivation_ratified_20260430.sql` (apply skeleton READY for Cowork Path C) + report + 3 adjudication CSVs (r1b N1 split, r1d T4 invasion, r1e mixed histology). Closes CF-87-AJCC (36 cols).

- 🔄 **mig_185** — `canonical_path_malignant_events_v1` 533-duplicate dedupe scoping. Prompt at `cursor_prompts/CURSOR_PROMPT_mig185_path_malignant_duplicate_probe_20260430.md`. Expected deliverables: classification report (Buckets A-D) + 3 dedupe options (R-A/R-B/R-C). **Logan must ratify rule before any apply.**

- 🔄 **mig_186** — NIFTP + uncertain-malignancy exclusion (213 + 7 events / 195 patients). Prompt at `cursor_prompts/CURSOR_PROMPT_mig186_niftp_uncertain_exclusion_20260430.md`. Expected deliverables: inventory + 4 disposition options (R-A/R-B/R-C/R-D — Cowork recommends R-D). **Logan must ratify rule before any apply.**

- 🔄 **mig_187** — `canonical_us_exam_master` rebuild (resolves 159 fallback us_exam_id from mig_171b). Prompt at `cursor_prompts/CURSOR_PROMPT_mig187_exam_master_rebuild_20260430.md`. Expected deliverables: 159-ID profile + 3 rebuild options (R-A/R-B/R-C). **Logan must ratify rule before any apply.**

### §2.3 Awaiting Logan ratification
- **mig_185** dedupe rule (R-A clear-only / R-B (rid,surg,ord,sridx) keep-MAX / R-C completeness-score collapse)
- **mig_186** NIFTP disposition rule (recommend R-D = delete + archive + CF)
- **mig_187** exam-master rebuild approach (R-A insert / R-B supplemental table / R-C accept fallbacks)
- **r1c size CSV review** — 72 patients / 85 events for hand-curation at `exports/mig184_r1_adjudication_20260430/r1c_size_unavailable_residual_121events.csv`

### §2.4 Pending Cowork-direct lane
- **mig_160b** — close 25 remaining PM date cols (gate5 → 0). Mirror mig_160 pattern: pre-snapshot PM + ALTER COLUMN per col + dependent-VIEW recompile if needed. Low-risk; Logan green-lit (a)-(e) for date retype work.

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
- **Tip at handoff:** `16a9833`
- **Author:** `Logan Glosser <logan.glosser@gmail.com>` for all commits
- **Surgical git add ONLY** — explicit paths/file-globs; never `-A` or `scripts/output/`. Lint Python before commit if `.py` changed (per `feedback_commit_workflow.md`).

### §3.3 MotherDuck

- **Tools:**
  - `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query` (read-only)
  - `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query_rw` (writes — one-statement-per-call wrapper)
- **Primary DB:** `thyroid_canonical_publication_v1_0` (live publication, MD account `logan.glosser.eras@gmail.com`)
- **Archive DB:** `"Thyroid 2026 UPdated"` schema `archive_pub_v1_0` (pre-snapshots BEFORE any mutating UPDATE/ALTER)
- **DuckDB quirks:**
  - `CURRENT_TIMESTAMP` is TIMESTAMPTZ → cast to TIMESTAMP for build_ts cols
  - FILTER not supported on window funcs (use SUM(CASE) OVER)
  - Cross-DB FROM in canonicals forbidden (`main.*` only) — except for archive snapshots
  - `ALTER COLUMN ... SET DATA TYPE T USING <expr>` works for in-place retypes
  - `INSERT BY NAME` requires source SELECT to have correct column types — `feeds_master_columns_array` is `VARCHAR[]` not `VARCHAR` (recurring trap)
- **MCP wrapper:** one statement per call — do NOT use `BEGIN TRANSACTION;` / `COMMIT;`

### §3.4 Cursor agents

- Logan runs Cursor agents on his other machine to author bulk SQL.
- **AGENTS governance:** agents commit SQL only; **Cowork applies via Path C after independent verification.** Verify each agent's commit is governance-compliant (no MD writes) before applying.
- Cursor prompts are dropped into `cursor_prompts/` directory; Logan dispatches to Cursor.
- Logan pastes agent summaries to you when each lane lands.

### §3.5 Auto-memory

- **Path:** `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
- **Index:** `MEMORY.md` (~150+ entries; index lines after 200 may be truncated — keep concise)
- Always read before deciding; updates persist across sessions.

---

## §4 Reference documents

### §4.1 In repo (`/Users/ros/THyroid 2026`)

| Path | What it is |
|---|---|
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v10.md` | **This doc** |
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v9.md` | Predecessor |
| `qc_framework_v1/migrations/*.sql` | All migration SQL (190+) |
| `qc_framework_v1/reports/*.md` | Read-only design + audit + close-out reports |
| `qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260430.md` | **Overall state — read first** |
| `qc_framework_v1/reports/mig_171b_apply_closeout_20260430.md` | US LN v2 BUILD |
| `qc_framework_v1/reports/mig_183_apply_closeout_20260430.md` | PM final not_started |
| `qc_framework_v1/reports/mig_177c_apply_174b_apply_closeout_20260430.md` | laterality + LVI/VI clean |
| `qc_framework_v1/reports/mig_160_apply_closeout_20260430.md` | Date retype + VIEW patch |
| `qc_framework_v1/reports/section_8_retro_audits_closeout_20260430.md` | §8 retro round 2 |
| `cursor_prompts/CURSOR_PROMPT_mig184_v2_*.md` | **In-flight 1: R1 AJCC** |
| `cursor_prompts/CURSOR_PROMPT_mig185_*.md` | **In-flight 2: dedupe** |
| `cursor_prompts/CURSOR_PROMPT_mig186_*.md` | **In-flight 3: NIFTP** |
| `cursor_prompts/CURSOR_PROMPT_mig187_*.md` | **In-flight 4: exam-master rebuild** |
| `exports/mig184_r1_adjudication_20260430/` | R1 AJCC adjudication CSVs (Logan reviewing 72-pt residual) |

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
- `feedback_alter_view_dependents.md` — type changes break dependent VIEW bodies (mig_160 hit this)

**Reference memories:**
- `reference_2digit_year_convention.md` — 20YY rule (00=2000, 25=2025)
- `reference_protocol_v2_md_accounts.md` — MD account gotcha (.eras for Cowork MCP)
- `reference_view_naming_convention.md` — `_VIEW` suffix required
- `reference_canonical_naming_convention.md` — Tier-2 masters naming
- `reference_detail_table_registry_schema.md` — `feeds_master_columns_array` is `VARCHAR[]`

**Project memories — recent close-outs:**
- `project_2026-04-30_section8_retro_round_2.md` — §8 retro round 2 patterns
- `project_2026-04-29_evening_marathon_round.md` — multi-lane round overview
- `project_motherduck_pro_trial_plan.md` — Pro trial 4-item plan

---

## §5 Database architecture

### §5.1 Tier structure

- **Tier 1** — `note_entities_llm_*`: raw LLM extraction outputs. Registry-seeded as `na` raw-mirror exempt.
- **Tier 2** — `canonical_*_events_v1/v2`: event-grain typed tables. Each ROW = one event/finding/specimen.
- **Tier 2 rollup** — `canonical_*_patient_rollup_v1/v2`: patient-grain rollups from events.
- **Tier 3** — `canonical_patient_master`: THE master patient-grain table. **1,620 cols (98.5% verified, 1.5% na, 0% not_started — fully signed off).**

### §5.2 Verification registries

- `canonical_table_signoff_registry_v1` — 1 row per canonical_*. Cols: `n_verified`, `n_columns_total`, `n_na`, `n_not_started`, `signoff_migration`, `table_status`.
- `canonical_column_verification_registry_v1` — 1 row per col. Cols: `verification_status`, `verified_by`, `verification_method`, `batch_id`, `notes` (CF appendices accumulate via `| mig_<N>: ...`).
- `manuscript_workspace.detail_table_registry_v1` — 15 cols; `feeds_master_columns_array` is `VARCHAR[]` (TYPE TRAP).
- `manuscript_workspace.cpm_reconciliation_provenance_v1` — provenance log for PM mutating lanes.

### §5.3 Verification methods (controlled vocabulary, top entries)

- `mechanical_derivation_compare` (244)
- `derivation_re_derivation_post_rollup_rebuild` (173)
- `derivation_re_derivation_against_verified_events` (141)
- `Path C: PM nlp cluster lineage + source-discovery + cohort-uniformity sweep` (115)
- `external_registry_nsqip_study_linkage_on_cpm` (101)
- `auto_no_source_counterpart` (96)
- `derivation_re_derivation_post_events_repair` (87)
- `auto_provenance_skip` (na — for build_ts/extracted_at/etc.)
- `helper_<placeholder>_pending_real_extraction` (na — Type-B placeholder pattern)
- (full list at v1_0_manuscript_readiness_report §3)

---

## §6 Workflow: Cowork ↔ Cursor ↔ Logan

### §6.1 Roles

- **Logan**: clinical-domain expert; ratifies clinical decisions; pastes agent summaries; runs Cursor agents on his other machine; reviews CSVs that need clinical adjudication.
- **Cursor agents** (per AGENTS protocol): bulk SQL authors; commit + push to GitHub but do NOT write to MD. **Watch for governance violations** — Cowork has caught 3+ in prior rounds (mig_178/173b/163b/180b/181). Each round, run §8 retro audit on lanes that touched MD without authorization.
- **Cowork (you)**: orchestrator + verifier + applier + small-fix author. Run Path-C verification on all Cursor work directly against live MD. Catch governance violations and shortfalls. Apply registry-only / low-risk lanes directly. Author Cursor prompts for heavier work.

### §6.2 Path C — the standard apply protocol

For any Cursor-authored migration SQL, do all of these BEFORE any `query_rw`:

1. **Read the SQL file end-to-end** — understand each block + claimed SSOTs
2. **Verify governance**: query live MD for any rows matching the agent's batch_id; if 0, agent honored governance; if >0, agent applied without authorization (governance violation; run §8 retro audit pattern)
3. **Pre-flight probes** (read-only): col count matches prompt; upstream tables live in `main` (`information_schema.tables`); cohort parity 10,871; schema compatibility for any UNION ALL or INSERT BY NAME
4. **Cohort-uniformity sweep BOTH directions** on every BOOLEAN flipped:
   - 0 TRUE → Type-B placeholder → reclassify verified→na in `mig_<N>b`
   - 0 FALSE / TRUE-only / NULL → Type-A presence flag → keep verified, add `CF-COHORT-NEAR-UNIFORM-TRUE-<col>` informational note
5. **Date-type check** — `*_date` cols MUST be DATE (not TIMESTAMP/VARCHAR); open `CF-mig<N>-CLINICAL-DATE-RETYPE` if violated
6. **Data-type sanity** — numeric measurements as DOUBLE (not VARCHAR-with-units)
7. **Dependent-VIEW recompile risk** — for any ALTER COLUMN, query `information_schema.views` for VIEWs that reference the col; recompile if old type semantics break (e.g., `length(trim(...))` on DATE input — caught in mig_160)
8. **Cross-source spot-check** on 5+ random rids; trace 1 col's derivation back to upstream
9. **Pre-snapshot** affected registry rows + any data-write tables to `archive_pub_v1_0`
10. **Apply** via query_rw (block-by-block due to MCP wrapper)
11. **Verify post-state**: math, signoff resync, 5-gate audit
12. **Author + apply b-cleanup** for any agent-QA misses (registry CF-closure notes, type bugs, missing follow-up, broken VIEWs)
13. **Write traceability close-out report**, commit + push

### §6.3 When to apply directly vs ask Logan

**Apply directly (Cowork-direct):**
- Registry-only Cowork-authored migrations
- Single-col retype with full preservation probe
- Focused data-write with clear rule (after Logan ratifies the rule)
- Path-C-compliant Cursor SQL where Logan has already ratified the design
- mig_160b (date retype) — Logan green-lit (a)-(e) class

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

For idempotency on retry, use `CREATE TABLE IF NOT EXISTS` if first apply may have succeeded partway.

---

## §7 Currently-clean / verified state

### §7.1 Recently closed CFs (this 2026-04-30 round)

| CF | Closure |
|---|---|
| CF-mig171-DESIGN-RATIFICATION-PENDING + 3 mig_171 source-coverage CFs | mig_171b |
| CF-mig183-PM-VESSEL-COUNT-LAST-NOT-STARTED | mig_183 |
| CF-mig174-CPM-CNLN-IMG-LATERALITY-MULTILABEL | mig_174b |
| CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN | mig_177c_apply |
| CF-119-FROZEN-ROLLUP-DATE-RETYPE | mig_160 |
| CF-mig121-ETE-EVENT-LAST-ALIVE-RETYPE | mig_160 |
| CF-100-DATE-RETYPE | mig_160 |
| CF-90-DATE-FORMAT (canonical_recurrence_v1) | mig_160 |
| CF-mig137-PM-MOL-DATE-RETYPE on canonical_molecular_genetics_v2 | mig_160 |

### §7.2 Top open CFs (post-round)

| CF | Col-impact | Closes via |
|---|---:|---|
| CF-mig171b-EXAM-MASTER-REBUILD | 77 | **mig_187 in-flight** (159 fallback IDs) |
| CF-mig136-DAYS-SEMANTIC | 58 | resolved 2026-04-29 mig_175b |
| CF-117-US-EXAM-ID-PORTABILITY | 53 | mig_171b addressed for LN; remaining = future US-nodule rebuild |
| CF-87-AJCC | 36 | **mig_184_v2 in-flight** (R1 derivation, 8 ratified rules) |
| CF-117-US-GLAND-PARENCHYMA | 28 | future US-gland rebuild |
| CF-mig137-PM-MOL-DATE-RETYPE on PM | 25 | **mig_160b pending** (Cowork-direct) |
| CF-117-US-LN-SHELL | 23 | mig_171b superseded; tag retained for trace |
| (NEW) CF-mig185-PATH-MALIGNANT-DUPLICATES | 533 events | **mig_185 in-flight** (Logan dedupe rule pending) |
| (NEW) CF-mig186-NIFTP-RECLASSIFICATION | 213 + 7 events | **mig_186 in-flight** (Logan disposition pending) |

### §7.3 PM verified column status

- **1,596 verified / 24 na / 0 not_started / 1,620 total** — `table_status='verified'`
- 5 new BOOLEANs added by mig_174b (cnln_img_*_present)
- vessel_count flipped to verified by mig_183

---

## §8 In-flight Cursor lanes (verify when Logan pastes summaries)

### §8.1 mig_184_v2 — R1 AJCC derivation (RATIFIED 8 rules)

**Logan-ratified rules** (verbatim from `cursor_prompts/CURSOR_PROMPT_mig184_v2_*.md`):
1. AJCC version: **AJCC 8** (2018+)
2. `gross_ete=1` + microscopic-text contradiction: **trust qualifier** → no upgrade
3. N1 unspecified (2,378 events): **keep as N1** at path-event grain; populate N1a/N1b at PM grain via upstream LN data
4. Stage-group computation: **PM grain only**
5. Mixed histology (e.g., MTC | PTC): **most aggressive component** for `stage_group_resolved`
6. T4 invasion: gross_ete=1 → T3b; cross-check `canonical_invasion_events_v1` for laryngeal/tracheal → T4a; prevertebral/mediastinal → T4b
7. Size-unavailable: **COALESCE** with tumor_size_cm_per_surgery (recovers 480/601); microcarcinoma → T1a default; NIFTP → exclude (mig_186); 72 patients hand-curate via r1c CSV
8. Age-unknown: **0 patients** — auto-resolves

**Verification approach when summary lands:**
1. Read SQL artifact end-to-end
2. Verify governance: 0 rows in `canonical_column_verification_registry_v1` should have `batch_id LIKE 'mig_184_v2%'` before Cowork applies
3. Apply skeleton SQL block-by-block per Path C (registry note appendix + new `*_resolved` cols + UPDATEs per ratified rules)
4. Confirm CF-87-AJCC closure notes appear on the 36 + 9 = 45 col rows
5. Cross-source drift cohort sanity (most should resolve under R1 rules)

### §8.2 mig_185 — path-malignant duplicate dedupe SCOPING

**Scope:** 533 duplicate (rid, surg_ep, tumor_ord) tuples in `canonical_path_malignant_events_v1` (6,689 total / 6,156 distinct = 533 dup excess). Cursor classifies into Buckets A-D (fully_identical / audit_only / synoptic_row_ix-different / clinically_different) and proposes 3 dedupe rules.

**Verification approach when summary lands:**
1. Read scoping report
2. **Logan ratifies R-A / R-B / R-C** before any apply
3. Cursor produces final apply SQL in follow-up `mig_185_apply` lane
4. Pre-snapshot mandatory before any DELETE

### §8.3 mig_186 — NIFTP + uncertain exclusion SCOPING

**Scope:** 213 NIFTP + 7 uncertain events / 195 patients in `canonical_path_malignant_events_v1`. WHO 2017 reclassified NIFTP as non-malignant — these shouldn't be on a "malignant events" table. Cursor surfaces 4 disposition options; Cowork recommends R-D (delete + archive snapshot + CF tag).

**Verification approach when summary lands:**
1. Read inventory report
2. **Logan ratifies R-A / R-B / R-C / R-D** before any apply
3. Cursor produces apply SQL in follow-up
4. Cascade impact check: rebuild downstream canonicals if needed (e.g., canonical_invasion_events_v1, canonical_path_malignant_patient_rollup_v1)

### §8.4 mig_187 — canonical_us_exam_master rebuild

**Scope:** 159 fallback `us_exam_id` values from mig_171b. Cursor profiles missing (rid, exam_date) pairs and proposes 3 rebuild approaches.

**Verification approach when summary lands:**
1. Read scoping report
2. **Logan ratifies R-A / R-B / R-C** before any apply
3. After apply, **re-run mig_171b §B** (events build) to flip G9 from WARN to PASS

---

## §9 Apply queue (priority order, post-round)

After §11 first-action checklist, work through in this order:

1. **Verify each in-flight Cursor lane** (mig_184_v2 / mig_185 / mig_186 / mig_187) **as Logan pastes summaries** — verify against live MD per Path C; surface for Logan ratification; apply if AGENTS-governance respected
2. **Apply mig_184_v2** (after Logan reviews 72-pt size CSV + ratifies any rule edge-cases)
3. **Apply mig_185** (after Logan picks dedupe rule R-A/R-B/R-C)
4. **Apply mig_186** (after Logan picks disposition R-A/R-B/R-C/R-D)
5. **Apply mig_187** (after Logan picks rebuild approach R-A/R-B/R-C; re-run mig_171b §B to flip G9 PASS)
6. **Apply mig_160b** (Cowork-direct — closes 25 PM date cols; gate5 → 0)
7. **Final 5-gate audit** + final manuscript readiness report (mig_162b if needed)

---

## §10 Pending Logan ratifications (don't touch yet)

- **mig_185 dedupe rule** (R-A clear-only / R-B (rid,surg,ord,sridx) keep-MAX / R-C completeness-score collapse) — pending Cursor scoping deliverable
- **mig_186 disposition rule** (recommend R-D = delete + archive + CF) — pending Cursor inventory deliverable
- **mig_187 rebuild approach** (R-A insert / R-B supplemental / R-C accept fallbacks) — pending Cursor profile deliverable
- **mig_184_v2** edge-case rules — pending Cursor R1 skeleton
- **r1c size CSV** — 72 patients / 85 events for hand-curation; logan_size_cm + logan_t_stage cols to fill

---

## §11 First-action checklist

```
1. git fetch origin && git pull --rebase origin main && git log --oneline -25
2. Run §14 5-gate audit (expect gate1=172 / 0 / 0 / 0 / 25)
3. Check PM batch progress + recent registry activity:
     SELECT n_verified, n_na, n_not_started, table_status, signoff_migration
     FROM main.canonical_table_signoff_registry_v1
     WHERE table_name='canonical_patient_master';
     -- Expect: 1596 / 24 / 0 / verified / qc_framework_v1/migrations/183_pm_vessel_count_verify_apply_20260430.sql (or later)
4. Check active in-flight lanes (Cursor activity since last commit):
     SELECT batch_id, COUNT(*) AS n, MAX(verified_ts)
     FROM main.canonical_column_verification_registry_v1
     WHERE batch_id LIKE 'mig_184_v2%' OR batch_id LIKE 'mig_185%' OR batch_id LIKE 'mig_186%' OR batch_id LIKE 'mig_187%'
     GROUP BY 1 ORDER BY 3 DESC;
     -- Expect: 0 rows (Cursor lanes are read-only scoping)
5. Read MEMORY.md end-to-end (auto-memory index)
6. Read this v10 handoff doc end-to-end
7. Read v1_0_manuscript_readiness_report_20260430.md (overall state)
8. Read the 5 close-out reports referenced in §0 (mig_171b / 183 / 174b+177c_apply / 160 / section_8_retro_round_2)
9. Read the 4 in-flight Cursor prompts (mig_184_v2 / 185 / 186 / 187)
10. Re-read §6.2 Path C protocol + §8 in-flight verification approaches + §9 apply queue
11. Decide A/B/C from §13
```

**FULL SCOPE REVIEW:** before doing anything else, complete steps 1–10. The 2026-04-30 round closed many CFs and 3 new canonicals landed; the database is in a strong state but data quality issues (533 dups, 195 NIFTP patients) remain and will be addressed by the in-flight lanes.

---

## §12 Critical reminders

**Verify all Cursor work directly and thoroughly.** Cursor agents have produced shortcuts that needed cleanup in EVERY round. Specific patterns to watch:

| Pattern | What happened | Lesson |
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
| mig_155 | Agent applied directly to MD without governance | Watch for governance violations; run §8 retroactive verification |
| mig_165 | Mass auto-na on 76 aux tables without governance | Run retroactive verification |
| mig_172 | mtc_ptc_mixed canonical_code Logan rejects (clinical reversal) | When Logan flags clinical concerns, treat as ratification reversal |
| mig_174a | Multi-label fields with literal 'null' token, casing/whitespace drift | Token-level enumeration before any parser design |
| mig_177 | Events extractor combined-CAP miss; 91+ patients undocumented LVI | **Source-text review is the only way to catch extractor bugs** |
| mig_178/173b/163b/180b/181 | Cursor applied to MD without Cowork Path C | Retroactive verification mandatory; §8 retro audit pattern is now established |
| mig_179 | feeds_master_columns_array NULL::VARCHAR vs VARCHAR[] schema mismatch | INSERT BY NAME requires exact-type NULL casts |
| mig_160 (this round) | `length(trim(date_col))` broke after DATE retype | Pre-flight scan for VIEWs whose body uses VARCHAR-only ops on cols being retyped |
| Logan's r1a CSV review | 533 path-malignant dups + 195 NIFTP patients found | **Logan's clinical eye on row-level data catches what schema-level audits miss** |

**Standing rules:**
- **Cohort parity 10,871 invariant**
- Run cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions
- Pre-check `information_schema.tables` for every methodology string
- Audit `data_type` for every numeric measurement col
- Check `*_date` cols are DATE not TIMESTAMP/VARCHAR
- **Check MotherDuck directly every round — never trust prior summaries**
- Verify governance compliance before applying any Cursor SQL (probe live MD for the agent's batch_id; should be 0 rows pre-apply)
- After applying, run 5-gate audit; should remain 172/0/0/0/25 unless lane explicitly changes verified count
- Pre-snapshot all affected slices to `archive_pub_v1_0` before mutating
- Surgical git add only (explicit paths)
- For ALTER COLUMN: pre-flight scan dependent VIEWs; patch broken ones in same commit

---

## §13 Decision tree

After §11 first-action checklist, decide:

**A. New Cursor lane summary just arrived from Logan** → **verify it** via §8 verification approach for that specific lane (mig_184_v2 / mig_185 / mig_186 / mig_187). Apply if governance-clean. Surface to Logan if ratification needed.

**B. Logan ratified mig_185 / mig_186 / mig_187 dedupe rule** → author + apply final apply lane per Path C.

**C. No new Cursor lanes pending; no Logan ratifications** → either (i) author next round of Cursor prompts, or (ii) **apply mig_160b** (close 25 PM date cols → gate5 = 0; Cowork-direct, low-risk), or (iii) consolidate memory + write status report.

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

**Expected at handoff: gate1=172, gate2=0, gate3=0, gate4=0, gate5=25.** After mig_160b apply: gate5=0.

---

## §15 Verbatim opening message to paste into the new Cowork chat

---

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v10.md` end-to-end before any tool use. Then read in order:
>
> 1. `qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260430.md` — current overall state (manuscript-ready)
> 2. `qc_framework_v1/reports/mig_171b_apply_closeout_20260430.md` — US LN v2 BUILD
> 3. `qc_framework_v1/reports/mig_183_apply_closeout_20260430.md` — PM final not_started
> 4. `qc_framework_v1/reports/mig_177c_apply_174b_apply_closeout_20260430.md` — laterality + LVI/VI clean
> 5. `qc_framework_v1/reports/mig_160_apply_closeout_20260430.md` — date retype + dependent VIEW patch
> 6. `qc_framework_v1/reports/section_8_retro_audits_closeout_20260430.md` — §8 retro round 2
> 7. `cursor_prompts/CURSOR_PROMPT_mig184_v2_*.md` + `mig_185_*.md` + `mig_186_*.md` + `mig_187_*.md` — the 4 in-flight Cursor lanes
> 8. `MEMORY.md` (auto-memory index)
>
> Then run the §11 first-action checklist (FULL SCOPE REVIEW first — git fetch + log, 5-gate audit, PM batch progress, in-flight lane status, all 4 in-flight Cursor prompts so you know what to expect when summaries land). Then decide A/B/C from §13.
>
> **Standing context:** I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're closing out the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`) for manuscript-grade survival/recurrence/outcomes analyses. **gate1 is at 172 / PM at 1,596 / 1,620 (table_status=verified) / 62/62 Tier-2 canonicals verified (100%).** Manuscript readiness verdict is READY. You're the orchestrator + verifier + applier; Cursor agents do the bulk lane work; I'm the final ratifier.
>
> **You have:**
> - **Desktop Commander MCP** for git/shell on my actual Mac
> - **MotherDuck MCP** (read + write against `thyroid_canonical_publication_v1_0` and `"Thyroid 2026 UPdated".archive_pub_v1_0`)
> - **GitHub repo** at `/Users/ros/THyroid 2026` (tip `16a9833`; URL `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`)
> - **Auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/` with ~150+ entries
>
> **CURRENTLY IN FLIGHT (Cursor working on these in parallel — verify each summary as it lands):**
> 1. **mig_184_v2** R1 AJCC derivation (Logan-RATIFIED 8 rules; closes CF-87-AJCC 36 cols)
> 2. **mig_185** path-malignant duplicate dedupe scoping (533 dups; I ratify R-A/R-B/R-C before apply)
> 3. **mig_186** NIFTP + uncertain exclusion scoping (213+7 events / 195 pts; I ratify R-A/R-B/R-C/R-D before apply; recommend R-D)
> 4. **mig_187** canonical_us_exam_master rebuild scoping (159 fallback IDs; I ratify R-A/R-B/R-C before apply)
>
> **AWAITING MY RATIFICATION:**
> - **mig_185** dedupe rule (R-A/R-B/R-C) — pending Cursor scoping
> - **mig_186** NIFTP disposition (R-A/R-B/R-C/R-D — recommend R-D)
> - **mig_187** rebuild approach (R-A/R-B/R-C)
> - **mig_184_v2** edge-case rules (recommended 8 already locked in prompt; verify Cursor honored them)
> - **r1c size CSV** — 72 patients / 85 events at `exports/mig184_r1_adjudication_20260430/r1c_*.csv`; I'll fill `logan_size_cm` + `logan_t_stage` columns
>
> **PENDING COWORK-DIRECT (when I say go):**
> - **mig_160b** — close 25 remaining PM date cols (gate5 → 0); mirror mig_160 pattern; I green-lit this class of work
>
> **CRITICAL RIGOR REMINDER:** verify all Cursor work directly against MotherDuck. EVERY round has shipped with agent QA misses Cowork had to clean up — including this latest round's mig_160 dependent-VIEW breakage and prior rounds' governance violations. Be skeptical of every "verified clean" Cursor claim. Run cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions. Pre-check `information_schema.tables` for every methodology string. Audit `data_type` for every numeric measurement col. Check date-type for every `*_date` col. **Pre-flight scan dependent VIEWs before any ALTER COLUMN.** Verify governance compliance before applying any Cursor SQL (probe live MD for the agent's batch_id; should be 0 rows pre-apply). Pre-snapshot every mutating lane to `archive_pub_v1_0`.
>
> **First task:** §11 first-action checklist (FULL SCOPE REVIEW — don't skip). Then choose A/B/C from §13:
> - **(A)** Cursor lane summary arrived → verify per §8 + apply per Path C (after my ratification if rule-picking needed)
> - **(B)** I ratified mig_185/186/187 rule → author + apply final SQL per Path C
> - **(C)** Nothing new in flight → either author next-round Cursor prompts, apply mig_160b (Cowork-direct, low-risk, my green-light is on file), or consolidate memory + status
>
> I'll paste agent summaries from in-flight Cursor runs separately as they come in — verify each against live MD per Path C and apply if AGENTS-governance was respected.

---

End of handoff doc.
