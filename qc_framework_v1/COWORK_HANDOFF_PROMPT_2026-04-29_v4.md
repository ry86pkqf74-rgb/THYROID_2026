# Cowork Handoff Prompt v4 — Thyroid Canonical Publication v1.0 Cleanup

**Generated:** 2026-04-29 (late-evening) — supersedes v1/v2/v3
**Tip of `origin/main`:** `6c5ffb1` at handoff time — `git fetch && git pull` before anything

---

## 0. TL;DR / First Actions

You are continuing a multi-week cleanup of the **Thyroid Canonical Publication v1.0** lakehouse on MotherDuck so it can support a manuscript pipeline. ~68% of the patient_master cluster is verified. There are **5 Cursor lanes in flight** when this doc was written (1 unblocked + 4 next-batch). Read sections 1–6 to orient, then 7–8 for current state, then run §10 first-action checklist, then choose between **(A) Verify lanes as they land + fix issues** (most likely path) or **(B) Generate the next batch of prompts** (if the in-flight ones complete cleanly and Logan needs more).

**Hard rules (Logan-ratified, do not violate):**
- **Always check MotherDuck directly** before recommending — never trust prior summaries (`feedback_motherduck_direct_check.md`).
- **Verify Cursor work yourself** by querying MD. Cursor agents have produced shortcuts (mig_135 21 degenerate-FALSE, mig_138 447-pt undercount, mig_141 2 near-uniform-TRUE missed, mig_144 VARCHAR units, mig_145 tracheal not_mentioned overreach, mig_147 nucmed-vs-RAI 83% drift). Don't accept "verified" claims at face value.
- **Cohort-uniformity sweep on every BOOLEAN col** flipped in any lane. Watch BOTH near-uniform-FALSE AND near-uniform-TRUE — both are degenerate.
- **Clinical event date columns must be DATE, not TIMESTAMP/VARCHAR** (`feedback_clinical_dates_calendar_only.md`). Audit/provenance timestamps (build_ts, extracted_at, etc.) exempt.
- **2-digit year → 20YY** (`reference_2digit_year_convention.md`, Logan-ratified 2026-04-27): all YY → 20YY (00=2000, 25=2025).
- **PHI safety** — never print clinical notes; research_id only; no cloud PHI (`feedback_phi_safety.md`).
- **Surgical git add** — never `git add -A`. Stage by explicit path (`feedback_surgical_git_add.md`).
- **Ask before query_rw / write** — describe SQL changes first; wait for explicit go. (Logan has been pre-authorizing "fix issues" in this round; still describe what you're about to do.)
- **Migration files often get refined post-application by Logan** to reflect idempotent state. Don't be confused if a migration that you applied as historical UPDATEs gets edited to read as a "live state already clean, no rows qualify" idempotent file. Don't revert.

---

## 1. Project Mission

Logan Glosser is a thyroid-cancer surgery researcher at Emory. The lakehouse is a multi-domain canonical clinical research database (`thyroid_canonical_publication_v1_0`) on MotherDuck that backs the v1.0 publication. **Goal: produce a clean, documented, audit-passing canonical layer with verified columns/tables, ready for the manuscript pipeline (survival analysis, recurrence outcomes, etc.).**

The 7 cleanup sub-goals:
1. Every analytic column in `main.canonical_*` is registered in `canonical_column_verification_registry_v1` and signed off in `canonical_table_signoff_registry_v1`.
2. Each column has a real verification method (derivation re-derivation, extraction faithfulness, cross-validation, etc.) tied to a `batch_id` (`mig_<N>_*`).
3. Standardized values on every analytic column (controlled vocabularies; SSOT enums).
4. Old / archived tables and columns removed.
5. Patient-level rollups + view layer aligned with verified events tables.
6. Lakehouse passes the 5-gate cleanliness audit (§11).
7. CFs (carry-forwards) resolved (date-retype batch, lineage fixes, etc.).

---

## 2. Tools & Access

### 2.1 Cowork environment
You're running in Cowork mode (Claude desktop app). Your scratchpad is a temp dir; **the user's workspace folder is `/Users/ros/THyroid 2026`** — that's the GitHub repo on disk. Final outputs go there.

### 2.2 Desktop Commander (preferred for git push)
`mcp__Desktop_Commander__*` tools control the user's actual Mac via a shell process. **Use this to `git push`** because the Cowork sandbox doesn't have GitHub credentials. Pattern:
```
1. start_process({command: "zsh", timeout_ms: 5000})  → returns PID
2. interact_with_process({pid, input: "cd '/Users/ros/THyroid 2026' && git push origin main"})
```
Per `feedback_use_desktop_commander_first.md`: Desktop Commander > Claude in Chrome > computer-use.

### 2.3 GitHub access
- Logan's repo is at `/Users/ros/THyroid 2026` (mounted as Cowork workspace folder). Origin = `origin/main`. Commit author: `Logan Glosser <logan.glosser@gmail.com>`.
- Workflow: `git add <explicit-path>` only; `git commit -c user.name=... -c user.email=...`; **push via Desktop Commander** (sandbox has no creds).
- Surgical git add ONLY: `git add scripts/output/*` and `git add -A` are FORBIDDEN.
- Lint Python before commit if any .py changed: `python3 -m py_compile <file>` + `pyflakes` (`feedback_commit_workflow.md`).

### 2.4 MotherDuck access
- Tools: `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query` (read-only) and `_query_rw` (writes).
- Primary DB: **`thyroid_canonical_publication_v1_0`** (live publication)
- Archive DB: **`"Thyroid 2026 UPdated"`** schema `archive_pub_v1_0` (snapshots before structural mutations — pre-snapshot ANY mutating UPDATE/ALTER here BEFORE applying)
- Workspace DB: `manuscript_workspace` (helper tables, candidate rollups, frozen analytic cohorts)
- Auth: on `logan.glosser.eras@gmail.com` MD account (`reference_protocol_v2_md_accounts.md`)
- DuckDB quirks:
  - `CURRENT_TIMESTAMP` returns TIMESTAMPTZ — always `CAST(... AS TIMESTAMP)` for build_ts cols
  - FILTER not supported on window funcs (use `SUM(CASE) OVER`)
  - Cross-DB FROM in canonicals is forbidden — `main.*` only (`feedback_no_cross_db_canonical_sourcing.md`)
  - `ALTER COLUMN ... SET DATA TYPE T USING <expr>` works for in-place retypes; pre-snapshot first

### 2.5 Cursor agents (parallel work on Logan's other machine)
- Logan runs Cursor agents to do bulk verification work in parallel (one prompt per "lane"). Cursor has its own MD + GitHub access via local CLI.
- Cursor agents commit + push themselves with Logan's authorship.
- Logan forwards agent summaries to you for verification. **Always verify against live MD; agent summaries are claims, not facts.**

### 2.6 Auto-memory
At `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`. Persists across sessions. Index in `MEMORY.md`. **Read `MEMORY.md` first.** It contains ~90 entries — feedback rules, project closeouts, references.

---

## 3. Reference Documents

### 3.1 In repo (`/Users/ros/THyroid 2026`)
| Path | What it is |
|---|---|
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v4.md` | This doc |
| `qc_framework_v1/REMAINING_WORK_INVENTORY.md` | Inventory of all canonicals + verification status |
| `qc_framework_v1/VERIFIED_TABLES.md` | Verified-tables log |
| `qc_framework_v1/AGENTS.md` | Continual-learning policy: survival CF caveats, calendar policy |
| `qc_framework_v1/migrations/*.sql` | All migration sign-off SQL files (~155 total). Number range relevant for this round: 100–153. |
| `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` | Latest 5-gate audit query |
| `qc_framework_v1/migrations/clinical_date_retype_20260428.md` | Calendar-DATE retype anchor (scripts/413) |
| `cursor_prompts/CURSOR_PROMPT_*.md` | All Cursor agent prompts (~70 files) |
| `scripts/203b_canonical_recurrence_harmonized_20260429.py` | Lane 19 harmonized recurrence rebuild script |

### 3.2 Auto-memory key files (read first)

Methodology / pattern memories (high relevance):
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

Project-state memories (this round):
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

Reference memories:
- `reference_2digit_year_convention.md` — 20YY rule
- `reference_protocol_v2_md_accounts.md` — MD accounts
- `reference_synoptic_row_ix.md` — synoptic_row_ix is Script 108 pandas-load-order
- `reference_view_naming_convention.md` — `_VIEW` suffix required
- `reference_canonical_naming_convention.md` — Tier-2 masters naming

---

## 4. Database Architecture

### 4.1 Tier structure
- **Tier 1 — `note_entities_llm_*`**: raw LLM extraction outputs (per-note, per-domain). Source of truth for unstructured signals. Registry-seeded as `na` raw-mirror exempt.
- **Tier 2 — `canonical_*_events_v1`**: event-grain typed tables built from Tier 1 + structured upstream. Most cleanup work happens here.
- **Tier 2 rollup — `canonical_*_patient_rollup_v1`**: patient-grain rollups from events.
- **Tier 3 — `canonical_patient_master`**: THE master patient-grain table. **1,598 columns. Currently 1,092/1,598 verified (68%).** This is the analytic SSOT.
- **Manuscript workspace** — `manuscript_workspace.*`: analytic helpers, candidate rollups, frozen cohort views.

### 4.2 Verification registries
- `canonical_table_signoff_registry_v1` — 1 row per canonical_*. Status = `not_started | in_progress | verified | failed`. Cols: `n_verified`, `n_columns_total`, `n_na`, `n_not_started`, `n_failed`, `signoff_migration`, `table_status`.
- `canonical_column_verification_registry_v1` — 1 row per canonical column. `verification_status`, `verified_by`, `verified_ts`, `verification_method`, `batch_id`, `notes`. The `notes` field accumulates per-migration appendices (separated by ` | mig_<N>: ...`).

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
- `helper_script_<N>_default_placeholder_pending_real_extraction` — when col is a stub
- `partial_dose_signal_supplanted_by_<source>_authoritative` — when col is partial vs authoritative SSOT
- Hybrid combinations OK; spell out the pipeline.

---

## 5. Workflow: Cowork ↔ Cursor ↔ Logan

### 5.1 Roles
- **Logan** — final ratifier; clinical-domain expert. Reviews proposed fixes, ratifies CFs, decides scope. He's a researcher not an engineer — explanations should match a senior researcher's perspective.
- **Cursor agents (the "agents")** — bulk lane workers. Run on Logan's other machine. Take a Cursor prompt (one lane = one cluster), do MD writes, commit, push, and report back. Have made shortcuts that need verification.
- **Cowork (you)** — orchestrator + verifier + small-fix applier. Generate Cursor prompts, fact-check landed work, propose fixes, apply small registry-only or focused-data-write fixes via query_rw, manage open CFs, write close-out memories, run the 5-gate audit, surface findings to Logan.

### 5.2 Default rhythm
1. Logan describes what just landed (forwards Cursor agent summary).
2. You verify against live MD — col counts, batch_id, methodology distribution, cohort parity, CF tags, sample derivations, **cohort-uniformity sweep on every BOOLEAN flipped**.
3. Surface drift / shortcuts / missing CFs to Logan.
4. If issue is small (registry-only or focused data write with clear rule): apply the fix yourself via query_rw with pre-snapshot. Logan has been pre-authorizing "fix any issues" in this round.
5. If issue is big (clinical adjudication required, multi-cluster impact): describe and ask Logan.
6. Write the migration SQL file in `qc_framework_v1/migrations/<N>b_*` for traceability of any directly-applied fix. Commit + push via Desktop Commander.
7. Update column-registry notes to record what was done + close any CF tags.
8. Generate next Cursor prompts when current batch lands.

### 5.3 Verification rigor (don't skip)
For any landed Cursor lane, check at minimum:
- **Col count flipped** — does it match the agent's claim?
- **batch_id consistent** — single `mig_<N>_*` string across all flipped cols.
- **Methodology distribution** — distinct verification_method values; do any look like `_misc`, `_passthrough`, `_residual`, or other "I don't know" placeholders?
- **Cohort parity** — does PM still have 10,871 patients?
- **Cohort-uniformity sanity** — sample BOOLEAN cols, run `SUM(CASE WHEN col THEN 1 ELSE 0 END)` plus FALSE and NULL counts.
  - **Flag near-uniform-FALSE (<1% TRUE) AND near-uniform-TRUE (>99% TRUE)**. Both are degenerate.
  - Type A real cohort invariance → keep verified, tag `CF-mig<N>-COHORT-INVARIANT-<col>` informational
  - Type B upstream not extracted → flip to `na`, open `CF-mig<N>-EXPAND-UPSTREAM-<col>`
  - Type C helper-script artifact → flip to `na`, document the script SSOT
- **CF tag accuracy** — do the `notes` columns carry the expected CF strings?
- **Cross-source spot-check** — pick 5 random rids and trace one verified col back to its upstream by hand.
- **Date type check** — any `*_date` cols TIMESTAMP or VARCHAR? CF-DATE-RETYPE if so.
- **Data-type sanity** — any numeric measurements stored as VARCHAR with embedded units (e.g. `"3.6 mm"`)? Retype to DOUBLE.
- **Cross-SSOT drift** — for cols with multiple authoritative upstreams (e.g. cumulative dose from RAI canonical AND nucmed scan reports), compute pairwise drift; if >10%, flag/reclassify.

### 5.4 When to ask Logan vs apply directly
- **Apply directly via query_rw**: registry-only updates, single-col retype with full preservation probe, focused data-write fixes (e.g., re-derive a col from the same upstream with a corrected enum filter).
- **Ask Logan first**: cross-canonical reconciles affecting >50 patients with clinical adjudication needed, structural schema changes (renames, drops), anything touching PHI semantics, novel SSOT precedence calls.
- Logan's been responding "do whatever's right for clean DB" when the fix has a clear rule. When the fix needs clinical judgment, ASK first.

### 5.5 Pre-snapshot rule for any data write
ALWAYS pre-snapshot to `archive_pub_v1_0` before mutating PM data or any verified canonical. Pattern:
```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_<table>_pre_mig<N>_<short>_20260429 AS
SELECT research_id, <affected cols>, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig<N>_snapshot_ts
FROM main.canonical_<table>;
```

---

## 6. Current State (as of 2026-04-29 late-evening)

### 6.1 Top-line metrics

| Metric | Value |
|---|---|
| Verified canonicals (gate1) | **88 / 175** (50%) |
| In-progress | 1 (`canonical_patient_master`: **1,092 / 1,598 cols ≈ 68%**) |
| 5-gate audit | **gate 1=88, 2=0, 3=0, 4=0, 5=21** (gate 5 baseline; will tick down when global date-retype batch closes) |
| Cross-SSOT drift cr↔crr | 0 (post-mig_153) |
| Pre-snapshots in archive | 5 (mig_139, mig_144b, mig_145b, mig_146b/147b, mig_153) |
| Latest commit | `6c5ffb1` mig_153 idempotent-state version |

### 6.2 Patient-master cluster progress (lanes that have landed)

| Lane | mig | Cluster | Cols | Status |
|---|---|---|---|---|
| 22 | 130 | Operative | 233 | landed (verified clean) |
| 23 | 132 | Pathology | 106 | landed (verified clean) |
| 24 | 133 | Lymph node | 138 | landed (1 outlier rid 68 — not yet fixed) |
| 25 | 134 | Labs | 65 | landed (1 degenerate col flagged) |
| 25-new | 135 | Complications | 147 | landed (21 degenerate cols, agent CF) |
| 26 | 136 | PMH+PSH | 80 | verified clean |
| 27 | 137 | Molecular | 66 | verified clean (with date-retype CF on 25 cols) |
| 28 | 138 | Recurrence-Response | 40 | landed (mig_139 resync applied for 9 cols) |
| 29 | 140 | ETE | 36 | verified (35 verified + 1 na) |
| 30 | 141 | Survival | 25 | verified + mig_141b cleanup applied |
| 32 | 143 | Small-clusters bundle | 30 | verified clean |
| 34 | 144 | US imaging | 23 | verified + mig_144b VARCHAR→DOUBLE retype |
| 35 | 145 | CT imaging | 29 | verified + mig_145b tracheal re-derive |
| 36 | 146 | MRI+PET | 49 | verified + mig_146b PET-date retype |
| 37 | 147 | Nucmed | 26 | verified + mig_147b/c date retype + dose reclassify |
| 38 | 148 | RAI upstream (rai_treatment_episode_v2) | 25 cols (20 verified + 5 reclassified to na in mig_148b) | verified — gate 1 ticked 87→88 |
| **PM total** | — | — | **1,092** | in_progress |

### 6.3 Cleanup migrations applied this round (Cowork-direct via query_rw)

| mig | Type | What it fixed |
|---|---|---|
| **139** | PM data write | CR-spine resync — 447 missing recurrences in PM (PM 82→514 TRUE matching SSOT) |
| **141b** | Registry-only | 2 near-uniform-TRUE BOOLEANs (survival_eligible_flag, prm_followup_has_complications) flagged with CF-COHORT-NEAR-UNIFORM-TRUE |
| **144b** | PM data write | 4 US measurement cols VARCHAR ("3.6 mm") → DOUBLE; analysts can compute stats directly |
| **145b** | PM data write | 2 CT tracheal cols re-derived (was counting `not_mentioned` as TRUE → 85% TRUE; now 45/30/25%) |
| **146b** | PM data write | pet_first_date / pet_last_date VARCHAR ('MM/DD/YYYY') → DATE |
| **147b** | PM data write | nucmed_first/last_scan_with_labs VARCHAR ('MM/DD/YY' 2-digit year) → DATE w/ 20YY rule |
| **147c** | Registry-only | nucmed_cumulative_therapeutic_dose verified→na (83% drift vs RAI canonical SSOT) |
| **148b** | Registry-only | 5 RAI placeholder cols verified→na (all 0 TRUE / 1857 FALSE script defaults + lab linkage backlog) |
| **153** | RR data write | 46 CRR rows demoted path_proven→imaging_only_unconfirmed (cross-SSOT drift between cr.recurrence_confirmed=FALSE and crr=path_proven). **Note: file was refined post-application by Logan to read as idempotent — drift now 0 in live DB; the migration creates a traceability table that's empty when re-run.** |

### 6.4 Open CFs (carry-forwards)

**Date-retype batch — single future repair migration to close gate-5 violators:**
- CF-100-DATE-RETYPE: `frozen_section_events.frozen_section_date` (1)
- CF-117-DATE-RETYPE: 4 cols on molecular_v2 + ete_event_resolved
- CF-119-FROZEN-ROLLUP-DATE-RETYPE: 14 cols on frozen_section_patient_rollup_v1
- CF-120/path-DATE-RETYPE: 2 cols on path_malignant_patient_rollup_v1
- CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE: 1 col
- CF-mig123-RECURRENCE-DATE-RETYPE: 1 col (downstream PM `recurrence_date` carries this)
- CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE: 2 cols (resurfaces when PM flips to verified)
- CF-mig132-PM-PATH-STAGE-DERIVED-AT-RETYPE: 2 cols
- CF-mig133-PM-CNCLN-DATE-PARSE: 6 cols
- CF-mig137-PM-MOL-DATE-RETYPE: 25 cols on molecular cluster
- CF-mig148-RAI-DATE-RETYPE: 3 TIMESTAMP cols on rai_treatment_episode_v2 (rai_date_native, resolved_rai_date, note_date_parsed)
- CF-118-UPSTREAM-DATE-FORMAT-DRIFT: `note_entities_procedures.note_date` MM/DD/YYYY VARCHAR

**Methodology / data-quality CFs:**
- CF-mig123-UPSTREAM-DATE-202-TYPO (rids 12057 yr-0202, 10622 yr-1950 + 4 NULL siblings)
- CF-mig123-NEGATIVE-TTR-9-PATIENTS (9 rows clipped)
- CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE
- CF-mig126-DATA-TYPE-DRIFT (9 cols globally)
- CF-mig126-ORDINAL-POSITION-DRIFT (119 unmatched pairs)
- **CF-mig133-PM-LN-COUNT-INTEGRITY** (rid 68 outlier — fix still pending)
- CF-58-1, CF-58-2, CF-58-3 (parathyroid LLM-extraction edge cases)
- CF-mig58-STRING-AGG-ORDER (medications rollup ordering)
- **CF-mig135-PM-COMPL-ROLLUP-SEMANTICS** (21 degenerate FALSE cols flagged — Type B reclassification deferred)
- CF-mig141-COHORT-NEAR-UNIFORM-TRUE-survival_eligible_flag (informational)
- CF-mig141-COHORT-NEAR-UNIFORM-TRUE-prm_followup_has_complications (informational)
- CF-mig144-PM-US-DUAL-SPINE (informational; imaging_exam_master_v1 vs canonical_us_thyroid_gland_v2 200-row delta)
- CF-mig146-UPSTREAM-CANONICAL-PENDING / CF-mig146-IMAGING-LLM-NO-PERSISTED-ENTITIES (informational; CT/MRI/PET don't have separate canonicals — derivation happens directly from raw exam tables)
- CF-mig147-NUCMED-VS-RAI-DOSE-SOURCE-SPLIT (mig_147c reclassify CLOSED this; informational record)
- 22 `llm_path_keyword` rids in mig_153 may have legitimate path-proven content not captured by mig_123 CR rebuild — flagged as future investigation if manuscript prep raises priority

---

## 7. Active Cursor Lanes — In Flight When This Doc Was Written

Logan launched these from the prompts in `cursor_prompts/`. They will report to him when done; he'll forward summaries to you for verification.

| Lane | mig | Cluster | Expected cols | Prompt file |
|---|---|---|---|---|
| 31 (re-launch) | 142 | RAI PM cluster | ~51 | `cursor_prompts/CURSOR_PROMPT_patient_master_rai_cluster_20260429.md` (was BLOCKED on rai_treatment_episode_v2; now unblocked since mig_148 verified) |
| 39 | 149 | Synoptic-pathology | ~32 | `cursor_prompts/CURSOR_PROMPT_patient_master_synoptic_pathology_cluster_20260429.md` |
| 40 | 150 | Parathyroid + Postop + TP | ~38 | `cursor_prompts/CURSOR_PROMPT_patient_master_parathyroid_postop_tp_cluster_20260429.md` |
| 41 | 151 | Meds + RadTx + Procedures | ~39 | `cursor_prompts/CURSOR_PROMPT_patient_master_meds_radtx_proc_cluster_20260429.md` |
| 42 | 152 | NLP cluster (all 25 sub-domains) | ~120 (biggest) | `cursor_prompts/CURSOR_PROMPT_patient_master_nlp_cluster_20260429.md` |

**Total expected scope when these land: ~280 cols.** PM should reach **~1,372 / 1,598 (~86%)** after verification.

### 7.1 Verification checklist for each landing lane

When Logan forwards an agent summary, follow this protocol:

```sql
-- (1) Mechanical: col count + methodology distribution
SELECT verification_method, COUNT(*) AS n
FROM main.canonical_column_verification_registry_v1
WHERE batch_id = 'mig_<N>_*'
GROUP BY 1 ORDER BY 2 DESC;

-- (2) PM signoff progress
SELECT n_verified, n_not_started, n_na, n_failed, table_status
FROM main.canonical_table_signoff_registry_v1
WHERE table_name='canonical_patient_master';

-- (3) 5-gate audit (full query in §11)

-- (4) Cohort-uniformity sweep on EVERY BOOLEAN flipped (CRITICAL)
SELECT
  SUM(CASE WHEN <col1> THEN 1 ELSE 0 END) AS c1_T,
  SUM(CASE WHEN NOT <col1> THEN 1 ELSE 0 END) AS c1_F,
  SUM(CASE WHEN <col1> IS NULL THEN 1 ELSE 0 END) AS c1_N,
  ...
FROM main.canonical_patient_master;

-- (5) Date-type check
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (cols flipped)
ORDER BY data_type, column_name;
-- Flag any TIMESTAMP / VARCHAR for *_date cols.

-- (6) CF tag review
SELECT regexp_extract(notes, 'CF-mig<N>-[A-Za-z0-9_-]+', 0) AS cf_tag, COUNT(*)
FROM main.canonical_column_verification_registry_v1
WHERE batch_id='mig_<N>_*' AND notes ILIKE '%CF-mig<N>%'
GROUP BY 1 ORDER BY 2 DESC;

-- (7) Cross-source spot-check on 3-5 random rids per lane
```

### 7.2 Lane-specific gotchas to watch for

- **mig_142 RAI PM**: 51 cols depend on `rai_treatment_episode_v2` (mig_148 just verified). Watch for VARCHAR-typed dose cols, cohort-uniform-FALSE on rare RAI subtypes. Cross-validate `nucmed_cumulative_therapeutic_dose` (now `na` per mig_147c) against PM `rai_total_cumulative_dose_mci`.
- **mig_149 Synoptic**: VARCHAR-typed measurement cols are likely (similar to mig_144 us_volume_ml issue). Pre-emptively check `syn_isthmus_size_cm`, `syn_*_weight_g` data types — VARCHAR with embedded units would need a mig_149b retype.
- **mig_150 Parathyroid+Postop+TP**: cross-validate with complications mig_98f (hypoparathyroidism) and mig_98g (hypocalcemia_clinical). PM `postop_low_pth_flag` should align with complications mortality.
- **mig_151 Meds+RadTx+Procedures**: STRING_AGG ordering CF (CF-mig58-STRING-AGG-ORDER) — use `list_sort` for set-equal probes. RadTx may not have a verified canonical; agent should derive from Tier-1 NLP.
- **mig_152 NLP (biggest)**: 25 sub-domains × 4 cols pattern. Look for sub-domains where `_has_data` = 0 TRUE (Type B upstream not extracted). `nlp_ne` may not fit the 4-col mold. Cross-validate `nlp_rec_*` against canonical_recurrence_v1 (post-mig_139), `nlp_path_*` against canonical_path_malignant_events_v1.

---

## 8. Open Issues / Carry-Forwards Not Yet Closed

These are NOT blockers for the current batch but should be addressed eventually:

### 8.1 rid 68 LN integrity outlier
PM `research_id='68'` has `ln_total_examined=0` and `ln_total_positive=1` (arithmetic violation). 19 sibling pts with same flag have `ln_total_examined=NULL`. Fix: single-row UPDATE setting `ln_total_examined=NULL`. Pre-drafted in v3 doc §8.1. Was reserved as mig_136 but that slot got taken; can be mig_154.

### 8.2 21 degenerate cohort-uniform-FALSE cols on mig_135 complications
Pneumothorax (3), wound_dehiscence (3), wound_infection (12), airway_complication (3). Agent flagged via CF-mig135-PM-COMPL-ROLLUP-SEMANTICS but kept verified. Should be reclassified to `na` per Type B (upstream not extracted). Same pattern as mig_148b applied for RAI.

### 8.3 Other near-uniform candidates to sweep
Run a global sweep across all 1,092 verified PM cols looking for BOOLEANs where TRUE=0 OR FALSE=0 OR (TRUE-rate >99% AND FALSE-rate <1%). Flag any survivors of the per-lane sweeps.

### 8.4 Date-retype batch
~21 cols in gate 5 baseline + new date-retype CFs from this round. A single global retype migration could close gate 5 to 0. Drafting deferred until all landing lanes done (since each new lane may add date CFs).

### 8.5 22 llm_path_keyword cases noted in mig_153
After mig_153 demoted, 22 of the 46 had `recurrence_path_proven_source='llm_path_keyword'` with evidence text. If a manuscript chapter on path-proven recurrence prep raises priority, re-investigate per-rid evidence and consider promoting some back to `cr.recurrence_confirmed=TRUE` in a focused follow-up lane.

---

## 9. Decision Tree After Reading This Doc

After §10 first-action checklist, decide:

### A. If 1+ Cursor lane has landed → verify it (most likely path)
1. Run §7 verification protocol (mechanical + cohort-uniformity + spot-check + CF tag review + date-type check + data-type sanity).
2. Surface findings to Logan.
3. **If issues are small (registry-only or focused PM data write with clear rule), apply via query_rw with pre-snapshot. Logan has been pre-authorizing "fix any issues" in this round.**
4. Write a `mig_<N>b_*.sql` migration file in `qc_framework_v1/migrations/` for traceability.
5. Commit + push via Desktop Commander.
6. Update column-registry notes.
7. Move on to next lane verification.

### B. If all lanes have landed and Logan needs more prompts
After mig_142, mig_149-152 land + verifications, ~225 unverified PM cols remain. Sub-cluster the residual:
- `n_*` (45 cols) — count cols across multiple domains
- `prm_*` (17 cols) — PRM rule cols
- `gm_*` (13) — generic provenance
- `any_*` (13) — overlap with existing verified _any cols (check for double-coverage)
- Misc residual (~150 cols) — needs further investigation

Generate 2-4 new prompts for the next batch.

### C. If something high-priority needs Logan ratification
Stop and ask. Examples:
- Cross-canonical reconciliation requiring clinical adjudication
- Structural schema changes
- A canonical that's been "verified" but you find a fundamental derivation bug

---

## 10. First-Action Checklist (do this before anything else)

```
1. git fetch && git pull
2. git -C "/Users/ros/THyroid 2026" log --oneline -20  # see what's landed since v4 doc was written
3. Run §11 5-gate audit  # confirm gates 2/3/4 still 0; record gate 1, gate 5
4. Check PM verification progress:
     SELECT batch_id, COUNT(*) FROM canonical_column_verification_registry_v1
     WHERE table_name='canonical_patient_master' AND verification_status='verified'
     GROUP BY 1 ORDER BY 2 DESC;
5. Check active lanes status (mig_142, mig_149-152):
     SELECT batch_id, COUNT(*), MAX(verified_ts) FROM canonical_column_verification_registry_v1
     WHERE batch_id LIKE 'mig_14%' OR batch_id LIKE 'mig_15%' GROUP BY 1 ORDER BY 1;
6. Read MEMORY.md end-to-end
7. Read §6.3 (cleanup migrations applied this round) carefully — these are the patterns of issues you'll likely see again
8. Read §7.2 (lane-specific gotchas) for the 5 in-flight lanes
9. Decide A/B/C from §9
```

---

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

Expected at handoff: **gate1=88, gate2=0, gate3=0, gate4=0, gate5=21**. PM is `in_progress` so its date-retype CFs don't count yet — when PM flips to verified, gate 5 will balloon temporarily until the global date-retype migration closes.

---

## 12. Verbatim prompt to paste into the new Cowork chat

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v4.md` end-to-end before any tool use. Then run the §10 first-action checklist (git tip + log, 5-gate audit, PM batch progress, active lane status, read MEMORY.md, re-read §6.3 cleanup patterns + §7.2 lane gotchas).
>
> **Standing context:** I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're cleaning up the `thyroid_canonical_publication_v1_0` lakehouse on MotherDuck so it's ready for manuscript-grade statistical analysis. ~68% done on the patient_master cluster. You're the orchestrator + verifier; Cursor agents do the bulk lane work on my other machine; I'm the final ratifier.
>
> **You have:**
> - Desktop Commander MCP (push to GitHub via my actual Mac — sandbox has no creds)
> - MotherDuck MCP (read + write against `thyroid_canonical_publication_v1_0` and `"Thyroid 2026 UPdated".archive_pub_v1_0`)
> - GitHub repo at `/Users/ros/THyroid 2026` mounted as your workspace folder
> - Auto-memory at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/` with ~90 entries
> - All migration SQL files in `qc_framework_v1/migrations/` (1-153)
> - All Cursor prompts in `cursor_prompts/`
>
> **Currently in flight (Cursor agents working on them — do NOT touch these clusters):**
> 1. mig_142 RAI PM cluster (~51 cols) — original prompt re-launched; was blocked on rai_treatment_episode_v2 (mig_148 cleared the block)
> 2. mig_149 Synoptic-pathology (~32 cols)
> 3. mig_150 Parathyroid + Postop + TP (~38 cols)
> 4. mig_151 Meds + RadTx + Procedures (~39 cols)
> 5. mig_152 NLP cluster (~120 cols, biggest)
>
> **First task:** start with the §10 first-action checklist. Then check git for any new commits from these lanes; if any landed, verify them per §7 protocol (mechanical → §7.2 cohort-uniformity sweep on every BOOLEAN → date-type check → data-type sanity → CF tag review → cross-source spot-check on 5 random rids). Apply small fixes directly via query_rw with pre-snapshot — I've been pre-authorizing "fix any issues" so don't bottleneck on me. Larger structural decisions or clinical adjudications, ask first.
>
> **Critical rigor reminder:** verify all Cursor work directly against MotherDuck. The lessons from this round:
> - mig_135 shipped with 21 degenerate-FALSE cols (agent CF-tagged but kept verified — needs Type-B reclassify to `na`)
> - mig_138 shipped with 447-pt undercount on recurrence_confirmed (PM was 82, SSOT was 514) — required mig_139 resync
> - mig_141 shipped with 2 near-uniform-TRUE BOOLEANs missed by agent — required mig_141b CF appendix
> - mig_144 shipped with 4 VARCHAR measurement cols ("3.6 mm" instead of DOUBLE 3.6) — required mig_144b retype
> - mig_145 shipped with CT tracheal cols counting `not_mentioned` as TRUE (85% TRUE rate, clinically implausible) — required mig_145b re-derive
> - mig_146/147 shipped with 4 VARCHAR date cols — required mig_146b/147b retype
> - mig_147 shipped with nucmed_cumulative_therapeutic_dose 83% drift vs RAI canonical — required mig_147c reclassify
> - 22 llm_path_keyword and 24 structural_confirmed cases drifted between cr.recurrence_confirmed=FALSE and crr.recurrence_status_final='path_proven' — required mig_153 demote
>
> Be skeptical of every "verified clean" agent claim. Run the cohort-uniformity sweep on EVERY BOOLEAN. Run the date-type check on EVERY date col. Compare numeric-measurement cols against their upstream data types. Cross-validate any col with multiple authoritative upstreams.
>
> **Standing reminders from auto-memory you must follow:**
> - feedback_motherduck_direct_check.md (always check MD)
> - feedback_clinical_dates_calendar_only.md (clinical event dates MUST be DATE)
> - reference_2digit_year_convention.md (YY → 20YY)
> - feedback_extraction_faithfulness_llm_canonical.md
> - feedback_findings_vs_staging.md
> - feedback_recurrence_imaging_n_events_null.md
> - feedback_etevent_resolved_cross_check.md
> - feedback_no_cross_db_canonical_sourcing.md
> - feedback_surgical_git_add.md
> - feedback_use_desktop_commander_first.md
>
> The handoff doc is ~570 lines and self-contained. You should be productive within ~5 minutes of reading it.

End of handoff doc.
