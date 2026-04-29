# Cowork Handoff Prompt — Thyroid Canonical Publication v1.0 Cleanup

**Generated:** 2026-04-29 (mid-session) — supersedes v1/v2
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before anything

---

## 0. TL;DR / First Actions

You are continuing a multi-week effort to clean up the **Thyroid Canonical Publication v1.0** lakehouse on MotherDuck so it can support the manuscript pipeline. ~50% of work is done. There are **3 active Cursor lanes in flight** and **4 specific open issues** that need fixing now. Read sections 1–6 to orient, then sections 7–8 for current state, then decide between **(A) Fix open issues** (section 8) or **(B) Generate next batch of Cursor prompts** (section 9). DO NOT touch anything until you've read this doc, run the §11 standing audit, and reviewed the live state in MotherDuck.

**Hard rules (Logan-ratified, do not violate):**
- **Always check MotherDuck directly** before recommending — never trust prior summaries (`feedback_motherduck_direct_check.md`).
- **Verify Cursor work yourself** by querying MD — Cursor agents have produced shortcuts that mass-pass-through cohort-uniform values without flagging the degeneracy. Don't accept "verified" claims at face value.
- **Clinical event date columns must be DATE, not TIMESTAMP** (`feedback_clinical_dates_calendar_only.md`). Audit/provenance timestamps (build_ts, extracted_at, etc.) exempt.
- **PHI safety** — never print clinical notes; research_id only; no cloud PHI (`feedback_phi_safety.md`).
- **Surgical git add** — never `git add -A`. Stage by explicit path (`feedback_surgical_git_add.md`).
- **Ask before query_rw / write** — describe SQL changes first; wait for explicit go.

---

## 1. Project Mission

Logan is a thyroid-cancer surgery researcher at Emory. The lakehouse is a multi-domain canonical clinical research database (`thyroid_canonical_publication_v1_0`) on MotherDuck that backs the v1.0 publication. Goal: produce a clean, documented, audit-passing canonical layer with verified columns/tables, ready for the manuscript pipeline.

The cleanup work has 7 sub-goals:
1. Every analytic column in `main.canonical_*` is registered in `canonical_column_verification_registry_v1` and signed off in `canonical_table_signoff_registry_v1`.
2. Each column has a real verification method (derivation re-derivation, extraction faithfulness, cross-validation, etc.) tied to a `batch_id` (`mig_<N>_*`).
3. Standardized values on every analytic column (controlled vocabularies; SSOT enums).
4. Old / archived tables and columns removed.
5. Patient-level rollups + view layer aligned with verified events tables.
6. Lakehouse passes the 5-gate cleanliness audit (§11).
7. CFs (carry-forwards) resolved (date-retype batch, lineage fixes, etc.).

Timezone: 2-digit YY → 20YY (Logan-ratified 2026-04-27, `reference_2digit_year_convention.md`).

---

## 2. Tools & Access

### 2.1 Cowork environment
- **You are running in Cowork mode** (Claude desktop app). Working dir is a temp scratchpad; **the user's selected folder is `/Users/ros/THyroid 2026`** — that's the GitHub repo on disk. All final outputs go there or to outputs scratchpad.
- **Desktop Commander MCP** — full filesystem access on the user's Mac. Use `mcp__Desktop_Commander__*` tools (load via ToolSearch). Preferred over Claude-in-Chrome and computer-use per `feedback_use_desktop_commander_first.md`.
- **MotherDuck MCP** — direct query/query_rw against `thyroid_canonical_publication_v1_0` and `"Thyroid 2026 UPdated".archive_pub_v1_0`. Auth is on `logan.glosser.eras@gmail.com` MD account (`reference_protocol_v2_md_accounts.md`).
- **Bash sandbox** for git operations and Python lint. Path mapping: `/Users/ros/THyroid 2026` ↔ `/sessions/<id>/mnt/THyroid 2026/`.
- **Auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/` — persists across sessions. Index in `MEMORY.md`. Read MEMORY.md first.

### 2.2 GitHub access
- Logan's repo is at `/Users/ros/THyroid 2026` (mounted as Cowork workspace folder). Origin = `origin/main`. Commit author: `Logan Glosser <logan.glosser@gmail.com>`.
- Surgical git workflow: `git add <explicit-path>` only; `git commit`; `git push origin main`. NEVER `git add -A` or `git add scripts/output/`.
- Lint Python before commit: `python3 -m py_compile <file>` and `pyflakes` (`feedback_commit_workflow.md`).

### 2.3 MotherDuck access
- Primary DB: `thyroid_canonical_publication_v1_0` (live publication)
- Archive DB: `"Thyroid 2026 UPdated"` schema `archive_pub_v1_0` (snapshots prior to migration changes)
- Workspace DB: `manuscript_workspace` (helper tables, candidate rollups, frozen analytic cohorts)
- DuckDB quirks: `CURRENT_TIMESTAMP` returns TIMESTAMPTZ — always `CAST(... AS TIMESTAMP)` for build_ts. FILTER not supported on window funcs (use `SUM(CASE) OVER`). Cross-DB FROM in canonicals is forbidden — `main.*` only (`feedback_no_cross_db_canonical_sourcing.md`).

### 2.4 Cursor (parallel agent)
- Logan runs Cursor agents on a separate machine to do bulk verification work in parallel (one prompt per "lane"). Cursor has its own MD + GitHub access via local CLI.
- Cursor agents commit + push themselves with Logan's authorship.
- Logan forwards agent summaries to you for verification. **Always verify against live MD; agent summaries are claims, not facts.**

---

## 3. Reference Documents

### 3.1 In repo (`/Users/ros/THyroid 2026`)
| Path | What it is |
|---|---|
| `qc_framework_v1/REMAINING_WORK_INVENTORY.md` | Inventory of all ~175 canonicals + verification status |
| `qc_framework_v1/VERIFIED_TABLES.md` | Verified-tables log |
| `qc_framework_v1/AGENTS.md` | Continual-learning policy: survival CF caveats, calendar policy |
| `qc_framework_v1/migrations/*.sql` | All migration sign-off SQL files (~150 total). Numbering: 100–134 are recent close-outs; 131 is canonical_recurrence_v1 rebuild; 132 PM pathology; 133 PM LN; 134 PM labs; 135 PM complications. |
| `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` | Latest 5-gate audit query (§11 below) |
| `qc_framework_v1/migrations/clinical_date_retype_20260428.md` | Calendar-DATE retype anchor (scripts/413) |
| `cursor_prompts/CURSOR_PROMPT_*.md` | All Cursor agent prompts (~60 files) |
| `scripts/203b_canonical_recurrence_harmonized_20260429.py` | Lane 19 harmonized rebuild script (final) |
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29.md` | Prior handoff doc (v1; this doc supersedes) |

### 3.2 Auto-memory (`memory/MEMORY.md`)
~90 entries indexed. Read MEMORY.md, then drill into specific files as needed. Highest-relevance entries for this session:

**Methodology / pattern memories:**
- `feedback_motherduck_direct_check.md` — verify against live MD every round
- `feedback_audit_regex_word_boundary.md` — gate-5 audit needs word boundaries
- `feedback_etevent_resolved_cross_check.md` — event-grain INNER JOIN with CAST(rid AS VARCHAR)
- `feedback_recurrence_imaging_n_events_null.md` — NULL not 0 for absent-event case; COALESCE before IS DISTINCT FROM
- `feedback_clinical_dates_calendar_only.md` — clinical event dates MUST be DATE
- `feedback_no_crossdomain_linkage_ids.md` — query-time JOIN, not linked_X_episode_id bloat
- `feedback_alter_view_dependents.md` — CREATE OR REPLACE dependents in same commit
- `feedback_surgical_git_add.md` — explicit path/glob; never -A
- `feedback_use_desktop_commander_first.md` — Desktop Commander > Chrome > computer-use
- `feedback_extraction_faithfulness_llm_canonical.md` — re-derive from upstream WHERE error=0
- `feedback_no_cross_db_canonical_sourcing.md` — canonicals are `main.*` standalone
- `feedback_findings_vs_staging.md` — anatomic findings primary; staging follows findings

**Project state:**
- `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md` — Lane 19 RESUME closeout (87th canonical)
- `project_patient_master_pathology_cluster_mig_132_closeout.md` — Lane 22 (+106 cols)
- `project_meta_registries_mig_126_closeout.md` — registry hygiene; 9 data_type drift CFs
- `project_complications_events_verified_2026-04-28.md` — 8 complication categories in upstream
- `project_psh_events_mig_104_closeout.md` — Script 365 deterministic rebuild SSOT
- `project_recurrence_resolved_v1_mig_125_closeout.md` — SSOT enum `imaging_only_unconfirmed`
- `project_invasion_family_signoff_2026-04-28.md` — invasion family complete

**Methodology library:**
- `project_op_path_consolidation_script_361_closeout.md` — 7 reusable patterns
- `project_op_procedure_codes_mig_118_closeout.md` — hybrid pattern #9
- `project_path_gland_family_complete_2026-04-28.md` — rebuild-then-verify pattern

### 3.3 Standing reference query (every session)
Before recommending anything, run the §11 5-gate audit and check git tip.

---

## 4. Database Architecture

### 4.1 Tier structure
- **Tier 1 — note_entities_llm_*** : raw LLM extraction outputs (per-note, per-domain). Source of truth for unstructured signals.
- **Tier 2 — canonical_*_events_v1** : event-grain typed tables built from Tier 1 + structured upstream. Most cleanup work happens here.
- **Tier 2 rollup — canonical_*_patient_rollup_v1** : patient-grain rollups from events.
- **Tier 3 — canonical_patient_master** : THE master patient-grain table. ~1,598 columns. Currently 689/1598 verified (~43%). This is the analytic SSOT.
- **Manuscript workspace** — `manuscript_workspace.*` : analytic helpers, candidate rollups, frozen cohort views.

### 4.2 Verification registries
- `canonical_table_signoff_registry_v1` — 1 row per canonical_*. Status = `not_started | in_progress | verified | failed`. Cols: `n_verified`, `n_columns_total`, `n_na`, `n_not_started`, `n_failed`, `signoff_migration`, `table_status`.
- `canonical_column_verification_registry_v1` — 1 row per canonical column. `verification_status`, `verified_by`, `verified_ts`, `verification_method`, `batch_id`, `notes`. The `notes` field accumulates per-migration appendices.

### 4.3 Naming conventions
- Tier 2 masters: `canonical_<domain>_events_v1` and `canonical_<domain>_patient_rollup_v1`
- VIEWs: `canonical_<domain>_<grain>_VIEW_v<N>` — must carry `_VIEW` suffix (`reference_view_naming_convention.md`)
- QA: `qa_*`
- No new `tier2.*` / `verify.*` schemas — being dropped

### 4.4 Verification methods (controlled vocab — see existing rows for examples)
- `derivation_vs_canonical_<source>_<col>` — re-derive from upstream
- `extraction_faithfulness_llm_canonical` — re-derive from note_entities_llm_* WHERE error=0
- `internal_consistency` — pairwise col rule (e.g., positive ≤ examined)
- `auto_identifier_skip` — research_id, primary keys
- `auto_provenance_skip` — build_ts, extracted_at (NA tier)
- Hybrid combinations OK; spell out the pipeline.

---

## 5. Workflow: Cowork ↔ Cursor ↔ Logan

### 5.1 Roles
- **Logan** — final ratifier; clinical-domain expert. Reviews proposed fixes, ratifies CFs, decides scope. He's a researcher not an engineer — explanations should match a senior researcher's perspective.
- **Cursor agents (the "agents")** — bulk lane workers. Run on Logan's other machine. Take a Cursor prompt (one lane = one cluster), do MD writes, commit, push, and report back. Have made shortcuts that need verification.
- **Cowork (you)** — orchestrator + verifier. Generate Cursor prompts, fact-check landed work, propose fixes, manage open CFs, write close-out memories, run the 5-gate audit, surface findings to Logan.

### 5.2 Default rhythm
1. Logan describes what just landed (forwards Cursor agent summary).
2. You verify against live MD — col counts, batch_id, methodology distribution, cohort parity, CF tags, sample derivations, cohort-uniformity sanity (TRUE-count not 0 for boolean cols where signal should exist).
3. Surface drift / shortcuts / missing CFs to Logan.
4. Logan ratifies: fix now / open CF / accept.
5. Apply fix (if MD write, ask for explicit go before query_rw).
6. Write close-out memory entry; update MEMORY.md index.
7. Generate next Cursor prompts when current batch lands (Logan launches them in parallel).

### 5.3 Verification rigor (don't skip)
For any landed Cursor lane, check at minimum:
- **Col count flipped** — does it match the agent's claim?
- **batch_id consistent** — single `mig_<N>_*` string across all flipped cols.
- **Methodology distribution** — distinct verification_method values; do any look like `_misc`, `_passthrough`, `_residual`, or other "I don't know" placeholders?
- **Cohort parity** — does the table still have 10,871 patients?
- **Cohort-uniformity sanity** — sample BOOLEAN cols, run `SUM(CASE WHEN col THEN 1 ELSE 0 END)`. **0 TRUE on a complication or LN-mets-rare-histology col is degenerate** unless the cohort genuinely has zero of that thing — and you should still tag a CF noting the degeneracy.
- **CF tag accuracy** — do the `notes` columns carry the expected CF strings?
- **Cross-source spot-check** — pick 5 random rids and trace one verified col back to its upstream by hand.

### 5.4 When to ask Logan
- Before any `query_rw` MD write — describe SQL.
- Before opening a high-impact CF that re-classifies many cols.
- Before committing structural changes (renames, drops, schema changes).
- When agent summary disagrees with live MD by >5%.

---

## 6. Current State (as of 2026-04-29 mid-session)

### 6.1 Top-line metrics

| Metric | Value |
|---|---|
| Verified canonicals | **87 / 175** (49.7%) |
| In-progress | 1 (`canonical_patient_master`: 689 / 1,598 cols ≈ 43%) |
| Total cols verified | ~3,800 / ~5,500 |
| 5-gate audit | **gate 1=87, 2=0, 3=0, 4=0, 5=21** (all 21 are documented or near-documented CFs; baseline is 20) |
| Recent recent commits | Lane 19 mig_123 (`c75ca65`), Lane 22 mig_132 (`4a1477c`), Lane 24 mig_133 (`a3d8353`), Lane 25 labs mig_134 (`1d39860`), Lane 25-new complications mig_135 (Logan hasn't forwarded summary but it's live) |

### 6.2 Patient-master cluster progress

| Lane | Cluster | mig | Cols | Status |
|---|---|---|---|---|
| Lane 22 | Operative (mig_130) | 130 | 233 | landed (verified clean) |
| Lane 22 | Pathology | 132 | 106 | landed (verified clean) |
| Lane 24 | Lymph node | 133 | 138 | landed (1 outlier rid 68 to fix) |
| Lane 25 | Labs | 134 | 65 | landed (1 degenerate col) |
| Lane 25-new | Complications | 135 | 147 | landed (21 degenerate cols) |
| **PM verified total** | — | — | **689** | in_progress |

### 6.3 Active Cursor lanes (in flight; do NOT touch these clusters)

Logan launched these from the 4 prompts the prior Cowork chat generated. mig_135 (complications) was Lane 25 in that batch and already landed. The other 3 are still running:

| Lane | Cluster | Expected mig | Prompt file |
|---|---|---|---|
| Lane 26 | PMH + PSH (~80 cols) | mig_137 (renumber from 135) | `cursor_prompts/CURSOR_PROMPT_patient_master_pmh_psh_cluster_20260429.md` |
| Lane 27 | Molecular (~57 cols) | mig_138 (renumber from 136) | `cursor_prompts/CURSOR_PROMPT_patient_master_molecular_cluster_20260429.md` |
| Lane 28 | Recurrence-Response (~50 cols) | mig_139 (renumber from 137) | `cursor_prompts/CURSOR_PROMPT_patient_master_recurrence_response_cluster_20260429.md` |

**These 3 prompts have stale mig# suggestions (135/136/137).** mig_135 is already taken by complications and mig_136 will be the rid 68 cleanup. When verifying agent summaries, check the actual batch_id used; if collision occurred, document and proceed.

### 6.4 Open CFs (carry-forwards)

**Date-retype batch — single future repair migration to close all 21 gate-5 violators:**
- CF-100-DATE-RETYPE: `frozen_section_events.frozen_section_date` (1)
- CF-117-DATE-RETYPE: 4 cols on molecular_v2 + ete_event_resolved
- CF-119-FROZEN-ROLLUP-DATE-RETYPE: 14 cols on frozen_section_patient_rollup_v1
- CF-120/path-DATE-RETYPE: 2 cols on path_malignant_patient_rollup_v1
- CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE: 1 col
- CF-mig123-RECURRENCE-DATE-RETYPE: 1 col (NEW from Lane 19 review)
- CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE: 2 cols (resurfaces when PM flips to verified)
- CF-mig132-PM-PATH-STAGE-DERIVED-AT-RETYPE: 2 cols
- CF-mig133-PM-CNCLN-DATE-PARSE: 6 cols
- CF-118-UPSTREAM-DATE-FORMAT-DRIFT: `note_entities_procedures.note_date` MM/DD/YYYY VARCHAR

**Methodology / data-quality CFs:**
- CF-mig123-UPSTREAM-DATE-202-TYPO (rids 12057 yr-0202, 10622 yr-1950 in path_proven_candidates_v1) + 4 NULL-date sibling rows (10005, 10074, 9443, 9798)
- CF-mig123-NEGATIVE-TTR-9-PATIENTS (9 rows clipped)
- CF-mig123-LEGACY-COMPLETION-CHECK-6674 (34d TTR — possibly 2-stage thyroid)
- CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE (Logan-approved phase-2 union)
- CF-mig126-DATA-TYPE-DRIFT (9 cols globally)
- CF-mig126-ORDINAL-POSITION-DRIFT (119 unmatched pairs)
- CF-mig133-PM-LN-COUNT-INTEGRITY (rid 68 — fix in §8.1)
- CF-58-1, CF-58-2, CF-58-3 (parathyroid LLM-extraction edge cases)
- CF-mig58-STRING-AGG-ORDER (medications rollup ordering)

---

## 7. Active Cursor Lanes — Verification Checklist When They Land

When Logan forwards an agent summary for any of Lanes 26/27/28, follow this protocol:

### 7.1 Mechanical checks (5 minutes)
```sql
-- col count + methodology
SELECT verification_method, COUNT(*) FROM canonical_column_verification_registry_v1
WHERE batch_id LIKE 'mig_<N>%' GROUP BY 1 ORDER BY 2 DESC;

-- PM signoff progress
SELECT n_verified, n_not_started, table_status
FROM canonical_table_signoff_registry_v1 WHERE table_name='canonical_patient_master';

-- gate audit (full §11 query)
```

### 7.2 Cohort-uniformity sanity check (CRITICAL — this surfaced the mig_135 21-col shortcut)
For every BOOLEAN col flipped in the lane, compute `SUM(CASE WHEN col THEN 1 ELSE 0 END)`. Flag any col where TRUE-count is 0 unless the cohort genuinely has zero of that thing.

```sql
-- Example pattern (substitute lane's batch + cols)
SELECT SUM(CASE WHEN <col1> THEN 1 ELSE 0 END) AS c1,
       SUM(CASE WHEN <col2> THEN 1 ELSE 0 END) AS c2, ...
FROM canonical_patient_master;
```

### 7.3 Cross-source spot-check (5 random rids per lane)
Pick 5 rids, trace 1–2 cols per rid back to upstream by hand. Confirm derivation logic works.

### 7.4 CF tag review
Read the `notes` column for sampled cols. Confirm CFs documented in the agent summary actually appear in the notes.

### 7.5 Lane-specific gotchas
- **Lane 26 (PMH+PSH)** — watch for the mortality crossover (defer to complications), STRING_AGG ordering, NULL-vs-0 on n_mentions cols. CF-104-PSH-OP-DRIFT should resurface at 17+ pts.
- **Lane 27 (Molecular)** — V7 vs V11 schema generations are version-pinned, don't conflate. Two-source union (v2 master + from_notes_v2). Many genes will likely be cohort-rare → degenerate FALSE risk: BRAF / TERT / NRAS will have signal; HRAS / KRAS / RET-PTC / MSI / PDL1 may be cohort-rare or absent. **Spot-check TRUE counts for every gene flag.**
- **Lane 28 (Recurrence-Response)** — `recurrence_status_final` SSOT enum is `imaging_only_unconfirmed` (NOT `_suspicious`). Cross-check uses event-grain INNER JOIN with `CAST(rid AS VARCHAR)`. recurrence_date will be TIMESTAMP — open `CF-mig139-PM-RECURRENCE-DATE-RETYPE`. **Note: mig# in the prompt says 137 but should be 139** (collision with complications + reserved cleanup mig_136).

---

## 8. Open Issues To Fix (in priority order)

### 8.1 rid 68 LN integrity outlier — fix now
**Investigation already done.** `canonical_patient_master.research_id='68'` has `ln_total_examined=0` and `ln_total_positive=1` (arithmetic violation). 19 sibling patients with same `ln_data_quality_flag='positive_without_exam_count'` have `ln_total_examined=NULL` (correctly representing "no formal exam count documented"). Upstream `manuscript_workspace.cohort_m052_mrlnd_ln_count_v1` parsed `path_synoptics.tumor_1_ln_examined='0'` (location: "right neck biopsy") as literal 0 instead of NULL. The 0-examined-1-positive combo is medically nonsensical.

**Proposed fix (mig_136):**
```sql
-- Pre-snapshot
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig136_rid68_20260429 AS
SELECT * FROM main.canonical_patient_master WHERE research_id='68';

-- Single-row UPDATE on PM
UPDATE main.canonical_patient_master
SET ln_total_examined = NULL,
    ln_count_reconciled = NULL  -- re-evaluate; may need to recompute if it depends on examined
WHERE research_id='68';

-- Mirror upstream so next rebuild doesn't reintroduce
UPDATE manuscript_workspace.cohort_m052_mrlnd_ln_count_v1
SET ln_total_examined = NULL
WHERE research_id='68';

-- Update column-registry notes on ln_total_examined
UPDATE canonical_column_verification_registry_v1
SET notes = notes || ' | mig_136 (2026-04-29): rid 68 was the single arithmetic violation in CF-mig133-PM-LN-COUNT-INTEGRITY. Root cause: upstream path_synoptics.tumor_1_ln_examined=''0'' (right neck biopsy) parsed as literal 0; semantically should be NULL like the other 19 positive_without_exam_count patients. Fixed in PM + cohort_m052; CF-mig133-PM-LN-COUNT-INTEGRITY CLOSED.'
WHERE table_name='canonical_patient_master' AND column_name IN ('ln_total_examined','ln_count_reconciled');
```

Sign-off SQL: `qc_framework_v1/migrations/136_rid_68_ln_integrity_fix_20260429.sql`. Logan must explicitly authorize the `query_rw` calls.

### 8.2 28+ degenerate cohort-uniform-FALSE cols — investigate, then reclassify

**Confirmed degenerates (TRUE count = 0 across 10,871 patients):**

| Lane | mig | Cols | Reason for degeneracy |
|---|---|---|---|
| 25-new complications | 135 | 21 cols: pneumothorax (3), wound_dehiscence (3), wound_infection (12), airway_complication (3) | `canonical_complications_events_v1` has 0 events for these categories — rollup cols exist but uniformly FALSE |
| 25 labs | 134 | 1 col: `biochemical_concern_flag` | Script 224 tier-3 helper produces uniform FALSE |
| 24 LN | 133 | 6 borderline: ln_mets_ftc, ln_mets_hurthle, ln_mets_pdtc + 3 ln_rollup_mets_* variants | Histology-rare LN-mets — cohort genuinely has 0 LN-mets-with-this-histology |

**Investigation plan (DO THIS FIRST before reclassifying):**

1. **Confirm the count** — re-run cohort-uniformity check across ALL booleans in mig_132/133/134/135 batches (the §7.2 pattern). Numbers above are confirmed but expand the survey.

2. **Check mig_130 (operative) and earlier batches** — same degenerate pattern may exist in already-landed lanes. Run cohort-uniformity sweep across all 689 verified PM cols.

3. **For each degenerate col, decide root cause:**
   - **Type A: Real cohort absence** — cohort actually has 0 of that thing (e.g., ln_mets_ftc in a thyroid cancer cohort dominated by PTC). Verification is technically valid; col is not a shortcut, it just happens to be invariant.
   - **Type B: Upstream signal not yet extracted** — cohort likely has cases (pneumothorax, wound_infection) but the LLM extraction pipeline doesn't cover them yet. Verification is degenerate; the FALSE is "we don't know" not "we know it's FALSE."
   - **Type C: Helper-script artifact** — col defined by a deterministic script that always emits one value (biochemical_concern_flag). Verification is structurally degenerate.

4. **Decide reclassification per type:**
   - **Type A** → keep `verified`, add CF tag `CF-mig<N>-COHORT-INVARIANT-<DOMAIN>` as informational; verification_method gets a `_cohort_invariant_documented` suffix
   - **Type B** → flip `verification_status='na'` with `verification_method='cohort_uniform_false_no_upstream_signal_pending_llm_extraction_expansion'`; PM n_verified drops accordingly; future CF-mig<N>-EXPAND-LLM-EXTRACTION-<DOMAIN> opens for re-verification when LLM coverage expands
   - **Type C** → flip to `na` with `verification_method='helper_script_invariant'`; document the script SSOT

5. **Reclassification migration (mig_140 placeholder):** Build a single SQL file that does the per-col UPDATEs. Pre-snapshot the column registry. Apply, verify n_verified moved correctly, commit.

**Estimate:** Lane 25-new pneumothorax/wound_*/airway_complication = Type B (chart review will surface real cases — these complications happen in any thyroid surgery cohort). LN mets-rare-histology = Type A (real). biochemical_concern_flag = Type C (helper artifact).

### 8.3 path_synoptics → cnln derivation gap — investigate, may need separate Cursor lane

**Finding:** 1,199 patients have `path_synoptics.tumor_1_ln_involved` as positive integer (1, 2, 3, ... up to 22+) but `canonical_patient_master.cnln_path_any_positive=FALSE`. This is much bigger than the rid 68 issue.

**Investigation steps:**
1. Read `qc_framework_v1/migrations/111_cervical_ln_clinical_events_table_signoff.sql` and `113_cervical_ln_clinical_patient_rollup_signoff.sql` to understand the documented lineage.
2. Check whether `canonical_cervical_ln_clinical_events_v1` is built from `path_synoptics` or excludes it by source-table policy.
3. Sample 10 of the 1,199 rids — what note types / pathology evidence do they actually have? Are they true LN-positive cases that the cervical_ln chain missed, or false-positives in path_synoptics (e.g., the involved count refers to a different specimen)?
4. If the gap is systematic and material, draft a Cursor prompt for a focused fix lane (rebuild cervical_ln_clinical_events_v1 to include path_synoptics, or add a CF-mig141-CERVICAL-LN-PATH-SYNOPTIC-MISS).

**Don't push downstream lanes that rely on cnln_*** until this is understood — Lane 28 (recurrence-response) derivation map references cnln_*.

### 8.4 mig# renumbering on the 3 pending Cursor prompts

The 3 in-flight prompts (PMH+PSH, molecular, recurrence-response) reference mig_135/136/137 in their sign-off SQL section, but those numbers are taken (135 = complications, 136 = reserved for rid 68 fix). Update before they confuse the agent:

- `cursor_prompts/CURSOR_PROMPT_patient_master_pmh_psh_cluster_20260429.md` — change `mig_135_*` → `mig_137_*`
- `cursor_prompts/CURSOR_PROMPT_patient_master_molecular_cluster_20260429.md` — change `mig_136_*` → `mig_138_*`
- `cursor_prompts/CURSOR_PROMPT_patient_master_recurrence_response_cluster_20260429.md` — change `mig_137_*` → `mig_139_*` (also update CF-mig137 → CF-mig139)

If the agents have already started, just verify the actual `batch_id` they used and document any collision. The fix is cosmetic.

---

## 9. Decision Tree After Reading This Doc

After §10 first-action checklist, decide:

### A. If 1+ active lanes have landed → verify them
1. Run §7 verification protocol (mechanical + cohort-uniformity + spot-check + CF tag review)
2. Surface findings to Logan
3. Write closeout memory entries
4. Update MEMORY.md index
5. Then return to (B) or (C)

### B. If no new lane landed, the open issues are higher priority → fix them
1. Start with §8.1 (rid 68 — cleanest, fastest) — describe SQL, ask Logan, apply, verify
2. Then §8.2 cohort-uniformity sweep (investigation first, no writes yet) — produces a reclassification plan
3. Then §8.3 path_synoptics → cnln (investigation only initially)
4. Then §8.4 (cosmetic prompt renumbering)

### C. If active lanes are slow and open issues are blocked on Logan → generate next prompts
The next-priority PM clusters are (in upstream-readiness order):
1. **ETE cluster** (~20 cols) — derives against `canonical_ete_event_resolved_v1` + `canonical_invasion_events_v1` (both verified)
2. **Survival cluster** (~21 cols) — derives against `canonical_survival_followup_v1` (verify status first); mortality crossover from complications
3. **RAI cluster** (~36 cols) — verify upstream RAI-treatments canonical exists/verified first
4. **FNA cluster** (~12 cols) — derives against `canonical_fna_events_v1`
5. **Frozen section** (~3 cols) — derives against `canonical_frozen_section_events_v1` (verified)
6. **Demographics** (~16 cols) — small, low-risk
7. **Other imaging** (~105 cols) — CT/MRI/PET; may need sub-clustering
8. **Staging** (~2 cols) — trivial
9. **Other / residual** (~516 cols) — needs further sub-clustering investigation

For each new prompt: model it on `cursor_prompts/CURSOR_PROMPT_patient_master_lymph_node_cluster_20260429.md` (well-structured template). Include the §11 audit query, §7.2 cohort-uniformity sanity check requirement, and the `feedback_*` references.

---

## 10. First-Action Checklist (do this before anything else)

1. **`git fetch && git pull`** — get current tip
2. **`git -C "/Users/ros/THyroid 2026" log --oneline -20`** — see latest commits; identify which Cursor lanes have landed since the v3 doc was written
3. **Run the §11 5-gate audit** — confirm gates 2/3/4 are still 0; record current gate 1 (verified canonicals) and gate 5 (date violations on verified)
4. **Check PM verification progress:**
   ```sql
   SELECT batch_id, COUNT(*) FROM canonical_column_verification_registry_v1
   WHERE table_name='canonical_patient_master' AND verification_status='verified'
   GROUP BY 1 ORDER BY 2 DESC;
   ```
5. **Check active lanes status:**
   ```sql
   SELECT batch_id, COUNT(*), MAX(verified_ts) FROM canonical_column_verification_registry_v1
   WHERE batch_id LIKE 'mig_13%' OR batch_id LIKE 'mig_14%' GROUP BY 1 ORDER BY 1;
   ```
6. **Read MEMORY.md** end-to-end — orientation
7. **Read 5 most-relevant memory files:** `feedback_motherduck_direct_check.md`, `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md`, `project_complications_events_verified_2026-04-28.md`, `feedback_clinical_dates_calendar_only.md`, `project_meta_registries_mig_126_closeout.md`
8. **Read this doc's §6 (current state) and §8 (open issues) carefully**
9. **Decide A/B/C from §9** based on what's landed

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
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1
     WHERE table_status='verified' AND signoff_migration IS NULL) AS gate2,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t
     WHERE t.table_status='verified'
       AND (t.n_verified + t.n_na <> t.n_columns_total OR t.n_not_started <> 0 OR t.n_failed <> 0)) AS gate3,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 r
     JOIN main.canonical_table_signoff_registry_v1 t USING (schema_name, table_name)
     WHERE t.table_status='verified' AND r.verification_status='verified'
       AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL)) AS gate4,
  (SELECT COUNT(*) FROM information_schema.columns c
     JOIN verified_tables v ON c.table_name = v.table_name
     LEFT JOIN main.canonical_column_verification_registry_v1 r
       ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
     WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
       AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
       AND c.column_name NOT LIKE '%_status'
       AND c.column_name NOT LIKE '%_source'
       AND c.column_name NOT LIKE '%_keyword'
       AND c.column_name NOT LIKE '%_raw'
       AND COALESCE(r.verification_status,'unknown') != 'na'
       AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
            OR (c.data_type='VARCHAR' AND (
                  regexp_matches(c.column_name, '(^|_)dates?(_|$)')
               OR regexp_matches(c.column_name, '(^|_)dt(_|$)')
            )))) AS gate5;
```

Expected at handoff: **gate1=87, gate2=0, gate3=0, gate4=0, gate5=21**. Increment gate 1 as PM and other tables verify out. Gate 5 should drop in batch when the global date-retype migration closes the 21 violators.

---

## 12. Verbatim prompt to paste into the new Cowork chat

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v3.md` end-to-end before any tool use. Then run the §10 first-action checklist (git tip, 5-gate audit, PM batch progress, active lane status, read MEMORY.md + 5 specific memory files). After that, follow the §9 decision tree:
>
> - If active Cursor lanes (PMH+PSH / molecular / recurrence-response) have landed since the doc was written, verify them per §7 verification protocol (especially the §7.2 cohort-uniformity sanity check that surfaced 21 degenerate cols last session) and surface findings.
> - Otherwise, prioritize the §8 open issues: rid 68 fix (§8.1), cohort-uniform-FALSE investigation across all PM batches (§8.2), and the path_synoptics → cnln 1,199-pt derivation gap (§8.3).
> - Only generate new Cursor prompts (§9 option C) if the open issues are blocked on Logan and active lanes are slow.
>
> **Critical rigor reminder:** verify all Cursor work directly against MotherDuck. Last session, mig_135 (complications, 147 cols) shipped with 21 degenerate cohort-uniform-FALSE cols that the agent reported as "verified" because the derivation `cohort-uniform-FALSE = cohort-uniform-FALSE` was technically true. Your job is to catch these patterns by running cohort-uniformity sweeps on every BOOLEAN col flipped in a lane. Be skeptical of agent summaries.

End of handoff doc.
