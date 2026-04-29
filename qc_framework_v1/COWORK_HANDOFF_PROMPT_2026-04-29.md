# Cowork Handoff Prompt — Thyroid Canonical Publication v1.0 Master Cleanup

**Generated:** 2026-04-29 (UTC) — comprehensive snapshot for a fresh Cowork chat
**For:** New Cowork conversation continuing the master canonical cleanup
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `8305607` (4 prompts in flight at handoff: Lane 19 RESUME + Lanes 23/24/25)

Read this entire prompt before any tool use. Then follow the **First-action checklist** at the bottom.

---

## 0. TL;DR

| Metric | Value |
|---|---|
| Verified canonicals | 87 / 175 (49.7%) |
| In-progress | 1 (`canonical_patient_master`: 233/1,598 cols ~14.6%) |
| Not-started | 87 |
| 5-gate audit | gate 1=87, 2=0, 3=0, 4=0, 5=20 (all 20 are documented CFs) |
| Total cols verified | ~3,200 / 5,502 (~58%) |
| Active Cursor lanes | 4 (Lane 19 RESUME + 23 + 24 + 25) |
| Recent commits | `8305607` (4 prompts), `63fcfb3` (mig_130 PM operative), `3f5772c` (mig_129 helpers), `35b66db` (mig_128 tier3 errors) |

Plan continues by: (1) finishing patient_master clusters, (2) RW-rebuilding `canonical_recurrence_v1` via Lane 19 RESUME mig_123, (3) closing remaining 87 not_started canonicals + raw mirrors, (4) executing the batch CF-100/117/119/120/mig122/mig130 date-retype migration.

---

## 1. Project goal

Verify and clean every table in `thyroid_canonical_publication_v1_0` under Protocol v2. Final outcome:

1. All canonical tables: `table_status='verified'` in `main.canonical_table_signoff_registry_v1`
2. All cols: `verification_status` flagged `verified` or `na` (zero not_started, zero failed)
3. Standardized values on every analytic column (controlled vocabularies)
4. Old / archived tables and columns removed
5. Patient-level rollups + view layer aligned with verified events tables
6. Lakehouse passes the 5-gate cleanliness audit (see §7)
7. CFs resolved (date-retype batch, lineage fixes, etc.)

---

## 2. Tools + access

### MotherDuck
- DB: `thyroid_canonical_publication_v1_0` on `logan.glosser.eras@gmail.com` MD account (NOT the `logan.glosser@gmail.com` account)
- Cowork's MotherDuck MCP is authed to `.eras` directly — use `mcp__eaae7896-…__query` (read) and `query_rw` (write)
- For local Python via Desktop Commander: SA token in `motherduck.local.toml` (gitignored, pre-configured for `.eras`)
- See: `reference_protocol_v2_md_accounts.md` (auto-memory)

### GitHub
- Repo: `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`
- Single branch: `main`
- Author convention: `Logan Glosser <logan.glosser@gmail.com>`
- Push from sandbox bash fails (no creds); use Desktop Commander for all `git push`
- Common gotcha: `.git/index.lock` sometimes stuck — remove with `rm -f .git/index.lock .git/HEAD.lock` before commits

### Desktop Commander
- Preferred for terminal/git/Python REPL operations (Logan's stated preference)
- See: `feedback_use_desktop_commander_first.md` (auto-memory)
- Pattern: spawn one persistent bash process at session start, reuse for all git + python ops

### Cowork-direct tools
- Read / Write / Edit (file tools)
- mcp__eaae7896-…__query / query_rw (MotherDuck)
- mcp__workspace__bash (sandbox bash — limited; use Desktop Commander instead for git)
- mcp__cowork__* (artifacts, request_cowork_directory)
- AskUserQuestion (clarifying questions to Logan)

### Auto-memory (Cowork-only, persists across conversations)
- Path: `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
- Index: `MEMORY.md` (84 entries)
- Read MEMORY.md FIRST after orientation — it surfaces Logan's preferences + reusable findings

---

## 3. Reference documents

### In repo (`/Users/ros/THyroid 2026/`)

| Path | Purpose |
|---|---|
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29.md` | THIS FILE |
| `qc_framework_v1/MASTER_VERIFICATION_PLAN.md` | Master plan (older but still authoritative) |
| `qc_framework_v1/REMAINING_WORK_INVENTORY.md` | Inventory of remaining tables (stale at top — at 13/184; current state is 87/175) |
| `qc_framework_v1/VERIFICATION_PROGRESS.md` | Progress dashboard |
| `qc_framework_v1/VERIFIED_TABLES.md` | Verified-tables log |
| `qc_framework_v1/AGENTS.md` | Continual-learning policy: survival CF caveats, calendar policy |
| `qc_framework_v1/migrations/100-130_*.sql` | Recent close-out migrations (148 total mig files) |
| `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` | Latest canonical 5-gate audit query template |
| `qc_framework_v1/migrations/clinical_date_retype_20260428.md` | Calendar-DATE retype anchor (scripts/413) |
| `cursor_prompts/CURSOR_PROMPT_*.md` | All Cursor agent prompts (one per parallel lane); ~30 files |
| `scripts/203b_canonical_recurrence_harmonized_20260429.py` | Lane 19 harmonized rebuild script (dry-run committed; RW pending) |
| `scripts/output/canonical_recurrence_203b_dry_run_report_20260429.{json,md}` | Lane 19 dry-run reports |
| `scripts/413_clinical_date_retype.py` | Calendar-date retype reference |
| `motherduck.local.toml` | Pre-configured `.eras` SA token (gitignored) |
| `_md_connect.py` / `motherduck_client.py` | MD connection helpers |

### In MotherDuck (`thyroid_canonical_publication_v1_0`)

| Object | Purpose |
|---|---|
| `main.canonical_table_signoff_registry_v1` | Per-table verification status (175 rows; verified mig_126) |
| `main.canonical_column_verification_registry_v1` | Per-column verification status (5,502 rows; verified mig_126) |
| `main.canonical_*_events_v1` | Tier 1 events tables (most verified) |
| `main.canonical_*_patient_rollup_v1` | Tier 1 patient-level rollups (most verified) |
| `main.canonical_labs_*_v1` | Tier 2 labs canonicals (5 tables, all verified mig_115) |
| `main.canonical_molecular_genetics_v2` | Tier 2 molecular master (verified mig_116) |
| `main.canonical_us_*_v2` | Tier 1 source US imaging family (3 tables, all verified mig_117) |
| `main.canonical_patient_master` | The big anchor (1,598 cols; 233 verified, 1,361 not_started, 4 na — IN PROGRESS) |
| `main.canonical_recurrence_v1` | Currently shell-verified mig_122 (degenerate; Lane 19 RW rebuild pending) |
| `main.canonical_recurrence_resolved_v1` | Tier 2 recurrence (verified mig_125) |
| `main.canonical_ete_event_resolved_v1` | Tier 2 ETE adjudication (verified mig_121) |
| `main.canonical_survival_followup_v1` | Cohort-wide survival (verified mig_123) |
| `archive_pub_v1_0` schema (in `"Thyroid 2026 UPdated"` DB) | Pre-Script-N snapshots for CTC verification |
| `manuscript_workspace.*` | 91 helpers; 16 verified by mig_129; 75 not_started |
| `manuscript_workspace.cpm_reconciliation_provenance_v1` | Provenance log |

### In auto-memory — MUST-READ before forging ahead

**Required:**
- `feedback_motherduck_direct_check.md` — Re-query MD before recommending; don't trust prior summaries
- `feedback_clinical_dates_calendar_only.md` (or `qc_framework_v1/migrations/clinical_date_retype_20260428.md`) — clinical date cols MUST be DATE; lab_datetime exempt
- `feedback_use_desktop_commander_first.md` — Desktop Commander > Claude in Chrome > computer-use
- `feedback_surgical_git_add.md` — Never `git add -A` or `scripts/output/`; explicit paths only
- `feedback_phi_safety.md` — Never print clinical notes; research_id only
- `feedback_commit_workflow.md` — Always stage/commit/push; lint Python first
- `reference_protocol_v2_md_accounts.md` — `.eras` account hosts publication DB
- `reference_duckdb_timestamp_tz.md` — Always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts
- `feedback_alter_view_dependents.md` — `CREATE OR REPLACE VIEW` for dependents in same commit
- `feedback_audit_regex_word_boundary.md` — gate-5 must use word-boundary regex; allowlist + na-filter (mig_127)
- `feedback_etevent_resolved_cross_check.md` — Cross-checks against ete_event_resolved use event-grain INNER JOIN with `CAST(rid AS VARCHAR)`; LEFT JOIN at patient grain produces ~6,734 false drift
- `feedback_recurrence_imaging_n_events_null.md` — NULL not 0 for no-events case; COALESCE before IS DISTINCT FROM compares

**Most-relevant close-outs (read 2-3):**
- `project_op_procedure_codes_mig_118_closeout.md` — hybrid pattern #9 (extraction-faithfulness + internal-consistency)
- `project_meta_registries_mig_126_closeout.md` — orphan-col cleanup + patient_master alignment patterns
- `project_recurrence_resolved_v1_mig_125_closeout.md` — Tier 2 recurrence
- `project_canonical_recurrence_v1_mig_122_closeout.md` — Cursor 14's shell signoff + 3 CFs
- `project_path_gland_family_complete_2026-04-28.md` — rebuild-then-verify pattern
- `project_cleanliness_audit_2026-04-29.md` — 5-gate audit (mig_109 + 117 + 127 evolution)

---

## 4. Verification methodology library (10 patterns established)

When picking a table to verify, match it to the right pattern:

1. **CTC-equivalence vs pre-Script-N archive** — for canonicals built by SELECT*+filter+UPDATE chains; pre-N archive snapshot in `archive_pub_v1_0` is value-source-of-truth (mig_87/90/91/100)
2. **Script-rule re-run** — for post-build UPDATE-derived cols (mig_88/90b)
3. **Derivation re-derivation against verified events** — for rollups (mig_95b/101/104/106/112/113)
4. **Per-finding Logan review with rule-based pre-filter** — for LLM-output canonicals (mig_91-94)
5. **Note-text REAL/TEMPLATE classifier on `clinical_notes_long`** — for finding-event canonicals (mig_98 family, mig_103)
6. **Extraction-faithfulness vs upstream JSON** — for LLM-built canonicals (mig_102/115/114)
7. **Extraction-faithfulness UNNEST variant** — entity-grain extraction (mig_110/111)
8. **Cross-table crosswalk via JOIN to verified rollup** — for cols populated from another verified canonical (mig_114 ete_subgrade rollup)
9. **Hybrid (extraction-faithfulness + internal-consistency)** — when upstream re-derivation is blocked by upstream drift (mig_118 / mig_125)
10. **Cohort-wide shell verification (degenerate case)** — when canonical is intentionally a shell pending rebuild (mig_122 recurrence_v1)

**Audit refinement evolution:**
- mig_109: established 5-gate audit
- mig_117: extended allowlist (`built_at`, `ingestion_date`); refined regex to word-boundary
- mig_127: added `na`-filter to gate 5 (eliminates 3 false positives on `note_date` cols correctly tagged `auto_provenance_skip`)

The latest canonical 5-gate audit query is in `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` (header comment block).

---

## 5. Workflow protocol

### Cowork direct (no Logan needed)
- State probing (information_schema, registries, archives)
- Mechanical CTC-equivalence + extraction-faithfulness verification
- Bulk priority-rule application (after Logan ratifies the rule once)
- Registry flips after all-cols-verified
- Memory writes + docs + commits

### Spawn Cursor agent (Cursor lane via prompt file in `cursor_prompts/`)
- Long mechanical work (>1 hour, multi-file)
- Classifier work (note-text scanning, clinical_notes_long pulls)
- Repair migrations (path_gland-style; date retypes; data backfills)
- Multi-table family verifications (labs, US v2, etc.)
- Substantial RW operations (always with explicit Logan-approval gate; e.g. Lane 19)

### Logan-touch (.xlsx review or AskUserQuestion)
- Per-row clinical adjudication (invasion family pattern, mig_91-94)
- Approve/modify proposed bulk dispositions
- Resolve gray-zone date-attribution edges
- Resolve ambiguous decision vocab
- Approve novel methodology decisions
- Approve RW migrations on canonical tables (e.g. Lane 19 mig_123)

---

## 6. ⚠️ Active in-flight Cursor lanes — DO NOT TOUCH these tables/scripts

At handoff time (2026-04-29):

| Lane | Target | Status | Prompt file |
|---|---|---|---|
| 19 RESUME | `canonical_recurrence_v1` Script 203b RW + mig_123 | Logan approved Option 1 fixes; Cursor pending RW execute | `cursor_prompts/CURSOR_PROMPT_lane19_resume_203b_fixes_and_RW_20260429.md` |
| 23 | `canonical_patient_master` PATHOLOGY cluster (~82 cols) | Likely in flight | `cursor_prompts/CURSOR_PROMPT_patient_master_pathology_cluster_20260429.md` |
| 24 | `canonical_patient_master` LYMPH_NODE cluster (~80 cols) | Likely in flight | `cursor_prompts/CURSOR_PROMPT_patient_master_lymph_node_cluster_20260429.md` |
| 25 | `canonical_patient_master` LABS cluster (~65 cols) | Likely in flight | `cursor_prompts/CURSOR_PROMPT_patient_master_labs_cluster_20260429.md` |

**Don't verify or touch:**
- `canonical_recurrence_v1` (Lane 19 owns it)
- `canonical_patient_master` rows tagged with `path_*`, `*histology*`, `*tumor*`, `*stage_*`, `bethesda_*`, `*synoptic*` (Lane 23)
- `canonical_patient_master` rows tagged with `ln_*`, `*lymph_node*`, `cervical_*`, `*ene_*`, `*lateral_neck*`, `*central_neck*` (Lane 24)
- `canonical_patient_master` rows tagged with `lab_*`, `*thyroglobulin*`, `*tg_*`, `*tsh*`, `*calcium*`, `*pth*`, `*vitamin_d*`, `*biochemical*` (Lane 25)
- `scripts/203b_canonical_recurrence_harmonized_20260429.py` (Lane 19 will edit)

Pull origin frequently to know when each lands: `git fetch && git log --oneline origin/main -5`

**Recently completed lanes (2026-04-29):**
- Lane 9 (mig_116 molecular_v2)
- Lane 10 (mig_117 us_v2_family — 3 tables)
- Lane 11 (mig_119 frozen_section_rollup — 188 cols)
- Lane 12 (mig_120 path rollup pair)
- Lane 13 (mig_121 ete_event_resolved + ete_inline_adjudication paired)
- Lane 14 (mig_122 recurrence_v1 SHELL — Cursor 19 will rebuild)
- Lane 15 (mig_123 survival_followup_v1)
- Lane 16 (mig_124 molecular_from_notes_v2)
- Lane 17 (mig_125 recurrence_resolved_v1)
- Lane 18 (mig_126 meta-registries pair)
- Lane 20 (mig_128 tier3_extraction error col batch)
- Lane 21 (mig_129 manuscript_workspace helper batch)
- Lane 22 (mig_130 patient_master OPERATIVE cluster — 233 cols)

---

## 7. Standing reminders (re-run before any forging-ahead push)

### 5-gate cleanliness audit (refined, mig_127 template)

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

Expected: gate1=87, gate2=0, gate3=0, gate4=0, gate5=20 (all 20 are documented CFs).

### Other standing reminders
- Confirm in MD before recommending: don't trust prior summaries
- 0 failed CFs target: every CF resolves to either verified (with explanation) or na (auto-skip)
- PHI rule: never print clinical notes; research_id only
- Lint Python: `python3 -m py_compile` before commit
- DuckDB CURRENT_TIMESTAMP: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts
- VIEW naming: any `main.*` VIEW must carry `_VIEW` suffix

---

## 8. Open carry-forwards (CFs)

**Date-retype batch — single future repair migration will close all 20 gate-5 violators:**
- CF-100-DATE-RETYPE: `frozen_section_events.frozen_section_date` (1)
- CF-117-DATE-RETYPE: 4 cols on molecular_v2 + ete_event_resolved
- CF-119-FROZEN-ROLLUP-DATE-RETYPE: 14 cols on frozen_section_patient_rollup_v1
- CF-120/path-DATE-RETYPE: 2 cols on path_malignant_patient_rollup_v1
- CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE: 1 col (resolves when Lane 19 RW lands)
- CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE: 2 cols on canonical_patient_master (`first_surgery_date`, `surg_first_date`); calendar SSOT is `first_surgery_date_v2` (DATE)

**Methodology / data-quality CFs (Lane 19 RESUME will open these):**
- CF-mig123-UPSTREAM-DATE-202-TYPO (rids 12057, 10622 in `manuscript_workspace.recurrence_path_proven_candidates_v1`)
- CF-mig123-NEGATIVE-TTR-9-PATIENTS (9 rows clipped to NULL)
- CF-mig123-LEGACY-COMPLETION-CHECK-6674 (34d "recurrence" possibly planned 2-stage thyroid)
- CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE (Tier 1 currently uses path_synoptics; Logan-approved phase-2 migration to canonical_path_malignant_events_v1 + canonical_pathology_clinical_events_v1 union — within 1-2 sessions)

**Other open CFs:**
- CF-mig126-DATA-TYPE-DRIFT (9 rows globally — TIMESTAMP/TZ/VARCHAR/DATE quirks)
- CF-mig126-ORDINAL-POSITION-DRIFT (119 unmatched pairs — schema reshuffle since seed)
- CF-118-UPSTREAM-DATE-FORMAT-DRIFT (note_entities_procedures.note_date now MM/DD/YYYY VARCHAR)
- CF-58-1, CF-58-2, CF-58-3 (parathyroid LLM-extraction edge cases)

---

## 9. First-action checklist

Before touching anything, do ALL of the following:

### 1. Verify MD access
```sql
SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified';
```
Expected: 87 (or higher if Cursor lanes 19/23/24/25 have landed)

### 2. Confirm tip of origin/main
```bash
cd "/Users/ros/THyroid 2026" && git fetch origin && git log --oneline origin/main -10
```
Expected at handoff time: `8305607` (or newer if Cursor lanes pushed)

### 3. Check Cursor 19 + 23 + 24 + 25 status
```sql
SELECT table_name, table_status, signoff_migration, signed_off_ts
FROM main.canonical_table_signoff_registry_v1
WHERE table_name IN ('canonical_recurrence_v1','canonical_patient_master');
```
- `canonical_recurrence_v1` signoff_migration='%122%' → Lane 19 still pending; signoff_migration='%123%' → Lane 19 done
- `canonical_patient_master` n_verified > 233 → Lane 23/24/25 partial done

### 4. Run the 5-gate audit (§7) — should be green except gate 5 = 20

### 5. Read 4 most-relevant memory files
Required: `feedback_motherduck_direct_check.md`, `feedback_audit_regex_word_boundary.md`, `feedback_etevent_resolved_cross_check.md`, `project_op_procedure_codes_mig_118_closeout.md`

### 6. Survey unverified scope
```sql
SELECT table_name, schema_name, n_columns_total, n_not_started, priority_tier, table_status
FROM main.canonical_table_signoff_registry_v1
WHERE table_status IN ('not_started','in_progress','partial')
ORDER BY priority_tier, n_not_started DESC;
```

### 7. Spawn one Desktop Commander bash for the session (for git ops + python REPLs)

---

## 10. Decision tree — what to do next

After §9 checklist:

### If Lanes 19/23/24/25 ALL still in flight:
Forge on small Tier 2 / Tier 3 work in your lane. Disjoint candidates:
- Verify `canonical_cleanup_audit_v1` (manuscript_workspace; 18 cols / 16 not_started)
- Sample-verify a few of the 75 remaining tier3_helper tables (Lane 21 paused at 16/91)
- Verify `canonical_patient_master` smaller clusters (RAI ~36, FNA ~25, ETE ~20, SURVIVAL ~18, MEDS ~17, MOLECULAR ~7) — but check that no Lane 23/24/25 sibling owns them first
- Check on Lane 19 dry-run (Cursor 19 may need a nudge)

### If 1+ lanes have landed:
1. Verify each landed lane (table_status, n_verified, no failed, methodology check)
2. Save close-out memory entries for each
3. Re-run 5-gate audit to confirm reconciliation
4. Decide next batch: continue patient_master clusters (RAI / PMH+PSH / US imaging / RECURRENCE / FNA / ETE / SURVIVAL / MEDS / MOLECULAR / COMPLICATIONS / FROZEN_SECTION / DEMOGRAPHICS / OTHER ~975) OR pivot to manuscript_workspace cleanup OR raw-mirror sample verification

### Big remaining scope (deferred — these need their own prompts):
- `canonical_patient_master` remaining clusters (~13 thematic + ~975 "other") — multi-session effort
- 12 raw mirror sources (path_synoptics 311 cols, manuscript_cohort_v1 151 cols, etc.) — sample-based
- 75 helpers in `manuscript_workspace` (Lane 21 paused at 16/91)
- 5 `note_entities_*` not raw-LLM-exempt (after Lane 20)
- 419 archive snapshots in `archive_pub_v1_0` (post-verification cleanup pass)
- Final batch CF-100/117/119/120/mig122/mig130 date-retype migration

---

## 11. Recent commits (for context)

```
8305607 docs: Lane 19 RESUME prompt + 3 next prompts (Lanes 23/24/25)
63fcfb3 qc: mig_130 CPM operative cluster verification (233 cols, partial)
3f5772c Add migration 129 tier3_helper manuscript_workspace batch sign-off
35b66db feat(qc): mig_128 tier3_extraction LLM mirrors — verify error columns
4b55fab mig_127 audit refinement (na filter) + 3 next cursor prompts
08325dc feat(recurrence): harmonized Script 203b + dry-run reports (Lane 19)
78b75bb qc: meta-registry pair sign-off + registry pre-reconcile (mig 126)
b367e91 qc: sign off canonical_recurrence_resolved_v1 (mig 125, Protocol v2)
4156471 docs: 3 next cursor prompts (Lanes 17/18/19)
a64ed3c qc: sign off canonical_molecular_genetics_from_notes_v2 (mig_124)
5d91feb Verify canonical_survival_followup_v1 (Protocol v2 mig_123)
```

---

## 12. Quick links

- [Master verification plan](computer:///Users/ros/THyroid 2026/qc_framework_v1/MASTER_VERIFICATION_PLAN.md)
- [Remaining work inventory](computer:///Users/ros/THyroid 2026/qc_framework_v1/REMAINING_WORK_INVENTORY.md)
- [Verified tables log](computer:///Users/ros/THyroid 2026/qc_framework_v1/VERIFIED_TABLES.md)
- [Cursor prompts dir](computer:///Users/ros/THyroid 2026/cursor_prompts/)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)
- [Latest audit refinement (mig_127)](computer:///Users/ros/THyroid 2026/qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql)
- [Lane 19 RESUME prompt](computer:///Users/ros/THyroid 2026/cursor_prompts/CURSOR_PROMPT_lane19_resume_203b_fixes_and_RW_20260429.md)
- [Lane 23 PM pathology cluster](computer:///Users/ros/THyroid 2026/cursor_prompts/CURSOR_PROMPT_patient_master_pathology_cluster_20260429.md)
- [Lane 24 PM lymph_node cluster](computer:///Users/ros/THyroid 2026/cursor_prompts/CURSOR_PROMPT_patient_master_lymph_node_cluster_20260429.md)
- [Lane 25 PM labs cluster](computer:///Users/ros/THyroid 2026/cursor_prompts/CURSOR_PROMPT_patient_master_labs_cluster_20260429.md)
- [Most recent verification template (mig_125 hybrid)](computer:///Users/ros/THyroid 2026/qc_framework_v1/migrations/125_recurrence_resolved_v1_signoff_20260429.sql)
- [Most recent extraction-faithfulness template (mig_124 mgfn)](computer:///Users/ros/THyroid 2026/qc_framework_v1/migrations/124_molecular_genetics_from_notes_v2_signoff.sql)

End of handoff. Begin with §9 First-action checklist. Once oriented, decide between (a) fact-check landed Cursor lanes (mig_123, mig_131-133 if any), (b) write the next 3 Cursor prompts for the next wave, OR (c) forge on small Tier 2 / Tier 3 work in your lane.

---

## Session continuity context (Cowork-only — bring to the new chat)

**Auto-memory entries from the prior session worth re-loading first:**

Required reading (ranked by relevance):
1. `MEMORY.md` — index of all 84 memory entries
2. `feedback_motherduck_direct_check.md`
3. `feedback_audit_regex_word_boundary.md`
4. `feedback_etevent_resolved_cross_check.md`
5. `feedback_recurrence_imaging_n_events_null.md`
6. `feedback_clinical_dates_calendar_only.md`
7. `feedback_use_desktop_commander_first.md`
8. `feedback_surgical_git_add.md`
9. `project_op_procedure_codes_mig_118_closeout.md`
10. `project_meta_registries_mig_126_closeout.md`
11. `project_recurrence_resolved_v1_mig_125_closeout.md`
12. `project_canonical_recurrence_v1_mig_122_closeout.md`

The 84 memory entries cover: user profile, project architecture, domain conventions (CTC-equivalence pattern, hybrid pattern, derivation re-derivation, etc.), Logan's preferences (no `git add -A`, surgical commits, Desktop Commander first, etc.), 13 close-outs from major mig families, and ~30 reusable feedback memories.
