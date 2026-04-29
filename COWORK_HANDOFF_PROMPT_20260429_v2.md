# Cowork Handoff Prompt — Thyroid Canonical Publication v1.0 Master Cleanup

**Generated:** 2026-04-29 (UTC) by prior Cowork session
**For:** New Cowork chat
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `3f96cb2` — `Verify canonical labs family under Protocol v2`
**Current state:** 50 of 175 tables verified (40 canonical + 10 raw note_entities mirrors); 831 of 5,503 columns verified

Read this entire prompt before any tool use. Then follow the **First-action checklist** at the bottom.

---

## 1. Project goal

Verify and clean every table in `thyroid_canonical_publication_v1_0` under **Protocol v2**. Final outcome:

1. All canonical tables have `table_status='verified'` in `main.canonical_table_signoff_registry_v1`
2. All columns flagged `verified` or `na` in `main.canonical_column_verification_registry_v1` (zero `not_started`, zero `failed`)
3. Standardized values on every analytic column (controlled vocabularies)
4. Old / archived tables and columns removed when no longer load-bearing
5. Patient-level rollups + view layer aligned with verified events tables
6. Lakehouse passes the **5-gate cleanliness audit** (see §7)

This is a multi-session effort. The current session has verified **27 canonical tables + 13 derived rollups + 5 lab tables + 5 raw mirrors** in one push (started at 27 verified, now at 50).

---

## 2. Tools + access

### MotherDuck
- DB: `thyroid_canonical_publication_v1_0` (lives on `logan.glosser.eras@gmail.com` MD account)
- Cowork's MotherDuck MCP is authed to `.eras` directly — use `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query` (read) and `query_rw` (write)
- For local Python via Desktop Commander: SA token in `motherduck.local.toml` (gitignored, pre-configured for `.eras`)
- See auto-memory: `reference_protocol_v2_md_accounts.md`

### GitHub
- Repo: `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`
- Single branch: `main`
- Author convention: `Logan Glosser <logan.glosser@gmail.com>`
- Push access via local credentials (Desktop Commander)
- Common gotcha: `.git/index.lock` sometimes stuck — remove with `rm -f .git/index.lock .git/HEAD.lock` before commits

### Desktop Commander
- Preferred tool for terminal/git/Python REPL operations
- Logan's preference: Desktop Commander > sandbox `mcp__workspace__bash` (workspace mount has stale-lock issues)
- See auto-memory: `feedback_use_desktop_commander_first.md`

### Auto-memory (Cowork-only persistence)
- Path: `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
- Index: `MEMORY.md`
- 50+ memory files capturing patterns, conventions, close-outs, CFs

---

## 3. Reference documents

### In repo (`/Users/ros/THyroid 2026/`)

| Path | Purpose |
|---|---|
| `qc_framework_v1/MASTER_VERIFICATION_PLAN.md` | Master plan |
| `qc_framework_v1/REMAINING_WORK_INVENTORY.md` | Inventory of remaining tables |
| `qc_framework_v1/VERIFICATION_PROGRESS.md` | Progress dashboard |
| `qc_framework_v1/VERIFIED_TABLES.md` | Verified-tables log |
| `qc_framework_v1/migrations/100-115_*.sql` | This-session migration files (most recent close-outs) |
| `qc_framework_v1/migrations/55-99_*.sql` | Prior-session migrations + canonical builds |
| `qc_framework_v1/scripts/build_*_review.py` | Cursor-built classifiers (chyle, voice/nerve, seroma, medications) |
| `qc_framework_v1/scripts/apply_mig_*_decisions.py` | Cursor-built apply scripts |
| `cursor_prompts/CURSOR_PROMPT_*.md` | All Cursor-agent prompt files (one per parallel lane) |
| `motherduck.local.toml` | Pre-configured `.eras` SA token (gitignored) |
| `motherduck_client.py` | MD connection helper used by all scripts |

### In MotherDuck (`thyroid_canonical_publication_v1_0`)

| Object | Purpose |
|---|---|
| `main.canonical_table_signoff_registry_v1` | Per-table verification status |
| `main.canonical_column_verification_registry_v1` | Per-column verification status |
| `main.canonical_*_events_v1` | Tier 1 events tables |
| `main.canonical_*_patient_rollup_v1` | Tier 1 patient-level rollups |
| `main.canonical_labs_*_v1` | Tier 2 labs canonicals (5 tables) |
| `main.canonical_molecular_genetics_v2` | Tier 2 molecular master |
| `main.canonical_us_*_v2` | Tier 1 source US imaging family (3 tables) |
| `main.canonical_patient_master` | The big anchor (1,592 cols, 1,588 not_started — biggest scope) |
| `archive_pub_v1_0` schema (in `"Thyroid 2026 UPdated"` DB) | Pre-Script-N snapshots for CTC verification |
| `manuscript_workspace.cpm_reconciliation_provenance_v1` | Provenance log |

### In auto-memory — most relevant for the next round

Required reading before starting:

| File | One-line hook |
|---|---|
| `feedback_motherduck_direct_check.md` | Re-query MD before recommending — don't trust prior summaries |
| `feedback_clinical_dates_calendar_only.md` | Clinical date cols MUST be DATE; lab_datetime is exempt |
| `feedback_use_desktop_commander_first.md` | Desktop Commander > Claude in Chrome > computer-use |
| `feedback_surgical_git_add.md` | Never `git add -A` or `scripts/output/`; explicit paths only |
| `feedback_phi_safety.md` | Never print clinical notes; research_id only |
| `feedback_commit_workflow.md` | Always stage/commit/push; lint Python first |
| `reference_protocol_v2_md_accounts.md` | `.eras` account hosts publication DB |
| `reference_duckdb_timestamp_tz.md` | Always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts |
| `feedback_alter_view_dependents.md` | `CREATE OR REPLACE VIEW` for dependents in same commit |

Most relevant close-outs to learn the patterns:
- `project_complications_events_verified_2026-04-28.md` — mig_98 8-sub-mig classifier family
- `project_parathyroid_events_mig_102_closeout.md` — first LLM-output extraction-faithfulness
- `project_medications_parathyroid_families_complete_2026-04-29.md` — 3 reusable patterns
- `project_path_gland_family_complete_2026-04-28.md` — stale-rollup-after-events-repair pattern
- `project_cleanliness_audit_2026-04-29.md` — 5-gate audit
- `project_pmh_events_mig_107_closeout.md` (created by Cursor 4 today) — multi-source 3-source pattern

---

## 4. Verification methodology library (8 patterns established)

When you pick a table to verify, match it to the right pattern:

1. **CTC-equivalence vs pre-Script-N archive** — for canonicals built by SELECT*+filter+UPDATE chains; pre-N archive snapshot in `archive_pub_v1_0` is value-source-of-truth. (mig_87 / mig_90 / mig_91 / mig_100)

2. **Script-rule re-run** — for post-build UPDATE-derived cols; re-run UPDATE as SELECT and compare. (mig_88 / mig_90b)

3. **Derivation re-derivation against verified events** — for rollups; re-derive each col fresh from verified events; expect 100% match (or rebuild if stale). (mig_95b / mig_101 / mig_104 / mig_106 / mig_112 / mig_113)

4. **Per-finding Logan review with rule-based pre-filter** — for LLM-output canonicals where per-row adjudication is needed; rule pre-filter + .xlsx for Logan in batches of 20. (mig_91 / mig_92 / mig_93 / mig_94)

5. **Note-text REAL/TEMPLATE classifier on `clinical_notes_long`** — for finding-event canonicals with template/negation noise; classifier output → bulk priority-rule disposition. (mig_98b/c/d/e/f for complications; mig_103 for medications)

6. **Extraction-faithfulness vs upstream JSON** — for LLM-built canonicals (deterministic SELECT*+`json_extract_string` from `note_entities_llm_*`); re-derive every col from upstream `WHERE error=0` and compare per-row. (mig_102 parathyroid / mig_115 labs / mig_114 ete_subgrade)

7. **Extraction-faithfulness UNNEST variant** — sibling of #6 for entity-grain extraction (multiple entities per upstream row); UNNEST(result_json.entities) + DISTINCT ON natural key. (mig_110 pathology_clinical / mig_111 cervical_ln_clinical)

8. **Cross-table crosswalk via JOIN to verified rollup** — for cols populated from another already-verified canonical (e.g., `any_pT4b_from_t4b_invasion`). LEFT JOIN to verified upstream + sub-pattern of #3. (mig_114 ete_subgrade rollup)

Plus 2 sub-variants: **format reshape probe** (for VARCHAR drift on dates — distinguish format vs semantic), **STRING_AGG ordering** (for set-equal but order-different cols), **CASE normalization** (for build-time enum mappings).

---

## 5. Workflow split

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

### Logan-touch (.xlsx review or AskUserQuestion)
- Per-row clinical adjudication (invasion family pattern, mig_91-94)
- Approve/modify proposed bulk dispositions
- Resolve gray-zone date-attribution edges
- Resolve ambiguous decision vocab
- Approve novel methodology decisions (e.g., the calendar-date-only rule)

---

## 6. ⚠️ Active Cursor lanes (parallel — do NOT touch these tables)

**At handoff time, 2 Cursor lanes are still in flight from the prior session's prompt batch:**

| Lane | Target | Estimated time | Prompt file |
|---|---|---|---|
| 9 | `canonical_molecular_genetics_v2` | 90-120 min | `cursor_prompts/CURSOR_PROMPT_molecular_genetics_v2_verification_20260429.md` |
| 10 | `canonical_us_nodule_v2` + `canonical_us_thyroid_gland_v2` + `canonical_us_lymph_node_v2` | 2-3 hrs | `cursor_prompts/CURSOR_PROMPT_us_v2_family_verification_20260429.md` |

**Don't touch these table_name rows in either registry while Cursor is running them.** Specifically avoid:
- `canonical_molecular_genetics_v2`
- `canonical_us_nodule_v2`
- `canonical_us_thyroid_gland_v2`
- `canonical_us_lymph_node_v2`

**Pull from origin frequently** — Cursor will commit + push when done; check `git fetch && git log --oneline origin/main -5` to know when each lands.

**Already completed by Cursor today (this session):**
- Cursor 1: clinical date retype (6 cols across 4 tables → DATE) — script 413
- Cursor 2: medications classifier (mig_103) — 1,028 deletes + 6 PMH attributions
- Cursor 3: PSH events (mig_104) — extraction-faithfulness
- Cursor 4: PMH events (mig_107) — 3-source multi-source pattern
- Cursor 5: complications rollup (mig_108) — derivation re-derivation
- Cursor 6: PSH rollup (mig_110_psh — note: filename collision with Cowork's mig_110_pathology_clinical_events; both functional)
- Cursor 7: PMH rollup REBUILD (mig_114_pmh — filename collision with Cowork's mig_114_ete_subgrade)
- Cursor 8: Labs family (mig_115) — all 5 lab canonicals

**Note on parallel filename collisions:** 3 collisions today (mig_104, mig_110, mig_114). All resolved organically — files coexist with same numeric prefix, different table targets, different filenames. Registry signoff_migration field is the canonical reference. Don't bother renaming retroactively unless Logan asks.

---

## 7. Standing reminders (re-run before any forging-ahead push)

### 5-gate cleanliness audit (mig_109 pattern)
Run via these 5 queries against `main.canonical_table_signoff_registry_v1` + `main.canonical_column_verification_registry_v1`:

```sql
-- Gate 1: verified_tables_total (current: 50)
SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified';

-- Gate 2: tables_missing_signoff (must be 0)
SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1
WHERE table_status='verified' AND signoff_migration IS NULL;

-- Gate 3: tables_count_mismatch (must be 0)
SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t
WHERE t.table_status='verified'
  AND (t.n_verified + t.n_na <> t.n_columns_total OR t.n_not_started <> 0 OR t.n_failed <> 0);

-- Gate 4: verified_cols_missing_metadata (must be 0)
SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 r
JOIN main.canonical_table_signoff_registry_v1 t USING (schema_name, table_name)
WHERE t.table_status='verified' AND r.verification_status='verified'
  AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL);

-- Gate 5: date_violations_on_verified (must be 0)
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name FROM (VALUES
    ('build_ts'),('extracted_at'),('llm_build_ts'),('verified_ts'),
    ('signed_off_ts'),('registered_ts'),('llm_extracted_at'),('updated_at'),
    ('created_at'),('promoted_at'),('completed_at'),('started_at'),('ended_at'),
    ('ingested_at_utc'),('lab_datetime')
  ) v(col_name)
)
SELECT COUNT(*) FROM information_schema.columns c
JOIN verified_tables v ON c.table_name = v.table_name
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
  AND c.column_name NOT LIKE '%_status'
  AND c.column_name NOT LIKE '%_source'
  AND c.column_name NOT LIKE '%_keyword'
  AND c.column_name NOT LIKE '%_raw'
  AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
       OR (c.data_type='VARCHAR' AND (c.column_name ILIKE '%date%' OR c.column_name ILIKE '%dt')));
```

If any gate is non-zero, fix before proceeding (or re-run the audit migration mig_109 pattern).

### Other standing reminders
- **Confirm in MD before recommending**: don't trust prior summaries
- **0 failed CFs target**: every CF resolves to either verified (with explanation) or na (auto-skip)
- **PHI rule**: never print clinical notes; research_id only
- **Lint Python**: `python3 -m py_compile` before commit
- **DuckDB CURRENT_TIMESTAMP**: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts
- **VIEW naming**: any `main.*` VIEW must carry `_VIEW` suffix

---

## 8. First-action checklist

Before touching anything, do ALL of the following:

1. **Verify MD access**:
   ```sql
   SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified';
   ```
   Expected: 50 (or higher if Cursor 9/10 landed)

2. **Confirm tip of origin/main**:
   ```bash
   cd "/Users/ros/THyroid 2026" && git fetch origin && git log --oneline origin/main -5
   ```
   Expected at handoff time: `3f96cb2` (or newer if Cursor 9/10 pushed)

3. **Check Cursor 9 + 10 status**:
   ```sql
   SELECT table_name, table_status, signoff_migration, signed_off_ts
   FROM main.canonical_table_signoff_registry_v1
   WHERE table_name IN ('canonical_molecular_genetics_v2','canonical_us_nodule_v2','canonical_us_thyroid_gland_v2','canonical_us_lymph_node_v2');
   ```
   - All `not_started` → both lanes still in flight, pick non-overlapping work
   - Any `verified` → Cursor lane done, verify on MD + report

4. **Run the 5-gate audit (§7)** — should all be green

5. **Read 4 most relevant memory files** (paths in §3)

6. **Survey unverified scope**:
   ```sql
   SELECT table_name, n_columns_total, n_not_started, priority_tier
   FROM main.canonical_table_signoff_registry_v1
   WHERE table_status='not_started' AND table_name LIKE 'canonical_%'
   ORDER BY priority_tier, n_not_started DESC;
   ```

---

## 9. Decision tree — what to do next

After §8 checklist:

### If Cursor 9 + 10 are still in flight:
**Forge on small Tier 2 derived canonicals in your lane.** All disjoint from Cursor 9/10. Pick one:

- `canonical_recurrence_v1` (10,871 rows / 11 not_started — cohort-wide derivation)
- `canonical_survival_followup_v1` (10,871 / 9 not_started — cohort-wide derivation)
- `canonical_recurrence_resolved_v1` (10,871 / 16 not_started)
- `canonical_operative_procedure_codes_v1` (small / 7 not_started)
- `canonical_ete_inline_adjudication_v1` (small / 9 not_started)

### If Cursor 9 + 10 BOTH have landed:
**Verify both on MotherDuck** (confirm registry signoff state matches Cursor's reported counts) + write the **next 3 Cursor prompts** for the next wave. Best candidates:

1. **`canonical_frozen_section_patient_rollup_v1`** (188 cols! Biggest single rollup. Probably needs derivation re-derivation against verified frozen_section events table.)
2. **`canonical_path_malignant_patient_rollup_v1` + `canonical_path_benign_patient_rollup_v1`** (closeout pair for path family rollups; 14 + 13 not_started cols)
3. **`canonical_ete_event_resolved_v1`** (62 cols / 57 not_started — Tier 2 ETE adjudication; may need cross-table verification against verified ete_subgrade + invasion families)

OR alternately:
- `canonical_molecular_genetics_from_notes_v2` (sibling of molecular_v2 from Cursor 9; LLM-from-notes path)
- `canonical_cleanup_audit_v1` (TBD — investigate)
- The 2 self-referential meta-registries (`canonical_table_signoff_registry_v1` + `canonical_column_verification_registry_v1`) — special verification

### Big remaining scope (deferred — these need their own prompt):
- `canonical_patient_master` (1,588 cols, biggest single table) — auto-derivable from verified Tier 1 cascade
- 12 raw mirror sources (path_synoptics 311 cols, manuscript_cohort_v1 151 cols, etc.) — sample-based verification
- 91 helpers in `manuscript_workspace`
- 17 `note_entities_*` (10 already raw-mirror-exempt; 7 more)
- 419 archive snapshots in `archive_pub_v1_0` (post-verification cleanup pass)

---

## 10. Workflow protocol — Cowork direct vs Cursor agent vs Logan-touch

When you decide to verify a table, think:

**Decision tree:**
- Pre-Script-N archive snapshot exists for the table → **Cowork-direct** CTC-equivalence (~30 min)
- LLM-extracted from `note_entities_llm_*` upstream → **Cowork-direct** extraction-faithfulness (~30 min)
- Cohort-wide rollup with derivation from verified events → **Cowork-direct** derivation re-derivation (~30 min)
- Has small (<200 row) ambiguous adjudication set → **Cowork builds .xlsx → Logan reviews**
- Has 100% NULL primary key cols / structural gaps / >300 distinct free-text values → **Spawn Cursor agent** (write prompt to `cursor_prompts/`, commit prompt, alert Logan)
- Multi-source UNION canonical (legacy + LLM + synthetic) → **Cowork-direct multi-source pattern** (mig_107)
- Methodology unclear → **Probe + propose to Logan via AskUserQuestion**

**If you spawn a parallel Cursor agent**, document explicit non-overlap zones in the prompt — name the tables Cowork will work in parallel.

---

## 11. Patterns the Cursor lanes will produce (review checklist for new coworker)

When a Cursor lane finishes, review:

1. **GitHub side**: pull origin, find the migration file(s), confirm structure matches established template (mig_102 / mig_110 / mig_114 are good examples)
2. **MotherDuck side**: query `canonical_table_signoff_registry_v1` for the target table — confirm `table_status='verified'`, `n_not_started=0`, signoff_migration populated
3. **Sanity verification**: re-run a sample of Cursor's verification probe against MD; expect 0 drift on cleanly-matched cols
4. **Math reconciliation** (for classifier-style migrations): pre-archive count = post count + DELETE bucket counts + PMH-attribute count
5. **Vocab confirmation**: enum cols (finding_status, evidence_strength, etc.) within expected values
6. **CF list completeness**: are there documented carry-forwards in the close-out doc + table_signoff_registry notes?

If anything looks off, ask Logan before declaring verified.

---

## 12. Recent commit log (for context)

```
3f96cb2  Verify canonical labs family under Protocol v2  (Cursor 8, mig_115)
59fb502  mig_114: canonical_ete_subgrade family complete (28th + 29th canonical)  (Cowork)
284d28a  docs: 3 next cursor prompts (labs family + molecular_genetics_v2 + US v2 family)
da951c5  qc: rebuild pmh patient rollup signoff  (Cursor 7, mig_114_pmh)
360fdb7  mig_111 + 112 + 113: cervical_ln + 2 rollups — close pathology_clinical + cervical_ln_clinical families  (Cowork)
30839db  qc: sign off PSH patient rollup  (Cursor 6, mig_110_psh)
e2441e4  mig_110: canonical_pathology_clinical_events_v1 verified  (Cowork)
ceea5d1  mig_109: cleanliness audit pass — all 5 gates green on 37 verified tables  (Cowork)
5ad0276  qc: sign off complications patient rollup  (Cursor 5, mig_108)
2703153  Verify canonical PMH events under Protocol v2  (Cursor 4, mig_107)
```

---

## 13. Quick links

- [Master verification plan](computer:///Users/ros/THyroid 2026/qc_framework_v1/MASTER_VERIFICATION_PLAN.md)
- [Remaining work inventory](computer:///Users/ros/THyroid 2026/qc_framework_v1/REMAINING_WORK_INVENTORY.md)
- [Verified tables log](computer:///Users/ros/THyroid 2026/qc_framework_v1/VERIFIED_TABLES.md)
- [Cursor prompts dir](computer:///Users/ros/THyroid 2026/cursor_prompts/)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)
- [Today's audit migration (mig_109)](computer:///Users/ros/THyroid 2026/qc_framework_v1/migrations/109_verified_tables_cleanliness_audit_20260429.sql)
- [Most recent extraction-faithfulness template (mig_114 ete_subgrade)](computer:///Users/ros/THyroid 2026/qc_framework_v1/migrations/114_ete_subgrade_family_signoff.sql)
- [Most recent multi-source template (Cursor 4 mig_107 PMH)](computer:///Users/ros/THyroid 2026/qc_framework_v1/migrations/107_pmh_events_table_signoff.sql)

---

End of handoff. Begin with §8 First-action checklist. Once oriented, decide between (a) forge on small Tier 2 in your lane (recurrence/survival/operative_procedure_codes are good candidates) OR (b) write the next 3 Cursor prompts for the next wave (frozen_section_rollup + path_malignant/benign rollups + ete_event_resolved).
