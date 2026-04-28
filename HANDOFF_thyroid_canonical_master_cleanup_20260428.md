# Cowork Continuation — `thyroid_canonical_publication_v1_0` Master Cleanup

**Generated:** 2026-04-28
**For:** New Cowork chat (parallel session while Cursor agent runs path_gland repair)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `ce3e1bf` — `docs: cursor prompt for path_gland repair`

> **Read this entire prompt before any tool use.** Then follow the **First-action checklist** at the bottom.

---

## 1. Project goal

Verify and clean every table in **`thyroid_canonical_publication_v1_0`** under Protocol v2. Final outcome:

1. All canonical tables `table_status='verified'` in `main.canonical_table_signoff_registry_v1`.
2. All columns flagged `verified` or `na` in `main.canonical_column_verification_registry_v1` (zero `not_started`, zero `failed`).
3. Standardized values on every analytic column (controlled vocabularies for status / type / modality / kind / temporal_class).
4. Old / archived tables and columns removed when no longer load-bearing (~419 archive tables in `archive_pub_v1_0`; many can be dropped post-verification).
5. Patient-level rollups + view layer aligned with verified events tables.

---

## 2. Accounts + access

- **MotherDuck:** account `logan.glosser.eras@gmail.com` (the `.eras` account hosts `thyroid_canonical_publication_v1_0`). The Cowork MotherDuck MCP is authed to `.eras` directly. Local duckdb-py via Desktop Commander uses the SA token in `motherduck.local.toml` (already configured; same `.eras` account).
- **GitHub:** `https://github.com/ry86pkqf74-rgb/THYROID_2026.git` — single-branch `main`. Desktop Commander has push access via local credentials (occasionally `.git/index.lock` gets stuck; remove with `rm -f .git/index.lock .git/HEAD.lock` before commits).
- **Desktop Commander:** available for terminal actions, file edits, git ops, Python REPL. Logan **prefers** Desktop Commander over sandbox `mcp__workspace__bash` because the workspace mount has stale-lock issues with git.
- **Working folder:** `/Users/ros/THyroid 2026` (the user's selected folder). Auto-memory is at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/MEMORY.md` and individual memory files in same dir.

---

## 3. Reference documents

### In repo (`/Users/ros/THyroid 2026/`)

| Path | Purpose |
|---|---|
| `qc_framework_v1/MASTER_VERIFICATION_PLAN.md` | Master plan (consult before strategy decisions) |
| `qc_framework_v1/REMAINING_WORK_INVENTORY.md` | Inventory of remaining tables (Tier 1 events / Tier 2 derived / Tier 4-5 patient_master + raw mirrors) |
| `qc_framework_v1/VERIFICATION_PROGRESS.md` | Progress dashboard |
| `qc_framework_v1/VERIFIED_TABLES.md` | Verified-tables log |
| `qc_framework_v1/migrations/` | Per-mig manifests (`98a_*.md` … `98d_*.md`, etc.) |
| `qc_framework_v1/scripts/build_*_review.py` | Builders (chyle, voice_nerve, seroma) |
| `qc_framework_v1/scripts/apply_mig_98*_decisions.py` | Apply scripts (98b chyle, 98c voice_nerve, 98d seroma, 98e hematoma, 98f hypopt, 98gh) |
| `cursor_prompts/CURSOR_PROMPT_path_gland_repair_20260428.md` | Active Cursor agent task (running in parallel — see §6) |
| `motherduck.local.toml` | Pre-configured `.eras` SA token (gitignored) |
| `motherduck_client.py` | MD connection helper used by all scripts |
| `verification_csvs/canonical_complications_events_v1/` | Workbooks + decision JSONs (gitignored except force-added Logan-curated copies) |

### In MotherDuck (`thyroid_canonical_publication_v1_0`)

| Object | Purpose |
|---|---|
| `main.canonical_table_signoff_registry_v1` | Per-table verification status; updated when a table is signed off |
| `main.canonical_column_verification_registry_v1` | Per-column verification status; updated as columns are verified |
| `main.canonical_*_events_v1` | Tier 1 events tables (verification scope) |
| `main.canonical_*_patient_rollup_v1` | Tier 1 patient-level rollups |
| `manuscript_workspace.cpm_reconciliation_provenance_v1` | Provenance row per applied migration |
| `archive_pub_v1_0` schema (in `"Thyroid 2026 UPdated"` DB) | Pre-Script-N snapshot tables for CTC-equivalence verification |

### In auto-memory (Cowork-only)

Index: `MEMORY.md`. Most relevant:

- `feedback_motherduck_direct_check.md` — re-query MD before recommending state changes; don't trust prior summaries
- `feedback_no_cross_db_canonical_sourcing.md` — never `FROM archive_pub_v1_0.*` at build time
- `feedback_findings_vs_staging.md` — staging cols follow anatomic findings, never override
- `feedback_invasion_orphan_clinical_rules.md` — 6-rule clinical adjudication library
- `feedback_surgical_git_add.md` — never `git add -A`; explicit paths only
- `feedback_commit_workflow.md` — always stage/commit/push; lint Python first
- `feedback_review_csv_formatting.md` — openpyxl + .xlsx, NOT csv.QUOTE_ALL
- `feedback_phi_safety.md` — never print clinical notes; research_id only
- `reference_protocol_v2_md_accounts.md` — `.eras` account hosts publication DB
- `reference_synoptic_row_ix.md` — Script-108 pandas-load-order; never synthesize via ROW_NUMBER (inherit OK)
- `reference_view_naming_convention.md` — main.* VIEW must carry `_VIEW` in name
- `reference_canonical_naming_convention.md` — `canonical_<domain>_events_v1` / `canonical_<domain>_patient_rollup_v1`
- `reference_2digit_year_convention.md` — all YY → 20YY (Logan-ratified 2026-04-27)
- `reference_duckdb_timestamp_tz.md` — always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts
- `feedback_alter_view_dependents.md` — `ALTER VIEW RENAME TO` is catalog-only; CREATE OR REPLACE dependents
- `feedback_mention_grain_partition_probe.md` — probe COUNT(*) vs COUNT(DISTINCT key) before ROW_NUMBER on mention tables
- `project_ctc_equivalence_verification_pattern.md` — CTC pattern for Script-N SELECT*+filter+UPDATE chains
- `project_complications_events_verified_2026-04-28.md` — most recent close-out (8 sub-migs done)

---

## 4. Verified tables (17 of 184; 432 of 5,502 cols)

| Table | Cols verified | Methodology | Migration |
|---|---|---|---|
| `canonical_fna_events_v1` | 38 | mechanical_source + d2s recompute | mig_78 → 96 |
| `canonical_airway_invasion_events_v1` | 23 | manual_source_review + findings-vs-staging | mig_83 |
| `canonical_path_malignant_events_v1` | 56 | CTC-equivalence + Script-rule re-run | mig_87 → 89 |
| `canonical_operative_events_v1` | 54 | CTC-equivalence single-migration | mig_90 |
| `canonical_t4b_invasion_events_v1` | 19 | per-finding Logan review + default-not | mig_92 |
| `canonical_esophageal_invasion_events_v1` | 15 | per-finding Logan review | mig_93 |
| `canonical_vascular_invasion_events_v1` | 22 | per-finding Logan review + extent backfill | mig_94 / 94b |
| `canonical_invasion_events_v1` | 11 verified + 9 na | CTC-equivalence on UNION canonical + orphan review | mig_91 / 91b → 95 |
| `canonical_path_benign_events_v1` | 51 verified + 4 na | NLP-flag audit + structural repair + specimen_master inherit | mig_97 / 97b |
| `canonical_*_invasion_patient_rollup_v1` (×4) | 47 + 11 na | derivation re-derivation against verified events | mig_95 |
| `canonical_fna_patient_rollup_v1` | 18 + 2 na | mig_95b |
| `canonical_operative_patient_rollup_v1` | 19 + 3 na | mig_95b |
| **`canonical_complications_events_v1`** | **15 + 4 na** | **8 sub-migs (98a-h) — bulk priority-rule + classifier** | **mig_98a-h + mig_99** ← closed 2026-04-28 |

Plus mig_98 close-outs landed **246 rows** in `canonical_pmh_events_v1` for non-operative complication attributions (`is_preexisting=TRUE`).

---

## 5. Verification methodology library (5 patterns)

1. **CTC-equivalence verification** — for canonicals built via Script-N SELECT*+filter+UPDATE chains OR UNION pipelines. Pre-Script-N archive snapshot in `archive_pub_v1_0` is the value-source-of-truth. One mass-equivalence query covers dozens of cols. (mig_87 / mig_90 / mig_91.)
2. **Script-rule re-run** — for post-build UPDATE-derived cols, re-execute UPDATE logic as SELECT and compare. (mig_88 / mig_90b.)
3. **Derivation re-derivation against verified upstream** — for rollups. >99% match acceptable. (mig_95 / mig_95b.)
4. **Per-finding Logan review with rule-based pre-filter** — for LLM-output canonicals. Apply 6-12 rule clinical pre-filter, surface ambiguous in batches of 20 .xlsx for ACCEPT/FLIP/REJECT. (mig_80→83 / mig_91 / mig_92 / mig_93 / mig_94.)
5. **Note-text REAL/TEMPLATE classifier on `clinical_notes_long`** — pull all entity mentions, classify each ~500-char context window REAL (treatment vocabulary) vs TEMPLATE (consent risk lists, prophylactic, negation), bulk priority-rule disposition (Logan-ratified for mig_98c onward). (mig_98b chyle / 98c voice_nerve / 98d seroma / 98e hematoma / 98f hypopt.)

**Date-based attribution rule (Logan-ratified, generalizable):**
- `timing_days < 0` or `timing_window='pre_surgery'` → PREEXISTING → PMH
- `0–30` → OPERATIVE → KEEP
- `31–180` → POSTOP_LATE → KEEP
- `181–365` → POSTOP_VERY_LATE → KEEP defensive
- `1–5y` → POSSIBLY_PRIOR_OP → PMH
- `>5y` → NOT_OPERATIVE → PMH

**PMH row template** (for moving non-op real findings out of complications):
```sql
INSERT INTO canonical_pmh_events_v1 (
  research_id, source_table, source_row_id, source_note_type,
  finding_text, finding_value, finding_value_norm,
  finding_date, mention_note_date, finding_status, evidence_strength,
  is_preexisting, anchor_source, evidence_span_hash, build_ts
) VALUES (
  '<rid>', 'mig_<X>_pmh_synthetic', <hash_key>, 'other_history',
  '<rationale>', '<entity>', '<entity>',
  CAST('<earliest_dt>' AS DATE), CAST('<earliest_dt>' AS DATE),
  'present', 'definitive', TRUE,
  'mig_<X>_classifier_logan_curated', sha256('<rationale>'),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
);
```

---

## 6. ⚠️ Active parallel work — Cursor agent on `canonical_path_gland_events_v1`

A Cursor agent is currently running on path_gland repair. **Don't pick path_gland or its related tables** as your next task to avoid conflict.

**What it's doing** (per `cursor_prompts/CURSOR_PROMPT_path_gland_repair_20260428.md`):

1. Investigation: find original path_gland builder, locate Script 108 synoptic_row_ix anchor, probe `path_synoptics` for width/depth source fields
2. Repair script (likely `scripts/397_*_path_gland_repair.py`): snapshot → JOIN-backfill `synoptic_row_ix` from `path_synoptics` → populate `gland_width_cm` / `gland_depth_cm` → repair `surgery_episode_id` 21% gap → parse 700 distinct free-text `parag_<N>_location` values into Logan's 7-value parathyroid taxonomy:
   - `right_superior` / `right_inferior` / `left_superior` / `left_inferior` / `intrathyroidal_right` / `intrathyroidal_left` / `extrathyroidal_other`
3. Re-verify post-repair, flip column registry, commit + push

**When the agent finishes:** it'll commit + push to origin/main. Watch for a new commit message like `"feat: path_gland repair — synoptic_row_ix backfilled / parathyroid taxonomy normalized"`. Re-verify on MD before signing off.

**Don't touch these tables / files while Cursor runs:**
- `main.canonical_path_gland_events_v1` (target of repair)
- `main.canonical_path_gland_patient_rollup_v1`
- `main.path_synoptics` (source — read-only OK, but no writes)
- `main.canonical_column_verification_registry_v1` rows for `canonical_path_gland_events_v1`
- `main.canonical_table_signoff_registry_v1` row for `canonical_path_gland_events_v1`
- `scripts/397_*` and adjacent (Cursor will likely use 397+)

---

## 7. Open work (priority queue, post-mig_98 + path_gland)

### Tier 1 events tables remaining (~7 tables)

| Table | Cols / Rows | Likely methodology |
|---|---|---|
| `canonical_frozen_section_events_v1` | 31 / 7,081 | CTC-equivalence (Script 360 closed; pre-360 snapshot exists) |
| `canonical_parathyroid_events_v1` | 25 / 8,697 | TBD — paired with hypoparathyroidism (verified) |
| `canonical_pmh_events_v1` | 19 / 12,690 (incl 246 new mig_98 rows) | Derivation re-derivation + sample audit |
| `canonical_psh_events_v1` | 19 / 3,919 | Sample audit + mechanical inheritance |
| `canonical_medications_events_v1` | 19 / 7,501 | Note-text REAL/TEMPLATE classifier |
| `canonical_pathology_clinical_events_v1` | 15 / 13,358 | TBD |
| `canonical_cervical_ln_clinical_events_v1` | 15 / 4,493 | TBD |

### Tier 2 derived canonicals (~16 tables)

Labs (Tg, calcium, PTH, TSH, vitamin D), molecular, recurrence, ETE adjudication, survival, etc. See `qc_framework_v1/REMAINING_WORK_INVENTORY.md` table 5b.

### Tier 4 / 5 (~120 tables)

- `canonical_patient_master` (1,592 cols) — auto-derivable cascade from verified events
- 12 raw mirror sources (path_synoptics 311 cols, manuscript_cohort_v1 151 cols, etc.) — sample-based verification
- 91 helpers in manuscript_workspace + 17 note_entities_*

### Cleanup

- `archive_pub_v1_0` has 419 archive snapshots; many redundant post-verification (drop pre-emptively risks losing snapshots Cursor still needs).

---

## 8. Workflow patterns (Cowork vs Cursor agent vs Logan review)

### Per-round protocol

1. **Re-query MD for current state before recommending changes** (memory: `feedback_motherduck_direct_check.md`). Don't trust prior summaries.
2. **Identify methodology** from §5 library.
3. **Apply the right tool tier:**
   - **Bulk-apply directly via `query_rw`** — for clean derivation checks, mass-equivalence, no clinical judgment.
   - **Build .xlsx review for Logan** — when clinical judgment is needed (`feedback_review_csv_formatting.md` — openpyxl + .xlsx, never csv.QUOTE_ALL).
   - **Spawn Cursor agent** — for substantial repairs (>1 hour of work, multi-file edits, complex SQL). Pattern from `cursor_prompts/CURSOR_PROMPT_path_gland_repair_20260428.md`.
4. **Verify post-state on MD** directly before declaring complete.
5. **Commit + push** — explicit-path `git add` only (memory: `feedback_surgical_git_add.md`); lint Python first; author `Logan Glosser <logan.glosser@gmail.com>`.
6. **Save memory** if reusable pattern emerges.

### Logan-touch tasks
- Per-row clinical judgment on .xlsx review files
- Approve/modify proposed bulk dispositions
- Resolve gray-zone date-attribution edges
- Resolve ambiguous decision vocab (e.g. INTENTIONAL_SACRIFICE vs CANCER_RELATED)

### Cowork-direct (no Logan needed)
- State probing
- Mechanical CTC-equivalence verification
- Bulk priority-rule application (after Logan ratifies the rule once — currently ratified for mig_98 family)
- Registry flips after all-cols-verified
- Memory writes / docs / commits

### Cursor agent (parallel)
- Long repair migrations (path_gland-style)
- Multi-file refactors
- Substantial new-script authoring with iterative testing

---

## 9. Standing reminders

- Confirm in MD before recommending: don't trust prior summaries; re-query.
- Execute directly when appropriate: clean derivation checks / mass-equivalences with no clinical judgment, run via `query_rw`.
- 0 failed CFs target: every CF resolves to either `verified` (with explanation) or `na` (auto-skip with rationale). 'failed' should be transient.
- PHI rule: never print clinical notes; research_id only; review .xlsx files in `verification_csvs/` are gitignored except Logan-reviewed copies which can be force-added.
- Lint Python with `python3 -m py_compile` before commit.
- DuckDB `CURRENT_TIMESTAMP` returns TIMESTAMPTZ — always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols.
- VIEW names must carry `_VIEW` suffix in `main.*` (memory: `reference_view_naming_convention.md`).

---

## 10. First-action checklist

When you start, do **ALL** of the following before touching anything:

1. **Verify MD access:**
   ```sql
   SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1
   WHERE table_status='verified';
   ```
   Should return **17** (or 18 if Cursor's path_gland repair landed).
2. **Confirm tip of origin/main:**
   ```bash
   cd "/Users/ros/THyroid 2026" && git log --oneline -5
   ```
   Tip should be `ce3e1bf` (or newer if Cursor agent has pushed). Pull if remote is ahead.
3. **Check whether Cursor agent has finished path_gland:**
   ```sql
   SELECT signoff_migration, table_status FROM main.canonical_table_signoff_registry_v1
   WHERE table_name = 'canonical_path_gland_events_v1';
   ```
   - If `table_status='verified'` and `signoff_migration='path_gland_repair_*'` → Cursor finished. Do post-flight verification and notify Logan.
   - If still `not_started` → Cursor still working. Pick a different table.
4. **Survey Tier 1 events table options** — query column-registry NULL rates for each candidate to spot early data-quality issues:
   ```sql
   WITH t AS (
     SELECT 'canonical_frozen_section_events_v1' AS t UNION ALL
     SELECT 'canonical_parathyroid_events_v1' UNION ALL
     SELECT 'canonical_pmh_events_v1' UNION ALL
     SELECT 'canonical_psh_events_v1' UNION ALL
     SELECT 'canonical_medications_events_v1' UNION ALL
     SELECT 'canonical_pathology_clinical_events_v1' UNION ALL
     SELECT 'canonical_cervical_ln_clinical_events_v1'
   )
   SELECT r.table_name, r.verification_status, COUNT(*) AS n_cols
   FROM main.canonical_column_verification_registry_v1 r
   JOIN t ON r.table_name = t.t
   GROUP BY 1, 2
   ORDER BY 1, 2;
   ```
5. **Read this prompt + the 4 most relevant memory files**:
   - `feedback_motherduck_direct_check.md`
   - `project_complications_events_verified_2026-04-28.md`
   - `project_ctc_equivalence_verification_pattern.md`
   - `feedback_surgical_git_add.md`
6. **Propose a next-table choice to Logan** with `AskUserQuestion`: include 2-4 candidates from Tier 1 with proposed methodology + estimated effort (CTC-direct = quick / Logan review = medium / Cursor repair = high).

---

## 11. Decision tree — start direct or spawn parallel Cursor agent?

After step #6 above, evaluate the chosen next table:

| Signal | Path |
|---|---|
| Pre-Script-N archive snapshot exists for the table | **Cowork-direct CTC-equivalence** (fast, ~30 min) |
| Cols mostly populated, clean derivation chains | **Cowork-direct Script-rule re-run** |
| Has a small (<200 row) ambiguous adjudication set | **Build .xlsx → Logan review** |
| Has 100% NULL primary key cols / structural gaps / >300 distinct free-text values to normalize | **Spawn second Cursor agent** (write prompt to `cursor_prompts/CURSOR_PROMPT_<table>_repair_<date>.md`); commit prompt; alert Logan |
| Methodology unclear | **Probe state + propose to Logan via AskUserQuestion** |

If you spawn a parallel Cursor agent, document **explicit non-overlap zones** with the active path_gland agent (different tables, different scripts, no shared DDL changes).

---

## 12. Recent commit log (for context)

```
ce3e1bf  docs: cursor prompt for path_gland repair (post-verification probe findings)
cbccd4a  mig_98g+h: hypocalcemia_clinical + mortality finalized — ALL 8 SUB-MIGS DONE
01247c6  mig_98f: hypoparathyroidism applied — 298 KEEP / 88 PMH / 39 DELETE (bulk)
f376ca9  mig_98e: hematoma applied — 70 KEEP / 18 PMH / 81 DELETE (bulk)
22d3fd1  mig_98d: seroma applied — 45 KEEP / 84 PMH / 744 DELETE (bulk)
cb5e200  mig_98c: voice/nerve applied — 28 KEEP / 40 PMH / 27 DELETE / 635 no_signal cleaned
a4f0cf0  mig_98b: chyle_leak applied — 5 kept / 1 PMH / 1,570 deleted
29d6c10  feat(qc): mig_98b v2 chyle_leak real-candidates classifier
3702232  feat(qc): mig_98b builder for chyle_leak Logan review workbook
96412da  docs: continuation prompt for next Cowork session (post-mig_98a + 16 tables verified)
```

---

## 13. Quick links

- [Cursor prompt — path_gland repair (active)](computer:///Users/ros/THyroid 2026/cursor_prompts/CURSOR_PROMPT_path_gland_repair_20260428.md)
- [Master verification plan](computer:///Users/ros/THyroid 2026/qc_framework_v1/MASTER_VERIFICATION_PLAN.md)
- [Remaining work inventory](computer:///Users/ros/THyroid 2026/qc_framework_v1/REMAINING_WORK_INVENTORY.md)
- [Verified tables log](computer:///Users/ros/THyroid 2026/qc_framework_v1/VERIFIED_TABLES.md)
- [Verification progress dashboard](computer:///Users/ros/THyroid 2026/qc_framework_v1/VERIFICATION_PROGRESS.md)
- [mig_98 manifests dir](computer:///Users/ros/THyroid 2026/qc_framework_v1/migrations/)
- [Latest mig_98 close-out (98c voice/nerve)](computer:///Users/ros/THyroid 2026/qc_framework_v1/migrations/98c_mig_voice_nerve_apply.md)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

**End of handoff. Begin with First-action checklist (§10).**
