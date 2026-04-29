# Cowork Handoff Prompt v6 — Thyroid Canonical Publication v1.0 Cleanup

Generated: 2026-04-29 (end of day) — supersedes v5
Tip of `origin/main` at handoff: `742bf69` (or later — `git fetch && git pull` first)

---

## §0 TL;DR / first action (do these in order)

You are continuing a multi-week cleanup of the `thyroid_canonical_publication_v1_0` lakehouse on MotherDuck so it can support a manuscript pipeline. The previous session did substantial work. Your job: **execute the apply queue Logan has pre-authorized, generate Cursor (GPT-5.5) prompts for higher-stakes lanes, and pick up direct cleanup tasks while waiting on three in-flight Cursor agent reports**.

**First action checklist — do all of these before any work:**

1. `git fetch origin && git pull --rebase origin main && git log --oneline -25`
2. Read `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-29.md` end-to-end (full session-1 audit + state)
3. Read `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` end-to-end (the 9-step apply queue)
4. Run the 5-gate audit (§13 below); confirm gate panel matches expectation
5. Read `MEMORY.md` index + the high-relevance memories listed in §4
6. Re-read §6 critical reminders below before any `query_rw`

---

## §1 Project mission (brief)

Logan Glosser, Emory thyroid-cancer surgery researcher, is closing out the v1.0 publication lakehouse. The canonical layer (`canonical_*` tables in `main` schema) backs survival/recurrence/outcomes analyses for the manuscript. Your role: orchestrator + verifier + applier. Cursor agents do bulk lane work on Logan's other machine; Logan ratifies decisions and pastes agent summaries to you.

Cleanup sub-goals (mostly hit, a few remaining):
1. Every analytic column registered in `canonical_column_verification_registry_v1` ✓
2. Every column has methodology + batch_id ✓
3. Standardized vocabularies on every analytic column — **mig_168 audit complete; mig_172 normalization apply pending**
4. Old/archived tables removed or properly methodology-tagged ✓ (post-mig_165/167)
5. Patient-level rollups + view layer aligned — **mig_164 pending Cowork apply**
6. Lakehouse passes 5-gate audit — **gate5 closes after mig_160 apply**
7. CFs resolved or explicitly deferred — **session 1 cut backlog roughly in half; mig_136 + mig_154 reconcile still open**

---

## §2 Current state (end of session 1)

| Metric | Value |
|---|---:|
| Latest commit on origin/main | `742bf69` (or later) |
| gate1 | **165** (was 88) |
| gate2 / gate3 / gate4 | 0 / 0 / 0 |
| gate5 | 21 (closes to 0 on mig_160 apply) |
| PM verified cols | 1,441 / 1,598 (90.2%; mig_159 + mig_152 pending) |
| Cohort parity | 10,871 / 10,871 ✓ |
| Distinct CF tags | 135 |

**Migrations committed but not yet applied to MD** (the apply queue):
- mig_159 (PM final residual, 27 cols)
- mig_160 (global date retype, 21 cols × 5 tables)
- mig_161 (mig_155 retro-verify, 31 col notes)
- mig_164 (VIEW layer signoff, 4 views)
- mig_166 (canonical_cleanup_audit_v1 ledger)
- mig_167 (mig_165 retro-verify, notes-only)

**Migration applied to MD without governance** (mig_165, retroactively verified by mig_167): 76 auxiliary tables auto-na'd, 10 deferred CF-only, 1 new tier-1 mirror registered. Net gate1 88→165.

**In-flight Cursor lanes** (Logan will paste summaries when they arrive):
- mig_152 NLP cluster (~116 PM cols)
- mig_169 PM dtype/units audit
- mig_163b HYBRID apply (Cursor authoring SQL after Logan ratified HYBRID)
- mig_170 cross-canonical dtype drift audit

---

## §3 Tools & access

### §3.1 Desktop Commander (preferred for git push)

```
mcp__Desktop_Commander__start_process({command: "zsh", timeout_ms: 5000})
mcp__Desktop_Commander__interact_with_process({pid, input: "cd '/Users/ros/THyroid 2026' && git push origin main"})
```

If process dies between calls, restart (no session continuity). Per `feedback_use_desktop_commander_first.md`: Desktop Commander > Claude in Chrome > computer-use.

### §3.2 GitHub repo

`/Users/ros/THyroid 2026` — mounted as Cowork workspace folder. URL: `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`. Author: `Logan Glosser <logan.glosser@gmail.com>`. Surgical `git add` only — never `-A`.

### §3.3 MotherDuck

- Read-only: `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query`
- Read-write: `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query_rw`
- Primary DB: `thyroid_canonical_publication_v1_0` (main schema; manuscript_workspace schema also has rows)
- Archive DB: `"Thyroid 2026 UPdated"` schema `archive_pub_v1_0`
- Auth: `logan.glosser.eras@gmail.com` MD account (`reference_protocol_v2_md_accounts.md`)
- Quirks: `CURRENT_TIMESTAMP` returns TIMESTAMPTZ — always `CAST(... AS TIMESTAMP)`; FILTER not supported on window funcs; cross-DB FROM forbidden in canonicals
- MCP wrapper: one statement per call

### §3.4 Cursor agents

Logan runs Cursor agents (typically Claude or GPT-5.5) on his other machine. They commit SQL only and push. AGENTS-governance: Cowork applies via Path C after independent verification. Two violations in session 1 (mig_155, mig_165) — flag any new ones.

### §3.5 Auto-memory

`/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`. Read `MEMORY.md` first. ~95 entries.

---

## §4 High-relevance auto-memory entries

- `feedback_motherduck_direct_check.md` — query MD directly every round
- `feedback_clinical_dates_calendar_only.md` — clinical event dates MUST be DATE
- `feedback_extraction_faithfulness_llm_canonical.md`
- `feedback_findings_vs_staging.md`
- `feedback_recurrence_imaging_n_events_null.md`
- `feedback_no_cross_db_canonical_sourcing.md`
- `feedback_alter_view_dependents.md` — CREATE OR REPLACE dependents in same commit
- `feedback_audit_regex_word_boundary.md`
- `feedback_surgical_git_add.md`
- `feedback_use_desktop_commander_first.md`
- `feedback_mention_grain_partition_probe.md`
- `feedback_no_crossdomain_linkage_ids.md`
- `reference_2digit_year_convention.md` — YY → 20YY
- `reference_protocol_v2_md_accounts.md`
- `reference_view_naming_convention.md`
- `reference_canonical_naming_convention.md`
- `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md`
- `project_complications_events_verified_2026-04-28.md`
- `project_invasion_family_signoff_2026-04-28.md`
- `project_path_gland_family_complete_2026-04-28.md`
- `project_op_path_consolidation_script_361_closeout.md`
- `project_lab_consolidation_script_347_closeout.md`

---

## §5 Database architecture (brief)

- **Tier 1**: `note_entities_llm_*` (raw LLM extraction outputs; auto-na exempt)
- **Tier 2**: `canonical_*_events_v1`, `canonical_*_patient_rollup_v1` (typed event-grain + patient-grain)
- **Tier 3**: `canonical_patient_master` (THE master, 1,598 cols, 10,871 patients)
- **VIEWs**: `canonical_<domain>_<grain>_VIEW_v<N>` — must carry `_VIEW` suffix

Registries:
- `canonical_table_signoff_registry_v1` (1 row per canonical_*; status = `not_started`/`in_progress`/`verified`/`failed`)
- `canonical_column_verification_registry_v1` (1 row per col; `verification_status`, `verified_by`, `verified_ts`, `verification_method`, `batch_id`, `notes`)

---

## §6 Critical reminders (READ BEFORE ANY query_rw)

1. **AGENTS governance is binding** — Cursor agents commit SQL only; Cowork applies. Watch for violations.
2. **Cohort-uniformity sweep BOTH directions** on every BOOLEAN flipped — Type-A (T-only) and Type-B (F-only) sneakers ship every round. Use `scripts/_cowork_pm_bool_sweep_batched.py` as template for non-PM tables.
3. **Pre-snapshot before any data mutation** — `"Thyroid 2026 UPdated".archive_pub_v1_0.<table>_pre_mig<N>_<short>_20260429`.
4. **PHI safety** — never print clinical notes; research_id only; no cloud PHI.
5. **Surgical git add** — explicit paths only; never `-A`; never `scripts/output/`.
6. **Verification methods MUST name LIVE `main.*` tables** — pre-check `information_schema.tables`.
7. **Clinical dates MUST be DATE** (`feedback_clinical_dates_calendar_only.md`); audit/provenance timestamps exempt.
8. **Always check MotherDuck directly** — never trust prior summaries.
9. **2-digit year → 20YY** when parsing.
10. **Cross-DB sourcing forbidden** — canonicals are standalone live objects in `main`.

---

## §7 Logan's pre-authorization for this chat

You are pre-authorized to:

✅ Apply the 9-step queue from `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` exactly as drafted. Describe each `query_rw` block in chat before pulling the trigger; pause for explicit "go" only between major steps (1→2→3 OK to chain; before mig_160 step pause for explicit OK; mig_160 → 5 → 6 → 7 OK to chain after that).

✅ Author + apply mig_168b (5 empty-VARCHAR reclass + 3 BOOLEAN sneaker CFs from session 1's findings). Notes-only data already enumerated.

✅ Verify mig_152 / mig_169 / mig_163b / mig_170 against live MD via Path C when Logan pastes their summaries. Apply governance-compliant SQL after verification.

✅ Run further read-only audits across non-PM canonicals (cohort-uniformity sweep, vocabulary audit) using session-1 helper scripts as templates.

✅ Update auto-memory with close-out entries for migrations applied this chat.

🛑 ASK FIRST before:
- Any apply on `canonical_patient_master` data (not registry — actual data writes), with the lone exception of mig_163b once Cursor authors the SQL.
- Any structural ALTER beyond the mig_160 set already vetted.
- Any DELETE on registry rows.
- Any clinical-definition decision (mig_136 days-semantic, mig_172 vocab normalization).

---

## §8 Pending work this chat is expected to handle

### §8.1 Direct cleanup work (do as Logan pastes summaries / approves apply queue)

1. **Apply queue Steps 1-9** from §7 (registry-only first, mig_160 in middle, then mig_164/mig_168b/mig_163b at end)
2. **Verify mig_152 NLP** when its summary arrives — Path C, then apply
3. **Verify + apply mig_169** when read-only summary arrives — opens CF-mig169-* tags
4. **Verify + apply mig_163b** HYBRID (Cursor authors SQL; Cowork applies after Path C)
5. **Verify + apply mig_170** when read-only summary arrives — opens CF-mig170-* tags

### §8.2 Author Cursor (GPT-5.5) prompts for things better done in Cursor/VSC

Save to `cursor_prompts/` and push. Each should be self-contained, ~150-300 lines, with §0 governance + §2 probes + §3 SQL/build pattern + §4 CFs + §5 git workflow + §6 out-of-scope.

Required prompts to author:

1. **mig_171 canonical_us_lymph_node_v2 BUILD** — Tier-2 build. Closes `CF-mig150-TP-UPSTREAM-NOT-IN-MAIN` (9 cols). Heavy lane: design events table (per-LN per-exam grain) + patient_rollup grain. Source: `canonical_us_lymph_node_v1` (if exists) + clinical_notes_long LN extractions + path_malignant LN events. Cursor agent designs + drafts skeleton SQL + verification plan. Logan ratifies before any apply.

2. **mig_172 vocabulary normalization apply** (recurrence_histology + completion family) — apply lane after Logan reviews `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft.csv`. Cursor agent maps raw variants to canonical codes + display labels per the dictionary; touches `recurrence_histology`, `recurrence_histology_v2`, `completion_prior_histology`, `completion_histology_type`, `histologic_types_all`, `histologic_variants_all`, `path_histology_raw`, `path_histology_variant_raw`. Pre-snapshot data; build mapping tables; apply UPDATE; verify post-state distribution. **High clinical-review priority.**

3. **mig_173 syn_*_size_cm dtype reform** — design + apply: `syn_right_lobe_size_cm`, `syn_left_lobe_size_cm`, `syn_isthmus_size_cm`. Currently 3-axis VARCHAR ('4.0 x 3.0 x 2.0'). Decompose to 3 new DOUBLE cols (length_cm, width_cm, height_cm) + computed volume_cc. Sentinel 'n/s' → NULL. Whitespace trim. Pre-snapshot. Add new cols, populate via parser, deprecate old VARCHAR cols (or rename `_size_cm_legacy`).

4. **mig_174 cnln_img_laterality + lateral_levels_v10 + ene_levels_v9 multi-label parser** — token-level normalization. Canonical lateralization enum: `left`, `right`, `bilateral`, `central`, `lateral_neck`. Strip literal `'null'` tokens. Decompose multi-label values (`'left; bilateral'`) into either an array col or per-side BOOLEAN cols. Logan-decision on representation.

5. **mig_175 mig_136 days-semantic adjudication** — 58 cols affected (`CF-mig136-DAYS-SEMANTIC`). Cursor agent profiles the 58 cols, builds 3-option decision package (anchor=event_start / anchor=first_surgery / anchor=LKA), live-counts the impact of each option. Logan ratifies before any apply.

6. **mig_176 dominant_nodule v1/v2 reconcile** — 1,065 patients with mismatch. Profile the mismatch pattern; propose resolution rules (prefer v2 / hybrid / case-by-case). Logan ratifies.

7. **mig_177 mig_154 invasion family PM-vs-events reconcile** — 12 cols (`CF-mig154-PM-VI/CAPSULAR/LVI/PNI-VS-EVENT-PRESENT`). Decide: is the PM legacy-rollup correct, or the canonical_invasion_events_v1 grain, or do both stay (with documentation)?

These prompts should be drafted as part of this chat's deliverables, then pushed for Logan to fire when ready. **Don't wait for the apply queue to finish before authoring these — they can be authored in parallel.**

### §8.3 Optional read-only audits (low priority, fill-time)

If the apply queue is blocked on Logan's input, run any of:
- Cohort-uniformity sweep on non-PM canonicals (e.g., canonical_recurrence_v1, canonical_complications_events_v1)
- Cross-canonical reconciliation probes on important pairs (PM `histology_final` vs canonical_path_malignant_*)
- gate-5 audit refinement for non-canonical_* base tables
- CF backlog inventory by table (which tables carry the most CF density?)

---

## §9 mig_168b — direct cleanup Cowork can do right now (Logan pre-authorized)

### §9.1 What it does

Closes 3 BOOLEAN sneakers (Cowork session-1 finding) + reclasses 5 empty_verified_varchar cols (mig_168 finding). Registry-only writes; no PM data mutation. ~10 query_rw calls.

### §9.2 The 8 cols affected

| col | current status | proposed action | reason |
|---|---|---|---|
| rln_permanent_flag | verified | open CF (keep verified) | 0/10871/0 contradicts comp_rln_injury_confirmed=39 |
| rln_transient_flag | verified | open CF (keep verified) | same lineage |
| nsqip_hypoparathyroidism_recovered_flag | verified | reclass → na | 0/10871/0 vs hypocalcemia mate=80 TRUE; cohort-degenerate |
| biochemical_concern_flag | verified | reclass → na | 0/10871/0; mig_134 deferred Script 224 |
| gm_recurrence_site_primary | verified | reclass → na | 100% NULL; CF-mig156-GM-RECURRENCE-SITE-ALLNULL already noted |
| tsh_suppressed_ever_source | verified | reclass → na | 100% NULL; CF-mig157-TSH-SUPPRESSED-SOURCE-ALL-NULL already noted |
| op_esophageal_inv_first_evidence_text | verified | reclass → na | 100% NULL; new CF |
| nucmed_tgab_max_source | verified | reclass → na | 100% NULL; new CF |
| biochemical_concern_first_date_source | verified | reclass → na | 100% NULL; new CF |

### §9.3 SQL skeleton

```sql
-- mig_168b — Cowork direct cleanup
-- Section A: pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig168b_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig168b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN (
    'rln_permanent_flag','rln_transient_flag',
    'nsqip_hypoparathyroidism_recovered_flag','biochemical_concern_flag',
    'gm_recurrence_site_primary','tsh_suppressed_ever_source',
    'op_esophageal_inv_first_evidence_text','nucmed_tgab_max_source',
    'biochemical_concern_first_date_source'
  );

-- Section B1: Open CF on rln_*_flag (keep verified)
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_168b: CF-mig167-RLN-FLAG-DEGENERATE-VS-COMP-RLN-39-CONFIRMED — '
            || 'rln_permanent_flag and rln_transient_flag both 0 TRUE / 10871 FALSE / 0 NULL while '
            || 'comp_rln_injury_confirmed=39 patients (mig_135 cluster). The extracted_rln_injury_refined_v2 '
            || 'spine appears unpopulated; PM displays the v2 flags but no data has flowed through. '
            || 'Defer to refined_v2 pipeline restoration; keep verified informational.'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN ('rln_permanent_flag','rln_transient_flag');

-- Section B2: Reclass nsqip_hypoparathyroidism_recovered_flag verified→na
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='na',
    verification_method='helper_nsqip_hypopara_recovered_pending_real_extraction',
    verified_by='cowork',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_168b: CF-mig167-NSQIP-HYPOPARA-RECOVERED-DEGENERATE — '
            || '0 TRUE / 10871 FALSE / 0 NULL; mate nsqip_hypocalcemia_recovered_flag has 80 TRUE '
            || 'in identical NSQIP study scope. Reclassified verified→na (placeholder pending real population). '
            || 'Pre-snapshot canonical_column_verification_registry_pre_mig168b_20260429.'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name='nsqip_hypoparathyroidism_recovered_flag';

-- Section B3: Reclass biochemical_concern_flag verified→na
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='na',
    verification_method='helper_biochemical_concern_pending_script_224_landing',
    verified_by='cowork',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_168b: CF-mig167-BIOCHEMICAL-CONCERN-DEFERRED-PLACEHOLDER — '
            || '0 TRUE / 10871 FALSE / 0 NULL; mig_134 marked Script 224 helper "deferred" but col was verified. '
            || 'Reclassified verified→na (placeholder pending Script 224 build). '
            || 'Inconsistent with biochemical_recurrence_flag=128 TRUE (different lineage; recurrence_v1 spine).'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name='biochemical_concern_flag';

-- Section B4: Reclass 5 empty_verified_varchar verified→na
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='na',
    verification_method='helper_empty_varchar_pending_real_extraction',
    verified_by='cowork',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_168b: CF-mig168b-EMPTY-VERIFIED-VARCHAR-RECLASS-NA — '
            || 'col has 0 non-null / 10871 NULL across cohort; reclassified verified→na (placeholder). '
            || 'See mig_168 audit (qc_framework_v1/reports/mig_168_pm_controlled_vocab_audit_20260429.md).'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN (
    'gm_recurrence_site_primary',
    'tsh_suppressed_ever_source',
    'op_esophageal_inv_first_evidence_text',
    'nucmed_tgab_max_source',
    'biochemical_concern_first_date_source'
  );

-- Section C: Resync canonical_table_signoff_registry_v1 for canonical_patient_master
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes           = COALESCE(ts.notes, '') || ' | mig_168b: 7 cols verified→na (Cowork session-1 sneakers + mig_168 empty-VARCHAR finds).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) AS subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;
```

After mig_168b apply, PM should be 1,441 verified / **20** na (was 13; +7 new) / 137 not_started / 0 failed / 1,598 total.

### §9.4 Order of operations

mig_168b should run AFTER mig_159 (so PM not_started has been drained from 144 to 117) but BEFORE mig_152 NLP lands (so the PM resync in mig_168b §C doesn't conflict with mig_152's resync). Easiest spot: between Step 3 (mig_159) and Step 4 (mig_160) of the apply queue.

---

## §10 Standing reference: 5-gate audit query

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

Expected at handoff: **165 / 0 / 0 / 0 / 21**. After full apply queue: **170+ / 0 / 0 / 0 / 0**.

---

## §11 Decision tree for the new Cowork

```
START
  │
  ├─→ Run §0 first-action checklist
  │
  ├─→ Are any of mig_152 / 169 / 163b / 170 summaries pasted yet?
  │     ├─ YES → Path-C verify the new ones first; apply if compliant
  │     └─ NO  → continue
  │
  ├─→ Has Logan said "go" on the apply queue?
  │     ├─ YES → execute Steps 1-3 (mig_161 + mig_161b + mig_159)
  │     │         pause for "go" before Step 4 (mig_160 structural ALTERs)
  │     │         then chain Steps 5-9
  │     └─ NO  → ask explicitly; in the meantime author the 7 new Cursor prompts (§8.2)
  │
  ├─→ Author the 7 Cursor prompts in §8.2 to cursor_prompts/, commit + push
  │
  ├─→ When apply queue is done, apply mig_168b (§9)
  │
  ├─→ When mig_152 / 169 / 163b / 170 summaries arrive, Path-C verify each, apply if clean
  │
  └─→ Update auto-memory close-outs for everything applied this chat
```

---

## §12 Verbatim opening message (paste into the new Cowork chat)

---

Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v6.md` end-to-end before any tool use. Then run the §0 first-action checklist (git pull, read session summary + apply queue plan, run 5-gate audit, read MEMORY.md).

Standing context: I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're cleaning up the `thyroid_canonical_publication_v1_0` lakehouse on MotherDuck so it's ready for manuscript-grade statistical analysis. Previous Cowork session (2026-04-29 day) made substantial progress — gate1 went 88→165, 14 commits landed, and a 9-step apply queue was authored.

Currently in flight (Cursor agents working — verify when summaries arrive; do NOT touch these clusters yourself):

1. mig_152 NLP cluster (~116 PM cols) — older lane, summary may arrive separately
2. mig_169 PM dtype/units audit — read-only; opens CF-mig169-DTYPE-* tags
3. mig_163b HYBRID apply (Cursor authoring SQL after I ratified HYBRID) — UPDATE PM.any_recurrence_flag
4. mig_170 cross-canonical dtype drift audit — read-only; opens CF-mig170-DTYPE-DRIFT-* tags

**Your pre-authorization** (per §7 of v6 doc):
- Apply the 9-step queue from `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md`. Describe each query_rw block in chat first; pause for "go" before Step 4 (mig_160 structural ALTERs); chain everything else.
- Apply mig_168b (registry-only, 9 affected cols) after Step 3.
- Verify + apply 152/169/163b/170 when their summaries arrive.
- Run further read-only audits (cohort-uniformity sweep on non-PM canonicals, cross-canonical reconcile probes).
- Update auto-memory.

**Author 7 Cursor (GPT-5.5) prompts** for higher-stakes lanes that benefit from Cursor's repo-aware context (§8.2 of v6 doc):
1. mig_171 canonical_us_lymph_node_v2 BUILD
2. mig_172 vocabulary normalization apply (recurrence_histology + completion family)
3. mig_173 syn_*_size_cm 3-axis dtype reform
4. mig_174 cnln_img_laterality multi-label parser
5. mig_175 mig_136 days-semantic adjudication (58 cols)
6. mig_176 dominant_nodule v1/v2 reconcile (1,065 mismatches)
7. mig_177 mig_154 invasion family PM-vs-events reconcile (12 cols)

Save these to `cursor_prompts/`, commit + push. Don't wait for the apply queue to finish — author them in parallel.

**First task**: Start with the §0 first-action checklist. After git pull + 5-gate audit + MEMORY read, decide what to work on first based on the §11 decision tree:
- (A) If 152/169/163b/170 summaries are already pasted, verify those first
- (B) Otherwise: I'll give you "go" on the apply queue when ready; meanwhile author the 7 Cursor prompts and start direct cleanup work

**Critical rigor reminders** (from prior rounds — every batch shipped with sneakers Cowork had to clean up):
- Cohort-uniformity sweep BOTH directions on every BOOLEAN flipped
- Verification methods must name LIVE main.* tables (information_schema.tables pre-check)
- Clinical event dates must be DATE not TIMESTAMP (mig_160 closes pending retypes)
- 2-digit year → 20YY
- Pre-snapshot every data mutation to archive_pub_v1_0
- Surgical git add (no -A)
- Always check MotherDuck directly — never trust prior summaries

I'll paste the agent summaries from mig_152, mig_169, mig_163b, mig_170 separately as they come in. Verify each against live MD per Path C and apply if AGENTS-governance was respected.

End of opening message.
