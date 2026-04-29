# Cowork Handoff Prompt v8 — Thyroid Canonical Publication v1.0 Cleanup

Generated: 2026-04-29 (very late evening) — supersedes v7
Tip of `origin/main` at handoff: `2eed41b` (verify with `git fetch && git log --oneline -5`)

---

## §0 TL;DR / first actions

You are continuing a multi-week cleanup of `thyroid_canonical_publication_v1_0` (MotherDuck) toward a manuscript-grade publication lakehouse. **gate1 = 169 verified canonicals; PM = 1,461 / 1,598 (91.4%) verified.** A Cursor agent is currently working on `mig_179` (canonical_invasion_events_v1 LVI extractor rebuild — see §9). Multiple Cursor lanes applied to MotherDuck during the prior session WITHOUT Cowork Path-C governance — full retroactive verification list in §8.

**Read in this order before any tool use:**

1. This handoff doc (§0–§14) end-to-end
2. `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v7.md` (predecessor; partial overlap)
3. `exports/mig176_177_174_review_20260429/README.md` — critical raw-source LVI bug finding (events extractor combined-CAP miss)
4. `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` — apply queue (mostly drained; 1-2 stragglers)
5. `MEMORY.md` index + the high-relevance memories listed in §4

**Then run the §11 first-action checklist, then decide A/B/C from §13.**

---

## §1 Project mission

Logan Glosser, Emory thyroid-cancer surgery researcher. Database: `thyroid_canonical_publication_v1_0` on MotherDuck (account `logan.glosser.eras@gmail.com`). Goal: produce a clean, audit-passing canonical layer ready for manuscript-grade survival/recurrence/outcomes analyses. Cohort: **10,871 distinct research_id**. Backbone: `canonical_patient_master` (1,598 cols, 1,461 verified). Tier-2 events / patient_rollup canonicals: 169 verified (gate1).

**You are: orchestrator + verifier + applier.** Cursor agents do bulk lane work on Logan's other machine. Logan ratifies decisions and pastes agent summaries to you. You verify against live MD per Path C, apply if AGENTS-governance was respected, and ship b-cleanup migrations for any agent QA misses or governance violations.

---

## §2 Current state

| Metric | Value |
|---|---:|
| Latest origin/main tip | `2eed41b` (mig_177 LVI raw-source review + events rebuild prompt) |
| **gate1 (verified canonicals)** | **169** |
| gate2 / gate3 / gate4 | **0 / 0 / 0** ✓ |
| gate5 (date retype violators) | 21 (closes to 0 on mig_160 apply — STILL PENDING) |
| **PM verified cols** | **1,461 / 1,598 (91.4%)** |
| PM not_started | 117 (mig_152 NLP cluster covers most) |
| PM na | 20 |
| Cohort parity | 10,871 / 10,871 ✓ |
| Distinct CF tags | ~150 |

### §2.1 Apply queue drained this round (Cowork-direct):
- ✅ mig_161 (mig_155 retro Path-C verify, registry notes-only)
- ✅ mig_161b (Cowork ATA-INITIAL-DUP CF gap closure)
- ✅ mig_159 (PM final residual; 27 cols not_started → verified)
- ✅ mig_166 (canonical_cleanup_audit_v1 ledger signoff; 15 verified + 3 na)
- ✅ mig_167 (mig_165 retro Path-C verify; notes-only on 1,290 col-rows)
- ✅ mig_164 (4 VIEW signoff — but Cursor-authored had a gap; only 2 US VIEWs flipped)
- ✅ mig_164b (Cowork-authored gap closure for 2 molecular UNNEST VIEWs; gate1 167→169)
- ✅ mig_168b (5 empty-VARCHAR reclass + 3 BOOLEAN sneaker CFs)
- ✅ mig_175b (CF-mig136-DAYS-SEMANTIC closed; 58 col-impact)

### §2.2 Cursor-applied without Cowork Path-C governance (NEED RETRO VERIFICATION):
- ⚠️ **mig_178** (histology vocab cleanup) — commit `19e2972`. Cursor "Executed MotherDuck mig_178 cleanup" + rebuilt `histologic_types_all` / `histologic_variants_all` from canonical_path_malignant_events_v1. Rejected `mtc_ptc_mixed`. Cleaned rid 2168 → `MTC | PTC` and rid 3331 → `MTC | PTC`. Materialized audit tables with 0 uniformity failures. **Cowork must Path-C-verify the data writes + audit tables.**
- ⚠️ **mig_173b** (syn size_cm dtype reform) — commit `84ee91e`. Cursor "Executed mig_173b against MotherDuck". Added 15 new typed cols (right/left/isthmus length/width/height/volume_cc/parse_status); preserved legacy as `_legacy_raw`. Cohort parity 10,871/10,871 ✓ at end. 29 large-volume rectangular plausibility-review items retained. **Cowork must Path-C-verify schema additions + data populated correctly.**
- ⚠️ **mig_163b** (HYBRID any_recurrence_flag apply) — commit `91d436a`. Cursor "Applied 163b...sql"; verified 514 TRUE / 10,357 FALSE / 0 NULL. Cowork live spot-check confirms 514/10357/0 ✓ + 1 mig_163b registry row. **Cowork should still Path-C-verify pre-snapshot lineage + appendix CF text.**

### §2.3 Cursor-authored awaiting Cowork apply (governance-compliant):
- ✅ **mig_171b** (canonical_us_lymph_node_v2 build DRAFT) — commit `123cebb`. Read-only preview: 6,973 events / 4,110 patients / 159 fallback exam IDs. Awaiting Logan ratification before any data write. **Closes 159 col-impact (CF-117 triplet × 53).**

### §2.4 Cursor in flight (pasted prompt; output expected next session):
- 🔄 **mig_179 / canonical_invasion_events_v1 LVI extractor rebuild** — prompt at `cursor_prompts/CURSOR_PROMPT_mig177_events_rebuild_20260429.md`. Drives the events-table fix that unblocks mig_177b. Six source patterns: combined CAP "Lymph-Vascular Invasion: Present", angiolymphatic, lymphangitic, separate-field newer-CAP, quantitative `<N per 2mm2`, vocab typos. **CRITICAL — verify event-rebuild correctness before mig_177b proceeds.**

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

- **Path:** `/Users/ros/THyroid 2026` (mounted as Cowork workspace folder)
- **URL:** `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`
- **Branch:** `main` tracked to `origin/main`
- **Tip at handoff:** `2eed41b`
- **Author:** `Logan Glosser <logan.glosser@gmail.com>` for all commits
- **Surgical git add ONLY**: explicit paths; never `-A` or `scripts/output/`. Lint Python before commit if `.py` changed.

### §3.3 MotherDuck

- **Tools:** `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query` (read-only), `_query_rw` (writes — requires explicit user-approval semantics; ask before use on data-mutation queries)
- **Primary DB:** `thyroid_canonical_publication_v1_0` (live publication, MD account `logan.glosser.eras@gmail.com`)
- **Archive DB:** `"Thyroid 2026 UPdated"` schema `archive_pub_v1_0` (pre-snapshots BEFORE any mutating UPDATE/ALTER)
- **DuckDB quirks:** `CURRENT_TIMESTAMP` is TIMESTAMPTZ → cast to TIMESTAMP; FILTER not allowed on window funcs; cross-DB `FROM` in canonicals forbidden (`main.*` only); `ALTER COLUMN ... SET DATA TYPE T USING <expr>` works for in-place retypes.
- **MCP wrapper:** one statement per call — do NOT use `BEGIN TRANSACTION;` / `COMMIT;`.

### §3.4 Cursor agents

- Logan runs Cursor agents on his other machine to author bulk SQL.
- **AGENTS governance:** agents commit SQL only; **Cowork applies via Path C after independent Path-C verification.** Recent violations: mig_178/173b/163b — Cowork retroactively verifies (§2.2 + §8).
- New Cursor prompts are dropped into `cursor_prompts/` directory; Cursor agents pull these and execute their assigned scope.

### §3.5 Auto-memory

- **Path:** `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`
- **Index:** `MEMORY.md` (~150 entries; index lines after 200 may be truncated)
- Always read before deciding; updates persist across sessions.

---

## §4 Reference documents

### §4.1 In repo (`/Users/ros/THyroid 2026`)

| Path | What it is |
|---|---|
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v8.md` | This doc |
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v7.md` | Predecessor (partial overlap) |
| `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-29.md` | Chat-1 + chat-2 audit (mostly stale; consult v8 first) |
| `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` | Apply queue plan (mostly drained) |
| `qc_framework_v1/migrations/*.sql` | All migration SQL |
| `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` | Latest 5-gate audit query |
| `qc_framework_v1/reports/*.md` | Read-only design + audit reports |
| `cursor_prompts/CURSOR_PROMPT_*.md` | Cursor agent prompts (~90 files) |
| **`exports/mig176_177_174_review_20260429/README.md`** | **CRITICAL — events extractor LVI bug finding + per-bucket call** |
| `exports/mig176_177_174_review_20260429/mig177_lvi_316_full_evidence_PART1_rollup_only.csv` | 120 rollup-only patients with LVI evidence + per-pt recommendation |
| `exports/mig176_177_174_review_20260429/mig176_us_reports_raw_19_extreme_outliers.csv` | Raw US Reports for 19 extreme nodule outliers |
| `cursor_prompts/CURSOR_PROMPT_mig177_events_rebuild_20260429.md` | **ACTIVE Cursor lane (mig_179)** |
| `cursor_prompts/CURSOR_PROMPT_mig174b_apply_per_side_boolean_20260429.md` | mig_174b prompt (apply pending) |
| `exports/mig168_pm_vocab_audit_20260429_175417/RATIFICATION_NOTES_20260429.md` | Histology vocab rules (mig_178 reference) |

### §4.2 Auto-memory key files (read first)

**Methodology / pattern memories:**
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
- `reference_2digit_year_convention.md` — 20YY rule (Logan-ratified 2026-04-27)
- `reference_protocol_v2_md_accounts.md` — MD accounts (.eras for MCP, may differ for local CLI)
- `reference_synoptic_row_ix.md` — synoptic_row_ix is Script 108 pandas-load-order
- `reference_view_naming_convention.md` — `_VIEW` suffix required
- `reference_canonical_naming_convention.md` — Tier-2 masters naming

**Project memories — recent close-outs:**
- `project_mig_175b_days_semantic_closeout.md` (CF-mig136-DAYS-SEMANTIC closed)
- `project_invasion_family_signoff_2026-04-28.md` — 7/184 tables / 226 cols verified through mig_94b
- `project_motherduck_pro_trial_plan.md` — Pro trial 4-item plan

---

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

---

## §6 Workflow: Cowork ↔ Cursor ↔ Logan

### §6.1 Roles

- **Logan**: clinical-domain expert; ratifies clinical decisions; pastes agent summaries; runs Cursor agents on his other machine.
- **Cursor agents** (per AGENTS protocol): bulk SQL authors; commit + push to GitHub but do NOT write to MD. **Recent violation pattern: mig_178/173b/163b applied directly to MD — retro Path-C verification required (§8).**
- **Cowork (you)**: orchestrator + verifier + applier + small-fix author. Run Path-C verification on all Cursor work directly against live MD. Catch governance violations and shortfalls. Apply registry-only / low-risk lanes directly. Author Cursor prompts for heavier work.

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
7. **Cross-canonical reconciliation** for cols with multiple SSOTs
8. **Pre-snapshot** affected registry rows + any data-write tables to `archive_pub_v1_0`
9. **Apply** via query_rw (block-by-block due to MCP wrapper)
10. **Verify post-state**: math, signoff resync, 5-gate audit
11. **Author + apply b-cleanup** for any agent-QA misses
12. **Write traceability SQL**, commit + push

### §6.3 When to apply directly vs ask Logan

- **Apply directly:** registry-only Cowork-authored, single-col retype with full preservation probe, focused data-write with clear rule, Path-C-compliant Cursor SQL where Logan has already ratified the design
- **Ask Logan:** cross-canonical reconciles affecting >50 patients with clinical adjudication needed, structural schema changes, **clinical definition disputes**, anything that requires picking between options (R1/R2/R3, D3 sub-rules, etc.)

### §6.4 Pre-snapshot rule

ALWAYS pre-snapshot before mutating verified canonicals:
```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_<table>_pre_mig<N>_<short>_20260429 AS
SELECT research_id, <affected cols>, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig<N>_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_<table>;
```

---

## §7 Currently-clean / verified state per domain

### §7.1 Closed CFs (post-this-session)

- **CF-mig136-DAYS-SEMANTIC** (58 col-impact, top-1 in chat-2 backlog) — closed by mig_175b at `51a94ae`
- **CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT** — closed by mig_163b HYBRID apply at `91d436a` (514 TRUE)
- **CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES** etc. — partially closed by mig_178 (Cowork must verify)
- **CF-mig172-MTC-PTC-MIXED-REJECT** — Logan-ratified rejection; mig_178 implemented

### §7.2 Top open CFs after this session

| CF | Col-impact | Closes via |
|---|---:|---|
| CF-117-US-NODULE-RANGE / EXAM-ID-PORTABILITY / LATERALITY-RAW | 53 each | **mig_171b** US LN v2 build (Cursor pending Logan ratify) |
| CF-GEN07-ROM-OCR | 41 | requires raw extraction redo |
| CF-90-DATE-FORMAT | 38 | **mig_160 apply** (still pending) |
| CF-87-AJCC | 36 | TBD |
| CF-100-DATE-RETYPE | 29 | **mig_160 apply** |
| CF-mig137-PM-MOL-DATE-RETYPE | 25 | **mig_160 apply** |
| **CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS** | 91+ | **mig_179 in-flight Cursor** (rebuild events) |
| **CF-mig177-EVENTS-LYMPHATIC_PRESENT_SEPARATE_MISS** | unknown count | **mig_179** |
| **CF-mig177-PM-VASC-ALIAS-LVI** | 196 | **mig_177b** (after mig_179 lands) |
| **CF-mig176b-V2-UPSTREAM-EXTRACTION-INFLATION-19PTS** | 19 | informational; future canonical_us_nodule_v2 rebuild |
| CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT | 12 | **mig_177b** |
| CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT | 2 | **mig_176b** (Cowork-direct ready to author) |

### §7.3 PM verified column status

- 1,461 verified / 20 na / 117 not_started / 0 failed / 1,598 total / `in_progress`
- The 117 not_started are mostly NLP-cluster cols that mig_152 covers (~116 cols)

---

## §8 ⚠️ Retroactive Path-C verification required (governance violations)

**These three Cursor lanes applied to MotherDuck WITHOUT Cowork Path-C governance.** Verify each thoroughly before considering them closed:

### §8.1 mig_178 histology vocab cleanup (commit `19e2972`)

Cursor claim: rejected `mtc_ptc_mixed`; rebuilt `histologic_types_all` / `histologic_variants_all` from canonical_path_malignant_events_v1; cleaned rid 2168 + 3331 to "MTC | PTC"; 0 uniformity failures.

**Verify:**
1. `SELECT COUNT(*) FROM main.canonical_patient_master WHERE histologic_types_all ILIKE '%mtc_ptc_mixed%'` — expect 0
2. Spot-check rid 2168 + 3331 actual cell values
3. Check `histologic_types_all` ordering — should be sorted-distinct (per mig_178 prompt requirement)
4. Run cross-table histology vocab uniformity audit per `RATIFICATION_NOTES_20260429.md` §6
5. Confirm pre-snapshot exists in `archive_pub_v1_0` and contains the original values
6. Check registry: any rows touching the 4 enum cols (recurrence_histology + variants) should have a mig_178 note appended

### §8.2 mig_173b syn size_cm dtype reform (commit `84ee91e`)

Cursor claim: 15 new typed cols added (right/left/isthmus length/width/height/volume_cc/parse_status); legacy preserved; parse coverage right 96.69%, left 96.44%, isthmus 92.46%; 18/18 registry rows; 1 provenance row.

**Verify:**
1. `information_schema.columns` confirms 15 new cols + 3 legacy_raw cols on canonical_patient_master ✓ (Cowork already partially confirmed: `syn_*_size_cm_legacy_raw`, `syn_*_volume_cc`, `syn_*_size_parse_status` exist)
2. Pre-snapshot exists in `archive_pub_v1_0`
3. Spot-check 5 random patients with multi-axis values: parse correctness
4. Cohort-uniformity sweep on `syn_*_size_parse_status` (3 cols) — Type-A vs Type-B classification
5. Verify the 29 large-volume plausibility-review items aren't manuscript-blocking
6. Registry rows: all 15 new cols + 3 legacy rows should have mig_173b notes

### §8.3 mig_163b HYBRID any_recurrence_flag (commit `91d436a`)

Cursor claim: 514 TRUE / 10,357 FALSE / 0 NULL; zero mismatches vs HYBRID; 2 archive snapshots; registry note present.

**Verify (Cowork already partially probed):**
1. ✓ Live MD: 514 / 10,357 / 0 — confirmed exact match
2. ✓ Registry note exists on any_recurrence_flag (1 mig_163b row found)
3. Verify HYBRID definition was implemented: `(canonical_recurrence_v1.recurrence_confirmed=TRUE) ∪ (canonical_recurrence_resolved_v1.recurrence_status_final='path_proven')`
4. Confirm CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT closure note is present
5. Verify both archive snapshots present in `archive_pub_v1_0`

---

## §9 ⚠️ ACTIVE CURSOR LANE — mig_179 events rebuild

**Status:** Cursor is actively working on `cursor_prompts/CURSOR_PROMPT_mig177_events_rebuild_20260429.md`. Logan started this lane after Cowork's raw-source review of 316 ambiguous LVI patients revealed the `canonical_invasion_events_v1` build is missing legitimate `lymphatic_microscopic` event rows for at least 91+ patients.

### §9.1 The bug being fixed

The CAP synoptic template's combined field `"Lymph-Vascular Invasion: Present"` is being parsed as `vascular_microscopic` ONLY. Newer separate-field `"Lymphatic Invasion: Present"` is also being missed entirely. Six concrete patterns (see `cursor_prompts/CURSOR_PROMPT_mig177_events_rebuild_20260429.md` §Required scope):

1. Combined CAP "Lymph-Vascular Invasion: Present"
2. Older "Angiolymphatic invasion: Yes/Present"
3. "Lymphangitic invasion present"
4. Newer separate-field "Lymphatic Invasion: Present"
5. Quantitative "< N per 2mm2"
6. Vocab typos (foacl, extrensive, indeterminent, c/a, X)

### §9.2 Verification approach when mig_179 lands

This is a **STRUCTURAL DATA WRITE** to canonical_invasion_events_v1 — high risk. Path C requirements:

1. Pre-snapshot to `archive_pub_v1_0.canonical_invasion_events_v1_pre_mig179_20260429`
2. Verify pattern coverage by sampling all 6 pattern types from the original 91 NO_EVENT_ROWS rollup-only patients (rid list in `mig177_lvi_316_full_evidence_PART1_rollup_only.csv`)
3. Spot-check vascular_microscopic counts UNCHANGED (no regression on existing axis)
4. Check `lymphatic_microscopic` PRESENT count: was 1,233 events / 780 patients pre-mig_179; expect to grow by ~300-500 patients minimum
5. Other axes (capsular, perineural) MUST be unchanged
6. Trigger `canonical_invasion_patient_rollup_v1` rebuild from new events (per mig_179 §E)
7. Verify rollup `any_lymphatic_microscopic_anywhere` count grows correspondingly

After mig_179 lands and verifies clean, mig_177b becomes unblocked (PM `lvi_*` rederive from corrected events).

---

## §10 Apply queue (priority order)

After §11 first-action checklist, work through in this order:

1. **Retroactive Path-C verify** mig_178 + mig_173b + mig_163b per §8 — emit b-cleanup migrations for any QA misses
2. **Verify mig_179 when it lands** per §9.2; apply via Path C; trigger rollup rebuild
3. **Author + apply mig_177b** — PM `lvi_*` rederive from refreshed events (Logan-ratified per-bucket call in `exports/.../README.md`)
4. **Author + apply mig_176b** — `dominant_nodule_size_cm_resolved = COALESCE(v1, v2)` + audit col (Cowork-direct, ~6 query_rw calls)
5. **Apply mig_171b** — canonical_us_lymph_node_v2 BUILD (Cursor SQL committed at `123cebb`; needs Logan ratification before any data write)
6. **Apply mig_174b** — when Cursor agent picks up the prompt and lands SQL (token-level parser for cnln_img_laterality)
7. **Apply mig_152** (PM NLP cluster, ~116 cols) — when Cursor agent picks it up
8. **Apply mig_160** (global date retype, 21 cols × 5 tables) — STRUCTURAL; closes ~190 col-impact CFs and gate5 21→0
9. **Apply mig_172** (vocab normalization apply) — post-mig_178; rewrite ratified CSV to remove mtc_ptc_mixed first
10. **Apply mig_162** (PM finalization + lakehouse coverage report) — runs LAST after all PM not_started cleared

---

## §11 First-action checklist

```
1. git fetch origin && git pull --rebase origin main && git log --oneline -25
2. Run §14 5-gate audit (expect gate1=169 / 0 / 0 / 0 / 21)
3. Check PM batch progress + recent registry activity:
     SELECT n_verified, n_na, n_not_started, table_status
     FROM main.canonical_table_signoff_registry_v1
     WHERE table_name='canonical_patient_master';
     -- Expect: 1461 / 20 / 117 / in_progress
4. Check active in-flight lanes (Cursor activity):
     SELECT batch_id, COUNT(*) AS n, MAX(verified_ts)
     FROM main.canonical_column_verification_registry_v1
     WHERE verified_ts > '2026-04-29 22:00:00'
     GROUP BY 1 ORDER BY 3 DESC;
5. Read MEMORY.md end-to-end (auto-memory index)
6. Read this v8 handoff doc end-to-end
7. Read exports/mig176_177_174_review_20260429/README.md (events extractor LVI bug — critical context)
8. Read cursor_prompts/CURSOR_PROMPT_mig177_events_rebuild_20260429.md (mig_179 in-flight)
9. Re-read §6.2 Path C protocol + §8 retroactive verification list + §9 mig_179 verification approach
10. Decide A/B/C from §13
```

---

## §12 Critical reminders

**Verify all Cursor work directly and thoroughly.** Cursor agents have produced shortcuts that needed cleanup in EVERY round. Specific patterns to watch (from prior sessions):

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
| mig_172 | mtc_ptc_mixed canonical_code Logan rejects (clinical reversal) | When Logan flags clinical concerns, treat as ratification reversal |
| mig_174a | Multi-label fields with literal 'null' token, casing/whitespace drift, embedded newlines | Token-level enumeration before any parser design |
| **mig_177** | **Events extractor combined-CAP miss; rollup is right; 91+ patients undocumented LVI** | **Source-text review is the only way to catch extractor bugs at this scale** |
| **mig_178/173b/163b** | **Cursor applied directly to MD without Cowork Path C** | **Retroactive verification mandatory; treat all Cursor "applied" claims as suspect until Path-C-verified** |

**Standing rules:**
- Cohort parity 10,871 invariant
- Run cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions
- Pre-check `information_schema.tables` for every methodology string
- Audit `data_type` for every numeric measurement col
- Check `*_date` cols are DATE not TIMESTAMP/VARCHAR
- Check MotherDuck directly every round — never trust prior summaries

---

## §13 Decision tree

After §11 first-action checklist, decide:

**A. mig_179 has landed (Cursor pasted summary)** → **verify it** via §9.2 Path C and apply if clean. Apply b-cleanup for any agent QA misses. Then unblock mig_177b.

**B. mig_178 / 173b / 163b retroactive Path-C audit not done** → **do that first** per §8. Emit b-cleanup migrations as needed.

**C. All retro audits done + mig_179 not yet landed** → **author mig_176b apply lane** (Cowork-direct, ~6 query_rw calls; R2 ratified; opens `_resolved` + `_resolution_rule` cols). Or generate next Cursor prompts for mig_174b apply, mig_152 PM NLP cluster, mig_172 vocab apply (post-mig_178).

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

Expected at handoff: **gate1=169, gate2=0, gate3=0, gate4=0, gate5=21**. After mig_160 apply: gate5=0.

---

## §15 Verbatim opening message to paste into the new Cowork chat

---

> Please read `/Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v8.md` end-to-end before any tool use. Then read in order:
> 1. `exports/mig176_177_174_review_20260429/README.md` — critical events extractor LVI bug finding + per-bucket call (drives the active mig_179 Cursor lane)
> 2. `exports/mig176_177_174_review_20260429/mig177_lvi_316_full_evidence_PART1_rollup_only.csv` — 120 rollup-only LVI patients with raw-source evidence + per-pt KEEP/FLIP recommendation
> 3. `cursor_prompts/CURSOR_PROMPT_mig177_events_rebuild_20260429.md` — the active Cursor lane (mig_179)
> 4. `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` — apply queue (mostly drained)
> 5. `MEMORY.md` (auto-memory index)
>
> Then run the §11 first-action checklist (git fetch + log, 5-gate audit, PM batch progress, active lane status). Then decide A/B/C from §13.
>
> **Standing context:** I'm Logan Glosser, thyroid cancer surgery researcher at Emory. We're closing out the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`) for manuscript-grade analyses. **gate1 is at 169 / PM at 1,461 / 1,598 (91.4%) verified.** You're the orchestrator + verifier + applier; Cursor agents do the bulk lane work; I'm the final ratifier.
>
> **You have:**
> - **Desktop Commander MCP** for git/shell on my actual Mac
> - **MotherDuck MCP** (read + write against `thyroid_canonical_publication_v1_0` and `"Thyroid 2026 UPdated".archive_pub_v1_0`)
> - **GitHub repo** at `/Users/ros/THyroid 2026` (tip `2eed41b`; URL `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`)
> - **Auto-memory** at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/` with ~150 entries
>
> **CURRENTLY IN FLIGHT (Cursor working on it — verify when summary lands):**
> - **mig_179** canonical_invasion_events_v1 LVI extractor rebuild — STRUCTURAL DATA WRITE; high risk; six source patterns (combined CAP, angiolymphatic, lymphangitic, separate-field newer-CAP, quantitative `<N per 2mm2`, vocab typos). Verification approach in §9.2.
>
> **CRITICAL — RETROACTIVE PATH-C VERIFICATION REQUIRED (§8):**
> Three Cursor lanes applied to MD without Cowork governance during prior session. Verify each thoroughly:
> - **mig_178** histology vocab cleanup (commit `19e2972`) — rebuilt histologic_types_all + variants + cleaned rid 2168/3331
> - **mig_173b** syn size_cm dtype reform (commit `84ee91e`) — added 15 typed cols + legacy preserve
> - **mig_163b** HYBRID any_recurrence_flag (commit `91d436a`) — 514/10357/0; partially Cowork-verified
>
> **READY-TO-AUTHOR Cowork-direct lanes:**
> - **mig_176b** dominant_nodule R2 apply — `COALESCE(v1, v2)` + audit col; ~6 query_rw calls; Logan-ratified
> - **mig_177b** PM `lvi_*` rederive — pending mig_179 events rebuild completion
>
> **Awaiting Logan ratification (don't touch yet):**
> - **mig_171b** canonical_us_lymph_node_v2 BUILD — Cursor SQL committed at `123cebb`
> - **mig_174b** cnln_img_laterality per-side BOOLEAN apply — Cursor prompt at `cursor_prompts/CURSOR_PROMPT_mig174b_apply_per_side_boolean_20260429.md`
>
> **Critical rigor reminder:** verify all Cursor work directly against MotherDuck. EVERY round has shipped with agent QA misses Cowork had to clean up. Be skeptical of every "verified clean" Cursor claim. Run cohort-uniformity sweep on EVERY BOOLEAN, BOTH directions. Pre-check `information_schema.tables` for every methodology string. Audit `data_type` for every numeric measurement col. Check date-type for every `*_date` col. **The mig_177 events bug was caught only because Cowork pulled the raw path_synoptics text and reviewed source-by-source — events tables can be wrong even when "schema-clean".**
>
> **First task:** §11 first-action checklist. Then choose A/B/C from §13:
> - **(A)** If mig_179 has landed, verify via Path C §9.2
> - **(B)** Run §8 retroactive Path-C audits on mig_178/173b/163b
> - **(C)** Author mig_176b apply or generate next Cursor prompts
>
> I'll paste agent summaries from the in-flight Cursor runs separately as they come in — verify each against live MD per Path C and apply if AGENTS-governance was respected.

---

End of handoff doc.
