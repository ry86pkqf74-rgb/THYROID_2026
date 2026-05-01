# Cowork Handoff Prompt v18 — Thyroid Canonical Publication v1.0

**Generated:** 2026-05-01 by Cowork mid-round (Wave 1 + mig_239 + mig_240 + mig_242 closed; mig_241 + mig_243 + mig_244 dispatched to agents)
**Tip of `origin/main` at write:** `c2a7b5f` — `feat(qc): mig_242 — semantic_publication.vw_frozen_section_safe_VIEW_v1`
**Supersedes:** v16 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md`
**Companion:** v17 batch at `cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md`

---

## §0 — First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v18.md` end-to-end before any tool use.
>
> **Then run the §3 first-action checklist:**
> 1. `git fetch origin && git log --oneline -15` from `/Users/loganglosser/THYROID_2026` (note: previous handoffs reference `/Users/ros/THyroid 2026` — that path is stale; the live repo is at `/Users/loganglosser/THYROID_2026`)
> 2. `SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;` for instant lakehouse health
> 3. Reconcile actual `gate1_verified_tables` against the §3 expected-state matrix (213 / 214 / 216 / 217 / 218 depending on which of mig_241/243/244 landed)
> 4. For every lane that landed since v18 was written, run the Path-C verification probe in §3
>
> **Standing context:** I'm Logan Glosser, Emory thyroid-cancer surgery researcher. We're cleaning up the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`). The v16 round (manuscript readiness verdict: READY) closed clean. The v17 round of cleanup migrations is in flight — based on a ChatGPT cleanup audit that Cowork verified live, then split into Wave 1 (Cowork-direct, mig_236/237/238) + Wave 2 + Wave 3 (agent dispatch, mig_239–244). Wave 1 + mig_239/240/242 closed in the previous chat; mig_241 / mig_243 / mig_244 were dispatched to agents at handoff.
>
> **You have:** Desktop Commander MCP for git/shell on my Mac (FileVault — `.git/index.lock` cleanup may be needed); MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; GitHub repo at `/Users/loganglosser/THYROID_2026`. **Use Desktop Commander for git ops.**
>
> **First task after §3 checklist:** Path-C verify whichever of mig_241/mig_243/mig_244 has landed since this doc was written, then prompt me with next steps from §5 decision menu (close v17 round → start manuscript drafting OR launch v19 round if any new gaps surface).

---

## §1 — Round delta v17 vs v16

| Metric | v16 final | v17 expected (after all 6 land) | Δ |
|---|---:|---:|---:|
| 5-gate gate1 (verified tables) | 211 | **218** | +1 mig_236 update (no gate1 chg) +1 mig_238 +1 mig_240 +1 mig_242 +1 mig_243 +3 mig_241 +1 mig_244 — wait: mig_236 was UPDATE (no gate1 chg), mig_237 was COMMENTs (no gate1 chg). Net additions: mig_238 (+1, 211→212 — wait original was 210, mig_238 made it 211; then v17 adds 7 more = 218). Reconcile: mig_239 (+0, CREATE OR REPLACE), mig_240 (+1), mig_241 (+3), mig_242 (+1), mig_243 (+1), mig_244 (+1). 211 + 0 + 1 + 3 + 1 + 1 + 1 = **218** |
| 5-gate gates 2–5 | 0/0/0/0 | 0/0/0/0 | unchanged |
| Cohort parity | 10871/10871/10871 | 10871/10871/10871 | unchanged |
| `verified_main_objects_missing_comment` | 28 (silent — not yet measured) | **0** | mig_237 closed governance gap |
| `semantic_publication` view count (excl. release_manifest) | 8 | **15** | +mig_238 +mig_240 +mig_241×3 +mig_242 +mig_243 +mig_244 |
| research_id type heterogeneity (semantic) | 3 numeric / 5 VARCHAR | **0 numeric / 8 VARCHAR** | mig_239 cast |
| col_registry duplicate keys | 166 (silent) | **0** | mig_239 §F dedup |
| Manuscript readiness | READY | READY ↑ | cleaner semantic layer + curated bridge view |

---

## §2 — Lanes already closed (verified at v18 write)

| Mig | Lane | Agent | Commit | Verified |
|---|---|---|---|---|
| mig_236 | Registry refresh — dedup VIEW 65→66 cols | Cowork-direct | `e9a1e02` | ✓ Cowork |
| mig_237 | Table-comment refresh (28 missing + 2 stale) | Cowork-direct | `9b584b9` | ✓ Cowork |
| mig_238 | `vw_publication_qc_status_VIEW_v1` (31-col superset of mig_233) | Cowork-direct | `b08432b` | ✓ Cowork |
| mig_239 | research_id VARCHAR cast in 3 semantic views + col_registry dedup of 166 dup keys | Cowork-direct | `6fc6f89` | ✓ Cowork |
| mig_240 | `vw_us_exam_safe_VIEW_v1` (25 cols, 11,880 rows) | Cline Sonnet 4.6 | `e0d3471` | ✓ Cowork (Path-C) |
| mig_242 | `vw_frozen_section_safe_VIEW_v1` (10 cols, 4,116 rows) | Cursor Composer | `c2a7b5f` | ✓ Cowork (Path-C) |

Live state at v18 write: **gate1=213**, gates 2-5=0, cohort_parity=TRUE, gov_gap=0.

---

## §3 — First-action checklist for new chat

### Step 3.1 — Confirm git state via Desktop Commander

```bash
cd "/Users/loganglosser/THYROID_2026"
git fetch origin
git log --oneline -15
git status --porcelain
```

Expect HEAD ≥ `c2a7b5f`. New commits past that point are likely `mig_241`, `mig_243`, `mig_244` from the agent dispatches — exact commit hashes depend on what's finished.

### Step 3.2 — One-query lakehouse health

```sql
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
```

Reconcile `gate1_verified_tables` against expected:

| If gate1 = | Then which agents have landed |
|---:|---|
| 213 | none of mig_241/243/244 yet (state at v18 write) |
| 214 | one of: mig_243 (1 view) or mig_244 (1 view) |
| 215 | mig_243 + mig_244, OR mig_241 partially landed (rare) |
| 216 | mig_241 alone (3 views), OR (mig_243 + mig_244) which = 215 — so 216 = mig_241 alone |
| 217 | mig_241 + (mig_243 OR mig_244) |
| **218** | all three landed (target — round complete) |

`cohort_parity_ok` MUST stay TRUE; gates 2-5 MUST stay 0; `verified_main_objects_missing_comment` MUST stay 0. If any regresses, surface to Logan immediately.

### Step 3.3 — Identify new views in semantic_publication

```sql
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'semantic_publication'
ORDER BY table_name;
```

At v18 write, semantic_publication has these views (8 from v16 + 4 from this round = 12 total + release_manifest_v1):
- `release_manifest_v1` (BASE TABLE)
- `vw_cohort_membership_safe_VIEW_v1` (mig_223 + mig_239 cast)
- `vw_fna_safe_VIEW_v1` (mig_223)
- `vw_frozen_section_safe_VIEW_v1` (mig_242 — NEW this round)
- `vw_labs_long_safe_VIEW_v1` (mig_223)
- `vw_molecular_safe_VIEW_v1` (mig_223)
- `vw_path_malignant_tumor_safe_VIEW_v1` (mig_223 + mig_239 cast)
- `vw_patient_master_safe_VIEW_v1` (mig_223)
- `vw_publication_qc_status_VIEW_v1` (mig_238 — NEW this round)
- `vw_recurrence_safe_VIEW_v1` (mig_223)
- `vw_us_exam_safe_VIEW_v1` (mig_240 — NEW this round)
- `vw_us_nodule_safe_VIEW_v1` (mig_223 + mig_239 cast)

Expect new arrivals from mig_241/243/244:
- `vw_ln_patient_safe_VIEW_v1` (mig_241; row count 4,008)
- `vw_ln_surgery_safe_VIEW_v1` (mig_241; row count 4,008)
- `vw_ln_histology_attribution_safe_VIEW_v1` (mig_241; row count 5,918)
- `vw_snake_case_aliases_VIEW_v1` (mig_243; row count 10,871) — agent may have chosen Option A (single view) or Option B (alias columns embedded in existing safe views — if B, no new view name; check via `git log` to see what landed)
- `vw_patient_domain_wide_safe_VIEW_v1` (mig_244; row count 10,871; 30–60 cols)

### Step 3.4 — Path-C verify each landed lane

For each lane that landed since v18 write:

```sql
-- Replace <view_name> + <expected_rows> per the table above

SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1
   WHERE schema_name='semantic_publication' AND table_name='<view_name>') AS signoff_rows,
  (SELECT n_columns_total FROM main.canonical_table_signoff_registry_v1
   WHERE schema_name='semantic_publication' AND table_name='<view_name>') AS signoff_n_total,
  (SELECT n_verified FROM main.canonical_table_signoff_registry_v1
   WHERE schema_name='semantic_publication' AND table_name='<view_name>') AS signoff_n_verified,
  (SELECT signoff_migration FROM main.canonical_table_signoff_registry_v1
   WHERE schema_name='semantic_publication' AND table_name='<view_name>') AS signoff_mig,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
   WHERE schema_name='semantic_publication' AND table_name='<view_name>') AS col_registry_rows,
  (SELECT COUNT(*) FROM information_schema.columns
   WHERE table_schema='semantic_publication' AND table_name='<view_name>') AS physical_cols,
  (SELECT COUNT(*) FROM semantic_publication.<view_name>) AS row_count;
```

Acceptance gates per lane:
- `signoff_rows` = 1
- `signoff_n_total` = `signoff_n_verified` = `col_registry_rows` = `physical_cols`
- `row_count` matches §3.3 expected
- `signoff_mig` references the correct migration SQL file

If anything mismatches, surface to Logan with hypothesis + propose remediation mig.

### Step 3.5 — Spot-check research_id type uniformity (mig_241 + mig_244 specifically)

```sql
SELECT table_name, data_type
FROM information_schema.columns
WHERE table_schema = 'semantic_publication'
  AND LOWER(column_name) = 'research_id'
ORDER BY table_name;
```

After all of mig_241/243/244 land, this should return ALL VARCHAR — including the new ones. If any new view introduces numeric research_id, that's a regression and the agent missed v17 batch §3 / §6 acceptance.

---

## §4 — Lanes in flight at v18 write

| Mig | Lane | Agent | Status |
|---|---|---|---|
| mig_241 | LN safe-view promotion (3 views) — `vw_ln_patient_safe_VIEW_v1`, `vw_ln_surgery_safe_VIEW_v1`, `vw_ln_histology_attribution_safe_VIEW_v1` | Cline Sonnet 4.6 (same window as mig_240) | dispatched after mig_240 commit |
| mig_243 | snake_case alias view for 17 nonstandard cols (full list in v17 batch §5; ChatGPT said 15 — incomplete) | Copilot GPT-5.5 OR Cline GPT-5.5 (Logan's call) | dispatched in parallel with Step 2 |
| mig_244 | curated `vw_patient_domain_wide_safe_VIEW_v1` (30-60 cols, 10,871 rows) | Cursor Composer (same window as mig_242) | dispatched after mig_239 + mig_242 committed |

**Dispatch order rationale:**
- mig_239 (research_id VARCHAR) BLOCKED mig_244 — landed first
- mig_240 (us_exam_safe) and mig_242 (frozen_safe) and mig_243 (aliases) ran in parallel — zero overlap
- mig_241 (LN promotion) requires same Cline window as mig_240 — sequenced after mig_240
- mig_244 (patient_domain_wide) requires Cursor window freed (mig_242 done) AND mig_239 done — final lane

---

## §5 — Decision menu (post-verify)

After §3 first-action checklist, choose:

- **(A) v17 round closed clean (all 6 lanes verified PASS)** → close round: write `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md` summarizing the round; refresh v17 batch doc with closeout state; OPTIONALLY refresh `parquet_export/pub_v1_0_20260430/` mirror if any of mig_240/241/242/243/244 changed table contents (none should — all are new VIEWs over existing data); bump handoff to v19.
- **(B) Some lanes failed Path-C verification** → surface to Logan with hypothesis + propose remediation mig per `feedback_remediation_pattern.md`.
- **(C) All clean and Logan wants to start manuscript drafting** → orient toward Lane M outputs:
  - `manuscript_outputs/v1_0_20260501/` (Tables 1–5 + cohort flow CSVs)
  - `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` (90-line Methods draft)
  - `semantic_publication.vw_*_safe_VIEW_v1` (read-path SSOT, now type-stable + with the new us_exam/frozen/LN/aliases/patient_domain_wide surfaces)
  - `parquet_export/pub_v1_0_20260430/` (offline reproducibility mirror)
- **(D) New lane needed** → write a new Cursor/Cline lane prompt to `cursor_prompts/`, document non-overlap zones, surface to Logan with agent assignment recommendation.
- **(E) Future H — Power BI marts begin** → ratify trigger with Logan; if green-light, write the multi-day Cursor Composer prompt for `bi_powerbi.*` star-schema build.

---

## §6 — Path-C verification protocol (mandatory for agent-applied lanes)

Same as v16 §7. After every commit landing while you watch:

1. **Probe live MD** for the agent's `batch_id`:
   ```sql
   SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE batch_id LIKE '<agent_batch_id_pattern>';
   ```
2. **Verify acceptance criteria** from the v17 batch prompt (row counts, view existence, flag presence)
3. **Re-run mig_233 dashboard** — confirm gate1 grew correctly + gates 2-5 stay 0:
   ```sql
   SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
   ```
4. **Re-run mig_238 publication QC view** — superset health:
   ```sql
   SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
   ```
5. **Cohort parity** — should stay 10,871 / 10,871 / 10,871
6. **For mutating lanes** (none expected this round — all are CREATE VIEW): confirm pre-snapshot exists
7. **If clean**: nothing for Cowork to do — agent already committed
8. **If issues**: surface to Logan with hypothesis + propose remediation

---

## §7 — Repo + tooling reminders

- **Workspace path:** `/Users/loganglosser/THYROID_2026` (NOT `/Users/ros/THyroid 2026` — that's stale templating from older handoff docs)
- **Surgical git add per `feedback_surgical_git_add.md`**: never `git add -A` or directory-wide; explicit paths only
- **Always commit + push per `feedback_commit_workflow.md`**: stage → commit → push; lint Python first
- **PHI safety per `feedback_phi_safety.md`**: never print clinical notes; research_id only
- **Desktop Commander > bash sandbox per `feedback_use_desktop_commander_first.md`**: bash sandbox can't unlink `.git/index.lock` (FileVault); use Desktop Commander for git ops
- **DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ** per `reference_duckdb_timestamp_tz.md`: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols
- **No cross-DB canonical sourcing** per `feedback_no_cross_db_canonical_sourcing.md`: canonicals are standalone live objects in `main`; never `FROM archive_pub_v1_0.*` at build time
- **VIEW naming** per `reference_view_naming_convention.md`: any `main.*` or `manuscript_workspace.*` or `semantic_publication.*` VIEW must carry `_VIEW_v1` suffix
- **research_id is now VARCHAR everywhere in `semantic_publication.*`** (mig_239) — agents must `CAST(research_id AS VARCHAR)` in any new safe view
- **col_registry dedup is fresh** (mig_239 §F removed 166 dup keys from mig_223/224) — any new col registry insert should be a clean single row per (schema, table, column); no need to "DELETE then INSERT" idempotent pattern unless agent is re-applying

---

## §8 — Reference object inventory (post-v17, post-mig_242)

### `semantic_publication` schema (publication-tier analyst SSOT)
- `release_manifest_v1` — BASE TABLE, 1 row (`pub_v1_0_20260430`)
- `vw_publication_qc_status_VIEW_v1` (mig_238) — single-row 31-col DB health superset
- `vw_patient_master_safe_VIEW_v1` (mig_223; 10,871 rows; research_id VARCHAR)
- `vw_cohort_membership_safe_VIEW_v1` (mig_223; mig_239 cast; 10,871 rows; research_id VARCHAR)
- `vw_path_malignant_tumor_safe_VIEW_v1` (mig_223; mig_239 cast; 5,944 rows; research_id VARCHAR)
- `vw_recurrence_safe_VIEW_v1` (mig_223; 10,739 rows; research_id VARCHAR)
- `vw_fna_safe_VIEW_v1` (mig_223; 8,050 rows; research_id VARCHAR)
- `vw_us_nodule_safe_VIEW_v1` (mig_223; mig_239 cast; 29,504 rows; research_id VARCHAR)
- `vw_us_exam_safe_VIEW_v1` (mig_240; 11,880 rows; 25 cols; **NEW v17**)
- `vw_molecular_safe_VIEW_v1` (mig_223; 1,384 rows)
- `vw_labs_long_safe_VIEW_v1` (mig_223; 44,124 rows)
- `vw_frozen_section_safe_VIEW_v1` (mig_242; 4,116 rows; 10 cols; **NEW v17**)
- *Expected after mig_241:* `vw_ln_patient_safe_VIEW_v1` / `vw_ln_surgery_safe_VIEW_v1` / `vw_ln_histology_attribution_safe_VIEW_v1`
- *Expected after mig_243:* `vw_snake_case_aliases_VIEW_v1` (Option A) — or alias cols embedded elsewhere (Option B)
- *Expected after mig_244:* `vw_patient_domain_wide_safe_VIEW_v1`

### `manuscript_workspace` schema (still in use)
- `qc_audit_dashboard_VIEW_v1` (mig_233) — 13-col 5-gate dashboard
- LN safe views (mig_224–229) — kept in place; mig_241 promotes them, doesn't drop
- Lane LN QC tables (mig_226–228)
- 4 TIRADS cohort views (Lane E mig_215/216/219/220/221)
- mig_232 narrow ACR view

### `main` schema notable objects
- `canonical_patient_master` (1,630 cols; 1,607 verified / 23 na — Lane J)
- `canonical_path_malignant_events_v1` (66 cols incl. `is_borderline_or_benign_with_staging`)
- `canonical_path_malignant_events_dedup_VIEW_v1` (mig_212; 66/66 registry post mig_236)
- `canonical_recurrence_resolved_v1` (Lane C mig_213; 132 quarantined rows)
- `canonical_us_nodule_v2` / `canonical_us_lymph_node_v2` / `canonical_us_thyroid_gland_v2` (US v2 layer; quarantine flags exposed in mig_238 dashboard)
- `canonical_column_verification_registry_v1` (6,623 rows post mig_239 §F dedup; was 6,789)
- `canonical_table_signoff_registry_v1` (213 verified rows at v18 write)

---

## §9 — Recent commit log

```
c2a7b5f  feat(qc): mig_242 — semantic_publication.vw_frozen_section_safe_VIEW_v1   [Cursor Composer, v17 §4]
e0d3471  mig_240: add semantic_publication.vw_us_exam_safe_VIEW_v1                  [Cline Sonnet 4.6, v17 §2]
6fc6f89  feat(qc): mig_239 — semantic research_id VARCHAR + col_registry dedup     [Cowork-direct, v17 §1]
f5d5fc5  docs(qc): v17 batch — Wave 2 + Wave 3 agent prompts (mig_239 thru mig_244) [Cowork, v17 batch]
b08432b  feat(qc): mig_238 — semantic_publication.vw_publication_qc_status_VIEW_v1 [Cowork-direct, v17 Wave 1]
9b584b9  docs(qc): mig_237 — table-comment refresh (28 missing + 2 stale)          [Cowork-direct, v17 Wave 1]
e9a1e02  feat(qc): mig_236 — registry refresh for canonical_path_malignant_events_dedup_VIEW_v1 [Cowork-direct, v17 Wave 1]
88929c8  docs(qc): v16 handoff amend — mig_233 dashboard landed + verified clean
bc0ad3b  docs(qc): v16 comprehensive handoff prompt
c49b971  feat(qc): mig_233 audit dashboard snapshot view                            [Cline Sonnet 4.6, v15 Prompt 4]
```

---

## §10 — Quick links

- [v18 handoff (this doc)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v18.md)
- [v17 batch (in-flight prompts)](computer:///Users/loganglosser/THYROID_2026/cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md)
- [v16 handoff (predecessor)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md)
- [mig_236 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/236_registry_refresh_path_dedup_view_borderline_20260501.sql)
- [mig_237 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/237_canonical_table_comments_refresh_20260501.sql)
- [mig_238 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/238_publication_qc_status_VIEW_v1_20260501.sql)
- [mig_239 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/239_semantic_research_id_varchar_standardization_20260501.sql)
- [Lane M Methods](computer:///Users/loganglosser/THYROID_2026/docs/Methods_thyroid_canonical_pub_v1_0_20260501.md)
- [Manuscript outputs](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/)
- [Verification suite v2](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/queries/cowork_verification_suite_20260430.md)
- [ISSUE_REGISTRY](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/ISSUE_REGISTRY.md)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

## §11 — Open carry-forwards (post-v17 expected)

| ID | Description | Status | Trigger to close |
|---|---|---|---|
| `CF-LN-METS-ARRAY-EMPTY-2801` | 2,801 of 2,847 LN-positive cases lack histology-attribution evidence | Methods caveat only | chart-review remediation if Logan wants tumor-type-specific LN claims |
| `Future-Gate6-Col-Registry-Distinct` | Add a "gate6" to `qc_audit_dashboard_VIEW_v1` that counts dup keys in col_registry (would have caught the 166 mig_223/224 dups before mig_239 §F) | Open suggestion | TBD; small Cowork-direct lane if greenlit |
| `Future-H-Power-BI-Marts` | `bi_powerbi.*` star-schema marts | Deferred | Phase 4 Power BI Desktop migration begins |

---

**End of v18 handoff. The new chat begins with §3 first-action checklist. Once oriented, most likely next action is (a) Path-C verify whichever of mig_241/243/244 has landed, then (b) close v17 round per §5 option A, OR (c) field whatever Logan asks next.**
