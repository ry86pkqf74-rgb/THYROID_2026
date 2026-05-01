# Cowork Handoff Prompt v19 — Thyroid Canonical Publication v1.0

**Generated:** 2026-05-01 by Cowork at v17 round closeout
**Tip of `origin/main` at write:** `273eb75` — `feat(qc): mig_244 — semantic_publication.vw_patient_domain_wide_safe_VIEW_v1` (will advance once the closeout commit lands; expect a `docs(qc): v17 round closeout + v19 handoff` immediately above)
**Supersedes:** v18 at `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v18.md`
**Closeout retrospective:** `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md`

---

## §0 — First message to paste into the new Cowork chat (verbatim)

> Please read `/Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v19.md` end-to-end before any tool use.
>
> **Then run the §3 baseline-confirm checklist:**
> 1. `git fetch origin && git log --oneline -10` from `/Users/loganglosser/THYROID_2026` (note: previous handoffs reference `/Users/ros/THyroid 2026` — that path is stale; the live repo is at `/Users/loganglosser/THYROID_2026`)
> 2. `SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;` — expect `gate1 = 218`, gates 2–5 = 0, cohort_parity TRUE
> 3. Confirm `semantic_publication` has 1 BASE TABLE + 15 VIEWs (full inventory in §3.2 below)
> 4. If anything regressed, surface to Logan immediately with hypothesis
>
> **Standing context:** I'm Logan Glosser, Emory thyroid-cancer surgery researcher. We're cleaning up the v1.0 publication lakehouse on MotherDuck (`thyroid_canonical_publication_v1_0`). The v17 round of cleanup migrations (6 lanes: mig_236 / mig_237 / mig_238 / mig_239 / mig_240 / mig_241 / mig_242 / mig_243 / mig_244) closed clean. v17 retrospective: `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md`. v18 mid-round handoff (predecessor): `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v18.md`.
>
> **You have:** Desktop Commander MCP for git/shell on my Mac (FileVault — `.git/index.lock` cleanup may be needed); MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; GitHub repo at `/Users/loganglosser/THYROID_2026`. **Use Desktop Commander for git ops.**
>
> **Most likely first task: Lane M manuscript drafting.** The semantic layer is now significantly cleaner than v16 — 15 safe views (was 8) including `vw_us_exam_safe_VIEW_v1`, `vw_frozen_section_safe_VIEW_v1`, 3 LN views, `vw_snake_case_aliases_VIEW_v1`, and `vw_patient_domain_wide_safe_VIEW_v1` (46-col per-patient bridge). research_id is VARCHAR everywhere in `semantic_publication.*`. Tables 1–5 in `manuscript_outputs/v1_0_20260501/` may benefit from refresh against the cleaner semantic layer; Methods doc at `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md`. Wait for Logan's direction before pulling any manuscript work, but be ready.

---

## §1 — Round delta v17 (closed) → v19 baseline

| Metric | v16 final | v17 closeout / v19 baseline | Δ |
|---|---:|---:|---:|
| `gate1_verified_tables` | 211 | **218** | +7 |
| Gates 2 / 3 / 4 / 5 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | unchanged |
| Cohort parity (CPM × US gland v2 × US LN v2) | 10871 / 10871 / 10871 | 10871 / 10871 / 10871 | unchanged |
| `verified_main_objects_missing_comment` | 28 (silent) | **0** | mig_237 |
| `semantic_publication` view count (excl. release_manifest) | 8 | **15** | +7 (mig_238 + mig_240 + 3×mig_241 + mig_242 + mig_243 + mig_244) |
| Numeric `research_id` in semantic | 3 / 8 | **0 / 14** | mig_239 cast; new lanes inherited VARCHAR |
| `col_registry` duplicate keys | 166 (silent) | **0** | mig_239 §F dedup |
| Manuscript readiness | READY | **READY ↑** | cleaner semantic layer + curated bridge view |

---

## §2 — Lanes closed in the v17 round

Full retrospective at [`COWORK_SESSION_SUMMARY_2026-05-01_v17.md`](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md). Quick ledger:

| Mig | Commit | Lane | Path-C |
|---|---|---|:---:|
| mig_236 | `e9a1e02` | dedup VIEW 65→66 cols (Cowork-direct) | ✓ |
| mig_237 | `9b584b9` | 28 missing + 2 stale table comments (Cowork-direct) | ✓ |
| mig_238 | `b08432b` | `vw_publication_qc_status_VIEW_v1` (Cowork-direct) | ✓ |
| mig_239 | `6fc6f89` | research_id VARCHAR + col_registry dedup (Cowork-direct) | ✓ |
| mig_240 | `e0d3471` | `vw_us_exam_safe_VIEW_v1` (Cline Sonnet 4.6) | ✓ |
| mig_242 | `c2a7b5f` | `vw_frozen_section_safe_VIEW_v1` (Cursor Composer) | ✓ |
| mig_243 | `9cf03cd` | `vw_snake_case_aliases_VIEW_v1` (Cline GPT-5.5) | ✓ |
| mig_241 | `35f29d3` | 3 LN views promoted to `semantic_publication` (Cline Sonnet 4.6) | ✓ |
| mig_244 | `273eb75` | `vw_patient_domain_wide_safe_VIEW_v1` (Cursor Composer) | ✓ |

---

## §3 — Baseline-confirm checklist for new chat

### Step 3.1 — Confirm git state via Desktop Commander

```bash
cd "/Users/loganglosser/THYROID_2026"
git fetch origin
git log --oneline -10
git status --porcelain
```

Expect HEAD ≥ `273eb75`. The most recent commit should be the v17 closeout / v19 handoff docs commit. If new commits past that exist, Logan started something new — orient via the commit messages.

### Step 3.2 — One-query lakehouse health

```sql
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
```

**Required baseline values:**
- `gate1_verified_tables` = 218
- `gate1_distinct_objects` = 218 (no dup signoffs)
- `gate2_missing_signoff` = 0
- `gate3_count_mismatch` = 0
- `gate4_verified_cols_missing_metadata` = 0
- `gate5_clinical_date_violations` = 0
- `cpm_pts` = `us_gland_v2_pts` = `us_ln_v2_pts` = 10871
- `cohort_parity_ok` = TRUE
- `verified_main_objects_missing_comment` = 0
- `release_id` = `pub_v1_0_20260430`

If anything regressed, surface to Logan with hypothesis BEFORE doing any other work.

### Step 3.3 — Confirm `semantic_publication` schema inventory

```sql
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'semantic_publication'
ORDER BY table_name;
```

Expect 16 entries (1 BASE TABLE + 15 VIEWs):

```
release_manifest_v1                          BASE TABLE
vw_cohort_membership_safe_VIEW_v1            VIEW    (mig_223 + mig_239)
vw_fna_safe_VIEW_v1                          VIEW    (mig_223)
vw_frozen_section_safe_VIEW_v1               VIEW    (mig_242, v17)
vw_labs_long_safe_VIEW_v1                    VIEW    (mig_223)
vw_ln_histology_attribution_safe_VIEW_v1     VIEW    (mig_241, v17 — 75 cols, 5,918 rows)
vw_ln_patient_safe_VIEW_v1                   VIEW    (mig_241, v17 — 10 cols, 4,008 rows)
vw_ln_surgery_safe_VIEW_v1                   VIEW    (mig_241, v17 — 11 cols, 4,008 rows)
vw_molecular_safe_VIEW_v1                    VIEW    (mig_223)
vw_path_malignant_tumor_safe_VIEW_v1         VIEW    (mig_223 + mig_239)
vw_patient_master_safe_VIEW_v1               VIEW    (mig_223)
vw_publication_qc_status_VIEW_v1             VIEW    (mig_238, v17 — DB health probe)
vw_recurrence_safe_VIEW_v1                   VIEW    (mig_223)
vw_snake_case_aliases_VIEW_v1                VIEW    (mig_243, v17 — 18 cols incl. 16 patient-grain aliases)
vw_us_exam_safe_VIEW_v1                      VIEW    (mig_240, v17 — 25 cols, 11,880 rows)
vw_us_nodule_safe_VIEW_v1                    VIEW    (mig_223 + mig_239)
vw_patient_domain_wide_safe_VIEW_v1          VIEW    (mig_244, v17 — 46 cols, 10,871 rows; per-patient bridge)
```

### Step 3.4 — research_id type uniformity

```sql
SELECT table_name, data_type
FROM information_schema.columns
WHERE table_schema = 'semantic_publication'
  AND LOWER(column_name) = 'research_id'
ORDER BY table_name;
```

Expect: ALL 14 view rows return VARCHAR. If any view has numeric research_id, mig_239 cast was undone — surface to Logan immediately.

---

## §4 — Decision menu (after baseline confirm)

After §3 baseline-confirm, choose:

- **(A) Lane M — manuscript drafting** *(most likely)* — orient toward:
  - `manuscript_outputs/v1_0_20260501/` (Tables 1–5 + cohort flow CSVs) — refresh against the cleaner semantic layer
  - `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` (90-line Methods draft) — refresh and spot-check that the live numbers from `semantic_publication.vw_publication_qc_status_VIEW_v1` match what's quoted in Methods
  - `semantic_publication.vw_*_safe_VIEW_v1` (read-path SSOT — now 14 safe views + the patient_domain_wide bridge)
  - `parquet_export/pub_v1_0_20260430/` (offline reproducibility mirror — should not need refresh; v17 lanes were all CREATE VIEW, no table content changes)
- **(B) Small carry-forward lane** — Logan picks one of:
  - `CF-PARATHYROID-EVENT-SAFE` — author `semantic_publication.vw_parathyroid_event_safe_VIEW_v1` to surface events-grain `intact_pth_value_ngL` (deferred from mig_243 due to grain mismatch). Small Cowork-direct lane.
  - `Future-Gate6-Col-Registry-Distinct` — add a "gate6" to `qc_audit_dashboard_VIEW_v1` that counts col_registry dup keys (would have caught the 166 mig_223/224 dups before mig_239 §F). Small Cowork-direct lane.
- **(C) New lane needed** — Logan describes a gap; Cowork writes a new Cursor/Cline lane prompt to `cursor_prompts/`, documents non-overlap zones, recommends agent.
- **(D) Future H — Power BI marts** — ratify trigger with Logan; if green-light, write the multi-day Cursor Composer prompt for `bi_powerbi.*` star-schema build.
- **(E) Read-only / orientation only** — Logan just wants to inspect / explain something. Answer in chat, no commits.

---

## §5 — Path-C verification protocol (mandatory if new agent lanes dispatched)

Same as v18 §6. After every commit landing while you watch:

1. **Probe live MD** for the agent's `batch_id`:
   ```sql
   SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE batch_id LIKE '<agent_batch_id_pattern>';
   ```
2. **Verify acceptance criteria** from the lane's prompt (row counts, view existence, flag presence)
3. **Re-run mig_233 dashboard** — confirm gate1 grew correctly + gates 2–5 stay 0:
   ```sql
   SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
   ```
4. **Re-run mig_238 publication QC view** — superset health:
   ```sql
   SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
   ```
5. **Cohort parity** — should stay 10,871 / 10,871 / 10,871
6. **For mutating lanes** (rare; v17 round had none): confirm pre-snapshot exists
7. **If clean**: nothing for Cowork to do — agent already committed
8. **If issues**: surface to Logan with hypothesis + propose remediation per `feedback_remediation_pattern.md`

---

## §6 — Repo + tooling reminders (unchanged from v18 §7)

- **Workspace path:** `/Users/loganglosser/THYROID_2026` (NOT `/Users/ros/THyroid 2026` — stale templating from older handoff docs)
- **Surgical git add per `feedback_surgical_git_add.md`**: never `git add -A` or directory-wide; explicit paths only
- **Always commit + push per `feedback_commit_workflow.md`**: stage → commit → push; lint Python first
- **PHI safety per `feedback_phi_safety.md`**: never print clinical notes; research_id only
- **Desktop Commander > bash sandbox per `feedback_use_desktop_commander_first.md`**: bash sandbox can't unlink `.git/index.lock` (FileVault); use Desktop Commander for git ops
- **DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ** per `reference_duckdb_timestamp_tz.md`: always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols
- **No cross-DB canonical sourcing** per `feedback_no_cross_db_canonical_sourcing.md`: canonicals are standalone live objects in `main`; never `FROM archive_pub_v1_0.*` at build time
- **VIEW naming** per `reference_view_naming_convention.md`: any `main.*` or `manuscript_workspace.*` or `semantic_publication.*` VIEW must carry `_VIEW_v1` suffix
- **research_id is VARCHAR everywhere in `semantic_publication.*`** (mig_239) — agents must `CAST(research_id AS VARCHAR)` in any new safe view
- **col_registry dedup is fresh** (mig_239 §F removed 166 dup keys from mig_223/224); v17 agents added 160+ clean rows on top — any new col registry insert should be a clean single row per (schema, table, column)

---

## §7 — Reference object inventory (v19 baseline)

### `semantic_publication` schema (publication-tier analyst SSOT)
- `release_manifest_v1` — BASE TABLE, 1 row (`pub_v1_0_20260430`)
- `vw_publication_qc_status_VIEW_v1` (mig_238) — single-row 31-col DB health superset
- `vw_patient_master_safe_VIEW_v1` (mig_223; 10,871 rows)
- `vw_cohort_membership_safe_VIEW_v1` (mig_223 + mig_239 cast; 10,871 rows)
- `vw_path_malignant_tumor_safe_VIEW_v1` (mig_223 + mig_239 cast; 5,944 rows)
- `vw_recurrence_safe_VIEW_v1` (mig_223; 10,739 rows)
- `vw_fna_safe_VIEW_v1` (mig_223; 8,050 rows)
- `vw_us_nodule_safe_VIEW_v1` (mig_223 + mig_239 cast; 29,504 rows)
- `vw_us_exam_safe_VIEW_v1` (mig_240; 11,880 rows; 25 cols)
- `vw_molecular_safe_VIEW_v1` (mig_223; 1,384 rows)
- `vw_labs_long_safe_VIEW_v1` (mig_223; 44,124 rows)
- `vw_frozen_section_safe_VIEW_v1` (mig_242; 4,116 rows; 10 cols)
- `vw_ln_patient_safe_VIEW_v1` (mig_241; 4,008 rows; 10 cols)
- `vw_ln_surgery_safe_VIEW_v1` (mig_241; 4,008 rows; 11 cols)
- `vw_ln_histology_attribution_safe_VIEW_v1` (mig_241; 5,918 rows; 75 cols)
- `vw_snake_case_aliases_VIEW_v1` (mig_243; 10,871 rows; 18 cols = 2 keys + 16 patient-grain aliases)
- `vw_patient_domain_wide_safe_VIEW_v1` (mig_244; 10,871 rows; 46 cols; per-patient bridge)

### `manuscript_workspace` schema (still in use)
- `qc_audit_dashboard_VIEW_v1` (mig_233) — 13-col 5-gate dashboard
- LN safe views (mig_224–229) — kept in place; mig_241 promoted them, didn't drop
- Lane LN QC tables (mig_226–228)
- 4 TIRADS cohort views (Lane E mig_215/216/219/220/221)
- mig_232 narrow ACR view

### `main` schema notable objects
- `canonical_patient_master` (1,630 cols; 1,607 verified / 23 na — Lane J)
- `canonical_path_malignant_events_v1` (66 cols incl. `is_borderline_or_benign_with_staging`)
- `canonical_path_malignant_events_dedup_VIEW_v1` (mig_212; 66/66 registry post mig_236)
- `canonical_recurrence_resolved_v1` (Lane C mig_213; 132 quarantined rows)
- `canonical_us_nodule_v2` / `canonical_us_lymph_node_v2` / `canonical_us_thyroid_gland_v2` (US v2 layer; quarantine flags exposed in mig_238 dashboard)
- `canonical_column_verification_registry_v1` (~6,800 rows post v17; 166 dup keys removed mig_239 §F, then ~160+ added by v17 agent lanes)
- `canonical_table_signoff_registry_v1` (218 verified rows at v19 baseline)

---

## §8 — Recent commit log

```
<closeout commit will go here once landed; tip until then is 273eb75>
273eb75  feat(qc): mig_244 — semantic_publication.vw_patient_domain_wide_safe_VIEW_v1   [Cursor Composer]
35f29d3  mig_241: LN safe-view promotion to semantic_publication (3 views)             [Cline Sonnet 4.6]
9cf03cd  feat(qc): mig_243 — snake_case alias view                                     [Cline GPT-5.5]
1b0e143  docs(qc): v18 handoff — mid-round (Wave 1 + mig_239/240/242 closed; mig_241/243/244 in flight)
c2a7b5f  feat(qc): mig_242 — semantic_publication.vw_frozen_section_safe_VIEW_v1       [Cursor Composer]
e0d3471  mig_240: add semantic_publication.vw_us_exam_safe_VIEW_v1                     [Cline Sonnet 4.6]
6fc6f89  feat(qc): mig_239 — semantic research_id VARCHAR + col_registry dedup         [Cowork-direct]
f5d5fc5  docs(qc): v17 batch — Wave 2 + Wave 3 agent prompts (mig_239 thru mig_244)
b08432b  feat(qc): mig_238 — semantic_publication.vw_publication_qc_status_VIEW_v1     [Cowork-direct]
9b584b9  docs(qc): mig_237 — table-comment refresh (28 missing + 2 stale)              [Cowork-direct]
e9a1e02  feat(qc): mig_236 — registry refresh for canonical_path_malignant_events_dedup_VIEW_v1 [Cowork-direct]
```

---

## §9 — Quick links

- [v19 handoff (this doc)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v19.md)
- [v17 closeout retrospective](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md)
- [v18 handoff (predecessor — mid-round)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v18.md)
- [v17 batch (closed)](computer:///Users/loganglosser/THYROID_2026/cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md)
- [v16 handoff (last pre-v17 baseline)](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md)
- [mig_241 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/241_ln_safe_view_promotion_to_semantic_publication_20260501.sql)
- [mig_243 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/243_snake_case_aliases_VIEW_v1_20260501.sql)
- [mig_244 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/244_vw_patient_domain_wide_safe_VIEW_v1_20260501.sql)
- [Lane M Methods](computer:///Users/loganglosser/THYROID_2026/docs/Methods_thyroid_canonical_pub_v1_0_20260501.md)
- [Manuscript outputs](computer:///Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/)
- [Verification suite v2](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/queries/cowork_verification_suite_20260430.md)
- [ISSUE_REGISTRY](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/ISSUE_REGISTRY.md)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

## §10 — Open carry-forwards (post-v17)

| ID | Description | Status | Trigger to close |
|---|---|---|---|
| `CF-LN-METS-ARRAY-EMPTY-2801` | 2,801 of 2,847 LN-positive cases lack histology-attribution evidence | Methods caveat only | chart-review remediation if Logan wants tumor-type-specific LN claims |
| `CF-PARATHYROID-EVENT-SAFE` | events-grain `intact_pth_value_ngL` deferred from mig_243 (grain mismatch) | Open suggestion | author `semantic_publication.vw_parathyroid_event_safe_VIEW_v1` if Logan needs per-event PTH access for Methods |
| `Future-Gate6-Col-Registry-Distinct` | Add a "gate6" to `qc_audit_dashboard_VIEW_v1` that counts col_registry dup keys (would have caught the 166 mig_223/224 dups before mig_239 §F) | Open suggestion | TBD; small Cowork-direct lane if greenlit |
| `Future-H-Power-BI-Marts` | `bi_powerbi.*` star-schema marts | Deferred | Phase 4 Power BI Desktop migration begins |

---

**End of v19 handoff. The new chat begins with §3 baseline-confirm. Most likely next action: Lane M manuscript drafting against the cleaner semantic layer.**
