# Parallel Agent Batch — v17 Round (Wave 2 + Wave 3 cleanup)

**Generated:** 2026-05-01 by Cowork (post Wave 1 Cowork-direct close: mig_236 + mig_237 + mig_238 landed at HEAD `b08432b`)
**For:** Logan to dispatch in parallel across 4 agents
**Working dir:** `/Users/loganglosser/THYROID_2026` (note: previous batch docs reference `/Users/ros/THyroid 2026` — that path is stale; use this one)
**Tip of `origin/main`:** `b08432b` — `feat(qc): mig_238 — semantic_publication.vw_publication_qc_status_VIEW_v1`
**MotherDuck state at write:**
- 5-gate v2 = **211 / 0 / 0 / 0 / 0** (gate1 distinct = 211; no dups) ✓
- Cohort parity 10,871 / 10,871 / 10,871 ✓
- `semantic_publication.vw_publication_qc_status_VIEW_v1` (mig_238) live; analysts probe DB health with one SELECT
- `verified_main_objects_missing_comment = 0` (mig_237 closed governance gap)

---

## §0 — Why this round exists

ChatGPT's 2026-05-01 cleanup audit identified 5 governance gaps + 5 missing safe views in semantic_publication. Cowork verified each claim live against MotherDuck on 2026-05-01:

| Claim | ChatGPT said | Live verdict |
|---|---|---|
| dedup VIEW registry drift | 65 vs 66 | VERIFIED — closed by **mig_236** (Wave 1) |
| 28–30 missing/stale table comments | 30 | VERIFIED — closed by **mig_237** (Wave 1) |
| Need a publication-tier QC dashboard | 1 view | VERIFIED — closed by **mig_238** (Wave 1) |
| research_id type heterogeneity in semantic views | 3 numeric / 5 VARCHAR | VERIFIED — Wave 2 mig_239 |
| `semantic_publication` missing 4 safe views (us_exam, ln × 3, frozen, patient_domain_wide) | 5 views | VERIFIED absent — Wave 2 mig_240/241/242 + Wave 3 mig_244 |
| 15 nonstandard column names | 15 | **REFUTED — actual is 17** (ChatGPT missed `canonical_invasion_patient_rollup_v1.any_pT4a_final_anywhere` and `any_pT4b_final_anywhere`) — Wave 3 mig_243 must use the 17-col list, not the 15-col list |
| 5 quarantine counts (use these in Methods) | path=27, recur=132, us_size=7, multi=8996, nlp=11 | **PARTIALLY REFUTED**: path=27 ✓, recur=132 ✓, **us_size=15 (not 7)**, **us_nodule_multi_attr_unresolved=10570 (not 8996)**, **us_nodule_nlp_pending=2061 (not 11)**. Plus ChatGPT missed `canonical_us_lymph_node_v2.nlp_backfill_pending=6793` and `canonical_us_thyroid_gland_v2.nlp_backfill_pending=13578`. **Use the live numbers from `semantic_publication.vw_publication_qc_status_VIEW_v1`** — never paraphrase ChatGPT's. |

Wave 1 closed in this chat. Waves 2 + 3 are dispatched here.

---

## TL;DR — 6 prompts, 4 agents, parallel-safe by design

| # | Lane | Agent / model | Mig label | Dispatch | Est. time |
|---|---|---|---|---|---|
| 1 | **mig_239 — research_id VARCHAR cast in 3 semantic views** | **Cowork-direct (Logan ratification required first)** | `mig_239` | Hold for ratification | 15–20 min once ratified |
| 2 | **mig_240 — `vw_us_exam_safe_VIEW_v1`** | **Cline Sonnet 4.6** | `mig_240` | Now | 30–45 min |
| 3 | **mig_241 — LN safe-view promotion (3 views)** | **Cline Sonnet 4.6** | `mig_241` | After #2 commits | 45–60 min |
| 4 | **mig_242 — `vw_frozen_section_safe_VIEW_v1`** (compact) | **Cursor Composer** | `mig_242` | Now | 60–90 min |
| 5 | **mig_243 — snake_case alias view (17 cols)** | **Cline GPT-5.5** | `mig_243` | Now | 30–45 min |
| 6 | **mig_244 — `vw_patient_domain_wide_safe_VIEW_v1`** (curated bridge) | **Cursor Composer** | `mig_244` | After #4 commits | 120–180 min |

**Non-overlap matrix:**
- #1 (mig_239) modifies 3 existing `semantic_publication.vw_*_safe_VIEW_v1` views (cohort_membership, path_malignant_tumor, us_nodule). **Blocks #6 (mig_244)** if mig_244 references those 3 — Logan should ratify mig_239 BEFORE Cursor Composer starts mig_244 to avoid type whiplash.
- #2 (mig_240) creates new `semantic_publication.vw_us_exam_safe_VIEW_v1`. No overlap.
- #3 (mig_241) creates 3 new views in `semantic_publication`. Reads-only from `manuscript_workspace.vw_ln_*`. No overlap.
- #4 (mig_242) creates new `semantic_publication.vw_frozen_section_safe_VIEW_v1`. Reads `main.canonical_frozen_section_patient_rollup_v1`. No overlap.
- #5 (mig_243) creates new alias view (location TBD by agent — recommend `semantic_publication.vw_snake_case_aliases_VIEW_v1` OR adds alias columns inside Wave-2 safe views; agent picks). Should NOT modify base canonical_* tables.
- #6 (mig_244) creates new curated bridge view. Reads from many canonicals + safe views.

**Zero write conflicts** assuming order (#1 ratify → #2/#3/#4/#5 parallel → #6 last).

**At any point, every agent should:** confirm gate1 grew correctly, gates 2–5 stay 0, and `cohort_parity_ok = TRUE` via either dashboard:
```sql
SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
```

---

## §1 — Prompt 1: mig_239 — research_id VARCHAR cast (Cowork-direct, ratification required)

**Why Cowork-direct:** Surgical 3-view `CREATE OR REPLACE` against existing semantic safe views; Cowork should also re-verify Lane M Table CSVs still parse cleanly afterward (read-after-write check).

**Why ratification required first:** Lane M Tables 1–5 (mig_234) were built reading from `vw_cohort_membership_safe_VIEW_v1`, `vw_path_malignant_tumor_safe_VIEW_v1`, `vw_us_nodule_safe_VIEW_v1` — all of which currently expose numeric (BIGINT/INTEGER) `research_id`. If Logan's analyst code does numeric comparisons or numeric-typed joins on those research_id values, the cast to VARCHAR will silently change behavior. Logan should green-light this before Cowork applies.

**Mig label:** `mig_239_semantic_research_id_varchar_standardization_20260501`

### Context

Verified live 2026-05-01:

| semantic view | research_id type |
|---|---|
| `vw_patient_master_safe_VIEW_v1` | VARCHAR ✓ |
| `vw_fna_safe_VIEW_v1` | VARCHAR ✓ |
| `vw_labs_long_safe_VIEW_v1` | VARCHAR ✓ |
| `vw_molecular_safe_VIEW_v1` | VARCHAR ✓ |
| `vw_recurrence_safe_VIEW_v1` | VARCHAR ✓ |
| `vw_cohort_membership_safe_VIEW_v1` | **BIGINT** ← cast |
| `vw_path_malignant_tumor_safe_VIEW_v1` | **INTEGER** ← cast |
| `vw_us_nodule_safe_VIEW_v1` | **INTEGER** ← cast |

Apply `CAST(research_id AS VARCHAR) AS research_id` (or equivalent `::VARCHAR`) inside the SELECT list of those 3 views; preserve all other columns and filter logic verbatim.

### Cowork-direct action plan (after Logan ratifies)

1. Read each of the 3 view DDLs (DuckDB: `SELECT sql FROM duckdb_views() WHERE schema_name='semantic_publication' AND view_name='<name>'`).
2. Author `qc_framework_v1/migrations/239_semantic_research_id_varchar_standardization_20260501.sql` with 3 `CREATE OR REPLACE VIEW` blocks.
3. Apply via MotherDuck `query_rw`.
4. Re-verify Lane M outputs:
   - Re-run `manuscript_outputs/v1_0_20260501/Table_1_cohort_demographics_v1_0_20260501.csv` regen logic against the new view types and confirm no row count or join cardinality changes.
   - Same for Table 2 (tumor stage), Table 4 (recurrence), and `cohort_flow_v1_0_20260501.csv`.
5. Path-C: gate1 stays 211 (no new view); gates 2–5 stay 0; parity TRUE.
6. Commit `feat(qc): mig_239 — semantic research_id VARCHAR standardization`; push.

### Acceptance

- All 8 `semantic_publication.vw_*_safe_VIEW_v1` views expose `research_id` as VARCHAR
- Lane M Table 1–5 CSVs regenerate byte-identical OR row-identical with documented type change in `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` §reproducibility
- 5-gate audit unchanged

---

## §2 — Prompt 2: mig_240 — `vw_us_exam_safe_VIEW_v1` (Cline Sonnet 4.6)

**Why Cline Sonnet 4.6:** Pattern-match safe view over an existing canonical view; mechanical, deterministic, Path-C verifiable. Same agent shipped mig_233 and mig_232.

**Mig label:** `mig_240_vw_us_exam_safe_VIEW_v1_20260501`

### Context

`semantic_publication` currently exposes nodule-level US (`vw_us_nodule_safe_VIEW_v1`) but not exam-level US. Analysts who need exam denominators (e.g., "how many distinct US exams in cohort") have to drop into `main.canonical_us_exam_master_VIEW_v2` directly, breaking the safe-view convention.

Build a thin safe view over `main.canonical_us_exam_master_VIEW_v2` that:
- Adds `release_id` (CROSS JOIN to `semantic_publication.release_manifest_v1`) — matches the convention used by other safe views
- Casts `research_id` to VARCHAR (do this regardless of whether mig_239 lands first)
- Surfaces exam_id, exam_date, exam_id_source, nodule counts, gland/LN summary fields, and the `any_nlp_backfill_pending_on_exam` indicator (already exists per live DESCRIBE)
- Filters out anything you wouldn't want in a publication context (none expected, but agent should confirm)

### Prompt to paste

> **mig_240 dispatch — v17 round.** Read `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md` §10 + `cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md` §2 before tool use.
>
> Build `semantic_publication.vw_us_exam_safe_VIEW_v1` over `main.canonical_us_exam_master_VIEW_v2`. Probe live first via `DESCRIBE main.canonical_us_exam_master_VIEW_v2` to enumerate columns; pick the publication-relevant subset (release_id, research_id::VARCHAR, us_exam_id, exam_date, exam_id_source, n_nodules_on_exam, n_lymph_nodes_on_exam, n_gland_findings_on_exam, any_nlp_backfill_pending_on_exam, plus any other fields you judge clinically relevant for exam-level denominators).
>
> Match the column-naming/style of existing safe views. CROSS JOIN to `semantic_publication.release_manifest_v1` for `release_id`.
>
> **Pre-snapshot:** N/A (new view).
>
> **Register in signoff + col registries:** verified, signoff_migration='qc_framework_v1/migrations/240_vw_us_exam_safe_VIEW_v1_20260501.sql', priority_tier='tier2_canonical_view', batch_id='mig_240_us_exam_safe', verified_by='cline_sonnet_4_6_mig_240'.
>
> **Output:**
> - SQL file: `qc_framework_v1/migrations/240_vw_us_exam_safe_VIEW_v1_20260501.sql`
> - Memory note: `memory/project_mig_240_us_exam_safe_view_20260501.md`
>
> **Acceptance:**
> - View exists; row count = distinct US exams in cohort (sanity-check vs `main.canonical_us_exam_master_VIEW_v2` count — should match if you didn't filter)
> - `gate1_verified_tables` grows by 1 (211 → 212)
> - gates 2-5 stay 0
> - `cohort_parity_ok` stays TRUE
> - Surgical git add per `feedback_surgical_git_add.md`; commit `feat(qc): mig_240 — semantic_publication.vw_us_exam_safe_VIEW_v1`; push.

---

## §3 — Prompt 3: mig_241 — LN safe-view promotion (Cline Sonnet 4.6)

**Why Cline Sonnet 4.6:** Three mechanical view promotions from `manuscript_workspace` to `semantic_publication`. No content authoring; Path-C verifiable.

**Mig label:** `mig_241_ln_safe_view_promotion_to_semantic_publication_20260501`

### Context

Lane LN (mig_224–229) shipped 3 LN-domain publication-safe views in `manuscript_workspace` for prototyping speed. They're now stable and used by Lane M Methods. Promote them into `semantic_publication` (the analyst SSOT) with `release_id` and `research_id::VARCHAR`.

| Source (`manuscript_workspace`) | Target (`semantic_publication`) | Cols | Rows |
|---|---|---|---|
| `vw_ln_patient_publication_safe_VIEW_v1` | `vw_ln_patient_safe_VIEW_v1` | 9 | 4,008 |
| `vw_ln_surgery_publication_safe_VIEW_v1` | `vw_ln_surgery_safe_VIEW_v1` | 10 | 4,008 |
| `vw_ln_histology_attribution_VIEW_v1` | `vw_ln_histology_attribution_safe_VIEW_v1` | 74 | 5,918 |

The `manuscript_workspace` versions stay in place (don't drop) — Lane LN QC tables reference them. The semantic versions are net-new analyst-facing surfaces.

### Prompt to paste

> **mig_241 dispatch — v17 round.** Read `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md` §10 + `cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md` §3 before tool use. Wait for mig_240 commit to land first (avoids race on signoff registry inserts).
>
> For each of the 3 LN views in `manuscript_workspace`:
> 1. `DESCRIBE` the source view to enumerate columns
> 2. Author the matching `semantic_publication.vw_ln_*_safe_VIEW_v1` as `CREATE OR REPLACE VIEW ... AS SELECT release_id, CAST(research_id AS VARCHAR) AS research_id, <all other source cols verbatim> FROM manuscript_workspace.<source> CROSS JOIN semantic_publication.release_manifest_v1`
> 3. Register in signoff + col registries (verified, batch_id='mig_241_ln_safe_promotion', verified_by='cline_sonnet_4_6_mig_241', signoff_migration=this SQL file)
>
> **Pre-snapshot:** N/A (3 new views).
>
> **Output:**
> - SQL file: `qc_framework_v1/migrations/241_ln_safe_view_promotion_to_semantic_publication_20260501.sql`
> - Memory note: `memory/project_mig_241_ln_safe_promotion_20260501.md` documenting that the manuscript_workspace originals remain (used by Lane LN QC tables).
>
> **Acceptance:**
> - 3 new views in semantic_publication; row counts 4008 / 4008 / 5918
> - `gate1_verified_tables` grows by 3 (212 → 215 if mig_240 has landed; 211 → 214 otherwise)
> - gates 2-5 stay 0
> - `cohort_parity_ok` stays TRUE
> - Surgical git add per `feedback_surgical_git_add.md`; commit `feat(qc): mig_241 — LN safe-view promotion to semantic_publication`; push.

---

## §4 — Prompt 4: mig_242 — `vw_frozen_section_safe_VIEW_v1` compact (Cursor Composer)

**Why Cursor Composer:** The source rollup `canonical_frozen_section_patient_rollup_v1` is 188 columns wide (12-slot wide rollup). Picking a clinically-relevant compact subset for analyst consumption requires authorial judgment — the same kind Cursor Composer applied for Lane M Methods.

**Mig label:** `mig_242_vw_frozen_section_safe_VIEW_v1_20260501`

### Context

`canonical_frozen_section_patient_rollup_v1` (188 cols, 4,116 patients) is verified but not in the semantic layer. Most analyst use cases need a compact summary, not the full 12-slot wide unpivot:

- Recommended compact fields: `release_id`, `research_id::VARCHAR`, `any_frozen_section_performed`, `any_frozen_malignant`, `any_frozen_deferred`, `n_frozen_events`, `frozen_section_first_date`, `frozen_section_last_date`, plus the 1–3 highest-value summary result fields (Cursor's call after reading the rollup DDL).

Cursor should NOT replicate the full 12-slot unpivot in semantic_publication — that's the rollup's job, not a safe view's.

### Prompt to paste

> **mig_242 dispatch — v17 round.** Read `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md` §10, `cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md` §4, and the live comment on `main.canonical_frozen_section_patient_rollup_v1` (refreshed by mig_237 — confirms CF-119 closed, dates are DATE type) before tool use.
>
> Build `semantic_publication.vw_frozen_section_safe_VIEW_v1` as a compact patient-level safe view over `main.canonical_frozen_section_patient_rollup_v1`. Probe via `DESCRIBE` first to see all 188 cols. Pick the 8–15 most clinically relevant summary fields for manuscript-facing analyst use; do NOT include all 12 frozen_X_date / frozen_X_result slots.
>
> Required cols: `release_id` (from CROSS JOIN to `semantic_publication.release_manifest_v1`), `research_id::VARCHAR`. Recommended cols (Cursor confirms availability + adjusts): any_frozen_section_performed, any_frozen_malignant, any_frozen_deferred, n_frozen_events, frozen_section_first_date, frozen_section_last_date, plus 1–3 high-value summary fields.
>
> **Pre-snapshot:** N/A.
>
> **Register in signoff + col registries:** verified, signoff_migration=this SQL file, batch_id='mig_242_frozen_safe', verified_by='cursor_composer_mig_242'.
>
> **Output:**
> - SQL file: `qc_framework_v1/migrations/242_vw_frozen_section_safe_VIEW_v1_20260501.sql`
> - Memory note: `memory/project_mig_242_frozen_safe_view_20260501.md` documenting the column-selection rationale (which 188 cols were excluded and why).
>
> **Acceptance:**
> - View exists; row count = 4,116 (matches base rollup)
> - `gate1_verified_tables` grows by 1
> - gates 2-5 stay 0
> - `cohort_parity_ok` stays TRUE
> - Surgical git add per `feedback_surgical_git_add.md`; commit `feat(qc): mig_242 — semantic_publication.vw_frozen_section_safe_VIEW_v1`; push.

---

## §5 — Prompt 5: mig_243 — snake_case alias view (Cline GPT-5.5)

**Why Cline GPT-5.5:** Repetitive deterministic alias mapping across 17 columns; same agent shipped Lane J CPM 24-na audit (mig_235) — strong on this kind of governance work.

**Mig label:** `mig_243_snake_case_aliases_VIEW_v1_20260501`

### Context

ChatGPT's audit listed 15 nonstandard column names. Cowork verified live: **actual count is 17** (ChatGPT missed 2 cols on `canonical_invasion_patient_rollup_v1`). The full list:

| Base table | Nonstandard col | snake_case alias |
|---|---|---|
| canonical_airway_invasion_patient_rollup_v1 | any_pT4a_direct | any_pt4a_direct |
| canonical_airway_invasion_patient_rollup_v1 | any_pT4a_final | any_pt4a_final |
| canonical_airway_invasion_patient_rollup_v1 | n_pT4a_events | n_pt4a_events |
| canonical_ete_subgrade_patient_rollup_v1 | any_pT3b | any_pt3b |
| canonical_ete_subgrade_patient_rollup_v1 | any_pT4a | any_pt4a |
| canonical_ete_subgrade_patient_rollup_v1 | any_pT4b | any_pt4b |
| canonical_ete_subgrade_patient_rollup_v1 | any_pT4b_from_t4b_invasion | any_pt4b_from_t4b_invasion |
| canonical_ete_subgrade_patient_rollup_v1 | pT4b_ete_vs_t4b_invasion_discordant | pt4b_ete_vs_t4b_invasion_discordant |
| **canonical_invasion_patient_rollup_v1** | **any_pT4a_final_anywhere** | any_pt4a_final_anywhere |
| **canonical_invasion_patient_rollup_v1** | **any_pT4b_final_anywhere** | any_pt4b_final_anywhere |
| canonical_parathyroid_events_v1 | intact_pth_value_ngL | intact_pth_value_ng_l |
| canonical_parathyroid_patient_rollup_v1 | max_intact_pth_value_ngL | max_intact_pth_value_ng_l |
| canonical_parathyroid_patient_rollup_v1 | min_intact_pth_value_ngL | min_intact_pth_value_ng_l |
| canonical_patient_master | ajcc8_t_stage_with_microete_t3b_DEPRECATED | ajcc8_t_stage_with_microete_t3b_deprecated |
| canonical_t4b_invasion_patient_rollup_v1 | any_pT4b_direct | any_pt4b_direct |
| canonical_t4b_invasion_patient_rollup_v1 | any_pT4b_final | any_pt4b_final |
| canonical_t4b_invasion_patient_rollup_v1 | n_pT4b_events | n_pt4b_events |

**Do NOT rename base columns in-place.** Add aliases in a new view. Two location options for the agent to pick:
- **Option A (recommended):** create `semantic_publication.vw_snake_case_aliases_VIEW_v1` — single small view that joins all 7 source tables on research_id and exposes both the original and snake_case names (or just snake_case if base table has only one such col)
- **Option B:** add alias columns inside the existing safe views in `semantic_publication` that already wrap the relevant rollups (e.g., if a future `vw_invasion_safe_VIEW_v1` exists, embed there)

Pick A unless the agent finds a strong reason for B.

### Prompt to paste

> **mig_243 dispatch — v17 round.** Read `cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md` §5 before tool use. The full 17-col list is in §5 above — use it verbatim, do NOT rely on ChatGPT's 15-col list (it's incomplete).
>
> Build the alias view per Option A (recommended) or Option B (if you find a stronger reason). For Option A:
> ```sql
> CREATE OR REPLACE VIEW semantic_publication.vw_snake_case_aliases_VIEW_v1 AS
> SELECT
>   r.release_id,
>   CAST(pm.research_id AS VARCHAR) AS research_id,
>   -- airway_invasion rollup (3 cols)
>   ai."any_pT4a_direct" AS any_pt4a_direct,
>   ai."any_pT4a_final" AS any_pt4a_final,
>   ai."n_pT4a_events"  AS n_pt4a_events,
>   -- ete_subgrade rollup (5 cols)
>   ...
>   -- invasion rollup (2 cols)
>   inv."any_pT4a_final_anywhere" AS any_pt4a_final_anywhere,
>   inv."any_pT4b_final_anywhere" AS any_pt4b_final_anywhere,
>   -- parathyroid (1 event + 2 rollup) — note these are at different grains
>   ...
>   -- patient_master (1 col)
>   pm."ajcc8_t_stage_with_microete_t3b_DEPRECATED" AS ajcc8_t_stage_with_microete_t3b_deprecated,
>   -- t4b_invasion rollup (3 cols)
>   ...
> FROM main.canonical_patient_master pm
> CROSS JOIN semantic_publication.release_manifest_v1 r
> LEFT JOIN main.canonical_airway_invasion_patient_rollup_v1 ai USING (research_id)
> LEFT JOIN main.canonical_ete_subgrade_patient_rollup_v1 ete USING (research_id)
> LEFT JOIN main.canonical_invasion_patient_rollup_v1 inv USING (research_id)
> LEFT JOIN main.canonical_parathyroid_patient_rollup_v1 para USING (research_id)
> LEFT JOIN main.canonical_t4b_invasion_patient_rollup_v1 t4b USING (research_id)
> ;
> ```
>
> **Note on parathyroid:** `intact_pth_value_ngL` is on the EVENTS table (`canonical_parathyroid_events_v1`) at per-event grain. Don't try to flatten it into a per-patient view — drop the events-level alias (Cowork accepts this scope reduction) OR add a separate `semantic_publication.vw_parathyroid_event_safe_VIEW_v1` if Cline finds it warranted (mark as a deferred follow-up; don't author in this lane).
>
> Use double-quoted identifiers (`"any_pT4a_direct"`) in the SELECT list to preserve the mixed-case literal column name from the base table — DuckDB needs this for case-sensitive identifier resolution.
>
> **Pre-snapshot:** N/A (new view).
>
> **Register in signoff + col registries:** verified, signoff_migration=this SQL file, batch_id='mig_243_snake_case_aliases', verified_by='cline_gpt_5_5_mig_243'.
>
> **Output:**
> - SQL file: `qc_framework_v1/migrations/243_snake_case_aliases_VIEW_v1_20260501.sql`
> - Memory note: `memory/project_mig_243_snake_case_aliases_20260501.md` listing all 17 aliased cols with the alias used, and noting why parathyroid_events_v1 was deferred (grain mismatch).
>
> **Acceptance:**
> - View exists; row count = 10,871 (one per CPM patient)
> - All 16 patient-grain aliases queryable; spot-check 3 rows where the original col is non-null and confirm the alias returns the same value
> - `gate1_verified_tables` grows by 1
> - gates 2-5 stay 0
> - Surgical git add per `feedback_surgical_git_add.md`; commit `feat(qc): mig_243 — snake_case alias view`; push.

---

## §6 — Prompt 6: mig_244 — `vw_patient_domain_wide_safe_VIEW_v1` (Cursor Composer)

**Why Cursor Composer:** Highest-judgment lane in the bundle. Curated multi-domain bridge view; Cursor Composer authored Lane M Methods (which already curates similar cross-domain summaries) and is the right agent for the column-selection rigor required.

**Mig label:** `mig_244_vw_patient_domain_wide_safe_VIEW_v1_20260501`

### Context

Analysts repeatedly ask "give me one row per patient with the headline features across domains." `canonical_patient_master` is too wide (1,630 cols) for this use; the semantic layer's domain-specific safe views require multiple joins. A curated bridge view is the missing link.

This view should be **curated, not exhaustive** — pick the 30–60 highest-yield columns across domains. Reference Lane M Table 1 (cohort demographics) + Table 2 (tumor stage) + Table 4 (recurrence/survival) for column selection clues — those are already manuscript-validated.

Recommended domain coverage (Cursor confirms availability + makes the call on which fields):

- **Identity:** `release_id`, `research_id::VARCHAR`, `cohort_membership_status`
- **Demographics:** age_at_first_surgery, sex, race_self_reported, ethnicity
- **Tumor / pathology:** primary_histology, max_tumor_size_mm, multifocality_flag, ajcc8_T_stage_final, ajcc8_N_stage_final, ajcc8_M_stage_final, ajcc8_stage_group_final, lymphovascular_invasion_flag, ete_subgrade_max
- **Surgery:** any_total_thyroidectomy, any_lobectomy, n_neck_surgeries, first_surgery_date
- **LN:** any_ln_dissection, n_ln_examined, n_ln_positive, any_macroscopic_ets, ln_size_max_mm
- **Recurrence (manuscript SSOT):** recurrence_status (path_proven / imaging_only_unconfirmed / none), recurrence_date_first, days_to_recurrence, recurrence_imaging_then_path_confirmed_flag
- **Molecular:** any_molecular_test, any_braf_v600e_pos, any_tert_promoter_pos
- **Limitations / quarantine bookkeeping:** is_borderline_or_benign_with_staging (from path), is_implausible_date_quarantine (from recurrence), us_v2_any_nlp_backfill_pending (from CPM)

### Prompt to paste

> **mig_244 dispatch — v17 round.** Read these inputs before tool use:
> - `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md`
> - `cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v17.md` §6
> - `docs/Methods_thyroid_canonical_pub_v1_0_20260501.md` (manuscript-validated column choices)
> - `manuscript_outputs/v1_0_20260501/Table_1_*.csv`, `Table_2_*.csv`, `Table_4_*.csv` (the column lists in those files are pre-vetted for clinical relevance)
> - `semantic_publication.*` view DDLs (read path SSOT)
> - WAIT for mig_239 (research_id VARCHAR cast) to land first — otherwise type heterogeneity in joins will require workarounds
>
> Build `semantic_publication.vw_patient_domain_wide_safe_VIEW_v1` — one row per `research_id` × `release_id`, 30–60 curated columns spanning the domains listed in §6 above. Use `semantic_publication.*` views as the read path where possible (so type/release_id semantics are inherited); fall back to `main.canonical_*` only when the safe view doesn't surface the column you need.
>
> Use LEFT JOINs from `vw_cohort_membership_safe_VIEW_v1` (10,871 patients) so missing-domain rows surface as NULL rather than dropping patients. Document each column's source in the col_registry `notes` field.
>
> **Pre-snapshot:** N/A (new view).
>
> **Register in signoff + col registries:** verified, signoff_migration=this SQL file, batch_id='mig_244_patient_domain_wide_safe', verified_by='cursor_composer_mig_244'.
>
> **Output:**
> - SQL file: `qc_framework_v1/migrations/244_vw_patient_domain_wide_safe_VIEW_v1_20260501.sql`
> - Memory note: `memory/project_mig_244_patient_domain_wide_safe_view_20260501.md` documenting the column-selection rationale + non-included rationale (e.g. "frozen section excluded — separate compact view at mig_242").
>
> **Acceptance:**
> - View exists; row count = 10,871 (one per CPM patient)
> - 30–60 columns; all column names lowercase snake_case
> - `gate1_verified_tables` grows by 1
> - gates 2-5 stay 0
> - `cohort_parity_ok` stays TRUE
> - For 3 randomly-selected research_ids, verify the view's domain values match the underlying safe view (spot-check)
> - Surgical git add per `feedback_surgical_git_add.md`; commit `feat(qc): mig_244 — semantic_publication.vw_patient_domain_wide_safe_VIEW_v1`; push.

---

## §7 — Path-C verification protocol for every dispatched lane

Same as v16 §7. After each agent commits, Cowork should:

```sql
-- 1. probe live for the agent's batch_id (+ run_id if provenance row applicable)
SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE batch_id='<agent_batch_id>';

-- 2. probe the new view existence + row count
SELECT 'view_exists' AS check, COUNT(*) FROM information_schema.tables
WHERE table_schema='<expected_schema>' AND table_name='<expected_view>';

SELECT COUNT(*) AS row_count FROM <expected_schema>.<expected_view>;

-- 3. one-query health check (the new dashboard makes this trivial)
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
-- expected: gate1 = baseline + N (where N = views added by the agent)
--           gates 2-5 = 0
--           cohort_parity_ok = TRUE
--           verified_main_objects_missing_comment = 0
```

If any check fails, surface to Logan with hypothesis + propose remediation mig.

---

## §8 — Round delta projection (v17 expected vs current)

| Metric | Current (post Wave 1) | v17 final (after all 6 land) |
|---|---:|---:|
| 5-gate gate1 | 211 | 217 (211 + 6 new views from Waves 2/3) |
| 5-gate gates 2–5 | 0/0/0/0 | 0/0/0/0 (unchanged) |
| Cohort parity | 10871/10871/10871 | 10871/10871/10871 (unchanged) |
| `verified_main_objects_missing_comment` | 0 | 0 (unchanged) |
| `semantic_publication` view count (excl. release_manifest) | 9 | 14 (mig_240/241×3/242/243/244 + the existing 8 + mig_238) |
| research_id type heterogeneity | 3 numeric / 5 VARCHAR | 0 numeric / 8 VARCHAR (after mig_239) |
| Manuscript readiness | READY | READY ↑ (cleaner semantic layer; bridge view available) |

---

## §9 — Quick links

- [v16 handoff](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md)
- [Wave 1 mig_236 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/236_registry_refresh_path_dedup_view_borderline_20260501.sql)
- [Wave 1 mig_237 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/237_canonical_table_comments_refresh_20260501.sql)
- [Wave 1 mig_238 SQL](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/migrations/238_publication_qc_status_VIEW_v1_20260501.sql)
- [v15 prompt batch (style template)](computer:///Users/loganglosser/THYROID_2026/cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v15.md)
- [Verification suite v2](computer:///Users/loganglosser/THYROID_2026/qc_framework_v1/queries/cowork_verification_suite_20260430.md)

---

**End of v17 batch. Logan dispatches in this recommended order:**
1. **Ratify mig_239 first** (or explicitly defer) — gates whether mig_244 can run cleanly
2. Dispatch **mig_240, mig_242, mig_243** in parallel (zero overlap)
3. After mig_240 commits, dispatch **mig_241** (LN promotion)
4. After mig_242 + mig_239 commit, dispatch **mig_244** (curated bridge — depends on type-stable semantic layer)
5. Cowork verifies each landing via Path-C using `semantic_publication.vw_publication_qc_status_VIEW_v1`.
