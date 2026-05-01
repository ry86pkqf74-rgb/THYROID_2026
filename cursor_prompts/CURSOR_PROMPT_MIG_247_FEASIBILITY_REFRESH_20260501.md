# Cursor Composer Dispatch — mig_247: manuscript_feasibility_v1 Refresh

**Generated:** 2026-05-01 by Cowork at HEAD `5bbcee0`
**Lane:** mig_247 — refresh `manuscript_workspace.manuscript_feasibility_v1` against post-v17 schema
**Recommended agent:** Cursor Composer (re-scoring 83 manuscripts against live schema is iterative; needs reasoning per-row)
**Estimated runtime:** 30–60 min

---

## §0 — First message to paste into Cursor Composer

> mig_247 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_247_FEASIBILITY_REFRESH_20260501.md` end-to-end before any tool use. You have MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; database is `thyroid_canonical_publication_v1_0`. GitHub repo is at `/Users/loganglosser/THYROID_2026`. Use Desktop Commander for git ops (FileVault — `.git/index.lock` cleanup may be needed).

---

## §1 — Why this lane exists

`manuscript_workspace.manuscript_feasibility_v1` is the manuscript registry — 83 rows (M001–M083), each with `key_variables[]` (column names from `canonical_patient_master`) + `variable_coverage_pct[]` (% non-null) + `candidate_n` (cohort size) + `feasibility_color` (RED/YELLOW/GREEN) + `gating_issues` (free text).

**Watermark:** every row has `scored_at = '2026-04-16 04:00:00+00'`. That's pre-v17 round (gate1 was 211; we're now at 218) and pre-mig_245 (8 silently-broken cohort views fixed) and pre-mig_246 (manuscript_dashboard_VIEW_v1 built). Some columns referenced in `key_variables[]` may have been renamed, dropped, or moved to new schemas.

**Goal:** re-score every row in `manuscript_feasibility_v1` against the CURRENT state of `canonical_patient_master` + `semantic_publication.*` views. Update `candidate_n`, `variable_coverage_pct`, `feasibility_color`, `gating_issues`, `recommended_next_step`, and `scored_at`. Preserve `manuscript_id`, `title`, `status`, `priority`, `project_leaders`, `key_variables` (the spec — don't drop variables just because they're missing; flag them in gating_issues instead).

---

## §2 — Pre-task probes (run these first)

```sql
-- Tip-state confirmation (expect gate1=218, gates 2-5=0, cohort_parity TRUE)
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- Current feasibility table shape
DESCRIBE manuscript_workspace.manuscript_feasibility_v1;

-- All 83 rows + their key_variables
SELECT manuscript_id, title, status, candidate_n, key_variables, variable_coverage_pct, feasibility_color, gating_issues, scored_at
FROM manuscript_workspace.manuscript_feasibility_v1
ORDER BY manuscript_id;

-- Current canonical_patient_master columns (for variable validation)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='main' AND table_name='canonical_patient_master'
ORDER BY column_name;
```

---

## §3 — Task spec (per row in manuscript_feasibility_v1)

For EACH `manuscript_id` in `manuscript_feasibility_v1`:

### Step 3.1 — Resolve key_variables against current schema

For each variable name in `key_variables[]`:
- Test if it exists in `main.canonical_patient_master`
- If YES: compute coverage as `(COUNT(*) WHERE var IS NOT NULL) / total_cohort_n` × 100
- If NO: mark as MISSING; check if it was renamed (e.g., `syn_isthmus_size_cm` → `syn_isthmus_size_cm_legacy_raw`); if so, log the rename

### Step 3.2 — Update candidate_n via cohort view

Look up the cohort view from `manuscript_workspace.manuscript_dive_map_v1` for this `manuscript_id`. If a cohort view is mapped:
```sql
SELECT COUNT(*) FROM manuscript_workspace.<cohort_view_name>;
```
**Caveat:** at least one cohort view is silently broken (`cohort_m031_nuclear_medicine_v1` references the renamed `syn_isthmus_size_cm`). If your COUNT(*) errors with a Catalog/Binder error, capture the error in gating_issues and use the `key_variables`-derived candidate_n instead.

### Step 3.3 — Re-derive feasibility_color

Apply the original heuristic (preserve consistency with 2026-04-16 scoring):
- **GREEN**: all key_variables have ≥80% coverage AND candidate_n ≥ 100
- **YELLOW**: at least one key_variable has 30–80% coverage, OR candidate_n is 50–100
- **RED**: at least one key_variable has <30% coverage, OR candidate_n < 50, OR a key_variable is MISSING from current schema, OR external data is required (zip codes, heavy metals, QoL, metabolomics)

If color worsened from 2026-04-16 → today, set `gating_issues` to explain. If improved, set `recommended_next_step` to "Reassess after v17 schema improvements".

### Step 3.4 — Update gating_issues + recommended_next_step

For each row, append a "post-v17 status" sentence to `gating_issues` if color changed OR a key_variable is missing OR cohort_view is broken. Examples:
- "post-v17: cohort_m031 broken (syn_isthmus_size_cm renamed); use canonical_patient_master directly"
- "post-v17: bethesda_final coverage held at 100%; mol_has_thyroseq improved from 40% to 48%"
- "post-v17: NEW feasibility GREEN — was YELLOW due to molecular coverage <50%; now 80%"

### Step 3.5 — Update scored_at

```sql
UPDATE manuscript_workspace.manuscript_feasibility_v1
SET scored_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
    canonical_version_at_scoring = 'v1_0_post_mig_246'
WHERE manuscript_id = <id>;
```

(Or rebuild the entire table via CREATE OR REPLACE TABLE if cleaner.)

---

## §4 — Output

### Files to author:
1. `qc_framework_v1/migrations/247_feasibility_refresh_20260501.sql` — full UPDATE/CREATE OR REPLACE TABLE statements applied
2. `memory/project_mig_247_feasibility_refresh_20260501.md` — agent's notes on:
   - Total rows refreshed (should be 83)
   - Color transitions (GREEN→GREEN, GREEN→YELLOW, RED→GREEN, etc.)
   - Variables flagged as MISSING (rename candidates documented)
   - Cohort views found broken during the refresh (additions to mig_248's scope)

### Verification (post-apply):
```sql
-- Should return 83 rows, all with scored_at near current timestamp
SELECT COUNT(*) AS total_refreshed,
       MIN(scored_at) AS earliest_scored_at,
       MAX(scored_at) AS latest_scored_at
FROM manuscript_workspace.manuscript_feasibility_v1
WHERE canonical_version_at_scoring = 'v1_0_post_mig_246';

-- Color distribution post-refresh (compare to pre: 46 GREEN / 17 YELLOW / 20 RED)
SELECT feasibility_color, COUNT(*) AS n
FROM manuscript_workspace.manuscript_feasibility_v1
GROUP BY feasibility_color
ORDER BY CASE feasibility_color WHEN 'GREEN' THEN 1 WHEN 'YELLOW' THEN 2 WHEN 'RED' THEN 3 ELSE 4 END;

-- Manuscripts that gained GREEN feasibility post-v17:
SELECT manuscript_id, title, feasibility_color, gating_issues
FROM manuscript_workspace.manuscript_feasibility_v1
WHERE feasibility_color = 'GREEN' AND gating_issues LIKE '%NEW feasibility GREEN%';

-- Manuscripts that lost feasibility post-v17:
SELECT manuscript_id, title, feasibility_color, gating_issues
FROM manuscript_workspace.manuscript_feasibility_v1
WHERE gating_issues LIKE '%post-v17%' AND feasibility_color IN ('YELLOW', 'RED');

-- Manuscript dashboard signals after refresh:
SELECT draft_readiness_signal, COUNT(*) AS n
FROM manuscript_workspace.manuscript_dashboard_VIEW_v1
GROUP BY draft_readiness_signal;
```

### Commit:
```
feat(qc): mig_247 — manuscript_feasibility_v1 refresh against post-v17 schema

Re-scored all 83 manuscripts. Color transitions: <agent fills in>
New READY_TO_DRAFT count (via dashboard): <agent fills in>
Variables flagged as MISSING from current schema: <agent fills in>
Cohort views broken during refresh (added to mig_248): <agent fills in>

Files:
- new: qc_framework_v1/migrations/247_feasibility_refresh_20260501.sql
- new: memory/project_mig_247_feasibility_refresh_20260501.md
- updated: manuscript_workspace.manuscript_feasibility_v1 (83 rows)
```

Then surgical `git push origin main`.

---

## §5 — Guardrails

- **Don't drop existing columns** from `manuscript_feasibility_v1` schema. The shape stays at 14 cols; only update VALUES.
- **Don't change manuscript_id, title, status, priority, project_leaders** — these are PI-curated. Only re-score the data-derived fields.
- **PHI safety**: `research_id` is the only patient identifier ever exposed. Don't print clinical_notes_long or note_entities_* contents.
- **Surgical git add** per `feedback_surgical_git_add.md` (don't `git add -A`).
- **gate1=218 should NOT change** — `manuscript_feasibility_v1` is in `manuscript_workspace`, not registered in `canonical_table_signoff_registry_v1`.

---

## §6 — Reference

- Live state: HEAD `5bbcee0` (post-mig_246), gate1=218, gates 2-5=0/0/0/0, cohort_parity TRUE
- Manuscript dashboard: `SELECT * FROM manuscript_workspace.manuscript_dashboard_VIEW_v1`
- Existing feasibility: `SELECT * FROM manuscript_workspace.manuscript_feasibility_v1`
- Dive↔manuscript map: `SELECT * FROM manuscript_workspace.manuscript_dive_map_v1`
- v17 closeout retrospective: `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-05-01_v17.md`
- v19 handoff: `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v19.md`
- mig_245 (cohort view repairs): `qc_framework_v1/migrations/245_stale_view_ref_repair_20260501.sql`
- mig_246 (this dashboard): `qc_framework_v1/migrations/246_manuscript_dashboard_VIEW_v1_20260501.sql`

**End of mig_247 dispatch.**
