# Cursor Composer Dispatch — mig_249: manuscript_feasibility_v1 Re-Refresh (post-mig_248)

**Generated:** 2026-05-01 by Cowork at HEAD `78b1ae3`
**Lane:** mig_249 — re-score `manuscript_workspace.manuscript_feasibility_v1` against post-mig_248 schema
**Recommended agent:** Cursor Composer (per-row reasoning over 83 manuscripts)
**Estimated runtime:** 30–60 min
**Supersedes (in spirit):** the post-mig_247 stale state. Closes carry-forward **CF-MIG_247-RERUN** from v20 §11.
**Note on numbering:** the v20 §5 "(D) per-manuscript Tables generator" idea shifts to **mig_250**.

---

## §0 — First message to paste into Cursor Composer

> mig_249 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_249_FEASIBILITY_RE_REFRESH_20260501.md` end-to-end before any tool use. You have MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; database is `thyroid_canonical_publication_v1_0`. GitHub repo is at `/Users/loganglosser/THYROID_2026`. Use Desktop Commander for git ops (FileVault — `.git/index.lock` cleanup may be needed).

---

## §1 — Why this lane exists

mig_247 (HEAD `80b3c43`) re-scored all 83 rows of `manuscript_feasibility_v1` against the post-v17 / post-mig_246 schema (`canonical_version_at_scoring = 'v1_0_post_mig_246'`). That refresh was correct *at the time*, but it predates **mig_248** (HEAD `78b1ae3`), the column-rename drift repair that:

1. **Rewrote `manuscript_workspace.cohort_descriptive_full_cohort_v1`** to expose backward-compat aliases:
   - `p.path_tumor_size_cm AS tumor_size_cm` (4,130/10,871 non-null in current state)
   - `cupm.tirads_category_at_first_exam AS tirads_best_category_v12` (3,282/10,871 non-null)
   - `cupm.max_tirads_category_ever AS tirads_worst_category_v12`
   - `cupm.max_nodule_size_mm AS tirads_nodule_size_max_mm_v12`
   - `p.syn_isthmus_size_cm_legacy_raw` (replaces stale `syn_isthmus_size_cm` ref) and matching left/right lobe legacy_raw columns
2. **Rewrote `cohort_m049_pyramidal_lobe_v1` and `cohort_m058_thyroid_size_weight_v1`** for the same `syn_*_legacy_raw` pattern.
3. **Built `manuscript_workspace.dive_cohort_size_v1`** — a materialized snapshot of every cohort view's row count.

Because every child cohort view (M048/M057/M076/etc.) `SELECT`s from `cohort_descriptive_full_cohort_v1`, the parent's new aliases propagate automatically. **All 63 cohort views in `manuscript_dive_map_v1` are now queryable** (verified by Cowork at v20-check time).

### Stale flags currently in `manuscript_feasibility_v1` (count from live query 2026-05-01):

| Pattern | Count | Now resolved? |
|---|---:|---|
| `gating_issues ILIKE '%cohort SELECT failed%'` | 24 | YES — every cohort view returns COUNT(*) without error |
| `gating_issues ILIKE '%syn_isthmus_size_cm%'` Binder Error | 24 | YES — replaced with `_legacy_raw` rename in mig_248 |
| `gating_issues ILIKE '%tirads_best_category_v12 MISSING%'` | 7 | YES — alias added to parent view |
| `gating_issues ILIKE '%tumor_size_cm MISSING%'` | 8 | YES — alias added to parent view |

### Specific manuscripts likely to flip RED → GREEN after this re-score (Cowork's pre-analysis):

| ID | Title | Pre-mig_248 RED reason | Why it should now be GREEN |
|---|---|---|---|
| M025 | TIRADS Performance | `tirads_best_category_v12 MISSING` | Alias resolves (3,282 non-null) |
| M029 | FNA Concordance | `tumor_size_cm MISSING` | Alias resolves (4,130 non-null) |
| M030 | Genetic Predictive | `tirads_best_category_v12 MISSING` | Alias resolves |
| M037 | LN Metastasis | `tumor_size_cm MISSING` | Alias resolves |
| M043 | LN Predictors | `tumor_size_cm MISSING` | Alias resolves |
| M045 | Multimodal Risk | both `tirads_v12` AND `tumor_size_cm` MISSING | Both aliases resolve |

### Manuscripts whose RED was for OTHER reasons (re-score should NOT flip these):

- **M031** (Nuclear Medicine) — RED for "Histology 69pct in nucmed cohort" — real coverage shortfall, not column rename. Likely stays RED.
- **M033** (Afirma/ThyroSeq) — RED for "Surgery type 68pct in mol+histol overlap". Likely stays RED.
- **M035** (Bethesda V) — RED for "Molecular testing 27pct in Beth V cohort". Likely stays RED (or YELLOW if rules permit secondary-only).
- **M036, M042** — RED with no specific column hint. Re-check whether the underlying coverage is actually intact post-mig_248; some may flip.

---

## §2 — Pre-task probes (run these first)

```sql
-- Tip-state confirmation (expect gate1=218, gates 2-5=0, cohort_parity TRUE)
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- Confirm mig_248 aliases are live in the parent view
SELECT
  COUNT(*) AS n_rows,
  COUNT(tumor_size_cm) AS coverage_tumor_size_cm,
  COUNT(tirads_best_category_v12) AS coverage_tirads_v12,
  COUNT(tirads_worst_category_v12) AS coverage_tirads_worst_v12
FROM manuscript_workspace.cohort_descriptive_full_cohort_v1;
-- Expected: 10871, 4130, 3282, ≥3282

-- Current feasibility table shape (columns shouldn't have changed since mig_247)
DESCRIBE manuscript_workspace.manuscript_feasibility_v1;

-- All 83 rows + their key_variables (current state)
SELECT manuscript_id, title, status, candidate_n,
       feasibility_color, scored_at, canonical_version_at_scoring,
       LEFT(COALESCE(gating_issues, ''), 250) AS gating_issues_excerpt
FROM manuscript_workspace.manuscript_feasibility_v1
ORDER BY manuscript_id;

-- Materialized cohort sizes (built by mig_248)
SELECT cohort_view_name, current_row_count, measured_at
FROM manuscript_workspace.dive_cohort_size_v1
ORDER BY cohort_view_name;
```

---

## §3 — Task spec (per row in manuscript_feasibility_v1)

For EACH `manuscript_id` in `manuscript_feasibility_v1`:

### Step 3.1 — Resolve key_variables against current schema (incl. mig_248 aliases)

For each variable name in `key_variables[]`:
- Test if it exists in `main.canonical_patient_master` **OR** in the parent cohort view `manuscript_workspace.cohort_descriptive_full_cohort_v1` (the latter exposes mig_248's backward-compat aliases like `tumor_size_cm`, `tirads_best_category_v12`)
- If YES (in either): compute coverage as `(COUNT(*) WHERE var IS NOT NULL) / total_cohort_n` × 100, sourcing the column from whichever object has it
- If NO (in neither): mark as MISSING; check the rename inventory below
- **Important:** if the variable was previously flagged as MISSING in the existing `gating_issues` but is now resolvable via the parent view alias, REMOVE that stale flag from gating_issues (don't keep history; keep it current)

### Mig_248 rename inventory (always check the alias before flagging MISSING):

| Old column name (in key_variables) | Resolved via | Source |
|---|---|---|
| `tumor_size_cm` | parent view alias → `path_tumor_size_cm` | mig_248 |
| `tirads_best_category_v12` | parent view alias → `cupm.tirads_category_at_first_exam` | mig_248 |
| `tirads_worst_category_v12` | parent view alias → `cupm.max_tirads_category_ever` | mig_248 |
| `tirads_nodule_size_max_mm_v12` | parent view alias → `cupm.max_nodule_size_mm` | mig_248 |
| `tirads_best_score_v12` | parent view derived: `CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT)` | mig_248 |
| `syn_isthmus_size_cm` | renamed → `syn_isthmus_size_cm_legacy_raw` | mig_173 (rename), mig_248 (cohort fix) |
| `syn_left_lobe_size_cm` | renamed → `syn_left_lobe_size_cm_legacy_raw` | mig_173 + mig_248 |
| `syn_right_lobe_size_cm` | renamed → `syn_right_lobe_size_cm_legacy_raw` | mig_173 + mig_248 |
| `rai_received_flag` | parent view alias → `rai_received_reconciled` | pre-existing |

### Step 3.2 — Update candidate_n via cohort view (now ALL 63 are queryable)

Look up the cohort view from `manuscript_workspace.manuscript_dive_map_v1` for this `manuscript_id`. If a cohort view is mapped:
```sql
SELECT COUNT(*) FROM manuscript_workspace.<cohort_view_name>;
```
Or use the pre-materialized count from `manuscript_workspace.dive_cohort_size_v1` (built by mig_248 — same snapshot).

**No more cohort SELECT failures expected.** If you hit one, treat it as a regression — capture the error and STOP; surface to Logan via your final summary rather than silently falling back.

### Step 3.3 — Re-derive feasibility_color

Apply the same heuristic mig_247 used (preserve consistency):
- **GREEN**: all key_variables have ≥80% coverage AND candidate_n ≥ 100
- **YELLOW**: at least one key_variable has 30–80% coverage, OR candidate_n is 50–100
- **RED**: at least one key_variable has <30% coverage, OR candidate_n < 50, OR a key_variable is MISSING from current schema (after exhausting the rename inventory above), OR external data is required (zip codes, heavy metals, QoL, metabolomics, literature)

If color flips RED→GREEN because of mig_248 aliases, set `gating_issues` to:
> `post-mig_248: column-rename drift resolved (e.g., tirads_best_category_v12 alias active); RED→GREEN.`

If color stays RED for genuine coverage reasons, RETAIN the coverage portion of gating_issues but DELETE any column-MISSING text that mig_248 invalidated.

### Step 3.4 — Update gating_issues + recommended_next_step

For each row, the gating_issues field should reflect ONLY current blockers, not historical ones. Specifically:

- DELETE any clause containing `cohort SELECT failed` (mig_248 fixed all 63 cohort views)
- DELETE any clause containing `tumor_size_cm MISSING` (alias active)
- DELETE any clause containing `tirads_best_category_v12 MISSING` (alias active)
- DELETE any clause containing `syn_isthmus_size_cm` Binder Error (mig_248 fixed)
- KEEP coverage-percent clauses (e.g., "Histology 69pct in nucmed cohort") — those are real
- KEEP "external data required" clauses (zip codes, heavy metals, QoL) — mig_248 doesn't help
- APPEND the post-mig_248 status sentence (RED→GREEN, GREEN→GREEN refreshed, etc.)

For `recommended_next_step`:
- If RED→GREEN: `"Re-evaluate cohort; consider draft start"`
- If GREEN→GREEN (was already GREEN): keep existing, append `"; refreshed post-mig_248"`
- If still RED for coverage: keep existing recommendation

### Step 3.5 — Update scored_at + canonical_version

```sql
UPDATE manuscript_workspace.manuscript_feasibility_v1
SET scored_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
    canonical_version_at_scoring = 'v1_0_post_mig_248'
WHERE manuscript_id = <id>;
```

(Or rebuild the entire table via CREATE OR REPLACE TABLE if cleaner — preserve schema and PI-curated cols.)

---

## §4 — Output

### Files to author:
1. `qc_framework_v1/migrations/249_feasibility_re_refresh_20260501.sql` — full UPDATE/CREATE OR REPLACE TABLE statements applied
2. `memory/project_mig_249_feasibility_re_refresh_20260501.md` — agent's notes on:
   - Total rows refreshed (should be 83)
   - Color transitions vs mig_247 baseline (RED→GREEN, etc.) — itemize each flipped manuscript
   - Manuscripts that resolved via mig_248 aliases (M025, M029, M030, M037, M043, M045 expected; confirm or refute)
   - Manuscripts that stayed RED — distinguish "real coverage shortfall" from "external data required"
   - Any cohort views found broken during the refresh (regression — surface immediately)

### Verification (post-apply):

```sql
-- Should return 83 rows, all with the new watermark
SELECT COUNT(*) AS total_refreshed,
       MIN(scored_at) AS earliest_scored_at,
       MAX(scored_at) AS latest_scored_at
FROM manuscript_workspace.manuscript_feasibility_v1
WHERE canonical_version_at_scoring = 'v1_0_post_mig_248';

-- Color distribution post-refresh (compare to post-mig_247 baseline: 27 GREEN / 5 YELLOW / 51 RED)
SELECT feasibility_color, COUNT(*) AS n
FROM manuscript_workspace.manuscript_feasibility_v1
GROUP BY feasibility_color
ORDER BY CASE feasibility_color WHEN 'GREEN' THEN 1 WHEN 'YELLOW' THEN 2 WHEN 'RED' THEN 3 ELSE 4 END;

-- Confirm zero stale flags remain
SELECT
  COUNT(*) FILTER (WHERE gating_issues ILIKE '%cohort SELECT failed%') AS stale_cohort_select,
  COUNT(*) FILTER (WHERE gating_issues ILIKE '%syn_isthmus_size_cm%' AND gating_issues ILIKE '%Binder%') AS stale_isthmus_binder,
  COUNT(*) FILTER (WHERE gating_issues ILIKE '%tirads_best_category_v12 MISSING%') AS stale_tirads_v12,
  COUNT(*) FILTER (WHERE gating_issues ILIKE '%tumor_size_cm MISSING%') AS stale_tumor_size
FROM manuscript_workspace.manuscript_feasibility_v1;
-- Expected: all four counts = 0

-- Restoration spot-check (M025/M029/M030/M037/M043/M045 should now be GREEN)
SELECT manuscript_id, feasibility_color, candidate_n,
       LEFT(COALESCE(gating_issues, ''), 200) AS gating_issues_excerpt
FROM manuscript_workspace.manuscript_feasibility_v1
WHERE manuscript_id IN (25, 29, 30, 37, 43, 45)
ORDER BY manuscript_id;

-- Manuscript dashboard signals after refresh
SELECT draft_readiness_signal, COUNT(*) AS n,
       STRING_AGG(CAST(manuscript_id AS VARCHAR), ',' ORDER BY manuscript_id) AS ids
FROM manuscript_workspace.manuscript_dashboard_VIEW_v1
GROUP BY draft_readiness_signal
ORDER BY draft_readiness_signal;
-- Expected READY_TO_DRAFT growth from 3 → 6-9 (M032, M038, M039 + restoration set)
```

### Commit:
```
feat(qc): mig_249 — manuscript_feasibility_v1 re-refresh post-mig_248

Re-scored all 83 manuscripts against schema with mig_248's backward-compat
aliases active. Closes CF-MIG_247-RERUN from v20 handoff.

Color transitions vs mig_247 baseline: <agent fills in>
Restoration set (RED→GREEN via mig_248 aliases): <agent fills in>
Stale-flag cleanup: 0 cohort SELECT failed / 0 syn_isthmus Binder /
                    0 tirads_v12 MISSING / 0 tumor_size MISSING (verified)
New READY_TO_DRAFT count (via dashboard): <agent fills in>

Files:
- new: qc_framework_v1/migrations/249_feasibility_re_refresh_20260501.sql
- new: memory/project_mig_249_feasibility_re_refresh_20260501.md
- updated: manuscript_workspace.manuscript_feasibility_v1 (83 rows)
```

Then surgical `git push origin main`.

---

## §5 — Guardrails

- **Don't drop existing columns** from `manuscript_feasibility_v1` schema. Only update VALUES.
- **Don't change manuscript_id, title, status, priority, project_leaders, key_variables** — these are PI-curated. Only re-score the data-derived fields.
- **Don't re-introduce stale flags.** If a manuscript flips RED→GREEN, its gating_issues should NOT still mention the resolved column-rename issue. Strip it.
- **Don't flag a column MISSING without first checking the parent cohort view alias.** The §3.1 rename inventory is the canonical reference.
- **PHI safety**: `research_id` is the only patient identifier ever exposed. Don't print clinical_notes_long or note_entities_* contents.
- **Surgical git add** per `feedback_surgical_git_add.md` (don't `git add -A` — there are 100+ untracked files in the repo).
- **gate1=218 should NOT change** — `manuscript_feasibility_v1` is in `manuscript_workspace`, not registered in `canonical_table_signoff_registry_v1`.
- **If you hit a regression** (cohort view that errored where Cowork said it worked), STOP and surface to Logan via your final summary. Don't silently fall back.

---

## §6 — Reference

- Live state: HEAD `78b1ae3` (post-mig_248), gate1=218, gates 2-5=0/0/0/0, cohort_parity TRUE (10871×3)
- mig_248 SQL: `qc_framework_v1/migrations/248_column_rename_drift_repair_20260501.sql`
- mig_247 SQL: `qc_framework_v1/migrations/247_feasibility_refresh_20260501.sql` (the prior baseline)
- mig_247 dispatch (template for this one): `cursor_prompts/CURSOR_PROMPT_MIG_247_FEASIBILITY_REFRESH_20260501.md`
- v20 handoff (most recent): `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v20.md`
- Manuscript dashboard: `SELECT * FROM manuscript_workspace.manuscript_dashboard_VIEW_v1`
- Existing feasibility: `SELECT * FROM manuscript_workspace.manuscript_feasibility_v1`
- Dive↔manuscript map: `SELECT * FROM manuscript_workspace.manuscript_dive_map_v1`
- Materialized cohort sizes (mig_248 output): `SELECT * FROM manuscript_workspace.dive_cohort_size_v1`

**End of mig_249 dispatch.**
