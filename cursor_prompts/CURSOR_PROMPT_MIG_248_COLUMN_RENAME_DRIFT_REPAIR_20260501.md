# Cursor Composer Dispatch — mig_248: Column-Rename Drift Repair (cohort views)

**Generated:** 2026-05-01 by Cowork at HEAD `5bbcee0`
**Lane:** mig_248 — find + repair every silently-broken cohort view in `manuscript_workspace`
**Recommended agent:** Cursor Composer (per-view probe + DDL repair is iterative; ~63 views to scan)
**Estimated runtime:** 20–45 min

---

## §0 — First message to paste into Cursor Composer

> mig_248 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_248_COLUMN_RENAME_DRIFT_REPAIR_20260501.md` end-to-end before any tool use. You have MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; database is `thyroid_canonical_publication_v1_0`. GitHub repo is at `/Users/loganglosser/THYROID_2026`. Use Desktop Commander for git ops (FileVault — `.git/index.lock` cleanup may be needed).

---

## §1 — Why this lane exists

mig_245 (Cowork-direct, HEAD `96e8ce3`) repaired 8 silently-broken views by fixing **inter-object** stale references — bare `_v2`/`_v1` identifiers that should have been `_VIEW_v2`/`_VIEW_v1`. Mig_245's regex scan covered 7 known _VIEW objects in `main`.

Today (during mig_246), a **DIFFERENT** kind of drift surfaced:
- `manuscript_workspace.cohort_m031_nuclear_medicine_v1` references column `syn_isthmus_size_cm` from `main.canonical_patient_master` alias `p`
- That column was RENAMED to `syn_isthmus_size_cm_legacy_raw` (with sibling `syn_isthmus_size_parse_status`)
- Querying the cohort view fails with: `Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm" — Candidate bindings: "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_status"`

This is **intra-table column-rename drift**, not inter-object DDL drift. mig_245's scan didn't catch it.

**Goal:** identify every cohort view in `manuscript_workspace.cohort_m0XX_*` (and adjacent manuscript views) that fails to query, identify the column-rename cause, and CREATE OR REPLACE each broken view with the correct column reference.

---

## §2 — Pre-task probes

```sql
-- Confirm tip state
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- All cohort views in manuscript_workspace (should be 63 from manuscript_dive_map_v1)
SELECT table_name FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name LIKE 'cohort_m0%'
ORDER BY table_name;

-- Other manuscript_workspace views worth scanning too:
SELECT table_name FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name NOT LIKE 'cohort_m0%'
ORDER BY table_name;
```

---

## §3 — Task spec

### Step 3.1 — Per-view queryability scan

For EACH cohort view in `manuscript_workspace.cohort_m0%`:
```sql
SELECT 'view_name' AS v, COUNT(*) AS row_count FROM manuscript_workspace.<view_name>;
```
Capture:
- view_name
- success / error
- if error: full error message (capture the missing column + candidate bindings)

(Use a tool/script that can iterate 63 queries with try/except. You can also batch using UNION ALL of `SELECT 1 FROM <view> LIMIT 0`, but if any branch fails the whole batch fails — so per-view is safer.)

### Step 3.2 — Identify the drift cause for each broken view

For each broken view:
1. Fetch DDL: `SELECT view_definition FROM information_schema.views WHERE table_schema='manuscript_workspace' AND table_name='<view_name>'`
2. Identify the column referenced in the error (e.g., `syn_isthmus_size_cm`)
3. Check current `canonical_patient_master` schema for the rename target:
   ```sql
   SELECT column_name FROM information_schema.columns
   WHERE table_schema='main' AND table_name='canonical_patient_master'
     AND (column_name LIKE '%<old_col_substring>%');
   ```
4. Determine the right replacement column. Heuristics:
   - If `<old_col>_legacy_raw` exists, the column was preserved as legacy and superseded by a normalized version. Choose normalized version if available; else `_legacy_raw` to preserve original semantics.
   - If `<old_col>_parse_status` exists alongside `<old_col>_legacy_raw`, the column was likely renormalized into multi-flag form.
   - Use migration history: search `qc_framework_v1/migrations/` for the mig that renamed it (likely a mig_17X or mig_18X based on timing).

### Step 3.3 — Repair each broken view

For each broken view:
- Fetch DDL via `SELECT view_definition FROM information_schema.views WHERE ...`
- Construct new DDL with corrected column reference
- Apply via `CREATE OR REPLACE VIEW manuscript_workspace.<view_name> AS <new_body>;`
- Verify by re-running `SELECT COUNT(*) FROM manuscript_workspace.<view_name>;`

### Step 3.4 — Edge case: variables that are MEANINGFULLY DIFFERENT post-rename

If a column was renamed to indicate a semantic change (not just a name change), flag the cohort view in `gating_issues` of `manuscript_feasibility_v1` and surface to Logan. Don't blindly rename — preserve original analytic intent. Example: if `syn_isthmus_size_cm` (size in cm) was split into `syn_isthmus_size_cm_legacy_raw` (raw, possibly mixed units) + `syn_isthmus_size_parse_status` (whether parse succeeded), the new analytic might need `WHERE syn_isthmus_size_parse_status = 'parsed' AND syn_isthmus_size_cm_legacy_raw IS NOT NULL` plus a unit cast.

For each non-trivial rename, document in the agent's memory note.

---

## §4 — Output

### Files to author:
1. `qc_framework_v1/migrations/248_column_rename_drift_repair_20260501.sql` — full CREATE OR REPLACE statements + comments documenting each rename's cause + treatment
2. `memory/project_mig_248_column_rename_drift_repair_20260501.md` — agent's notes:
   - Per-view scan summary (63 cohort + N other views, M broken)
   - Per-broken-view: original column → replacement column → semantic check
   - Any views FLAGGED for Logan review (semantic ambiguity)

### Verification (post-apply):
```sql
-- Re-test each previously-broken view (should all return COUNT(*) without errors)
SELECT 'cohort_m031' AS v, COUNT(*) FROM manuscript_workspace.cohort_m031_nuclear_medicine_v1
UNION ALL SELECT 'cohort_m0XX', COUNT(*) FROM manuscript_workspace.<other_repaired>
... ;

-- Now retry the dive_cohort_size CTAS that was blocked in mig_246
-- (if all 63 views work, the CTAS from mig_246 §3 carry-forward should succeed):
CREATE OR REPLACE TABLE manuscript_workspace.dive_cohort_size_v1 AS
  SELECT 'cohort_m001_indeterminate_genetics_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
  FROM manuscript_workspace.cohort_m001_indeterminate_genetics_v1
  UNION ALL ... ;
-- (Generate from manuscript_workspace.manuscript_dive_map_v1's distinct cohort_view_name list.)
```

If the post-mig_248 dive_cohort_size_v1 CTAS succeeds, optionally extend `manuscript_dashboard_VIEW_v1` to JOIN `dive_cohort_size_v1` and expose live `current_cohort_n` alongside `scored_at_candidate_n`. (mig_249 if substantial; otherwise inline in mig_248.)

### Commit:
```
feat(qc): mig_248 — column-rename drift repair across cohort views

Repaired N cohort views in manuscript_workspace silently broken due to
canonical_patient_master column renames post-original cohort-view authoring.

Renames detected + repaired:
- syn_isthmus_size_cm -> syn_isthmus_size_cm_legacy_raw (cohort_m031 + ?)
- <other renames as discovered>

Views previously broken, now queryable:
- manuscript_workspace.cohort_m031_nuclear_medicine_v1 (1148 rows)
- <others>

Live cohort sizes now computable: dive_cohort_size_v1 table populated
(from earlier-blocked mig_246 CTAS).

Files:
- new: qc_framework_v1/migrations/248_column_rename_drift_repair_20260501.sql
- new: memory/project_mig_248_column_rename_drift_repair_20260501.md
- updated: <list of N cohort view DDLs in manuscript_workspace>
```

Surgical `git add` (per `feedback_surgical_git_add.md`), then `git push origin main`.

---

## §5 — Guardrails

- **Don't change column LISTS** in cohort view DDLs unless the original column was definitively renamed. If unclear, FLAG for Logan instead of silently dropping.
- **Don't touch base tables** (`canonical_patient_master`, `canonical_*`). This lane is view-DDL only.
- **PHI safety**: never SELECT * from `clinical_notes_long`, `note_entities_*`, or other PHI surfaces during your probe.
- **gate1=218 should NOT change** — manuscript_workspace cohort views aren't registered in `canonical_table_signoff_registry_v1`.
- **Idempotent**: use `CREATE OR REPLACE VIEW`, not DROP + CREATE. Don't break dependent views.
- **Path-C verify** each repair: re-run COUNT(*) on the just-repaired view post-apply.

---

## §6 — Reference

- Live state: HEAD `5bbcee0` (post-mig_246), gate1=218
- mig_245 stale-ref repair (sister lane, different drift type): `qc_framework_v1/migrations/245_stale_view_ref_repair_20260501.sql`
- mig_246 dashboard (where cohort_m031 was first surfaced): `qc_framework_v1/migrations/246_manuscript_dashboard_VIEW_v1_20260501.sql`
- Manuscript dashboard: `SELECT * FROM manuscript_workspace.manuscript_dashboard_VIEW_v1`
- Dive map (the 63 cohort views to scan): `SELECT cohort_view_name FROM manuscript_workspace.manuscript_dive_map_v1`

**End of mig_248 dispatch.**
