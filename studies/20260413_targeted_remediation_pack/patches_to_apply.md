# Patches to apply (ordered, smallest safe)

All paths relative to repository root. **Review diff before apply.**

## Patch A — Deploy classification views (non-destructive)

- **File:** `scripts/sql/source_truth_confirmation_v1.sql` (already in repo)
- **Action:** Run `scripts/151_source_truth_confirmation_v1.py --md` so `v_fna_episode_bethesda_resolved_v1` and `v_imaging_nodule_linkage_classification_v1` exist on MotherDuck.
- **Addresses:** Consumption layer for Bethesda + linkage gaps; **does not** mutate fact tables.

## Patch B — Rebuild multimodal imaging↔FNA linkage

- **File:** `scripts/129_imaging_fna_linkage_mm_v1.py`
- **Action:** After optional logic tweak (see `candidate_sql_fixes.sql` / `candidate_python_fixes.py`), run:
  - `.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py --md`
- **Addresses:** `linkage_gap_worklist_unresolved_20260413_174900.csv` **lines 2–129** (128 nodules) where `candidate_fna_in_90d_window_but_no_mm_link`.

## Patch C — FNA episode Bethesda backfill (only if policy-approved)

- **Files:** `scripts/motherduck_seed_fna_episode_master_v2.py` (if used in your env) or targeted `UPDATE` via `candidate_sql_fixes.sql`
- **Addresses:** `fna_bethesda_conflicts.csv` only where **single gold source** is agreed — **not** bulk auto-update.

## Patch D — Documentation only (optional)

- **`docs/`:** Add one paragraph: downstream analytics should read `bethesda_resolved_num` from `v_fna_episode_bethesda_resolved_v1`, not raw `fna_episode_master_v2.bethesda_category` alone.

## Explicit non-patches

- **`scripts/50_multinodule_imaging.py`**: do not widen ±30d dedup without a new validation run (impacts `imaging_nodule_master_v1` grain).
- **Auto-fill** `imaging_nodule_master_v1.linked_fna_episode_id` — **deferred** (executive audit: column unused for linkage truth).
