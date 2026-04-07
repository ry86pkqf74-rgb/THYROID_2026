# Governance corrections — 2026-04-07 final master run

## Orchestrator outcome (`124_md_live_release_audit.py`)

The first `--final-release` attempt completed promotion, materialization, and snapshot **`release_20260407_final`**, then **failed validation** because `114` re-hydrated `qa.manual_review_queue` from a gate CSV with **null `verification_status`** for 5,622 promotable rows.

## Corrective actions

1. `DELETE FROM qa.manual_review_queue WHERE run_label = 'promotion_gate'` — remove unsigned re-hydrate bucket.
2. Re-run `scripts/114_qa_schema_setup.py --md --hydrate-from studies/20260409_final_master_release/mrq_hydrate_gate` — restores signed-off queue rows (`verification_status` populated).
3. Remove duplicate `run_label = 'gate'` rows (exact key overlap with `mrq_hydrate_gate`).
4. Rebuild `scripts/125_master_verified_views.py --md`.
5. Create **new** snapshot **`release_20260407_final2`** with `--final-master` (do not drop prior schemas).
6. Export `exports/parquet_release_20260407_final2/`.
7. `scripts/119_md_formalization_validate.py --md --release-mode` → **PASS** (20/20).

## Code change

`scripts/124_md_live_release_audit.py` now passes **`--final-master`** to `115` when **`--final-release`** is set so manuscript snapshots include labs and master verified tables by default.

## Recommendation

Teach `114` / `124` to use a **stable `run_label`** from `--run-label` instead of the hydrate directory leaf name (`promotion_gate`) to avoid accidental partial hydrates on re-runs.
