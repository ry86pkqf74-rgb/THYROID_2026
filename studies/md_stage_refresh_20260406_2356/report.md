> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# MotherDuck staging refresh + QA hydration

**Study folder:** `studies/md_stage_refresh_20260406_2356/`  
**Gate `--run-label`:** `md_stage_refresh_20260406_2356`  
**Gate artifact directory:** `studies/md_stage_refresh_20260406_2356/gate/`

## Execution summary

1. **Preflight** (read-only): `preflight/preflight_report.md`
2. **`116_md_stage_loader.py --md`:** 30 `v2_stage` tables refreshed; load inventory updated; post-load row parity OK for all tables.
3. **`112_v2_domain_promotion_gate.py --motherduck-check`:** **PASS** (G1–G8). Artifacts under `gate/`.
4. **`114_qa_schema_setup.py --md --hydrate-from …/gate`:** QA tables hydrated. Hydration **DELETE/INSERT** uses `run_label = 'gate'` (basename of `--hydrate-from`), not the gate script’s `md_stage_refresh_*` label — see `114_qa_schema_setup.py`.

## This-run hydration (from `114` stdout)

| Table | Rows inserted (this hydrate) |
|-------|------------------------------|
| `qa.promotion_scorecard` | 8 |
| `qa.domain_validation` | 23 |
| `qa.concordance_summary` | 19 |
| `qa.manual_review_queue` | 5622 |

## Total row counts in MotherDuck after this task

Counts below are **table totals** (may include earlier runs with other `run_label` values).

| Object | Count |
|--------|------:|
| `v2_stage.load_inventory` | 90 |
| `qa.promotion_scorecard` | 40 |
| `qa.domain_validation` | 115 |
| `qa.concordance_summary` | 95 |
| `qa.manual_review_queue` | 11244 |

## Out of scope (per task)

- No `115_release_snapshot.py` / new `release_YYYYMMDD` schema created in this run. Pre-existing release schemas in the catalog are unrelated.

## Evidence

- Gate scorecard: `gate/promotion_scorecard.csv`
- `116` parity lines in terminal log; staging verified on MotherDuck with fail-closed connections throughout.
