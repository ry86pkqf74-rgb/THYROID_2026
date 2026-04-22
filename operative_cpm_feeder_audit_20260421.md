# CPM feeder audit — Script 362 (2026-04-22)

Read-only audit produced by Step 7 of Script 362. Identifies CPM operative-flavored columns (`nlp_*`, `op_*`, `operative_*`, `frozen_*`, `parathyroid_*`, `rln_*`, `ebl_*`) that may be sourced from `operative_episode_detail_v2`. A follow-up script will repoint these feeders to `canonical_operative_events_v1`.

**CPM total columns:** 1532 | **Operative-candidate columns:** 194

## Per-table grep hits (`git grep -l <table> -- scripts/`)

| deprecated table | feeder script files |
|---|---|
| `operative_episode_detail_v2` | `scripts/00_deid_gateway.py`, `scripts/100_canonical_metrics_registry.py`, `scripts/100_episode_linkage_v2_hardening.py`, `scripts/101_multi_episode_linkage_hardening.py`, `scripts/101_review_ops.py`, `scripts/103_fact_lineage_materialize.py`, `scripts/104_operative_truth_state_hardening.py`, `scripts/105_manuscript_freeze_v1.py`, `scripts/107_global_completion_oed_path_linkage.py`, `scripts/111_llm_extraction_validation.py` (+125 more) |

## Likely CPM column ↔ deprecated source matches

| CPM column | likely feeder table | matched source column |
|---|---|---|
| (none) | | |
