# patient_completion_oed_path_linkage_v1

Global spine: all `research_id` in `operative_episode_detail_v2` ∪ `path_synoptics`. OED vs path-synoptic completion flags follow `studies/proposal_2to4cm_extent_molecular_20260326/cohort_logic.py`.

Rebuild: `.venv/bin/python scripts/107_global_completion_oed_path_linkage.py --md`
