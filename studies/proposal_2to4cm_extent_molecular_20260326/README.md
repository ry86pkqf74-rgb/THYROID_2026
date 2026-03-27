# Proposal: 2–4 cm extent + molecular

Run: `.venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/study_pipeline.py`

Requires `MOTHERDUCK_TOKEN`.

**Completion thyroidectomy:** `study_pipeline.py` writes **dual-definition** metrics to `table7_completion_thyroidectomy.csv` (OED pipeline vs path-synoptic definite) and updates `fig_completion_rates.png` / `initial_ultimate_extent_transition_counts.csv`. Independent audit bundle: `.venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/run_completion_audit_motherduck.py` → `completion_audit_outputs/`.
