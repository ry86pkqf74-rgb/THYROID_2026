# Revision rerun (2026-03-26)

Self-contained PSM replay on current `exports/*.csv`. Outputs are **not** the manuscript primary; they document reproducibility vs [`../audit_tables/table6_propensity_matching_effect.csv`](../audit_tables/table6_propensity_matching_effect.csv).

- `run_psm_reproduction.py` — standalone script (creates local `.venv` if you run `python3 -m venv .venv && .venv/bin/pip install scikit-learn pandas scipy numpy`).
- `psm_reproduction_summary.txt` — pool and pair counts from last run.
- `table6_propensity_matching_effect_rerun.csv` — effect row from last run.

**Last run:** 712 pairs vs 711 frozen; OR 1.30 / p 0.13 vs frozen 1.43 / p 0.03 (see main packet).
