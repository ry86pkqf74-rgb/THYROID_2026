# M037 — manuscript number helper (20260504)

**Cohort (M037 view × CPM join):** n = 2,234
**LN-positive (AJCC N1+):** 1,124 (50.31%)
**NLP family hx thyroid (TRUE):** 141

## Cowork headline (post–mig_286)

- Family hx aOR **1.05** (0.74–1.51), p = 0.77 (null association).
- Male sex aOR **1.81**; age OR **0.98**/yr; tumor size OR **1.18**/cm — verify against `Table2b_primary_coef` after rebuild.

## Regenerate

```bash
.venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_tables.py
.venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_figures.py
.venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_manuscript_md.py
```
