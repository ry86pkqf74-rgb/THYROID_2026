# H2 v3 Task 5 — closeout (2026-05-06)

## Deliverables

- `studies/hypothesis2_goiter_sdoh/build_h2_v3.py` — BigQuery pull, omnibus tests, hypopara logit (Wald *p* in `hypopara_logit.pvalues`), Fisher (RLN / VC paralysis), figures.
- `studies/hypothesis2_goiter_sdoh/h2_v3_stats.json` — frozen numbers for Tables 1–5 and figures.
- `studies/hypothesis2_goiter_sdoh/figures_v3/` — Figure 1–3 PNG + SVG.
- `studies/hypothesis2_goiter_sdoh/H2_manuscript_v3_20260506.md` — full manuscript; Black/AA hypopara Wald *p* = 0.47 (from stats JSON).

## Governance

- MFL **MFL-20260506-003** before v3 manuscript write (Airtable).

## Re-run

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/thyroid-pub-loader-key.json"
.venv/bin/python studies/hypothesis2_goiter_sdoh/build_h2_v3.py
```

## Primary inferential headline

- Adjusted hypopara OR Black/AA vs White ≈ **1.13** (95% CI **0.81–1.57**), **Wald *p* ≈ 0.47** — not significant at α = 0.05.
- CT substernal extension OR ≈ **2.00** (95% CI **1.34–2.98**).
