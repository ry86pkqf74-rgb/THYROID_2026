# M044 sensitivity — Gross-vs-Microscopic aOR with smoking covariate
**Generated:** 2026-05-04
**Cohort:** strict-DTC + ETE in (none/micro/gross), n=3275
**Outcome:** path-proven recurrence (n_events=400)

## Gross-vs-Microscopic aOR — sensitivity to smoking covariate

| Spec | n | events | aOR | 95% CI | p |
|---|---:|---:|---:|---|---:|
| Baseline (no smoking) | 3149 | 386 | 1.29 | (1.02-1.63) | 0.03103 |
| + NLP smoking | 960 | 124 | 1.36 | (0.90-2.05) | 0.1456 |
| + smoking_combined (NLP+NSQIP) | 1320 | 169 | 1.47 | (1.04-2.09) | 0.02885 |

## Interpretation

If aOR remains close to v1.1 baseline (~2.08) across all 3 specs, smoking is NOT confounding the ETE effect. M044 v1.1 primary spec stands as-is.
If aOR shifts >25%, smoking should be added to primary model (manuscript revision needed).
