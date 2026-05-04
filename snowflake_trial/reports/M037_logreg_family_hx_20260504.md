# M037 — LN Predictors logreg (post-mig_286 family-hx augment)
**Generated:** 2026-05-04
**Cohort:** n=2147 (complete-case)
**Outcome:** LN_pos (N1 vs N0)
**Predictor of interest:** family_hx_thyroid (n_present=133)

## Logreg results (OR + 95% CI)

|                |     OR |   OR_CI_low |   OR_CI_high |   P>|z| |
|:---------------|-------:|------------:|-------------:|--------:|
| Intercept      | 1.3490 |      1.0020 |       1.8170 |  0.0486 |
| C(sex)[T.male] | 1.8120 |      1.4850 |       2.2100 |  0.0000 |
| fhx_thy        | 1.0550 |      0.7370 |       1.5110 |  0.7698 |
| age_at_surgery | 0.9820 |      0.9770 |       0.9880 |  0.0000 |
| tumor_size_cm  | 1.1770 |      1.1190 |       1.2370 |  0.0000 |

Pseudo-R² (McFadden) = 0.0400
LR vs null χ² = 118.97, df = 4