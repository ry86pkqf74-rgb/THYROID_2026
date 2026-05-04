# M004 — Autoimmune × Malignancy (NLP-augmented Option 2)
**Generated:** 2026-05-04
**Cohort:** n=10,871 (complete-case)

## Concordance (NLP vs syn_*)

### Hashimoto
| metric | n |
|---|---:|
| n_pts_total | 10871 |
| n_either | 400 |
| n_both | 31 |
| n_syn_only | 217 |
| n_nlp_only | 152 |

### Graves
| metric | n |
|---|---:|
| n_pts_total | 10871 |
| n_either | 1656 |
| n_both | 304 |
| n_syn_only | 270 |
| n_nlp_only | 1082 |

## Combined autoimmune × malignancy (NLP+syn)

| category | n | n_malig | %_malig |
|---|---:|---:|---:|
| A_both | 52 | 21.0 | 40.4% |
| B_hashimoto_only | 348 | 153.0 | 44.0% |
| C_graves_only | 1,604 | 554.0 | 34.5% |
| D_neither | 8,867 | 3,291.0 | 37.1% |

## Logreg — predictors of malignancy

|                |     OR |   OR_CI_low |   OR_CI_high |   P>|z| |
|:---------------|-------:|------------:|-------------:|--------:|
| Intercept      | 0.8090 |      0.7020 |       0.9330 |  0.0035 |
| C(sex)[T.male] | 1.5830 |      1.4430 |       1.7370 |  0.0000 |
| has_hashi      | 1.3720 |      1.1200 |       1.6810 |  0.0022 |
| has_graves     | 0.8740 |      0.7820 |       0.9760 |  0.0172 |
| age_at_surgery | 0.9920 |      0.9890 |       0.9940 |  0.0000 |

Pseudo-R² (McFadden): **0.0093**; LR vs null χ²: **132.63** (df=4)

## Headline

- **Hashimoto:** has_hashi aOR 1.37 (95% CI 1.12–1.68), p=0.002247
- **Graves:** has_graves aOR 0.87 (95% CI 0.78–0.98), p=0.01717

Note: Graves remains paradoxically protective in NLP-augmented analysis — likely confounding by surgical indication (Graves operated for thyrotoxicosis vs nodule workup).