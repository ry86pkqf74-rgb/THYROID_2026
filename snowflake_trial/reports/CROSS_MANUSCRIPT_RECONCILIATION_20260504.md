# Cross-Manuscript Reconciliation — pub_v1_1_20260504
**Generated:** 2026-05-04T11:33:44.724325
**MD HEAD:** post-mig_300 (release_pub_v1_1_20260504)

## Headline numbers per manuscript (live MD)

| Manuscript | Cohort n | Events | PTC count | Sub-stat 1 | Sub-stat 2 |
|---|---:|---:|---:|---|---|
| M044 ETE | 4,013 | 499 (any recur) | 3,244 | gross=1,262 | micro=2,518 |
| M037 LN | 2,234 | 1,124 (N1+) | 1,893 | N1a=1,049 | N1b=75 |
| M025 TIRADS | 3,375 | 1,479 (malig) | 1,169 | n/a | n/a |
| M032 25-yr | 10,871 | 4,019 (malig) | 3,246 | smk=3,022 | fhx=3,018 |
| M038 Goiter | 10,871 | 4,019 (malig) | 3,246 | >=200g=475 | comp=583 |
| M004 Autoim | 10,871 | 4,019 (malig) | 3,246 | hashi=400 | graves=1,656 |
| PUB v1.1 | 10,871 | 4,019 (malig) | 3,246 | recur=514 | deceased=192 |

## Internal consistency checks

### Cohort hierarchy (full-cohort papers must equal PUB; subset papers must be ⊆ PUB)
- PUB=10,871 | M032=10,871 should equal PUB: ✓
- PUB=10,871 | M038=10,871 should equal PUB: ✓
- PUB=10,871 | M004=10,871 should equal PUB: ✓
- M044=4,013 should be subset of PUB malig 4,019: ✓
- M037=2,234 should be subset of PUB malig 4,019: ✓
- M025=3,375 should be subset of PUB 10,871: ✓

### PTC malignancy
- CPM PTC malig (gold standard) = **3,246**
- M044 cohort PTC count = 3,244 (subset filter)
- M037 cohort PTC count = 1,893 (subset filter)
- M044 PTC ≤ CPM PTC: ✓
- M037 PTC ≤ CPM PTC: ✓

### NLP-augmentation cohort scale (post-mig_281)
- Smoking known on M032: **3,022** (27.8% of cohort)
- Family-hx known on M032: **3,018** (27.8% of cohort)
- Hashimoto combined on M004: **400**
- Graves combined on M004: **1,656**

## Headline invariants for all 7 manuscripts

- **Total cohort:** 10,871
- **Total malignant:** 4,019 (post-mig_277 NIFTP carve-out; rate ~37.0%)
- **PTC malignant:** 3,246
- **Recurrence (any):** 514
- **Deceased:** 192
- **Release tag:** `pub_v1_1_20260504` (post-NLP-augmentation milestone)

## Pre-submission QC checklist (per manuscript)

1. Methods §Cohort cites total cohort = 10,871 + release tag pub_v1_1
2. Any cited PTC denominator within: M044 ≤ M037 ≤ CPM PTC = 3,246
3. Any RR/OR/HR reported with 95% CI
4. Reproducibility SQL package in 08_analysis_code/ runs against current MD
5. No `/Users/loganglosser/` paths leftover (mig_299 cleared 14)
6. M044 only: no v1.0 numbers in body (post-mig_295)
