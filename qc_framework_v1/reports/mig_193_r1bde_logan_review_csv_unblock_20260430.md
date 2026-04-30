# mig_193 r1b/r1d/r1e Logan-review CSV unblock + post-mig_188b regeneration

**Run ID:** `mig_193_r1bde_logan_review_csv_unblock_20260430`  
**Generated:** 2026-04-30T04:40:23.915826+00:00  
**Posture:** READ-ONLY MotherDuck SELECTs + local CSV/report authoring only.  
**Target DB:** `thyroid_canonical_publication_v1_0`

## 1. Pre-flight gate

| Check | Result |
|---|---:|
| mig_188b registry/verification rows | 46 |
| `canonical_path_malignant_events_v1.t_stage_ajcc8_resolved` non-null | 6467 |
| `canonical_patient_master.ajcc8_t_stage_resolved` non-null | 4021 |

**Gate:** PASS. The lane ran against the post-mig_188b state.

## 2. r1b 0-row diagnosis

The original r1b exact filter returned **0** rows. This is not a SQL casing/whitespace bug: the legacy PM `ajcc8_n_stage` distribution has no plain `N1` values. Post-mig_188b, `ajcc8_n_stage_resolved` also has **0** unresolved plain-`N1` rows.

Legacy PM N-stage distribution:

| n_stage   |    n |
|:----------|-----:|
| NULL      | 5381 |
| N0        | 2640 |
| N1a       | 2562 |
| Nx        |  205 |
| N1b       |   83 |

Resolved PM N-stage distribution:

| n_stage   |    n |
|:----------|-----:|
| NULL      | 5375 |
| N0        | 2640 |
| N1a       | 2562 |
| Nx        |  205 |
| N1b       |   83 |
| NX        |    6 |

Interpretation: by the time this lane ran, N1 had already been split or normalized upstream (mainly into `N1a` and `N1b`). Therefore the correct post-mig_188b r1b review bundle is header-only / 0 rows, and no Logan N1-unspecified PM-grain adjudication is pending.

## 3. r1b post-mig_188b inventory

`r1b_n1_unspecified_pm_grain_post_mig188.csv`: **0** rows.

## 4. r1d T4 invasion inventory

`r1d_t4_invasion_post_mig188.csv`: **387** rows.  
Rows with `mig_188_caught_t4=TRUE`: **40** / 387.

The CSV includes invasion-event evidence fields plus current `t_stage_ajcc8_resolved` and `t_resolution_source` for Logan review.

## 5. r1e mixed-histology inventory

`r1e_mixed_histology_post_mig188.csv`: **168** rows.

The CSV includes the current resolved stage group and a computed proposed most-aggressive component for Rule #5 review.

## 6. r1c disposition CSV row counts post-apply

| CSV | Rows |
|---|---:|
| r1c_disposition_strong_prior_thy.csv | 54 |
| r1c_disposition_weak_or_none.csv | 13 |
| r1c_disposition_ambiguous_pm_only.csv | 50 |

These reflect the final mig_188b explicit-T0 state: 54 prior-thy carry-forward rows, 13 no-primary pT0/unstaged rows, and 50 ambiguous PM-size-only rows.

## 7. Logan review unblock checklist

- [x] Post-mig_188b gate verified.
- [x] r1b 0-row result diagnosed as data-state normalization/splitting rather than a failed CSV build.
- [x] r1b/r1d/r1e CSVs regenerated under `exports/mig193_r1_adjudication_post_mig188_20260430/`.
- [x] r1c disposition CSVs regenerated from the post-apply state.
- [x] Manifest written with row counts and diagnostic distributions.

## 8. Deliverables

- `qc_framework_v1/reports/mig_193_r1bde_logan_review_csv_unblock_20260430.md`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1b_n1_unspecified_pm_grain_post_mig188.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1d_t4_invasion_post_mig188.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1e_mixed_histology_post_mig188.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1c_disposition_strong_prior_thy.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1c_disposition_weak_or_none.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1c_disposition_ambiguous_pm_only.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/manifest.json`
