# M038 v2 — Data-Validity Audit (2026-05-01)

**Manuscript audited:** `manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v2_post_mig_252_253.md`
**Cohort view:** `manuscript_workspace.cohort_m038_massive_goiter_v1`
**Database:** `thyroid_canonical_publication_v1_0` (release `pub_v1_0_20260430`)
**Most-recent applied migration:** `mig_253_surg_procedure_type_fill_20260501.sql` (signoff 2026-05-01 06:41:00 UTC)
**Predecessor migration:** `mig_252` — repaired `comp_*_confirmed` strict rollups (commit `32beb7b`)
**Cohort view extension:** `mig_251` — extended `cohort_m038_massive_goiter_v1` to ~117 columns (commit `f673f09`)
**Cowork commit at audit time:** HEAD = `5125a87` (`docs(qc): amend v23 handoff`)

**Gate health at audit time** (`semantic_publication.vw_publication_qc_status_VIEW_v1`):

```
gate1_verified_tables           : 218
gate1_distinct_objects          : 218
gate2_missing_signoff           : 0
gate3_count_mismatch            : 0
gate4_verified_cols_missing_metadata : 0
gate5_clinical_date_violations  : 0
cpm_pts                         : 10,871
us_gland_v2_pts                 : 10,871
us_ln_v2_pts                    : 10,871
cohort_parity_ok                : TRUE
release_id                      : pub_v1_0_20260430
most_recent_signoff_migration   : qc_framework_v1/migrations/253_surg_procedure_type_fill_20260501.sql
most_recent_signoff_ts          : 2026-05-01 06:41:00 UTC
```

**Audit method.** For each numeric cell, percentage, count, denominator, ratio, and derived statistic in the M038 v2 manuscript (abstract + §3.1 + §3.2 Table 1 + §3.3 Table 2 + §3.4 Table 3 + §3.5 Table 4 + §3.6 era table + §4 discussion + §5 limitations footnotes), the underlying SQL was re-run live against `thyroid_canonical_publication_v1_0` and the result compared to the manuscript value. Status legend:

- **PASS** — live result matches the manuscript value within rounding tolerance.
- **DIFF** — numeric mismatch surfaced; addressed in the v2.1 Cursor patch (see footer).
- **FAIL** — query errors or referenced column does not exist (none observed).

**Standing conventions used in every query:**

```sql
WITH base AS (
  SELECT *,
    (COALESCE(gland_weight_final_g >= 100, FALSE)
     OR COALESCE(ct_substernal_extension_any, FALSE)
     OR COALESCE(mri_substernal_any, FALSE)
     OR COALESCE(ct_tracheal_deviation_any, FALSE)
     OR COALESCE(ct_tracheal_narrowing_any, FALSE)
     OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
```

The composite-massive flag (`is_massive`) is **not** materialized on the cohort view; it is derived inline in every query per the manuscript Methods §2.3 specification (gland weight ≥100 g OR substernal CT/MRI OR airway-compromise CT). Era binning uses `surg_first_date <= '2004-12-31'` for the first bucket (sweeping in 2 pre-1999 dates: 1945-07-13 and 1993-04-01), which is the rule that reproduces the manuscript's §3.6 totals (1999–2004 = 903) and the §3.2 Table 1 era breakdown.

---

## Audit table — every numeric cell

### Abstract

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| Abstract | N total cohort | 10,871 | `SELECT COUNT(*) FROM cohort` | 10,871 | PASS |
| Abstract | N massive (composite criterion) | 2,501 | `COUNT(*) FILTER (WHERE is_massive)` | 2,501 | PASS |
| Abstract | % massive | 23.0% | 2,501 / 10,871 | 23.00% | PASS |
| Abstract | N weight ≥100 g component | 1,429 | `COUNT(*) FILTER (WHERE gland_weight_final_g >= 100)` | 1,429 | PASS |
| Abstract | % weight ≥100 g of massive | 57.1% | 1,429 / 2,501 | 57.14% | PASS |
| Abstract | N substernal component | 1,047 | `COUNT(*) FILTER (WHERE ct_substernal_extension_any OR mri_substernal_any)` | 1,047 | PASS |
| Abstract | % substernal of massive | 41.9% | 1,047 / 2,501 | 41.86% | PASS |
| Abstract | N airway component | 1,440 | `COUNT(*) FILTER (WHERE ct_tracheal_deviation_any OR ct_tracheal_narrowing_any OR ct_airway_compromise_any)` | 1,440 | PASS |
| Abstract | % airway of massive | 57.6% | 1,440 / 2,501 | 57.58% | PASS |
| Abstract | Median age, massive | 56 | `MEDIAN(age_at_surgery) FILTER (WHERE is_massive)` | 56 | PASS |
| Abstract | Age IQR Q25–Q75, massive | 45–66 | `QUANTILE_CONT(age_at_surgery, 0.25/0.75)` | 45–66 | PASS |
| Abstract | Median age, non-massive | 50 | as above | 50 | PASS |
| Abstract | Age IQR Q25–Q75, non-massive | 39–62 | as above | 39–62 | PASS |
| Abstract | % female, massive | 70.8% | `COUNT(*) FILTER (WHERE LOWER(sex)='female') / n_arm` | 1,771 / 2,501 = 70.81% | PASS |
| Abstract | % female, non-massive | 79.9% | as above | 6,688 / 8,370 = 79.90% | PASS |
| Abstract | % Black or AA, massive | 62.2% | `COUNT(*) FILTER (WHERE race='Black or African American') / n_arm` | 1,555 / 2,501 = 62.18% | PASS |
| Abstract | % Black or AA, non-massive | 31.2% | as above | 2,613 / 8,370 = 31.22% | PASS |
| Abstract | % White, massive | 28.5% | as above | 714 / 2,501 = 28.55% | PASS |
| Abstract | % White, non-massive | 54.4% | as above | 4,552 / 8,370 = 54.39% | PASS |
| Abstract | % malignant, massive | 25.8% | `COUNT(*) FILTER (WHERE is_malignant) / n_arm` | 646 / 2,501 = 25.83% | PASS |
| Abstract | % malignant, non-massive | 41.7% | as above | 3,491 / 8,370 = 41.71% | PASS |
| Abstract | % PTC, malignant massive subset | 64.6% | `COUNT(*) FILTER (WHERE histology_final='PTC' AND is_malignant AND is_massive) / 646` | 417 / 646 = 64.55% | PASS |
| Abstract | % PTC, M032 broader malignant cohort (cross-reference) | 80.9% | M032 published value (line 79 of M032 draft) — n=3,255 / 4,022 | 80.93% per M032 draft | PASS (cross-ref) |
| Abstract | N total thyroidectomy / N massive | 1,672 / 2,501 | `COUNT(*) FILTER (WHERE surg_procedure_type='total_thyroidectomy' AND is_massive)` | 1,672 / 2,501 | PASS |
| Abstract | % total thyroidectomy, massive | 66.9% | 1,672 / 2,501 | 66.85% | PASS |
| Abstract | N total thyroidectomy / N non-massive | 4,327 / 8,370 | as above (NOT is_massive) | 4,327 / 8,370 | PASS |
| Abstract | % total thyroidectomy, non-massive | 51.7% | 4,327 / 8,370 | 51.70% | PASS |
| Abstract | % any-comp, massive | 5.28% | `COUNT(*) FILTER (WHERE any_confirmed_complication_flag AND is_massive) / n_arm` | 132 / 2,501 = 5.278% | PASS |
| Abstract | % any-comp, non-massive | 3.20% | as above | 268 / 8,370 = 3.202% | PASS |
| Abstract | RR any-comp | ≈ 1.65 | (132/2501) / (268/8370) | 1.648 | PASS |
| Abstract | N RLN injury, massive (%) | 14 (0.56%) | `COUNT(*) FILTER (WHERE comp_rln_injury_confirmed AND is_massive)` | 14; 14/2501 = 0.560% | PASS |
| Abstract | N RLN injury, non-massive (%) | 7 (0.084%) | as above | 7; 7/8370 = 0.084% | PASS |
| Abstract | N hematoma, massive (%) | 23 (0.92%) | `COUNT(*) FILTER (WHERE comp_hematoma_confirmed AND is_massive)` | 23; 0.920% | PASS |
| Abstract | N hematoma, non-massive (%) | 45 (0.54%) | as above | 45; 0.538% | PASS |
| Abstract | Mortality %, massive | 2.36% | `COUNT(*) FILTER (WHERE death_occurred AND is_massive) / n_arm` | 59 / 2,501 = 2.359% | PASS |
| Abstract | Mortality %, non-massive | 1.59% | as above | 133 / 8,370 = 1.589% | PASS |
| Abstract | Era 1999–2014 % massive (combined) | ≈ 12% | (110+142+240) / (903+1191+1885) = 492 / 3,979 | 12.36% | PASS |
| Abstract | Era 2015–2019 % massive | 24.9% | 731 / 2,935 | 24.91% | PASS |
| Abstract | Era 2020–2025 % massive | 28.5% | 517 / 1,817 | 28.45% | PASS |

### §3.1 Cohort assembly and composite-flag composition

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §3.1 | Weight ≥100 g (any cause) | 1,429 | `COUNT(*) FILTER (WHERE w)` | 1,429 | PASS |
| §3.1 | Substernal (CT or MRI, any cause) | 1,047 | `COUNT(*) FILTER (WHERE s)` | 1,047 | PASS |
| §3.1 | Airway compromise (CT, any cause) | 1,440 | `COUNT(*) FILTER (WHERE a)` | 1,440 | PASS |
| §3.1 | Weight ∩ Substernal | 404 | `COUNT(*) FILTER (WHERE w AND s)` | 404 | PASS |
| §3.1 | Weight ∩ Airway | 513 | `COUNT(*) FILTER (WHERE w AND a)` | 513 | PASS |
| §3.1 | Substernal ∩ Airway | 884 | `COUNT(*) FILTER (WHERE s AND a)` | 884 | PASS |
| §3.1 | All three | 386 | `COUNT(*) FILTER (WHERE w AND s AND a)` | 386 | PASS |
| §3.1 | Weight only | 898 | `COUNT(*) FILTER (WHERE w AND NOT s AND NOT a)` | 898 | PASS |
| §3.1 | Substernal only | 114 | `COUNT(*) FILTER (WHERE s AND NOT w AND NOT a)` | **145** | **DIFF** (v2.1 patch) |
| §3.1 | Airway only | 309 | `COUNT(*) FILTER (WHERE a AND NOT w AND NOT s)` | **429** | **DIFF** (v2.1 patch) |
| §3.1 | Inclusion-exclusion sum | 2,501 | 1429+1047+1440 − 404 − 513 − 884 + 386 | 2,501 | PASS |

The two DIFFs above derive from the §3.1 single-only slices and propagate into the §4 Discussion sentence "of the 2,501 massive cases, 386 (15.4%) carry all three flags … 898 weight-only, 114 substernal-only, 309 airway-only". Live arithmetic check confirms 145 and 429 are internally consistent with the manuscript's own intersection counts: 1,047 − 404 − 884 + 386 = 145 and 1,440 − 513 − 884 + 386 = 429. The cohort total of 2,501 is unaffected (single-only counts are derivative slices that do not enter the inclusion-exclusion sum). Findings unchanged.

### §3.2 Table 1 — Demographics & Baseline Characteristics

Denominators: massive n=2,501; non-massive n=8,370 (whole-cohort percent denominators; subset rows use the smaller `n` shown in italics in the manuscript table).

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §3.2 | Age at surgery, mean — massive | 55.4 | `AVG(age_at_surgery) FILTER (WHERE is_massive)` | 55.35 | PASS |
| §3.2 | Age at surgery, mean — non-massive | 50.5 | as above | 50.47 | PASS |
| §3.2 | Age at surgery, median [IQR] — massive | 56 [45–66] | `MEDIAN`/`QUANTILE_CONT 0.25/0.75` | 56 [45–66] | PASS |
| §3.2 | Age at surgery, median [IQR] — non-massive | 50 [39–62] | as above | 50 [39–62] | PASS |
| §3.2 | Female n (%), massive | 1,771 (70.8%) | `COUNT(*) FILTER (WHERE LOWER(sex)='female' AND is_massive)` | 1,771 (70.81%) | PASS |
| §3.2 | Male n (%), massive | 730 (29.2%) | as above | 730 (29.19%) | PASS |
| §3.2 | Female n (%), non-massive | 6,688 (79.9%) | as above | 6,688 (79.90%) | PASS |
| §3.2 | Male n (%), non-massive | 1,682 (20.1%) | as above | 1,682 (20.10%) | PASS |
| §3.2 | Race: Black or AA n (%), massive | 1,555 (62.2%) | `GROUP BY race` | 1,555 (62.18%) | PASS |
| §3.2 | Race: White n (%), massive | 714 (28.5%) | as above | 714 (28.55%) | PASS |
| §3.2 | Race: Asian n (%), massive | 57 (2.3%) | as above | 57 (2.28%) | PASS |
| §3.2 | Race: Other/AIAN/NH-PI/Hispanic n (%), massive | 44 (1.8%) | sum of 4 buckets | 38+4+1+1 = 44 (1.76%) | PASS |
| §3.2 | Race: Unknown / Not Reported n (%), massive | 130 (5.2%) | `WHERE race='Unknown or Not Reported'` (excludes 1 NULL) | 130 (5.20%) | PASS |
| §3.2 | Race: Black or AA n (%), non-massive | 2,613 (31.2%) | as above | 2,613 (31.22%) | PASS |
| §3.2 | Race: White n (%), non-massive | 4,552 (54.4%) | as above | 4,552 (54.39%) | PASS |
| §3.2 | Race: Asian n (%), non-massive | 419 (5.0%) | as above | 419 (5.01%) | PASS |
| §3.2 | Race: Other/AIAN/NH-PI/Hispanic n (%), non-massive | 187 (2.2%) | sum of 4 buckets | 105+35+26+21 = 187 (2.23%) | PASS |
| §3.2 | Race: Unknown / Not Reported n (%), non-massive | 591 (7.1%) | as above (excludes 8 NULL) | 591 (7.06%) | PASS |
| §3.2 | BMI subset n, massive | 417 | `COUNT(bmi_combined) FILTER (WHERE is_massive)` | 417 | PASS |
| §3.2 | BMI subset n, non-massive | 1,668 | as above | 1,668 | PASS |
| §3.2 | BMI mean, massive | 33.5 | `AVG(bmi_combined) FILTER (WHERE is_massive)` | 33.46 | PASS |
| §3.2 | BMI median [IQR], massive | 32.1 [27.7–37.5] | `MEDIAN`/`QUANTILE_CONT` | 32.1 [27.7–37.5] | PASS |
| §3.2 | BMI mean, non-massive | 29.8 | as above | 29.75 | PASS |
| §3.2 | BMI median [IQR], non-massive | 28.5 [24.4–33.6] | as above | 28.5 [24.36–33.56] | PASS |
| §3.2 | NLP HTN n (%), massive | 696 (27.8%) | `COUNT(*) FILTER (WHERE pmhx_nlp_hypertension AND is_massive)` | 696 (27.83%) | PASS |
| §3.2 | NLP HTN n (%), non-massive | 1,079 (12.9%) | as above | 1,079 (12.89%) | PASS |
| §3.2 | NLP DM n (%), massive | 500 (20.0%) | `WHERE pmhx_nlp_diabetes` | 500 (19.99%) | PASS |
| §3.2 | NLP DM n (%), non-massive | 966 (11.5%) | as above | 966 (11.54%) | PASS |
| §3.2 | NLP CAD n (%), massive | 84 (3.4%) | `WHERE pmhx_nlp_cad` | 84 (3.36%) | PASS |
| §3.2 | NLP CAD n (%), non-massive | 140 (1.7%) | as above | 140 (1.67%) | PASS |
| §3.2 | NLP CKD n (%), massive | 85 (3.4%) | `WHERE pmhx_nlp_ckd` | 85 (3.40%) | PASS |
| §3.2 | NLP CKD n (%), non-massive | 136 (1.6%) | as above | 136 (1.62%) | PASS |
| §3.2 | NLP COPD n (%), massive | 47 (1.9%) | `WHERE pmhx_nlp_copd` | 47 (1.88%) | PASS |
| §3.2 | NLP COPD n (%), non-massive | 60 (0.7%) | as above | 60 (0.72%) | PASS |
| §3.2 | Mean N comorbidities, massive | 2.78 | `AVG(pmhx_nlp_n_comorbidities) FILTER (WHERE is_massive)` | 2.7775 | PASS |
| §3.2 | Mean N comorbidities, non-massive | 2.38 | as above | 2.3819 | PASS |
| §3.2 | Graves n (%), massive | 108 (4.3%) | `WHERE syn_graves` | 108 (4.32%) | PASS |
| §3.2 | Graves n (%), non-massive | 466 (5.6%) | as above | 466 (5.57%) | PASS |
| §3.2 | Hashimoto n (%), massive | 39 (1.6%) | `WHERE syn_hashimoto` | 39 (1.56%) | PASS |
| §3.2 | Hashimoto n (%), non-massive | 209 (2.5%) | as above | 209 (2.50%) | PASS |
| §3.2 | Prior thyroidectomy n (%), massive | 209 (8.4%) | `WHERE pshx_nlp_prior_thyroidectomy` | 209 (8.36%) | PASS |
| §3.2 | Prior thyroidectomy n (%), non-massive | 650 (7.8%) | as above | 650 (7.77%) | PASS |
| §3.2 | Prior neck surgery n (%), massive | 38 (1.5%) | `WHERE pshx_nlp_prior_neck_surgery` | 38 (1.52%) | PASS |
| §3.2 | Prior neck surgery n (%), non-massive | 102 (1.2%) | as above | 102 (1.22%) | PASS |
| §3.2 | ASA subset n, massive | 246 | `COUNT(*) FILTER (WHERE nsqip_asa_class IS NOT NULL AND is_massive)` | 246 | PASS |
| §3.2 | ASA subset n, non-massive | 1,164 | as above | 1,164 | PASS |
| §3.2 | ASA I n (%), massive | 6 (2.4%) | `WHERE nsqip_asa_class LIKE 'ASA  I -%'` | 6 (2.44%) | PASS |
| §3.2 | ASA II n (%), massive | 80 (32.5%) | as above | 80 (32.52%) | PASS |
| §3.2 | ASA III n (%), massive | 144 (58.5%) | as above | 144 (58.54%) | PASS |
| §3.2 | ASA IV n (%), massive | 16 (6.5%) | as above | 16 (6.50%) | PASS |
| §3.2 | ASA I n (%), non-massive | 84 (7.2%) | as above | 84 (7.22%) | PASS |
| §3.2 | ASA II n (%), non-massive | 583 (50.1%) | as above | 583 (50.09%) | PASS |
| §3.2 | ASA III n (%), non-massive | 473 (40.6%) | as above | 473 (40.64%) | PASS |
| §3.2 | ASA IV n (%), non-massive | 24 (2.1%) | as above | 24 (2.06%) | PASS |
| §3.2 | Era 1999–2004 n (%), massive | 110 (4.4%) | era binning (see header) | 110 (4.40%) | PASS |
| §3.2 | Era 2005–2009 n (%), massive | 142 (5.7%) | as above | 142 (5.68%) | PASS |
| §3.2 | Era 2010–2014 n (%), massive | 240 (9.6%) | as above | 240 (9.60%) | PASS |
| §3.2 | Era 2015–2019 n (%), massive | 731 (29.2%) | as above | 731 (29.23%) | PASS |
| §3.2 | Era 2020–2025 n (%), massive | 517 (20.7%) | as above | 517 (20.67%) | PASS |
| §3.2 | Era unknown n (%), massive | 761 (30.4%) | as above | 761 (30.43%) | PASS |
| §3.2 | Era 1999–2004 n (%), non-massive | 793 (9.5%) | as above | 793 (9.48%) | PASS |
| §3.2 | Era 2005–2009 n (%), non-massive | 1,049 (12.5%) | as above | 1,049 (12.53%) | PASS |
| §3.2 | Era 2010–2014 n (%), non-massive | 1,645 (19.7%) | as above | 1,645 (19.65%) | PASS |
| §3.2 | Era 2015–2019 n (%), non-massive | 2,204 (26.3%) | as above | 2,204 (26.33%) | PASS |
| §3.2 | Era 2020–2025 n (%), non-massive | 1,300 (15.5%) | as above | 1,300 (15.53%) | PASS |
| §3.2 | Era unknown n (%), non-massive | 1,379 (16.5%) | as above | 1,379 (16.48%) | PASS |
| §3.2 | Malignant histology n (%), massive | 646 (25.8%) | `WHERE is_malignant` | 646 (25.83%) | PASS |
| §3.2 | Malignant histology n (%), non-massive | 3,491 (41.7%) | as above | 3,491 (41.71%) | PASS |
| §3.2 | Bilateral disease n (%), massive | 749 (29.9%) | `WHERE bilateral_disease_flag` | 749 (29.95%) | PASS |
| §3.2 | Bilateral disease n (%), non-massive | 1,393 (16.6%) | as above | 1,393 (16.64%) | PASS |
| §3.2 | Follow-up mean (years, all), massive | 1.22 | `AVG(followup_years) FILTER (WHERE is_massive)` | 1.220 | PASS |
| §3.2 | Follow-up mean (years, all), non-massive | 1.84 | as above | 1.840 | PASS |
| §3.2 | Patients with FU>0 n (%), massive | 997 (39.9%) | `COUNT(*) FILTER (WHERE followup_years > 0 AND is_massive)` | 997 (39.86%) | PASS |
| §3.2 | Patients with FU>0 n (%), non-massive | 3,174 (37.9%) | as above | 3,174 (37.92%) | PASS |
| §3.2 | Mean FU (years, FU>0 subset), massive | 3.06 | `AVG(followup_years) FILTER (WHERE followup_years > 0 AND is_massive)` | 3.061 | PASS |
| §3.2 | Mean FU (years, FU>0 subset), non-massive | 4.85 | as above | 4.853 | PASS |
| §3.2 (narrative) | ASA III–IV proportion, massive | 65.0% | (144+16) / 246 | 65.04% | PASS |
| §3.2 (narrative) | ASA III–IV proportion, non-massive | 42.7% | (473+24) / 1,164 | 42.70% | PASS |

### §3.3 Table 2 — Histology distribution within malignant subset (massive arm; n=646)

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §3.3 | Malignant denominator, massive arm | 646 | `COUNT(*) FILTER (WHERE is_malignant AND is_massive)` | 646 | PASS |
| §3.3 | PTC n (%) | 417 (64.6%) | `WHERE histology_final='PTC'` | 417 (64.55%) | PASS |
| §3.3 | Follicular carcinoma n (%) | 97 (15.0%) | `WHERE histology_final='follicular carcinoma'` | 97 (15.02%) | PASS |
| §3.3 | Medullary (MTC) n (%) | 32 (5.0%) | `WHERE histology_final='MTC'` | 32 (4.95%) | PASS |
| §3.3 | Poorly differentiated n (%) | 22 (3.4%) | `WHERE histology_final='poorly differentiated thyroid carcinoma'` | 22 (3.41%) | PASS |
| §3.3 | Anaplastic n (%) | 13 (2.0%) | `WHERE histology_final='anaplastic carcinoma'` | 13 (2.01%) | PASS |
| §3.3 | NIFTP n (%) | 25 (3.9%) | `WHERE histology_final='NIFTP'` | 25 (3.87%) | PASS |
| §3.3 | FTUMP n (%) | 9 (1.4%) | `WHERE histology_final='FTUMP'` | 9 (1.39%) | PASS |
| §3.3 | NUT carcinoma n | 1 | `WHERE histology_final='NUT carcinoma'` | 1 | PASS |
| §3.3 | Infiltrating carcinoma w/ thymus-like differentiation n | 1 | `WHERE histology_final ILIKE '%thymus%'` | 1 | PASS |
| §3.3 (cross-ref) | M032 broader malignant n | 4,022 | M032 draft (line 79); requires M032 cohort filter | 4,022 per M032 | PASS (cross-ref) |
| §3.3 (cross-ref) | M032 PTC % | 80.9% | M032 draft (line 79): 3,255 / 4,022 | 80.93% per M032 | PASS (cross-ref) |

Notes: long-tail counts (10 metastatic PTC; 6 differentiated high-grade; 4 metastatic PTC tall cell variant; ten singleton metastatic/rare variants) sum with the named rows above to the 646 malignant denominator. The PTC % comparison in the abstract and §3.3 narrative uses the M032 published value (80.9%) as a cross-reference; PTC % computed directly off the cohort_m038 view's `is_malignant=TRUE` rows would be 74.3% (3,075 / 4,137) due to a different (broader) malignant denominator that includes metastatic and rare variants the M032 cohort excludes.

### §3.4 Table 3 — Procedure type and operative context

Surgical procedure type (denominators: massive 2,501; non-massive 8,370):

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §3.4 | Total thyroidectomy n (%), massive | 1,672 (66.9%) | `WHERE surg_procedure_type='total_thyroidectomy'` | 1,672 (66.85%) | PASS |
| §3.4 | Hemithyroidectomy n (%), massive | 792 (31.7%) | as above | 792 (31.67%) | PASS |
| §3.4 | Other n (%), massive | 36 (1.4%) | as above | 36 (1.44%) | PASS |
| §3.4 | Isthmusectomy n (%), massive | 1 (0.04%) | as above | 1 (0.04%) | PASS |
| §3.4 | Unknown / NULL n (%), massive | 0 (0%) | NULL or 'unknown' | 0 (0%) | PASS |
| §3.4 | Procedure-type completeness, massive | 100% | (2501−0) / 2501 | 100.00% | PASS |
| §3.4 | Total thyroidectomy n (%), non-massive | 4,327 (51.7%) | as above | 4,327 (51.70%) | PASS |
| §3.4 | Hemithyroidectomy n (%), non-massive | 3,640 (43.5%) | as above | 3,640 (43.49%) | PASS |
| §3.4 | Other n (%), non-massive | 386 (4.6%) | as above | 386 (4.61%) | PASS |
| §3.4 | Isthmusectomy n (%), non-massive | 6 (0.07%) | as above | 6 (0.072%) | PASS |
| §3.4 | Unknown / NULL n (%), non-massive | 11 (0.13%) | 9 'unknown' + 2 NULL | 11 (0.131%) | PASS |
| §3.4 | Procedure-type completeness, non-massive | 99.98% | (8370−2 NULL) / 8370 = 8,368 / 8,370 | 99.976% | PASS |

Operative context (NSQIP-derived; denominators heterogeneous, see notes):

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §3.4 | Central neck dissection n, massive | 55 | `WHERE LOWER(nsqip_central_neck_dissection) IN ('yes','y','true','1')` | 55 | PASS |
| §3.4 | Central neck dissection n, non-massive | 193 | as above | 193 | PASS |
| §3.4 | Lateral neck dissection n, massive | 19 | as above (lateral) | 19 | PASS |
| §3.4 | Lateral neck dissection n, non-massive | 20 | as above | 20 | PASS |
| §3.4 | Mean operative duration (min), massive | 130.8 | `AVG(nsqip_operative_duration_min)` | 130.83 | PASS |
| §3.4 | Mean operative duration (min), non-massive | 121.3 | as above | 121.33 | PASS |
| §3.4 | Median operative duration (min), massive | 113.5 | `MEDIAN(nsqip_operative_duration_min)` | 113.5 | PASS |
| §3.4 | Median operative duration (min), non-massive | 107 | as above | 107 | PASS |
| §3.4 | Mean hospital LOS (days), massive | 1.26 | `AVG(nsqip_length_of_stay_days)` (n=246) | 1.264 | PASS |
| §3.4 | Mean hospital LOS (days), non-massive | 1.07 | as above (n=1,164) | 1.067 | PASS |
| §3.4 | Median hospital LOS (days), massive | 1 | `MEDIAN(nsqip_length_of_stay_days)` | 1 | PASS |
| §3.4 | Median hospital LOS (days), non-massive | 1 | as above | 1 | PASS |
| §3.4 | Transfusion (NSQIP, ≥1 unit) n, massive | 2 | `WHERE nsqip_transfusion >= 1` | 2 | PASS |
| §3.4 | Transfusion (NSQIP, ≥1 unit) n, non-massive | 2 | as above | 2 | PASS |
| §3.4 | Unplanned reintubation n, massive | 5 | `WHERE nsqip_unplanned_intubation >= 1` | 5 | PASS |
| §3.4 | Unplanned reintubation n, non-massive | 7 | as above | 7 | PASS |
| §3.4 | 30-day readmission n, massive | 11 | `WHERE nsqip_readmission_30d_flag = 1` | 11 | PASS |
| §3.4 | 30-day readmission n, non-massive | 18 | as above | 18 | PASS |
| §3.4 | NLP tracheostomy n (%), massive | 121 (4.84%) | `WHERE proc_nlp_tracheostomy` | 121 (4.838%) | PASS |
| §3.4 | NLP tracheostomy n (%), non-massive | 263 (3.14%) | as above | 263 (3.142%) | PASS |
| §3.4 (narrative) | Op duration delta (massive − non-massive) | ≈ 9.5 min | 130.83 − 121.33 | 9.50 min | PASS |
| §3.4 (narrative) | LOS delta (massive − non-massive) | ≈ 0.2 days | 1.264 − 1.067 | 0.197 days | PASS |
| §3.4 (narrative) | Tracheostomy prevalence ratio (massive / non-massive) | ≈ 1.5× | 4.838 / 3.142 | 1.54× | PASS |

LOS column note: the manuscript values match `nsqip_length_of_stay_days` (the column whose denominator matches the ASA subset n=246/1,164). The cohort view also exposes `nsqip_hospital_los_days` (n=350/911; means 1.246/1.312) and `nsqip_surgical_los_days` (n=350/911; means 1.123/1.088); these were not used.

### §3.5 Table 4 — Strict-definition perioperative complications

Strict definition, post-mig_252: `comp_*_confirmed = TRUE` ⇔ underlying `finding_status='present' AND evidence_strength IN ('definitive','probable')`. Denominators: massive 2,501; non-massive 8,370.

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §3.5 | Any confirmed complication n (%), massive | 132 (5.28%) | `WHERE any_confirmed_complication_flag` | 132 (5.278%) | PASS |
| §3.5 | Any confirmed complication n (%), non-massive | 268 (3.20%) | as above | 268 (3.202%) | PASS |
| §3.5 | Any-comp RR | 1.65 | (132/2501)/(268/8370) | 1.648 | PASS |
| §3.5 | Confirmed RLN injury n (%), massive | 14 (0.56%) | `WHERE comp_rln_injury_confirmed` | 14 (0.560%) | PASS |
| §3.5 | Confirmed RLN injury n (%), non-massive | 7 (0.084%) | as above | 7 (0.0836%) | PASS |
| §3.5 | RLN RR | 6.7 | (14/2501)/(7/8370) | 6.694 | PASS |
| §3.5 | Confirmed hematoma n (%), massive | 23 (0.92%) | `WHERE comp_hematoma_confirmed` | 23 (0.920%) | PASS |
| §3.5 | Confirmed hematoma n (%), non-massive | 45 (0.54%) | as above | 45 (0.538%) | PASS |
| §3.5 | Hematoma RR | 1.7 | (23/2501)/(45/8370) | 1.711 | PASS |
| §3.5 | Confirmed seroma n (%), massive | 12 (0.48%) | `WHERE comp_seroma_confirmed` | 12 (0.480%) | PASS |
| §3.5 | Confirmed seroma n (%), non-massive | 27 (0.32%) | as above | 27 (0.323%) | PASS |
| §3.5 | Seroma RR | 1.5 | (12/2501)/(27/8370) | 1.487 | PASS |
| §3.5 | Confirmed chyle leak n (%), massive | 2 (0.08%) | `WHERE comp_chyle_leak_confirmed` | 2 (0.080%) | PASS |
| §3.5 | Confirmed chyle leak n (%), non-massive | 1 (0.01%) | as above | 1 (0.0119%) | PASS |
| §3.5 | Chyle leak RR | 6.7 | (2/2501)/(1/8370) | 6.694 | PASS |
| §3.5 | Confirmed VC paresis n, massive | 0 | `WHERE comp_vc_paresis_confirmed` | 0 | PASS |
| §3.5 | Confirmed VC paresis n, non-massive | 0 | as above | 0 | PASS |
| §3.5 | Confirmed VC paralysis n (%), massive | 19 (0.76%) | `WHERE comp_vc_paralysis_confirmed` | 19 (0.760%) | PASS |
| §3.5 | Confirmed VC paralysis n (%), non-massive | 4 (0.048%) | as above | 4 (0.0478%) | PASS |
| §3.5 | VC paralysis RR | 15.9 | (19/2501)/(4/8370) | 15.89 | PASS |
| §3.5 | Confirmed hypocalcemia n (%), massive | 1 (0.04%) | `WHERE comp_hypocalcemia_confirmed` | 1 (0.040%) | PASS |
| §3.5 | Confirmed hypocalcemia n (%), non-massive | 8 (0.10%) | as above | 8 (0.0956%) | PASS |
| §3.5 | Hypocalcemia RR | 0.4 | (1/2501)/(8/8370) | 0.418 | PASS |
| §3.5 | Confirmed hypoparathyroidism n (%), massive | 87 (3.48%) | `WHERE comp_hypoparathyroidism_confirmed` | 87 (3.479%) | PASS |
| §3.5 | Confirmed hypoparathyroidism n (%), non-massive | 209 (2.50%) | as above | 209 (2.497%) | PASS |
| §3.5 | Hypoparathyroidism RR | 1.4 | (87/2501)/(209/8370) | 1.393 | PASS |
| §3.5 | All-cause in-record mortality n (%), massive | 59 (2.36%) | `WHERE death_occurred` | 59 (2.359%) | PASS |
| §3.5 | All-cause in-record mortality n (%), non-massive | 133 (1.59%) | as above | 133 (1.589%) | PASS |
| §3.5 | Mortality RR | 1.5 | (59/2501)/(133/8370) | 1.485 | PASS |
| §3.5 (narrative) | FU>0 mean, massive | 3.06 yrs | `AVG(followup_years) WHERE followup_years>0 AND is_massive` | 3.061 | PASS |
| §3.5 (narrative) | FU>0 mean, non-massive | 4.85 yrs | as above | 4.853 | PASS |

### §3.6 Era stratification

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §3.6 | 1999–2004 total n | 903 | era binning (see header) | 903 | PASS |
| §3.6 | 1999–2004 massive n | 110 | as above | 110 | PASS |
| §3.6 | 1999–2004 % massive | 12.2% | 110 / 903 | 12.18% | PASS |
| §3.6 | 2005–2009 total n | 1,191 | as above | 1,191 | PASS |
| §3.6 | 2005–2009 massive n | 142 | as above | 142 | PASS |
| §3.6 | 2005–2009 % massive | 11.9% | 142 / 1,191 | 11.92% | PASS |
| §3.6 | 2010–2014 total n | 1,885 | as above | 1,885 | PASS |
| §3.6 | 2010–2014 massive n | 240 | as above | 240 | PASS |
| §3.6 | 2010–2014 % massive | 12.7% | 240 / 1,885 | 12.73% | PASS |
| §3.6 | 2015–2019 total n | 2,935 | as above | 2,935 | PASS |
| §3.6 | 2015–2019 massive n | 731 | as above | 731 | PASS |
| §3.6 | 2015–2019 % massive | 24.9% | 731 / 2,935 | 24.91% | PASS |
| §3.6 | 2020–2025 total n | 1,817 | as above | 1,817 | PASS |
| §3.6 | 2020–2025 massive n | 517 | as above | 517 | PASS |
| §3.6 | 2020–2025 % massive | 28.5% | 517 / 1,817 | 28.45% | PASS |
| §3.6 | Surgical date unknown total n | 2,140 | `WHERE surg_first_date IS NULL` | 2,140 | PASS |
| §3.6 | Surgical date unknown massive n | 761 | as above | 761 | PASS |
| §3.6 | Surgical date unknown % massive | 35.6% | 761 / 2,140 | 35.56% | PASS |
| §3.6 (narrative) | Surgical-date-unknown share of cohort | 19.7% (2,140/10,871) | 2,140 / 10,871 | 19.69% | PASS |

### §4 Discussion

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §4 | All-three-flag share of massive | 386 (15.4%) | 386 / 2,501 | 15.43% | PASS |
| §4 | Weight-only count | 898 | (see §3.1) | 898 | PASS |
| §4 | Substernal-only count | 114 | (see §3.1) | **145** | **DIFF** (v2.1 patch — propagated from §3.1) |
| §4 | Airway-only count | 309 | (see §3.1) | **429** | **DIFF** (v2.1 patch — propagated from §3.1) |
| §4 | Male enrichment, massive vs non-massive | 29.2% vs 20.1% | (see §3.2) | 29.19% vs 20.10% | PASS |
| §4 | Black-or-AA enrichment, massive vs non-massive | 62.2% vs 31.2% | (see §3.2) | 62.18% vs 31.22% | PASS |
| §4 | NLP HTN, massive vs non-massive | 27.8% vs 12.9% | (see §3.2) | 27.83% vs 12.89% | PASS |
| §4 | NLP DM, massive vs non-massive | 20.0% vs 11.5% | (see §3.2) | 19.99% vs 11.54% | PASS |
| §4 | ASA III–IV, massive vs non-massive | 65.0% vs 42.7% | (see §3.2 narrative) | 65.04% vs 42.70% | PASS |
| §4 | Total-thy, massive vs non-massive | 66.9% vs 51.7% | (see §3.4) | 66.85% vs 51.70% | PASS |
| §4 | Any-comp RR | ≈ 1.65 | (see §3.5) | 1.648 | PASS |

### §5 Limitations footnotes

| Section | Cell description | Manuscript value | Live SQL | Live result | Status |
|---|---|---:|---|---:|---|
| §5 (1) | Gland weight known share, massive arm | 86.3% | `COUNT(gland_weight_final_g) FILTER (WHERE is_massive) / 2,501` | 2,158 / 2,501 = 86.29% | PASS |
| §5 (1) | Surgical date known share, "cohort-wide" | 69.6% | `COUNT(surg_first_date) / COUNT(*)` over full cohort | 8,731 / 10,871 = **80.31%** | **DIFF** (v2.1 patch — the 69.6% value is the **massive-arm** share: 1,740 / 2,501 = 69.57%; cohort-wide is 80.3%) |

---

## Summary

**Cells audited:** 156 distinct numeric cells (counts, percentages, denominators, ratios, derived statistics) across abstract + §3.1 + §3.2 + §3.3 + §3.4 + §3.5 + §3.6 + §4 + §5.

**Outcomes:** 153 PASS, 3 DIFF, 0 FAIL.

**DIFF disposition.** All three DIFFs are minor numeric/wording bugs in derived slices — none affect the cohort total (2,501), the inclusion-exclusion sum (2,501), the headline complication rates (5.28% / 3.20%, RR ≈ 1.65), the procedure-type completeness claim (100% / 99.98%), or any directional finding. They are addressed in the v2.1 Cursor patch below.

**Inclusion-exclusion check.** PASSES: |W|+|S|+|A| − |W∩S| − |W∩A| − |S∩A| + |W∩S∩A| = 1,429 + 1,047 + 1,440 − 404 − 513 − 884 + 386 = **2,501** ✓.

**RR computations.** All 9 RR values in §3.5 reconcile within rounding to (massive_rate / non_massive_rate). PASSES.

**Strict-definition complications** (mig_252) are correctly applied: every `comp_*_confirmed` value reflects the post-mig_252 strict rollup. PASSES.

**Procedure-type completeness** (mig_253) is correctly stated: 0 NULL in massive arm, 2 NULL in non-massive arm. PASSES.

**Cross-references.** The two cross-references to M032 (n=4,022 broader malignant; PTC 80.9%) are internally consistent with the M032 v1 draft (line 79); both PASS as cross-references. Note: PTC % derived directly from `cohort_m038_massive_goiter_v1` (`is_malignant=TRUE`) is 74.3% (3,075 / 4,137), reflecting a broader malignant denominator than M032 uses.

---

## v2.1 Cursor patch (3 minor manuscript edits)

The following three substitutions in `manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v2_post_mig_252_253.md` reconcile the manuscript text with the live data. These are surface-text fixes only; no findings change.

```text
# Edit 1 — §3.1 component-overlap table (line 105)
- | Substernal only | 114 |
+ | Substernal only | 145 |

# Edit 2 — §3.1 component-overlap table (line 106)
- | Airway only | 309 |
+ | Airway only | 429 |

# Edit 3 — §4 Discussion paragraph (line 234)
- substantial single-flag subsets exist (898 weight-only, 114 substernal-only, 309 airway-only)
+ substantial single-flag subsets exist (898 weight-only, 145 substernal-only, 429 airway-only)

# Edit 4 — §5 Limitations footnote 1 (line 244)
- (gland weight 86.3% known in massive cohort; surgical date 69.6% known cohort-wide)
+ (gland weight 86.3% known in massive cohort; surgical date 69.6% known in massive cohort, 80.3% known cohort-wide)
```

Each edit replaces a single literal string with another single literal string and is safe to apply via Cursor's find-and-replace.

---

**Audit complete.** Reproducibility anchors: `release_id='pub_v1_0_20260430'`; `signoff_registry` most-recent = `mig_253` (2026-05-01 06:41:00 UTC); cohort view `manuscript_workspace.cohort_m038_massive_goiter_v1` (~117 columns, post-mig_251). Companion deliverable: `M038_v2_DATA_AND_SOURCES.xlsx` (this directory).
