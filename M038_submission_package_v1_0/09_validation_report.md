# M038 Validation Report (full audit reconciliation)

**Date:** 2026-05-01
**Auditor:** Cowork (Claude)
**Manuscript audited:** `manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v2_post_mig_252_253.md` (post-Cursor v2.1 patches)
**Cohort view:** `manuscript_workspace.cohort_m038_massive_goiter_v1` (post-mig_255)
**Database:** `thyroid_canonical_publication_v1_0` (release `pub_v1_0_20260430`)

## Summary

**156 numeric cells re-derived** against live SQL on MotherDuck.

| Outcome | n |
|---|---:|
| PASS (matches manuscript within rounding) | 153 |
| DIFF (numeric mismatch, addressed in v2.1 Cursor patch) | 3 |
| FAIL (query error or missing column) | 0 |

## Key reconciliations (all PASS)

- **Cohort sum-to-2,501** inclusion-exclusion check: 1,429 + 1,047 + 1,440 − 404 − 513 − 884 + 386 = 2,501 ✓
- **All 10 strict-definition complication rows** (Table 4) including the headline 132 (5.28%) vs 268 (3.20%), RR ≈ 1.65
- **All 9 RR computations** match within rounding tolerance
- **Procedure-type completeness** 100% massive / 99.98% non-massive (post-mig_253)
- **All 30+ Table 1 demographic rows** (age, sex, race, BMI, NLP comorbidities, thyroid history, ASA, era, pathology, follow-up)
- **All 11 Table 2 histology rows** + cross-reference to M032 (n=4,022; PTC 80.9%) verified internally consistent with M032 v1 draft
- **All 11 Table 3 procedure / op-context rows** (LOS column = `nsqip_length_of_stay_days`, n=246 / 1,164)
- **All 6 Table 5 era-stratification rows** + the abstract's "12% / 24.9% / 28.5%" claim
- **§4 Discussion claims** (male enrichment, Black/AA enrichment, comorbidity prevalence ratios, ASA III–IV %, total-thy preference, any-comp RR)

## DIFFs (3 — all minor; resolved in v2.1 Cursor patch)

1. **§3.1 + §4: substernal-only** = manuscript 114 vs live **145** (live is internally consistent with the manuscript's own intersection counts: 1,047 − 404 − 884 + 386 = 145).
2. **§3.1 + §4: airway-only** = manuscript 309 vs live **429** (live: 1,440 − 513 − 884 + 386 = 429).
3. **§5 footnote 1:** "surgical date 69.6% known cohort-wide" — actually the **massive-arm** coverage (1,740/2,501 = 69.6%); cohort-wide is 80.3% (8,731/10,871).

All three DIFFs were patched in the M038 v2 manuscript via the v2.1 Cursor edits before the submission package was assembled.

## Standing-rule application

Per `memory/feedback_complications_transient_vs_permanent.md` (set 2026-05-01), Table 4 was restructured:

- **Hypoparathyroidism** split into postop transient (<6mo) + postop permanent (>6mo). M038 cohort: massive 83+4=87 ✓; non-massive 197+12=209 ✓ (matches original sum totals).
- **Hypocalcemia** adds "present preop" row using `timing_window='pre_surgery'`. M038: massive 7, non-massive 46.
- **RLN injury / VC paralysis / VC paresis** preop status not encoded; carry-forwards `CF-RLN-PREOP-FLAG` and `CF-VC-PARALYSIS-PREOP-FLAG` are open. Manuscript footnote inserted.

## Database lineage at audit time

| Anchor | Value |
|---|---|
| Database | `thyroid_canonical_publication_v1_0` (MotherDuck) |
| Release ID | `pub_v1_0_20260430` |
| Most-recent applied migration | `mig_255_cohort_m038_complication_temporality_columns_20260502` |
| Cohort view | `manuscript_workspace.cohort_m038_massive_goiter_v1` (post-mig_255 passthrough; ~129 cols) |
| Gate health | gate1=218; gates 2–5=0; cohort_parity=TRUE |
| cpm_pts (canonical_patient_master) | 10,871 |

## Cowork commit chain

- `b6cc14d` mig_255 cohort view temporality columns
- `c3203b0` standing rule + propagation
- `e236356` mig_254 surg_first_date backfill
- `b232007` initial audit + Excel
- `5125a87` v23 handoff amend
- `4b48107` v23 handoff + M038 v2 draft

---

