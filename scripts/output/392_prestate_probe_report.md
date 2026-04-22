# Script 392 — ETE Boolean-String Normalization: Phase 0 Probe Report

**Generated:** 2026-04-22T23:46:25.970661+00:00
**Database:** thyroid_canonical_publication_v1_0

---

## 1. Baseline Drift Check

| Metric | Expected | Actual | Status |
|--------|----------|--------|--------|
| CPM_ROWCOUNT | 10,871 | 10,871 | ✅ OK |
| N_JUNK_FALSE | 179 | 179 | ✅ OK |
| N_JUNK_TRUE | 4 | 4 | ✅ OK |
| N_JUNK_TOTAL | 183 | 183 | ✅ OK |

---

## 2. Q0-A: Junk Cohort Distribution

| ete_grade_final_v2 | n |
|--------------------|---|
| `false` | 179 |
| `true` | 4 |

---

## 3. Q0-B: Evidence Bucketing

| junk_val | evidence_bucket | n | Notes |
|----------|----------------|---|-------|
| `false` | `all_flags_negative` | 179 | EXPECT 179 |
| `true` | `all_flags_negative` | 2 | EXPECT 2 → queue |
| `true` | `gross_corroborated` | 2 | EXPECT 2 → gross-flip |

---

## 4. Q0-C: Legacy ete_grade Parity

Rows with `ete_grade == ete_grade_final_v2` (junk cohort): **183** (expect 183)

---

## 5. Q0-D: Gross-Flip Candidates (Bucket B — will become 'gross')

| research_id | diag | curr_t | stage_group | size_cm | age | op_gross | path_gross | gross_ete | any_micro | micro_t3b_corr |
|-------------|------|--------|-------------|---------|-----|----------|------------|-----------|-----------|---------------|
| 11599 | PTC | T3b | IVB | 1.2 | 72 | False | 1 | True | False | False |
| 3430 | PTC | T2 | II | 3.0 | 10 | True | None | False | False | False |

---

## 6. Q0-D2: Queue Candidates (Bucket C — 'true' preserved, routed to queue)

| research_id | diag | curr_t | stage_group | size_cm | age | op_gross | path_gross | gross_ete | any_micro | micro_t3b_corr |
|-------------|------|--------|-------------|---------|-----|----------|------------|-----------|-----------|---------------|
| 10626 | FTC | T2 | IVB | 3.0 | 60 | False | None | False | False | False |
| 9708 | FTC | T3a | IVB | 11.0 | 76 | False | None | False | False | False |

---

## 7. Q0-E: Queue Table Pre-Existence

Queue table `manuscript_workspace.cpm_ete_self_contradiction_queue_v1` found: **True**
Rows already queued from 183-cohort: **185**

---

## 8. Q0-F: ete_ordinal_worst Scale Verification

Ordinal=3 grade distribution (live scale: 2=gross, not 3; GREATEST(...,2) applied):

| ete_grade_final_v2 | n |
|--------------------|---|

---

## 9. Halt Gate Summary

✅ **ALL HALT GATES PASS** — 179 false→none, 2 true→gross, 2 true→queue confirmed.

Logan: review gross-flip table (Section 5) and confirm T-stage cascade looks clean.
Then run: `python3 scripts/392_ete_boolean_string_normalization.py --apply`