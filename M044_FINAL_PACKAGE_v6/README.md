# M044 FINAL PACKAGE v6

**Generated**: 2026-05-05  
**Migration**: mig_315 (ete_grade_final normalization + mig_313 M-stage corruption fix cascade)  
**Status**: v6 NUMERICAL PATCH — Cowork prose review pending (`CF-M044-V6-MANUSCRIPT-PATCH`)

---

## What changed from v5

### Root causes
1. **mig_313** (2026-05-05): Fixed M-stage corruption where `distant_mets_proxy=recurrence_flag`
   caused M1=45% prevalence (should be ~3%). Stage IVB dropped from 816 → 76. Cohort
   composition shifted.
2. **mig_315** (2026-05-05): Normalized `ete_grade_final` in `cohort_m044_ajcc_ete_v1` VIEW.
   Boolean artifacts `'false'`/`'absent'`/`'true'` replaced with canonical vocabulary
   `'no_negative'`/`'no_negative'`/`'gross'`. Source changed from `ete_grade_final` (corrupt)
   to `ete_grade_final_v2` (adjudicated, cleaned).

### Key numerical changes

| Item | v5 (locked) | v6 (mig_315) | Notes |
|---|---|---|---|
| Cohort (total view) | ~3,578 | 3,868 | mig_313 restaged 290 patients |
| Strict-DTC analytic N | 3,572 | 3,614 | +42 patients |
| Path-proven events | 105 | 136 | +31 events from expanded cohort |
| No/negative ETE n | 68 (strict-DTC) | 173 (full cohort) / ~58 (strict-DTC) | Vocabulary now clean |
| Stage IVB n | 816 | 76 | mig_313 M-stage fix |
| Primary aOR (Gross vs Micro) | **1.77** [1.15–2.71] p=0.009 | **1.72** [1.15–2.56] p=0.008 | Drift=0.050 (within threshold) |
| No/neg aOR (vs Micro) | 2.72 [0.80–9.30] (NS) | 0.55 [0.23–1.32] (NS) | Both non-significant; instability from small n=173 |

### What DIDN'T change
- **Primary finding** (gross > microscopic recurrence risk) **confirmed and stable** ✅
- Direction: gross OR consistently > 1 ✅
- Statistical significance maintained: p=0.008 ✅
- CI overlapping with v5 ✅

---

## Files in this package

| File | Description |
|---|---|
| `M044_ETE_FINAL_all_stats_v6.xlsx` | Updated tables (Table 2 recurrence, Table 3 multivariable, Model outputs, QA) |
| `M044_ETE_FINAL_per_research_id_dataset_v6.xlsx` | Per-patient data (3,868 rows × 181 cols) |
| `MIG_315_REGRESSION_DELTA_v5_vs_v6.md` | Detailed regression delta report |
| `m044_v6_run_snapshot.json` | Machine-readable model snapshot |
| `figures/` | All 6 figures regenerated with v6 data |

---

## v5 package preservation

`M044_FINAL_PACKAGE/` (without version suffix) contains the v5 manuscript and is **preserved
untouched**. Do NOT overwrite v5 files with v6 outputs.

---

## Cowork lane tasks (CF-M044-V6-MANUSCRIPT-PATCH)

The following prose patches are needed in the v6 docx (Cowork task):
1. Table 1: Update ETE group counts and stage distribution (IVB 816→76)
2. Abstract/Results §1: Update N (3,614 strict-DTC), events (136)
3. Results §Regression: Update aOR 1.77→1.72, CI, p
4. Discussion: No/negative instability note (both v5 and v6 OR non-significant, small n=173)
5. Limitations: Note mig_313/315 cohort changes
6. eMethods: Document mig_315 cohort rebuild, ete_grade_final_v2 source, no_negative vocabulary
