# M033 Platform Comparison — v3 Analysis Notes (Strict-Preop Bethesda)
**Run:** 2026-05-08  
**Audit:** DFL-20260508-M033-V3-COHORT-B3B4-BUILD (rec0EFYYHKQjcRSvF)  
**MFL:** MFL-20260508-M033-V3-MANUSCRIPT-WRITE (rec9VO2QWCbtJx84N)  
**Parent:** THY-48 sub-task B  
**BQ tables built:**
- `pub_workspace.cohort_m033_b3b4_v3` (PRIMARY, N=520)
- `pub_workspace.cohort_m033_all_molecular_v3` (SENSITIVITY-1, N=969)
- `pub_workspace.cohort_m033_b5b6_v3` (SENSITIVITY-2, N=214)

---

## Bethesda Source Change (v1 → v3)

| Attribute | v1 (legacy) | v3 (strict-preop) |
|---|---|---|
| Bethesda field | `bethesda_final` (legacy loose-window aggregation) | `bethesda_final_strict_preop` (MIG-45 recompute, strictly-preop events only) |
| Source table | MotherDuck `manuscript_workspace.cohort_m033_afirma_thyroseq_v1` | BQ `pub_canonical.canonical_patient_master_v1_1` |
| Primary cohort definition | All-molecular (N=969) | B3/B4-restricted (N=520, Logan confirmed 2026-05-08) |

---

## CONSORT-Style Cohort Flow (v3)

```
All molecularly-tested patients in CPM v1_1 (Afirma OR ThyroSeq)
  N = 969
  │
  ├── NULL strict-preop Bethesda (no preop FNA scoreable):  n = 180
  ├── Bethesda I (nondiagnostic):                           n =   4
  ├── Bethesda II (benign):                                 n =  51
  │   Excluded from PRIMARY (B1/B2 total = 55)
  │
  ├── Bethesda III (AUS/FLUS):  n = 306  ┐
  ├── Bethesda IV (FN/SFN):     n = 214  ┘ PRIMARY COHORT: N = 520
  │
  └── Bethesda V (suspicious):  n =  66  ┐
      Bethesda VI (malignant):  n = 148  ┘ SENSITIVITY-2 COHORT: N = 214
```

---

## V3 vs V1 Delta (Bethesda Category Shift)

| Bethesda category | N in v1 (legacy) | N in v3 (strict-preop) | Delta |
|---|---|---|---|
| NULL | 165 | 180 | +15 |
| B1 | 8 | 4 | -4 |
| B2 | 76 | 51 | -25 |
| B3 | 294 | 306 | +12 |
| B4 | 216 | 214 | -2 |
| B5 | 67 | 66 | -1 |
| B6 | 143 | 148 | +5 |

**Net B3/B4 change:** +10 patients (510 → 520). 28 patients changed Bethesda category between v1 and v3; 10 patients lost Bethesda (legacy-only); 2 patients gained Bethesda (strict-only).

---

## Primary Results — B3/B4 Cohort (N=520)

### Platform Diagnostic Performance

| Platform | N | ROM Overall | B3 ROM (n) | B4 ROM (n) | BRAF+ | Recurrence |
|---|---|---|---|---|---|---|
| ThyroSeq | 372 | 52.7% | 53.4% (234) | 51.4% (138) | 7.8% | 4.3% |
| Afirma | 75 | 53.3% | 47.2% (36) | 59.0% (39) | 46.7% | 5.3% |
| Dual | 73 | 56.2% | 38.9% (36) | 73.0% (37) | 86.3% | 6.8% |

### V3 vs V1 ROM Comparison (B3/B4 stratum)

| Platform | V1 ROM (B3/B4 legacy, N=510) | V3 ROM (B3/B4 strict-preop, N=520) | Delta |
|---|---|---|---|
| ThyroSeq | 53.7% | 52.7% | -1.0 pp |
| Afirma | 52.5% | 53.3% | +0.8 pp |

**Interpretation:** The strict-preop Bethesda recompute results in minimal shifts in ROM by platform (<1 percentage point). The core finding that ThyroSeq and Afirma have comparable B3/B4 ROM is preserved.

### Mutation Spectrum (B3/B4 primary cohort)

| Platform | N | BRAF+ | RAS+ | Fusion+ |
|---|---|---|---|---|
| ThyroSeq | 372 | 7.8% | 30.1% | 57.3% |
| Afirma | 75 | 46.7% | 0.0% | 74.7% |
| Dual | 73 | 86.3% | 21.9% | 91.8% |

### Outcomes by Platform (B3/B4 primary cohort)

| Platform | N | Recurrence | Total Thy | Hemi | LN+ | Avg Tumor Size |
|---|---|---|---|---|---|---|
| ThyroSeq | 372 | 4.3% | 44.6% | 54.8% | 3.5% | 2.70 cm |
| Afirma | 75 | 5.3% | 50.7% | 46.7% | 4.0% | 2.12 cm |
| Dual | 73 | 6.8% | 39.7% | 60.3% | 1.4% | 3.18 cm |

---

## Data Limitations (v3 additions)

1. **Bethesda shift:** 15 more patients have NULL strict-preop Bethesda vs v1 legacy (180 vs 165). These patients are excluded from the primary B3/B4 analysis; they are retained in the sensitivity-1 (all-molecular) cohort.
2. **B2 collapse:** v3 shows 51 B2 patients (vs 76 v1). The -25 shift reflects the strict-preop window excluding B2 events that occurred after surgery under the legacy looser aggregation.
3. **Afirma RAS:** 0 RAS-positive Afirma patients. This reflects the Afirma GSC platform not reporting RAS mutations in the structured molecular fields; not a clinical claim about RAS absence.
4. **Primary vs v1 cohort change:** v3 reports on B3/B4 (N=520) whereas v1 analysis reported on all-molecular (N=969). This is a deliberate methodological upgrade (Logan confirmed). The all-molecular universe is preserved in `cohort_m033_all_molecular_v3` for sensitivity.

---

## Output Files

| File | Content |
|---|---|
| `rom_by_bethesda_v3.csv` | ROM by platform + Bethesda category (B3/B4 primary) |
| `platform_diagnostic_performance_v3.csv` | Overall ROM + per-Bethesda ROM + BRAF + recurrence |
| `mutation_spectrum_by_platform_v3.csv` | BRAF/RAS/fusion rates |
| `outcomes_by_platform_v3.csv` | Recurrence, surgery type, LN+, tumor size |
| `crosstab_v3_vs_v1.csv` | Cross-tab of legacy vs strict-preop Bethesda (N=969) |
