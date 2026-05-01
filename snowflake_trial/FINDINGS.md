# Snowflake Trial — Round 2 Validation Findings

**Date:** 2026-05-01

## Actionable findings surfaced by Cortex validation

### From Prompt 3 (Survival/recurrence integrity)

| Finding | N | Severity |
|---|---|---|
| `any_recurrence_flag=FALSE` but `time_to_recurrence_days` NOT NULL | 740 | High — likely flag/timing mismatch |
| Benign patients flagged with recurrence | 6 | Medium — review whether pathology was upgraded |
| Deceased patients with `followup_years > overall_survival_years` | 100 | Medium — followup-after-death encoding |

AI_CLASSIFY graded a 50-patient malignant sample: 46 Consistent / 4 Minor discrepancy / 0 Major contradiction.

### From Prompt 5 (AJCC 8 staging consistency)

| Finding | N | Severity |
|---|---|---|
| **M1 patients in Stage II** (AJCC 8 says M1 → IVB always) | 1,058 | **High — manuscript-blocking** |
| M1 patients with NULL stage_group | 1 | Low |
| AI_COMPLETE flagged 36/100 sampled malignants as INCONSISTENT | — | Sample evidence supporting the M1→II finding |

The M1→II count (1,058) is large enough that this is either a coding pattern Logan needs to document (e.g. "biochemically incomplete M1" treated as Stage II for analysis) or a real upstream propagation issue worth a CF entry.

### From Prompt 2 (Molecular)

- 0 internal contradictions on `tested=FALSE` + BRAF/RAS positive (clean)
- 1.1% molecular testing rate <2015 → 22.1% in 2020+ (era-driven adoption confirmed)
- 49 patients positive for BOTH BRAF and RAS — review for biology vs. assay artifact

### From Prompt 1 (Demographics)

- 4 "metastatic PTC" histology variants AI_CLASSIFY couldn't disambiguate — recommend prompt refinement to include "metastatic" prefix mapping
- 1 patient (rid=1568) age=17 borderline for adult-cohort criteria

## M037 Table 1 highlights (publication-ready)

Cohort: 4,137 malignant; 1,126 LN+ (27.2%) vs 3,011 LN-.

- **LN+ are younger:** mean 47.2 vs 52.0 yrs (p<0.0001)
- **Larger tumors:** 2.5 vs 2.1 cm (p<0.0001)
- **More T3b:** 45.0% vs 25.5%
- **More total thyroidectomy:** 85.8% vs 60.9%
- **Higher recurrence:** 16.3% vs 10.8%
- **BRAF NOT a discriminator:** 6.9% in both arms (p=1.00) — counter-intuitive given BRAF's reputation in PTC literature; warrants subgroup analysis (e.g. PTC-only, by tumor size)
- **Race signal:** Black/AA 13.1% LN+ vs 28.0% LN- — needs CF on access-to-care vs. tumor biology
- **Cross-validation issue:** 1,501 patients have `AJCC8_N_STAGE = N1a` but `LN_POSITIVE_FLAG=0 / LN_TOTAL_POSITIVE NULL`. N-staging encodes positivity that the count fields don't reflect — Logan should reconcile in the canonical pipeline.

## Recommended next actions for Logan

1. **Validate M1→II mapping** — pull all 1,058 patients, confirm whether this is a documented research convention (e.g., "M1 detected post-op → still treated as locoregional") or a propagation bug. Decision drives whether stage_group needs a re-derivation pass.
2. **Reconcile N-stage vs LN_POSITIVE flag** — 1,501 N1a patients with no positive-LN count creates Table 1 inconsistencies. Likely a question of whether `LN_TOTAL_POSITIVE` requires structured surgical pathology vs. accepting N-staging assertion.
3. **Recurrence flag/timing audit** — investigate the 740 patients with `recurrence_flag=FALSE` but `time_to_recurrence_days NOT NULL` to recover the real recurrence dates.
4. **Refine AI_CLASSIFY prompt** — add explicit mapping for "metastatic PTC*" so the standardization step doesn't drop those rows.
