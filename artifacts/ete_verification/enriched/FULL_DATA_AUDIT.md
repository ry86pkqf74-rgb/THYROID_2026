# Full Data Audit — MotherDuck-Enriched PTC Cohort (N=3,278)

Audit date: 2026-04-13
Source database: MotherDuck `Thyroid 2026` → `tumor_pathology`
Cohort file (delivered): `artifacts/ete_verification/ete_final_cohort_N3278.csv`
Enriched file: `artifacts/ete_verification/enriched/ete_final_cohort_N3278_ENRICHED.csv`

## 1. Why this audit

The delivered cohort reported **ln_ratio coverage of 63.3% (2,076/3,278)**, and the ordinal-regression complete-case N was 2,069. This is inconsistent with the 95% LN-coverage target referenced in the proposal. The user requested a pull of pathology fields directly from MotherDuck to (a) find the ~1,037 patients being dropped and (b) verify all manuscript variables against the source of record.

Per-patient values for 15 pathology fields were pulled from `tumor_pathology` for the full 3,278-patient cohort, using 9 packed STRING_AGG queries (~400 research_ids per chunk) to fit within the 50 KB MCP response cap. Data were merged into `ete_final_cohort_N3278_ENRICHED.csv`.

## 2. Headline findings

| Variable | Delivered cohort | Enriched (MotherDuck) | Δ |
|---|---:|---:|---:|
| ln_examined non-null | 2,171 (66.2%) | 3,260 (99.5%) | **+1,089** |
| ln_positive non-null | 2,074 (63.3%) | 3,115 (95.0%) | +1,041 |
| **ln_ratio non-null** | **2,076 (63.3%)** | **3,113 (95.0%)** | **+1,037** |
| tumor_1_gross_ete = 1 | 818 | 873 | +55 |
| tumor_1_extrathyroidal_ext non-null | 3,278 (100%) | 3,278 (100%) | 0 |
| tumor_1_ete_microscopic_only non-null | 253 (7.7%) | 253 (7.7%) | 0 |
| LVI (tumor_1_lymphatic_invasion) non-null | — | 2,206 (67.3%) | new |
| VI (tumor_1_vascular_invasion) non-null | — | 2,663 (81.2%) | new |
| PNI (tumor_1_perineural_invasion) non-null | — | 1,200 (36.6%) | new |
| Margin (tumor_1_margin_status) non-null | — | 2,696 (82.2%) | new |
| Capsular invasion non-null | — | 536 (16.4%) | new |
| ENE (histology_1_ln_extranodal_extension) non-null | — | 1,098 (33.5%) | new |
| Central LN positive non-null | — | 1,988 (60.7%) | new |
| Multifocal (tumor_2_present) non-null | — | 3,278 (100%) | new |

`ln_ratio` coverage improvement of **+31.7 percentage points** restores the 95% target. 1,040 patients gained a usable LN ratio; 3 were lost (edge cases where the MotherDuck value could not be aggregated cleanly).

## 3. ETE grade reclassification

Enriched-cohort ETE group distribution vs. delivered:

| ETE group | Delivered | Enriched | Δ |
|---|---:|---:|---:|
| No ETE | 724 | 681 | −43 |
| Microscopic ETE | 1,736 | 1,724 | −12 |
| Gross ETE | 818 | 873 | **+55** |

Upgrade breakdown (delivered → enriched):
- **43 patients reclassified No ETE → Gross ETE** (latent gross-ETE flag in MotherDuck that wasn't surfaced in the delivered `tumor_1_gross_ete` column)
- **12 patients reclassified Microscopic → Gross ETE**
- No downgrades observed.

Microscopic ETE derivation note: the `tumor_1_ete_microscopic_only` column is sparsely populated (only 253/3,278 patients). The delivered pipeline (and this audit) therefore derives Microscopic ETE as `tumor_1_extrathyroidal_ext=True AND NOT gross ETE`, which is 100% non-null. This convention is preserved in the enriched dataset.

## 4. Ordinal regression (Table 3) — before vs. after enrichment

Dependent variable: `recurrence_risk_band` (low/intermediate/high). Predictors: ete_micro, ete_gross, age_at_surgery, female, largest_tumor_cm, ln_ratio.

| Variable | Delivered (N=2,069) OR [95% CI], p | Enriched (N=3,106) OR [95% CI], p |
|---|---|---|
| ete_micro | 0.46 [0.37–0.58], p=5.7e-11 | 0.52 [0.43–0.64], p=9.0e-11 |
| ete_gross | 144 [91–228], p=5.1e-101 | 169 [113–252], p=1.6e-139 |
| age_at_surgery | 1.046 [1.039–1.053], p=1.4e-36 | 1.047 [1.041–1.053], p=8.9e-56 |
| female | 0.91 [0.73–1.15], p=0.44 | 0.82 [0.68–1.00], p=0.045 |
| largest_tumor_cm | 1.02 [0.98–1.07], p=0.37 | 1.03 [1.00–1.07], p=0.065 |
| ln_ratio | 2.27 [1.80–2.86], p=4.0e-12 | 2.32 [1.92–2.80], p=1.1e-18 |

Effect direction and magnitude are **consistent** between the delivered and enriched analyses. The enriched analysis adds 1,037 patients, tightens confidence intervals, and confirms the core conclusions of the manuscript.

A prior "reproduced" ordinal regression (`reproduced_ordinal_regression.csv`) produced an implausible ete_gross OR of 2.24e10 with p≈0.99 — indicative of quasi-complete separation on the delivered subset. The enriched fit (OR ≈ 169, p<1e-139) does not exhibit this pathology.

Sensitivity models fitted on the enriched cohort and saved in `table3_ordinal_regression_ENRICHED.csv`:
- No-LN-ratio (N=3,270): ete_gross OR = 166, ete_micro OR = 0.62
- +LVI (N=2,166): LVI OR = 0.55, p=0.010
- +Margin (N=2,603): margin_positive OR = 0.88, p=0.65 (not significant)
- Delivered-LN for comparison (N=2,069): matches Table 3 of the delivered analysis closely.

## 5. High-risk logistic sensitivity (Table 4)

Binary outcome P(risk=high). Full table in `table4_sensitivity_high_risk_ENRICHED.csv`. Key columns:

| Scenario | N | ete_gross OR | ete_micro OR | ln_ratio OR |
|---|---:|---:|---:|---:|
| Enriched + LN ratio | 3,106 | 98.6 | 0.51 | 1.23 (ns) |
| Enriched, no LN | 3,270 | 99.1 | 0.52 | — |
| Enriched + LVI | 2,166 | 117.7 | 0.36 | 1.26 (ns) |
| Enriched + Margin | 2,603 | 123.3 | 0.56 | 1.31 (ns) |
| Delivered LN | 2,069 | 85.7 | 0.44 | 1.16 (ns) |

Within the high-risk subset model LN ratio is not an independent predictor (masked by the extremely strong gross-ETE effect). The gross-ETE effect is robust across scenarios (OR 86–123).

## 6. Pipeline defect

`studies/proposal2_ete_staging/proposal2_expanded_cohort.py` pulls from the stale `exports/ptc_full.csv`:

```python
ptc_orig = pd.read_csv(ROOT / "exports" / "ptc_full.csv")
orig_cols = ["research_id", "ln_examined", "ln_positive", "m_stage_ajcc8",
             "tumor_1_ete_microscopic_only"]
orig_dedup = ptc_orig[orig_cols].drop_duplicates(
    subset=["research_id"], keep="first"   # ← collapses multi-tumor rows
)
```

Effects:
1. `ln_examined` / `ln_positive` are read from a snapshot that pre-dates the LN-extraction remediation (see `LYMPH_NODE_EXTRACTION_REMEDIATION_SUMMARY.md`), capping coverage at ~63%.
2. `drop_duplicates(keep="first")` silently discards secondary-tumor rows. For patients with >1 tumor this can drop the row that actually carries the LN count.

**Recommended fix:** Replace the `exports/ptc_full.csv` lookup with a direct DuckDB/MotherDuck read from `tumor_pathology`, aggregating per research_id with MAX/BOOL_OR (same pattern used in this audit). The enriched cohort CSV in this directory is a drop-in replacement for downstream scripts.

## 7. Reproducibility artifacts

All artifacts are in `artifacts/ete_verification/enriched/`:

- `ete_final_cohort_N3278_ENRICHED.csv` — cohort with 15 new enriched fields
- `enrichment_summary.json` — coverage + audit metrics
- `table3_ordinal_regression_ENRICHED.csv` — Table 3 refit
- `table4_sensitivity_high_risk_ENRICHED.csv` — Table 4 refit
- `FULL_DATA_AUDIT.md` — this file

Raw MotherDuck PSV pulls (1 chunk per ~400 research_ids) are retained in the working directory at `/tmp/enrich/out/chunk_{0..8}.psv`.

## 8. Conclusions

1. The ~1,037-patient drop in the delivered cohort is fully explained by stale reads of `ln_examined` / `ln_positive` in `proposal2_expanded_cohort.py`. Reading directly from MotherDuck restores 95.0% LN coverage and raises the complete-case regression N from 2,069 to 3,106.
2. Gross-ETE prevalence increases from 818 to 873 after enrichment; 55 patients were under-classified in the delivered cohort.
3. Manuscript-level conclusions do not change: gross ETE remains the dominant predictor of recurrence-risk band (OR ≈ 169), ln_ratio remains an independent predictor (OR ≈ 2.3), microscopic ETE remains mildly protective / close to neutral (OR ≈ 0.52).
4. Additional pathology fields (LVI, VI, PNI, margin, capsular invasion, ENE, central LN positivity, multifocality) are now available for downstream analyses and should be used in any revised manuscript tables.
