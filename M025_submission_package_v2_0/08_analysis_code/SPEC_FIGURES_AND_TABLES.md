# M025 v2.0 figures + tables specification

**Author:** Cowork (Claude). For Cursor mig_307 build agent.
**Driving table:** `manuscript_workspace.cohort_m025_nodule_level_v1` (live MotherDuck view, mig_306).
**Strict cohort filter:** `analytic_eligible_strict_acr_pernodule = TRUE` (n=3,687 nodules).
**Outcome:** `nodule_path_proven_malignant` (same-side path tumor ≤365d post-US).
**Predictor:** `acr2017_tirads_category` (TR1–TR5 strict ACR 2017).
**CI method:** Wilson 95% throughout. All numbers prebaked in `08_analysis_outputs/m025v2_*.csv`.

---

## Tables

### Table 1 — Cohort characteristics
- One column per cohort: (a) all patients with US (n=6,523), (b) strict-ACR analytic-eligible nodules' patients (subset of the 6,523), (c) per-nodule strict cohort (n=3,687 nodules).
- Rows: n_patients, n_nodules, age (median IQR), sex (% male), n_with_FNA, n_with_path, multi-nodule % (≥2 nodules), TR1/2/3/4/5 distribution.
- Source: `cohort_m025_nodule_level_v1` filtered + joined to `canonical_patient_master` for demographics.

### Table 2 — Per-nodule diagnostic performance at three thresholds
- **Pre-baked** in `m025v2_threshold_metrics_per_nodule.csv`.
- Columns: Threshold, TP, FP, FN, TN, Sensitivity (95% CI), Specificity (95% CI), PPV (95% CI), NPV (95% CI).
- Rows: TR≥TR3, TR≥TR4 (primary), TR≥TR5.
- Footer: "AUC (ordinal TR rank) = 0.640 [Mann-Whitney equivalent over 1,928,336 comparable pairs]".

### Table 3 — Patient-level vs nodule-level ROM with attribution-error decomposition (THE HEADLINE)
- **Pre-baked** in `m025v2_per_tr_rom_with_ci.csv`.
- Columns: TR | Patient n | Patient ROM (95% CI) | Nodule n | Nodule ROM (95% CI) | Inflation (pp) | ACR expected | Inside ACR band?
- Footer rows: patient-level cohort = 3,375 patients; nodule-level strict cohort = 3,687 nodules.

### Table 4 — Bethesda × TIRADS cross-stratification at nodule grain
- Pivot: rows = Bethesda 2023 (I–II, III, IV, V, VI, missing), columns = TR2/3/4/5.
- Cells: n / n_malignant / ROM%.
- Source SQL:
```sql
SELECT bethesda_2023_num, acr2017_tirads_category,
       COUNT(*) AS n, COUNT(*) FILTER (WHERE nodule_path_proven_malignant) AS n_malig
FROM manuscript_workspace.cohort_m025_nodule_level_v1
WHERE analytic_eligible_strict_acr_pernodule
GROUP BY 1,2;
```

### Supplementary Table S1 — Sensitivity arms
- Re-compute Table 2 with these alternate cohorts:
  - (i) `analytic_eligible` relaxed (drop the ACR-feature-points-complete requirement) — n increases from 3,687 to ~5,120
  - (ii) Restrict to first US per patient (temporal): `ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY exam_date)`
  - (iii) Restrict to single-nodule patients only (eliminate attribution problem entirely)
  - (iv) Bilateral-path nodules excluded (laterality match uses unilateral path only)

---

## Figures (300 DPI; matplotlib in build_m025_v2_figures.py)

### Figure 1 — CONSORT-style cohort flow
- Boxes: 10,871 CPM total → 6,523 with any US nodule → 22,187 nodule-rows ACR-categorized → 3,687 strict-ACR analytic-eligible → 2,216 with FNA → 1,230 patients with path-confirmed malignant nodule.
- Right-side annotations for each filter.
- CSV sidecar: `m025v2_fig1_flow_data.csv`.

### Figure 2 — ROC curve, per-nodule TIRADS rank
- X = 1−Spec, Y = Sens; six points (origin + 4 thresholds + (1,1)).
- Annotate AUC = 0.640.
- Underlay light gray patient-level v1.0 ROC curve for comparison; legend distinguishes.
- CSV sidecar: `m025v2_fig2_roc_per_nodule.csv` (compute via cumulative thresholds).

### Figure 3 — Patient-vs-nodule ROM bars, per TR, with ACR-expected bands (THE HEADLINE FIG)
- Grouped bars: side-by-side patient-level (gray) and nodule-level (color) bar per TR.
- Error bars = Wilson 95% CI.
- Horizontal dashed lines for ACR-expected band per TR (TR1<2%, TR2<2%, TR3<5%, TR4 5-20%, TR5 >20%).
- Annotation: "Δ = inflation_pp" above each TR group.
- Source: `m025v2_per_tr_rom_with_ci.csv`.

### Figure 4 — Attribution-error decomposition (waterfall)
- Five vertical bars per TR: (i) patient-level ROM, (ii) attributed correctly = nodule-level ROM, (iii) gap = "attribution-error component", (iv) ACR-expected midpoint, (v) operative-bias residual = nodule-level minus ACR-expected midpoint.
- For TR4: 47.4% (patient) → 18.7% (nodule) → 12.5% (ACR midpoint of 5-20%) → +6.2 pp residual operative bias.
- Conveys: most of the inflation is attribution error, residual is true operative-cohort effect.

### Supplementary Figure S1 — Bethesda × TIRADS heatmap (nodule grain)
- 5×5 heatmap (Beth I-II, III, IV, V, VI vs TR2-5), color = ROM%.
- CSV sidecar: `m025v2_figS1_beth_tr_heatmap.csv`.

---

## SQL reproducibility files

Build script should write these alongside the CSVs:
- `M025_v2_tirads_analysis.sql` — exact queries used by build_m025_v2_tables.py
- `m025v2_analytic_spine.parquet` — strict cohort dump for downstream R/Python re-runs
- `m025v2_run_snapshot.json` — counts (already pre-written)

---

## Manuscript prose hooks (for the docx writer)

- "In a 25-year operative cohort of 6,523 patients with thyroid US, we identified 3,687 strict ACR 2017 analytic-eligible nodules in 1,XXX patients (TBD by build)."
- "Per-nodule risk of malignancy at TR4 (18.7%, 95% CI 16.3–21.5%) and TR5 (26.1%, 95% CI 23.7–28.6%) was concordant with ACR-published expected ranges (5–20% and >20%, respectively), in contrast to patient-level analysis of the same cohort which showed substantial overshoot at every TR (TR4 47.4% [43.0–51.8], TR5 58.7% [56.1–61.2])."
- "The patient-vs-nodule inflation was 19–33 percentage points across TR2–TR5, indicating that the majority of operative-cohort ROM inflation observed in patient-level TI-RADS validation studies is attributable to misattribution from multinodular patients rather than true selection bias."
- "Per-nodule discrimination (AUC = 0.640) was similar to patient-level (0.648), confirming that the headline finding is calibration-driven, not discrimination-driven."

---

## Open carry-forwards to footnote

- **CF-FNA-SIZE-CM-NULL** (v1_1 NLP TODO): per-nodule FNA size NULL by design in `imaging_fna_linkage_v3` v1_0 — bridge currently uses laterality + 30d temporal alone. Recovers ~70% of FNA links. Will upgrade in v1_1.
- **CF-mig_264-BETHESDA2-LINKAGE-MISMAP**: 360 residual Bethesda-2 + malignant patients pending audit (mig_264 in cursor queue). If reclassified, re-run this build.
