# Cursor prompt — M048 Racial Disparities in TI-RADS Performance

**Repo:** `THYROID_2026`
**Author of prompt:** Logan D. Glosser (via Cowork session 2026-05-05)
**Database:** `thyroid_canonical_publication_v1_0` @ release tag `pub_v1_1`
**Builds on:** M025 v2 submission package (mig_307 / mig_307b)
**Status of inputs:** READY (no new canonical work; reuses
`m025_analytic_master_patient_v1` and `m025_analytic_master_nodule_v1`)

---

## Goal

Produce, in one Cursor pass, the full analytic + tables + figures package for
**M048 — Racial Disparities in ACR TI-RADS Performance** in our 25-year
operative thyroid cohort. Race composition: 45.5% Black, 40.9% White, 6.0%
Asian — uniquely diverse for the published TI-RADS literature. The paper
mirrors M025's patient-grain + nodule-grain framework, stratified by race.

The deliverable is a study folder under `studies/m048_racial_disparities_tirads/`
parallel in structure to `studies/m025_tirads_performance/`, plus a manuscript
package under `M048_submission_package/` parallel in structure to
`M025_submission_package/`.

---

## Pre-specified analytic plan (do not modify without senior-author sign-off)

### Cohort and strata
- Reuse the M025 patient analytic master (n=3,375) and nodule analytic master
  (n=37,438; n=3,687 strict-eligible).
- Race strata (primary inferential): **Black**, **White**, **Asian**.
- Race strata (descriptive only, collapsed): **Other** (NHPI / AI/AN / Other),
  **Unknown** (Unknown or Not Reported / NULL).

### Predictor and outcome (unchanged from M025)
- Patient grain: `max_tirads_category_ever` ∈ {TR1..TR5}; outcome
  `is_malignant` (any path-proven thyroid malignancy on operative specimen).
- Nodule grain: `acr2017_tirads_category` ∈ {TR1..TR5} on the strict
  feature-complete subset (`analytic_eligible_strict_acr_pernodule = TRUE`);
  outcome `nodule_path_proven_malignant` (same-side malignant tumor within
  365 days of index US).

### Pre-specified primary analyses
1. **Per-race AUC** at patient and nodule grain (closed-form Mann–Whitney with
   tie correction; see `m025_sensitivity_lib.wilson_ci` style).
2. **Per-race per-TR ROM** at patient and nodule grain with Wilson 95% CIs.
   Compare to ACR 2017 expected bands (TR1–2 <2%, TR3 <5%, TR4 5–20%, TR5 >20%).
3. **Per-race threshold metrics** (sens/spec/PPV/NPV) at TR≥TR3, TR≥TR4
   (Youden-optimal in M025), TR≥TR5 with Wilson 95% CIs.
4. **Per-race per-feature score distribution** (composition / echogenicity /
   shape / margin / foci): chi-square test of independence between race and
   each feature's discrete score, with Bonferroni correction for the 5
   feature tests.
5. **Per-race FNA-eligibility audit** (mirror M025's 1,553 unnecessary / 472
   below-threshold cancers). Stratify the audit numerators/denominators by
   race.

### Pre-specified secondary analyses
6. **Patient × nodule grain inflation by race**: report TR4 and TR5 inflation
   (patient ROM − nodule ROM, percentage points) per race. Hypothesis: the
   inflation magnitude (~+28.6 pp at TR4, +32.6 pp at TR5 in the overall
   M025 cohort) is similar across races; deviations indicate race-specific
   multinodular-disease prevalence.
7. **Per-race Bethesda × TR cross-tabulation** (descriptive; for a
   supplementary heatmap).
8. **Sensitivity arm S048-A**: include `Other` race stratum as a single
   pooled comparator to bound small-cell inference.

### Statistical conventions (DO NOT change)
- Wilson 95% CIs on all proportions (sens, spec, PPV, NPV, ROM).
- AUC via closed-form rank Mann–Whitney equivalent (matches M025).
- Bootstrap 1,000 replicates × race × grain for AUC 95% CIs (use stratified
  bootstrap on each race subset separately; mirrors M025 bootstrap).
- Chi-square with Yates correction for race × feature score tests.
- All p-values reported with effect sizes; do NOT chase significance.

---

## Files to produce (deterministic)

```
studies/m048_racial_disparities_tirads/
  ├── M048_motherduck_queries.sql          (already present — driver)
  ├── m048_run_snapshot.json               (Cursor: write run metadata)
  ├── m048_diagnostic_performance.csv      (per-grain × per-race)
  ├── m048_rom_by_race_x_tr.csv            (long-format)
  ├── m048_auc_by_race.csv                 (with bootstrap CIs)
  ├── m048_threshold_metrics.csv           (Wilson CIs)
  ├── m048_feature_distribution.csv        (chi-sq results + raw counts)
  ├── m048_fna_compliance_by_race.csv
  ├── m048_bethesda_x_race_x_tr.csv
  ├── m048_inflation_by_race.csv           (TR4/TR5 patient–nodule pp)
  └── m048_qa_gates.csv

M048_submission_package/
  ├── figures/
  │   ├── Figure_1_Cohort_Flow_by_Race.pdf/.png
  │   ├── Figure_2_ROC_by_Race.pdf/.png            (overlaid; one curve per race)
  │   ├── Figure_3_ROM_by_Race_Patient.pdf/.png    (paired bars by race × TR)
  │   ├── Figure_3b_ROM_by_Race_Nodule.pdf/.png    (paired bars by race × TR)
  │   ├── Figure_4_Inflation_by_Race.pdf/.png      (forest-style by race × TR)
  │   ├── Figure_5_Feature_Distribution.pdf/.png   (5 small multiples)
  │   └── Figure_S1_Bethesda_x_Race_x_TR.pdf/.png  (heatmap, faceted by race)
  ├── manuscript/
  │   ├── 01_Title_Page.docx
  │   ├── 02_Manuscript_Main.docx
  │   ├── 03_Tables.docx
  │   └── 04_Figures_with_Legends.docx
  └── cover_and_admin/
      ├── 00_Cover_Letter.docx
      ├── 01_Highlights.docx
      └── 02_Submission_Checklist.docx
```

---

## Implementation steps for Cursor

### Step 1 — Run SQL on MotherDuck

Execute `studies/m048_racial_disparities_tirads/M048_motherduck_queries.sql`
section-by-section. After each `CREATE OR REPLACE TABLE`, dump the table to
CSV under `studies/m048_racial_disparities_tirads/`.

```python
import duckdb, pandas as pd, os, json
from datetime import datetime, timezone
from motherduck_client import get_token  # repo root

DB = "thyroid_canonical_publication_v1_0"
con = duckdb.connect(f"md:{DB}?motherduck_token={get_token()}")
con.execute("USE thyroid_canonical_publication_v1_0;")
con.execute("SET schema = 'manuscript_workspace';")

for stmt in open("studies/m048_racial_disparities_tirads/M048_motherduck_queries.sql").read().split(";"):
    if stmt.strip():
        con.execute(stmt)
```

### Step 2 — Compute Wilson CIs and bootstrap AUC CIs in Python

Reuse `M025_FINAL_PACKAGE/m025_sensitivity_lib.py` Wilson + bootstrap helpers.
Stratify bootstrap on race × grain (sample with replacement within stratum,
1,000 reps, 95% percentile CIs).

### Step 3 — Statistical tests

For per-feature score distribution (Section 5 in the SQL), pivot
`m048_feature_distribution.csv` to wide and run scipy.stats.chi2_contingency
on the **(race × score)** contingency for each of the 5 features. Apply
Bonferroni correction (alpha = 0.05 / 5).

### Step 4 — Figures (matplotlib, 300 dpi PNG + vector PDF)

Mirror `M025_submission_package/figures/build_figures.py` style and palette.
Race color encoding: Black=`#1f4e79`, White=`#7a7a7a`, Asian=`#c55a11`,
Other=`#9c9c9c`, Unknown light grey.

- **Figure 2 (ROC by race):** three solid curves (Black/White/Asian),
  diagonal reference line, Youden-optimal point per race annotated.
- **Figure 3 (ROM patient grain × race):** grouped bars; race × TR; ACR bands
  shaded.
- **Figure 3b (ROM nodule grain × race):** same layout at nodule grain.
- **Figure 4 (Inflation by race):** forest-plot style; one row per race × TR
  pairing with patient–nodule pp delta and 95% CI.
- **Figure 5 (Feature score distribution by race):** 5 small multiples
  (composition / echogenicity / shape / margin / foci); stacked bar showing
  proportion of nodules at each score level, faceted by race; chi-sq p-value
  in panel.
- **Figure S1 (Bethesda × race × TR heatmap):** three heatmaps faceted by race.

### Step 5 — Manuscript DOCX (mirror M025 build_submission_docs.py)

Reuse the python-docx skeleton from `M025_submission_package/build_submission_docs.py`.
Substitute M048 abstract/body/tables/figures. Targets:

- **Title:** *Racial Disparities in ACR TI-RADS Performance: A 25-Year
  Operative Thyroid Cohort Stratified by Patient and Nodule Grain.*
- **Target journal:** Thyroid (primary) or JAMA Otolaryngology (secondary).
- **Abstract structure:** Background / Methods / Results / Conclusions; ≤250
  words.
- **Body:** ~3,500 words (within Thyroid Original Article limit).
- **References:** Vancouver, 25–35 entries. Pre-seed: Tessler 2017, Haugen
  2015, Wright 2022, Ramonell 2022, plus 5–8 prior race-stratified thyroid
  papers (literature search needed; see Cursor task in Step 6).

### Step 6 — Light literature search

Run a short Elicit-equivalent search:
> *"Racial or ethnic disparities in thyroid ultrasound risk-stratification
> performance, ACR TI-RADS, ATA, in operative cohorts. Identify any
> published studies with race-stratified per-TR ROM or AUC."*

Capture 5–8 citations with extraction notes. Do not duplicate M025
references unrelated to race.

### Step 7 — QA gates

Before sign-off, verify:
- Patient cohort race totals match M025 Table 7 (Black 1,535 / White 1,382 /
  Asian 204).
- Nodule strict cohort totals reconcile to M025 (3,687 strict eligible).
- Overall AUC values (across all races pooled) reproduce M025 (patient
  0.6478, nodule 0.6399).
- Per-race AUC bootstrap CIs do not include 0.5 (or note explicitly when they
  do — particularly small-Asian-stratum risk).
- All Wilson CI bounds in [0, 100].

---

## Pre-emptive caveats (write into Limitations)

1. **Asian stratum power:** n=204 patients overall; nodule-strict subset
   likely <250. AUC and TR-specific ROM CIs will be wide. Acknowledge.
2. **Self-reported race:** US-EHR self-report; we are not testing biological
   ancestry. Frame the disparities question as social-determinants /
   institutional-pathway, not biology.
3. **Mixed-race / Unknown handling:** kept descriptive; do not include in
   inferential primary tests.
4. **Multiple comparisons:** the 5-feature chi-square gets Bonferroni;
   per-TR comparisons (5 categories × 3 race strata × 2 grains) are
   descriptive — mention that joint inference would require a hierarchical
   model.

---

## Coordination notes

- Update `MASTER_MANUSCRIPT_LIST_DATA_READINESS_20260504.xlsx` row for ID 48
  Status from "Proposed" → "In Progress" once SQL has been run.
- Create migration sign-off `mig_315_m048_racial_disparities` once tables are
  populated (mirror mig_307 pattern in `qc_framework_v1/migrations/`).
- When the manuscript reaches DRAFT v0.1, drop a Cowork handoff prompt at
  `COWORK_HANDOFF_M048_*.md` for the writing pass.

---

## Definition of done

- [ ] All eight SQL tables (m048_*_v1) populated and signed off.
- [ ] Per-race AUC, threshold metrics, ROM, and feature-distribution CSVs
      committed to `studies/m048_racial_disparities_tirads/`.
- [ ] Seven figures rendered as 300 dpi PNG + vector PDF in
      `M048_submission_package/figures/`.
- [ ] Manuscript DOCX skeleton committed (Title, Methods, Results, Discussion
      stub).
- [ ] QA gates pass.
- [ ] mig_315 migration signed off.
- [ ] Update MASTER list row 48 Status → "In Progress".

When finished, post a one-paragraph Cowork summary noting the per-race AUC
deltas, per-race TR4/TR5 inflation magnitudes, and any feature-distribution
disparities that survived Bonferroni correction.
