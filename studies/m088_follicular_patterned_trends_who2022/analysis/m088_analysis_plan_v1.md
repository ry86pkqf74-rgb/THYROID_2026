# M088 — Pre-specified Analysis Plan, version 1.0

**Working title:** Twenty-five years of follicular-patterned thyroid neoplasms: incidence, oncocytic separation, and impact of the 2022 WHO reclassification in a single-institution surgical cohort (1990–2025)

**Manuscript code:** M088
**Owner:** Logan Glosser
**Plan-lock date:** 2026-05-09 (Cowork session 1)
**Lock authority:** This plan is the pre-specified analytic protocol for M088. Any change after lock requires a row in Manuscript Feedback Log (`tblYSCBzRFC4RGPMq`) with `change_type = restructure` or `data_correction` and a justification.

---

## 1. Aim (verbatim from Airtable record)

Quantify temporal incidence and proportional shift of follicular adenoma (FA), oncocytic (Hurthle) adenoma (OA), follicular tumor of uncertain malignant potential (FT-UMP), well-differentiated tumor of uncertain malignant potential (WT-UMP), NIFTP, follicular thyroid carcinoma (FTC), oncocytic carcinoma, and DHGTC across 1990–2025; apply 2022 WHO criteria to historical diagnoses to produce a reclassification matrix; quantify the immediate index-encounter surgical management impact of the reclassification (extent of resection, completion thyroidectomy intent at index).

**Scope is intentionally cross-sectional.** No longitudinal/recurrence/survival endpoints. No imaging or molecular discrimination modeling (those belong to M089/M093/M097). FV-PTC variant of PTC is not included as an arm (M091/M092 own that thread).

## 2. Hypotheses

- **H1.** Under 2022 WHO, a quantifiable proportion of historical Hurthle / oncocytic-variant FTC diagnoses migrate to the formally separate Oncocytic Neoplasm family (OA / OTUMP / OC). The migration is driven primarily by re-assignment of FTC oncocytic_warthin variant (n=209) and Hurthle cell adenoma (n=143) to the oncocytic family, plus 2 explicit HCC.
- **H2.** Under 2022 WHO, a quantifiable proportion of historical follicular adenomas (n=707) reclassify to FT-UMP when capsular invasion is judged equivocal (`finding_status IN ('indeterminate','suspected')` or `evidence_qualifier` in the equivocal vocabulary).
- **H3.** Under 2022 WHO, a quantifiable proportion of historical minimally invasive FTC (n=169) reclassify to FT-UMP when capsular invasion qualifier is `minimal`/`focal`/`single focus`/`partial` AND no documented vascular invasion.
- **H4.** The reclassified cohort would have had materially different index-encounter surgical management — quantified for two endpoints: (a) extent of resection (hemi vs total at index) and (b) completion thyroidectomy at index. RAI ordered/received at index has been **dropped from H4** per Notable Finding `NF-2026-05-09-rai-extraction-sparse-follicular-cohort` (BQ NLP extraction of `rai_treatment_episode_v2` is too sparse for use; <2% likely_received across all groups).

## 3. Cohort

**Inclusion.** All surgical resections in `thyroid-canonical-pub-2026.pub_canonical` 1990-01-01 through 2025-12-31, primary thyroid tissue, with `canonical_diagnosis_unified_v1.diagnosis_primary` in: `follicular_adenoma`, `hurthle_cell_adenoma`, `FTUMP`, `atypical_follicular_adenoma`, `NIFTP`, `FTC` (any variant), `HCC`, `DHGTC`, `PDTC`, `hyalinizing_trabecular_tumor`.

**Exclusion.** Classical PTC, medullary, anaplastic without follicular-patterned histology, lymphoma, metastatic to thyroid, consult-only specimens without primary signout, and recurrent/persistent disease (`canonical_histology_lookup_v1.is_recurrent = TRUE` join).

**BQ-verified counts at lock (2026-05-09).**

| diagnosis_primary | n |
|---|---|
| follicular_adenoma | 707 |
| FTC (oncocytic_warthin 209, minimally_invasive 169, widely_invasive 12, null 106) | 496 |
| hurthle_cell_adenoma | 143 |
| NIFTP | 116 |
| FTUMP | 34 |
| atypical_follicular_adenoma | 33 |
| DHGTC | 11 |
| HCC | 2 |
| hyalinizing_trabecular_tumor | 4 |
| PDTC | 1 |
| **Total** | **1,547 (BQ Q1 returned 1,542 distinct research_id; difference is patients with multiple primaries)** |

The cohort total is locked as 1,542 distinct research_id at the patient level; entity-level counts above are tumor-primary-level.

## 4. Reclassification rules (2022 WHO)

### 4.1 H1 — Oncocytic family migration (Tier A: deterministic)

| Historical | 2022 WHO assignment |
|---|---|
| `hurthle_cell_adenoma` | Oncocytic Adenoma (OA) |
| `HCC` (n=2) | Oncocytic Carcinoma (OC) |
| `FTC` + `oncocytic_warthin` variant | Oncocytic Carcinoma (OC) |
| `FTUMP` with oncocytic morphology in `tumor_N_histology_comment` | Oncocytic UMP (OTUMP) |
| `atypical_follicular_adenoma` with oncocytic morphology in `path_synoptics.hurthle_cell_oncocytic_adenoma` | OA or OTUMP per invasion status |

Tier A is deterministic and does not require re-review. Tier B (sister-manuscript M090 blinded re-review) is acknowledged but out of scope for M088 v1.

### 4.2 H2 — FA → FT-UMP migration

A historical `follicular_adenoma` reclassifies to FT-UMP if review identifies capsular invasion as `indeterminate` or `suspected` (not absent) under 2022 WHO criteria.

**Sensitivity analyses, locked:**
- **Strict:** `finding_status IN ('indeterminate','suspected')` OR `evidence_qualifier IN ('equivocal','uncertain','possible','questionable','indeterminate','cannot be assessed','infiltrative?','focal suggestion of penetration','focal suspicious','focally')`.
- **Broad:** Strict ∪ `evidence_qualifier IN ('focal','partial','single focus','minimal')`.

Both bounds are reported. Both are stratified by 5-year era to test for diagnostic drift.

### 4.3 H3 — MI-FTC → FT-UMP migration

A historical minimally invasive FTC reclassifies to FT-UMP if (a) capsular invasion qualifier is in {`minimal`, `minimally invasive`, `focal`, `single focus`, `partial`, `yes (minimal)`, `yes (focal)`} OR `finding_status IN ('indeterminate','suspected')` AND (b) no documented vascular invasion in `canonical_vascular_invasion_events_v1`.

**Sensitivity analyses, locked:**
- **Strict:** only `finding_status = 'indeterminate'` AND no vascular.
- **Moderate:** strict + qualifier `'minimal'`/`'minimally invasive'`.
- **Broad:** moderate + qualifier `'focal'`/`'single focus'`/`'partial'`.

Cross-check with `canonical_path_indeterminate_events_v1.angioinvasion_quantify` (FLOAT64): if `quantify ≥ 1`, vascular is unequivocal under 2022 WHO and the case does NOT reclassify regardless of capsular qualifier.

### 4.4 H4 — Index-encounter management impact

Two endpoints (RAI dropped per NF-2026-05-09-rai-extraction-sparse-follicular-cohort):

1. **Extent of resection at index:** binary {hemi, total}. Definition: `canonical_operative_patient_rollup_v1.n_total_thyroidectomies > 0` at index → total; otherwise → hemi. Where the index-encounter surgery is unambiguous from `earliest_surgery_date` and `canonical_operative_events_v1.resolved_surgery_date`.
2. **Completion thyroidectomy at index:** binary. Definition: `n_completion_thyroidectomies > 0`.

**Comparison:** within each historical_dx group, observed rate vs. counterfactual rate under 2022 WHO reclassification. Counterfactual = the rate the patient would be expected to receive under 2022 criteria, which we approximate using the observed rate of patients in the reclassified-target group (after stratifying for entity). Bootstrap 1000 iterations; 95% nonparametric CI for Δrate.

## 5. Statistical methods

- **Trends.** Annual entity counts by primary 1990–2025. Joinpoint regression (`pyjoinpoint` or PyJoinpoint) with **pre-specified breakpoints** at 2008 (Bethesda), 2015 (ATA guidelines + WHO 4th edition), 2017 (NIFTP introduction), 2022 (WHO 5th + oncocytic family). Report annual percent change (APC) with 95% CI per segment. LOESS sensitivity overlay.
- **Reclassification rates.** Proportions with **95% Wilson confidence intervals**. Stratified by 5-year era (1990–94, 1995–99, 2000–04, 2005–09, 2010–14, 2015–19, 2020–25). Era × historical_dx interaction tested with chi-squared on era × reclassification_target. Multiple-comparison adjustment: **Benjamini-Hochberg FDR (q < 0.05)** across the H1/H2/H3 sensitivity-bound family.
- **Index-encounter impact.** Within-group Δrate (observed historical vs. counterfactual reclassified) with 1000-iteration nonparametric bootstrap 95% CIs.
- **Missingness.** Per-field missingness reported in Methods. If <40% missing on a critical field (capsular invasion, surgery type, completion thyroidectomy), MICE multiple-imputation sensitivity is run. If ≥40% missing, complete-case primary + missingness pattern as a limitation.
- **Multifocality.** For multifocal cases (`multifocality_flag = TRUE`, `n_tumors > 1`), the dominant tumor (largest `size_greatest_dimension_cm`) is used in the index analysis; sensitivity using any-tumor-positive logic is reported in supplement.
- **Pre-specification lock.** This plan is locked at v1.0. Any deviation requires a Manuscript Feedback Log row with `change_type = restructure` or `data_correction` and the deviation justification.

## 6. Deliverables

### Code (analysis/)

- `m088_cohort_assembly.sql` — cohort definition CTE.
- `m088_h1_oncocytic_migration.sql`
- `m088_h2_fa_to_ftump.sql`
- `m088_h3_mi_ftc_to_ftump.sql`
- `m088_h4_management_impact.sql`
- `m088_h4_rai_feasibility.sql` (already run; preserved for audit trail of the NF)
- `m088_trends_joinpoint.py`
- `m088_tables.py` (Tables 1–4)
- `m088_figures.py` (Figures 1–4)

### Tables (tables/)

1. **Table 1** — Demographics × entity (n, sex, age at surgery, era, multifocality, dominant-tumor size).
2. **Table 2** — Era × entity counts (1990–94, 1995–99, …, 2020–25).
3. **Table 3** — Reclassification matrix (historical → 2022 WHO label) with strict/broad bounds for H2/H3.
4. **Table 4** — Index-encounter management impact (Δrate with 95% CI, by historical → reclassified transition).

### Figures (figures/)

1. **Figure 1** — CONSORT cohort flow.
2. **Figure 2** — Annual incidence trends 1990–2025 with WHO breakpoint overlay (vertical lines at 2008/2015/2017/2022) and joinpoint segments + APC labels.
3. **Figure 3** — Sankey: historical_label → 2022_WHO_label (colored by reclassification family).
4. **Figure 4** — Forest plot of management-impact Δrate (extent + completion) with bootstrap 95% CI by transition type.

### Manuscript (manuscript/)

- `m088_draft_v0.1.md` — IMRaD, ~3000 words target (methods may exceed); placeholders for Tables/Figures.

### Airtable artifacts

- **Sections** rows (table `tblU9JLinirdcXUb8`): Abstract, Introduction, Methods, Results-H1, Results-H2, Results-H3, Results-H4, Discussion, Limitations, Conclusion.
- **Tables and Figures** rows (table `tblR10rBaDTeTcABv`): one per Table 1–4, Figure 1–4 with `last_regenerated`.
- **Manuscript Feedback Log** rows: one per major content edit.

### Linear

- Per skill rules, no per-manuscript Linear project is auto-spawned at status=Planned. The project will spawn when status advances to Cohort Definition or Analysis. This session does NOT advance status.
- Notable Finding issue THY-55 already filed for the RAI extraction observation.

## 7. Triggers to stop and ask Logan

- BQ verification queries deviate from expected by >10% (NOT triggered at lock; all four queries within 0.13% tolerance).
- A reclassification rule produces a borderline cohort >20% of the historical group. If H2 broad-bound shows >20% of FAs reclassifying to FT-UMP, surface and ask before going to publication.
- A Notable Finding emerges (already triggered for RAI; remain alert for trend or reclassification surprises).
- Index-encounter management data is too sparse to support H4 (already triggered for RAI; remaining endpoints — extent and completion — pass: 1,537 patients in operative rollup vs. 1,542 cohort = 99.7% coverage).
- Sister-manuscript cohort overlap creates ambiguity on a case label.

## 8. Sister-manuscript coordination

| Sister | M088 interaction |
|---|---|
| **M090** (FT-UMP reproducibility re-review) | M088 supplies case-selection criteria; M090 supplies blinded re-review labels. M088's Tier A reclassification matrix gets a Tier B refinement from M090 in v2. |
| **M091** (NIFTP cohort 2016–2025) | M088 reports NIFTP at family-incidence level only; M091 owns the deep dive. |
| **M092** (EFV-PTC vs NIFTP reproducibility) | M088 does NOT touch FV-PTC. |
| **M093** (Oncocytic preoperative discrimination) | M088 H1 supplies historical-counts numerator; M093 owns preoperative discrimination. |
| **M095** (DHGTC series) | M088 reports DHGTC at family-counts level only. |
| **M096** (MI vs WI vs angioinvasive FTC) | M088 H3 (MI-FTC → FT-UMP) overlaps. Cohort definitions to be reconciled before M088 submission and M096 cohort lock. |
| **M097** (Multisystem TIRADS) | M088 does NOT touch TIRADS. |

If a M088 result changes a cohort definition affecting any sister, file a Notable Finding tagged via `applies_to_manuscripts`.

## 9. Plan-lock signature

Locked 2026-05-09 by Cowork session 1, Logan Glosser supervising. Any subsequent deviation requires a Manuscript Feedback Log row.
