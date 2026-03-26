# Manuscript state audit — 2–4 cm extent + molecular

**Scope:** This audit treats [`studies/proposal_2to4cm_extent_molecular_20260326/`](.) as the **only** quantitative source of truth for this paper. Repo-root KPIs ([`README.md`](../../README.md)), [`MANUSCRIPT_TRACKER.md`](../../MANUSCRIPT_TRACKER.md), and other study folders are **not** used to override numbers here.

**MotherDuck:** This documentation pass used **artifact-first** verification (CSVs, JSON, local Python aggregates). No MotherDuck tables, views, or data were created, altered, or deleted.

---

## 1. Study question (inferred from current-folder materials)

Among adults undergoing **first qualifying hemithyroidectomy or total thyroidectomy** in the integrated thyroid research database, what **patient-level characteristics** (including preoperative imaging nodule size 2.0–4.0 cm, Bethesda category, limited preoperative molecular testing, and imaging covariates in extended models) are **associated** with **initial total thyroidectomy** versus **initial lobectomy**?

Secondary / exploratory layers in the same folder:

- **Broad vs strict nodal exclusion:** sensitivity cohort with broader suspicious-node exclusion (N=635 vs N=558).
- **Completion thyroidectomy** after initial lobectomy (descriptive rates in [`table7_completion_thyroidectomy.csv`](table7_completion_thyroidectomy.csv); multivariable model **not interpretable** — zero events, separation).
- **Molecular-tested subset** (n=20): descriptive concordance ([`table6_molecular_pathology_concordance.csv`](table6_molecular_pathology_concordance.csv)); multivariable logistic outputs have **severely limited sample size and separation** — not primary inference.

---

## 2. Design elements

| Element | Definition (current folder) |
|--------|-----------------------------|
| **Primary exposure (for descriptive Table 1)** | Initial procedure: `procedure_normalized` ∈ {`hemithyroidectomy`, `total_thyroidectomy`} on **first** qualifying episode (see [`cohort_logic.py`](cohort_logic.py), [`study_pipeline.py`](study_pipeline.py) `run()`). |
| **Primary outcome (for regression)** | Binary **`initial_total`** (1 = first procedure was total thyroidectomy, else 0 among lobectomy-eligible rows). Same analytic frame as exposure stratification but aligned with logistic outcome column in pipeline. |
| **Comparator** | Patients who underwent **initial lobectomy** (binary `initial_lobectomy` = 1) for univariable stratification and Table 1; logistic models contrast covariate profiles between those with vs without `initial_total` on the shared cohort. |
| **Index size (primary cohort)** | Preoperative imaging index nodule 2.0–4.0 cm using pipeline logic (`preop_imaging_size_cohort`, `imaging_nodule_long_v2.size_cm_max`, exam on/before index surgery). See [`cohort_build_log.md`](cohort_build_log.md). |
| **Sensitivity (pathology size)** | Intended pathology-defined 2.0–4.0 cm arm; **current analytic N = 0** after linkage and exclusions ([`cohort_build_log.md`](cohort_build_log.md), [`analysis_manifest.json`](analysis_manifest.json) `path_sensitivity_n`). |
| **Strict nodal exclusion** | Documented in [`supplement_exclusions_and_definitions.csv`](supplement_exclusions_and_definitions.csv): CT/MRI pathologic lymph nodes preop; or Bethesda-6 node-specimen FNA (`strict_ln_exclusion`). |
| **Broad nodal exclusion** | `broad_sensitivity`: any suspicious node on exam or `suspicious_node_flag` on nodule preop. |
| **Primary analytic N** | **558** (`patient_level_dataset.csv`, [`validation_report.md`](validation_report.md), manifest). |
| **Broad preop cohort N** | **635** (`patient_level_dataset_broad_nodal_exclusion.csv`, manifest). |

---

## 3. Source hierarchy (conflict resolution)

When sources disagree, rank as follows:

1. **Final generated tables/CSVs and** [`analysis_manifest.json`](analysis_manifest.json) **in this folder.**
2. **Scripts that regenerate them:** [`study_pipeline.py`](study_pipeline.py), [`cohort_logic.py`](cohort_logic.py).
3. **Draft prose** in this folder (e.g. [`manuscript_full_draft.md`](manuscript_full_draft.md)).
4. **Repo-level trackers/readmes** — context only; never replace (1).

---

## 4. Known stale / ambiguous artifacts

- **[`cohort_flow.csv`](cohort_flow.csv)** / **[`cohort_flow.md`](cohort_flow.md):** Some intermediate `n` values are **zero** (e.g. pathology size arm, path cohort after strict exclusion) while later steps show **558** / **635**. This is **consistent** with an **empty pathology sensitivity analytic set** and with pipeline ordering, but the flow table is **easy to misread** without [`cohort_build_log.md`](cohort_build_log.md). **Prefer** cohort_build_log + validation_report + row counts on `patient_level_dataset.csv`.
- **`journal_style_results.md`:** Single-line file; redundant with CSVs — low authority.
- **`model_summary_final.csv`:** Convenience rollup; **completion_after_lobe** and **molecular_subset** rows reflect **separation / non-convergence** — align with [`logistic_completion_after_lobe.csv`](logistic_completion_after_lobe.csv) and [`model_summary_final.csv`](model_summary_final.csv) `separation_flag` before citing ORs.

---

## 5. Overreach flags (prose and interpretation)

- **Causal language** (e.g. “effect of X on choice of total thyroidectomy”) — **avoid**; use **associated with**, **adjusted odds ratios** in a **cross-sectional** surgical cohort.
- **Pathology-defined sensitivity analysis** — do **not** imply a second completed N&gt;0 cohort; current **N=0**.
- **Molecular inference** — full cohort has **20/558** preoperative molecular tests; subgroup models are **exploratory only**.
- **Completionafter lobectomy** — **zero** documented completion events in [`table7_completion_thyroidectomy.csv`](table7_completion_thyroidectomy.csv); do **not** cite completion logistic ORs as meaningful.
- **Generalizability** — single integrated database; residual confounding and selection into surgery/FNA/molecular testing **not** fully observable from artifacts.

---

## 6. Inventory — current folder only

### Manuscripts and narrative

| File | Role |
|------|------|
| `manuscript_full_draft.md` | Abstract-only seed |
| `abstract_only.md` | Abstract variant |
| `journal_style_results.md` | Minimal results line |
| `supplement.md` | Supplement draft |
| `figure_legends.md` | Superseded by `figure_legends_v1.md` |
| `analysis_plan.md` | Short study classification |
| `cohort_build_log.md` | **Canonical cohort narrative** |
| `cohort_flow.md` / `cohort_flow.csv` | Flow counts (see ambiguity above) |
| `schema_notes.md` | Variable/schema notes |
| `findings_note.md` | Findings note |
| `qa_reconciliation.md` | QA reconciliation |
| `validation_report.md` | Row-count / ID integrity |
| `strobe_tripod_gap_check.md` | Prior gap check |
| `ambiguity_audit.csv` | Multi-lesion ambiguity audit |
| `exploratory_note_rationale_snippets.csv` | Truncated note snippets |

### Tables / data (CSV)

| File | Role |
|------|------|
| `patient_level_dataset.csv` | Primary analytic cohort (N=558) |
| `patient_level_dataset_broad_nodal_exclusion.csv` | Broad exclusion cohort (N=635) |
| `lesion_level_dataset.csv` | Lesion-level export |
| `completion_cases.csv` | Completion linkage |
| `surgery_extent_audit.csv` | Extent audit |
| `table1_by_initial_extent.csv` | Table 1 |
| `table2_multivariable_total_vs_lobectomy.csv` | Pooled multivariable OR tables |
| `logistic_*.csv` | Per-model OR tables |
| `univariable_tests.csv` | Univariable tests |
| `table3_molecular_tested_subset.csv` / `logistic_molecular_subset.csv` | Molecular subset model |
| `table4_thyroseq_subgroup.csv` / `logistic_thyroseq_only.csv` | ThyroSeq-only model |
| `table5_afirma_subgroup.csv` / `logistic_afirma_only.csv` | Afirma-only model |
| `table6_molecular_pathology_concordance.csv` | Concordance summaries |
| `molecular_concordance_cases.csv` | Case listing |
| `table7_completion_thyroidectomy.csv` | Completion rates (lobectomy subgroup) |
| `missingness_summary.csv` | Column missingness (wide export in current file — two cohort blocks) |
| `sensitivity_summary.csv` | Sensitivity summary |
| `model_performance.csv` / `model_summary_final.csv` | Model rollup |
| `supplement_exclusions_and_definitions.csv` | Rule definitions |
| `variable_source_map.csv` / `source_inventory.csv` | Provenance mapping |
| `initial_ultimate_extent_transition_counts.csv` | Extent transitions |
| `baseline_table_primary.csv` / `baseline_table_broad_nodal.csv` | Baseline variants (if used) |

**Spreadsheet:** No `.xlsx` in folder.

### Figures (`fig_*.png`)

| File | Role | Main text? |
|------|------|------------|
| `fig_cohort_flow.png` | Pipeline cohort counts (zeros for pathology arm / intermediate steps per `cohort_build_log.md`) | **Yes — Figure 1** |
| `fig_forest_total_vs_lobectomy.png` | Forest plot, **primary_parsimonious** model | **Yes — Figure 2** |
| `fig_completion_rates.png` | Completion bars (empty at export) | **No** — do not submit |
| `fig_bethesda_by_extent.png` | Extent counts by group | Optional / redundant with Table 1 |
| `fig_initial_to_ultimate_extent.png` | Initial vs ultimate extent | Optional supplement only |
| `fig_molecular_result_by_extent.png` | Rates by molecular class (tiny N per cell) | **No** — exploratory, misleading if overread |
| `fig_platform_specific_extent.png` | Platform N bar chart | **No** — overlapping labels |

**No** separate `.svg` / publication `.pdf` exports are in-folder; journals may require vector replot.

### Code

| File | Role |
|------|------|
| `study_pipeline.py` | End-to-end extract, cohorts, tables, models *(MotherDuck **read** queries; **local** writes only)* |
| `cohort_logic.py` | Cohort construction helpers |
| `generate_manuscript_bundle.py` | Reads **local** CSVs only; generates auxiliary markdown |
| `run_schema_audit.py` | Schema audit helper |

### Logs / manifests

| File | Role |
|------|------|
| `analysis_manifest.json` | Run timestamp, git_sha, primary/broad N, path_sensitivity_n, DuckDB version |
| `claims_ledger.csv` | Partial claim index (superseded by `CLAIM_SOURCE_LEDGER.md`) |

### Bibliography

| File / note |
|-------------|
| `references_working_20260326.md` — Vancouver-style working list (Grok-sourced; **NEEDS AUTHOR CHECK** on incomplete entries). |
| **No** `.bib` / `.ris` machine-readable export in folder. |

---

## 7. README / discoverability

Study [`README.md`](README.md) should point to `manuscript_submission_v1.md` and this audit trio (updated in this task).
