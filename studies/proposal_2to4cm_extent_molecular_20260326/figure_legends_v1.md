# Figure legends — submission v1 (2026-03-26)

> **Status (2026-03-27):** **`figure_legends_v2.md`** is the **preferred** file for journal submission. Publication assets: **`fig1_cohort_flow_publication.png`/`.pdf`**, **`fig2_forest_primary_publication.png`/`.pdf`**. This v1 file documents **legacy** 150 DPI exports produced by `study_pipeline.py` for replication; **do not** treat v1 as the canonical submit set.

## Legacy pipeline exports (internal / reproducibility)

| File | Role |
|------|------|
| `fig_cohort_flow.png` | **Legacy Figure 1**—horizontal bar chart from the frozen pipeline export (truncated labels). |
| `fig_forest_total_vs_lobectomy.png` | **Legacy Figure 2**—forest plot, **primary parsimonious** logistic model. |

## Not for main submission (retained in folder)

| File | Reason |
|------|--------|
| `fig_completion_rates.png` | Horizontal bars: **OED pipeline** completion rates (ever, 30/90/365 d; **0** in this cohort) vs **path-synoptic definite** completion (`table7_completion_thyroidectomy.csv`). **Supplemental** use; relabel for journal style. |
| `fig_molecular_result_by_extent.png` | Exploratory; **tiny** denominators by molecular class; risks misreading as performance—**not** a main figure. |
| `fig_platform_specific_extent.png` | **Overlapping** x-axis labels; descriptive **n=20** context only—**not** a main figure. |
| `fig_bethesda_by_extent.png` | Largely **redundant** with Table 1; optional only if a journal requests a simple bar summary. |
| `fig_initial_to_ultimate_extent.png` | Descriptive transition counts; **redundant** with text (25 lobectomy patients with ultimate total-class extent per `initial_ultimate_extent_transition_counts.csv`; **OED-only** completion flags remain **0/238**); optional supplement only. |

---

## Figure 1. Cohort selection (primary and sensitivity frames)

**File:** `fig_cohort_flow.png`.

**Legend:** Horizontal bar chart of **patient counts** at successive cohort-definition steps in `study_pipeline.py`, beginning from the thyroid surgical spine and applying imaging-defined **2.0–4.0 cm** preoperative index nodule selection, **strict** preoperative nodal exclusion (final primary cohort **N = 558**), and **broad** suspicious-node exclusion (**N = 635**). Bars with **zero** patients (e.g., pathology-defined 2.0–4.0 cm analytic arm in this freeze) appear empty and correspond to **`path_sensitivity_n = 0`** in `analysis_manifest.json`. Y-axis labels in this export are **truncated** pipeline step names; authors should replace with **full-text** labels for publication if required (`AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`).

---

## Figure 2. Adjusted odds ratios—primary parsimonious model

**File:** `fig_forest_total_vs_lobectomy.png`.

**Legend:** Forest plot of **adjusted odds ratios (95% CI)** for **initial total thyroidectomy** (binary outcome) associated with **age at surgery** (per year), **female sex** (reference male), **Bethesda category ≥4** (reference &lt;4 after pipeline coercion of missing Bethesda to not ≥4), and **any preoperative molecular test**, from **`logistic_primary_parsimonious.csv`** (**N = 558** complete cases). Vertical reference line at **OR = 1**. Title on export: “Forest: primary_parsimonious.”

---

## Tables

Primary numeric displays remain **tabular** as listed in `manuscript_submission_v1.md` (Tables 1–6).

---

## Supplemental figure (optional—not exported here)

**Extended-model forest plot** (predictors in **`logistic_primary_extended.csv`**, including bilateral nodule indicator and TIRADS score) is **not** present as a dedicated `.png` in this folder; authors may generate from the same plotting utility if a future pipeline run is approved, or build externally for an supplement.
