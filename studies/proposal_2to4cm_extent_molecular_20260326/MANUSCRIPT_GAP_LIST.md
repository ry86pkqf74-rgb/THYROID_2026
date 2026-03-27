# Manuscript gap list — submission readiness (2026-03-26)

Gaps are relative to a typical observational surgery manuscript and to **what this folder actually contains**.

---

## Essential content gaps

| Gap | Severity | Notes |
|-----|----------|-------|
| **Full IMRAD draft** | Lower | Addressed by `manuscript_submission_v1.md` + **Figures 1–2** callouts. |
| **References / bibliography** | **Low–Medium** | `references_working_20260326.md` — items **1–10** reconciled 2026-03-27 (Crossref / PMC). **Kim MH** (ex-ref 4) and **placeholder completion SR** (ex-ref 12) remain **removed** (`SCHOLAR_GPT_REFERENCE_RECONCILIATION_20260326.md`). |
| **Figure assets** | **Lower** | **Main:** `fig_cohort_flow.png`, `fig_forest_total_vs_lobectomy.png`. **Figure 1** labels truncated—production relabel recommended (`AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`). |
| **Institution, IRB, funding, COI** | **High** | `AUTHOR_INPUTS_REQUIRED_20260326.md` + `AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`. |
| **Exact calendar study period** | Medium | `surgery_year` exists in data but **year range not asserted** in prose without explicit summary table in folder. |
| **STARD/TRIPOD** | Low–N/A | Observational cohort, not diagnostic/prognostic model paper; STROBE is primary. |

---

## Methods / reproducibility gaps

| Gap | Notes |
|-----|-------|
| **Pseudo R²** | `study_pipeline.py` computes `pseudo_r2` in `fit_lr` return dict but **not** written to standard CSV outputs — optional addition in future pipeline run. |
| **Missing data in regression** | Models use `dropna()` on outcome + predictors (`fit_lr`); effective N may be **&lt;558** if unmeasured covariates added; current parsimonious set aligns with **558** in OR table `n` column. |
| **Multiple testing** | Univariable battery **without** formal multiplicity adjustment in outputs; gap for Methods transparency. |
| **Pathology size sensitivity** | **N=0** analytic set — sensitivity analysis **not completed** in current run. |

---

## Statistical reporting gaps (outputs)

| Item | Status |
|------|--------|
| **Completion after-lobe logistic** | **Zero events**; coefficients unstable — **omit** from primary results or label non-estimable. |
| **Molecular subset logistic** | n=20; **exploratory only**; some p-values/CIs not trustworthy. |
| **ThyroSeq-only / Afirma-only models** | `model_summary_final.csv` shows **separation flags** and extreme p-values — **not** primary manuscript material. |
| **Concordance tables** | Descriptive 2×2 counts only — **no** formal kappa or sensitivity/specificity CI in folder. |

---

## Reporting / ethics gaps

| Gap | Notes |
|-----|-------|
| **PHI in exploratory CSV** | `exploratory_note_rationale_snippets.csv` contains truncated notes — manuscript must **not** paste raw snippets without IRB-compliant clearance. |
| **Single-center / database** | Generalizability limitation must stay prominent. |

---

## STROBE-oriented checklist (short)

| Topic | Gap |
|-------|-----|
| Flow diagram | **Figure 1** present; may need **relabel** for publication (truncated axis text in export). |
| Non-participants | Not quantified separately from exclusions in one table **in manuscript** (exclusion logic in CSV + code). |
| **Address missingness** | `missingness_summary.csv` available; prose cites key columns (Bethesda, completion, molecular). |

---

## Resolved or improved by 2026-03-26 package pass

- **`figure_legends_v1.md`** — main vs non-submitted figures documented.  
- **`manuscript_submission_v1.md`** — numbered references; **no** `[REF:NEEDS_*]` tags; STROBE citation; figure callouts.  
- **`references_working_20260326.md`** — Working bibliography (verified **1–10** in two blocks; ScholarGPT reconciliation 2026-03-26).  
- **`AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`** — submission blockers consolidated.  
- **`journal_fit_matrix_v1.md`** — three concrete targets (*Thyroid*, *Head & Neck*, *Ann Surg Onc*).  
