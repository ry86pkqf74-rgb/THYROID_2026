# Manuscript gap list — submission readiness

Gaps are relative to a typical observational surgery manuscript and to **what this folder actually contains**.

---

## Essential content gaps

| Gap | Severity | Notes |
|-----|----------|--------|
| **Full IMRAD draft** | **Was** high — addressed by `manuscript_submission_v1.md` | Prior `manuscript_full_draft.md` was abstract-only. |
| **References / bibliography** | High | No `.bib` or `.ris` in folder. Placeholder tags + `revision_packet_v1.md` NEEDS REFERENCE CHECK. |
| **Figure assets** | High | No `.png`/`.svg`/`.pdf` in folder — no figure submission package without new exports or external journal upload policy. |
| **Institution, IRB, funding, COI** | High | Not present in artifacts; must be supplied by authors. |
| **Exact calendar study period** | Medium | `surgery_year` exists in data but **year range not asserted** in prose without explicit summary table in folder. |
| **STARD/TRIPOD** | Low–N/A | Observational cohort, not diagnostic/prognostic model paper; STROBE is primary. |

---

## Methods / reproducibility gaps

| Gap | Notes |
|-----|--------|
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
|-----|--------|
| **PHI in exploratory CSV** | `exploratory_note_rationale_snippets.csv` contains truncated notes — manuscript must **not** paste raw snippets without IRB-compliant clearance. |
| **Single-center / database** | Generalizability limitation must stay prominent. |

---

## STROBE-oriented checklist (short)

| Topic | Gap |
|-------|-----|
| Flow diagram | No CONSORT-style figure file; `cohort_flow.csv` requires careful labeling (pathology arm zeros). |
| Non-participants | Not quantified separately from exclusions in one table **in manuscript** (exclusion logic in CSV + code). |
| **Address missingness** | `missingness_summary.csv` available; prose must cite key columns (FNA link, molecular, completion flags). |

---

## Resolved by this submission package

- `MANUSCRIPT_STATE_AUDIT.md` — inventory + hierarchy.
- `CLAIM_SOURCE_LEDGER.md` — numeric traceability.
- `manuscript_submission_v1.md` — IMRAD backbone.
- `supplement_methods_v1.md` — method alignment to code.
- `strobe_checklist_v1.md` — structured checklist with honest NAs.
- `revision_packet_v1.md` — reference TODOs and reproducibility notes.
