# Author fill-ins for journal submission — 2026-03-26

Companion to **`AUTHOR_INPUTS_REQUIRED_20260326.md`** (institution, IRB, COI, funding). This file flags **submission-blocking** items surfaced while tightening the manuscript package.

---

## 1. Bibliography

| Item | Action |
|------|--------|
| References **1–10** in `references_working_20260326.md` | **Reconciled 2026-03-27** (Crossref/journal pages). Journal submission: apply target style (Vancouver/Harvard) and confirm pagination if proofs differ. |
| Optional background / Grok-only rows | Entries in `external_context_grok_live_literature_20260326.md` not mirrored in `references_working` — **verify before citing**. |
| Software | Add statsmodels / SciPy / NumPy per journal **Methods** policy if required. |

---

## 2. Figures (production)

| Item | Action |
|------|--------|
| **Figure 1** (`fig_cohort_flow.png`) | Pipeline export uses **truncated** y-axis labels and a **horizontal bar** layout. For submission, **relabeled** “publication” flow (full text per `cohort_build_log.md` / `cohort_flow.csv`) is **recommended** (redraw in Illustrator/PowerPoint or regenerate labels if a future pipeline run is approved). |
| **Figure 2** (`fig_forest_total_vs_lobectomy.png`) | Matches **primary parsimonious** model; suitable as main figure. Confirm font size meets target journal minimum on export. |
| **Optional supplemental** | `fig_completion_rates.png` (OED vs path-synoptic completion bars; non-blank after 2026-03-27 pipeline). `fig_molecular_result_by_extent.png` (tiny cell sizes; **not** a performance figure). `fig_platform_specific_extent.png` (**overlapping** x-labels). |
| **Not proposed as main text figures** | `fig_bethesda_by_extent.png`, `fig_initial_to_ultimate_extent.png` — redundant with **Table 1** / text; use only if a journal requests a simple extent diagram. |

---

## 3. Ethics and governance

| Item | Action |
|------|--------|
| IRB / ethics | Insert approved language + protocol or exemption ID in Methods. |
| Database / site name | Replace generic “integrated thyroid research database” if journal requires named site. |
| PHI | Do **not** paste raw note content from `exploratory_note_rationale_snippets.csv` without clearance. |

---

## 4. Cover letter and journal choice

| Item | Action |
|------|--------|
| Journal name | Replace **[JOURNAL TBD]** in `cover_letter_v1.md`. |
| Author instructions | Re-verify **word limits**, reference style, and figure file formats **at submit time** (see `journal_fit_matrix_v1.md`). |

---

## 5. Completeness checklist (quick)

- [ ] Final journal reference formatting applied (`references_working_20260326.md` items **1–10** verified at source)  
- [ ] Figure 1 labels legible at journal min resolution  
- [ ] IRB / funding / COI / contributions / corresponding author  
- [ ] Suggested reviewers (if requested)  
- [ ] Prior submission / preprint statement  
