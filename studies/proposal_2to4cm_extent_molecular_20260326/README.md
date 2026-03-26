# Proposal: 2–4 cm extent + molecular (preoperative imaging cohort)

This folder contains a **retrospective cohort analysis** of **initial total thyroidectomy versus initial lobectomy** among patients whose **preoperative ultrasound-defined** index nodule was **2.0–4.0 cm**, with **strict** or **broad** preoperative nodal exclusion rules, plus **exploratory** molecular concordance among the small subset with preoperative molecular testing.

## Latest manuscript (submission package)

- **Main manuscript (IMRAD):** [`manuscript_submission_v1.md`](manuscript_submission_v1.md)
- **Structured abstract:** [`abstract_structured_v1.md`](abstract_structured_v1.md)
- **Audit / traceability:** [`MANUSCRIPT_STATE_AUDIT.md`](MANUSCRIPT_STATE_AUDIT.md), [`CLAIM_SOURCE_LEDGER.md`](CLAIM_SOURCE_LEDGER.md), [`MANUSCRIPT_GAP_LIST.md`](MANUSCRIPT_GAP_LIST.md)
- **External context & author checklist (2026-03-26):** [`external_context_grok_live_literature_20260326.md`](external_context_grok_live_literature_20260326.md), [`external_context_elicit_molecular_background_20260326.md`](external_context_elicit_molecular_background_20260326.md), [`AUTHOR_INPUTS_REQUIRED_20260326.md`](AUTHOR_INPUTS_REQUIRED_20260326.md)
- **Bibliography / submission blockers:** [`references_working_20260326.md`](references_working_20260326.md), [`AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`](AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md)
- **Pre-submission QA (2026-03-26):** [`FINAL_QA_CHECKLIST_20260326.md`](FINAL_QA_CHECKLIST_20260326.md), [`RED_FLAG_SENTENCES_20260326.md`](RED_FLAG_SENTENCES_20260326.md), [`READY_TO_SUBMIT_STATUS_20260326.md`](READY_TO_SUBMIT_STATUS_20260326.md)
- **Supporting submission docs:** [`supplement_methods_v1.md`](supplement_methods_v1.md), [`figure_legends_v1.md`](figure_legends_v1.md), [`strobe_checklist_v1.md`](strobe_checklist_v1.md), [`cover_letter_v1.md`](cover_letter_v1.md), [`journal_fit_matrix_v1.md`](journal_fit_matrix_v1.md), [`reviewer_attack_sheet_v1.md`](reviewer_attack_sheet_v1.md), [`revision_packet_v1.md`](revision_packet_v1.md)
- **Main figures (PNG):** `fig_cohort_flow.png` (**Figure 1**), `fig_forest_total_vs_lobectomy.png` (**Figure 2**); see `figure_legends_v1.md` for non-submitted exports.

## Regenerating analytic CSVs (optional)

Requires MotherDuck credentials (read/write client in script, but pipeline issues **SELECT** queries and writes **local** files only):

```bash
cd ../..   # repo root THYROID_2026
.venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/study_pipeline.py
```

Use **`analysis_manifest.json`** for the frozen **git SHA** and run timestamp associated with the CSVs.

## Prior short draft

- [`manuscript_full_draft.md`](manuscript_full_draft.md) — abstract-only seed (superseded for submission by `manuscript_submission_v1.md`).
