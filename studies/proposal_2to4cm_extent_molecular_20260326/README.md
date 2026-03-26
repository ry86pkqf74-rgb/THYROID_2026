# Proposal: 2–4 cm extent + molecular (preoperative imaging cohort)

This folder contains a **retrospective cohort analysis** of **initial total thyroidectomy versus initial lobectomy** among patients whose **preoperative ultrasound-defined** index nodule was **2.0–4.0 cm**, with **strict** or **broad** preoperative nodal exclusion rules, plus **exploratory** molecular concordance among the small subset with preoperative molecular testing.

## Latest manuscript (submission package)

- **Main manuscript (IMRAD):** [`manuscript_submission_v1.md`](manuscript_submission_v1.md)
- **Structured abstract:** [`abstract_structured_v1.md`](abstract_structured_v1.md)
- **Audit / traceability:** [`MANUSCRIPT_STATE_AUDIT.md`](MANUSCRIPT_STATE_AUDIT.md), [`CLAIM_SOURCE_LEDGER.md`](CLAIM_SOURCE_LEDGER.md), [`MANUSCRIPT_GAP_LIST.md`](MANUSCRIPT_GAP_LIST.md)
- **Supporting submission docs:** [`supplement_methods_v1.md`](supplement_methods_v1.md), [`figure_legends_v1.md`](figure_legends_v1.md), [`strobe_checklist_v1.md`](strobe_checklist_v1.md), [`cover_letter_v1.md`](cover_letter_v1.md), [`journal_fit_matrix_v1.md`](journal_fit_matrix_v1.md), [`reviewer_attack_sheet_v1.md`](reviewer_attack_sheet_v1.md), [`revision_packet_v1.md`](revision_packet_v1.md)

## Regenerating analytic CSVs (optional)

Requires MotherDuck credentials (read/write client in script, but pipeline issues **SELECT** queries and writes **local** files only):

```bash
cd ../..   # repo root THYROID_2026
.venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/study_pipeline.py
```

Use **`analysis_manifest.json`** for the frozen **git SHA** and run timestamp associated with the CSVs.

## Prior short draft

- [`manuscript_full_draft.md`](manuscript_full_draft.md) — abstract-only seed (superseded for submission by `manuscript_submission_v1.md`).
