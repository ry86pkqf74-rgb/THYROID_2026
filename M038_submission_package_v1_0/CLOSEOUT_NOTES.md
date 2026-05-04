# CLOSEOUT NOTES

## What was delivered (M038 submission package v1.0)

✓ `01_title_page.docx` (placeholders for authors/IRB/disclosures)
✓ `02_manuscript.docx` (US Letter, Arial 11pt, 4 figures embedded)
✓ `03_supplement.docx` (supplementary Methods + Results + Tables S1–S6 reference)
✓ `04_tables.xlsx` (14 tabs: Cover + Tables 1–5 + Supp S1–S6 + Data dict + QA)
✓ `06_figures/` (4 PNG @ 300 DPI + 4 underlying CSV)
✓ `07_response_to_reviewers_template.docx`
✓ `08_analysis_code/` (SQL + 4 Python build scripts)
✓ `09_validation_report.md` (156-cell audit reconciliation)
✓ `00_README.md`, `CLOSEOUT_NOTES.md`

## What is parked

- **`05b_per_patient_with_sources.xlsx`** — Per-patient master (n=10,871 × ~80 cols) + Source Map. Build script staged at `08_analysis_code/build_m038_per_patient.py`. Awaiting MotherDuck `logan.glosser.eras@gmail.com` auth on local duckdb CLI; then a single `python3 build_m038_per_patient.py` run will produce the file. ~2 minutes once auth is in.

## Outstanding [VERIFY] items for Logan

1. Authorship list and corresponding author (title page + manuscript byline + supplement)
2. IRB protocol number (Methods §2.1 + title page)
3. Final journal selection (currently AMA-reference formatted)
4. ~30 AMA references (BibTeX stubs in `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib`)
5. Confidence intervals on RR estimates (per author-input gap #11; Wald or exact)
6. Funding & conflict-of-interest disclosures
7. Approve / revise the v2.1 Cursor-patched M038 v2 manuscript text (already applied)

## Push command (when ready to commit + push)

```bash
cd "$(git rev-parse --show-toplevel)"
git add -- M038_submission_package_v1_0/
git commit -m "manuscript(M038): v1.0 submission package + tables/figures + validation report"
git push origin main
```

## Hard constraint (carry-over)

Do NOT touch M044 / M051 — owned by the ChatGPT lane.
