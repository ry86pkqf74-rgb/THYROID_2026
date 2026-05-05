# M044 ETE — Final Submission Package (v5, journal-ready)

**Manuscript title:** Gross, but Not Microscopic, Extrathyroidal Extension Is Associated with Path-Proven Recurrence in Differentiated Thyroid Cancer: A Strict-Cohort Retrospective Study

**Cohort:** strict-DTC v1.1 (n = 3,578) | **Lock:** 2026-05-04 | **Path-proven events:** 105

---

## Files in this package

### Manuscript (use the **v5** files for submission)
- `M044_ETE_FINAL_Manuscript_v5.docx` — Word version, journal-ready (separate title page, structured abstract, double-spaced body, line numbering, full demographics table, complete administrative placeholders)
- `M044_ETE_FINAL_Manuscript_v5.md` — Markdown version mirroring the Word document
- `M044_ETE_FINAL_Tables_v5.tex` — LaTeX-ready Tables 1–4 (booktabs / threeparttable; expanded Table 1 with race, BMI, T-stage, stage group, molecular markers, AGES, deaths)

### Data
- `M044_ETE_FINAL_per_research_id_dataset.xlsx` — every analytic-relevant column for every patient (3,578 rows × 80 columns) with Cover, Patient analytic, Data lock, and Data Dictionary tabs

### Statistics
- `M044_ETE_FINAL_all_stats.xlsx` — every regression model run (13 models) with Cover, Crude rates, No-neg ETE audit, All-models long format, per-model tabs, Forest-plot data tab, and Published-vs-reproduced comparison tab

### Figures (each provided as PNG and SVG)
- `figures/M044_Figure1_CONSORT_flow.{png,svg}` — CONSORT-style cohort flow
- `figures/M044_Figure2_Recurrence_by_ETE.{png,svg}` — Crude path-proven and composite recurrence rates
- `figures/M044_Figure3_Forest_plot.{png,svg}` — Forest plot of gross-vs-microscopic ORs across 10 model specifications

### Documentation
- `M044_ETE_FINAL_Synthesis_Summary_v2.md` — full synthesis writeup with source-of-truth lineage, reproducibility verdict, and headline numbers tabulation

### Legacy v3/v4 files (kept for reference)
- `M044_ETE_FINAL_Manuscript_v4.docx` and `.md`, `M044_ETE_FINAL_Tables_v4.tex` — superseded by v5

---

## What's new in v5

1. **Academic publishing format applied throughout:**
   - Separate title page with full administrative fields (authors, affiliations, corresponding author, word count, keywords)
   - Structured abstract with bolded labels (Background / Methods / Results / Conclusions), within the 250-word journal limit
   - Double-spaced body, line numbering enabled (continuous, every line)
   - Each table on its own page; figure legends consolidated on a separate page; references in Vancouver style
   - Complete Declarations block (Ethics, Consent, Data availability, Code availability, Competing interests, Funding, Author contributions, Acknowledgments) as journal-ready placeholders

2. **Expanded baseline characteristics (Table 1 + Results narrative):**
   - Race (White, Black/African American, Asian, Other/Unknown)
   - BMI median (IQR)
   - Closest margin distance, mm
   - Aggressive variant prevalence
   - Full T-stage distribution (T1a, T1b, T2, T3a, T3b)
   - AJCC overall stage groups (I, II, III, IVB)
   - Lymphatic and vascular invasion at all five levels (present, extensive, focal, indeterminate, missing)
   - LN examined and LN positive medians
   - All four molecular markers (BRAF V600E, TERT promoter, RAS, RET fusion)
   - AGES score
   - All recurrence categories (path-proven, imaging-only-unconfirmed, composite, imaging-then-path, death)
   - Two new dedicated paragraphs in the Results "Cohort characteristics, demographics, and baseline pathology" subsection

3. **Verified MotherDuck lineage:** the source-of-truth view (`thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_m044_ajcc_ete_v1`) was confirmed to back the locked patient-level dataset; research_id MD5 (`368f06…d27c5b`) matches the eMethods Table 1 lock hash exactly.

4. **All headline numbers preserved verbatim** from the locked Excel deliverables (primary aOR 1.77 [1.15–2.71]; *p* = 0.009 for gross vs microscopic ETE; etc.).

5. **All v4 numerical corrections retained:** no/negative audit follow-up IQR `5.91` (not `5.89`); AGES medians as `8.90 [7.72–9.98] vs 5.86 [3.00–7.15]`.

---

## Submission checklist

- [x] Locked cohort size, ETE strata, and recurrence endpoints exact (3,578 / 68 / 2,359 / 1,151 / 105)
- [x] Locked analyst headline aOR 1.77 (95 % CI, 1.15–2.71; *p* = 0.009)
- [x] Structured abstract ≤ 250 words
- [x] IMRAD structure with full Methods, Tables 1–4, three figure legends, Discussion, Limitations, Future directions, and Declarations
- [x] Figures 1–3 referenced in sequence and provided in PNG + SVG
- [x] CT imaging context integrated (12 studies, 85–98 % specificity, 14–99 % sensitivity, gross ~80 % vs microscopic ~50 %)
- [x] Author/affiliation/IRB/funding/disclosure fields preserved as journal-ready placeholders (not invented)

Generated 2026-05-04. Cohort lock MD5 verified.
