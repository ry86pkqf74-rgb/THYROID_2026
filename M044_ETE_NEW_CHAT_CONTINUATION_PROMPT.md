# M044 — Microscopic vs Gross ETE Manuscript: New Cowork Chat Continuation Prompt

**Last session:** 2026-05-01 (Claude Cowork)
**Repo:** `ry86pkqf74-rgb/THYROID_2026` (branch `main`, latest commit at session end was `8fa2456`)
**Goal of this manuscript:** Fully clean, verify, analyze, and draft the final microscopic-vs-gross ETE manuscript using the THYROID_2026 canonical publication v1.0 database. Final deliverable must include an Excel file containing **all per-patient analytic data points plus the original source data** for every variable used.

---

## Read-first artifacts (already on GitHub `origin/main`)

Before doing anything, read these files in order to absorb the current state:

1. `M044_ETE_validation_report.md` — independent verification of the ChatGPT handoff against MotherDuck. Key finding: the cohort-view `any_recurrence_flag` (n=503) and `structural_recurrence_flag` (n=1,819) are inconsistent with the canonical dual-track recurrence schema in `main.canonical_recurrence_resolved_v1`. Primary endpoint switched to `recurrence_path_proven` (n=145), with imaging-only-unconfirmed (n=195) and composite (n=340) as pre-specified secondary endpoints. Section 8b documents the full canonical_patient_master demographic audit.
2. `M044_ETE_analysis_plan.md` — pre-specified statistical analysis plan with primary, secondary, and sensitivity models.
3. `M044_ETE_analysis.sql` — reproducible SQL for cohort, Tables 1–5, and sensitivity analyses against `thyroid_canonical_publication_v1_0`.
4. `M044_ETE_demographics_addendum.md` — full canonical_patient_master (1,630 cols) audit pulling race, BMI, smoking (99.7% null — flagged), comorbidities, Hashimoto/Graves, multifocality, bilateral, margins, BRAF/TERT/RAS/RET, AGES, surg_total_thyroidectomy.
5. `M044_ETE_manuscript_draft.md` — manuscript draft v0.1 with Cursor's first multivariable fits populated (commit a953ae1). The Methods/Results section has been edited; some `[VERIFY]` placeholders may remain in Discussion/References.
6. `M044_ETE_supplement.md` — supplementary methods and Tables S1–S7 + PRISMA-style literature context from the Elicit report.
7. `M044_ETE_tables.xlsx` — tables workbook (Cover, Tables 1–5, Supp S1/S6/S7, Demographics & molecular, Data dictionary, Model outputs scaffold, QA).
8. `M044_ETE_claude_handoff_notes.md` — verified vs uncertain vs next-action items from the validation pass.
9. `cursor_prompts/CURSOR_PROMPT_M044_ETE_MULTIVARIABLE_MODELS_20260501.md` — first Cursor prompt (executed; commit a953ae1).
10. `cursor_prompts/CURSOR_PROMPT_M044_ETE_FOLLOWUP_REFITS_20260501.md` — second Cursor prompt (NOT yet executed — covers the five refits below).
11. `scripts/m044_ete_fit_models.py` (1,059 lines) — Cursor's Python script that fitted the primary, secondary, sensitivity, and Cox models.
12. `data/m044/analytic_file_v1.parquet` — the analytic file Cursor materialized (4,128 rows × ~50 columns).
13. `data/m044/m044_cox_primary_summary.csv` — Cox model output.
14. `figures/m044_forest_primary.png` and `figures/m044_forest_primary_data.csv` — forest plot from Cursor's primary logistic.

---

## Current state (commits on origin/main, in order)

- `58cfd19` — initial M044 deliverables (validation report, SQL, plan, draft v0.1, supplement, demographics addendum, tables xlsx, handoff notes, Cursor prompt #1).
- `32beb7b` — unrelated mig252 fix (CPM complication confirmed rollups).
- `a953ae1` — Cursor's first multivariable fit: primary logistic (path-proven), secondary endpoint models, sensitivity panel, Cox model, no/neg subgroup, tumor-size panel, forest plot, Table 3 populated, 1,059-line Python fit script.
- `0143539` — qc handoff (unrelated).
- `8fa2456` — second Cursor prompt (`CURSOR_PROMPT_M044_ETE_FOLLOWUP_REFITS_20260501.md`) for the five refits below.

---

## Headline clinical findings (from the work to date)

1. **Cohort:** n=4,128 patients; primary exposure ETE grouping based on `ete_grade_final`; primary outcome `recurrence_path_proven` from `main.canonical_recurrence_resolved_v1`.
2. **Crude path-proven recurrence rate:** 2.3% microscopic ETE, 5.8% gross ETE, 6.3% no/negative ETE (the no/negative signal is confounded — see #5).
3. **Person-year rates (FU>0):** 0.71, 1.76, 1.71 per 100 PY for microscopic, gross, no/negative.
4. **Crude OR gross-vs-microscopic = 2.61 (1.84–3.70); crude OR no/neg-vs-microscopic = 2.84 (1.50–5.39).**
5. **Cursor primary logistic regression:** gross-vs-microscopic adjusted OR = 1.39 (0.94–2.06, p=0.098); no/neg-vs-microscopic aOR = 1.03 (0.47–2.24, p=0.95).
6. **Cursor Cox model (surgery-date-known + FU>0; n=2,129):** gross-vs-microscopic HR = 2.10 (1.22–3.62, p=0.007).
7. **Five issues identified after the Cursor fit (documented in this prompt):** RAI confounding by indication, non-DTC histologies in the "other" bucket, NIFTP/FTUMP driving the "follicular-like protective" signal, ETE × N stage interaction (microscopic ETE N1b PP rate 27.6%), residual size-confounding within the no/negative-N1a cell.
8. **Pooled-LVI artifact reproduction:** in this dataset, pooling lymphatic + vascular and treating missing as absent produces an **elevated** OR 1.60 (p=0.017), not a protective one — the prior literature's protective LVI signal does not reproduce here.

---

## Outstanding work — what the new chat must finish

### Phase A — Run the second Cursor prompt and verify

Open `cursor_prompts/CURSOR_PROMPT_M044_ETE_FOLLOWUP_REFITS_20260501.md` and feed it to a Cursor / VS Code agent (or run it directly in the new chat if MotherDuck access is available). The prompt covers five pre-specified refits:

1. **Strict-DTC sensitivity** — exclude MTC, anaplastic, NIFTP, FTUMP, follicular adenoma, NUT, adenoid cystic. Cohort drops to ~3,783. Refit primary logistic and Cox.
2. **Drop RAI as covariate** — RAI is confounded by indication (RAI receipt rate 11% microscopic → 26% gross; RAI patients have 2× higher path-proven rate). Refit primary without RAI.
3. **Split histology factor** into PTC (ref) / FTC / Metastatic-PTC / Poorly-differentiated DTC / High-grade DTC.
4. **Test ETE × N stage interaction.** Microscopic ETE N1b had PP rate 27.6% (n=29, 8 events) — higher than gross-ETE N1b (14.6%). Document whether this interaction is significant.
5. **No/negative-ETE subgroup model** with size, N stage, central/lateral compartment flags, RAI, ≥2-surgery indicator, days-to-second-surgery as covariates. Confirm whether the residual within-stratum no/neg signal is fully explained.

After Cursor finishes, verify the numbers against MotherDuck directly (use the SQL in `M044_ETE_analysis.sql` as the source of truth).

### Phase B — Build the per-patient analytic Excel with original source data

**This is a new requirement.** Every manuscript final deliverable must include an Excel file that contains both the analytic-file rows and the original source-table values for every variable. For M044, build `M044_ETE_master_data.xlsx` with these tabs:

1. **Cover** — manuscript title, cohort definition, n, date prepared, source database/tables, contact.
2. **Patient-level analytic file** — one row per `research_id` (n=4,128) with all derived variables used in any model: ETE group, age, sex, race, BMI, histology grouped, tumor size, AJCC T/N/M/stage group, lvi_clean, vasc_clean, central/lateral/total LN positive flags, RAI receipt, all comorbidity flags, multifocality, bilateral, margins, BRAF/TERT/RAS, surg_total_thyroidectomy, AGES, follow-up years, all recurrence endpoints (path-proven, imaging-only, composite, imaging-then-path), and reoperative covariates.
3. **Source — cohort_m044_ajcc_ete_v1** — raw 29-column dump from the cohort view.
4. **Source — canonical_patient_master selected columns** — the ~50 CPM columns used in the demographics addendum and analysis, raw values.
5. **Source — canonical_recurrence_resolved_v1** — the 20-column raw recurrence table joined to the cohort.
6. **Source — ln_master_rollup_v1 (pre-aggregated)** — the LN rollup MAX(...) per `research_id`.
7. **Source — cohort_m040_reoperative_v1 (pre-aggregated)** — the reoperative MAX(...) per `research_id`.
8. **Source — path_synoptics LVI/vascular re-extract** — the raw `tumor_1_lymphatic_invasion`, `tumor_1_angioinvasion`, `tumor_1_angioinvasion_quantify`, `tumor_1_extrathyroidal_extension` for source-of-truth audit.
9. **Crosswalk — derived → raw source** — variable-by-variable map showing how each derived analytic column is built from raw source columns, including the cleaning rules in `M044_ETE_analysis.sql` (e.g., `lvi_clean` collapses `preesent`, `extensivre`, etc.).
10. **Data dictionary** — type, definition, allowed values, source object for every column on the patient-level analytic file.
11. **QA / data quality flags** — every patient with a known data quality issue (smoking missing, BMI missing, surgery date missing, lvi_grade spelling variant, ete_grade_final='true' ambiguity, recurrence flag mismatch with canonical resolved, completion-pathway recurrence ascertainment).

The build pattern: SELECT each source object from MotherDuck, JOIN to `cohort_m044_ajcc_ete_v1.research_id`, write each as its own tab using openpyxl. Use the same MAX(...) per-research-id aggregation on `ln_master_rollup_v1` and `cohort_m040_reoperative_v1` as in `M044_ETE_analysis.sql` so analytics reproduce.

This file is the canonical handoff for journal submission and for any reviewer who wants to inspect the data. Save it to `M044_ETE_master_data.xlsx` in the repo root.

**Standing rule going forward:** Apply the same pattern to every manuscript final — analytic file + raw source data + crosswalk + dictionary + QA flags in one workbook. Add this rule to the manuscript-workflow README.

### Phase C — Finalize manuscript draft v1.0

After Phase A refits land:

1. **Methods:** lock cohort definition (strict-DTC, n=~3,783); finalize covariate list; mention RAI is a sensitivity, not primary; specify the exact `ete_grade_final` mapping; reference the master data Excel as a supplementary file.
2. **Results — Multivariable analysis:** populate Table 3 with the strict-DTC primary results (with-RAI and without-RAI rows). Lead with the strict-DTC + no-RAI primary OR (expected to be larger than the current 1.39, closer to the crude 2.6). Report the Cox HR. Discuss the ETE × N stage interaction. Discuss the histology coefficients with the new five-level factor.
3. **Results — Negative ETE subgroup:** update with the new subgroup-specific logistic. State explicitly that the global aOR went to 1.03 mostly via tumor-size and N-stage adjustment, but within-stratum the signal is partial; the explanatory mechanism is lateral-LN ascertainment + completion-pathway, not biology.
4. **Results — Lymphatic/vascular:** update with the new strict-DTC fits.
5. **Discussion paragraph 1:** rewrite to lead with the strict-DTC + no-RAI primary findings. Acknowledge the logistic-vs-Cox split was largely driven by RAI confounding-by-indication and by the heterogeneous histology bucket (NIFTP/FTUMP/anaplastic/MTC).
6. **Limitations:** flag smoking-status unusable, BMI missing in 80%, family history under-extracted by NLP, 22% of patients missing surgery date, 1,400 zero-FU patients.
7. **Tables 1, 2, 3, 4** — finalize with strict-DTC denominators where appropriate.
8. **Figures:**
   - Figure 1: Cohort flow diagram (4,128 → strict-DTC ~3,783 → analytic; show exclusions).
   - Figure 2: ETE group distribution.
   - Figure 3: Path-proven recurrence rate by ETE group with 95% CI.
   - Figure 4: Path-proven /100 PY by ETE group.
   - Figure 5: Forest plot from strict-DTC + no-RAI primary logistic regression.
   - Figure 6: Kaplan-Meier path-proven recurrence-free survival by ETE group on surgery-date-known subset.
   - Figure 7: No/negative ETE explanatory panel.
9. **References:** verify all in Zotero; replace `[VERIFY DOI]` placeholders. The Elicit report (`Elicit - Microscopic vs Gross ETE in Thyroid Cancer Outcome - Report.pdf`) is the source for the citation list. Convert to journal-required format (Vancouver, AMA, etc.).
10. **IRB / data protection / authorship:** populate the placeholders.

### Phase D — Final QA and submission package

1. Run `python scripts/recalc.py M044_ETE_tables.xlsx` and `python scripts/recalc.py M044_ETE_master_data.xlsx` — confirm zero formula errors.
2. Cross-verify every number in the manuscript abstract and tables against MotherDuck queries from `M044_ETE_analysis.sql`. Flag any mismatch.
3. Build the journal submission package: title page, manuscript .docx, tables (separate file), figures (separate files at submission DPI), supplement, response-to-reviewers template (empty), `M044_ETE_master_data.xlsx` (supplementary file), `M044_ETE_analysis.sql` (supplementary code).
4. Pre-check against journal guidelines (target journal TBD by Logan).
5. Push everything to `origin/main` with one final commit.

---

## Standing rules (apply to every manuscript)

1. **Demographics + full-canonical-schema review** must be done at the start of validation, not as a follow-up. The 29-column M044 cohort view is a working subset; the master is `main.canonical_patient_master` (1,630 cols).
2. **Recurrence dual-track is mandatory.** Use `recurrence_path_proven`, `recurrence_imaging_suspicious`, and `recurrence_status_final` from `main.canonical_recurrence_resolved_v1`. Never collapse into `any_recurrence`.
3. **Lymphatic and vascular invasion are separate variables.** Always model them with explicit missing/indeterminate categories. Never recode missing as absent.
4. **Treatment variables are not predictors.** RAI receipt is confounded by indication; report as sensitivity, not primary.
5. **Strict-DTC inclusion** for any DTC manuscript: PTC, FTC, poorly-differentiated DTC, high-grade DTC, metastatic-PTC. Exclude MTC, anaplastic, NIFTP, FTUMP, follicular adenoma, NUT carcinoma, adenoid cystic.
6. **Master Excel deliverable** for every manuscript final: patient-level analytic file + raw source data + crosswalk + dictionary + QA flags in one workbook.
7. **Pre-aggregate** any source object that is many-rows-per-patient (LN rollup, reoperative, surgical events) using `MAX(...)` per `research_id` before joining to the cohort.

---

## Cohort identifiers and SQL anchors

- Database: `thyroid_canonical_publication_v1_0`
- Cohort view: `manuscript_workspace.cohort_m044_ajcc_ete_v1` (n=4,128)
- Recurrence column-of-record: `main.canonical_recurrence_resolved_v1` (build mig_62 2026-04-27)
- LN rollup: `manuscript_workspace.ln_master_rollup_v1` (pre-aggregate with MAX(...))
- Reoperative: `manuscript_workspace.cohort_m040_reoperative_v1` (pre-aggregate with MAX(...))
- Master patient features: `main.canonical_patient_master` (1,630 cols)
- Pathology source: `main.path_synoptics`

Spot-check (must reproduce):
```sql
SELECT COUNT(*) AS n,
       SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS legacy_n,
       ROUND(MEDIAN(followup_years),3) AS med_fu
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1;
-- Expected: 4128, 503, 1.002
```

Canonical recurrence:
```sql
SELECT SUM(CASE WHEN recurrence_path_proven THEN 1 ELSE 0 END) AS pp,
       SUM(CASE WHEN recurrence_status_final='imaging_only_unconfirmed' THEN 1 ELSE 0 END) AS img_only,
       SUM(CASE WHEN recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1 ELSE 0 END) AS comp
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id);
-- Expected: 145, 195, 340
```

---

## Acceptance criteria for the new chat

You are done when:

1. The five refits in `CURSOR_PROMPT_M044_ETE_FOLLOWUP_REFITS_20260501.md` are executed and the strict-DTC + no-RAI primary results are populated in Table 3 and the manuscript Results.
2. `M044_ETE_master_data.xlsx` exists in the repo root with all 11 tabs described in Phase B and zero formula errors.
3. The manuscript draft has all `[VERIFY]` placeholders resolved (or annotated with a defensible reason for remaining).
4. Tables 1–4 are finalized; Supplement Tables S1–S7 are reproduced with strict-DTC numbers; QA tab is current.
5. Figures 1–7 exist as PNGs (and CSVs of the underlying data) in `figures/`.
6. References are populated and verified (or clearly flagged for Zotero).
7. Final commit pushed to `origin/main` with a single comprehensive message.

---

## Notes on data-quality flags to monitor

- `pmhx_nlp_smoking_status` is 99.7% null — do not report.
- `bmi_combined` is 80% missing — descriptive only.
- Family-history fields are under-extracted (≤12 patients in the cohort).
- `surg_first_date` missing for 22.1% of patients.
- 1,400 patients (33.9%) have `followup_years = 0`.
- Two patients with surgery dates pre-1999 (earliest 1945-07-13) — exclude in sensitivity, document in cohort flow.
- 318 patients have `any_recurrence_flag = TRUE` but `recurrence_status_final = 'none'` — legacy flag noise; do not use legacy flag as primary endpoint.
- 1,467 patients have `structural_recurrence_flag = TRUE` with no canonical evidence — do not use this flag.
- 4 patients with `ete_grade_final = 'true'` (ambiguous from `tumor_episode_master_v2`) — currently in Missing/other per ChatGPT's grouping.

---

## How to invoke this in the new chat

Open a fresh Cowork chat, attach this file (`M044_ETE_NEW_CHAT_CONTINUATION_PROMPT.md`), and say:

> "Read this prompt and the artifacts it references. The goal is to finalize the M044 microscopic-vs-gross ETE manuscript per the standing rules. Run the second Cursor prompt's five refits, build the per-patient master Excel with original source data, finalize the manuscript draft v1.0, and produce a journal-ready submission package. Reference the SQL package as the source of truth and verify every number against MotherDuck before finalizing."

End of continuation prompt.
