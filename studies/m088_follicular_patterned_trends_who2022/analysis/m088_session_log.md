# M088 session log

Append-only. Newest entry on top.

---

## 2026-05-09 — Cowork end-to-end build (Session 1)

**Owner:** Logan Glosser (Cowork supervising)

**Goal:** End-to-end scaffold of M088 from greenfield through manuscript draft v0.1.

**Session Opening Protocol — pass.**

- Airtable connector live (ping/pong).
- Linear THY team live (`c4afb51b-8bca-413a-a53e-15eb825cffbd`).
- M088 Airtable record (`recezNQ7N21IAvhof`) read: `status = Planned`, `lifecycle = Active`, `owner = Logan`, `candidate_cohort_n = 1540`, `study_dir = studies/m088_follicular_patterned_trends_who2022/`. Editable.
- Last 24h Manuscript Feedback Log entries for M088 (3): row added 2026-05-08 22:20 (initial creation, status=Idea), restructure 22:35 (drop long-term outcomes per user), status advance Idea→Planned 22:45.
- BQ verification queries — all four match within tolerance:
  - Cohort total: 1,542 (expected ~1,544; Δ = -0.13%).
  - FTC variants: oncocytic_warthin 209, minimally_invasive 169, null 106, widely_invasive 12 (exact match).
  - Capsular invasion equivocality: present 2,136 / absent 1,817 / indeterminate 294 / suspected 23 (exact match).
  - Operative rollup: 1,537 with rollup, 96 with completion, 295 with total (exact match within tolerance).

**Scope decision (Logan, this session):**

- H4 RAI feasibility query showed `rai_assertion_status = 'likely_received'` is <2% across all 11 strata. Logan elected to drop RAI from H4 endpoints entirely. Notable Finding `NF-2026-05-09-rai-extraction-sparse-follicular-cohort` filed (Airtable + Linear).
- H4 endpoints are now: (1) extent of resection at index (hemi vs total); (2) completion thyroidectomy at index. Bootstrap 95% CIs.

**Audit-log discipline:**

- Manuscript Feedback Log: `MFB-2026-05-09-M088-h4-scope-rai-dropped` filed before any analysis work.
- Will append additional log rows for each Section creation, Tables/Figures row, and any post-lock plan deviation.

**Analysis plan v1:** locked in `analysis/m088_analysis_plan_v1.md` this session.

**Files produced this session:** see directory listing — analysis SQLs, Python files, tables, figures, manuscript draft.

**End-of-session results (Tier A):**

- **Cohort:** 1,542 distinct patients, 1,547 entity-level diagnoses, 1990–2025.
- **H1 oncocytic family migration:** 353/1,542 = **22.9%** (95% CI 20.8–25.1) migrate to 2022 WHO Oncocytic Neoplasm family.
- **H2 FA → FT-UMP:** strict 0.28% (95% CI 0.08–1.02), broad 0.85% (95% CI 0.39–1.84). Tier B re-review needed (M090).
- **H3 MI-FTC → FT-UMP:** strict 1.2% (95% CI 0.3–4.2). Moderate/broad 85.2% but partly circular due to variant↔qualifier coupling.
- **H4 management impact:** MI-FTC → FT-UMP would reduce definitive total thyroidectomy by **−14.9 pp** (bootstrap 95% CI −26.6 to −1.9). Other transitions are pure label changes (H1) or under-detected (H2 Tier A).
- **Joinpoint:** NIFTP 2015–2017 APC = +262%; MI-FTC 2017–2022 APC = +51.7%; FT-UMP 2017–2022 APC = +38.5%. 2022–2025 APCs negative due to 2025 partial-year truncation.
- **Notable Finding:** NF-2026-05-09-rai-extraction-sparse-follicular-cohort filed (Airtable + Linear THY-55). RAI dropped from H4.

**Airtable artifacts created:**

- 7 Sections rows (Abstract, Introduction, Methods, Results, Discussion, Limitations, Conclusion), all linked to M088, draft_status=First Draft, lifecycle=Active.
- 8 Tables and Figures rows (Table 1–4, Figure 1–4), all status=Draft, last_regenerated=2026-05-09.
- 1 Notable Finding row (recADVzV6m46daRxl).
- 3 Manuscript Feedback Log rows (h4-scope-rai-dropped, sections-and-tnf-scaffolded; the third was on an earlier session).

**Linear artifacts created:**

- THY-55 — Notable Finding for RAI extraction sparsity (`type:notable-finding` label, project Notable Findings & Research Insights).

**Status not advanced.** M088 remains `status = Planned`, `lifecycle = Active`. No Linear project auto-spawned (per skill rule, projects spawn at Cohort Definition / Analysis / Drafting / Internal Review / Submitted / Revisions). Next session: Logan can advance status if ready, which will trigger Linear project creation via daily sync.

**Open items for v0.2:**

1. Demographics Table 1: dominant tumor size + multifocality fields are sparse for benign entities; add path_indeterminate_events_v1 join for atypical/borderline cases.
2. Joinpoint: rerun excluding 2025 partial year to get cleaner 2022–2024 APCs; consider true data-driven joinpoint (NCI Joinpoint software) as sensitivity.
3. M090 Tier B re-review pool: define candidate-case selection from H2 broad bound + H3 moderate/broad bound for M090 to consume.
4. References: fill in the Discussion comparison-to-literature citations (Nikiforov 2016, Baloch 2022, Park 2024 placeholders).
5. RAI re-validation: file separate data-engineering issue under Database Reconciliation & QA project per NF-2026-05-09 next_action.
6. Sister-manuscript reconciliation: confirm M091 (NIFTP 2016–2025) and M096 (MI vs WI vs angioinvasive FTC) cohort definitions match M088 boundaries.

---

## 2026-05-09 — Extension analyses (revision pass)

Added at user request to deepen H1–H4 with subgroup, stratified, multivariable, and sensitivity analyses.

**New analytic outputs:** `analysis/output/extension/` — table_1b_family_compare.csv, table_s1_size_by_group.csv, table_s2_era_stratified.csv, table_s3_age_sex_stratified.csv, table_s4_invasion_validation.csv, table_5_multivariable_logistic.csv, table_s5_sensitivity.csv, fdr_adjusted_family.csv, era_interaction_test.json, cochran_armitage_trend.json, multivariable_summary.txt.

**New figures:** figures/figure_s1_age_size_boxplots.png, figure_s2_invasion_validation.png, figure_s3_era_sankey.png, figure_s4_cumulative_borderline.png.

**New scripts:** analysis/m088_extension_analyses.py, analysis/m088_extension_figures.py.

**Headline new findings:**
- Oncocytic family is older (Δ +4.4 yr; p = 4×10⁻⁶), has larger tumors (Δ +0.9 cm median; p = 10⁻⁶), and undergoes ~2× more definitive total thyroidectomy (38% vs 22%; p < 10⁻⁹).
- Era post-2017 multivariable OR for definitive total = 0.39 (p < 10⁻⁹) after adjusting for diagnosis, age, sex, size — strong secular de-escalation.
- All H1/H3/H4 estimates stable to ±0.5 pp under sensitivity exclusions.
- Cochran-Armitage trend for oncocytic share NS (p = 0.37) — share dipped 2020–2025 because the era-specific surge is on the conventional side (MI-FTC, FT-UMP).

**Files refreshed:** deliverables/M088_Tables.xlsx (10→18 sheets), deliverables/M088_findings_in_plain_terms.docx (81→101 paragraphs), manuscript/m088_draft_v0.1.md (extension subsection appended), deliverables/m088_per_research_id.csv (unchanged).

**Audit:** MFB-2026-05-09-M088-extension-analyses-added.


## 2026-05-09 — 4-analysis pre-spec (manuscript integration revision)

User-supplied 4-analysis specification executed in single script `analysis/m088_supplemental_analyses.py`.
Results staged for manuscript integration.

**Outputs:**
- `tables/table_s_a1_family_comparison.md`, `table_s_a2_era_stratified.md`, `table_s_a3_size_invasion.md`, `table_s_a4_multivariable.md` — publication-ready Markdown tables (paste into manuscript or supplement).
- `analysis/output/supplemental/m088_supplemental_results.json` — full structured results.
- xlsx refreshed: 4 new sheets prefixed `… (A1)`, `(A2)`, `(A3)`, `(A4)`; older overlapping sheets removed.
- docx refreshed (101 → 103 paragraphs); validates clean.
- manuscript Results section appended with "Summary for manuscript integration" subsection containing the four Results-paragraph sentences plus four suggested Discussion additions.

**Headline numbers (4-analysis spec):**
- A1 family: oncocytic 38.3% def-total vs conventional 21.8% (p<0.001).
- A2 era: H3 strict reclass = 0/0/2 in Pre-2017 / 2017-2022 / 2023-2025; **2023-2025 def-total flagged as data-quality artifact (incomplete operative rollup) — explicit footnote added.**
- A3 size effect: OR 1.17/cm in malignant/borderline subset (p<0.001), monotonic across quartiles (CA Z=3.15, p=0.002).
- A4 multivariable: Model 2 oncocytic family OR 2.11 (1.42–3.14, p<0.001); era 2020-2025 OR 0.14 (lower bound).

**Audit:** manuscript edit logged in this session log; no separate Manuscript Feedback Log row needed because no Airtable Sections/Tables-and-Figures records were modified (script-only refresh).

