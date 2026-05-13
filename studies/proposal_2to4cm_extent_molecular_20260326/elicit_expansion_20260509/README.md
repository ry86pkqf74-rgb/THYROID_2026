# EXT2-4 Elicit-driven expansion (2026-05-09)

This subfolder is an additive expansion of the 2–4 cm extent + molecular manuscript (`EXT2-4`) that responds to the evidence-gap list in the attached Elicit systematic review (80 studies, 1999–2026; surgical decision-making in thyroid nodules). It does **not** modify the v1 submission package (abstract, manuscript draft, or any prose). It rebuilds the cohort numbers on the **BigQuery canonical layer** (parent EXT2-4 was DuckDB-based), adds **formal diagnostic performance with Wilson 95% CIs** (parent had only descriptive 2×2 — explicitly listed as a gap in `MANUSCRIPT_GAP_LIST.md`), and stratifies by era (pre-2015 vs 2015+) and size band.

## What's in this folder

| File | Description |
|---|---|
| `executive_summary_elicit_alignment.md` | 1–2 page exec summary with headline findings, mapped to Elicit gaps. Read this first. |
| `data_dictionary.md` | Field-by-field provenance, cohort-definition delta vs EXT2-4 v1, and audit guidance. |
| `cohort_flow_bq.csv` | Cohort flow on the BQ canonical layer. Step-by-step n's. |
| `figures/fig_cohort_flow_bq_20260509.png` | High-res figure of the cohort flow (log-scale horizontal bars). |
| `tables/table1_cohort_overall_and_2to4cm.csv` | Cohort characteristics: overall, lobe vs total, size bands, era. |
| `tables/table2_malignancy_by_bethesda_size_era.csv` | Malignancy rate (strict + NIFTP-inclusive) by Bethesda × size × era with Wilson 95% CIs. |
| `tables/table2b_surgical_extent_by_bethesda_size_era.csv` | Total thyroidectomy rate by Bethesda × size × era. |
| `tables/table3_v2_diagnostic_performance_actual_reported_call.csv` | **CORRECTED Table 3.** Sens/Spec/PPV/NPV with Wilson 95% CIs for the actual platform-reported call (Afirma `overall_result_class`; ThyroSeq `rom_descriptor` + `overall_result_class`) in B3, B4, B3+B4 × {2–4 cm, <2 cm, unknown, all sizes} × {NIFTP-as-benign, NIFTP-as-malignant}. INTERMEDIATE-only ThyroSeq calls reported as a third category, not pooled. Built by `build_table3_v2_actual_call.py`; SQL `sql/04b_table3_v2_actual_reported_call.sql`. |
| `tables/table3_v2_rom_pct_descriptive_stats.csv` | Numeric ROM% (median [IQR]) by platform × reported call × histology. Afirma shows "n/a — binary call only". |
| `tables/superseded/table3_diagnostic_performance_thyroseq_vs_afirma_DERIVED_CALL.csv` | **Superseded.** Original Table 3 used a derived call from molecular_risk_tier + mutation flags. Preserved per the project's append-only rule. See `tables/superseded/SUPERSEDED_NOTE.md`. |
| `tables/table4_recurrence_by_molecular_status.csv` | Path-proven recurrence (biopsy or op-path documented) by molecular group + mutation class, per user definition. |
| `sql/01_cohort_flow.sql` | Reproduces `cohort_flow_bq.csv`. |
| `sql/02_table1_cohort_characteristics.sql` | Reproduces Table 1 row-by-row aggregates. |
| `sql/03_table2_malignancy_by_bethesda_size_era.sql` | Reproduces Table 2 cell counts. |
| `sql/04_table3_diagnostic_performance.sql` | Reproduces the **superseded** Table 3 (derived call). Kept for audit. |
| `sql/04b_table3_v2_actual_reported_call.sql` | **Reproduces the corrected Table 3** using actual reported call from `canonical_molecular_genetics_v2`. |
| `sql/05_table4_recurrence_by_molecular_status.sql` | Reproduces Table 4 cell counts. |
| `sql/06_surgical_extent_by_bethesda_size_era.sql` | Reproduces Table 2b cell counts. |
| `build_elicit_expansion.py` | Pure-Python script that consumes the SQL aggregate counts (hardcoded) and emits all CSVs + the figure. Wilson CIs computed here. No BigQuery client required to rerun. |

## Reproduction

If you only want to re-render the CSVs and figure from the saved aggregate counts:
```
cd studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/
python3 build_elicit_expansion.py
```

If you want to re-derive the aggregate counts from BigQuery (e.g., after `manuscript_cohort_v1` is rebuilt), run each SQL in `sql/` against `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1` and update the hardcoded blocks in `build_elicit_expansion.py` (`table1_strata`, `table2_input`, `table2b_input`, `table3_input`, `table4_input`, `cohort_flow`).

## How this relates to the EXT2-4 parent manuscript

| Aspect | EXT2-4 v1 (parent) | This expansion |
|---|---|---|
| Source layer | DuckDB pipeline (`study_pipeline.py`) | BigQuery canonical (`manuscript_cohort_v1`) |
| Primary cohort N | 558 (preop 2–4 cm + strict nodal exclusion) | 8,368 (whole surgical cohort), 400 (preop 2–4 cm subset) |
| Molecular subset | 20 (3.6% of 558) | 238 head-to-head (B3/B4 × Afirma|ThyroSeq × histology) |
| Diagnostic performance | Descriptive 2×2 only | Sens/Spec/PPV/NPV + Wilson 95% CIs (actual reported call from canonical_molecular_genetics_v2; INTERMEDIATE as third category; ROM% descriptive stats) |
| Era stratification | Not explicit | Pre-2015 vs 2015+ throughout |
| Recurrence | Not in primary outputs | Path-proven only, per user direction |
| Manuscript prose | Submission v1 in folder | Untouched |

## Hard-rules compliance

- **No PHI**: all outputs are research_id-grain or aggregate. `histology_final` text is normalized, never quoted. No raw note text.
- **Append-only logging**: a Manuscript Feedback Log row (`MFL-20260509-EXT2-4-ELICIT-EXPANSION`, `recDdqL9CDf4iZPQZ`) was appended to Airtable BEFORE any file was written. Lifecycle on EXT2-4 stayed `Active`; no unlock was needed.
- **No deletion**: nothing in the parent EXT2-4 folder was modified. This is an additive subfolder.
- **Manuscript-Locked**: not applicable — EXT2-4 is `Active`/`Drafting`.

## Open follow-ups (not done this session)

- Re-run multivariable logistic models (parsimonious + extended + ThyroSeq-only + Afirma-only) on the BQ cohort with BH-FDR adjustment for the univariable battery. Reconcile the 558→400 / 635→400 cohort delta first.
- Re-derive completion thyroidectomy ascertainment on BQ (parent EXT2-4 has dual-definition OED-only vs path-synoptic-definite already; not redone here).
- Filing of Notable Finding: `NF-2026-05-09-ext24-verification-bias-quantified` (PPV vs NPV bias asymmetry in a surgical cohort with named-platform molecular testing) — recommended in the executive summary §3.
- Filing of Notable Finding: `NF-2026-05-09-thyroseq-vs-afirma-specificity-gap` if the ThyroSeq–Afirma specificity contrast (84% vs 26%) holds after accounting for the Afirma+Xpression-Atlas readout conflation.
- Decision-curve / NRI / IDI analyses if the manuscript is reframed toward decision-impact.
