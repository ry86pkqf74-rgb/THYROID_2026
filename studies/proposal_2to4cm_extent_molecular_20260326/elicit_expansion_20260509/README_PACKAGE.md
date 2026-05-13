# EXT2-4 Elicit-expansion manuscript package v2 — 2026-05-09

This zip is the fully updated manuscript package that consolidates the
2026-05-09 BigQuery-canonical re-analysis of the EXT2-4 (2–4 cm Extent +
Molecular) study, with **diagnostic performance computed from the actual
platform-reported call** (Afirma `overall_result_class`; ThyroSeq
`rom_descriptor` + `overall_result_class`) — not the earlier derived call
that was superseded mid-session.

## Read order

1. **`manuscript_v2_draft.docx`** — IMRAD draft (working). Open this first.
2. **`executive_summary_elicit_alignment.md`** — 1–2 page summary mapping the
   findings to the Elicit systematic-review evidence gaps. Read this before
   citing any number.
3. **`data_dictionary.md`** — field-by-field provenance from BQ canonical
   tables, the cohort-definition delta vs the EXT2-4 v1 (DuckDB) draft, and
   the molecular-call rule (corrected version).
4. **`tables/` and `figures/`** — see file-by-file map below.
5. **`sql/`** — six BigQuery SQL files that regenerate every aggregate count.

## What's in the zip

```
manuscript_v2_package_20260509/
├── README_PACKAGE.md                     ← this file
├── manuscript_v2_draft.docx              ← IMRAD draft (working)
├── executive_summary_elicit_alignment.md
├── data_dictionary.md
├── cohort_flow_bq.csv
├── tables/
│   ├── table1_cohort_overall_and_2to4cm.csv
│   ├── table2_malignancy_by_bethesda_size_era.csv
│   ├── table2b_surgical_extent_by_bethesda_size_era.csv
│   ├── table3_v2_diagnostic_performance_actual_reported_call.csv   ← CORRECTED Table 3
│   ├── table3_v2_rom_pct_descriptive_stats.csv                    ← ThyroSeq numeric ROM%
│   ├── table4_recurrence_by_molecular_status.csv
│   └── superseded/
│       ├── SUPERSEDED_NOTE.md
│       └── table3_diagnostic_performance_thyroseq_vs_afirma_DERIVED_CALL.csv
├── figures/
│   ├── fig_cohort_flow_bq_20260509.png
│   ├── fig2_forest_diagnostic_performance.png  (+ .pdf)
│   ├── fig3_rom_pct_distribution.png          (+ .pdf)
│   └── fig4_era_trends.png                    (+ .pdf)
├── sql/
│   ├── 01_cohort_flow.sql
│   ├── 02_table1_cohort_characteristics.sql
│   ├── 03_table2_malignancy_by_bethesda_size_era.sql
│   ├── 04_table3_diagnostic_performance.sql                  ← the SUPERSEDED derived-call query (kept for audit)
│   ├── 04b_table3_v2_actual_reported_call.sql                ← USE THIS for Table 3
│   ├── 05_table4_recurrence_by_molecular_status.sql
│   └── 06_surgical_extent_by_bethesda_size_era.sql
├── build_elicit_expansion.py                  ← rebuilds Tables 1, 2, 2b, 4 + cohort flow figure
├── build_table3_v2_actual_call.py             ← rebuilds Table 3 v2 + ROM%
├── build_figures_v2.py                        ← rebuilds Figures 2, 3, 4
└── build_manuscript_docx.js                   ← rebuilds manuscript_v2_draft.docx
```

## Headline numbers — v3 (must reconcile across docx + CSVs + figures)

**Updated 2026-05-13 after mig_323 platform reclassification + Afirma rescue.** v2 numbers preserved at `superseded_v2/`; v1 derived-call numbers at `tables/superseded/`.

- **Surgical cohort:** n=8,368 (1999–2025; lobectomy or total thyroidectomy with resolved date)
- **Pre-2015 vs 2015+:** 3,756 / 4,612
- **Preoperative imaging 2.0–4.0 cm subgroup:** n=400 (392 in 2015+; 222/400 = 55.5% initial total thyroidectomy; 232/400 = 58.0% malignant on path)
- **Bethesda III/IV with named platform:** 497 patients
- **Bethesda III/IV with classifiable reported call AND final histology (v3):** **317** evaluable in the binary 2×2 (Afirma **91**, ThyroSeq **226**) + ThyroSeq INTERMEDIATE-band as a separate third category; **17** ThyroSeq not-classifiable (down from 165 in v2 — 90% reduction)
- **Afirma B3+B4 all sizes (v3, Strict, NIFTP=benign):** Sens **90.4%** [79.4–95.8], Spec **20.5%** [10.8–35.5], PPV **60.3%** [49.2–70.4], NPV **61.5%** [35.5–82.3]
- **ThyroSeq B3+B4 all sizes (v3, Strict):** Sens **69.7%** [60.5–77.6], Spec **63.2%** [54.2–71.4], PPV **63.9%** [54.9–71.9], NPV **69.2%** [59.9–77.1]
- **ThyroSeq B3+B4 2–4 cm (v3, Strict, n=31):** Sens **86.7%** [62.1–96.3], Spec **75.0%** [50.5–89.8], PPV **76.5%** [52.7–90.4], NPV **85.7%** [60.1–96.0]
- **Path-proven recurrence among malignant cases:** Afirma 0/137 (follow-up artifact), ThyroSeq 4/161 (2.5%), Other 68/2,538 (2.7%), Untested 4/257 (1.6%)

**Canonical-layer coverage (post-mig_323):** Afirma `frac_classified` = **98.1%** (per-platform gate passed); ThyroSeq = **90.4%** (per-platform gate near-miss, source-limited — see `VC-MOL-PARSE-002`). `thyroid-integration` skill bumped to **v2.2.0**.

## Reproduction

```
# 1. (optional) re-pull aggregate counts from BigQuery
bq query --use_legacy_sql=false < sql/01_cohort_flow.sql
bq query --use_legacy_sql=false < sql/02_table1_cohort_characteristics.sql
bq query --use_legacy_sql=false < sql/03_table2_malignancy_by_bethesda_size_era.sql
bq query --use_legacy_sql=false < sql/04b_table3_v2_actual_reported_call.sql
bq query --use_legacy_sql=false < sql/05_table4_recurrence_by_molecular_status.sql
bq query --use_legacy_sql=false < sql/06_surgical_extent_by_bethesda_size_era.sql
# (If counts have shifted, update the hardcoded blocks in the build_*.py scripts.)

# 2. rebuild tables and figures
python3 build_elicit_expansion.py
python3 build_table3_v2_actual_call.py
python3 build_figures_v2.py

# 3. rebuild manuscript .docx (requires Node + `npm install docx`)
node build_manuscript_docx.js manuscript_v2_draft.docx figures/
```

## Hard-rules compliance

- **No PHI**: all outputs are research_id-grain or aggregate. No raw note text. Histology is normalized via case-insensitive keyword classifier; raw `histology_final` strings are not quoted.
- **Append-only logging**: three Manuscript Feedback Log rows on Airtable for this expansion:
  - `MFL-20260509-EXT2-4-ELICIT-EXPANSION` — initial expansion
  - `MFL-20260509-EXT2-4-TABLE3-CORRECTION` — switch from derived to actual reported call
  - `MFL-20260509-EXT2-4-FULL-PACKAGE-v2` — this manuscript package
- **No deletion**: the original derived-call Table 3 is preserved at `tables/superseded/`. The original EXT2-4 v1 manuscript prose (`abstract_structured_v1.md`, `manuscript_submission_v1.md`) was not modified by this expansion.
- **Manuscript-Locked**: not applicable — EXT2-4 lifecycle stayed `Active`.

## Open follow-ups

- File `VC-MOL-PARSE-001` Verification Check for the 165 ThyroSeq tests with non-classifiable reported call (rom_descriptor IS NULL AND overall_result_class NOT IN ('positive','negative')).
- File two Notable Findings: `NF-2026-05-09-ext24-verification-bias-quantified` and `NF-2026-05-09-thyroseq-vs-afirma-actual-call-pattern`.
- Re-run multivariable logistic models on the BigQuery cohort with BH-FDR adjustment for the univariable battery; reconcile the 8,368-vs-558 cohort definitional delta first.
- Re-derive completion thyroidectomy ascertainment on BQ; the EXT2-4 v1 dual-definition `table7_completion_thyroidectomy.csv` remains authoritative until then.
- Decision-curve / NRI / IDI analyses if the manuscript is reframed toward decision-impact.
- Multi-center pooling using actual platform-reported call (rather than risk tier) for size-stratified diagnostic-performance estimation.

## Provenance

- Source layer: `thyroid-canonical-pub-2026.pub_canonical.*` (BigQuery, query date 2026-05-09).
- Primary tables: `manuscript_cohort_v1` (n=10,871), `canonical_molecular_genetics_v2` (n=10,370), `canonical_recurrence_resolved_v1`, `canonical_us_nodule_v2`, `canonical_fna_events_v1`.
- Cohort build timestamp: see `manuscript_cohort_v1.cohort_build_timestamp` at query time.
- Author: Logan Glosser; co-authors per the existing EXT2-4 author-input checklist.
- Session: 2026-05-09 Cowork session (full transcript referenced in the MFL rows).
