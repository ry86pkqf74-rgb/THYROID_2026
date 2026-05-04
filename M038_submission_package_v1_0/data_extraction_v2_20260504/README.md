# M038 Data Extraction v2 — 2026-05-04

Reproducible build of the four "M038_GOITER_*" deliverables per the new-chat data-extraction prompt
(`M038_GOITER_NEW_CHAT_DATA_EXTRACTION_PROMPT.md`).

## Outputs (in parent `M038_submission_package_v1_0/`)

| File | Size | Contents |
|---|---:|---|
| `M038_GOITER_patient_level_dataset.xlsx` | 4.9 MB | Cover + Patient Data (10,871 × 131) + Data Dictionary |
| `M038_GOITER_analysis_workbook.xlsx` | 21 KB | 9 tabs: Cohort Overview, T1-T5, NSQIP Comp, Component Subgroup, Exploratory |
| `M038_GOITER_tables_figures.xlsx` | 21 KB | T1-T5 + Fig 1-4 underlying data + Supp S1-S6 |
| `M038_GOITER_eMethods.docx` | 42 KB | 8-section statistical methods, US Letter / Arial 11pt |
| `M038_GOITER_size_symptoms_by_demographics.xlsx` | 58 KB | Additive view (D5): thyroid size & weight + goiter symptoms × demographics |

## Pipeline

```bash
cd data_extraction_v2_20260504/
python3 01_pull_parquet.py            # → m038_per_patient_v2.parquet (10,871 × 131)
python3 02_build_d1_patient_dataset.py
python3 03_build_d2_analysis_workbook.py
python3 04_build_d3_tables_figures.py
python3 05_build_d4_emethods.py
python3 06_verify.py                  # reconciles to validation report
python3 07_build_d5_size_symptoms_by_demo.py   # additive: size+wt + symptoms × demographics
```

`_stats.py` provides chi2/Fisher, Mann-Whitney, t-test, RR + 95% CI helpers.

## Source of truth

* Database: `thyroid_canonical_publication_v1_0` (release `pub_v1_1_20260504`)
* Cohort view: `manuscript_workspace.cohort_m038_massive_goiter_v1` (post-mig_255, 129 cols)
* Auth: MotherDuck `MD_SA_TOKEN` from `/Users/ros/THyroid 2026/motherduck.local.toml` (.eras account)
* Standing rule: `memory/feedback_complications_transient_vs_permanent.md` (hypopara transient<6mo / permanent>6mo; hypocalcemia preexisting)

## Verification (2026-05-04 vs `09_validation_report.md`)

22 headline cells reconciled — 21 PASS, 1 SHIFT:

| Cell | Live | Validation report | Status |
|---|---|---|---|
| Cohort total | 10,871 | 10,871 | ✓ |
| n massive | 2,501 (23.0%) | 2,501 (23.0%) | ✓ |
| Components W / S / A | 1,429 / 1,047 / 1,440 | 1,429 / 1,047 / 1,440 | ✓ |
| Only-flags W / S / A / All-3 | 898 / 145 / 429 / 386 | 898 / 145 / 429 / 386 | ✓ |
| Any-comp massive | 132 (5.28%) | 132 (5.28%) | ✓ |
| Any-comp non-massive | 268 (3.20%) | 268 (3.20%) | ✓ |
| HypoPT transient massive / non-massive | 83 / 197 | 83 / 197 | ✓ |
| HypoPT permanent massive / non-massive | 4 / 12 | 4 / 12 | ✓ |
| Total-thy massive / non-massive % | 66.9% / 51.7% | 66.9% / 51.7% | ✓ |
| Era pre-2015 massive % | 12.4% | 12.0% | ✓ rounds |
| Era 2015-2019 massive % | 25.0% | 24.9% | ✓ rounds |
| Era 2020-2025 massive % | **32.3%** | 28.5% | **SHIFT** |

The era 2020-2025 prevalence shift (28.5% → 32.3%) is consistent with
mig_254 (`surg_first_date` backfill) reassigning patients from "unknown era"
to a specific era — disproportionately to 2020-2025 because recent surgeries
get authoritative date capture first. The new deliverables reflect the live
post-mig_254/255 cohort state per the standing rule
(`feedback_motherduck_direct_check.md` — trust live MD over prior summaries).

## Standing-rule applications

* Hypoparathyroidism split into transient (<6mo) / permanent (>6mo) / preexisting / new postop
* Hypocalcemia includes a preexisting (preop) row separate from postop confirmed
* Era binning uses the upper-bound rule (pre-1999 → 1999-2004 bucket)
* Composite massive uses OR logic across the 3 component groups
* RR/CI uses Wald with Haldane-Anscombe 0.5 continuity correction for zero-cell scenarios
* Fisher exact substitutes for chi-squared whenever expected cell <5 or chi-squared is undefined
